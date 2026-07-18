"""Proxy pool orchestration for opencode platform.

Combines fetchers from ``proxyfetch.py`` and data classes from
``proxymodels.py`` into a single master fetch entry point.
"""
from __future__ import annotations

import time
from typing import Optional

from src.foundation.logger import get_logger

from ..consts import PROXY_DEFAULT_FETCH_PAGES
from .pxyfetch import (
    fetch_api_all_protocols,
    fetch_api_proxies,
    fetch_multiple_pages,
    fetch_page_proxies,
    fetch_text_proxies,
)
from .pxymodels import ProxyInfo, ProxyPool

log = get_logger("opencode.proxypool")

# Re-exported for backward compatibility with existing imports of this module.
__all__ = [
    "ProxyInfo",
    "ProxyPool",
    "fetch_api_proxies",
    "fetch_api_all_protocols",
    "fetch_page_proxies",
    "fetch_multiple_pages",
    "fetch_text_proxies",
    "fetch_all_proxies",
]


def _fetch_into_pool(
    pool: ProxyPool,
    sess,
    *,
    num_pages: int,
    include_api: bool,
    include_text: bool,
    include_pages: bool,
) -> None:
    """依次从各来源拉取代理并汇入 pool。"""
    if include_api:
        log.debug("Fetching proxies from API (all protocols)")
        api_proxies = fetch_api_all_protocols(session=sess)
        pool.add_many(api_proxies)
        log.debug("API returned %d proxies", len(api_proxies))

    if include_text:
        log.debug("Fetching proxies from text endpoint")
        text_proxies = fetch_text_proxies(session=sess)
        pool.add_many(text_proxies)
        log.debug("Text endpoint returned %d proxies", len(text_proxies))

    if include_pages and num_pages > 0:
        log.debug("Fetching proxies from %d HTML pages", num_pages)
        page_proxies = fetch_multiple_pages(
            start_page=1,
            num_pages=num_pages,
            session=sess,
        )
        pool.add_many(page_proxies)
        log.debug("HTML pages returned %d proxies", len(page_proxies))


def fetch_all_proxies(
    num_pages: int = PROXY_DEFAULT_FETCH_PAGES,
    include_api: bool = True,
    include_text: bool = True,
    include_pages: bool = True,
) -> ProxyPool:
    """Master fetch combining all proxy sources.

    Args:
        num_pages: Number of HTML pages to crawl.
        include_api: Whether to include the JSON API source.
        include_text: Whether to include the text endpoint source.
        include_pages: Whether to include the HTML page source.

    Returns:
        ProxyPool with deduplicated proxies sorted by speed.
    """
    now = time.time()
    fetch_time_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    pool = ProxyPool(
        fetch_time=fetch_time_str,
        fetch_time_epoch=now,
    )
    from .pxyfetch import _make_session  # local import: internal helper

    sess = _make_session()
    try:
        _fetch_into_pool(
            pool,
            sess,
            num_pages=num_pages,
            include_api=include_api,
            include_text=include_text,
            include_pages=include_pages,
        )
        pool.sort_by_speed()
        log.debug("Total unique proxies in pool: %d", pool.count)
    finally:
        sess.close()
    return pool
