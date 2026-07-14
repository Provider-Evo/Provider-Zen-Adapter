"""Proxy pool fetcher for opencode platform.

Fetches free proxies from proxy.scdn.io using multiple sources:
- JSON API endpoint (per-protocol)
- Paginated HTML table on main page
- Plain-text IP:port endpoint

This module is synchronous (``requests``-based) and is intended to be
called from async code via ``loop.run_in_executor``.
"""
from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    raise SystemExit(
        "The 'requests' package is required for opencode proxy pool. "
        "Install it with: pip install requests"
    )

try:
    from bs4 import BeautifulSoup  # type: ignore[import-untyped]
except ImportError:
    BeautifulSoup = None  # type: ignore[assignment,misc]

from src.foundation.logger import get_logger
from .constants import (
    PROXY_API_GET,
    PROXY_API_MAX_COUNT,
    PROXY_BASE_URL,
    PROXY_DEFAULT_FETCH_PAGES,
    PROXY_HTTP_TIMEOUT,
    PROXY_MAIN_PAGE,
    PROXY_MAX_PAGES,
    PROXY_MAX_RETRIES,
    PROXY_PER_PAGE,
    PROXY_TEXT_ENDPOINT,
)

log = get_logger("opencode.proxypool")

# Suppress noisy library loggers
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ProxyInfo:
    """Metadata for a single proxy."""

    ip: str
    port: int
    protocol: str = "http"
    country: str = ""
    response_time: float = 0.0  # ms
    response_ms: float = 0.0  # alias kept for convenience
    last_verified: str = ""
    anonymity: str = ""

    @property
    def address(self) -> str:
        return f"{self.ip}:{self.port}"

    def __hash__(self) -> int:
        return hash(self.address)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ProxyInfo):
            return NotImplemented
        return self.address == other.address


@dataclass
class ProxyPool:
    """Collection of proxies with deduplication.

    Attributes:
        proxies: List of proxy entries.
        fetch_time: ISO-8601 timestamp string of when the pool was fetched.
        fetch_time_epoch: Unix timestamp (float) for age calculations.
        total_available: Total available proxies reported by source.
    """

    proxies: List[ProxyInfo] = field(default_factory=list)
    _seen: set = field(default_factory=set)
    fetch_time: str = ""
    fetch_time_epoch: float = 0.0
    total_available: int = 0

    def __post_init__(self) -> None:
        """Set fetch_time_epoch from fetch_time if not already set."""
        if self.fetch_time_epoch == 0.0 and self.fetch_time:
            try:
                self.fetch_time_epoch = time.mktime(
                    time.strptime(self.fetch_time, "%Y-%m-%dT%H:%M:%SZ")
                )
            except (ValueError, OverflowError):
                self.fetch_time_epoch = 0.0

    def add(self, p: ProxyInfo) -> None:
        """Add a proxy, skipping duplicates by address."""
        if p.address not in self._seen:
            self._seen.add(p.address)
            self.proxies.append(p)

    def add_many(self, items: List[ProxyInfo]) -> None:
        for p in items:
            self.add(p)

    @property
    def count(self) -> int:
        return len(self.proxies)

    def sort_by_speed(self) -> None:
        """Sort proxies by response_time ascending (fastest first)."""
        self.proxies.sort(key=lambda p: p.response_time if p.response_time > 0 else float("inf"))

    def to_address_list(self) -> List[str]:
        return [p.address for p in self.proxies]

    def to_dict(self) -> dict:
        return {
            "fetch_time": self.fetch_time,
            "fetch_time_epoch": self.fetch_time_epoch,
            "total_available": self.total_available,
            "proxies": [
                {
                    "ip": p.ip,
                    "port": p.port,
                    "protocol": p.protocol,
                    "country": p.country,
                    "response_time": p.response_time,
                    "last_verified": p.last_verified,
                    "anonymity": p.anonymity,
                }
                for p in self.proxies
            ],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ProxyPool":
        pool = cls(
            fetch_time=data.get("fetch_time", ""),
            fetch_time_epoch=data.get("fetch_time_epoch", 0.0),
            total_available=data.get("total_available", 0),
        )
        for item in data.get("proxies", []):
            pool.add(ProxyInfo(
                ip=item["ip"],
                port=item["port"],
                protocol=item.get("protocol", "http"),
                country=item.get("country", ""),
                response_time=item.get("response_time", 0.0),
                last_verified=item.get("last_verified", ""),
                anonymity=item.get("anonymity", ""),
            ))
        return pool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _make_session() -> requests.Session:
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry = Retry(
        total=PROXY_MAX_RETRIES,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;"
                  "q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return session


def _parse_response_time(text: str) -> float:
    """Parse response time text to milliseconds.

    Handles formats like '4ms', '1.2s', '320 ms', '2.5 s'.
    Returns 0.0 on parse failure.
    """
    text = text.strip().lower()
    m = re.match(r"([\d.]+)\s*(ms|s)", text)
    if not m:
        return 0.0
    value = float(m.group(1))
    unit = m.group(2)
    if unit == "s":
        return value * 1000.0
    return value


def _parse_address(addr: str) -> Optional[Tuple[str, int]]:
    """Parse 'ip:port' string to (ip, port) tuple.

    Returns None on parse failure.
    """
    addr = addr.strip()
    m = re.match(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{1,5})", addr)
    if not m:
        return None
    port = int(m.group(2))
    if port < 1 or port > 65535:
        return None
    return m.group(1), port


def _parse_page_html(html: str) -> List[ProxyInfo]:
    """Parse proxy table from proxy.scdn.io main page HTML.

    Table columns: IP, Port, Protocol (span.protocol-badge), Country,
    ResponseTime, LastVerified, Status, Actions.

    Uses BeautifulSoup if available, falls back to regex.
    """
    results: List[ProxyInfo] = []

    if BeautifulSoup is not None:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            return results
        tbody = table.find("tbody") or table
        rows = tbody.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 6:
                continue
            try:
                ip = cells[0].get_text(strip=True)
                port_text = cells[1].get_text(strip=True)
                port = int(port_text)
                protocol_span = cells[2].find("span", class_="protocol-badge")
                protocol = protocol_span.get_text(strip=True) if protocol_span else cells[2].get_text(strip=True)
                country = cells[3].get_text(strip=True)
                response_text = cells[4].get_text(strip=True)
                response_time = _parse_response_time(response_text)
                last_verified = cells[5].get_text(strip=True)
                anonymity = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                results.append(ProxyInfo(
                    ip=ip,
                    port=port,
                    protocol=protocol.lower().strip(),
                    country=country,
                    response_time=response_time,
                    response_ms=response_time,
                    last_verified=last_verified,
                    anonymity=anonymity,
                ))
            except (ValueError, IndexError, AttributeError) as exc:
                log.debug("Skipping malformed table row: %s", exc)
                continue
        return results

    # Regex fallback when BeautifulSoup is unavailable
    row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
    cell_pattern = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL)
    tag_pattern = re.compile(r"<[^>]+>")

    for row_match in row_pattern.finditer(html):
        cells_raw = cell_pattern.findall(row_match.group(1))
        if len(cells_raw) < 6:
            continue
        try:
            ip = tag_pattern.sub("", cells_raw[0]).strip()
            port = int(tag_pattern.sub("", cells_raw[1]).strip())
            protocol = tag_pattern.sub("", cells_raw[2]).strip().lower()
            country = tag_pattern.sub("", cells_raw[3]).strip()
            response_text = tag_pattern.sub("", cells_raw[4]).strip()
            response_time = _parse_response_time(response_text)
            last_verified = tag_pattern.sub("", cells_raw[5]).strip()
            anonymity = tag_pattern.sub("", cells_raw[6]).strip() if len(cells_raw) > 6 else ""
            results.append(ProxyInfo(
                ip=ip,
                port=port,
                protocol=protocol,
                country=country,
                response_time=response_time,
                response_ms=response_time,
                last_verified=last_verified,
                anonymity=anonymity,
            ))
        except (ValueError, IndexError) as exc:
            log.debug("Skipping malformed regex row: %s", exc)
            continue

    return results


# ---------------------------------------------------------------------------
# Fetch functions
# ---------------------------------------------------------------------------


def fetch_api_proxies(
    protocol: str = "",
    count: int = PROXY_API_MAX_COUNT,
    session: Optional[requests.Session] = None,
) -> List[ProxyInfo]:
    """Fetch proxies from the JSON API endpoint.

    Args:
        protocol: Protocol filter (http, https, socks4, socks5). Empty for all.
        count: Maximum number of proxies to request.
        session: Optional pre-configured session.

    Returns:
        List of ProxyInfo objects.
    """
    own_session = session is None
    sess = session or _make_session()
    try:
        params = {"count": count}
        if protocol:
            params["protocol"] = protocol
        resp = sess.get(PROXY_API_GET, params=params, timeout=PROXY_HTTP_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 200:
            log.warning("API returned non-200 code: %s", data.get("code"))
            return []
        raw_proxies = data.get("data", {}).get("proxies", [])
        results: List[ProxyInfo] = []
        for addr in raw_proxies:
            parsed = _parse_address(str(addr))
            if parsed:
                ip, port = parsed
                results.append(ProxyInfo(
                    ip=ip,
                    port=port,
                    protocol=protocol or "http",
                ))
        return results
    except Exception as exc:
        log.warning("fetch_api_proxies(protocol=%r) failed: %s", protocol, exc)
        return []
    finally:
        if own_session:
            sess.close()


def fetch_api_all_protocols(
    session: Optional[requests.Session] = None,
) -> List[ProxyInfo]:
    """Fetch proxies from the API for all common protocols.

    Calls the API once per protocol with a short delay between requests.

    Returns:
        Combined list of ProxyInfo objects.
    """
    own_session = session is None
    sess = session or _make_session()
    results: List[ProxyInfo] = []
    try:
        for proto in ("http", "https", "socks4", "socks5"):
            batch = fetch_api_proxies(protocol=proto, session=sess)
            results.extend(batch)
            time.sleep(0.3)
        return results
    finally:
        if own_session:
            sess.close()


def fetch_page_proxies(
    page: int = 1,
    per_page: int = PROXY_PER_PAGE,
    session: Optional[requests.Session] = None,
) -> List[ProxyInfo]:
    """Fetch proxies from a single page of the HTML table.

    Args:
        page: Page number (1-based).
        per_page: Items per page.
        session: Optional pre-configured session.

    Returns:
        List of ProxyInfo objects parsed from the page.
    """
    own_session = session is None
    sess = session or _make_session()
    try:
        params = {"page": page, "per_page": per_page}
        resp = sess.get(PROXY_MAIN_PAGE, params=params, timeout=PROXY_HTTP_TIMEOUT)
        resp.raise_for_status()
        return _parse_page_html(resp.text)
    except Exception as exc:
        log.warning("fetch_page_proxies(page=%d) failed: %s", page, exc)
        return []
    finally:
        if own_session:
            sess.close()


def fetch_multiple_pages(
    start_page: int = 1,
    num_pages: int = PROXY_DEFAULT_FETCH_PAGES,
    per_page: int = PROXY_PER_PAGE,
    session: Optional[requests.Session] = None,
    delay: float = 0.5,
) -> List[ProxyInfo]:
    """Fetch proxies from multiple HTML pages sequentially.

    Args:
        start_page: First page number.
        num_pages: Number of pages to fetch.
        per_page: Items per page.
        session: Optional pre-configured session.
        delay: Seconds to wait between pages.

    Returns:
        Combined list of ProxyInfo objects.
    """
    own_session = session is None
    sess = session or _make_session()
    results: List[ProxyInfo] = []
    try:
        end_page = min(start_page + num_pages - 1, PROXY_MAX_PAGES)
        for page in range(start_page, end_page + 1):
            batch = fetch_page_proxies(page=page, per_page=per_page, session=sess)
            results.extend(batch)
            if page < end_page:
                time.sleep(delay)
        return results
    finally:
        if own_session:
            sess.close()


def fetch_text_proxies(
    session: Optional[requests.Session] = None,
) -> List[ProxyInfo]:
    """Fetch proxies from the plain-text endpoint.

    The endpoint returns one proxy per line in ``IP:port`` format.

    Returns:
        List of ProxyInfo objects.
    """
    own_session = session is None
    sess = session or _make_session()
    try:
        resp = sess.get(PROXY_TEXT_ENDPOINT, timeout=PROXY_HTTP_TIMEOUT)
        resp.raise_for_status()
        results: List[ProxyInfo] = []
        for line in resp.text.splitlines():
            parsed = _parse_address(line)
            if parsed:
                ip, port = parsed
                results.append(ProxyInfo(ip=ip, port=port))
        return results
    except Exception as exc:
        log.warning("fetch_text_proxies failed: %s", exc)
        return []
    finally:
        if own_session:
            sess.close()


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
    sess = _make_session()
    try:
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

        pool.sort_by_speed()
        log.debug("Total unique proxies in pool: %d", pool.count)
    finally:
        sess.close()
    return pool
