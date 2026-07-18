"""Opencode 代理池管理 -- 加载/持久化/刷新，供 OpencodeClient 混入。"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Optional

import aiohttp

from src.foundation.logger import get_logger
from ..consts import (
    PROXY_FETCH_ENABLED,
    PROXY_POOL_PERSIST_PATH,
    PROXY_REFRESH_INTERVAL,
)
from .pxypool import ProxyInfo, ProxyPool, fetch_all_proxies

from ...support.config_seed import load_local_proxies

LOCAL_PROXIES: list = load_local_proxies()

logger = get_logger(__name__)


class OpencodePoolMixin:
    """代理池加载、持久化与定期刷新逻辑。

    依赖宿主类提供：``self._pool``、``self._selector``、``self._proxy_lock``、
    ``self._active_requests``、``self._pool_refresh_pending``、``self._last_fetch_time``。
    """

    # 缓存有效期：超过此时间的缓存视为过期，需要刷新
    _POOL_CACHE_MAX_AGE: float = PROXY_REFRESH_INTERVAL * 0.8  # 80% 刷新间隔

    async def init_immediate(self, session: aiohttp.ClientSession) -> None:
        """存储会话对象并加载缓存的代理池 -- 非阻塞操作。

        无论 PROXY_FETCH_ENABLED 是否为 True，都加载缓存代理池和评分数据。
        始终注入本地代理。如果缓存有效且 PROXY_FETCH_ENABLED=True，跳过后续刷新。
        """
        self._session = session

        # 始终加载缓存代理池（保留学习数据）
        pool = await self._load_pool_from_disk()
        if pool is not None and pool.count > 0:
            self._inject_local_proxies(pool)
            self._pool = pool
            self._selector.update_pool(pool.to_address_list())
            self._last_fetch_time = pool.fetch_time_epoch
            logger.debug(
                "opencode client init_immediate: loaded %d cached proxies (age=%.0fs, fetch_enabled=%s)",
                pool.count,
                time.time() - self._last_fetch_time if self._last_fetch_time else 0,
                PROXY_FETCH_ENABLED,
            )
        else:
            # 无缓存时至少注入本地代理
            local_pool = ProxyPool()
            self._inject_local_proxies(local_pool)
            if local_pool.count > 0:
                self._pool = local_pool
                self._selector.update_pool(local_pool.to_address_list())
            logger.debug(
                "opencode client init_immediate: no valid cache, %d local proxies (fetch_enabled=%s)",
                local_pool.count,
                PROXY_FETCH_ENABLED,
            )

    async def background_setup(self) -> None:
        """后台获取新代理池（在线程池中执行）并启动定期刷新任务。

        仅在 PROXY_FETCH_ENABLED=True 时执行刷新。
        PROXY_FETCH_ENABLED=False 时保留已有缓存，不触发任何网络请求。
        """
        if not PROXY_FETCH_ENABLED:
            logger.debug(
                "opencode background_setup: proxy fetch disabled, "
                "keeping cached pool (%d proxies) and score data intact",
                self._pool.count,
            )
            return

        # 检查缓存是否有效
        cache_age = time.time() - self._last_fetch_time if self._last_fetch_time else float("inf")
        cache_valid = (
            self._pool.count > 0
            and self._last_fetch_time > 0
            and cache_age < self._POOL_CACHE_MAX_AGE
        )

        if cache_valid:
            logger.debug(
                "opencode background_setup: cache valid (age=%.0fs < max=%.0fs), skipping immediate refresh",
                cache_age, self._POOL_CACHE_MAX_AGE,
            )
        else:
            logger.debug(
                "opencode background_setup: cache expired or empty (age=%.0fs), fetching now",
                cache_age if cache_age != float("inf") else -1,
            )
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                await self._apply_pool(pool)
            except Exception as e:
                logger.warning("opencode background proxy fetch failed: %s", e)

        # 启动定期刷新任务
        self._refresh_task = asyncio.ensure_future(self._bg_refresh_proxy())

    @staticmethod
    def _do_proxy_fetch() -> ProxyPool:
        """同步获取代理（在线程池中调用）。"""
        return fetch_all_proxies()

    async def _apply_pool(self, pool: ProxyPool) -> None:
        """在锁保护下应用新获取的代理池，合并本地代理并持久化。

        记录获取时间戳，用于判断缓存有效期。
        """
        self._inject_local_proxies(pool)

        async with self._proxy_lock:
            if self._active_requests > 0:
                self._pool_refresh_pending = True
                logger.debug(
                    "Proxy pool refresh deferred: %d active requests, %d new proxies",
                    self._active_requests, pool.count,
                )
                return

            self._pool = pool
            self._selector.update_pool(pool.to_address_list())
            self._last_fetch_time = pool.fetch_time_epoch
            self._pool_refresh_pending = False
            await self._save_pool_to_disk(pool)
            logger.debug(
                "Proxy pool refreshed: %d proxies, fetch_time=%s (epoch=%.0f)",
                pool.count, pool.fetch_time, self._last_fetch_time,
            )

    async def _check_deferred_refresh(self) -> None:
        """检查并应用待处理的代理池刷新。"""
        if not PROXY_FETCH_ENABLED:
            return
        if self._pool_refresh_pending and self._active_requests == 0:
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                self._inject_local_proxies(pool)

                async with self._proxy_lock:
                    self._pool = pool
                    self._selector.update_pool(pool.to_address_list())
                    self._last_fetch_time = pool.fetch_time_epoch
                    self._pool_refresh_pending = False
                    await self._save_pool_to_disk(pool)
                    logger.debug(
                        "Deferred proxy pool refresh applied: %d proxies",
                        pool.count,
                    )
            except Exception as e:
                logger.warning("Failed to apply deferred proxy refresh: %s", e)

    @staticmethod
    def _inject_local_proxies(pool: ProxyPool) -> None:
        """将accounts.py中的本地代理合并到代理池中。"""
        for addr in LOCAL_PROXIES:
            addr = addr.strip()
            if not addr or ":" not in addr:
                continue
            parts = addr.rsplit(":", 1)
            try:
                ip, port = parts[0], int(parts[1])
            except (ValueError, IndexError):
                continue
            pool.add(ProxyInfo(
                ip=ip, port=port, protocol="http",
                country="local",
            ))

    async def _bg_refresh_proxy(self) -> None:
        """定期刷新代理池的后台任务。

        仅在 PROXY_FETCH_ENABLED=True 时运行。
        """
        if not PROXY_FETCH_ENABLED:
            return

        try:
            while True:
                await asyncio.sleep(PROXY_REFRESH_INTERVAL)
                try:
                    loop = asyncio.get_running_loop()
                    pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                    await self._apply_pool(pool)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("opencode periodic proxy refresh failed: %s", e)
        except asyncio.CancelledError:
            raise

    async def _load_pool_from_disk(self) -> Optional[ProxyPool]:
        """从JSON文件反序列化缓存的代理池。

        无论 PROXY_FETCH_ENABLED 如何，都尝试加载。
        返回 None 如果文件不存在、损坏或缓存为空。
        """
        path = Path(PROXY_POOL_PERSIST_PATH)
        if not path.exists():
            logger.debug("No pool cache file at %s", path)
            return None
        try:
            raw = path.read_text(encoding="utf-8")
            data = json.loads(raw)
            pool = ProxyPool.from_dict(data)
            if pool.count == 0:
                logger.debug("Loaded pool cache is empty")
                return None
            logger.debug(
                "Loaded pool cache: %d proxies, fetch_time=%s",
                pool.count, pool.fetch_time,
            )
            return pool
        except Exception as e:
            logger.warning("Failed to load proxy pool from %s: %s", path, e)
            return None

    async def _save_pool_to_disk(self, pool: ProxyPool) -> None:
        """原子性地将代理池持久化到JSON文件。

        无论 PROXY_FETCH_ENABLED 如何，都保存。
        """
        path = Path(PROXY_POOL_PERSIST_PATH)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(pool.to_dict(), indent=2), encoding="utf-8"
            )
            os.replace(str(tmp), str(path))
        except Exception as e:
            logger.warning("Failed to save proxy pool to %s: %s", path, e)
