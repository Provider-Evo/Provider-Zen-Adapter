"""Opencode HTTP客户端 -- 基于代理池，无需API密钥。"""

from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import aiohttp

from src.core.dispatch.cand import Candidate, make_id
from src.core.utils.errors import PlatformError
from src.foundation.logger import get_logger
from .consts import CAPS, MODELS, MODELS_PATH, FILTER_PAID_MODELS, BASE_URL, MAX_RETRIES, PROXY_FETCH_ENABLED, PROXY_SCORE_PERSIST_PATH
from .proxy.poolshr import OpencodePoolMixin
from .reqshr import OpencodeRequestMixin
from .proxy.pxypool import pxypool
from .proxy.pxyscore import DIRECT, ProxyPoolSelector

logger = get_logger(__name__)


class OpencodeClient(OpencodePoolMixin, OpencodeRequestMixin):
    """Opencode HTTP客户端。

    不使用API密钥，而是将代理池中的每个代理视为候选节点。
    通过ProxyPoolSelector（基于汤普森采样算法）为每个请求选择最优代理，
    代理池定期刷新，启动时优先使用有效缓存。

    PROXY_FETCH_ENABLED=False 时：
    - 保留代理池缓存和评分数据（不丢失学习结果）
    - 请求时仅使用直连，不通过代理
    - 后台不刷新代理池
    """

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._pool: ProxyPool = ProxyPool()
        self._selector: ProxyPoolSelector = ProxyPoolSelector(PROXY_SCORE_PERSIST_PATH)
        self._models: List[str] = list(MODELS)
        self._refresh_task: Optional[asyncio.Task] = None
        self._proxy_lock: asyncio.Lock = asyncio.Lock()
        self._active_requests: int = 0
        self._pool_refresh_pending: bool = False
        self._last_fetch_time: float = 0.0  # 上次成功获取代理池的时间戳

    # ------------------------------------------------------------------
    # 候选节点管理
    # ------------------------------------------------------------------

    async def candidates(self) -> List[Candidate]:
        """返回单个候选节点 -- 代理选择在内部处理。"""
        if self._pool.count == 0 and PROXY_FETCH_ENABLED:
            # 只有启用代理且池为空时才需要代理
            return []
        return [
            Candidate(
                id=make_id("opencode"),
                platform="opencode",
                resource_id="proxy-pool",
                models=list(self._models),
                context_length=None,
                meta={"proxy_addr": "", "proxy_protocol": ""},
                **CAPS,
            )
        ]

    async def ensure_candidates(self, count: int) -> int:
        """确保候选节点可用（代理池非空）。

        即使 PROXY_FETCH_ENABLED=False，只要有本地代理或缓存就返回可用。
        PROXY_FETCH_ENABLED=True 且池为空时尝试获取。
        """
        if self._pool.count == 0 and PROXY_FETCH_ENABLED:
            try:
                loop = asyncio.get_running_loop()
                pool = await loop.run_in_executor(None, self._do_proxy_fetch)
                await self._apply_pool(pool)
            except Exception as e:
                logger.warning("opencode ensure_candidates fetch failed: %s", e)
        return 1 if self._pool.count > 0 else 0

    # ------------------------------------------------------------------
    # 聊天补全
    # ------------------------------------------------------------------

    async def complete(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        *,
        thinking: bool = False,
        search: bool = False,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行聊天补全请求，最多重试MAX_RETRIES次。

        PROXY_FETCH_ENABLED=True 时通过汤普森采样选择最优代理。
        PROXY_FETCH_ENABLED=False 时始终直连，但仍记录评分（保留学习数据）。
        """
        failed_proxies: set = set()
        self._active_requests += 1

        try:
            async for chunk in self._complete_retry_loop(
                candidate, messages, model, stream, kw, failed_proxies
            ):
                yield chunk
        finally:
            self._active_requests -= 1
            if self._active_requests == 0 and self._pool_refresh_pending:
                asyncio.ensure_future(self._check_deferred_refresh())

    @staticmethod
    def _log_platform_error_attempt(attempt: int) -> None:
        logger.warning(
            "opencode attempt %d/%d: platform error, not retrying",
            attempt, MAX_RETRIES,
        )

    @staticmethod
    def _log_generator_exit_attempt(attempt: int) -> None:
        logger.debug(
            "opencode generator closed externally during attempt %d/%d",
            attempt, MAX_RETRIES,
        )

    @staticmethod
    def _log_retry_exception(attempt: int, new_addr: Optional[str], exc: Exception) -> None:
        logger.warning(
            "opencode retry %d/%d (proxy=%s failed): %s",
            attempt, MAX_RETRIES, new_addr or "direct", exc,
        )

    @staticmethod
    async def _backoff_before_next_attempt(attempt: int) -> None:
        if attempt < MAX_RETRIES:
            await asyncio.sleep(min(2 ** (attempt - 1), 10))

    async def _run_one_attempt(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        kw: Dict[str, Any],
        attempt: int,
        new_addr: str,
        failed_proxies: set,
        result: Dict[str, Any],
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行单次尝试并透传响应块，将结果状态写入 ``result``。"""
        try:
            has_content = False
            async for chunk in self._run_attempt(
                candidate, messages, model, stream, attempt, new_addr, kw
            ):
                has_content = True
                yield chunk
            result["has_content"] = has_content

        except PlatformError:
            self._log_platform_error_attempt(attempt)
            raise

        except GeneratorExit:
            self._log_generator_exit_attempt(attempt)
            raise

        except Exception as e:
            result["last_exc"] = e
            if new_addr:
                failed_proxies.add(new_addr)
            self._log_retry_exception(attempt, new_addr, e)

    async def _complete_retry_loop(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        kw: Dict[str, Any],
        failed_proxies: set,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """驱动重试循环：每次尝试选代理、执行请求，失败则退避重试。"""
        last_exc: Optional[Exception] = None
        attempt = 0
        while attempt < MAX_RETRIES:
            attempt += 1
            new_addr = await self._select_proxy(failed_proxies, attempt)

            candidate.meta["proxy_addr"] = new_addr
            candidate.meta["proxy_protocol"] = "proxy" if new_addr else "direct"

            result: Dict[str, Any] = {"has_content": False, "last_exc": None}
            async for chunk in self._run_one_attempt(
                candidate, messages, model, stream, kw, attempt, new_addr, failed_proxies, result
            ):
                yield chunk

            if result["has_content"]:
                return

            if result["last_exc"] is None and new_addr:
                failed_proxies.add(new_addr)
            if result["last_exc"] is not None:
                last_exc = result["last_exc"]
            await self._backoff_before_next_attempt(attempt)

        self._raise_all_attempts_failed(last_exc, failed_proxies)

    @staticmethod
    def _raise_all_attempts_failed(
        last_exc: Optional[Exception], failed_proxies: set
    ) -> None:
        """在所有重试用尽后记录日志并抛出对应的PlatformError。"""
        logger.error(
            "All %d attempts failed (tried %d unique proxies), last error: %s",
            MAX_RETRIES, len(failed_proxies), last_exc,
        )
        if last_exc:
            raise PlatformError(
                "All {} attempts failed with proxy pool".format(MAX_RETRIES)
            ) from last_exc
        raise PlatformError(
            "All {} attempts returned empty responses".format(MAX_RETRIES)
        )

    async def _select_proxy(self, failed_proxies: set, attempt: int) -> str:
        """按汤普森采样为本次尝试选择一个代理地址（空字符串代表直连）。"""
        if not (PROXY_FETCH_ENABLED and self._pool.count > 0):
            return ""

        async with self._proxy_lock:
            pool_addrs = self._pool.to_address_list()

        available_proxies = [a for a in pool_addrs if a not in failed_proxies]
        if not available_proxies:
            logger.warning(
                "No available proxies left for attempt %d/%d (%d failed proxies)",
                attempt, MAX_RETRIES, len(failed_proxies),
            )
            raise PlatformError(
                "No available proxies in pool (tried {} proxies)".format(len(failed_proxies))
            )

        chosen = self._selector.select(available_proxies)
        if chosen == DIRECT or chosen is None:
            return ""
        return chosen

    async def _run_attempt(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        attempt: int,
        new_addr: str,
        kw: Dict[str, Any],
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行一次请求尝试并透传响应块；调用方通过是否收到任何 chunk 判断是否为空响应。"""
        logger.debug(
            "Attempt %d/%d with proxy=%s (fetch_enabled=%s)",
            attempt, MAX_RETRIES, new_addr or "direct", PROXY_FETCH_ENABLED,
        )
        has_content = False
        async for chunk in self._do_request(candidate, messages, model, stream, **kw):
            has_content = True
            yield chunk

        if not has_content:
            logger.debug(
                "opencode retry %d/%d (proxy=%s): empty response",
                attempt, MAX_RETRIES, new_addr or "direct",
            )
            return

        logger.debug(
            "Request succeeded on attempt %d/%d with proxy=%s",
            attempt, MAX_RETRIES, new_addr or "direct",
        )

    # ------------------------------------------------------------------
    # 远程模型获取
    # ------------------------------------------------------------------

    async def fetch_remote_models(self) -> List[str]:
        """从远程API获取可用模型列表（直连方式）。"""
        if not self._session:
            return []

        url = "{}{}".format(BASE_URL, MODELS_PATH)

        try:
            async with self._session.get(
                url,
                ssl=False,
                timeout=aiohttp.ClientTimeout(connect=10, total=30),
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        "opencode fetch models failed, HTTP%s", resp.status,
                    )
                    return []
                data = await resp.json()
                model_data = data.get("data", [])
                if isinstance(model_data, list):
                    all_models = [
                        m.get("id", "")
                        for m in model_data
                        if isinstance(m, dict) and m.get("id")
                    ]
                    if FILTER_PAID_MODELS:
                        return [m for m in all_models if m.endswith("-free")]
                    return all_models
                return []
        except Exception as e:
            logger.debug("opencode fetch models exception: %s", e)
            return []

    # ------------------------------------------------------------------
    # 模型列表管理
    # ------------------------------------------------------------------

    def get_available_models(self) -> List[str]:
        """返回当前可用模型列表。"""
        return list(self._models)

    def update_models(self, models: List[str]) -> None:
        """替换当前模型列表。"""
        self._models = list(models)

    # ------------------------------------------------------------------
    # 生命周期管理
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """取消后台任务，清理资源。"""
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                logger.debug("opencode proxy refresh task cancelled")
