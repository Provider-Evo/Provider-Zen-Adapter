
from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import aiohttp

from src.core.dispatch.cand import Candidate
from src.core.utils.compat.models_cache import ModelsCache
from src.foundation.logger import get_logger
from provider_sdk.extensions.platform.adapter import PlatformAdapter
from .support.consts import (
    CAPS,
    FETCH_MODELS_ENABLED,
    MODEL_FETCH_INTERVAL,
    MODELS,
)

logger = get_logger(__name__)

from .support.config_seed import load_use_proxy_pool


class ZenAdapter(PlatformAdapter):
    """Zen 平台：USE_PROXY_POOL=True 走 opencode 代理池，False 走 API Key。"""

    def __init__(self) -> None:
        self._client: Any = None
        self._models: List[str] = list(MODELS)
        self._cache: Optional[ModelsCache] = None
        self._refresh_task: Optional[asyncio.Task] = None
        self._use_proxy_pool = load_use_proxy_pool()

    @property
    def name(self) -> str:
        return "zen"

    @property
    def supported_models(self) -> List[str]:
        return list(self._models)

    @property
    def default_capabilities(self) -> Dict[str, bool]:
        return CAPS

    async def init(self, session: aiohttp.ClientSession) -> None:
        if self._use_proxy_pool:
            from .opencode.client import OpencodeClient

            self._client = OpencodeClient()
        else:
            from .client import ZenClient

            self._client = ZenClient()

        await self._client.init_immediate(session)
        self._cache = ModelsCache(
            platform="zen",
            fallback_models=MODELS,
            fetch_enabled=FETCH_MODELS_ENABLED,
        )
        cached = await self._cache.load()
        if cached:
            self._models = cached
            self._client.update_models(self._models)
        self._refresh_task = asyncio.ensure_future(self._background_init())

    async def _background_init(self) -> None:
        try:
            await self._client.background_setup()
        except Exception as exc:
            logger.warning("zen 后台初始化失败: %s", exc)
        if self._cache is not None:
            asyncio.ensure_future(
                self._cache.start_refresh_loop(
                    fetch_fn=self.fetch_remote_models,
                    interval=MODEL_FETCH_INTERVAL,
                    on_update=self._on_models_updated,
                )
            )

    async def _on_models_updated(self, models: List[str]) -> None:
        self._models = models
        if self._client is not None:
            self._client.update_models(models)
        logger.debug("zen 模型列表已更新: %d 个", len(models))

    async def fetch_remote_models(self) -> List[str]:
        if self._client is None:
            return list(MODELS)
        return await self._client.fetch_remote_models()

    async def candidates(self) -> List[Candidate]:
        if self._client is None:
            return []
        return await self._client.candidates()

    async def ensure_candidates(self, count: int) -> int:
        if self._client is None:
            return 0
        return await self._client.ensure_candidates(count)

    async def complete(
        self,
        candidate: Candidate,
        messages: List[Dict[str, Any]],
        model: str,
        stream: bool,
        *,
        thinking: bool = False,
        search: bool = False,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        async for chunk in self._client.complete(
            candidate,
            messages,
            model,
            stream,
            thinking=thinking,
            search=search,
            **kw,
        ):
            yield chunk

    async def close(self) -> None:
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                logger.debug("zen 刷新任务已取消")
        if self._client is not None:
            await self._client.close()


Adapter = ZenAdapter

# =======================================================================
# 重导出 — 同包内协同模块的公共符号（保持外部 ``from .. import`` 路径稳定）
# =======================================================================

__all__ = [
]
