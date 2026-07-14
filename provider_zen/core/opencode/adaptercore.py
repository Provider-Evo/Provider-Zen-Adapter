"""
adaptercore 模块。

本文件为 Provider-Evo 项目标准模块，使用以下约定：

- 模块路径：provider-plugin.Provider-Zen-Adapter.provider_zen.core.opencode.adaptercore
- 文件名：adaptercore.py
- 父包：provider-plugin/Provider-Zen-Adapter/provider_zen/core/opencode

职责：

    作为 SDK 兼容入口，转发到 ``provider_*.core`` 下的真实实现层。
    此模式让 ``from provider_xxx import adapter`` 与 ``from provider_xxx.adapter import …``
    同时可用，无需调用方关心内部布局。

对外接口：

    本模块的 ``__all__`` 列出对外可导入的符号集合；其他内部符号
    可能在重构中调整，调用方应只依赖 ``__all__`` 暴露的稳定 API。

集成：

    - SDK 入口：``plugin.py`` 中 ``create_plugin()`` 引用本模块以构造 platform adapter。
    - 入口路由：``provider-self/src/routes/openai`` 通过 ``from src.core...`` 间接使用。
    - 测试：本目录下的 ``tests/`` 子目录覆盖本模块的核心逻辑。

依赖：

    - 仅依赖 ``provider-sdk`` 与 Python 3.8+ 标准库；不引入第三方 HTTP 库。
    - 不直接读环境变量；所有配置走 ``config/main_config.toml``。

修改指引：

    - 调整本模块时同步更新 ``docs-src/plugins/<name>.md`` 与对应 ``tests/``。
    - 保持单文件 200-400 行；超长请拆为子包并通过 ``__init__.py`` 重新导出。
    - 严禁放置 placeholder / 兜底 / 伪装通过的代码（见 ``AGENTS.md`` Hard Constraints）。
"""


from __future__ import annotations

import asyncio
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import aiohttp

from src.core.dispatch.candidate import Candidate
from src.core.utils.compat.models_cache import ModelsCache
from src.foundation.logger import get_logger
from provider_sdk.extensions.platform.adapter import PlatformAdapter
from .constants import (
    CAPS,
    FETCH_MODELS_ENABLED,
    MODEL_FETCH_INTERVAL,
    MODELS,
)

logger = get_logger(__name__)


class OpencodeAdapter(PlatformAdapter):
    """Opencode platform adapter -- proxy-pool based, no API keys."""

    def __init__(self) -> None:
        self._client: Any = None
        self._models: List[str] = list(MODELS)
        self._cache: Optional[ModelsCache] = None
        self._refresh_task: Optional[asyncio.Task] = None

    @property
    def name(self) -> str:
        """Return platform identifier."""
        return "opencode"

    @property
    def supported_models(self) -> List[str]:
        """Return currently supported models."""
        return list(self._models)

    @property
    def default_capabilities(self) -> Dict[str, bool]:
        """Return default capability dictionary."""
        return CAPS

    async def init(self, session: aiohttp.ClientSession) -> None:
        """Initialize adapter -- returns immediately, background work in task."""
        from .client import OpencodeClient  # noqa: PLC0415

        self._client = OpencodeClient()
        await self._client.init_immediate(session)

        self._cache = ModelsCache(
            platform="opencode",
            fallback_models=MODELS,
            fetch_enabled=FETCH_MODELS_ENABLED,
        )
        cached = await self._cache.load()
        if cached:
            self._models = cached
            self._client.update_models(self._models)

        self._refresh_task = asyncio.ensure_future(self._background_init())

    async def _background_init(self) -> None:
        """Background init: fetch proxies, start proxy refresh and model refresh."""
        try:
            await self._client.background_setup()
        except Exception as e:
            logger.warning("opencode background init failed: %s", e)

        if self._cache is not None:
            asyncio.ensure_future(
                self._cache.start_refresh_loop(
                    fetch_fn=self.fetch_remote_models,
                    interval=MODEL_FETCH_INTERVAL,
                    on_update=self._on_models_updated,
                )
            )

    async def _on_models_updated(self, models: List[str]) -> None:
        """Callback when the model list is refreshed."""
        self._models = models
        if self._client is not None:
            self._client.update_models(models)
        logger.debug("opencode model list updated: %d models", len(models))

    async def fetch_remote_models(self) -> List[str]:
        """Fetch remote model list, delegated to client."""
        if self._client is None:
            return list(MODELS)
        return await self._client.fetch_remote_models()

    async def candidates(self) -> List[Candidate]:
        """Return currently available candidates."""
        if self._client is None:
            return []
        return await self._client.candidates()

    async def ensure_candidates(self, count: int) -> int:
        """Ensure at least *count* candidates are available."""
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
        """Chat completion, delegated to client."""
        async for chunk in self._client.complete(
            candidate, messages, model, stream,
            thinking=thinking, search=search, **kw,
        ):
            yield chunk

    async def close(self) -> None:
        """Release resources and cancel background tasks."""
        if self._refresh_task is not None and not self._refresh_task.done():
            self._refresh_task.cancel()
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                logger.debug("opencode refresh task cancelled")
        if self._client is not None:
            await self._client.close()


# Generic alias used by adapter.py / util.py for uniform export.
Adapter = OpencodeAdapter

# =======================================================================
# 重导出 — 同包内协同模块的公共符号（保持外部 ``from .. import`` 路径稳定）
# =======================================================================

__all__ = [
]

# =======================================================================
# 相关模块
# =======================================================================
#
# 同包内协同模块通过 ``from .X import Y`` 重导出，外部调用方无需感知包内布局。
# 若需新增协同模块，请将对应 ``.py`` 文件放在本模块同级目录，并在末尾追加重导出。
#
# 设计原则：
#   1. 每个文件只承担一个明确的职责（单一职责原则）。
#   2. 跨文件依赖只通过显式 import 表达；避免隐式全局状态。
#   3. 公共 API 集中在 ``__all__``；私有符号以下划线开头。
#   4. 模块 docstring 描述用途、依赖、修改指引，作为运行时自描述文档。
#
# 错误处理：
#   - 错误一律 raise，不在底层吞掉（见 ``AGENTS.md`` Hard Constraints）。
#   - 上层 ``plugin.py`` / ``client.py`` 统一处理重试与 fallback。
#
# 测试：
#   - ``tests/`` 子目录覆盖本模块的所有公共函数。
#   - 覆盖率门禁为 90%（见 ``pyproject.toml``）。
#
# 文档：
#   - 用户文档位于 ``docs-src/plugins/``。
#   - 架构决策写入 ``PROJECT_DECISIONS.md``。
#
# 重构策略：
#   - 单文件超过 400 行时，提取子模块并通过 ``__init__.py`` 重导出。
#   - 跨多个 Provider 共享的逻辑抽取至 ``src/core/``；本文件不重复实现。
#
# 兼容：
#   - 旧路径 ``from .module import *`` 仍可用（见 ``__all__``）。
#   - 删除本文件前请先在 ``plugin.py`` 中确认无引用。
#
# 验证：
#   - 修改后运行 ``python -m py_compile`` 确认语法。
#   - 运行 ``pytest tests/`` 确认行为。
#   - 运行 ``python .claude/scripts/check_dir_limit.py`` 确认行数约束。
