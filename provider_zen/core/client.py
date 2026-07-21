from __future__ import annotations

from pathlib import Path

from src.foundation.config.reader import load_plugin_api_keys

_PLUGIN_DIR = Path(__file__).resolve().parents[2]
import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Optional, Union

import aiohttp

from src.core.dispatch.cand import Candidate, make_id
from src.core.utils.errors import PlatformError
from src.foundation.logger import get_logger
from ..accounts import API_KEYS
from .support.consts import (
    BASE_URL,
    CAPS,
    CHAT_PATH,
    FILTER_PAID_MODELS,
    MODELS_PATH,
    RATE_LIMIT_COOLDOWN,
    RECOVERY_INTERVAL,
)
from .support.utils import build_headers, build_payload, parse_sse_line

logger = get_logger(__name__)
MAX_RETRIES: int = 3


class _KeyState:
    """单个API Key的运行时状态。"""

    __slots__ = (
        "key", "valid", "busy", "error_count",
        "consecutive_failures", "last_error_time",
        "rate_limit_until",
    )

    def __init__(self, key: str) -> None:
        """初始化Key状态。"""
        self.key: str = key
        self.valid: bool = True
        self.busy: bool = False
        self.error_count: int = 0
        self.consecutive_failures: int = 0
        self.last_error_time: float = 0.0
        self.rate_limit_until: float = 0.0

    @property
    def available(self) -> bool:
        """判断是否可用。"""
        if not self.valid:
            if time.time() - self.last_error_time >= RECOVERY_INTERVAL:
                self.valid = True
                self.error_count = 0
                self.consecutive_failures = 0
            else:
                return False
        if self.busy:
            return False
        if self.rate_limit_until > time.time():
            return False
        if self.consecutive_failures >= 3:
            if time.time() - self.last_error_time < RATE_LIMIT_COOLDOWN:
                return False
            self.consecutive_failures = 0
        return True

    def mark_success(self) -> None:
        """标记请求成功。"""
        self.busy = False
        self.consecutive_failures = 0

    def mark_failure(self, status: int = 0) -> None:
        """根据HTTP状态码分类处理失败。"""
        self.busy = False
        self.last_error_time = time.time()
        if status in (401, 403):
            self.valid = False
            logger.warning(
                "zen Key无效 (HTTP%s): %s...", status, self.key[:20]
            )
        elif status == 429:
            self.rate_limit_until = time.time() + RATE_LIMIT_COOLDOWN
            logger.warning("zen Key限速: %s...", self.key[:20])
        elif status in (500, 502, 503, 504):
            self.consecutive_failures += 1
            self.error_count += 1
        elif status in (400, 404, 422):
            logger.debug("zen 请求参数或模型错误，保留 key 可用状态: %s", status)
        else:
            self.consecutive_failures += 1
            self.error_count += 1


class ZenClient:
    """Zen HTTP客户端。"""

    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._models: List[str] = []
        self._keys: List[_KeyState] = []

    async def init_immediate(self, session: aiohttp.ClientSession) -> None:
        """立即初始化，不阻塞。"""
        self._session = session
        self._keys = [_KeyState(k) for k in load_plugin_api_keys(_PLUGIN_DIR, API_KEYS)]
        logger.debug(
            "zen客户端初始化完成, %s个APIKey", len(self._keys)
        )

    async def background_setup(self) -> None:
        """后台完善：拉取远程模型列表。"""
        try:
            models = await self.fetch_remote_models()
            if models:
                self.update_models(models)
        except Exception as e:
            logger.warning("zen后台拉取模型失败: %s", e)

    async def fetch_remote_models(self) -> List[str]:
        """从Zen API拉取可用模型列表。

        Returns:
            模型ID列表，失败时返回空列表。
        """
        if not self._session or not self._keys:
            return []

        ks = next((k for k in self._keys if k.available), None)
        if ks is None:
            return []

        headers = build_headers(ks.key)
        url = "{}{}".format(BASE_URL, MODELS_PATH)

        try:
            async with self._session.get(
                url,
                headers=headers,
                ssl=False,
                timeout=aiohttp.ClientTimeout(connect=10, total=30),
            ) as resp:
                if resp.status != 200:
                    logger.warning(
                        "zen拉取模型列表失败, HTTP%s", resp.status
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
                    # If FILTER_PAID_MODELS is True, only keep free models
                    if FILTER_PAID_MODELS:
                        return [
                            m for m in all_models if m.endswith("-free")
                        ]
                    return all_models
                return []
        except Exception as e:
            logger.warning("zen拉取模型列表异常: %s", e)
            return []

    def update_models(self, models: List[str]) -> None:
        """更新模型列表。

        Args:
            models: 新的模型列表。
        """
        self._models = list(models)

    def _find_key(self, candidate: Candidate) -> Optional[_KeyState]:
        """根据候选项找到对应的KeyState。"""
        api_key = candidate.meta.get("api_key", "")
        for ks in self._keys:
            if ks.key == api_key:
                return ks
        return None

    async def candidates(self) -> List[Candidate]:
        """每个可用Key生成一个候选项。"""
        models = self._models
        return [
            Candidate(
                id=make_id("zen", ks.key[:20]),
                platform="zen",
                resource_id=ks.key[:20],
                models=list(models),
                context_length=None,
                meta={"api_key": ks.key},
                **CAPS,
            )
            for ks in self._keys
            if ks.available
        ]

    async def ensure_candidates(self, count: int) -> int:
        """返回可用Key数量。"""
        return sum(1 for ks in self._keys if ks.available)

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
        """执行聊天补全，含重试。"""
        last_exc: Optional[Exception] = None
        for attempt in range(MAX_RETRIES + 1):
            if attempt > 0:
                await asyncio.sleep(1.0 * (2 ** (attempt - 1)))
            try:
                async for chunk in self._do_request(
                    candidate, messages, model, stream, **kw
                ):
                    yield chunk
                return
            except PlatformError:
                raise
            except Exception as e:
                last_exc = e
                logger.warning(
                    "zen重试 %s/%s: %s", attempt + 1, MAX_RETRIES, e
                )
        if last_exc:
            raise last_exc

    async def _do_request(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行单次HTTP请求。"""
        ks = self._find_key(candidate)
        if not ks:
            raise PlatformError("zen: 未找到对应APIKey")

        headers = build_headers(ks.key)
        payload = build_payload(messages, model, stream=stream, **kw)
        url = "{}{}".format(BASE_URL, CHAT_PATH)

        ks.busy = True
        try:
            async with self._session.post(
                url,
                headers=headers,
                json=payload,
                ssl=False,
                timeout=aiohttp.ClientTimeout(
                    connect=10,
                    total=600 if stream else 120,
                ),
            ) as resp:
                async for chunk in self._dispatch_response(resp, ks, stream):
                    yield chunk
        except PlatformError:
            raise
        except Exception as e:
            ks.mark_failure(0)
            raise PlatformError(
                "zen请求失败: {}".format(e)
            ) from e

    async def _dispatch_response(
        self, resp: aiohttp.ClientResponse, ks: "_KeyState", stream: bool
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """校验HTTP状态码后，将响应分发给非流式/流式处理器。"""
        if resp.status != 200:
            body = await resp.text()
            ks.mark_failure(resp.status)
            raise PlatformError(
                "zen HTTP{}: {}".format(resp.status, body[:200])
            )

        if not stream:
            async for chunk in self._handle_non_stream_response(resp, ks):
                yield chunk
        else:
            async for chunk in self._handle_stream_response(resp, ks):
                yield chunk

    async def _handle_non_stream_response(
        self, resp: aiohttp.ClientResponse, ks: "_KeyState"
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """解析非流式响应并产出内容/工具调用/用量。"""
        data = await resp.json()
        ks.mark_success()
        choice = (data.get("choices") or [{}])[0]
        msg = choice.get("message", {})
        content = msg.get("content", "")
        if content:
            yield content
        tc = msg.get("tool_calls")
        if tc:
            yield {"tool_calls": tc}
        usage = data.get("usage")
        if usage:
            yield {"usage": {
                "prompt_tokens": usage.get("prompt_tokens", 0),
                "completion_tokens": usage.get("completion_tokens", 0),
            }}

    async def _handle_stream_response(
        self, resp: aiohttp.ClientResponse, ks: "_KeyState"
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """解析SSE流式响应，累积工具调用增量并产出内容。"""
        _tc_accumulator: Dict[int, Dict[str, Any]] = {}
        async for line in resp.content:
            text = line.decode("utf-8", errors="replace").strip()
            if not text or not text.startswith("data:"):
                continue
            data_str = text[5:].strip()
            if data_str == "[DONE]":
                break
            parsed = parse_sse_line(data_str)
            if parsed is None:
                continue
            if isinstance(parsed, dict) and "tool_calls" in parsed:
                # Accumulate streaming tool_calls deltas
                self._accumulate_tool_call_deltas(_tc_accumulator, parsed["tool_calls"])
            else:
                yield parsed
        if _tc_accumulator:
            tool_calls = [
                v for _, v in sorted(_tc_accumulator.items())
            ]
            yield {"tool_calls": tool_calls}
        ks.mark_success()

    @staticmethod
    def _accumulate_tool_call_deltas(
        accumulator: Dict[int, Dict[str, Any]], tc_deltas: List[Dict[str, Any]]
    ) -> None:
        """将一批流式工具调用增量合并进累积器。"""
        for tc_delta in tc_deltas:
            idx = tc_delta.get("index", 0)
            if idx not in accumulator:
                accumulator[idx] = {
                    "id": "",
                    "type": "function",
                    "function": {"name": "", "arguments": ""},
                }
            acc = accumulator[idx]
            if tc_delta.get("id"):
                acc["id"] = tc_delta["id"]
            if tc_delta.get("type"):
                acc["type"] = tc_delta["type"]
            fn = tc_delta.get("function") or {}
            if fn.get("name"):
                acc["function"]["name"] += fn["name"]
            if fn.get("arguments"):
                acc["function"]["arguments"] += fn["arguments"]

    async def close(self) -> None:
        """清理资源。"""
        return
