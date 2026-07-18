"""Opencode 单次 HTTP 请求处理，供 OpencodeClient 混入。"""

from __future__ import annotations

import asyncio
import time
from typing import Any, AsyncGenerator, Dict, List, Union

import aiohttp

from src.core.dispatch.cand import Candidate
from src.core.utils.errors import PlatformError
from src.foundation.logger import get_logger
from .consts import BASE_URL, CHAT_PATH
from .utils import build_headers, build_payload, parse_sse_line
from .proxy.pxyscore import DIRECT

logger = get_logger(__name__)


async def _handle_non_stream(
    resp: aiohttp.ClientResponse,
    proxy_addr: str,
    result: Dict[str, bool],
) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
    """处理非流式响应体，把成败写入 result 字典。"""
    data = await resp.json()
    choice = (data.get("choices") or [{}])[0]
    msg = choice.get("message", {})
    content = msg.get("content", "")
    content_received = False

    if content:
        content_received = True
        yield content

    tc = msg.get("tool_calls")
    if tc:
        content_received = True
        yield {"tool_calls": tc}

    usage = data.get("usage")
    if usage:
        yield {"usage": {
            "prompt_tokens": usage.get("prompt_tokens", 0),
            "completion_tokens": usage.get("completion_tokens", 0),
        }}

    if not content_received:
        logger.debug(
            "Empty response content from proxy %s, finish_reason=%s",
            proxy_addr, choice.get("finish_reason", "unknown"),
        )
        result["ok"] = False
        result["should_record_failure"] = True
    else:
        result["ok"] = True
        result["content_received"] = True
        result["should_record_failure"] = False


def _accumulate_tool_call(
    accumulator: Dict[int, Dict[str, Any]],
    tc_delta: Dict[str, Any],
) -> None:
    """把一个流式 tool_call delta 合并进累加器。"""
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


async def _iter_stream_lines(
    resp: aiohttp.ClientResponse,
    tc_accumulator: Dict[int, Dict[str, Any]],
    state: Dict[str, Any],
) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
    """逐行消费 SSE 响应，把 tool_call 累加进 tc_accumulator，把是否收到内容/finish_reason 写入 state。"""
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

        if isinstance(parsed, str):
            if parsed:
                state["content_received"] = True
                yield parsed
            continue

        if "tool_calls" in parsed:
            state["content_received"] = True
            for tc_delta in parsed["tool_calls"]:
                _accumulate_tool_call(tc_accumulator, tc_delta)
            continue

        choices = parsed.get("choices", [])
        if choices:
            delta = choices[0].get("delta", {})
            content = delta.get("content")
            if content:
                state["content_received"] = True
                yield content
            state["finish_reason"] = choices[0].get("finish_reason")


async def _handle_stream(
    resp: aiohttp.ClientResponse,
    proxy_addr: str,
    result: Dict[str, bool],
) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
    """处理流式响应体，把成败写入 result 字典。"""
    tc_accumulator: Dict[int, Dict[str, Any]] = {}
    state: Dict[str, Any] = {"content_received": False, "finish_reason": None}

    try:
        async for chunk in _iter_stream_lines(resp, tc_accumulator, state):
            yield chunk

        if state["content_received"]:
            result["ok"] = True
            result["content_received"] = True
            result["should_record_failure"] = False
        else:
            logger.debug(
                "Empty stream response from proxy %s, finish_reason=%s",
                proxy_addr, state["finish_reason"],
            )
            result["ok"] = False
            result["should_record_failure"] = True

    except GeneratorExit:
        if state["content_received"]:
            result["ok"] = True
            result["content_received"] = True
            result["should_record_failure"] = False
        raise
    except Exception as e:
        logger.warning(
            "Stream processing error with proxy %s: %s",
            proxy_addr, e,
        )
        result["ok"] = False
        result["should_record_failure"] = True
        raise RuntimeError(
            "Stream processing error with proxy {}: {}".format(proxy_addr, e)
        ) from e

    if tc_accumulator:
        tool_calls = [v for _, v in sorted(tc_accumulator.items())]
        yield {"tool_calls": tool_calls}


def _build_request_kwargs(
    proxy_addr: str,
    messages: List[Dict],
    model: str,
    stream: bool,
    **kw: Any,
) -> Dict[str, Any]:
    """构造 aiohttp 请求参数。从 _do_request 抽出以控制行数。"""
    headers = build_headers(proxy_addr)
    payload = build_payload(messages, model, stream=stream, **kw)

    request_kwargs: Dict[str, Any] = dict(
        headers=headers,
        json=payload,
        ssl=False,
        timeout=aiohttp.ClientTimeout(
            connect=10,
            total=600 if stream else 120,
        ),
    )
    if proxy_addr:
        request_kwargs["proxy"] = "http://{}".format(proxy_addr)
    return request_kwargs


async def _raise_for_error_status(
    resp: aiohttp.ClientResponse,
    proxy_addr: str,
) -> None:
    """检查响应状态码，非 200 时记录日志并抛出对应异常。"""
    if resp.status == 200:
        return

    body = await resp.text()

    if resp.status == 429:
        logger.debug(
            "Rate limited (429) on proxy %s: %s",
            proxy_addr, body[:200],
        )
        raise RuntimeError(
            "opencode rate limited (429): {}".format(body[:200])
        )

    if 500 <= resp.status < 600:
        logger.debug(
            "Server error (HTTP %d) on proxy %s: %s",
            resp.status, proxy_addr, body[:200],
        )
        raise RuntimeError(
            "opencode server error (HTTP {}): {}".format(
                resp.status, body[:200]
            )
        )

    logger.warning(
        "Client error (HTTP %d) on proxy %s: %s",
        resp.status, proxy_addr, body[:200],
    )
    raise PlatformError(
        "opencode HTTP{}: {}".format(resp.status, body[:200])
    )


async def _consume_response(
    resp: aiohttp.ClientResponse,
    proxy_addr: str,
    stream: bool,
    result: Dict[str, bool],
) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
    """校验响应状态并按流式/非流式分发消费，从 _do_request 抽出。"""
    logger.debug(
        "Response status=%d from proxy=%s",
        resp.status, proxy_addr or "direct",
    )
    await _raise_for_error_status(resp, proxy_addr)

    if not stream:
        async for chunk in _handle_non_stream(resp, proxy_addr, result):
            yield chunk
    else:
        async for chunk in _handle_stream(resp, proxy_addr, result):
            yield chunk


def _handle_request_exception(
    e: Exception,
    proxy_addr: str,
    t0: float,
    result: Dict[str, bool],
) -> Exception:
    """根据异常类型记录日志并返回待抛出的异常。从 _do_request 抽出。"""
    result["ok"] = False
    result["should_record_failure"] = True

    if isinstance(e, aiohttp.ClientError):
        logger.warning(
            "Network error with proxy %s: %s (%s)",
            proxy_addr, type(e).__name__, e,
        )
        return RuntimeError(
            "Network error with proxy {}: {}".format(proxy_addr, e)
        )

    if isinstance(e, asyncio.TimeoutError):
        logger.warning(
            "Timeout with proxy %s after %.1fs: %s",
            proxy_addr, time.time() - t0, e,
        )
        return RuntimeError(
            "Timeout with proxy {}: {}".format(proxy_addr, e)
        )

    if isinstance(e, (PlatformError, RuntimeError)):
        return e

    logger.warning(
        "Unexpected error with proxy %s: %s (%s)",
        proxy_addr, type(e).__name__, e,
    )
    return RuntimeError(
        "Unexpected error with proxy {}: {}".format(proxy_addr, e)
    )


def _record_result(
    selector: Any,
    selector_key: str,
    t0: float,
    result: Dict[str, bool],
) -> None:
    """根据请求结果记录评分。从 _do_request 的 finally 块抽出。"""
    if result["ok"] and result["content_received"]:
        latency_ms = (time.time() - t0) * 1000.0
        selector.record_success(selector_key, latency_ms)
        logger.debug(
            "Request succeeded with proxy %s in %.0fms",
            selector_key, latency_ms,
        )
    elif result["should_record_failure"]:
        selector.record_failure(selector_key)
        logger.debug(
            "Request failed with proxy %s, recorded as failure",
            selector_key,
        )


class OpencodeRequestMixin:
    """单次 HTTP 请求的发送与流式/非流式响应解析。

    依赖宿主类提供：``self._session``、``self._selector``。
    """

    async def _do_request(
        self,
        candidate: Candidate,
        messages: List[Dict],
        model: str,
        stream: bool,
        **kw: Any,
    ) -> AsyncGenerator[Union[str, Dict[str, Any]], None]:
        """执行单次HTTP请求，通过候选节点的代理或直连发送。

        评分记录统一在finally块中处理。
        PROXY_FETCH_ENABLED=False 时仍记录直连的评分（保留学习数据）。
        """
        proxy_addr = candidate.meta.get("proxy_addr", "")
        selector_key = proxy_addr if proxy_addr else DIRECT

        url = "{}{}".format(BASE_URL, CHAT_PATH)
        request_kwargs = _build_request_kwargs(proxy_addr, messages, model, stream, **kw)

        t0 = time.time()
        result: Dict[str, bool] = {
            "ok": False,
            "content_received": False,
            "should_record_failure": True,
        }

        try:
            async with self._session.post(url, **request_kwargs) as resp:
                async for chunk in _consume_response(resp, proxy_addr, stream, result):
                    yield chunk

        except (aiohttp.ClientError, asyncio.TimeoutError, PlatformError, RuntimeError, Exception) as e:
            raise _handle_request_exception(e, proxy_addr, t0, result) from e

        finally:
            # 始终记录评分（即使 PROXY_FETCH_ENABLED=False）
            # 这样切换到 True 时学习数据仍然存在
            _record_result(self._selector, selector_key, t0, result)
