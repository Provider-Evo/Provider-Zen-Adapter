


from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from provider_sdk.extensions.platform.sse_common import load_sse_json


def build_headers(proxy_addr: str = "") -> Dict[str, str]:
    """Build request headers.

    Args:
        proxy_addr: Proxy address (informational only, not used in headers).

    Returns:
        Header dictionary.
    """
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
    }
    return headers


def build_payload(
    messages: List[Dict[str, Any]],
    model: str = "",
    stream: bool = True,
    **kw: Any,
) -> Dict[str, Any]:
    """Build chat completion request body.

    Args:
        messages: Message list.
        model: Model name.
        stream: Whether to stream the response.
        **kw: Extra parameters (temperature, top_p, max_tokens, stop, tools, tool_choice).

    Returns:
        Request body dictionary.
    """
    payload: Dict[str, Any] = {
        "model": model,
        "messages": messages,
        "stream": stream,
    }
    if kw.get("temperature") is not None:
        payload["temperature"] = kw["temperature"]
    if kw.get("top_p") is not None:
        payload["top_p"] = kw["top_p"]
    if kw.get("max_tokens") is not None:
        payload["max_tokens"] = kw["max_tokens"]
    if kw.get("stop"):
        payload["stop"] = kw["stop"]
    if kw.get("tools"):
        payload["tools"] = kw["tools"]
    if kw.get("tool_choice"):
        payload["tool_choice"] = kw["tool_choice"]
    return payload


def parse_sse_line(data_str: str) -> Optional[Union[str, Dict[str, Any]]]:
    """Parse SSE data field content (OpenAI-compatible + reasoning + tool_calls).

    Args:
        data_str: String after the ``data:`` prefix, with leading
            whitespace already stripped.

    Returns:
        str (text chunk), dict (thinking/tool_calls/usage), or None (skip).
    """
    obj = load_sse_json(data_str)
    if obj is None:
        return None

    choice = (obj.get("choices") or [{}])[0]
    delta = choice.get("delta", {})

    reasoning = delta.get("reasoning") or delta.get("reasoning_content")
    if reasoning:
        return {"thinking": reasoning}

    content = delta.get("content", "")
    if content:
        return content

    tc = delta.get("tool_calls")
    if tc:
        return {"tool_calls": tc}

    usage = obj.get("usage")
    if usage and isinstance(usage, dict):
        return {"usage": usage}

    return None

# =======================================================================
# 重导出 — 同包内协同模块的公共符号（保持外部 ``from .. import`` 路径稳定）
# =======================================================================

__all__ = [
]
