


from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from provider_sdk.extensions.platform.sse_common import load_sse_json


def build_headers(api_key: str = "") -> Dict[str, str]:
    """构建请求头。

    Args:
        api_key: Zen API Key。

    Returns:
        请求头字典。
    """
    headers: Dict[str, str] = {
        "Content-Type": "application/json",
    }
    if api_key:
        headers["Authorization"] = "Bearer {}".format(api_key)
    return headers


def build_payload(
    messages: List[Dict[str, Any]],
    model: str = "",
    stream: bool = True,
    **kw: Any,
) -> Dict[str, Any]:
    """构建聊天请求体。

    Args:
        messages: 消息列表。
        model: 模型名。
        stream: 是否流式。
        **kw: 额外参数（temperature, top_p, max_tokens, stop）。

    Returns:
        请求体字典。
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
    """解析 SSE data 字段内容（OpenAI 兼容 + reasoning + tool_calls）。

    Args:
        data_str: data: 前缀之后的字符串，已去除前缀和空白。

    Returns:
        str（文本片段）、dict（thinking/tool_calls/usage）或 None（跳过）。
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

__all__: List[str] = []
