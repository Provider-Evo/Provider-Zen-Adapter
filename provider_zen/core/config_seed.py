"""从插件 config.toml 读取非凭证配置（use_proxy_pool、local_proxies）。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from src.foundation.config.reader import get_config_reader

_PLUGIN_DIR = Path(__file__).resolve().parents[2]


def _load_config() -> Dict[str, Any]:
    reader = get_config_reader()
    config, _schema, _raw = reader.get_plugin_config(_PLUGIN_DIR)
    return config


def load_use_proxy_pool(default: bool = True) -> bool:
    """读取 use_proxy_pool 配置：True 走 opencode 代理池，False 走 API Key。"""
    config = _load_config()
    return bool(config.get("use_proxy_pool", default))


def load_local_proxies() -> List[str]:
    """读取本地代理列表（ip:port），注入 opencode 代理池。"""
    config = _load_config()
    raw = config.get("local_proxies", [])
    if isinstance(raw, list):
        return [str(p) for p in raw if isinstance(p, str) and p.strip()]
    return []
