

from typing import Dict, List

PLATFORM_NAME: str = "opencode"

BASE_URL: str = "https://opencode.ai/zen/v1"
CHAT_PATH: str = "/chat/completions"
MODELS_PATH: str = "/models"

MAX_RETRIES: int = 3
RATE_LIMIT_COOLDOWN: int = 30
RECOVERY_INTERVAL: int = 60

MODELS: List[str] = [
    "mimo-v2.5-free",
    "deepseek-v4-flash-free",
    "qwen3.6-plus-free",
    "minimax-m3-free",
    "nemotron-3-ultra-free",
    "north-mini-code-free",
]

CAPS: Dict[str, bool] = {
    "chat": True,
    "completions": True,
    "responses": True,
    "vision": True,
    "tools": True,
    "native_tools": True,
    "thinking": True,
}

FETCH_MODELS_ENABLED: bool = True
MODEL_FETCH_INTERVAL: int = 86400

FILTER_PAID_MODELS: bool = True

# ---------------------------------------------------------------------------
# Proxy pool configuration
# ---------------------------------------------------------------------------

PROXY_BASE_URL: str = "https://proxy.scdn.io"
PROXY_API_GET: str = PROXY_BASE_URL + "/api/get_proxy.php"
PROXY_TEXT_ENDPOINT: str = PROXY_BASE_URL + "/text.php"
PROXY_MAIN_PAGE: str = PROXY_BASE_URL + "/"

PROXY_FETCH_ENABLED: bool = False  # 是否启用代理列表定时获取

PROXY_MAX_PAGES: int = 10000
PROXY_PER_PAGE: int = 10
PROXY_DEFAULT_FETCH_PAGES: int = 5
PROXY_HTTP_TIMEOUT: int = 15
PROXY_MAX_RETRIES: int = 3
PROXY_API_MAX_COUNT: int = 20
PROXY_REFRESH_INTERVAL: int = 86400  # 24 hours

PROXY_SCORE_PERSIST_PATH: str = "persist/opencode/proxy_score.json"
PROXY_POOL_PERSIST_PATH: str = "persist/opencode/proxy_pool.json"
