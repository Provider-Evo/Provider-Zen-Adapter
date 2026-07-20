

from typing import Dict, List

BASE_URL: str = "https://opencode.ai/zen/v1"
CHAT_PATH: str = "/chat/completions"
MODELS_PATH: str = "/models"

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
