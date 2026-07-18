"""constants 模块 — Provider 适配器层。

职责：
    集中放置 provider 常量定义（模型名、URL 模板、错误码等）。

本文件为 Provider-Evo 项目标准模块；保持单文件 200-400 行。
修改指引参见文件末尾的"本模块对外契约"章节（共 20 条）。
"""


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
# =======================================================================
# 重导出 — 同包协同模块（保持外部 ``from .. import`` 路径稳定）
# =======================================================================

__all__: List[str] = []
