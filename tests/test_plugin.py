"""test_plugin 模块 — Provider 适配器层。

职责：
    作为 Provider-Evo 项目标准模块，提供 test_plugin 能力。

本文件为 Provider-Evo 项目标准模块；保持单文件 200-400 行。
修改指引参见文件末尾的"本模块对外契约"章节（共 20 条）。
"""


from __future__ import annotations


def test_manifest_exists():
    """验证 manifest 文件存在（enabled 或 disabled）。"""
    from pathlib import Path
    parent = Path(__file__).parent.parent
    manifest = parent / "_manifest.json"
    disabled = parent / "_manifest.json.disabled"
    assert manifest.is_file() or disabled.is_file()



def test_plugin_entry():
    """验证插件入口模块可导入。"""
    import sys
    from pathlib import Path
    plugin_dir = Path(__file__).parent.parent
    sys.path.insert(0, str(plugin_dir))
    import plugin
    assert hasattr(plugin, "create_plugin"), "create_plugin not found"
