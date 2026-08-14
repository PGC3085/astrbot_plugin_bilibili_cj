"""pytest 路径引导。

将插件包根目录（``astrbot_plugin_bilibili_cj``）插入 ``sys.path`` 首位，
保证离线环境（无 AstrBot 运行时、无 pip install）下 ``import config`` 等
顶层模块可解析。配合 ``tests/__init__.py``（使 tests 成为包，pytest 默认
prepend 导入模式会自动把包根目录加入 sys.path），双保险。
"""

import sys
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(_PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_ROOT))
