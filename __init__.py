"""astrbot_plugin_bilibili_cj 插件包。

对外公开核心组件，便于外部代码与测试以包形式导入：

- 配置：:class:`Subscription` / :func:`normalize`
- 数据：:class:`Database`
- 推送：:func:`build_chain` / :func:`send`
- 调度：:class:`Scheduler`
- 仓储：:class:`BiliRepository` / :class:`SdkRepository` 及类型化异常
"""

from .config import Subscription, normalize
from .db import Database
from .push import build_chain, send
from .repository import (
    BiliApiError,
    BiliAuthError,
    BiliError,
    BiliNetworkError,
    BiliNotFound,
    BiliRateLimited,
    BiliRepository,
    SdkRepository,
)
from .scheduler import Scheduler

__all__ = [
    "Subscription",
    "normalize",
    "Database",
    "build_chain",
    "send",
    "Scheduler",
    "BiliRepository",
    "SdkRepository",
    "BiliError",
    "BiliRateLimited",
    "BiliAuthError",
    "BiliNotFound",
    "BiliNetworkError",
    "BiliApiError",
]
