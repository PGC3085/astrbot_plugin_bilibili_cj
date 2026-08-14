"""B 站仓储层：类型化接口 + SDK 实现 + 类型化异常。

对外仅暴露 :class:`BiliRepository` 抽象接口与 :class:`SdkRepository` 实现；
轮询器（T6/T7/T8）与测试（T17）只依赖接口，SDK 仅在本包内被引用。
"""

from .bili import (
    BiliApiError,
    BiliAuthError,
    BiliError,
    BiliNetworkError,
    BiliNotFound,
    BiliRateLimited,
    BiliRepository,
    SdkRepository,
)

__all__ = [
    "BiliRepository",
    "SdkRepository",
    "BiliError",
    "BiliRateLimited",
    "BiliAuthError",
    "BiliNotFound",
    "BiliNetworkError",
    "BiliApiError",
]
