"""B 站接口的类型化仓储层（bilibili_api SDK 封装）。

对外仅暴露 :class:`BiliRepository` 抽象接口与 :class:`SdkRepository`
实现；调用方（轮询器、测试）只依赖接口，不直接依赖 SDK，便于离线注入
mock / fake 进行单测。

bilibili_api 仅在模块顶部受保护的 try/except 中导入：SDK 未安装时，
接口与类型化异常仍可被干净导入（离线冒烟/单测场景），真正的 SDK 调用
错误会在构造 ``SdkRepository`` 或首次取凭据时以清晰的 :class:`BiliError`
抛出。
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any

try:
    from bilibili_api import Credential
    from bilibili_api.channel_series import (
        ChannelOrder,
        ChannelSeries,
        ChannelSeriesType,
    )
    from bilibili_api.exceptions import (
        ApiException,
        NetworkException,
        ResponseCodeException,
    )
    from bilibili_api.live import LiveRoom
    from bilibili_api.user import User

    # 凭据缺失异常：bilibili-api-python 17.x 起将 CredentialNoBuvidException
    # 拆分为 CredentialNoBuvid3Exception / CredentialNoBuvid4Exception；更早
    # 版本为单一 CredentialNoBuvidException。逐名兜底导入并归一化到
    # CredentialNoBuvidException（下方 except 子句引用的名字），兼容新旧
    # 版本；单个异常名缺失绝不影响 SDK 可用性判定。
    try:
        from bilibili_api.exceptions import (
            CredentialNoBuvid3Exception as CredentialNoBuvidException,
        )
    except ImportError:
        from bilibili_api.exceptions import CredentialNoBuvidException

    try:
        from bilibili_api.exceptions import CredentialNoBuvid4Exception
    except ImportError:
        # 旧版 SDK 无 buvid4 异常：归一化为 buvid3 异常（映射行为一致）
        CredentialNoBuvid4Exception = CredentialNoBuvidException

    _SDK_IMPORT_ERROR: ImportError | None = None
except ImportError as _import_error:
    _SDK_IMPORT_ERROR = _import_error

    # SDK 未安装（离线环境）时的占位异常类，形状与真实 SDK 异常兼容，
    # 仅用于让下方错误映射逻辑可被离线 mock 测试覆盖；生产环境导入真实
    # SDK，此分支不生效。
    class ApiException(Exception):
        """bilibili_api.exceptions.ApiException 的离线占位基类。"""

    class NetworkException(Exception):
        """bilibili_api.exceptions.NetworkException 的离线占位类。"""

    class CredentialNoBuvidException(Exception):
        """bilibili_api.exceptions.CredentialNoBuvid3Exception 的离线占位类。"""

    class CredentialNoBuvid4Exception(Exception):
        """bilibili_api.exceptions.CredentialNoBuvid4Exception 的离线占位类。"""

    class ResponseCodeException(Exception):
        """bilibili_api.exceptions.ResponseCodeException 的离线占位类。"""

        def __init__(self, code: int = 0, msg: str = "") -> None:
            super().__init__(msg)
            self.code = code
            self.msg = msg


# Credential 支持的字段白名单（其余配置键一律忽略，绝不透传给 SDK）。
_CREDENTIAL_FIELDS: frozenset[str] = frozenset(
    {"sessdata", "bili_jct", "buvid3", "buvid4", "dedeuserid", "ac_time_value"}
)
# SDK ResponseCodeException 的 code -> 类型化异常 映射表。
_RATE_LIMIT_CODES: frozenset[int] = frozenset({-412, -352})
_AUTH_CODES: frozenset[int] = frozenset({-101, -400})
_NOT_FOUND_CODE: int = -404


class BiliError(Exception):
    """B 站仓储层异常基类。"""


class BiliRateLimited(BiliError):
    """B 站风控/频率限制（SDK ResponseCodeException code=-412/-352）。"""


class BiliAuthError(BiliError):
    """B 站凭据无效或未授权（SDK code=-101/-400，或凭据缺少 buvid3/buvid4）。"""


class BiliNotFound(BiliError):
    """B 站资源不存在（SDK code=-404）。"""


class BiliNetworkError(BiliError):
    """网络/超时类错误（SDK NetworkException 或请求超时）。"""


class BiliApiError(BiliError):
    """其他 B 站接口错误（SDK ResponseCodeException 兜底）。"""


class BiliRepository(ABC):
    """B 站数据访问抽象接口；轮询器与测试只依赖本接口。"""

    @abstractmethod
    async def get_room_info(self, room_id: int) -> dict[str, Any]:
        """获取直播间信息（含 live_status/title 等）。"""

    @abstractmethod
    async def get_live_info(self, uid: int) -> dict[str, Any]:
        """获取用户直播间信息（含 live_room.roomid 等）。"""

    @abstractmethod
    async def get_dynamics(self, uid: int, offset: str | int = 0) -> dict[str, Any]:
        """获取用户动态（含 items/has_more/offset 等）。"""

    @abstractmethod
    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict[str, Any]:
        """获取合集/列表视频（含 archives）。"""


class SdkRepository(BiliRepository):
    """基于 bilibili_api SDK 的 :class:`BiliRepository` 实现。

    参数:
        credential: B 站 Cookie 配置字典，仅识别
            ``sessdata``/``bili_jct``/``buvid3``/``buvid4``/``dedeuserid``/
            ``ac_time_value`` 六个字段；其余键被忽略。
    """

    _TIMEOUT_SECONDS: float = 30.0
    """单次 SDK 请求超时（秒），可被子类覆写。"""

    def __init__(self, credential: dict[str, Any] | None = None) -> None:
        if _SDK_IMPORT_ERROR is not None:
            raise BiliError(
                "bilibili_api SDK 未安装，无法创建 SdkRepository"
                f"（请执行 pip install bilibili-api-python）: {_SDK_IMPORT_ERROR}"
            )
        self._credential_cfg: dict[str, Any] = dict(credential or {})
        self._credential: Credential | None = None

    # ------------------------------------------------------------------
    # 公开接口
    # ------------------------------------------------------------------

    async def get_room_info(self, room_id: int) -> dict[str, Any]:
        """获取直播间信息。"""
        return await self._call_sdk(lambda: self._sdk_room_info(room_id))

    async def get_live_info(self, uid: int) -> dict[str, Any]:
        """获取用户直播间信息。"""
        return await self._call_sdk(lambda: self._sdk_live_info(uid))

    async def get_dynamics(self, uid: int, offset: str | int = 0) -> dict[str, Any]:
        """获取用户动态；``offset=0`` 表示从最新开始。"""
        return await self._call_sdk(lambda: self._sdk_dynamics(uid, offset))

    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict[str, Any]:
        """获取合集/列表视频。"""
        return await self._call_sdk(
            lambda: self._sdk_videos(uid, list_id, series_type, pn, ps)
        )

    async def check_login(self) -> dict[str, Any]:
        """校验凭据登录状态；未登录/凭据无效抛 :class:`BiliAuthError`。

        Returns:
            已登录用户信息（尽力提取 ``mid`` / ``uname``，字段缺失时为 None）。
        """
        return await self._call_sdk(lambda: self._sdk_check_login())

    # ------------------------------------------------------------------
    # SDK 调用缝（测试子类覆写此层注入 mock，从而离线覆盖异常映射）
    # ------------------------------------------------------------------

    async def _sdk_room_info(self, room_id: int) -> dict[str, Any]:
        """调用 SDK ``LiveRoom.get_room_info``。"""
        room = LiveRoom(room_id, credential=self._get_credential())
        return await room.get_room_info()

    async def _sdk_live_info(self, uid: int) -> dict[str, Any]:
        """调用 SDK ``User.get_live_info``。"""
        user = User(uid, credential=self._get_credential())
        return await user.get_live_info()

    async def _sdk_dynamics(self, uid: int, offset: str | int) -> dict[str, Any]:
        """调用 SDK ``User.get_dynamics_new``。

        ``offset`` 归一化：空与 ``"0"`` 均表示从头开始（polymer 接口第一页
        传空串），其余原样透传。
        """
        user = User(uid, credential=self._get_credential())
        offset_str = str(offset).strip() if offset is not None else ""
        if offset_str in ("", "0"):
            offset_str = ""
        return await user.get_dynamics_new(offset=offset_str)

    async def _sdk_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict[str, Any]:
        """调用 SDK ``ChannelSeries.get_videos``（合集/系列，按默认排序分页）。"""
        try:
            series_type_enum = ChannelSeriesType(series_type)
        except ValueError as exc:
            raise BiliApiError(f"非法的 series_type: {series_type}") from exc
        series = ChannelSeries(
            uid=uid,
            type_=series_type_enum,
            id_=list_id,
            credential=self._get_credential(),
        )
        return await series.get_videos(sort=ChannelOrder.DEFAULT, pn=pn, ps=ps)

    async def _sdk_check_login(self) -> dict[str, Any]:
        """调用 SDK ``get_self_info`` 获取当前登录用户信息。

        ``get_self_info`` 惰性导入：登录检查失败绝不影响其余接口的 SDK 可用性
        判定；不同 SDK 版本返回形状（裸数据或含 ``data`` 包装）均兼容提取。
        """
        from bilibili_api.user import get_self_info

        info = await get_self_info(self._get_credential())
        if not isinstance(info, dict):
            return {}
        data = info.get("data") if isinstance(info.get("data"), dict) else info
        return {"mid": data.get("mid"), "uname": data.get("uname")}

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _get_credential(self) -> Credential:
        """惰性构造并缓存 Credential；SDK 缺失时抛出清晰错误。"""
        if _SDK_IMPORT_ERROR is not None:
            raise BiliError(
                "bilibili_api SDK 未安装，无法发起 B 站请求"
                f"（请执行 pip install bilibili-api-python）: {_SDK_IMPORT_ERROR}"
            )
        if self._credential is None:
            self._credential = self._build_credential()
        return self._credential

    def _build_credential(self) -> Credential:
        """按白名单显式挑选受支持字段构造 Credential。

        只传入非空且受支持的字段；绝不使用 ``Credential(**config_dict)``
        透传整个配置（未知键会因不支持的 kwargs 触发 TypeError）。
        """
        fields: dict[str, str] = {
            key: value
            for key, value in self._credential_cfg.items()
            if key in _CREDENTIAL_FIELDS and value
        }
        return Credential(**fields)

    async def _call_sdk(
        self, sdk_call: Callable[[], Awaitable[dict[str, Any]]]
    ) -> dict[str, Any]:
        """统一包裹 SDK 调用：超时、取消透传与异常映射。

        必须先重抛 ``CancelledError`` 再处理超时：``asyncio.wait_for``
        在超时触发时会吞掉内部 CancelledError 并抛出 TimeoutError，而外层
        任务被取消（如轮询任务 shutdown）时若先匹配超时分支，会把取消误判
        为网络超时；因此 CancelledError 必须优先透传。
        """
        try:
            return await asyncio.wait_for(sdk_call(), timeout=self._TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError as exc:
            raise BiliNetworkError(
                f"B 站接口请求超时（>{self._TIMEOUT_SECONDS}s）"
            ) from exc
        except NetworkException as exc:
            raise BiliNetworkError(f"B 站网络请求异常: {exc}") from exc
        except (CredentialNoBuvidException, CredentialNoBuvid4Exception) as exc:
            raise BiliAuthError(f"B 站凭据缺少 buvid3/buvid4: {exc}") from exc
        except ResponseCodeException as exc:
            raise self._map_response_error(exc) from exc
        except ApiException as exc:
            # 兜底：SDK 其余未分类异常（ArgsException/VerifyException 等）
            # 一律映射为 BiliApiError，维持「SDK 失败必为 BiliError」契约。
            raise BiliApiError(f"B 站 SDK 异常: {exc}") from exc

    @staticmethod
    def _map_response_error(exc: ResponseCodeException) -> BiliError:
        """将 SDK ``ResponseCodeException`` 按 code 映射为类型化异常。"""
        code = exc.code
        message = getattr(exc, "msg", "") or getattr(exc, "message", "")
        if code in _RATE_LIMIT_CODES:
            return BiliRateLimited(f"B 站风控/频率限制（code={code}）: {message}")
        if code in _AUTH_CODES:
            return BiliAuthError(f"B 站凭据无效或未授权（code={code}）: {message}")
        if code == _NOT_FOUND_CODE:
            return BiliNotFound(f"B 站资源不存在（code={code}）: {message}")
        return BiliApiError(f"B 站接口错误（code={code}）: {message}")
