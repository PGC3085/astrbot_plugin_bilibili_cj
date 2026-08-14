"""repository 层 SDK 导入守卫回归测试（线上部署修复 v1.0.1）。

线上曾出现 ``ImportError: cannot import name 'CredentialNoBuvidException'
from 'bilibili_api.exceptions'``：bilibili-api-python 17.x 起将凭据缺失异常
拆分为 ``CredentialNoBuvid3Exception`` / ``CredentialNoBuvid4Exception``，
旧名导入失败导致整个 SDK 导入块失败，插件误报「SDK 未安装」。

修复（repository/bili.py）：逐名兜底导入并归一化到
``CredentialNoBuvidException``（``_call_sdk`` except 子句引用的名字），
兼容新旧版本 SDK；单个异常名缺失绝不影响 SDK 可用性判定。

本文件离线锁定修复后的结构契约：

1. buvid3/buvid4 两个凭据缺失异常名在模块内始终可用（真实类或离线
   占位类，二者必居其一）。
2. SDK 缺失时 ``SdkRepository`` 构造抛出带 pip 安装提示的 ``BiliError``
   （SDK 已安装的环境跳过该离线断言）。
3. ``_call_sdk`` 对 buvid3/buvid4 两类异常均映射为 ``BiliAuthError``
   （离线 mock 覆盖，与真实 SDK 的版本差异被导入归一化吸收）。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from repository import BiliAuthError, BiliError, SdkRepository
from repository import bili as bili_module


def test_credential_exception_names_always_available() -> None:
    """buvid3/buvid4 异常名在模块内始终可导入（真实类或离线占位类）。"""
    assert issubclass(bili_module.CredentialNoBuvidException, Exception)
    assert issubclass(bili_module.CredentialNoBuvid4Exception, Exception)


def test_sdk_missing_raises_clear_error() -> None:
    """SDK 缺失时 SdkRepository 构造抛出带 pip 提示的 BiliError。"""
    if bili_module._SDK_IMPORT_ERROR is None:
        pytest.skip("本环境已安装 bilibili_api，跳过离线断言")
    with pytest.raises(BiliError) as exc_info:
        SdkRepository(credential={})
    message = str(exc_info.value)
    assert "bilibili_api" in message
    assert "pip install bilibili-api-python" in message


def test_call_sdk_maps_buvid_exceptions_to_auth_error() -> None:
    """_call_sdk 将 buvid3/buvid4 缺失异常均映射为 BiliAuthError。

    通过绕过 ``__init__`` 的离线构造（SDK 缺失时构造函数直接抛错）
    验证异常映射分支；真实/占位类名称差异已被导入层归一化。
    """

    class _FakeSdkRepo(SdkRepository):
        def __init__(self) -> None:
            self._credential_cfg: dict[str, Any] = {}
            self._credential = None

    async def scenario() -> None:
        repo = _FakeSdkRepo()
        for name in ("CredentialNoBuvidException", "CredentialNoBuvid4Exception"):
            exc_cls = getattr(bili_module, name)

            async def raise_it() -> dict[str, Any]:
                raise exc_cls("缺少凭据")

            with pytest.raises(BiliAuthError) as exc_info:
                await repo._call_sdk(raise_it)
            assert "buvid3/buvid4" in str(exc_info.value)

    asyncio.run(scenario())
