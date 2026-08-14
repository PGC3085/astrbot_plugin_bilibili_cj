"""凭据启动检查 / 登录状态校验 单元测试。

离线测试（无 AstrBot 运行时）验证：

1. ``PluginLifecycle._log_credential_status``：sessdata 缺失 → 匿名告警；
   有 sessdata 但缺 bili_jct/dedeuserid → 不完整告警；完整 → 无告警。
   buvid3/buvid4/ac_time_value 为可选，缺失不产生告警。
2. ``PluginLifecycle._log_login_status``：后台登录校验成功/失败分别记录
   info/warning，scheduler 不支持时静默跳过。
3. ``Scheduler.check_login``：委托 repository 的 ``check_login``；repository
   不支持时返回 None。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from main import PluginLifecycle
from scheduler import Scheduler


def _make_logger(name: str) -> tuple[logging.Logger, Any]:
    """返回隔离的测试 logger + 记录 handler。"""
    handler = _RecordListHandler()
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger, handler


class _RecordListHandler(logging.Handler):
    """收集 LogRecord 的内存 handler。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class _SchedulerWithLogin:
    """带 ``check_login`` 的 fake scheduler。"""

    def __init__(
        self, result: dict[str, Any] | None = None, error: Exception | None = None
    ) -> None:
        self._result = result
        self._error = error

    async def check_login(self) -> dict[str, Any] | None:
        if self._error is not None:
            raise self._error
        return self._result


def _make_lifecycle(
    credential_cfg: dict[str, Any],
    scheduler: Any,
    logger: logging.Logger,
) -> PluginLifecycle:
    """构造最小 PluginLifecycle（仅凭据检查用，不 initialize）。"""
    return PluginLifecycle(
        config={},
        context=None,
        credential_cfg=credential_cfg,
        db=None,
        scheduler=scheduler,
        reloader=None,
        logger=logger,
        webui=None,
    )


# --------------------------------------------------------------------------
# 1. _log_credential_status
# --------------------------------------------------------------------------


def test_anonymous_warning_when_no_sessdata() -> None:
    logger, handler = _make_logger("test_credentials.anon")
    lifecycle = _make_lifecycle({}, object(), logger)

    lifecycle._log_credential_status()

    warnings_ = [
        r.getMessage() for r in handler.records if r.levelno == logging.WARNING
    ]
    assert any("sessdata" in m and "匿名" in m for m in warnings_)


def test_incomplete_credential_warns_missing_bili_jct_dedeuserid() -> None:
    """有 sessdata 但缺 bili_jct/dedeuserid：告警缺字段，且不含 sessdata。"""
    logger, handler = _make_logger("test_credentials.incomplete")
    lifecycle = _make_lifecycle({"sessdata": "abc"}, object(), logger)

    lifecycle._log_credential_status()

    warnings_ = [
        r.getMessage() for r in handler.records if r.levelno == logging.WARNING
    ]
    assert any("bili_jct" in m and "dedeuserid" in m for m in warnings_)
    assert all("sessdata" not in m for m in warnings_)


def test_complete_credential_no_warning() -> None:
    """sessdata/bili_jct/dedeuserid 齐全：无告警，且记录已配置 info。"""
    logger, handler = _make_logger("test_credentials.complete")
    lifecycle = _make_lifecycle(
        {"sessdata": "a", "bili_jct": "b", "dedeuserid": "1"}, object(), logger
    )

    lifecycle._log_credential_status()

    warnings_ = [
        r.getMessage() for r in handler.records if r.levelno == logging.WARNING
    ]
    assert warnings_ == []
    infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
    assert any("已配置" in m for m in infos)


def test_optional_credential_fields_not_warned() -> None:
    """buvid3/buvid4/ac_time_value 缺失不告警（仅它们缺失，其余齐全）。"""
    logger, handler = _make_logger("test_credentials.optional")
    lifecycle = _make_lifecycle(
        {"sessdata": "a", "bili_jct": "b", "dedeuserid": "1"}, object(), logger
    )

    lifecycle._log_credential_status()

    warnings_ = [
        r.getMessage() for r in handler.records if r.levelno == logging.WARNING
    ]
    assert warnings_ == []
    assert all(
        "buvid3" not in m and "buvid4" not in m and "ac_time_value" not in m
        for m in warnings_
    )


# --------------------------------------------------------------------------
# 2. _log_login_status
# --------------------------------------------------------------------------


def test_login_status_success_logs_username() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_ok")
        scheduler = _SchedulerWithLogin({"uname": "测试用户", "mid": 123})
        lifecycle = _make_lifecycle({"sessdata": "a"}, scheduler, logger)

        await lifecycle._log_login_status()

        infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
        assert any("登录校验通过" in m and "测试用户" in m for m in infos)

    asyncio.run(scenario())


def test_login_status_failure_logs_warning() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_fail")
        scheduler = _SchedulerWithLogin(error=RuntimeError("cookie expired"))
        lifecycle = _make_lifecycle({"sessdata": "a"}, scheduler, logger)

        await lifecycle._log_login_status()

        warnings_ = [
            r.getMessage() for r in handler.records if r.levelno == logging.WARNING
        ]
        assert any("登录状态校验失败" in m for m in warnings_)

    asyncio.run(scenario())


def test_login_status_skips_when_scheduler_unsupported() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_skip")
        lifecycle = _make_lifecycle({"sessdata": "a"}, object(), logger)

        await lifecycle._log_login_status()

        assert handler.records == []

    asyncio.run(scenario())


# --------------------------------------------------------------------------
# 3. Scheduler.check_login
# --------------------------------------------------------------------------


def test_scheduler_check_login_delegates_to_repo() -> None:
    class _Repo:
        async def check_login(self) -> dict[str, Any]:
            return {"uname": "u", "mid": 1}

    async def scenario() -> None:
        scheduler = Scheduler(subscriptions=[], credential_cfg={}, repo=_Repo())
        assert await scheduler.check_login() == {"uname": "u", "mid": 1}

    asyncio.run(scenario())


def test_scheduler_check_login_none_when_repo_unsupported() -> None:
    async def scenario() -> None:
        scheduler = Scheduler(subscriptions=[], credential_cfg={}, repo=object())
        assert await scheduler.check_login() is None

    asyncio.run(scenario())
