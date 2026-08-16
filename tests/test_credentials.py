"""凭据启动检查 / 登录状态校验 单元测试。

离线测试（无 AstrBot 运行时）验证：

1. ``PluginLifecycle._log_credential_status``：sessdata 缺失 → 匿名告警；
   有 sessdata 但缺 bili_jct/dedeuserid → 不完整告警；完整 → 无告警。
   buvid3/buvid4/ac_time_value 为可选，缺失不产生告警。
2. ``PluginLifecycle._check_login_once``：登录校验成功/失败分别记录 info/warning
   并更新监控状态；连续失败达阈值时向 notify_session 发送告警。
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
        self.calls = 0

    async def check_login(self) -> dict[str, Any] | None:
        self.calls += 1
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
# 2. _check_login_once / 连续失败告警
# --------------------------------------------------------------------------


def test_login_status_success_logs_username() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_ok")
        scheduler = _SchedulerWithLogin({"uname": "测试用户", "mid": 123})
        lifecycle = _make_lifecycle({"sessdata": "a"}, scheduler, logger)

        ok = await lifecycle._check_login_once()

        assert ok is True
        assert lifecycle._login_last_ok_at is not None
        assert lifecycle._login_consecutive_failures == 0
        infos = [r.getMessage() for r in handler.records if r.levelno == logging.INFO]
        assert any("登录校验通过" in m and "测试用户" in m for m in infos)

    asyncio.run(scenario())


def test_login_status_failure_increments_and_warns() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_fail")
        scheduler = _SchedulerWithLogin(error=RuntimeError("cookie expired"))
        lifecycle = _make_lifecycle({"sessdata": "a"}, scheduler, logger)

        ok = await lifecycle._check_login_once()

        assert ok is False
        assert lifecycle._login_consecutive_failures == 1
        assert lifecycle._login_last_error == "cookie expired"
        warnings_ = [
            r.getMessage() for r in handler.records if r.levelno == logging.WARNING
        ]
        assert any("登录状态校验失败" in m for m in warnings_)

    asyncio.run(scenario())


def test_login_status_skips_when_scheduler_unsupported() -> None:
    async def scenario() -> None:
        logger, handler = _make_logger("test_credentials.login_skip")
        lifecycle = _make_lifecycle({"sessdata": "a"}, object(), logger)

        ok = await lifecycle._check_login_once()

        assert ok is True
        assert handler.records == []

    asyncio.run(scenario())


class _FakeContext:
    """记录 send_message 调用的假 context。"""

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send_message(self, session: str, chain: Any) -> bool:
        self.sent.append((session, chain))
        return True


def _make_monitor_lifecycle(
    scheduler: Any, context: Any, login_cfg: dict[str, Any], logger: logging.Logger
) -> PluginLifecycle:
    """构造带 login_monitor 配置的最小 PluginLifecycle。"""
    return PluginLifecycle(
        config={"login_monitor": login_cfg},
        context=context,
        credential_cfg={"sessdata": "a"},
        db=None,
        scheduler=scheduler,
        reloader=None,
        logger=logger,
        webui=None,
    )


def test_login_monitor_notifies_after_threshold() -> None:
    """连续失败达阈值时向 notify_session 发送一次告警。"""

    async def scenario() -> None:
        logger, _handler = _make_logger("test_credentials.notify")
        scheduler = _SchedulerWithLogin(error=RuntimeError("expired"))
        context = _FakeContext()
        lifecycle = _make_monitor_lifecycle(
            scheduler,
            context,
            {
                "enabled": True,
                "interval_sec": 60,
                "fail_threshold": 2,
                "notify_session": "aiocqhttp:GroupMessage:1",
            },
            logger,
        )

        assert await lifecycle._check_login_once() is False
        assert context.sent == []  # 第 1 次失败未达阈值，不通知

        assert await lifecycle._check_login_once() is False
        assert len(context.sent) == 1  # 第 2 次失败达阈值，通知一次
        session, chain = context.sent[0]
        assert session == "aiocqhttp:GroupMessage:1"
        assert "连续 2 次校验失败" in str(chain)

        assert await lifecycle._check_login_once() is False
        assert len(context.sent) == 1  # 第 3 次失败不再重复通知

    asyncio.run(scenario())


def test_login_monitor_defaults_when_absent() -> None:
    """未配置 login_monitor 时使用默认值。"""
    lifecycle = _make_lifecycle(
        {"sessdata": "a"}, object(), logging.getLogger("test_credentials.defaults")
    )
    assert lifecycle._login_monitor_enabled() is True
    assert lifecycle._login_monitor_interval() == 3600
    assert lifecycle._login_monitor_threshold() == 3


def test_login_monitor_interval_non_finite_falls_back() -> None:
    """interval_sec 为 inf/nan（手改配置）→ 回退默认值，监控任务不被永久挂起。"""
    for bad in (float("inf"), float("-inf"), float("nan")):
        lifecycle = _make_monitor_lifecycle(
            object(),
            _FakeContext(),
            {"interval_sec": bad},
            logging.getLogger("test_credentials.nonfinite"),
        )
        assert lifecycle._login_monitor_interval() == 3600


def test_login_monitor_skips_check_without_sessdata() -> None:
    """配置中无 sessdata（匿名模式）→ 监控任务空转，不调用 check_login。"""

    async def scenario() -> None:
        logger, _handler = _make_logger("test_credentials.anon_monitor")
        scheduler = _SchedulerWithLogin({"uname": "u", "mid": 1})
        context = _FakeContext()
        lifecycle = _make_monitor_lifecycle(
            scheduler,
            context,
            {"enabled": True, "interval_sec": 60},
            logger,
        )
        lifecycle._start_login_monitor()  # 配置无 credential → 匿名空转
        await asyncio.sleep(0)
        assert scheduler.calls == 0
        task = lifecycle._login_monitor_task
        assert task is not None
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

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
