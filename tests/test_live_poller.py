"""直播轮询器单元测试（计划 todo 6）。

全部离线：fake repository（脚本化 live/room 信息）、临时 SQLite
（``tmp_path``）、记录式 fake context、真实 ``push.build_chain``/
``push.send``（离线 str 模式）、注入可控时钟。每个用例在单个
``asyncio.run`` 内完成 init → 轮询 → close（aiosqlite 连接不跨
event loop 复用）。

覆盖计划验收点：

1. seed 静默写状态；2. 0→1 推开播（title/分区/url）；3.
``live_start_time==0`` 回退 now；4. 改标题（启用/禁用）；5.
1→1→0×2 仅第 2 次 0 推下播（含时长）、不重复；6. 从未直播不推
下播；7. status 2 视同未播；8. 重启抑制 + arming/counting 恢复；
9. pending 重投成功后清空（当轮唯一推送）；10. pending 达上限丢弃
并告警；11. pending 相反转移清除（复播不推陈旧下播）；12. pending
24h 过期丢弃；13. roomid==0 跳过 get_room_info；14. 网络错误计数、
不触发下播、不崩溃、下轮重解析 room_id。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from types import SimpleNamespace
from uuid import uuid4

from config import Subscription
from db import Database
from poller.live import LivePoller
from push import build_chain, send
from repository import BiliNetworkError

_SESSION = "aiocqhttp:GroupMessage:123"
_T0 = 1_700_000_000


class Clock:
    """可变时钟：``tick(sec)`` 推进注入给 poller 的 ``now``。"""

    def __init__(self, start: int = _T0) -> None:
        self.t = start

    def __call__(self) -> float:
        return float(self.t)

    def tick(self, sec: int) -> None:
        self.t += sec


def _room(
    status: int = 0,
    title: str = "测试标题",
    start: int = _T0,
    area: str = "游戏",
    cover: str = "https://example.com/cover.jpg",
) -> dict:
    return {
        "live_status": status,
        "title": title,
        "live_start_time": start,
        "area_name": area,
        "cover": cover,
    }


class FakeRepo:
    """脚本化 BiliRepository fake：get_live_info 返回 roomid，get_room_info
    返回房间 dict；``room_error``/``live_error`` 设置后持续抛出（显式清为
    None 才恢复），用于模拟连续网络故障。
    """

    def __init__(self, roomid: int = 1, room: dict | None = None) -> None:
        self.roomid = roomid
        self.room = dict(room or _room(0))
        self.room_info_calls = 0
        self.live_info_calls = 0
        self.room_error: Exception | None = None
        self.live_error: Exception | None = None

    async def get_live_info(self, uid: int) -> dict:
        self.live_info_calls += 1
        if self.live_error is not None:
            raise self.live_error
        return {"live_room": {"roomid": self.roomid}}

    async def get_room_info(self, room_id: int) -> dict:
        self.room_info_calls += 1
        if self.room_error is not None:
            raise self.room_error
        return dict(self.room)


class FakeContext:
    """记录 ``send_message`` 调用；``ok`` 可变（False 模拟投递失败）。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.sent: list[tuple[str, object]] = []

    async def send_message(self, session: str, chain: object) -> bool:
        self.sent.append((session, chain))
        return self.ok


class _RecordListHandler(logging.Handler):
    """收集 LogRecord 的 handler，供断言日志内容。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _recording_logger() -> tuple[logging.Logger, list[logging.LogRecord]]:
    logger = logging.getLogger(f"test_live_poller.{uuid4().hex}")
    logger.propagate = False
    handler = _RecordListHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, handler.records


def _make_subscription(sub_id: str = "live-1") -> Subscription:
    return Subscription(
        id=sub_id,
        type="live",
        name="测试主播",
        uid=10086,
        push_session_ids=[_SESSION],
    )


def _make_poller(
    repo: FakeRepo,
    db: Database,
    context: FakeContext | None = None,
    logger: logging.Logger | None = None,
    subscription: Subscription | None = None,
    push_title_change: bool = True,
    now: Callable[[], float] | None = None,
    push_cover: bool = True,
) -> tuple[LivePoller, dict]:
    sub = subscription or _make_subscription()
    status: dict = {sub.id: SimpleNamespace(last_push_at=None, last_error=None)}
    poller = LivePoller(
        subscription=sub,
        repo=repo,
        db=db,
        build_chain=build_chain,
        send=send,
        status=status,
        logger=logger,
        context=context or FakeContext(),
        push_title_change=push_title_change,
        now=now if now is not None else lambda: float(_T0),
        push_cover=push_cover,
    )
    return poller, status


def test_seed_silent_writes_state(tmp_path) -> None:
    """1. 首轮静默 seed：不推送，但把 room_id/last_status 写入 live_state。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            assert await poller.poll() is False
            assert context.sent == []
            state = await db.get_live_state("live-1")
            assert state is not None
            assert state.room_id == 1
            assert state.last_status == 0
            assert state.uid == 10086
        finally:
            await db.close()

    asyncio.run(scenario())


def test_open_transition_pushes_live_on(tmp_path) -> None:
    """2. 0→1 推"开播"，载荷含 title/分区/url。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 0，静默
            repo.room = _room(1, title="开播啦", area="音乐")
            assert await poller.poll() is True
            assert len(context.sent) == 1
            session, chain = context.sent[0]
            assert session == _SESSION
            assert isinstance(chain, str)
            assert "【B站开播】" in chain
            assert "开播啦" in chain
            assert "音乐" in chain
            assert "https://live.bilibili.com/1" in chain
            state = await db.get_live_state("live-1")
            assert state.last_status == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_live_start_time_zero_falls_back_to_now(tmp_path) -> None:
    """3. 0→1 且 live_start_time==0 → last_live_time 回退为当前时间。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        clock = Clock(_T0)
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context, now=clock)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1, start=0)
            await poller.poll()
            state = await db.get_live_state("live-1")
            assert state.last_live_time == _T0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_title_change_pushed_when_enabled(tmp_path) -> None:
    """4a. 1→1 且标题变化 + push_title_change=True → 推"改标题"。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1, title="旧标题")
            await poller.poll()  # 开播
            repo.room = _room(1, title="新标题")
            assert await poller.poll() is True
            assert len(context.sent) == 2
            chain = str(context.sent[1][1])
            assert "【B站改标题】" in chain
            assert "旧标题" in chain
            assert "新标题" in chain
            await poller.poll()  # 标题不变 → 不再推
            assert len(context.sent) == 2
        finally:
            await db.close()

    asyncio.run(scenario())


def test_title_change_not_pushed_when_disabled(tmp_path) -> None:
    """4b. push_title_change=False → 标题变化不推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context, push_title_change=False)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1, title="旧标题")
            await poller.poll()  # 开播
            repo.room = _room(1, title="新标题")
            await poller.poll()
            assert len(context.sent) == 1  # 仅开播，无改标题
        finally:
            await db.close()

    asyncio.run(scenario())


def test_offline_two_strikes_push_once_with_duration(tmp_path) -> None:
    """5. 1→1→0×2：仅第 2 次 0 推"下播"（时长=now-last_live_time），不重复。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        clock = Clock(_T0)
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context, now=clock)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            await poller.poll()  # 开播（last_live_time=_T0）
            assert len(context.sent) == 1
            await poller.poll()  # 1→1 无变化
            assert len(context.sent) == 1
            repo.room = _room(0)
            clock.tick(3600)
            await poller.poll()  # 0（1/2）
            assert len(context.sent) == 1  # 首次不推（等复查确认）
            assert await poller.poll() is True  # 0（2/2）→ 下播
            assert len(context.sent) == 2
            session, chain = context.sent[1]
            assert session == _SESSION
            assert "【B站下播】" in str(chain)
            assert "时长：3600" in str(chain)
            assert "https://live.bilibili.com/1" in str(chain)
            await poller.poll()  # 0（3/3）：同一离线期不重复推
            assert len(context.sent) == 2
        finally:
            await db.close()

    asyncio.run(scenario())


def test_never_observed_live_no_offline_push(tmp_path) -> None:
    """6. 从未观测到 status==1 → 离线轮次不计数、不推下播。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 0
            for _ in range(5):
                await poller.poll()
            assert context.sent == []
            state = await db.get_live_state("live-1")
            assert state.consecutive_offline_count == 0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_status_2_treated_as_offline(tmp_path) -> None:
    """7. status 2 视同未播：2→1 触发开播；1→2 计入离线漏判。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(2))
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 2，静默
            assert context.sent == []
            repo.room = _room(1)
            assert await poller.poll() is True  # 2→1 开播
            repo.room = _room(2)
            await poller.poll()  # 1→2（1/2）
            await poller.poll()  # （2/2）→ 下播
            assert len(context.sent) == 2
            assert "【B站下播】" in str(context.sent[1][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_restart_continuity_suppression_and_arming(tmp_path) -> None:
    """8. 重建 poller：首轮观测到 status==1 时静默刷新、不推（含改标题）；
    随后 0×2 仍推下播（arming/counting 从 DB 恢复）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        clock = Clock(_T0)
        context_a = FakeContext()
        poller_a, _ = _make_poller(repo, db, context=context_a, now=clock)
        try:
            await poller_a.poll()  # seed 0
            repo.room = _room(1, title="旧标题")
            await poller_a.poll()  # 开播（推一次）
            assert len(context_a.sent) == 1
            # 重建（如插件重载）
            repo.room = _room(1, title="新标题")
            context_b = FakeContext()
            poller_b, _ = _make_poller(repo, db, context=context_b, now=clock)
            assert await poller_b.poll() is False  # 抑制：不推（含改标题）
            assert context_b.sent == []
            state = await db.get_live_state("live-1")
            assert state.last_title == "新标题"  # 静默刷新
            await poller_b.poll()  # 1→1 无变化
            assert context_b.sent == []
            # 0×2 → 下播（arming 恢复；时长按静默刷新后的 last_live_time）
            repo.room = _room(0)
            clock.tick(1800)
            await poller_b.poll()
            await poller_b.poll()
            assert len(context_b.sent) == 1
            assert "时长：1800" in str(context_b.sent[0][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pending_live_redelivered_then_cleared(tmp_path) -> None:
    """9. 开播推送失败 → pending{tries:1}；下一轮仍直播 → 重投（当轮唯一
    推送），成功即清空。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext(ok=False)
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            assert await poller.poll() is False  # 开播失败
            state = await db.get_live_state("live-1")
            pending = json.loads(state.pending_push)
            assert pending == {"kind": "live", "tries": 1, "timestamp": _T0}
            context.ok = True
            assert await poller.poll() is True  # 重投成功
            assert len(context.sent) == 2  # 首次失败 + 重投成功（仅此一条）
            state = await db.get_live_state("live-1")
            assert state.pending_push is None
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pending_live_dropped_after_max_tries(tmp_path) -> None:
    """10. 重投 3 次全失败 → 清空 pending 并告警，不再尝试。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext(ok=False)
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            await poller.poll()  # 失败 1（tries=1）
            await poller.poll()  # 重投 2（tries=2）
            assert await poller.poll() is False  # 重投 3 → 达上限：丢弃+告警
            state = await db.get_live_state("live-1")
            assert state.pending_push is None
            assert any("丢弃" in r.getMessage() for r in records)
            await poller.poll()  # 不再尝试
            assert len(context.sent) == 3
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pending_offline_cleared_on_relive(tmp_path) -> None:
    """11. 下播失败留 pending{offline}；复播（相反转移）→ 清空，不重投陈旧
    下播。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        clock = Clock(_T0)
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context, now=clock)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            await poller.poll()  # 开播成功
            repo.room = _room(0)
            clock.tick(3600)
            await poller.poll()  # 0（1/2）
            context.ok = False
            await poller.poll()  # 0（2/2）→ 下播失败 → pending{offline}
            state = await db.get_live_state("live-1")
            assert json.loads(state.pending_push)["kind"] == "offline"
            assert state.offline_notified == 1  # 首次尝试即置位（失败也置）
            # 复播：相反转移 → 清空 pending，正常推开播
            context.ok = True
            repo.room = _room(1)
            assert await poller.poll() is True
            state = await db.get_live_state("live-1")
            assert state.pending_push is None
            assert "【B站开播】" in str(context.sent[-1][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pending_expired_24h_discarded(tmp_path) -> None:
    """12. pending 超过 24h → 丢弃并走常规分支，不重投。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        clock = Clock(_T0)
        context = FakeContext(ok=False)
        poller, _ = _make_poller(repo, db, context=context, now=clock)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            await poller.poll()  # 开播失败 → pending
            state = await db.get_live_state("live-1")
            assert json.loads(state.pending_push)["kind"] == "live"
            clock.tick(25 * 3600)  # 超过 24h
            context.ok = True
            assert await poller.poll() is False  # 过期丢弃；1→1 无变化 → 不推
            state = await db.get_live_state("live-1")
            assert state.pending_push is None
            assert len(context.sent) == 1  # 仅首次失败那次
        finally:
            await db.close()

    asyncio.run(scenario())


def test_roomid_zero_skips_room_info(tmp_path) -> None:
    """13. roomid==0 → 视同未播：跳过 get_room_info，不推送、不报错。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(roomid=0)
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()
            await poller.poll()
            assert context.sent == []
            assert repo.room_info_calls == 0
            assert repo.live_info_calls == 2  # 每轮解析一次
            state = await db.get_live_state("live-1")
            assert state.last_status == 0
        finally:
            await db.close()

    asyncio.run(scenario())


def test_network_error_counted_no_crash_no_offline(tmp_path) -> None:
    """14. 网络错误：error_count/last_error 记录、不触发下播、不崩溃；
    失败后 room_id 置 0 → 下轮重解析。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(room=_room(0))
        context = FakeContext()
        poller, status = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 0
            repo.room = _room(1)
            await poller.poll()  # 开播成功（arming）
            repo.room_error = BiliNetworkError("网络炸了")
            assert await poller.poll() is False
            assert await poller.poll() is False
            assert len(context.sent) == 1  # 错误轮次不推送任何内容
            assert poller.error_count == 2
            assert "网络炸了" in status["live-1"].last_error
            state = await db.get_live_state("live-1")
            assert state.room_id == 0  # 置 0 → 下轮重解析
            assert repo.live_info_calls == 2  # seed 1 + 第 2 个失败轮重解析 1
            # 恢复：下一轮重解析后正常观测，无陈旧 pending/下播
            repo.room_error = None
            await poller.poll()
            assert len(context.sent) == 1  # 1→1 无变化：不推
            assert repo.live_info_calls == 3  # 恢复轮再重解析 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_live_payload_cover_gated_by_setting(tmp_path) -> None:
    """push_cover=False 时开播载荷不携带封面；缺省 True 时携带。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        room = {
            "room_id": 1,
            "title": "标题",
            "area_name": "游戏",
            "live_start_time": _T0,
            "cover": "https://example.com/cover.jpg",
        }

        poller_on, _ = _make_poller(FakeRepo(room=_room(0)), db)
        payload = poller_on._live_payload(room)
        assert payload["cover"] == "https://example.com/cover.jpg"

        poller_off, _ = _make_poller(FakeRepo(room=_room(0)), db, push_cover=False)
        payload = poller_off._live_payload(room)
        assert "cover" not in payload
        await db.close()

    asyncio.run(scenario())
