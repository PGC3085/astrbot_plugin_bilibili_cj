"""整体冒烟测试（计划 todo 17）— 离线端到端。

把整个插件链路（真实 poller + 真实 ``push.build_chain``/``push.send`` +
真实 ``Scheduler`` 编排 + 注入的 fake repository + 临时/即时数据层 + 记录式
fake context）一次性组装，证明计划 Success Criteria #2：直播/动态/合集三类订阅
各能检测变更，并通过 ``context.send_message`` 推送到配置的多平台会话。

全程不发起任何真实网络请求：repository 全部为注入的 fake，并断言
``Scheduler`` 持有的就是该 fake（``SdkRepository`` 从未被构造）、
``bilibili_api`` 从未被导入。

覆盖场景：

1. 全链路直播流：seed（静默）→ 0/2→1 推开播（标题/分区/url 齐全）→ 1→1
   不重复推送 → 1→0/2 × 3 仅第 3 轮推下播（时长 = now - last_live_time）。
2. 全链路动态流：seed（静默）→ 第二轮注入新动态 → 推送一次（跨轮去重）。
3. 全链路合集流：seed（静默）→ 追加新 bvid → 推送一次（跨轮去重）。
4. 调度器集成：真实 Scheduler + ``InstantDb``（即时假数据层，无 aiosqlite
   线程往返，时序全确定性）+ ``ControlledSleep`` 受控推进 → 三类订阅 seed
   静默 → 注入变更 → 一轮内三类各推一次 → 直播转离线 ×3 → 推下播（时长正确）
   → ``stop()`` 干净退出。
5. 无网络断言：fake repository 被注入、调用计数 > 0、``bilibili_api`` 未导入。

复用既有测试确立的模式：每用例单个 ``asyncio.run``；直播/动态/合集单链用临时
SQLite（``tmp_path``，init→poll→close 同 loop）；调度器场景复用
``tests/test_scheduler.py`` 的 InstantDb/ControlledSleep 假时钟方案（真实
SQLite × 假时钟的线程往返竞态已在 T10 证明不可靠）。首轮静默 seed 反洪泛闸门
（首轮零推送、第二轮才推送）为计划 QA 回归点。
"""

from __future__ import annotations

import asyncio
import sys
from types import SimpleNamespace
from typing import Any

from config import Subscription
from db import Database
from poller.collection import CollectionPoller
from poller.dynamic import DynamicPoller
from poller.live import LivePoller
from push import build_chain, send
from scheduler import Scheduler

_SESSION = "aiocqhttp:GroupMessage:123"
_T0 = 1_700_000_000
#: 空闲睡眠哨兵：非 idle 过滤阈值（_IDLE_SLEEP_SEC = 3600）。
_IDLE = 3600.0


class Clock:
    """可控时钟：``tick(sec)`` 推进，``t`` 为当前秒数。"""

    def __init__(self, start: int = _T0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def tick(self, sec: float) -> None:
        self.t += sec


class ControlledSleep:
    """受控假 sleep：记录时长并阻塞，直到测试推进时钟满足条件。

    与 ``tests/test_scheduler.py`` 同款：调度器场景的确定性时间驱动，
    每 ``advance(sec)`` 唤醒全部阻塞中的 sleep，条件不满足者重新注册。
    """

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.deltas: list[float] = []
        self._pending: list[tuple[float, asyncio.Event]] = []

    async def __call__(self, sec: float) -> None:
        self.deltas.append(sec)
        start = self.clock.t
        while self.clock.t - start < sec:
            event = asyncio.Event()
            self._pending.append((sec, event))
            await event.wait()

    async def advance(self, sec: float) -> None:
        """推进时钟并唤醒所有阻塞中的 sleep；不满足条件的会重新注册。"""
        self.clock.tick(sec)
        pending, self._pending = self._pending, []
        for _sec, event in pending:
            event.set()
        await asyncio.sleep(0)


class InstantDb:
    """即时假数据层：无真实 I/O（无 aiosqlite 线程往返），调度时序全确定性。

    仅实现调度器/轮询器实际用到的方法；真实 SQLite 路径由单链冒烟用例覆盖，
    调度器场景只关心编排时序（同 ``tests/test_scheduler.py`` 方案）。
    """

    def __init__(self) -> None:
        self._live: dict[str, Any] = {}
        self.seeded: set[str] = set()
        self.seen: set[tuple[str, str]] = set()

    async def get_live_state(self, sub_id: str) -> Any | None:
        return self._live.get(sub_id)

    async def upsert_live_state(self, sub_id: str, **fields: Any) -> None:
        state = self._live.get(sub_id)
        if state is None:
            # 与 db.LiveState 字段形状一致（含 pending_push 等），
            # 缺省 None，避免轮询器读取缺失属性。
            state = SimpleNamespace(
                sub_id=sub_id,
                uid=None,
                room_id=None,
                last_status=None,
                last_title=None,
                last_live_time=None,
                live_ended_at=None,
                consecutive_offline_count=None,
                last_checked_at=None,
                pending_push=None,
                offline_notified=None,
            )
            self._live[sub_id] = state
        for key, value in fields.items():
            setattr(state, key, value)

    async def get_seeded(self, table: str, sub_id: str) -> bool:
        del table
        return sub_id in self.seeded

    async def set_seeded(self, table: str, sub_id: str, value: bool) -> None:
        del table
        if value:
            self.seeded.add(sub_id)
        else:
            self.seeded.discard(sub_id)

    async def insert_video_if_new(
        self, sub_id: str, bvid: str, uid: int, list_id: int
    ) -> bool:
        del uid, list_id
        key = (sub_id, bvid)
        if key in self.seen:
            return False
        self.seen.add(key)
        return True

    async def insert_dynamic_if_new(
        self, sub_id: str, dynamic_id: str, type_: int
    ) -> bool:
        del type_
        return await self.insert_video_if_new(sub_id, dynamic_id, 0, 0)

    async def prune_old(self) -> None:
        return None


class FakeContext:
    """记录 ``send_message`` 调用；默认全部投递成功。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.sent: list[tuple[str, object]] = []

    async def send_message(self, session: str, chain: object) -> bool:
        self.sent.append((session, chain))
        return self.ok


class FakeLiveRepo:
    """脚本化 live 仓库：get_live_info 返回 roomid，get_room_info 返回房间 dict。"""

    def __init__(self, roomid: int = 1, room: dict | None = None) -> None:
        self.roomid = roomid
        self.room = dict(room or _room(0))
        self.room_info_calls = 0
        self.live_info_calls = 0

    async def get_live_info(self, uid: int) -> dict:
        self.live_info_calls += 1
        return {"live_room": {"roomid": self.roomid}}

    async def get_room_info(self, room_id: int) -> dict:
        self.room_info_calls += 1
        return dict(self.room)


class FakeDynamicRepo:
    """脚本化动态仓库：按调用序号返回 canned 页（越界复用最后一页）。"""

    def __init__(self, pages: list[dict]) -> None:
        self.pages = list(pages)
        self.calls = 0

    async def get_dynamics(self, uid: int, offset: object = 0) -> dict:
        del offset
        self.calls += 1
        if not self.pages:
            return {"items": [], "has_more": 0}
        return self.pages[(self.calls - 1) % len(self.pages)]


class FakeCollectionRepo:
    """脚本化合集仓库：按 ``ps`` 切片服务一个扁平 item 列表。"""

    def __init__(self, items: list[dict]) -> None:
        self.items = list(items)
        self.calls = 0

    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict:
        del uid, list_id, series_type
        self.calls += 1
        start = (pn - 1) * ps
        return {
            "archives": self.items[start : start + ps],
            "meta": {"name": "测试合集"},
        }


class SmokeFakeRepo:
    """调度器场景一站式 fake：同时服务三类轮询器接口（全离线、无网络）。"""

    def __init__(
        self,
        roomid: int = 1,
        room: dict | None = None,
        dynamic_pages: list[dict] | None = None,
        collection_items: list[dict] | None = None,
    ) -> None:
        self.roomid = roomid
        self.room = dict(room or _room(0))
        self.room_info_calls = 0
        self.live_info_calls = 0
        self.dynamic_calls = 0
        self.video_calls = 0
        self.dynamic_pages = list(dynamic_pages or [])
        self.collection_items = list(collection_items or [])

    async def get_live_info(self, uid: int) -> dict:
        self.live_info_calls += 1
        return {"live_room": {"roomid": self.roomid}}

    async def get_room_info(self, room_id: int) -> dict:
        self.room_info_calls += 1
        return dict(self.room)

    async def get_dynamics(self, uid: int, offset: object = 0) -> dict:
        del offset
        self.dynamic_calls += 1
        if not self.dynamic_pages:
            return {"items": [], "has_more": 0}
        return self.dynamic_pages[(self.dynamic_calls - 1) % len(self.dynamic_pages)]

    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict:
        del uid, list_id, series_type
        self.video_calls += 1
        start = (pn - 1) * ps
        return {
            "archives": self.collection_items[start : start + ps],
            "meta": {"name": "测试合集"},
        }


def _room(
    status: int = 0,
    title: str = "冒烟标题",
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


def _page(items: list[dict], has_more: int = 0) -> dict:
    return {"items": list(items), "has_more": has_more}


def _dyn_item(dyn_id: int, title: str, desc: str = "") -> dict:
    """构造 get_dynamics_new 形状的 type=8（视频投稿）item。"""
    return {
        "id": dyn_id,
        "type": 8,
        "modules": {
            "module_author": {"name": "冒烟UP"},
            "module_dynamic": {
                "desc": {"text": desc},
                "major": {
                    "archive": {
                        "title": title,
                        "desc": desc,
                        "cover": "https://example.com/c.jpg",
                    }
                },
            },
        },
    }


def _col_item(i: int, title: str) -> dict:
    return {
        "bvid": f"BV{i:04d}",
        "aid": i,
        "pubdate": _T0 + i,
        "title": title,
        "pic": f"https://example.com/pic{i}.jpg",
    }


def _live_sub(sub_id: str = "smoke-live") -> Subscription:
    return Subscription(
        id=sub_id,
        type="live",
        name="冒烟主播",
        uid=10086,
        poll_interval_sec=5,
        push_session_ids=[_SESSION],
    )


def _dynamic_sub(sub_id: str = "smoke-dyn") -> Subscription:
    return Subscription(
        id=sub_id,
        type="dynamic",
        name="冒烟动态订阅",
        uid=10086,
        poll_interval_sec=5,
        push_session_ids=[_SESSION],
    )


def _collection_sub(sub_id: str = "smoke-col") -> Subscription:
    return Subscription(
        id=sub_id,
        type="collection",
        name="冒烟合集订阅",
        uid=10086,
        list_id=1,
        series_type=0,
        poll_interval_sec=5,
        push_session_ids=[_SESSION],
    )


def _make_scheduler(
    subs: list[Subscription],
    repo: Any,
    db: Any,
    clock: Clock,
    sleep: Any,
    poll_settings: dict[str, Any] | None = None,
    context: FakeContext | None = None,
) -> Scheduler:
    return Scheduler(
        subscriptions=subs,
        credential_cfg={},
        repo=repo,
        db=db,
        build_chain=build_chain,
        send=send,
        context=context if context is not None else FakeContext(),
        status={},
        retry_counts={},
        poll_settings=poll_settings if poll_settings is not None else {},
        now=clock,
        sleep=sleep,
    )


async def _settle(rounds: int = 20) -> None:
    """让出几次事件循环，保证任务注册/唤醒有确定性机会。"""
    for _ in range(rounds):
        await asyncio.sleep(0)


# ----------------------------------------------------------------------
# 1. 全链路直播流
# ----------------------------------------------------------------------


def test_full_chain_live_flow(tmp_path) -> None:
    """seed 静默 → 开播推送 → 1→1 不重复 → 三连 0 推下播（时长正确）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeLiveRepo(room=_room(0))
        clock = Clock()
        context = FakeContext()
        sub = _live_sub()
        status: dict = {sub.id: SimpleNamespace(last_push_at=None, last_error=None)}
        poller = LivePoller(
            subscription=sub,
            repo=repo,
            db=db,
            build_chain=build_chain,
            send=send,
            status=status,
            context=context,
            push_title_change=True,
            now=clock,
        )
        try:
            # 首轮静默 seed：反洪泛闸门——零推送。
            assert await poller.poll() is False
            assert context.sent == []
            assert repo.live_info_calls == 1  # seed 解析 room_id
            assert repo.room_info_calls == 1

            # 0→1 推开播：标题/分区/url 齐全，目标会话正确。
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

            # 1→1 无变化：不重复推送。
            assert await poller.poll() is False
            assert len(context.sent) == 1

            # 1→0 × 3：仅第 3 轮推下播，时长 = now - last_live_time。
            repo.room = _room(0)
            clock.tick(3600)
            assert await poller.poll() is False  # 0（1/3）
            assert await poller.poll() is False  # 0（2/3）
            clock.tick(3600)  # now = _T0 + 7200
            assert await poller.poll() is True  # 0（3/3）→ 下播
            assert len(context.sent) == 2
            session, chain = context.sent[1]
            assert session == _SESSION
            assert "【B站下播】" in chain
            assert "时长：7200" in chain
            assert "https://live.bilibili.com/1" in chain
            await poller.poll()  # 同一离线期不重复推
            assert len(context.sent) == 2
        finally:
            await db.close()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 2. 全链路动态流
# ----------------------------------------------------------------------


def test_full_chain_dynamic_flow(tmp_path) -> None:
    """seed（含旧动态）静默 → 第二轮新动态推送一次（跨轮去重）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeDynamicRepo(
            [_page([_dyn_item(1, "旧动态1"), _dyn_item(2, "旧动态2")])]
        )
        context = FakeContext()
        sub = _dynamic_sub()
        status: dict = {sub.id: SimpleNamespace(last_push_at=None, last_error=None)}
        poller = DynamicPoller(
            subscription=sub,
            repo=repo,
            db=db,
            build_chain=build_chain,
            send=send,
            context=context,
            status=status,
            retry_counts={},
        )
        try:
            # 首轮 seed：两条旧动态静默标记，零推送。
            await poller.poll()
            assert context.sent == []
            assert await db.get_seeded("dynamic_state_v2", sub.id) is True

            # 第二轮注入新动态 → 推送一次。
            repo.pages = [_page([_dyn_item(100, "新视频", desc="新视频正文")])]
            await poller.poll()
            assert len(context.sent) == 1
            session, chain = context.sent[0]
            assert session == _SESSION
            assert isinstance(chain, str)
            assert "【B站动态】" in chain
            assert "视频投稿" in chain
            assert "新视频" in chain
            assert "https://t.bilibili.com/100" in chain

            # 第三轮：无新动态 → 不重复推送。
            await poller.poll()
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 3. 全链路合集流
# ----------------------------------------------------------------------


def test_full_chain_collection_flow(tmp_path) -> None:
    """seed（含旧 bvid）静默 → 追加新 bvid 推送一次（跨轮去重）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeCollectionRepo([_col_item(i, f"旧视频{i}") for i in range(5)])
        context = FakeContext()
        sub = _collection_sub()
        status: dict = {sub.id: SimpleNamespace(last_push_at=None, last_error=None)}
        poller = CollectionPoller(
            subscription=sub,
            repo=repo,
            db=db,
            build_chain=build_chain,
            send=send,
            context=context,
            status=status,
            retry_counts={},
        )
        try:
            # 首轮 seed：5 条 bvid 静默标记，零推送。
            await poller.poll()
            assert context.sent == []
            assert await db.get_seeded("collection_state_v2", sub.id) is True

            # 追加一个新视频 → 推送一次。
            repo.items.append(_col_item(5, "新视频"))
            await poller.poll()
            assert len(context.sent) == 1
            session, chain = context.sent[0]
            assert session == _SESSION
            assert isinstance(chain, str)
            assert "【B站合集更新】" in chain
            assert "新视频" in chain
            assert "合集：测试合集" in chain
            assert "https://www.bilibili.com/video/BV0005" in chain

            # 再轮：无新增 → 不重复推送。
            await poller.poll()
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 4. 调度器集成：三类订阅同调度，变更一轮内全部推送
# ----------------------------------------------------------------------


def test_scheduler_drives_all_types_end_to_end() -> None:
    """真实 Scheduler + InstantDb + 受控时钟：seed 静默 → 注入变更 → 三类各
    推一次 → 直播转离线 ×3 推下播（时长正确）→ stop() 干净。"""

    async def scenario() -> None:
        clock = Clock()
        sleep = ControlledSleep(clock)
        repo = SmokeFakeRepo(
            room=_room(0),
            dynamic_pages=[_page([_dyn_item(1, "旧动态1"), _dyn_item(2, "旧动态2")])],
            collection_items=[_col_item(i, f"旧视频{i}") for i in range(5)],
        )
        context = FakeContext()
        scheduler = _make_scheduler(
            [_live_sub(), _dynamic_sub(), _collection_sub()],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
            context=context,
        )
        scheduler.start()
        try:
            # 两轮 advance 让三类完成 seed（合集首轮被桶补牌延迟一轮），
            # 期间零推送——反洪泛闸门。
            await _settle()  # 三个类型任务先注册首轮睡眠，再开始受控推进
            await sleep.advance(5)
            await _settle()
            await sleep.advance(5)
            await _settle()
            assert context.sent == []

            # 注入变更：直播开播、新动态、新视频。
            repo.room = _room(1, title="开播啦", area="音乐")
            repo.dynamic_pages = [_page([_dyn_item(100, "新视频", desc="新视频正文")])]
            repo.collection_items.append(_col_item(5, "新视频"))
            await sleep.advance(5)
            await _settle()
            # 一轮内三类各推一次，目标会话均为 _SESSION。
            assert len(context.sent) == 3
            assert all(session == _SESSION for session, _ in context.sent)
            texts = [str(chain) for _, chain in context.sent]
            live_on = next(t for t in texts if "【B站开播】" in t)
            dyn = next(t for t in texts if "【B站动态】" in t)
            col = next(t for t in texts if "【B站合集更新】" in t)
            assert "开播啦" in live_on
            assert "音乐" in live_on
            assert "https://live.bilibili.com/1" in live_on
            assert "新视频" in dyn
            assert "https://t.bilibili.com/100" in dyn
            assert "新视频" in col
            assert "https://www.bilibili.com/video/BV0005" in col

            # 直播转离线 ×3：前两轮不推，第 3 轮推下播（时长 = 30）。
            repo.room = _room(0)
            await sleep.advance(5)
            await _settle()  # 0（1/3）
            await sleep.advance(5)
            await _settle()  # 0（2/3）
            assert all("【B站下播】" not in str(chain) for _, chain in context.sent)
            await sleep.advance(5)
            await _settle()  # 0（3/3）→ 下播
            offline = next(
                str(chain) for _, chain in context.sent if "【B站下播】" in str(chain)
            )
            assert "时长：30" in offline
            assert "https://live.bilibili.com/1" in offline
        finally:
            await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 5. 无网络断言：fake repository 被注入
# ----------------------------------------------------------------------


def test_no_real_network_fake_repo_injected() -> None:
    """Scheduler 收到注入的 fake（SdkRepository 从未构造），三类 poller 均持
    同一 fake，调用计数 > 0；``bilibili_api`` 全程未导入。"""

    async def scenario() -> None:
        clock = Clock()
        sleep = ControlledSleep(clock)
        repo = SmokeFakeRepo()
        scheduler = _make_scheduler(
            [_live_sub(), _dynamic_sub(), _collection_sub()],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler.start()
        try:
            await _settle()
            await sleep.advance(5)
            await _settle()
            await sleep.advance(5)
            await _settle()
            # fake 被真实使用：三类接口均有调用。
            assert repo.live_info_calls > 0
            assert repo.room_info_calls > 0
            assert repo.dynamic_calls > 0
            assert repo.video_calls > 0
            # 注入的 fake 是唯一 repository：SdkRepository 从未被构造。
            assert scheduler._repo is repo
            for poller in scheduler.pollers.values():
                assert poller.repo is repo
        finally:
            await scheduler.stop()

    asyncio.run(scenario())

    # 全程无 bilibili_api 导入：repository.bili 的守卫导入在 SDK 缺失时静默
    # 失败，故 SdkRepository 构造会抛 BiliError——假仓库避免了该路径。
    assert "bilibili_api" not in sys.modules
