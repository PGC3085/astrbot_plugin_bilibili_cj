"""动态轮询器单元测试（计划 todo 7）。

全部离线：fake repository（脚本化分页）、临时 SQLite（``tmp_path``）、
记录式 fake context、真实 ``push.build_chain``/``push.send``（离线 str 模式）。
每个用例在单个 ``asyncio.run`` 内完成 init → 轮询 → close（aiosqlite 连接
不跨 event loop 复用）。

覆盖计划验收点：

1. 重复 dynamic_id 只推一次（同轮重复 + 跨轮重复）；2. 首轮静默 seed 不推送、
   第二轮新动态才推送；3. type=8 命中"视频投稿"模板；4. 未知类型走
   "发布新动态" 通用兜底；5. ``has_more`` 恒真触 10 页上限：告警、不崩溃、
   旧项不重复推、新项只推一次；6. 全失败 mark-after-send 重试计数 1→2→3，
   3 轮后标记并告警；7. seed 持久化：重建 poller 不重 seed，停机期新动态
   照常推送；8. 2 页 × 3 条翻页全量扫描去重、offset 透传；9. 空动态流不推送
   不报错；10. 仓库 ``BiliNetworkError`` 吞掉记日志（另有未知异常吞掉、
   ``CancelledError`` 透传、类型码表守卫三个补充用例）。
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace
from uuid import uuid4

import pytest

from config import Subscription
from db import Database
from poller.dynamic import DYNAMIC_TYPE_HANDLERS, DynamicPoller
from push import build_chain, format_event_time, send
from repository import BiliNetworkError

_SESSION = "aiocqhttp:GroupMessage:123"


class FakeRepo:
    """脚本化动态 feed fake：按调用序号返回 canned 页（越界复用最后一页）。

    ``calls`` 记录每次 ``(uid, offset)``；``error`` 设置后在下次
    ``get_dynamics`` 抛出一次即清空。
    """

    def __init__(self, pages: list[dict]) -> None:
        self.pages = list(pages)
        self.calls: list[tuple[int, object]] = []
        self.error: Exception | None = None

    async def get_dynamics(self, uid: int, offset: object = 0) -> dict:
        self.calls.append((uid, offset))
        if self.error is not None:
            exc, self.error = self.error, None
            raise exc
        if not self.pages:
            return {"items": [], "has_more": 0}
        # 每轮都从最新（offset=0）开始：按调用序号对页列表取模，循环复用。
        return self.pages[(len(self.calls) - 1) % len(self.pages)]


class FakeContext:
    """记录 ``send_message`` 调用；默认全部投递成功。"""

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
    logger = logging.getLogger(f"test_dynamic_poller.{uuid4().hex}")
    logger.propagate = False
    handler = _RecordListHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, handler.records


def _page(items: list[dict], has_more: int = 0, offset: str = "") -> dict:
    return {"items": list(items), "has_more": has_more, "offset": offset}


def _item(
    dyn_id: int,
    type_: int,
    title: str = "",
    desc: str = "",
    cover: str = "",
    major_key: str = "archive",
) -> dict:
    """构造 get_dynamics_new 形状的 item（major 按类型码填充）。"""
    major: dict = {}
    if type_ == 2:
        major["draw"] = {"title": title, "items": [{"src": cover}] if cover else []}
    elif type_ != 1 and type_ != 4:
        major[major_key] = {"title": title, "desc": desc, "cover": cover}
    return {
        "id": dyn_id,
        "type": type_,
        "modules": {
            "module_author": {"name": "测试UP"},
            "module_dynamic": {"desc": {"text": desc}, "major": major},
        },
    }


def _make_subscription(sub_id: str = "sub-1") -> Subscription:
    return Subscription(
        id=sub_id,
        type="dynamic",
        name="测试动态订阅",
        uid=10086,
        push_session_ids=[_SESSION],
    )


def _make_poller(
    repo: FakeRepo,
    db: Database,
    context: FakeContext | None = None,
    retry_counts: dict | None = None,
    logger: logging.Logger | None = None,
    subscription: Subscription | None = None,
    push_cover: bool = True,
) -> tuple[DynamicPoller, dict]:
    status: dict = {
        subscription.id if subscription else "sub-1": SimpleNamespace(
            last_push_at=None, last_error=None
        )
    }
    poller = DynamicPoller(
        subscription=subscription or _make_subscription(),
        repo=repo,
        db=db,
        build_chain=build_chain,
        send=send,
        context=context or FakeContext(),
        status=status,
        retry_counts=retry_counts if retry_counts is not None else {},
        logger=logger,
        push_cover=push_cover,
    )
    return poller, status


def test_duplicate_dynamic_id_pushed_once(tmp_path) -> None:
    """1. 重复 dynamic_id 只推一次（同轮重复 + 跨轮重复）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed（空 feed，静默）
            assert context.sent == []
            repo.pages = [_page([_item(100, 8, title="A"), _item(100, 8, title="A")])]
            await poller.poll()  # 同轮出现两次 → 只推一次
            assert len(context.sent) == 1
            await poller.poll()  # 跨轮重复 → 不再推
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_first_round_silent_then_push_new(tmp_path) -> None:
    """2. 首轮静默 seed 不推送；第二轮新动态才推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()
            assert context.sent == []
            assert await db.get_seeded("dynamic_state_v2", "sub-1")
            repo.pages = [_page([_item(100, 8, title="新视频")])]
            await poller.poll()
            assert len(context.sent) == 1
            assert "新视频" in str(context.sent[0][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_type_8_hits_video_template(tmp_path) -> None:
    """3. type=8 命中"视频投稿"模板：type_text + 标题 + 链接 + UP 名。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed
            repo.pages = [
                _page(
                    [
                        _item(
                            100,
                            8,
                            title="标题A",
                            desc="简介A",
                            cover="https://example.com/c.jpg",
                        )
                    ]
                )
            ]
            await poller.poll()
            assert len(context.sent) == 1
            assert context.sent[0][0] == _SESSION
            text = str(context.sent[0][1])
            assert "视频投稿" in text
            assert "标题A" in text
            assert "https://t.bilibili.com/100" in text
            assert "测试UP" in text
        finally:
            await db.close()

    asyncio.run(scenario())


def test_unknown_type_generic_fallback(tmp_path) -> None:
    """4. 未知类型走"发布新动态"通用文案，正文回退 desc 文本。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed
            repo.pages = [_page([_item(100, 999, desc="神秘动态正文")])]
            await poller.poll()
            assert len(context.sent) == 1
            text = str(context.sent[0][1])
            assert "发布新动态" in text
            assert "神秘动态正文" in text
            assert "https://t.bilibili.com/100" in text
        finally:
            await db.close()

    asyncio.run(scenario())


def test_has_more_cap_warns_and_dedup_idempotent(tmp_path) -> None:
    """5. has_more 恒真：触 10 页上限告警不崩溃；旧项不重复推、新项只推一次。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(
            [_page([_item(100, 8, title="旧1"), _item(101, 4, desc="旧2")])]
        )
        context = FakeContext()
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()  # seed，1 页即止
            assert len(repo.calls) == 1
            repo.pages = [
                _page(
                    [_item(100, 8), _item(101, 4), _item(102, 4, desc="新3")],
                    has_more=1,
                    offset="next",
                )
            ]
            await poller.poll()  # 10 页上限：告警；仅新项被推
            assert len(context.sent) == 1
            assert "新3" in str(context.sent[0][1])
            assert len(repo.calls) == 11
            assert any("上限" in r.getMessage() for r in records)
            await poller.poll()  # 再来一轮：旧项不重复、新项已见
            assert len(context.sent) == 1
            assert len(repo.calls) == 21
            assert await db.get_seeded("dynamic_state_v2", "sub-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_all_fail_retry_then_mark_and_warn(tmp_path) -> None:
    """6. mark-after-send：全失败重试计数 1→2→3，3 轮后标记并告警。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext(ok=False)
        retry_counts: dict = {}
        logger, records = _recording_logger()
        poller, _ = _make_poller(
            repo, db, context=context, retry_counts=retry_counts, logger=logger
        )
        try:
            await poller.poll()  # seed（空 feed）
            repo.pages = [_page([_item(100, 8, title="A")])]
            await poller.poll()  # 首次推送失败 → 计数 1
            assert len(context.sent) == 1
            assert retry_counts["sub-1"]["100"] == 1
            await poller.poll()  # 重试 1 → 计数 2
            assert len(context.sent) == 2
            assert retry_counts["sub-1"]["100"] == 2
            await poller.poll()  # 重试 2 → 达上限：告警 + 清除计数（仍标记）
            assert len(context.sent) == 3
            assert "100" not in retry_counts["sub-1"]
            assert any("已标记为已见" in r.getMessage() for r in records)
            await poller.poll()  # 第 4 轮：不再尝试
            assert len(context.sent) == 3
        finally:
            await db.close()

    asyncio.run(scenario())


def test_seed_persisted_new_poller_does_not_reseed(tmp_path) -> None:
    """7. seed 持久化：重建 poller 不重 seed，停机期新动态照常推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([_item(i, 4, desc=f"旧{i}") for i in range(100, 105)])])
        retry_counts: dict = {}
        context1 = FakeContext()
        poller1, _ = _make_poller(repo, db, context=context1, retry_counts=retry_counts)
        try:
            await poller1.poll()  # seed 5 条，静默
            assert context1.sent == []
            repo.pages = [_page([_item(105, 8, title="停机期新动态")])]
            context2 = FakeContext()
            poller2, _ = _make_poller(
                repo, db, context=context2, retry_counts=retry_counts
            )
            await poller2.poll()  # 未重 seed → 停机期新动态被推送
            assert len(context2.sent) == 1
            assert "停机期新动态" in str(context2.sent[0][1])
            await poller2.poll()  # 不重复
            assert len(context2.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pagination_two_pages_six_items(tmp_path) -> None:
    """8. 2 页 × 3 条：6 条全量扫描去重，每页 offset 透传仓库。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo(
            [
                _page(
                    [_item(i, 4, desc=f"t{i}") for i in range(3)],
                    has_more=1,
                    offset="page2",
                ),
                _page([_item(i, 4, desc=f"t{i}") for i in range(3, 6)]),
            ]
        )
        await db.set_seeded(
            "dynamic_state_v2", "sub-1", True
        )  # 直接置 seed（等价空 seed）
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # 两页 6 条全部推送一次，offset 透传
            assert len(context.sent) == 6
            assert repo.calls == [(10086, 0), (10086, "page2")]
            urls = {str(chain).split("链接：")[-1] for _, chain in context.sent}
            assert urls == {f"https://t.bilibili.com/{i}" for i in range(6)}
            await poller.poll()  # 全已见 → 不重复
            assert len(context.sent) == 6
            assert len(repo.calls) == 4
        finally:
            await db.close()

    asyncio.run(scenario())


def test_empty_feed_no_push(tmp_path) -> None:
    """9. 空动态流：seed 置位，任何轮次不推送不报错。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()
            await poller.poll()
            assert context.sent == []
            assert await db.get_seeded("dynamic_state_v2", "sub-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_repo_network_error_swallowed(tmp_path) -> None:
    """10. BiliNetworkError 被吞掉记日志；恢复后下一轮正常 seed。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([_item(100, 8, title="A")])])
        repo.error = BiliNetworkError("网络炸了")
        context = FakeContext()
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()  # 不抛异常
            assert context.sent == []
            assert any("动态轮询失败" in r.getMessage() for r in records)
            await poller.poll()  # 恢复：正常 seed
            assert context.sent == []
            assert await db.get_seeded("dynamic_state_v2", "sub-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_unexpected_repo_error_swallowed(tmp_path) -> None:
    """补充：非 Bili 异常同样吞掉记日志，不崩溃。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([_item(100, 8, title="A")])])
        repo.error = RuntimeError("boom")
        context = FakeContext()
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()
            assert context.sent == []
            assert any("动态轮询异常" in r.getMessage() for r in records)
        finally:
            await db.close()

    asyncio.run(scenario())


def test_cancelled_error_re_raised(tmp_path) -> None:
    """补充：CancelledError 透传（任务取消不被吞成"网络错误"）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([_item(100, 8, title="A")])])
        repo.error = asyncio.CancelledError()
        poller, _ = _make_poller(repo, db)
        try:
            with pytest.raises(asyncio.CancelledError):
                await poller.poll()
        finally:
            await db.close()

    asyncio.run(scenario())


def test_handler_table_covers_required_types() -> None:
    """补充：计划规定的全部类型码在 DYNAMIC_TYPE_HANDLERS 中（回归闸）。"""

    for code in (8, 2, 4, 1, 64, 256, 512, 2048, 4200, 4308, 4300, 4302, 4310):
        assert code in DYNAMIC_TYPE_HANDLERS
    assert DYNAMIC_TYPE_HANDLERS[8][0] == "视频投稿"
    assert DYNAMIC_TYPE_HANDLERS[2][0] == "图片"
    assert DYNAMIC_TYPE_HANDLERS[4][0] == "文字"
    assert DYNAMIC_TYPE_HANDLERS[1][0] == "转发"
    assert DYNAMIC_TYPE_HANDLERS[64][0] == "专栏"
    assert DYNAMIC_TYPE_HANDLERS[256][0] == "音频"
    assert DYNAMIC_TYPE_HANDLERS[512][0] == "番剧"
    assert DYNAMIC_TYPE_HANDLERS[2048][0] == "图文"
    assert DYNAMIC_TYPE_HANDLERS[4200][0] == "直播分享"
    assert DYNAMIC_TYPE_HANDLERS[4308][0] == "直播分享"
    assert DYNAMIC_TYPE_HANDLERS[4300][0] == "收藏夹"
    assert DYNAMIC_TYPE_HANDLERS[4302][0] == "课程"
    assert DYNAMIC_TYPE_HANDLERS[4310][0] == "合集更新"


def test_dynamic_payload_includes_event_time(tmp_path) -> None:
    """新 API 的 module_author.pub_ts 会格式化进 payload.event_time。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        poller, _ = _make_poller(repo, db)
        try:
            item = {
                "id": "100",
                "type": 4,
                "modules": {
                    "module_author": {"name": "UP", "pub_ts": 1_700_000_000},
                    "module_dynamic": {"desc": {"text": "正文"}},
                },
            }
            payload = poller._payload(poller.subscription, item, "100", 4)
            assert payload["event_time"] == format_event_time(1_700_000_000)
        finally:
            await db.close()

    asyncio.run(scenario())


def _polymer_item(
    dyn_id: int,
    type_str: str,
    *,
    title: str = "",
    desc: str = "",
    cover: str = "",
) -> dict:
    """构造 polymer feed/space 形状的 item（``id_str`` + 字符串枚举 ``type``）。"""
    major: dict = {}
    if type_str == "DYNAMIC_TYPE_AV":
        major = {"archive": {"title": title, "desc": desc, "cover": cover}}
        major["type"] = "MAJOR_TYPE_ARCHIVE"
    elif type_str == "DYNAMIC_TYPE_DRAW":
        major = {"draw": {"title": title, "items": [{"src": cover}] if cover else []}}
        major["type"] = "MAJOR_TYPE_DRAW"
    elif type_str == "DYNAMIC_TYPE_WORD" and title:
        # 图文：WORD + MAJOR_TYPE_OPUS
        major = {"opus": {"title": title, "summary": desc, "pics": []}}
        major["type"] = "MAJOR_TYPE_OPUS"
    elif type_str == "DYNAMIC_TYPE_LIVE_RCMD":
        major = {"live_rcmd": {"title": title, "desc": desc, "cover": cover}}
        major["type"] = "MAJOR_TYPE_LIVE_RCMD"
    elif type_str == "DYNAMIC_TYPE_UGC_SEASON":
        major = {"ugc_season": {"title": title, "desc": desc, "cover": cover}}
        major["type"] = "MAJOR_TYPE_UGC_SEASON"
    return {
        "id_str": str(dyn_id),
        "type": type_str,
        "modules": {
            "module_author": {"name": "测试UP", "pub_ts": 1_700_000_000},
            "module_dynamic": {"desc": {"text": desc}, "major": major},
        },
    }


def test_polymer_item_id_and_type_parsed(tmp_path) -> None:
    """polymer feed/space 形状：id_str 提取、字符串枚举 type 映射正确。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        poller, _ = _make_poller(FakeRepo([_page([])]), db)
        try:
            av = _polymer_item(100, "DYNAMIC_TYPE_AV", title="视频A", desc="简介A")
            assert poller._dynamic_id(av) == "100"
            assert poller._dynamic_type(av) == 8
            draw = _polymer_item(101, "DYNAMIC_TYPE_DRAW", cover="https://x/1.jpg")
            assert poller._dynamic_id(draw) == "101"
            assert poller._dynamic_type(draw) == 2
            word = _polymer_item(102, "DYNAMIC_TYPE_WORD", desc="纯文字")
            assert poller._dynamic_type(word) == 4
            opus = _polymer_item(103, "DYNAMIC_TYPE_WORD", title="图文", desc="正文")
            assert poller._dynamic_type(opus) == 2048  # WORD + MAJOR_TYPE_OPUS → 图文
            forward = _polymer_item(104, "DYNAMIC_TYPE_FORWARD", desc="转发语")
            assert poller._dynamic_type(forward) == 1
            live_rcmd = _polymer_item(105, "DYNAMIC_TYPE_LIVE_RCMD", desc="开播了")
            assert poller._dynamic_type(live_rcmd) == 4308
            medialist = _polymer_item(106, "DYNAMIC_TYPE_MEDIALIST", desc="收藏夹")
            assert poller._dynamic_type(medialist) == 4300
            ugc = _polymer_item(107, "DYNAMIC_TYPE_UGC_SEASON", desc="合集更新")
            assert poller._dynamic_type(ugc) == 4310
        finally:
            await db.close()

    asyncio.run(scenario())


def test_polymer_av_dynamic_pushed_end_to_end(tmp_path) -> None:
    """polymer 形状的投稿视频动态能正常 seed → 检测 → 推送（回归闸）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_page([])])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 空
            assert context.sent == []
            repo.pages = [
                _page(
                    [
                        _polymer_item(
                            100,
                            "DYNAMIC_TYPE_AV",
                            title="新视频A",
                            desc="简介A",
                            cover="https://example.com/c.jpg",
                        )
                    ]
                )
            ]
            await poller.poll()
            assert len(context.sent) == 1
            assert context.sent[0][0] == _SESSION
            text = str(context.sent[0][1])
            assert "视频投稿" in text
            assert "新视频A" in text
            assert "https://t.bilibili.com/100" in text
            assert "测试UP" in text
            # 跨轮去重：不再重复推送
            await poller.poll()
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_v1_seeded_state_reseeded_silently_no_flood(tmp_path) -> None:
    """v1 解析缺陷留下的「seeded=1 但无去重记录」状态：升级后首轮静默重
    seed（记录当前可见内容、不推送），第二轮仅推送真正新增的动态——根治
    升级后的历史内容洪水推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        # 模拟缺陷时代的遗留状态：v1 表已置位，但 known_dynamics 为空
        await db.set_seeded("dynamic_state", "sub-1", True)
        repo = FakeRepo(
            [_page([_item(100, 8, title="旧视频"), _item(101, 4, desc="旧文字")])]
        )
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # v2 未置位 → 静默重 seed，绝不推送历史
            assert context.sent == []
            assert await db.get_seeded("dynamic_state_v2", "sub-1") is True

            repo.pages = [_page([_item(102, 8, title="新视频")])]
            await poller.poll()  # 仅推送新的一条
            assert len(context.sent) == 1
            assert "新视频" in str(context.sent[0][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_dynamic_payload_cover_gated_by_setting(tmp_path) -> None:
    """push_cover=False 时动态载荷不携带封面；缺省 True 时携带。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        item = _item(100, 8, title="A", cover="https://example.com/c.jpg")

        poller_on, _ = _make_poller(FakeRepo([_page([])]), db)
        payload = poller_on._payload(poller_on.subscription, item, "100", 8)
        assert payload["cover"] == "https://example.com/c.jpg"

        poller_off, _ = _make_poller(FakeRepo([_page([])]), db, push_cover=False)
        payload = poller_off._payload(poller_off.subscription, item, "100", 8)
        assert "cover" not in payload
        await db.close()

    asyncio.run(scenario())
