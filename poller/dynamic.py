"""动态轮询器：静默 seed + 空 offset 扫描推送新动态（计划 todo 7）。

- 每轮从空 offset（最新在前）拉取，翻页直到 ``has_more`` 为假或触
  :data:`_MAX_PAGES` 页上限（防失控；触上限记告警）。
- 首轮静默 seed：全部 ``dynamic_id`` 写入 ``known_dynamics``（不推送），
  seed 标志持久化到 ``dynamic_state.seeded``——重启恢复，避免重启后重 seed
  吞掉停机期新动态；即使首轮触页上限也照常置位。
- seed 后每轮只推送新项，按 ``(sub_id, dynamic_id)`` 经
  ``insert_dynamic_if_new`` 去重（PK 幂等吸收重复）。
- mark-after-send：任一 session 成功即视为已见；全部失败由
  ``retry_counts[sub_id][dynamic_id]``（main.py 持有、跨重建保留）计数重试，
  达 :data:`_MAX_RETRY_ROUNDS` 轮上限后仍标记并告警（行保留即"仍标记"）。
- 消息构造遵循 DDBOT ``notify.group.bilibili.news.tmpl`` 原逻辑：
  ``{name}{动作短语}：``（``发布了新动态`` / ``投稿了视频`` / ``发布了新专栏``
  / ``投稿了新音频`` / ``发表了新动态`` / ``发布了直播信息`` / ``发布了收藏夹``
  ...）+ 正文 + 类型专属行（视频的标题/简介、专栏的标题/摘要、音频的
  标题/简介/作者、图文的标题/正文、直播与收藏夹的标题、课程的徽章与标题）；
  转发动态取 ``orig`` 的作者与内容，按被转发类型选用转发句式（``转发了Y的
  视频：`` / ``分享了Y的直播：`` ...）与 ``原动态：`` / ``原视频：`` 等标注行，
  其余未知类型走通用 ``发布了新动态``。
- 字段提取兼容两代接口：新 polymer API（``id_str`` 动态 id、字符串枚举
  ``type``（``DYNAMIC_TYPE_*``）、``modules.module_dynamic.desc`` /
  ``major.*``、转发动态的 ``orig`` 子条目）与旧 API（``desc`` / ``card``
  JSON、整数 ``type``、``desc.orig_type``），缺失键返回空串；纯文字与图文
  共用 ``DYNAMIC_TYPE_WORD``，按 ``major.type`` 区分。
- 仓库/未知异常吞掉记日志；``asyncio.CancelledError`` 透传。

``build_chain`` / ``send`` 由 main.py 注入 push 模块实现（离线测试可替换）。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

try:
    from ..config import Subscription
    from ..db import Database
    from ..push import format_event_time
    from ..repository import BiliError, BiliRepository
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from push import format_event_time  # type: ignore[import-not-found]
    from repository import BiliError, BiliRepository  # type: ignore[import-not-found]

#: 单轮扫描的页数上限（超出即停止翻页并告警）。
_MAX_PAGES: int = 10
#: 推送全失败后的最大重试轮数，达上限后仍标记为已见并告警。
_MAX_RETRY_ROUNDS: int = 3
#: seed 标志所在持久化表（v2 代标记：解析缺陷修复后强制重 seed，避免洪水推送）。
_SEED_TABLE: str = "dynamic_state_v2"
#: 动态详情页链接模板（DDBOT DynamicUrl 同款，t.bilibili.com/<dynamic_id>）。
_DYNAMIC_URL_TMPL: str = "https://t.bilibili.com/{}"
#: 未知动态类型的通用动作短语（DDBOT tmpl default 分支同款）。
_DEFAULT_ACTION: str = "发布了新动态"

#: 新 API（polymer feed/space）动态类型字符串枚举 → 内部类型码（沿用旧 API 整数码）。
_DYNAMIC_TYPE_ENUM_MAP: dict[str, int] = {
    "DYNAMIC_TYPE_AV": 8,
    "DYNAMIC_TYPE_DRAW": 2,
    "DYNAMIC_TYPE_WORD": 4,
    "DYNAMIC_TYPE_FORWARD": 1,
    "DYNAMIC_TYPE_ARTICLE": 64,
    "DYNAMIC_TYPE_MUSIC": 256,
    "DYNAMIC_TYPE_PGC": 512,
    "DYNAMIC_TYPE_LIVE": 4200,
    "DYNAMIC_TYPE_LIVE_RCMD": 4308,
    "DYNAMIC_TYPE_MEDIALIST": 4300,
    "DYNAMIC_TYPE_COMMON_SQUARE": 4300,
    "DYNAMIC_TYPE_COURSES": 4302,
    "DYNAMIC_TYPE_COURSES_SEASON": 4302,
    "DYNAMIC_TYPE_COURSES_BATCH": 4302,
    "DYNAMIC_TYPE_UGC_SEASON": 4310,
    "DYNAMIC_TYPE_NONE": 0,
}

_logger: logging.Logger | None = None


async def _noop_acquire() -> None:
    """默认无操作取牌：未注入令牌桶时行为与之前完全一致。"""
    return None


def _get_logger() -> logging.Logger:
    """返回插件统一 logger；离线环境回退 stdlib logger。"""
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger(__name__)
        else:
            _logger = astrbot_logger
    return _logger


def _dig(item: dict[str, Any], *keys: str) -> Any:
    """沿 ``keys`` 逐层取嵌套值；任一层缺失返回 ``None``。"""
    current: Any = item
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _text(value: Any) -> str:
    """标量转去除首尾空白的文本；``None``/容器返回空串。"""
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value).strip()


def _join(*parts: str) -> str:
    """用换行连接非空片段。"""
    return "\n".join(p for p in parts if p)


def _major(item: dict[str, Any], major_key: str) -> dict[str, Any]:
    """取新 API ``modules.module_dynamic.major.<major_key>``；缺失返回空 dict。"""
    major = _dig(item, "modules", "module_dynamic", "major", major_key)
    return major if isinstance(major, dict) else {}


def _card(item: dict[str, Any]) -> dict[str, Any]:
    """解析旧 API 的 ``card`` JSON 字符串为 dict；非字符串/解析失败返回空 dict。"""
    card = item.get("card")
    if not isinstance(card, str):
        return {}
    try:
        parsed = json.loads(card)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _title_body_cover(
    item: dict[str, Any], major_keys: tuple[str, ...], body_keys: tuple[str, ...]
) -> tuple[str, str, str]:
    """统一提取 (标题, 正文, 封面)：按候选 key 依次尝试，新 API ``major`` 优先，旧 API ``card`` 兜底。

    同一内部类型码可能对应多种 polymer major 子键（如直播分享同时存在
    ``live`` / ``live_rcmd``），因此 ``major_keys`` 为候选列表；旧 API 的
    卡片 JSON 可能直接平铺在被转发条目上（无 ``card`` 包装），故最后再
    回退条目顶层同名键。
    """
    card = _card(item)
    for major_key in major_keys:
        major = _major(item, major_key)
        title = _text(major.get("title") or card.get("title") or item.get("title"))
        body = ""
        for key in body_keys:
            value = major.get(key) or card.get(key) or item.get(key)
            if value:
                body = _text(value)
                break
        cover = _text(
            major.get("cover")
            or major.get("pic")
            or card.get("pic")
            or card.get("cover")
            or item.get("pic")
        )
        if title or body or cover:
            return title, body, cover
    return "", "", ""


def _label(key: str, value: str) -> str:
    """``键：值`` 标签行（DDBOT 的 标题：/简介： 等）；值为空返回空串。"""
    return f"{key}：{value}" if value else ""


def _text_or_inner(value: Any) -> str:
    """文本或容器内嵌文本（如 opus 的 ``summary`` 为 ``{"text": ...}``）。"""
    if isinstance(value, dict):
        value = value.get("text") or value.get("summary")
    return _text(value)


def _pics_cover(
    item: dict[str, Any], major: dict[str, Any], card: dict[str, Any]
) -> str:
    """取多图类型首图封面：``pics``/``covers``/``items`` 列表首元素，
    兼容旧 API ``card.item.pictures`` 与平铺被转发条目 ``item.pictures``。"""
    for key in ("pics", "covers", "items"):
        source = major.get(key)
        if not isinstance(source, list):
            source = card.get(key)
        if isinstance(source, list) and source and isinstance(source[0], dict):
            return _text(
                source[0].get("src")
                or source[0].get("url")
                or source[0].get("img_src")
                or source[0].get("cover")
            )
    for source in (card.get("item"), item.get("item")):
        if isinstance(source, dict):
            pics = source.get("pictures") or source.get("pics")
            if isinstance(pics, list) and pics and isinstance(pics[0], dict):
                return _text(pics[0].get("img_src") or pics[0].get("src"))
    return ""


def _extract_body_cover(
    item: dict[str, Any], desc: str, type_: int, marker: str = ""
) -> tuple[str, str]:
    """DDBOT ``news.tmpl`` 同款按类型组装 (body, cover)。

    body 为正文 + 类型专属行；``marker`` 非空时作为首行（转发动态的
    ``原动态：`` / ``原视频：`` / ``原专栏：`` / ``原音频：`` / ``原直播间：`` /
    ``原收藏夹：`` 标注行，DDBOT 同款）。
    """
    if type_ == 8:  # 视频投稿：正文 + 标题/简介
        title, extra, cover = _title_body_cover(item, ("archive",), ("desc", "dynamic"))
        return _join(marker, desc, _label("标题", title), _label("简介", extra)), cover
    if type_ == 2:  # 图片动态：正文 + 首图封面
        major = _major(item, "draw")
        return _join(marker, desc), _pics_cover(item, major, _card(item))
    if type_ == 64:  # 专栏：正文 + 标题/摘要
        major = _major(item, "article")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        summary = _text(major.get("summary") or card.get("summary") or card.get("desc"))
        cover = _pics_cover(item, major, card) or _text(
            major.get("cover") or card.get("pic")
        )
        return _join(
            marker, desc, _label("标题", title), _label("摘要", summary)
        ), cover
    if type_ == 256:  # 音频：正文 + 标题/简介/作者
        major = _major(item, "music")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        intro = _text(major.get("intro") or card.get("intro"))
        author = _text(major.get("author") or card.get("author"))
        cover = _text(major.get("cover") or card.get("cover"))
        return (
            _join(
                marker,
                desc,
                _label("标题", title),
                _label("简介", intro),
                _label("作者", author),
            ),
            cover,
        )
    if type_ == 512:  # 番剧/影视：正文 + 标题
        major = _major(item, "pgc")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        cover = _text(major.get("cover") or card.get("cover") or card.get("pic"))
        return _join(marker, desc, _label("标题", title)), cover
    if type_ == 2048:  # 图文：正文 + 标题/正文
        major = _major(item, "opus")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        summary = _text_or_inner(
            major.get("summary") or card.get("summary") or card.get("desc")
        )
        cover = _pics_cover(item, major, card) or _text(
            major.get("cover") or card.get("pic")
        )
        return _join(marker, desc, title, summary), cover
    if type_ in (4200, 4308):  # 直播分享：正文 + 直播间标题
        title, cover = "", ""
        for key in ("live", "live_rcmd"):
            major = _major(item, key)
            if not major:
                continue
            title = _text(major.get("title"))
            cover = _text(major.get("cover") or major.get("pic"))
            if not title:
                info = major.get("live_play_info")
                if isinstance(info, dict):
                    title = _text(info.get("title"))
                    cover = _text(info.get("cover")) or cover
            if title or cover:
                break
        return _join(marker, desc, title), cover
    if type_ == 4300:  # 收藏夹：正文 + 标题
        title, _, cover = _title_body_cover(
            item, ("medialist", "mylist", "common"), ("desc", "title")
        )
        return _join(marker, desc, title), cover
    if type_ == 1024:  # 已消失内容：正文 + 提示
        card = _card(item)
        tips = ""
        inner = card.get("item") or item.get("item")
        if isinstance(inner, dict):
            tips = _text(inner.get("tips"))
        return _join(marker, desc, tips), ""
    if type_ == 4310:  # 合集更新：正文 + 合集名
        title, _, cover = _title_body_cover(item, ("ugc_season",), ("desc",))
        return _join(marker, desc, title), cover
    # 文字(4)/未知类型：仅正文。
    return _join(marker, desc), ""


def _course_parts(item: dict[str, Any], desc: str) -> tuple[str, str, str]:
    """课程动态：DDBOT 句式 ``转发了{课程作者}的{徽章}：`` + 正文 + 原课程标题。"""
    major = _major(item, "courses") or _major(item, "course")
    card = _card(item)
    title = _text(major.get("title") or card.get("title"))
    badge = _text(major.get("badge") or card.get("badge")) or "课程"
    course_name = _text(
        major.get("up_name") or major.get("name") or card.get("up_name")
    )
    cover = _text(major.get("cover") or card.get("cover") or card.get("pic"))
    action = f"转发了{course_name}的{badge}" if course_name else "发布了新课程"
    return action, _join(desc, _label("原课程", title)), cover


#: 动态类型码 → 动作短语（不含尾部冒号；DDBOT news.tmpl 同款句式）。
#: 512（番剧/影视）与未知类型一样走通用文案，与 DDBOT default 分支一致。
TYPE_ACTION: dict[int, str] = {
    2: "发布了新动态",
    4: "发布了新动态",
    8: "投稿了视频",
    64: "发布了新专栏",
    256: "投稿了新音频",
    512: "发布了新动态",
    1024: "发布了新动态",
    2048: "发表了新动态",
    4200: "发布了直播信息",
    4308: "发布了直播信息",
    4300: "发布了收藏夹",
    4310: "更新了合集",
}

#: 转发动态：被转发类型码 → 动作句式（``{origin}`` 为原作者名）。
FORWARD_ACTION: dict[int, str] = {
    8: "转发了{origin}的视频",
    64: "转发了{origin}的专栏",
    256: "转发了{origin}的音频",
    4200: "分享了{origin}的直播",
    4308: "分享了{origin}的直播",
    4300: "分享了{origin}的收藏夹",
    4302: "转发了{origin}的课程",
}
#: 转发动态的默认动作句式。
_FORWARD_DEFAULT_ACTION: str = "转发了{origin}的动态"

#: 转发动态：被转发类型码 → 标注行（DDBOT 原动态/原视频/原专栏/原音频/原直播间/原收藏夹同款）。
_FORWARD_MARKER: dict[int, str] = {
    8: "原视频：",
    64: "原专栏：",
    256: "原音频：",
    4200: "原直播间：",
    4308: "原直播间：",
    4300: "原收藏夹：",
    4302: "原课程：",
}
#: 转发动态的默认标注行。
_DEFAULT_FORWARD_MARKER: str = "原动态："


class DynamicPoller:
    """动态订阅轮询器：检测并推送 UP 主新动态。

    Args:
        subscription: 规范化后的 dynamic 订阅（uid 非空）。
        repo: :class:`BiliRepository` 实现（或测试 fake）。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload -> str|MessageChain）。
        send: ``push.send``（subscription, chain, context, status -> dict[str, bool]）。
        context: AstrBot Context（或暴露 ``async send_message(session, chain) -> bool``
            的 fake）。
        status: main.py 持有的 runtime status dict（按 sub_id，可变对象）。
        retry_counts: main.py 持有的重试计数 dict（``{sub_id: {dynamic_id: n}}``），
            跨 poller 重建保留；``initialize()`` 清空即重启后重新计数。
        logger: 显式 logger；缺省用插件统一 logger。
        acquire: 每轮轮询开始前调用的异步取牌函数（调度器注入令牌桶，
            per-poll 限速）；缺省为无操作，行为不变。
        push_cover: 是否在推送中携带封面图片（``poll.push_dynamic_cover``）；
            部分平台（如飞书）图文混合消息存在兼容问题时关闭以仅推送文字。
        push_live_share: 是否推送「直播分享」类动态（``poll.push_dynamic_live_share``，
            类型码 4308）。B 站会在直播结束后自动生成该类动态（非 UP 主动发送），
            缺省不推送以避免与开播/下播通知重复。
    """

    def __init__(
        self,
        subscription: Subscription,
        repo: BiliRepository,
        db: Database,
        build_chain: Callable[[str, dict[str, Any]], Any],
        send: Callable[
            [Subscription, Any, Any, dict[str, Any]], Awaitable[dict[str, bool]]
        ],
        context: Any,
        status: dict[str, Any],
        retry_counts: dict[str, dict[str, int]],
        logger: logging.Logger | None = None,
        acquire: Callable[[], Awaitable[None]] | None = None,
        push_cover: bool = True,
        push_live_share: bool = False,
    ) -> None:
        self.subscription = subscription
        self.repo = repo
        self.db = db
        self.build_chain = build_chain
        self.send = send
        self.context = context
        self.status = status
        self.retry_counts = retry_counts
        self._acquire: Callable[[], Awaitable[None]] = (
            acquire if acquire is not None else _noop_acquire
        )
        self._logger = logger if logger is not None else _get_logger()
        self.push_cover = push_cover
        self.push_live_share = push_live_share
        self.error_count = 0

    async def poll(self) -> None:
        """执行一轮动态扫描；仓库/未知异常记录错误状态后吞掉，不向上抛出。

        轮询开始前取一枚令牌（per-poll 限速：每轮只取一枚，轮内分页请求不限速）。
        失败会写 ``status.last_error`` 并递增 ``error_count``（调度器据此退避
        与自动禁用）。``asyncio.CancelledError`` 透传（任务取消属正常 shutdown）。
        """
        try:
            await self._acquire()
            await self._poll_once()
        except asyncio.CancelledError:
            raise
        except BiliError as exc:
            self._record_error(exc, "动态轮询")
            self._logger.warning(
                "动态轮询失败（sub=%s）: %s", self.subscription.name, exc
            )
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._record_error(exc, "动态轮询")
            self._logger.error(
                "动态轮询异常（sub=%s）: %s",
                self.subscription.name,
                exc,
                exc_info=True,
            )

    def _record_error(self, exc: Exception, where: str) -> None:
        """记录轮询错误：写 ``status[sub_id].last_error`` 并递增 ``error_count``。

        与 LivePoller 同款信号：调度器据此累计连续失败、指数退避并自动禁用；
        缺失修复前动态/合集错误对调度器完全不可见。
        """
        self.error_count += 1
        entry = self.status.get(self.subscription.id)
        if entry is None:
            entry = SimpleNamespace(last_push_at=None, last_error=None)
            self.status[self.subscription.id] = entry
        entry.last_error = f"{where}: {exc}"

    async def _poll_once(self) -> None:
        sub = self.subscription
        if sub.uid is None:
            self._logger.warning("动态订阅缺少 uid，跳过: %s", sub.name)
            return
        items = await self._fetch_feed(sub.uid)
        if not await self.db.get_seeded(_SEED_TABLE, sub.id):
            await self._seed(sub, items)
            return
        for item in items:
            await self._handle_item(sub, item)

    async def _fetch_feed(self, uid: int) -> list[dict[str, Any]]:
        """从空 offset 拉取动态，翻页直到 ``has_more`` 为假或达页数上限。"""
        collected: list[dict[str, Any]] = []
        offset: str | int = 0
        pages = 0
        while True:
            resp = await self.repo.get_dynamics(uid, offset=offset)
            pages += 1
            items = resp.get("items")
            if isinstance(items, list):
                collected.extend(items)
            if not resp.get("has_more"):
                break
            if pages >= _MAX_PAGES:
                self._logger.warning(
                    "动态扫描已达 %d 页上限（has_more 仍为真），停止翻页: sub=%s",
                    _MAX_PAGES,
                    self.subscription.name,
                )
                break
            offset = resp.get("offset", offset)
        return collected

    async def _seed(self, sub: Subscription, items: list[dict[str, Any]]) -> None:
        """首轮静默 seed：全量写入 known_dynamics（不推送），持久化 seed 标志。"""
        for item in items:
            dyn_id = self._dynamic_id(item)
            if dyn_id:
                await self.db.insert_dynamic_if_new(
                    sub.id, dyn_id, self._dynamic_type(item)
                )
        await self.db.set_seeded(_SEED_TABLE, sub.id, True)

    async def _handle_item(self, sub: Subscription, item: dict[str, Any]) -> None:
        """处理单条动态：去重 → 推送 → 按结果维护 mark/重试计数。

        mark-after-send：``insert_dynamic_if_new`` 即持久化 mark；推送全失败时
        保留重试计数继续重试（达上限后告警并停止，行保留即"仍标记"）；
        任一 session 成功则清除重试计数。
        """
        dyn_id = self._dynamic_id(item)
        if not dyn_id:
            return
        type_ = self._dynamic_type(item)
        if type_ == 4308 and not self.push_live_share:
            # 直播分享动态（B 站自动生成，非 UP 主动发送）：默认不推送，
            # 也不写入去重记录（后续轮次保持可被配置重新启用时捕获）。
            return
        retries = self.retry_counts.get(sub.id, {}).get(dyn_id, 0)
        newly = await self.db.insert_dynamic_if_new(sub.id, dyn_id, type_)
        if not newly and retries == 0:
            return  # 已见且无进行中的重试
        chain = self.build_chain("dynamic", self._payload(sub, item, dyn_id, type_))
        results = await self.send(sub, chain, self.context, self.status)
        if any(results.values()):
            if retries:
                self.retry_counts[sub.id].pop(dyn_id, None)
            return
        retries += 1
        self.retry_counts.setdefault(sub.id, {})[dyn_id] = retries
        if retries >= _MAX_RETRY_ROUNDS:
            self.retry_counts[sub.id].pop(dyn_id, None)
            self._logger.warning(
                "动态推送连续 %d 轮失败，已标记为已见不再重试: sub=%s dynamic=%s",
                _MAX_RETRY_ROUNDS,
                sub.name,
                dyn_id,
            )

    def _payload(
        self, sub: Subscription, item: dict[str, Any], dyn_id: str, type_: int
    ) -> dict[str, Any]:
        """构造 dynamic 推送载荷：DDBOT ``news.tmpl`` 同款消息结构。

        载荷键：``name``（UP 名）、``action``（动作短语，**含尾部冒号**，如
        ``发布了新动态：`` / ``投稿了视频：`` / ``转发了XXX的视频：``）、
        ``body``（正文 + 类型专属行，转发动态含 ``原动态：`` 等标注行）、
        ``event_time``、``url``，封面存在且 ``push_cover`` 开启时携带
        ``cover``。转发动态取 ``orig`` 的作者与内容；课程动态单独组装
        动作句式。
        """
        desc = self._plain_text(item)
        name = self._author_name(item) or sub.name
        if type_ == 1:
            action, body, cover = self._forward_parts(item, desc)
        elif type_ == 4302:
            action, body, cover = _course_parts(item, desc)
            action += "："
        else:
            action = f"{TYPE_ACTION.get(type_, _DEFAULT_ACTION)}："
            body, cover = _extract_body_cover(item, desc, type_)
        payload: dict[str, Any] = {
            "name": name,
            "action": action,
            "body": body,
            "event_time": format_event_time(self._dynamic_timestamp(item)),
            "url": _DYNAMIC_URL_TMPL.format(dyn_id),
        }
        if self.push_cover and cover:
            payload["cover"] = cover
        return payload

    def _forward_parts(self, item: dict[str, Any], desc: str) -> tuple[str, str, str]:
        """转发动态的 (动作短语, body, cover)：DDBOT 转发句式 + 被转发内容。

        - ``orig`` 缺失时降级为 ``转发了动态：`` + 仅转发正文。
        - 被转发类型为转发（套娃）时不加标注行，直接展示其内容（DDBOT
          type=1 分支同款）。
        """
        orig = self._origin_item(item)
        if not orig:
            return "转发了动态：", desc, ""
        origin_name = self._author_name(orig) or "该用户"
        origin_type = self._dynamic_type(orig)
        if not origin_type:
            origin_type = self._origin_type_fallback(item)
        action_tmpl = FORWARD_ACTION.get(origin_type, _FORWARD_DEFAULT_ACTION)
        action = f"{action_tmpl.format(origin=origin_name)}："
        marker = _FORWARD_MARKER.get(origin_type, _DEFAULT_FORWARD_MARKER)
        if origin_type == 1:
            marker = ""
        body, cover = _extract_body_cover(
            orig, self._plain_text(orig), origin_type, marker
        )
        return action, body, cover

    @staticmethod
    def _origin_item(item: dict[str, Any]) -> dict[str, Any]:
        """转发动态的被转发条目：polymer ``orig`` 字段；旧 API ``card.origin``
        （JSON 字符串或对象）兜底。"""
        orig = item.get("orig")
        if isinstance(orig, dict):
            return orig
        origin_raw = _card(item).get("origin")
        if isinstance(origin_raw, dict):
            return origin_raw
        if isinstance(origin_raw, str):
            try:
                parsed = json.loads(origin_raw)
            except (json.JSONDecodeError, TypeError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    @staticmethod
    def _origin_type_fallback(item: dict[str, Any]) -> int:
        """旧 API 转发动态：``desc.orig_type`` 保存被转发类型码。"""
        desc = item.get("desc")
        if not isinstance(desc, dict):
            return 0
        try:
            return int(desc.get("orig_type"))
        except (TypeError, ValueError):
            return 0

    def _plain_text(self, item: dict[str, Any]) -> str:
        """动态正文文本：新 API ``desc.text``，旧 API ``desc`` / ``card.item`` 兜底。"""
        text = _text(_dig(item, "modules", "module_dynamic", "desc", "text"))
        if not text:
            desc = item.get("desc")
            if isinstance(desc, dict):
                text = _text(desc.get("text"))
            elif isinstance(desc, str):
                text = desc.strip()
        if not text:
            inner = _card(item).get("item") or item.get("item")
            if isinstance(inner, dict):
                text = _text(inner.get("content") or inner.get("description"))
        return text

    def _author_name(self, item: dict[str, Any]) -> str:
        """UP 主名：新 API ``module_author.name``；旧 API ``desc.user_profile``
        与平铺被转发条目 ``user.uname`` 兜底；缺失返回空串。"""
        name = _dig(item, "modules", "module_author", "name")
        if not name:
            name = _dig(item, "desc", "user_profile", "info", "uname")
        if not name:
            user = item.get("user")
            if isinstance(user, dict):
                name = user.get("uname") or user.get("name")
        return str(name).strip() if name else ""

    def _dynamic_id(self, item: dict[str, Any]) -> str:
        """动态 ID：新 API ``id_str``（polymer feed/space，仅此字段）优先，
        旧 API ``desc.dynamic_id*`` 兜底；取不到返回空串。"""
        raw = _dig(item, "id_str") or _dig(item, "id")
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("dynamic_id_str") or desc.get("dynamic_id")
                if isinstance(raw, dict):
                    raw = raw.get("dynamic_id_str")
        if raw is None:
            return ""
        return str(raw).strip()

    def _dynamic_type(self, item: dict[str, Any]) -> int:
        """动态类型码：新 API 字符串枚举（``DYNAMIC_TYPE_*``），旧 API ``desc.type``
        整数兜底；纯文字与图文共用 ``DYNAMIC_TYPE_WORD``，按 ``major.type`` 区分。"""
        raw = _dig(item, "type")
        if isinstance(raw, str) and raw:
            code = _DYNAMIC_TYPE_ENUM_MAP.get(raw)
            if code is None:
                return 0
            if code == 4:
                major_type = _dig(item, "modules", "module_dynamic", "major", "type")
                if major_type == "MAJOR_TYPE_OPUS":
                    return 2048  # 图文
            return code
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("type")
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def _dynamic_timestamp(self, item: dict[str, Any]) -> int:
        """动态发布时间戳：新 API ``module_author.pub_ts``，旧 API ``desc.timestamp``。"""
        raw = _dig(item, "modules", "module_author", "pub_ts")
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("timestamp")
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0
