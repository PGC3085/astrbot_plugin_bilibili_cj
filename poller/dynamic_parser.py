"""动态消息解析与构造（从 :mod:`poller.dynamic` 拆出的纯函数层）。

本模块不依赖 AstrBot、不发起任何网络请求，只负责把 B 站动态 API 的
原始条目（item）转换为 ``poller.dynamic.DynamicPoller`` 需要的推送载荷：

- **双代 API 兼容**：新 polymer API（``id_str``、字符串枚举
  ``DYNAMIC_TYPE_*``、``modules.module_dynamic.desc`` / ``major.*``、
  转发动态的 ``orig`` 子条目）与旧 API（``desc`` / ``card`` JSON、
  整数 ``type``、``desc.orig_type``）都在这里解析。
- **图文（opus）兼容**：B 站开启 ``itemOpusStyle`` 后，图文动态的条目类型
  仍可能是 ``DYNAMIC_TYPE_DRAW``，但实际内容在
  ``major.type == MAJOR_TYPE_OPUS`` 的 ``major.opus`` 里，且
  ``module_dynamic.desc`` 为 ``null``。旧实现只看 ``major.draw``，导致
  正文与图片全部丢失；这里以 ``major.type`` 作为内容类型的最终裁决。
- **消息结构遵循 DDBOT ``notify.group.bilibili.news.tmpl``**：动作句式、
  类型专属行（标题/简介/摘要/作者等）、转发动态的 ``原视频：`` 等标注行
  与 DDBOT 保持一致；图片动态收集全部图片 URL，首张同时作为 ``cover``。

验收点（离线可测）：

- ``DYNAMIC_TYPE_DRAW`` + ``MAJOR_TYPE_OPUS`` 会解析出 summary 正文与
  ``pics`` 中的图片；
- ``DYNAMIC_TYPE_WORD`` + ``MAJOR_TYPE_OPUS`` 走图文（2048）句式；
- 旧 API 的整数类型码与 card JSON 行为不回退；
- 转发动态（含被转发内容同样为 opus 的情况）能带出原内容。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

try:
    from ..config import Subscription
    from ..push import format_event_time
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription  # type: ignore[import-not-found]
    from push import format_event_time  # type: ignore[import-not-found]

#: 动态详情页链接模板（DDBOT DynamicUrl 同款，t.bilibili.com/<dynamic_id>）。
DYNAMIC_URL_TMPL: str = "https://t.bilibili.com/{}"
#: 未知动态类型的通用动作短语（DDBOT tmpl default 分支同款）。
DEFAULT_ACTION: str = "发布了新动态"

#: 新 API（polymer feed/space）动态类型字符串枚举 → 内部类型码（沿用旧 API 整数码）。
DYNAMIC_TYPE_ENUM_MAP: dict[str, int] = {
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

#: ``modules.module_dynamic.major.type`` → 内部类型码。
#:
#: ``item.type`` 与 ``major.type`` 并不总是同义：请求带 ``itemOpusStyle`` 时，
#: 图文动态的条目类型仍可能是 ``DYNAMIC_TYPE_DRAW`` / ``DYNAMIC_TYPE_WORD``，
#: 而真正的内容类型是 ``MAJOR_TYPE_OPUS``。因此字符串枚举解析完成后，若
#: major 类型可识别且不是 ``MAJOR_TYPE_NONE``，以 major 类型为最终结果。
MAJOR_TYPE_ENUM_MAP: dict[str, int] = {
    "MAJOR_TYPE_NONE": 0,
    "MAJOR_TYPE_DRAW": 2,
    "MAJOR_TYPE_ARCHIVE": 8,
    "MAJOR_TYPE_ARTICLE": 64,
    "MAJOR_TYPE_MUSIC": 256,
    "MAJOR_TYPE_PGC": 512,
    "MAJOR_TYPE_OPUS": 2048,
    "MAJOR_TYPE_LIVE": 4200,
    "MAJOR_TYPE_LIVE_RCMD": 4308,
    "MAJOR_TYPE_MEDIALIST": 4300,
    "MAJOR_TYPE_COMMON": 4300,
    "MAJOR_TYPE_COURSES": 4302,
    "MAJOR_TYPE_COURSES_SEASON": 4302,
    "MAJOR_TYPE_COURSES_BATCH": 4302,
    "MAJOR_TYPE_UGC_SEASON": 4310,
}

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
FORWARD_DEFAULT_ACTION: str = "转发了{origin}的动态"

#: 转发动态：被转发类型码 → 标注行（DDBOT 原动态/原视频/原专栏/原音频/原直播间/原收藏夹同款）。
FORWARD_MARKER: dict[int, str] = {
    8: "原视频：",
    64: "原专栏：",
    256: "原音频：",
    4200: "原直播间：",
    4308: "原直播间：",
    4300: "原收藏夹：",
    4302: "原课程：",
}
#: 转发动态的默认标注行。
FORWARD_DEFAULT_MARKER: str = "原动态："


@dataclass(frozen=True)
class DynamicContent:
    """一条动态构造完成后的消息内容。

    Attributes:
        action: 动作短语，**含尾部冒号**（``发布了新动态：`` / ``投稿了视频：`` /
            ``转发了原作者：的视频：`` ...）。
        body: 正文 + 类型专属行；转发动态含 ``原动态：`` 等标注行。
        images: 动态携带的图片 URL（按 B 站返回顺序去重）；可能为空。
    """

    action: str
    body: str
    images: tuple[str, ...]


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


def _major_type(item: dict[str, Any]) -> str:
    """取 ``modules.module_dynamic.major.type``；缺失返回空串。"""
    return _text(_dig(item, "modules", "module_dynamic", "major", "type"))


def _card(item: dict[str, Any]) -> dict[str, Any]:
    """解析旧 API 的 ``card`` JSON 为 dict；已是 dict 直接使用。"""
    card = item.get("card")
    if isinstance(card, dict):
        return card
    if not isinstance(card, str):
        return {}
    try:
        parsed = json.loads(card)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _image_url(value: Any) -> str:
    """从图片对象取 URL（兼容 src/url/img_src/cover 等字段名）。"""
    if not isinstance(value, dict):
        return _text(value)
    for key in ("src", "url", "img_src", "cover", "image_src"):
        url = _text(value.get(key))
        if url:
            return url
    # 极少数形状下 src 本身还是嵌套对象（如 src.remote.url）。
    for key in ("src", "url", "cover"):
        nested = value.get(key)
        if isinstance(nested, dict):
            url = _text(nested.get("url") or nested.get("remote_url"))
            if url:
                return url
    return ""


def _dedupe(values: Any) -> list[str]:
    """把图片 URL 候选列表/可迭代对象转成保序去重后的非空列表。"""
    result: list[str] = []
    if isinstance(values, (str, bytes)) or not isinstance(values, (list, tuple)):
        return result
    for value in values:
        url = _image_url(value)
        if url and url not in result:
            result.append(url)
    return result


def _collect_images(item: dict[str, Any], major: dict[str, Any]) -> list[str]:
    """收集动态携带的全部图片 URL（多图保序去重），major 优先，card/item 兜底。

    Args:
        item: 原始动态条目（新/旧 API 均可）。
        major: 已解析出的 ``major.<子键>`` 内容 dict（如 opus/draw/article）。

    Returns:
        保序去重后的图片 URL 列表。
    """
    card = _card(item)
    images: list[str] = []
    # 多图列表：major 的 pics/covers/items 优先，旧 API card.item.pictures 兜底。
    for key in ("pics", "covers", "items"):
        for source in (major.get(key), card.get(key)):
            images.extend(_dedupe(source))
    for source in (card.get("item"), item.get("item")):
        if isinstance(source, dict):
            images.extend(_dedupe(source.get("pictures") or source.get("pics")))
    # 单封面字段：有列表结果时不再混入（封面通常就是列表首图，混入会重复）。
    if not images:
        for key in ("cover", "pic"):
            url = _text(major.get(key) or card.get(key) or item.get(key))
            if url and url not in images:
                images.append(url)
    return images


def _label(key: str, value: str) -> str:
    """``键：值`` 标签行（DDBOT 的 标题：/简介： 等）；值为空返回空串。"""
    return f"{key}：{value}" if value else ""


def _text_or_inner(value: Any) -> str:
    """文本或容器内嵌文本（如 opus 的 ``summary`` 为富文本 dict）。"""
    if isinstance(value, dict):
        direct = _text(value.get("text"))
        if direct:
            return direct
        nodes = value.get("rich_text_nodes")
        if isinstance(nodes, list):
            joined = "".join(
                _text(node.get("text") or node.get("orig_text"))
                for node in nodes
                if isinstance(node, dict)
            )
            if joined:
                return joined
        paragraphs = value.get("paragraphs")
        if isinstance(paragraphs, list):
            parts: list[str] = []
            for paragraph in paragraphs:
                if not isinstance(paragraph, dict):
                    continue
                text = paragraph.get("text")
                if isinstance(text, dict):
                    parts.append(_text_or_inner(text))
                else:
                    parts.append(_text(text))
            joined = "".join(part for part in parts if part)
            if joined:
                return joined
        nested = value.get("summary")
        if isinstance(nested, str):
            return nested.strip()
    return _text(value)


def _title_body_cover(
    item: dict[str, Any], major_keys: tuple[str, ...], body_keys: tuple[str, ...]
) -> tuple[str, str, list[str]]:
    """统一提取 (标题, 正文, 图片列表)：按候选 major 子键依次尝试。

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
                body = _text_or_inner(value)
                break
        images = _collect_images(item, major)
        if title or body or images:
            return title, body, images
    return "", "", []


def _body_images(
    item: dict[str, Any], desc: str, type_: int, marker: str = ""
) -> tuple[str, tuple[str, ...]]:
    """DDBOT ``news.tmpl`` 同款按类型组装 (body, images)。

    body 为正文 + 类型专属行；``marker`` 非空时作为首行（转发动态的
    ``原动态：`` / ``原视频：`` / ``原专栏：`` / ``原音频：`` / ``原直播间：`` /
    ``原收藏夹：`` 标注行，DDBOT 同款）。
    """
    if type_ == 8:  # 视频投稿：正文 + 标题/简介
        title, extra, images = _title_body_cover(
            item, ("archive",), ("desc", "dynamic")
        )
        return (
            _join(marker, desc, _label("标题", title), _label("简介", extra)),
            tuple(images),
        )
    if type_ == 2:  # 图片动态：正文 + 全部图片
        images = _collect_images(item, _major(item, "draw"))
        return _join(marker, desc), tuple(images)
    if type_ == 64:  # 专栏：正文 + 标题/摘要
        major = _major(item, "article")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        summary = _text_or_inner(
            major.get("summary") or card.get("summary") or card.get("desc")
        )
        images = _collect_images(item, major)
        return (
            _join(marker, desc, _label("标题", title), _label("摘要", summary)),
            tuple(images),
        )
    if type_ == 256:  # 音频：正文 + 标题/简介/作者
        major = _major(item, "music")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        intro = _text(major.get("intro") or card.get("intro"))
        author = _text(major.get("author") or card.get("author"))
        images = _collect_images(item, major)
        return (
            _join(
                marker,
                desc,
                _label("标题", title),
                _label("简介", intro),
                _label("作者", author),
            ),
            tuple(images),
        )
    if type_ == 512:  # 番剧/影视：正文 + 标题
        major = _major(item, "pgc")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        images = _collect_images(item, major)
        return _join(marker, desc, _label("标题", title)), tuple(images)
    if type_ == 2048:  # 图文（opus）：正文 + 标题/正文 + 全部图片
        major = _major(item, "opus")
        card = _card(item)
        title = _text(major.get("title") or card.get("title"))
        summary = _text_or_inner(
            major.get("summary") or card.get("summary") or card.get("desc")
        )
        images = _collect_images(item, major)
        return _join(marker, desc, title, summary), tuple(images)
    if type_ in (4200, 4308):  # 直播分享：正文 + 直播间标题
        title = ""
        images: list[str] = []
        for key in ("live", "live_rcmd"):
            major = _major(item, key)
            if not major:
                continue
            title = _text(major.get("title"))
            images = _collect_images(item, major)
            if not title:
                info = major.get("live_play_info")
                if isinstance(info, dict):
                    title = _text(info.get("title"))
                    if not images:
                        images = _dedupe([info.get("cover")])
            if title or images:
                break
        return _join(marker, desc, title), tuple(images)
    if type_ == 4300:  # 收藏夹：正文 + 标题
        title, _, images = _title_body_cover(
            item, ("medialist", "mylist", "common"), ("desc", "title")
        )
        return _join(marker, desc, title), tuple(images)
    if type_ == 1024:  # 已消失内容：正文 + 提示
        card = _card(item)
        tips = ""
        inner = card.get("item") or item.get("item")
        if isinstance(inner, dict):
            tips = _text(inner.get("tips"))
        return _join(marker, desc, tips), ()
    if type_ == 4310:  # 合集更新：正文 + 合集名
        title, _, images = _title_body_cover(item, ("ugc_season",), ("desc",))
        return _join(marker, desc, title), tuple(images)
    # 文字(4)/未知类型：仅正文。
    return _join(marker, desc), ()


def _course_content(item: dict[str, Any], desc: str) -> DynamicContent:
    """课程动态：DDBOT 句式 ``转发了{课程作者}的{徽章}：`` + 正文 + 原课程标题。"""
    major = _major(item, "courses") or _major(item, "course")
    card = _card(item)
    title = _text(major.get("title") or card.get("title"))
    badge = _text(major.get("badge") or card.get("badge")) or "课程"
    course_name = _text(
        major.get("up_name") or major.get("name") or card.get("up_name")
    )
    images = _collect_images(item, major)
    action = f"转发了{course_name}的{badge}" if course_name else "发布了新课程"
    return DynamicContent(
        action=f"{action}：",
        body=_join(desc, _label("原课程", title)),
        images=tuple(images),
    )


def extract_id(item: dict[str, Any]) -> str:
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


def extract_type(item: dict[str, Any]) -> int:
    """动态类型码：新 API 字符串枚举 + ``major.type`` 裁决，旧 API 整数兜底。

    2025 年起 B 站 feed/space 接口请求带 ``itemOpusStyle`` 后，大量图文动态
    的条目类型仍为 ``DYNAMIC_TYPE_DRAW``，但实际内容位于
    ``major.type == MAJOR_TYPE_OPUS`` 的 ``major.opus`` 中，且
    ``module_dynamic.desc`` 为 ``null``。这类条目必须按 2048（图文）处理，
    否则会得到只有标题行和链接、没有正文和图片的空消息。
    """
    raw = _dig(item, "type")
    if isinstance(raw, str) and raw:
        raw_code = DYNAMIC_TYPE_ENUM_MAP.get(raw)
        if raw == "DYNAMIC_TYPE_FORWARD":
            return 1
        major_code = MAJOR_TYPE_ENUM_MAP.get(_major_type(item), 0)
        if major_code:
            return major_code
        return raw_code if raw_code is not None else 0
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


def extract_author(item: dict[str, Any]) -> str:
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


def extract_timestamp(item: dict[str, Any]) -> int:
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


def extract_text(item: dict[str, Any]) -> str:
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


def extract_origin(item: dict[str, Any]) -> dict[str, Any]:
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


def _legacy_origin_type(item: dict[str, Any]) -> int:
    """旧 API 转发动态：``desc.orig_type`` 保存被转发类型码。"""
    desc = item.get("desc")
    if not isinstance(desc, dict):
        return 0
    try:
        return int(desc.get("orig_type"))
    except (TypeError, ValueError):
        return 0


def _forward_content(item: dict[str, Any], desc: str) -> DynamicContent:
    """转发动态的 (动作, body, images)：DDBOT 转发句式 + 被转发内容。

    - ``orig`` 缺失时降级为 ``转发了动态：`` + 仅转发正文。
    - 被转发类型为转发（套娃）时不加标注行，直接展示其内容（DDBOT
      type=1 分支同款）。
    """
    orig = extract_origin(item)
    if not orig:
        return DynamicContent("转发了动态：", desc, ())
    origin_name = extract_author(orig) or "该用户"
    origin_type = extract_type(orig)
    if not origin_type:
        origin_type = _legacy_origin_type(item)
    action_tmpl = FORWARD_ACTION.get(origin_type, FORWARD_DEFAULT_ACTION)
    action = f"{action_tmpl.format(origin=origin_name)}："
    marker = FORWARD_MARKER.get(origin_type, FORWARD_DEFAULT_MARKER)
    if origin_type == 1:
        marker = ""
    body, images = _body_images(orig, extract_text(orig), origin_type, marker)
    return DynamicContent(action, body, images)


def build_content(item: dict[str, Any], type_: int | None = None) -> DynamicContent:
    """把原始动态条目解析为 DDBOT 同款消息内容。

    Args:
        item: 新/旧 API 的动态条目 dict。
        type_: 已解析的内部类型码；None 时自行解析。

    Returns:
        :class:`DynamicContent`；转发动态自动取 ``orig`` 内容。
    """
    resolved = extract_type(item) if type_ in (None, 0) else int(type_)
    desc = extract_text(item)
    if resolved == 1:
        return _forward_content(item, desc)
    if resolved == 4302:
        return _course_content(item, desc)
    action = f"{TYPE_ACTION.get(resolved, DEFAULT_ACTION)}："
    body, images = _body_images(item, desc, resolved)
    return DynamicContent(action, body, images)


def build_payload(
    subscription: Subscription,
    item: dict[str, Any],
    dyn_id: str | None = None,
    type_: int | None = None,
    *,
    push_cover: bool = True,
) -> dict[str, Any]:
    """构造 dynamic 推送载荷：DDBOT ``news.tmpl`` 同款消息结构。

    载荷键：``name``（UP 名）、``action``（动作短语，**含尾部冒号**，如
    ``发布了新动态：`` / ``投稿了视频：`` / ``转发了XXX的视频：``）、
    ``body``（正文 + 类型专属行，转发动态含 ``原动态：`` 等标注行）、
    ``event_time``、``url``；``push_cover`` 开启且动态携带图片时，附带
    ``cover``（首图）与 ``images``（全部图片，供消息链按顺序追加）。

    Args:
        subscription: 规范化后的 dynamic 订阅（取订阅名兜底 UP 名）。
        item: 新/旧 API 的动态条目 dict。
        dyn_id: 动态 ID；None 时从 ``item`` 解析。
        type_: 内部类型码；None 时从 ``item`` 解析。
        push_cover: 是否在载荷中携带图片（``poll.push_dynamic_cover``）。

    Returns:
        可直接交给 ``push.build_chain("dynamic", payload)`` 的载荷字典。
    """
    resolved_id = extract_id(item) if dyn_id is None else str(dyn_id)
    resolved_type = extract_type(item) if type_ in (None, 0) else int(type_)
    content = build_content(item, resolved_type)
    payload: dict[str, Any] = {
        "name": extract_author(item) or subscription.name,
        "action": content.action,
        "body": content.body,
        "event_time": format_event_time(extract_timestamp(item)),
        "url": DYNAMIC_URL_TMPL.format(resolved_id),
    }
    if push_cover and content.images:
        payload["cover"] = content.images[0]
        payload["images"] = list(content.images)
    return payload
