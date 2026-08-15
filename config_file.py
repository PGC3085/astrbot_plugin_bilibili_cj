"""插件配置文件读写工具（从 main.py 拆出的独立模块）。

职责只有两件：

1. 解析 AstrBot 约定的配置文件路径（``<data>/config/<plugin>_config.json``）；
2. 提供安装兜底初始化、批量配置读取、JSON 读写与深度合并等**无 AstrBot
   依赖**的文件工具。

本模块不接触调度、轮询与 AstrBot 插件 API，离线测试可直接导入。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    from . import util
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    import util  # type: ignore[import-not-found]

#: 插件配置文件文件名（AstrBot 约定：``<plugin 根目录名>_config.json``）。
_CONFIG_FILE_NAME: str = "astrbot_plugin_bilibili_cj_config.json"
#: 插件目录内的批量配置文件名：存在时启动读入并合并到 AstrBot 配置（便于大规模设置）。
_BUNDLED_CONFIG_NAME: str = "config.json"

#: ``_conf_schema.json`` 各类型缺省 ``default`` 字段时的回退值（与 AstrBotConfig 同语义）。
_SCHEMA_DEFAULT_MAP: dict[str, Any] = {
    "int": 0,
    "float": 0.0,
    "bool": False,
    "string": "",
    "text": "",
    "list": [],
    "file": [],
    "object": {},
    "template_list": [],
    "dict": {},
}


def _default_config_path() -> Path:
    """解析生产配置文件路径：``<data>/config/astrbot_plugin_bilibili_cj_config.json``。

    AstrBot 将插件配置存放在 ``get_astrbot_config_path()``（即
    ``<data>/config/``，见 ``star_manager.py`` 的 ``f"{root_dir_name}_config.json"``）；
    AstrBot 不可导入（离线）时回退到相对 ``./data/config/...``。
    """
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_config_path
    except ImportError:
        util.get_logger(__name__).warning(
            "get_astrbot_config_path unavailable; using ./data/config/%s",
            _CONFIG_FILE_NAME,
        )
        return Path("data") / "config" / _CONFIG_FILE_NAME
    return Path(get_astrbot_config_path()) / _CONFIG_FILE_NAME


def _schema_to_defaults(schema: dict[str, Any]) -> dict[str, Any]:
    """把 ``_conf_schema.json`` 转为默认配置字典（与 ``AstrBotConfig`` 同语义）。

    ``object`` 递归展开其 ``items``；其余类型取 ``default``，缺失时按类型回退。

    Args:
        schema: 插件配置 schema（``_conf_schema.json`` 内容）。

    Returns:
        由 schema 派生的默认配置字典。
    """
    conf: dict[str, Any] = {}
    for key, spec in schema.items():
        if spec.get("type") == "object":
            conf[key] = _schema_to_defaults(spec.get("items") or {})
        else:
            conf[key] = spec.get("default", _SCHEMA_DEFAULT_MAP.get(spec.get("type")))
    return conf


def ensure_config_file(
    config_path: str | Path | None = None, logger: Any | None = None
) -> Path:
    """确保插件配置文件存在：安装时缺失则按 schema 默认值初始化（幂等）。

    AstrBot 在插件加载时会依据 ``_conf_schema.json`` 自动创建配置文件；本函数
    作为插件自身的兜底，覆盖手动安装 / 离线等边缘场景——文件已存在时**不改写**
    任何内容。

    Args:
        config_path: 配置文件路径；None 按 AstrBot 约定解析。
        logger: 显式 logger；缺省用插件统一 logger。

    Returns:
        解析后的配置文件路径。
    """
    path = Path(config_path) if config_path is not None else _default_config_path()
    logger = logger if logger is not None else util.get_logger(__name__)
    if path.is_file():
        return path
    schema_path = Path(__file__).with_name("_conf_schema.json")
    defaults: dict[str, Any] = {}
    if schema_path.is_file():
        try:
            with open(schema_path, "r", encoding="utf-8-sig") as f:
                defaults = _schema_to_defaults(json.load(f))
        except (OSError, ValueError) as exc:
            logger.warning("读取 _conf_schema.json 失败，回退空默认配置: %s", exc)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(defaults, f, indent=2, ensure_ascii=False)
            f.write("\n")
        logger.info("初始化插件配置文件: %s", path)
    except OSError as exc:
        logger.warning("初始化配置文件 %s 失败: %s", path, exc)
    return path


def _bundled_config_path() -> Path | None:
    """返回插件目录下的批量配置文件路径；不存在返回 None。"""
    path = Path(__file__).with_name(_BUNDLED_CONFIG_NAME)
    return path if path.is_file() else None


def read_config_file(
    path: str | Path, logger: Any | None = None
) -> dict[str, Any] | None:
    """读取 JSON 配置文件为 dict；失败返回 None（告警但不中断）。

    Args:
        path: 配置文件路径。
        logger: 显式 logger；缺省用插件统一 logger。

    Returns:
        解析出的 dict；文件不可读 / JSON 非法 / 顶层非对象时返回 None。
    """
    logger = logger if logger is not None else util.get_logger(__name__)
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        logger.warning("读取配置文件 %s 失败: %s", path, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("配置文件 %s 顶层不是对象，已忽略", path)
        return None
    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> None:
    """就地深度合并：dict 递归合并，其余（含 list）整体覆盖。

    Args:
        base: 被合并的目标字典（就地修改）。
        override: 覆盖来源字典。
    """
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
