# B站监控 · AstrBot 插件

[![AstrBot](https://img.shields.io/badge/AstrBot-Plugin-blue)](https://github.com/Soulter/AstrBot)
[![Python](https://img.shields.io/badge/Python-3.10%2B-green)](https://www.python.org/)

一个用于 [AstrBot](https://github.com/Soulter/AstrBot) 的插件，监控 B 站 UP 主的**直播开播/下播/改标题**、**动态更新**（视频投稿、图片、文字、转发、专栏、音频、图文、直播分享、收藏夹、课程等）与**合集视频更新**，检测到新内容后自动推送到你指定的多平台会话（QQ 群、Telegram 等）。

三种监控能力统一由一套异步轮询器调度，统一推送、统一状态库（SQLite）、统一配置，并自带一个独立的 WebUI 管理面板。

---

## ✨ 功能

- 🔴 **直播监控** — 开播推送（含标题、分区、**开播时间**、封面）、下播推送（含直播时长、**下播时间**）、直播中改标题推送（含**时间**）。下播采用"连续 3 轮未检测到直播"判定，避免瞬时抖动误报；首次轮询静默记录，重启不重复推送
- 📢 **动态推送** — 检测 UP 主新动态并推送（经 B 站新版 polymer `feed/space` 接口，兼容新旧两代返回结构），覆盖 12 类动态：视频投稿、图片、文字、转发、专栏、音频、番剧、图文、直播分享、收藏夹、课程、合集更新，未知类型走通用文案；每条均含动态**发布时间**；图文（opus）动态会带出正文标题与**全部图片**
- 🎬 **合集推送** — 监控用户自制合集（视频合集 / 收藏夹）的新增视频，逐页扫描、按 bvid 去重，含视频**发布时间**

> 上述事件时间均以 **UTC+8（中国标准时间）** 显示。
- 🖥️ **独立 WebUI** — 插件自带 HTTP 面板（独立端口，不依赖 AstrBot 仪表盘）：订阅增删改查、设置、状态、日志查看、测试推送
- 📱 **多平台推送** — 每个订阅可配置多个目标会话，经 AstrBot `context.send_message` 路由到任意已接入平台
- 🎲 **随机抖动反风控** — 每次轮询间隔附加随机延迟，配合全局令牌桶限速，降低被 B 站风控拦截的风险
- 🍪 **Cookie 认证** — 支持完整 B 站凭据（SESSDATA、bili_jct 等），显著降低风控概率
- 💾 **SQLite 持久化** — 已推送记录、直播状态、seed 状态存本地数据库，重启不重复推送
- 🔄 **配置热重载** — 手动修改配置文件后自动重载轮询任务，无需重启 AstrBot
- 💬 **平台指令查询** — 在任意会话发送 `/bili`（或 `/bl`）即可查看该会话已配置的订阅清单
- 📋 **启动订阅清单** — 插件启动时在 AstrBot 控制台打印所有订阅的摘要日志
- ⚙️ **安装即初始化配置** — 首次加载自动按 `_conf_schema.json` 生成配置文件，无需手动创建

---

## 📦 安装

### 前置条件

- 已部署并运行 [AstrBot](https://github.com/Soulter/AstrBot)
- Python 3.10+

### 安装步骤

1. 将 `astrbot_plugin_bilibili_cj` 整个文件夹复制到 AstrBot 的 **`data/plugins/`** 目录下（AstrBot 的插件目录，`data` 为 AstrBot 数据根目录）
2. 安装依赖（一般由 AstrBot 自动安装）：

   ```bash
   pip install bilibili-api-python aiohttp aiosqlite
   ```

   依赖清单见插件内 `requirements.txt`。

3. **重启 AstrBot**，或在 AstrBot 的插件管理界面中重新加载插件。

> 也可以把插件文件夹打包成 `.zip`，在 AstrBot 的插件管理界面中导入。

---

## ⚙️ 配置

插件配置位于 AstrBot 的插件配置面板中（`data/config/astrbot_plugin_bilibili_cj_config.json`），面板字段由 `_conf_schema.json` 定义。完整示例见插件目录下的 **`config.example.json`**。

> 配置文件在首次加载时自动初始化：AstrBot 依据 `_conf_schema.json` 的默认值创建 `data/config/astrbot_plugin_bilibili_cj_config.json`；插件自身也会在启动时兜底检查，文件缺失则按 schema 默认值补建，无需手动创建。初始化后 `subscriptions` 默认为空数组，请在面板 / WebUI 中添加订阅，或参考 `config.example.json` 手工编辑。

### 📦 批量配置（插件目录 `config.json`）

为方便**大规模设置**（一次性写入大量订阅），插件支持直接读入位于插件目录下的 `config.json`：

1. 复制插件目录内的 `config.example.json` 为 `config.json`（就放在 `main.py` 同级目录）。
2. 编辑 `config.json`，填入你的 `credential` / `poll` / `webui` / `subscriptions`（可只写需要覆盖的分组）。
3. 重载插件：启动时插件会读入 `config.json`，**深度合并**进 AstrBot 配置（dict 分组逐键覆盖、`subscriptions` 数组整体覆盖），并自动落盘到 `data/config/astrbot_plugin_bilibili_cj_config.json`，随后按正常流程热重载生效。

规则说明：

- 只有插件目录内**存在** `config.json` 时才会触发读入；没有该文件则完全按 AstrBot 面板/`data/config/...` 配置运行。
- `config.json` 在每个插件启动（重载）时都会重新读入并覆盖对应分组，因此它相当于「批量配置源」；若你后续改用面板编辑，请先删除或重命名 `config.json`，避免再次重载时被文件覆盖。
- `config.example.json` 仅作示例，不会被自动加载。

### 配置结构

```json
{
    "credential": { "sessdata": "", "bili_jct": "", "buvid3": "", "buvid4": "", "dedeuserid": "", "ac_time_value": "" },
    "poll": { "global_min_interval_sec": 60, "poll_jitter_sec": 15, "push_title_change": true },
    "webui": { "enabled": true, "host": "127.0.0.1", "port": 8765, "token": "" },
    "login_monitor": { "enabled": true, "interval_sec": 3600, "fail_threshold": 3, "notify_session": "" },
    "subscriptions": [ ... ]
}
```

配置共五个分组，逐项说明如下。

### 🔐 credential（B站凭据）

从浏览器中获取 Cookie 信息，用于通过 B 站 API 认证。

| 字段 | 说明 | 必填 |
|------|------|------|
| `sessdata` | 浏览器 Cookies 中的 SESSDATA（URL 编码后的值） | ✅ 登录核心 |
| `bili_jct` | 浏览器 Cookies 中的 bili_jct | ✅ 建议（写操作 CSRF，缺失会告警但不阻断只读监控） |
| `dedeuserid` | 浏览器 Cookies 中的 DedeUserID（即你的 B 站 UID） | ✅ 建议（缺失会告警） |
| `buvid3` | 浏览器 Cookies 中的 buvid3 | 选填（设备指纹，SDK 可自动生成） |
| `buvid4` | 浏览器 Cookies 中的 buvid4 | 选填（设备指纹，SDK 可自动生成） |
| `ac_time_value` | 浏览器 localStorage 中的 ac_time_value，用于刷新 Cookie | 选填（仅用于刷新） |

> 插件**启动时**会做两层凭据检查并打印到 AstrBot 控制台：① 检查 `sessdata`/`bili_jct`/`dedeuserid` 是否齐全（缺失会告警，`buvid3`/`buvid4`/`ac_time_value` 为可选、缺失不告警）；② 实际调用 B 站接口校验登录状态，登录成功会输出登录用户名，失败/过期会告警提示重新获取 Cookie。未配置 `sessdata` 时按匿名模式运行（告警提示）。

#### 凭据获取

1. 用浏览器登录 B 站（bilibili.com）
2. 按 `F12` 打开开发者工具，切到「Application（应用）」→ 左侧「Cookies」→ 选择 `https://www.bilibili.com`
3. 逐个复制 `SESSDATA`、`bili_jct`、`buvid3`、`buvid4`、`DedeUserID` 的值填入配置
4. `ac_time_value` 在「Local Storage」→ `https://www.bilibili.com` 下，可选

> ⚠️ Cookie 含敏感信息，请勿分享给他人。Cookie 过期后需重新获取（`ac_time_value` 可帮助 SDK 自动刷新）。

### ⏱️ poll（轮询设置）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `global_min_interval_sec` | int | 60 | 全局最小轮询间隔（秒）。任何订阅的两次轮询间隔不会低于此值。最小为 1 |
| `poll_jitter_sec` | float | 15 | 随机波动上限（秒）。每次轮询额外增加 0~N 秒随机延迟，避免被风控。不能为负 |
| `push_title_change` | bool | true | 直播中检测到标题变化时，是否推送「改标题」通知 |
| `push_live_cover` | bool | true | 开播推送是否携带直播间封面（封面位于消息尾部） |
| `push_dynamic_cover` | bool | true | 动态推送是否携带图片（多图动态携带全部图片） |
| `push_collection_cover` | bool | true | 合集更新推送是否携带视频封面 |
| `push_dynamic_live_share` | bool | false | 是否推送「直播分享」动态。B 站在直播结束后会自动生成这类动态（非 UP 主动发送），默认关闭以避免与开播/下播通知重复 |

> **每个订阅由独立任务按自身间隔轮询**：实际间隔 = `max(订阅轮询间隔, global_min_interval_sec) + random(0, poll_jitter_sec)` 秒，订阅之间互不影响（条目多不会拉长单个订阅的间隔）。全局令牌桶速率为各启用订阅的聚合需求，只吸收同时刻的突发，不会拖慢配置的轮询间隔。
>
> 图片统一追加在消息链**尾部**（文字在前，多图按 B 站返回顺序）。部分平台（如飞书）对「文字+图片」混合消息存在兼容问题（可能丢文字），此时可将对应 `push_*_cover` 设为 `false` 仅推送文字。

### 🖥️ webui（WebUI 设置）

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | true | 是否启动内置 WebUI 服务 |
| `host` | string | `127.0.0.1` | 监听地址。仅本机访问填 `127.0.0.1`；局域网访问填 `0.0.0.0` |
| `port` | int | 8765 | WebUI 监听端口（1-65535） |
| `token` | string | `""` | WebUI 的 Bearer 访问令牌。留空时首次启动会自动生成并打印到日志 |

> `host`/`port`/`enabled` 变更需**重载插件**后生效（详见下文 WebUI 一节）。

### 🔐 login_monitor（登录状态监控）

周期性校验 B 站登录状态，并在连续失败达阈值时向指定会话发送告警。

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | true | 是否启用周期性登录状态校验 |
| `interval_sec` | int | 3600 | 两次校验间隔（秒），最小 60 |
| `fail_threshold` | int | 3 | 连续校验失败达该次数后发送告警，最小 1 |
| `notify_session` | string | `""` | 告警目标会话（`platform:message_type:session_id`，通过 `/sid` 获取）；留空仅记日志不发送 |

> 登录校验通过时间会显示在 WebUI **顶栏**（`登录校验通过：…`）；校验在插件启动后立即执行一次，之后按 `interval_sec` 周期执行。

### 📋 subscriptions（订阅列表）

JSON 数组，每个元素定义一条订阅。加载时逐项校验，**非法项会被跳过并记入日志**。

| 字段 | 类型 | 适用类型 | 说明 |
|------|------|----------|------|
| `type` | string | 全部 | 订阅类型：`live`（直播）/ `dynamic`（动态）/ `collection`（合集） |
| `name` | string | 全部 | 订阅名称（用于日志 / WebUI / 推送显示），缺省为 `type:uid` |
| `uid` | int | 全部 | B 站用户 UID（数字） |
| `list_id` | int | collection | 合集 ID（数字，从合集 URL 获取） |
| `series_type` | int | collection | 合集类型：`0`=视频合集，`1`=收藏夹 |
| `poll_interval_sec` | int | 全部 | 该订阅的轮询间隔（秒），缺省 300 |
| `enabled` | bool | 全部 | 是否启用 |
| `push_session_ids` | string[] | 全部 | 推送目标会话 ID 列表（见下节），**不能为空** |

三类订阅的完整示例见 `config.example.json`。

---

## 📡 push_session_ids 格式与 /sid 获取方法

`push_session_ids` 是推送目标的会话 ID 列表，每个元素是 AstrBot 的 `unified_msg_origin` 字符串，格式为：

```
platform:message_type:session_id
```

例如：

- `aiocqhttp:GroupMessage:123456789` — 通过 aiocqhttp 连接的 QQ 群 123456789
- `telegram:GroupMessage:12345` — Telegram 群 12345

### 获取会话 ID

1. 在目标会话（群聊等）中，向机器人发送 `/sid` 指令
2. 机器人会回复当前会话的完整 ID，形如 `aiocqhttp:GroupMessage:123456789`
3. 把完整字符串填入对应订阅的 `push_session_ids` 数组

### 注意事项

- **qq_official 平台不支持主动消息**（无法主动向用户/群推送，见 AstrBot 文档）。如果你的 QQ 账号走的是官方接口，请改用 **aiocqhttp**（NapCat / Lagrange / go-cqhttp 等）连接，否则推送会失败
- 会话格式非法（缺少冒号、平台/消息类型为空）的项会在配置加载时被过滤；**对不支持主动消息的平台（如 qq_official），对应会话同样会在加载时被拒绝并记入日志**
- 若某订阅的全部会话都被过滤，该订阅会因为没有有效推送目标而被跳过
- 订阅写入后，可用 WebUI 的「测试推送」按钮验证目标会话是否可达

---

## 💬 平台指令

插件注册了查询指令，方便在会话内直接查看该会话的订阅，无需打开 WebUI：

| 指令 | 说明 |
|------|------|
| `/bili`（别名 `/bl`） | 列出当前会话（`unified_msg_origin`）所配置的全部订阅，含类型、名称、uid、启用状态与轮询间隔 |

在目标会话中发送 `/bili` 即可；若当前会话没有订阅，会回复「当前会话 … 没有订阅。」。指令依赖 AstrBot 的唤醒前缀（默认 `/`）或 @机器人 触发。

---

## 🖥️ WebUI

插件自带独立 WebUI 面板，用于可视化管理订阅、查看状态与日志、测试推送。

1. **访问**：浏览器打开 `http://<host>:<port>`（默认 `http://127.0.0.1:8765`）
2. **获取 token**：首次启动时 token 为空，插件会在日志中**打印一次**自动生成的 token（形如 32 位十六进制字符串）。也可以在配置的 `webui.token` 中手动指定
3. 在页面令牌门中输入 token 进入面板

面板能力：

- **订阅管理**：增删改查订阅（类型、uid、合集参数、轮询间隔、启用状态、推送会话），**单条即时保存**——新增 / 编辑 / 删除 / 启停勾选都立即写回后端并热重载生效，无需再点「保存全部」
- **设置**：查看 / 修改 credential、poll、webui（`host`/`port`/`enabled` 改动需重载插件后生效）
- **状态**：**列出全部订阅**（含已停用/未轮询的订阅，停用/未轮询以徽章标注），每个订阅显示最近轮询时间、错误计数、直播状态、最近推送时间、自动禁用标志；同时显示**配置文件健康状态**（读取失败时在此显示原因，不再刷屏控制台日志）。卡片采用逐行布局，时间戳完整显示不再被截断，时间均按 **UTC+8** 展示
- **顶栏**：实时显示 B 站登录校验状态（`登录校验通过：<时间>` 或 `登录校验失败 ×N`）
- **日志**：实时查看插件日志
- **测试推送**：既可向指定会话发送单条测试消息，也可选**事件类型**（开播/下播/改标题/动态/合集），按对应类型**仿照真实推送格式**生成测试文案；每条订阅提供「试推」按钮（填入首个会话并自动选中对应类型）与「试推全部」按钮（向该订阅的**全部**会话批量发送并返回逐会话结果）

> token 保存在配置中（`webui.token`），修改后会随配置持久化。请勿将 token 与端口暴露到公网。

---

## 🛡️ 风控注意事项

B 站对高频率 API 访问有反爬 / 风控机制，请务必注意：

- **配置 Cookie**：匿名模式只能访问公开端点，风控概率更高。填写完整凭据（尤其是 `sessdata`）可显著降低风险
- **不要激进轮询**：默认全局最小轮询间隔 60 秒已是较保守的值；不建议把订阅间隔调到 30 秒以下
- **全局令牌桶**：所有订阅共享一个令牌桶（速率=各启用订阅的聚合轮询需求，容量 3），每个订阅**每轮只取一枚令牌**，用于吸收多订阅对齐瞬间的突发；不会拖慢配置的轮询间隔（轮内分页请求不再逐个排队占桶）
- **随机抖动**：每次轮询附加 0~N 秒随机延迟，进一步打散请求节奏
- **后果**：触发风控时 API 返回 -412 / -352 等错误，插件会降频退避；长期高频访问可能导致**账号被限制**（影响 Cookie 对应账号本身），风险自负

---

## ❓ 常见问题（FAQ）

### Q: 为什么收不到推送？

请按顺序排查：

1. **会话格式 / 平台**：`push_session_ids` 必须是 `platform:message_type:session_id` 完整格式，且平台支持主动消息（`qq_official` 不支持，请用 `aiocqhttp`）。可用 WebUI「测试推送」验证
2. **订阅未启用**：确认 `enabled` 为 `true`
3. **订阅被跳过**：查看插件日志，非法配置项（type 非法、缺必填字段、会话全非法等）会在加载时被跳过
4. **自动禁用**：订阅连续失败 10 次会被自动禁用（WebUI 状态面板显示琥珀色「自动禁用」徽章），重启后恢复
5. **首次轮询静默**：插件设计为首轮静默记录当前状态，不会推送历史内容，只有后续检测到的新内容才会推送

### Q: 升级后突然洪水推送历史动态怎么办？

早期版本存在动态解析缺陷（polymer `feed/space` 的 `id_str` 未被识别），会在去重记录为空的情况下仍标记「已种子化」。修复版引入了**第二代种子标记**（`dynamic_state_v2` / `collection_state_v2`）：升级后首次轮询会**静默重新记录当前可见内容、不推送历史**，之后仅推送真正新增的内容。若仍出现洪水，请删除 `data/plugin_data/astrbot_plugin_bilibili_cj/state.db` 后重启插件（会全部重新静默种子化一次）。

### Q: 匿名模式（不填 Cookie）能用吗？

能，但有限制。不填凭据时插件以匿名身份调用 B 站公开端点，直播 / 动态 / 合集三套监控都会尽力运行，启动时会有日志告警提示。匿名模式风控概率更高、部分接口可能受限，建议填写 `sessdata` 等凭据。

### Q: 为什么改配置没生效？

- 插件带**配置文件监视器**：手动修改 `data/config/astrbot_plugin_bilibili_cj_config.json` 后约 5 秒内自动热重载（无需重启）。订阅变更即时生效
- 例外：**`webui.host` / `webui.port` / `webui.enabled` 变更需要在 AstrBot 中重载插件**（监听地址 / 端口在启动时绑定）
- WebUI 面板中保存的订阅 / 设置变更会立即生效（自动触发重建）

### Q: Cookie 过期了怎么办？

重新从浏览器获取最新 Cookie 值并更新配置（见《凭据获取》一节）。若配置了 `ac_time_value`，SDK 会尝试自动刷新。

### Q: 数据存在哪里？配置和程序会混在一起吗？

完全隔离，三者物理分离：

| 类别 | 位置 |
|------|------|
| **程序** | `data/plugins/astrbot_plugin_bilibili_cj/`（插件代码 + WebUI 静态资源） |
| **配置** | `data/config/astrbot_plugin_bilibili_cj_config.json`（AstrBot 管理） |
| **数据** | `data/plugin_data/astrbot_plugin_bilibili_cj/state.db`（SQLite：已推送记录、直播状态等） |

日志走 AstrBot 统一 logger（`data/logs/astrbot.log`）。删掉插件目录不会丢失配置与状态数据；反之亦然。

---

## 📂 项目结构

```
astrbot_plugin_bilibili_cj/
├── main.py                # 插件入口：BilibiliMonitor(Star) + /bili 指令 + 历史名字重导出
├── lifecycle.py           # 组件装配 / 启停 / 登录状态监控
├── config_reloader.py     # 配置热重载器（防抖 + watcher + 状态清理）
├── config_file.py         # 配置文件路径解析 / 安装兜底初始化 / 批量配置合并
├── util.py                # 跨模块公共小工具（logger / 时间 / 取牌 / 类型标签）
├── config.py              # 配置校验与规范化（normalize）
├── _conf_schema.json      # AstrBot 配置面板的 Schema 定义
├── config.example.json    # 配置模板（含三类订阅示例）
├── config.json            # （可选）批量配置：放置后启动自动读入合并
├── metadata.yaml          # 插件元信息
├── requirements.txt       # Python 依赖
├── db.py                  # SQLite 数据层（去重 / 直播状态 / seed 状态）
├── push.py                # 推送模板与发送
├── scheduler.py           # 任务编排、限速、退避、自动禁用
├── poller/                # 轮询器：live.py / dynamic.py（调度）/ dynamic_parser.py（动态消息解析）/ collection.py
├── repository/            # B站 API 类型化封装（唯一允许 import bilibili_api 的层）
└── webui/                 # 独立 WebUI：server.py + index.html + app.js + style.css
```

---

## 🔧 依赖

| 依赖 | 用途 |
|------|------|
| `bilibili-api-python` | 调用 B 站 API，获取直播 / 动态 / 合集信息 |
| `aiohttp` | WebUI HTTP 服务 |
| `aiosqlite` | SQLite 异步数据层 |
| `astrbot.api`（运行时提供） | AstrBot SDK：Star 基类、配置、消息链、日志等 |

---

## 📄 许可证

MIT License

## 🙏 致谢

- [AstrBot](https://github.com/Soulter/AstrBot) — 优秀的聊天机器人框架
- [bilibili-api-python](https://github.com/Nemo2011/bilibili-api) — B 站 API Python SDK
