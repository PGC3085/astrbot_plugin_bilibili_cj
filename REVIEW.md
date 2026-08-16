# astrbot_plugin_bilibili_cj 代码快速审查指南

> 面向准备 review 本仓库的读者。读完本文约 10 分钟即可掌握架构、核心不变量与
> 高风险点,并直接定位到需要细看的代码。
>
> 配套文档:`task01.md`(迁移需求)、`task02.md`(第二阶段需求)、
> `对话总结.md`(开发全记录与迭代历史)。

---

## 1. 项目是什么

AstrBot 插件:监控 B 站 UP 主**直播开播/下播、动态更新、合集更新**,检测到新
内容后自动推送到多平台会话(aiocqhttp / telegram / discord / kook / lark)。

- 生产部署:`/home/ubuntu/astr/AstrBot`(Linux),插件目录
  `astrbot_plugin_bilibili_cj`;
- 状态库:`<data>/plugin_data/astrbot_plugin_bilibili_cj/state.db`(SQLite + WAL);
- 配置:`<data>/config/astrbot_plugin_bilibili_cj_config.json`
  (字段说明见 `_conf_schema.json`);
- 消息构造参考 DDBOT-WSa 的 `notify.group.bilibili.news.tmpl` 原逻辑。

---

## 2. 架构总览

```
main.py                     Star 入口 + /bili 指令(仅运行时接线)
└─ lifecycle.py             组件装配 / 启停顺序 / 登录状态监控
   ├─ config.py             配置校验规范化(normalize → Subscription 列表)
   ├─ config_file.py        配置路径 / 安装兜底 / 批量配置(首次部署合并)
   ├─ config_reloader.py    热重载:200ms 防抖 + 5s watcher + 身份变更清状态
   ├─ scheduler.py          每订阅一个独立任务 + 令牌桶限速 + 指数退避
   │                        + 10 连败自动禁用 + 下播快速复查
   │   ├─ poller/live.py               开播/下播/改标题状态机
   │   ├─ poller/dynamic.py            动态轮询(去重/重试/seed)
   │   │   └─ poller/dynamic_parser.py 纯函数解析层:双代 API + DDBOT 消息构造
   │   └─ poller/collection.py         合集全量扫描
   ├─ push.py               模板渲染 / MessageChain 组装 / 逐会话投递
   ├─ db.py                 aiosqlite 数据层(全部表按 sub_id 隔离)
   ├─ repository/bili.py    bilibili_api SDK 封装 + 类型化异常
   └─ webui/server.py       aiohttp 静态 + JSON API + Bearer 鉴权
       └─ webui/app.js      原生 JS 管理页
```

**两条贯穿始终的原则**(审查时先验证这两点):

1. **离线可测**:所有 `astrbot.api` / `bilibili_api` 导入都在 `try/except` 守卫
   下;时钟、睡眠、随机、仓库、数据库全部可注入 → 无 AstrBot、无 SDK 也能跑
   全量单测。
2. **错误绝不杀死任务**:轮询器吞掉业务异常并写 `status[sub_id].last_error`
   (调度器据此退避/禁用);只有 `asyncio.CancelledError` 透传(正常 shutdown)。

---

## 3. 模块职责与审查要点

| 模块 | 职责 | 审查要点 |
| --- | --- | --- |
| `config.py` | 配置校验;`coerce_bool` 防字符串布尔 | 会话格式 `platform:type:id`;平台能力过滤;NaN/inf 钳制;uid/list_id 解析 |
| `config_file.py` | 路径解析、schema 默认值、JSON 读写、深度合并 | BOM 兼容(utf-8-sig);批量配置只在**无有效订阅**时合并 |
| `config_reloader.py` | 热重载唯一入口 `request_rebuild` | 防抖循环的锁/waiter 结算;`_closing` 后不重建;快照比对基准 |
| `scheduler.py` | 任务编排、令牌桶、退避、自动禁用、快速复查 | `_bucket_rate` 覆盖聚合需求;`_interval_for` 非有限防护;`_run_sub` 内层快速复查循环 |
| `poller/live.py` | 直播状态机 | seed 静默;2 连确认 + 15s 快速复查;pending_push 重投/过期;重启抑制 |
| `poller/dynamic.py` | 动态轮询 | seed v2 表防升级洪水;mark-after-send 重试;直播分享默认不推 |
| `poller/dynamic_parser.py` | 动态→消息纯函数 | 双代 API(整数 type 与 `DYNAMIC_TYPE_*`);`MAJOR_TYPE_OPUS` 裁决;转发 `orig` 解析 |
| `poller/collection.py` | 合集扫描 | 全量翻页 + `_MAX_PAGES=50` 上限;bvid 去重;标题 None 兜底 |
| `push.py` | 消息模板与投递 | 事件时间 UTC+8;500 字截断**链接必保留**;图片失败降级纯文本 |
| `db.py` | SQLite 数据层 | 列白名单防注入;`sub_id` 全表隔离;WAL;prune 窗口 > 10 页扫描窗 |
| `repository/bili.py` | SDK 封装 | 超时/CancelledError 顺序;ResponseCode→类型化异常;凭据白名单 |
| `webui/server.py` | WebUI 后端 | Bearer 常量时间比对;`/assets` 防路径穿越;配置写锁=重建锁 |
| `webui/app.js` | 管理页 | 401 令牌门;状态卡 UTC+8 展示;订阅编辑器按 id 定位 |
| `lifecycle.py` | 装配与启停 | 启动/关停顺序;登录监控逐轮读实时配置 |

---

## 4. 三条关键数据流

### 4.1 配置加载与热重载

```
启动:ensure_config_file(缺失按 schema 兜底) → 读 config.json(仅无有效订阅时)
     → normalize(raw) → Subscription[] → Scheduler(...) → reloader.seed_active_config
热重载:WebUI 保存(锁内 normalize+落盘,锁外 request_rebuild) 或
      watcher 每 5s 比对 (size, mtime_ns) → request_rebuild
     → 防抖合并 → 重读磁盘 → 与快照比对(no-op/rebuilt) → 身份变更 delete_sub_state
     → scheduler.rebuild → 持久化 → 换快照
```

**双锁约定**(todo 11,最容易看错的地方):WebUI 的配置写锁与 reloader 的重建锁是
**同一把** `asyncio.Lock`;写路径在锁内 normalize+save,**释放后**才调
`request_rebuild`(其内部自行取锁)。绝不跨 `request_rebuild` 持锁,否则非重入死锁。

### 4.2 轮询调度

```
每个订阅一个 asyncio.Task(_run_sub):
  睡眠 max(订阅间隔, global_min) + rand(0, jitter)
  → 取令牌桶一枚(per-poll 限速,轮内分页不限速)
  → poller.poll() 一轮
  → 出错:退避 2^n(上限 300s),连续 10 次自动禁用并推送告警
  → 直播订阅处于"下播确认中"(已观测离线未通知):15s 快速复查,不等到下个整间隔
```

- 令牌桶速率 = 全部启用订阅的 `Σ 1/间隔`(不低于 `1/global_min`)→ 桶不成为
  瓶颈,各订阅实际间隔贴近配置值;
- 时钟**双基准**:令牌桶用 `time.monotonic`(抗系统时间跳变),直播轮询器用
  `time.time`(epoch 语义)——混用曾导致"下播时间 1970"事故,勿改。

### 4.3 推送链路

```
poller 组装 payload(type 专属字段,事件时间已格式化为 UTC+8)
  → push.build_chain(event_type, payload):
      模板渲染(500 字截断后链接行回补) → MessageChain 文字在前,
      images/cover 图片追加在链尾(飞书图文顺序兼容) → 图片失败降级纯文本
  → push.send(subscription, chain, context, status):
      逐会话 validate_session → context.send_message → 逐会话记日志
      → 任一成功:last_push_at=now、清 last_error;全失败:记 last_error
  → mark-after-send:轮询器侧任一 session 成功即视为已见;全失败按轮重试
      (live: pending_push 重投 ≤3 次 / dynamic+collection: retry_counts ≤3 轮)
```

---

## 5. 核心不变量(必须守住)

| # | 不变量 | 所在位置 |
| --- | --- | --- |
| 1 | **首轮静默 seed 防洪水**:动态/合集首轮只记去重不推送;seed 标志用 v2 表(`dynamic_state_v2`/`collection_state_v2`),升级后强制静默重 seed | `poller/{dynamic,collection}.py` |
| 2 | **mark-after-send**:先持久化去重标记再推送;全失败保留重试计数(≤3 轮) | 三个 poller 的 `_handle_item` |
| 3 | **错误信号**:轮询失败必须写 `status[sub_id].last_error` 并递增 `error_count`,否则调度器无法退避/禁用 | `_record_error`(三个 poller 同款) |
| 4 | **时间语义**:推送内事件时间一律 UTC+8;`last_poll` 记录观测完成时刻 | `push._EVENT_TZ`、`scheduler._poll_one` |
| 5 | **链接永不被截断**:正文截断后 `链接：{url}` 回补到末尾 | `push.text_for` |
| 6 | **字符串布尔**:配置值必须经 `coerce_bool`(`"false"` ≠ True) | `config.coerce_bool` 及所有调用点 |
| 7 | **凭据白名单**:只允许 sessdata/bili_jct/buvid3/buvid4/dedeuserid/ac_time_value 进入 SDK | `repository/_CREDENTIAL_FIELDS` |
| 8 | **任务可取消**:所有 while 循环吞业务异常、透传 CancelledError;terminate 幂等 | scheduler/reloader/lifecycle |
| 9 | **配置写锁=重建锁**,且绝不跨 `request_rebuild` 持锁 | `webui/server.py`、`config_reloader.py` |

---

## 6. 高风险点审查清单

### 6.1 安全

- [ ] Bearer 令牌比对用 `hmac.compare_digest`(常量时间)——`webui/server.py:_auth_middleware_factory`
- [ ] `/assets/{path}` 有 `.resolve()` + `is_relative_to` 防路径穿越
- [ ] SQL 动态列名仅来自白名单 `_LIVE_STATE_COLUMNS` / `_SEED_TABLES` / `_ALL_TABLES`——`db.py`
- [ ] **token 会完整打印到日志**(`_ensure_token`)与 `/api/settings` 响应——**有意保留**
  (历史评审 #1,用户明确不修),审查时勿当缺陷重复提交
- [ ] 会话字符串在 `push.send` 发送前经 `validate_session` 形状校验

### 6.2 并发与任务生命周期

- [ ] `TokenBucket.acquire` 取消安全(锁内无 await 长眠)
- [ ] `Database` 全部读写走同一把 `asyncio.Lock`,aiosqlite 连接不跨 event loop 复用
- [ ] `Scheduler.rebuild` 取消旧任务→重建桶与 poller→再启动;`status`/`retry_counts`
      跨重建保留,身份变更由 reloader 先行清库
- [ ] 所有后台任务(watcher/维护/登录监控/轮询)在 terminate 中可取消,重复调用幂等

### 6.3 数值与边界

- [ ] poll 设置与订阅间隔对 NaN/inf 的钳制(config.normalize + scheduler 二道防线)
- [ ] 500 字截断、`format_event_time` 对 0/负数/非法值返回空串
- [ ] 合集翻页上限 `_MAX_PAGES=50`、动态翻页上限 `_MAX_PAGES=10`(防请求风暴触发 -412 风控)
- [ ] 推送封面开关(`push_*_cover`)为字符串布尔时经 `coerce_bool` 正确生效

### 6.4 消息正确性(对照 DDBOT)

- [ ] 动作句式/类型专属行/转发标注行与 `news.tmpl` 一致:`poller/dynamic_parser.py`
- [ ] `MAJOR_TYPE_OPUS` 图文动态正文与多图不丢;转发取 `orig` 而非转发语
- [ ] 直播分享动态(4308)默认不推(`push_dynamic_live_share=False`),避免与开播/下播重复

---

## 7. 如何运行测试与静态检查

```bash
# 全部离线(无需 AstrBot、无需 bilibili_api;仅需 pytest + aiosqlite)
cd astrbot_plugin_bilibili_cj
python -m pytest -q          # 期望:268 passed, 1 skipped

python -m ruff check .       # 期望:All checks passed!
python -m ruff format --check .
node --check webui/app.js    # 期望:无输出
```

测试分层:`tests/` 下按模块一个文件;fake repository / 假时钟 / 假 sleep 全确定性;
`test_smoke.py` 覆盖三类订阅的全链路;`test_reload.py` 覆盖热重载与状态清理。

---

## 8. 已知取舍(审查时不必当作缺陷"纠错")

1. **下播时间 = 检测时间**:B 站接口不返回真实下播时刻;2 连确认 + 15s 快速复查
   已将误差缩到约「1 个轮询间隔 + 15s」。
2. **合集翻页上限 50 页**:超出部分的新视频本轮不检测(实际合集规模远小于此)。
3. **批量配置只首次合并**:插件目录 `config.json` 仅在现有配置无有效订阅时合入,
   避免重启时覆盖用户后续修改。
4. **未知平台一律放行**:平台能力检查只拦已知不支持主动消息的平台
   (qq_official 等),避免误杀未来适配器。
5. **token 打印日志 / `/api/settings` 返回 token**:历史评审 #1,用户明确保留。
6. **`push.py` 模块文档为英文**:纯风格遗留,其余模块中文。

---

## 9. 版本历史(快速定位变更)

| 版本 | 关键内容 |
| --- | --- |
| v1.0.x~v1.1.4 | 插件骨架、配置批量载入、WebUI、防洪水 seed v2、截断保链接、epoch 时钟修复、字符串布尔 |
| v1.1.5 | 动态消息构造按 DDBOT news.tmpl 重写 |
| v1.1.6 | opus 图文解析 + 动态多图推送 |
| v1.2.0 | main 生命周期/热重载拆分为独立模块 |
| v1.2.1 | 下播延迟修复:2 连确认 + 15s 快速复查、last_poll 观测后写入 |
| v1.2.2 | 评审修复:字符串布尔、非有限数值防护、合集翻页上限、批量配置合并门槛、登录监控实时凭据 |
| v1.2.3 | 下播「时长」改为可读格式(`format_duration`:`3小时2分26秒`),不再输出原始秒数 |
