<div align="center">

# Rapid Inbox

**本地优先的临时邮箱服务**

高吞吐 C++ SMTP 收件入口、公开收件箱、管理后台和 HTTP API<br/>
邮件、附件、元数据和审计全部落本地磁盘与 SQLite

[![Python](https://img.shields.io/badge/python-3.10%2B-3776AB?logo=python&logoColor=white)](https://www.python.org)
[![C++](https://img.shields.io/badge/C%2B%2B-20-00599C?logo=cplusplus&logoColor=white)](https://isocpp.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.136-009688?logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?logo=sqlite&logoColor=white)](https://www.sqlite.org)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange.svg)](CHANGELOG.md)

[快速开始](#快速开始) · [Demo](#demo) · [路线图](https://github.com/wendaochangsheng/Rapid-Inbox/issues/5) · [特性](#特性) · [配置](#配置) · [使用](#基本使用) · [贡献](CONTRIBUTING.md) · [安全](SECURITY.md)

</div>

---

## 简介

Rapid Inbox 是一个本地优先的临时收件服务，面向 **验证码收件**、**测试环境邮件捕获**、**内部工具联调** 和 **轻量自托管** 场景。

核心目标是把 **收得到、看得清、管得住、容易恢复** 做好，不依赖外部邮件服务或云端数据库。

> 项目目前处于早期版本（Alpha），接口和数据结构可能继续调整。

## Demo

![Rapid Inbox 从公开收件箱到邮件详情的演示](docs/assets/rapid-inbox-demo.gif)

<details>
<summary>查看静态截图</summary>

![Rapid Inbox 邮件详情静态截图](docs/assets/rapid-inbox-demo.png)

</details>

> 演示使用隔离环境中的脱敏测试数据，不包含真实邮箱、邮件或凭据。

## 特性

| 分类 | 能力 |
| --- | --- |
| **邮件接收** | C++ `rapid-inbox-ingestd` 提供 durable ACK、字节级背压、group commit、多 worker MIME 解析和毒任务隔离；Python SMTP 保留为开发模式 |
| **域名模式** | 支持仅接收已配置域名，或对到达本服务的 SMTP 投递启用 `managed_plus_catchall` 任意域模式；最长后缀规则优先，域规则可热刷新 |
| **收件箱** | 域名默认私有；邮箱公开位默认启用，但只在域级公共开关开启后生效，且可按邮箱单独关闭；支持列表、详情、原始 EML、沙箱 HTML 预览和附件下载 |
| **实时更新** | 公开收件箱通过 WebSocket 推送，管理后台通过 SSE 查看 SMTP 接收事件 |
| **验证码识别** | 打分制提取算法，支持中英日韩西多语言上下文与字母数字/分隔符组合 |
| **权限管理** | `viewer` / `operator` / `superadmin` RBAC；API Key 按 kind、scope、域授权模式、邮箱 glob、IP、限速和有效期约束 |
| **HTTP API** | 推荐 `/api/v2`：Bearer-only、严格模型、Problem Details、稳定 cursor；保留 `/api/v1` 公开和管理接口 |
| **可观测性** | JSON/text 结构化日志、安全 Request ID、Prometheus 指标、live/ready 探针和缓存化运维仪表盘 |
| **持久化与恢复** | SQLite WAL 保存索引；磁盘保存 raw / text / html / attachments / manifests；manifest 可重建未提交元数据 |
| **清理系统** | 按投递保留期分批清理；事务内登记文件 GC，事务外删除并指数退避重试；独立清理会话、空邮箱、指标和审计 |
| **维护工具** | 跨进程 `.maintenance.lock` 协调清空邮件，暂停新收件后清理文件并压缩 SQLite |

## 技术栈

`C++20` · `Python 3.10+` · `FastAPI` · `aiosmtpd` · `Jinja2` · `SQLite` · `Uvicorn` · `WebSocket` · `SSE`

## 快速开始

```bash
bash quickstart.sh
```

脚本会自动创建 `.venv`、安装依赖、复制 `.env.example`，先由 Python 完成 SQLite schema 初始化和
轻量迁移，成功后才会启动 C++ ingestd，从而避免首次启动时 ingestd 抢先读取尚不存在的表。默认从
GitHub Releases 下载并校验 SHA-256 后使用预编译 ingestd。默认绑定：

```text
HTTP: 127.0.0.1:8000
SMTP: 0.0.0.0:25
```

默认 quickstart 会在 `0.0.0.0:25` 启动 C++ SMTP ingestd。邮件元数据、text/html 正文、附件和验证码会由 ingestd 直接写入现有 SQLite 数据库和 `storage/` 目录；Python 服务只负责 HTTP、管理后台和公开 API。
数据库初始化或迁移失败时，脚本会整体退出，不会启动 HTTP 或 ingestd。

打开管理后台：

```text
http://127.0.0.1:8000/admin/login
```

首次运行会创建 bootstrap 管理员。`quickstart.sh` 在第一次复制 `.env.example`
时生成随机密码，并在终端中只显示一次：

```text
用户名：admin
密码：<quickstart 输出的随机密码>
```

> 首次 bootstrap 管理员登录后，后台会**强制**进入系统设置页修改初始密码；完成改密前不能访问其他后台页面。
> 如果绕过 quickstart 手工使用 `change-me-now`，将 `HOST` 配为非回环地址时服务会拒绝以不安全默认凭据启动。

默认启动器使用当前工作目录作为项目运行目录。从仓库根目录启动时，数据会写入：

```text
./storage/
./storage/app.db
```

> [!WARNING]
> 首次对外部署前，请确认 bootstrap 密码已更换，并确认 Metrics Token 已由 quickstart 生成或手工配置。兼容用
> `ADMIN_TOKEN` / `PUBLIC_API_KEY` 默认不启用；新接入应使用后台签发的 API Key。HTTP 默认只监听
> `127.0.0.1`；显式改为非回环地址时 quickstart 会输出醒目警告，管理面必须放在可信 HTTPS 反向代理后，
> 不应把无 TLS 的 Uvicorn 直接暴露到不可信网络。

如果希望强制本地编译 C++ ingestd，而不是下载 GitHub Release 二进制：

```bash
bash quickstart.sh --build-local
```

如果要下载已审核的指定版本或指定二进制地址：

```bash
bash quickstart.sh --ingestd-version "$REVIEWED_INGESTD_TAG"
bash quickstart.sh --binary-url https://example.com/rapid-inbox-ingestd-linux-x86_64.tar.gz
```

其中 `REVIEWED_INGESTD_TAG` 应由部署方设置为仓库中实际存在且已审核的标签。
自定义二进制地址应提供同路径 `.sha256` 文件；也可通过 `INGESTD_SHA256=<64 位十六进制值>`
显式传入可信校验和。校验失败时不会执行下载的文件。

未指定版本时脚本仍使用 GitHub 的可变 `latest` 指针，并明确输出漂移警告。相邻 `.sha256` 只能证明
下载内容与该次发布资产一致，不能固定版本；可重复部署应使用仓库中实际存在且已经审核的 release tag，
或从固定、已审核的源码提交使用 `--build-local`。本文不假定当前一定存在某个具体 release tag。

> 当前预编译二进制目标为 Linux x86_64。非 Linux x86_64、下载失败或指定 `--build-local` 时，quickstart 会回退到本地编译。

如果本机需要本地编译，可先安装：

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip cmake g++ libsqlite3-dev libssl-dev libunistring-dev libicu-dev
```

只想使用 Python 内嵌 SMTP 兼容模式时：

```bash
bash quickstart.sh --python-smtp
```

## 启动方式

<details>
<summary><b>C++ SMTP ingestd + Python HTTP</b>（高吞吐生产模式，推荐）</summary>

```bash
# 1. 构建 C++ SMTP 收件入口
cmake -S cpp/ingestd -B cpp/ingestd/build
cmake --build cpp/ingestd/build

# 2. 启动 Python HTTP，不启用内嵌 SMTP
.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000

# 3. 启动 C++ SMTP 收件入口
SMTP_HOST=0.0.0.0 SMTP_PORT=25 cpp/ingestd/build/rapid-inbox-ingestd --base-dir .
```

默认 `INGEST_DURABLE_ACK=true`。SMTP 返回 `250 queued` 前，ingestd 已以原子替换方式写入
raw EML 和 pending manifest；SQLite 元数据可以稍后批量提交，若进程在此期间退出，Python
恢复器会从 manifest 重建记录。`INGEST_STORAGE_FSYNC=false` 只保证进程级崩溃恢复；需要覆盖
主机掉电时，将其设为 `true`，代价是更高的磁盘同步延迟。关闭 durable ACK 会退回“仅进入
内存队列即确认”的高吞吐模式，此时异常退出可能丢失已返回 `250` 的邮件。
每批 SQLite 提交都会在事务内重新匹配全部收件人；并发 rename/delete 或向其它租户降级改投会
以 `policy conflict` 回滚。若 durable ACK 已先返回，raw 与 manifest 会保留并进入明确的
quarantine 取证路径，恢复器依据持久 tombstone 禁止用陈旧 manifest 复活已改名/删除域。

</details>

<details>
<summary><b>HTTP + Python 内嵌 SMTP 同进程</b>（开发/兼容模式）</summary>

```bash
.venv/bin/rapid-inbox-http
```

</details>

<details>
<summary><b>仅启动 Python SMTP 监听器</b>（兼容模式）</summary>

```bash
.venv/bin/rapid-inbox-smtp
```

</details>

<details>
<summary><b>开发模式（模块入口）</b></summary>

```bash
.venv/bin/uvicorn app.main:app --reload
```

直接使用 `uvicorn app.main:app` 时**不会**启用内嵌 SMTP。需要接收 SMTP 邮件时，生产推荐另开进程运行 `rapid-inbox-ingestd`，开发可使用 `rapid-inbox-http` 或 `rapid-inbox-smtp`。

</details>

## 发布二进制

仓库包含 GitHub Actions 工作流 `.github/workflows/release-ingestd.yml`：

- 普通 push / pull request：运行 Python 测试并构建、测试 C++ ingestd。
- 推送 `v*` tag：构建 Linux x86_64 release 包，并把以下文件发布到 GitHub Release：
  - `rapid-inbox-ingestd-linux-x86_64.tar.gz`
  - `rapid-inbox-ingestd-linux-x86_64.tar.gz.sha256`

发版示例：

```bash
git tag "$NEW_RELEASE_TAG"
git push origin "$NEW_RELEASE_TAG"
```

`NEW_RELEASE_TAG` 应由发布者按实际版本策略显式设置；本文不声明尚未发布的具体 tag。

Release 发布完成后，`bash quickstart.sh` 默认从可变的 latest release 下载预编译 ingestd 并输出漂移警告。
可重复部署应显式传入已审核 tag；需要本地编译时使用 `--build-local`。

## 配置

启动器读取变量的优先级：

```text
真实环境变量  >  当前工作目录下的 .env  >  app/config.py 默认值
```

<details>
<summary><b>完整环境变量表</b></summary>

Python HTTP、兼容 SMTP 与共享配置：

| 变量 | `.env.example` | 说明 |
| --- | --- | --- |
| `STORAGE_ROOT` | `./storage` | 邮件文件、附件、manifest 和临时文件根目录 |
| `DATABASE_PATH` | `./storage/app.db` | SQLite 数据库路径；Python 与 ingestd 必须指向同一文件 |
| `BOOTSTRAP_ADMIN_USERNAME` | `admin` | 首次启动自动创建的管理员用户名 |
| `BOOTSTRAP_ADMIN_PASSWORD` | `change-me-now`（quickstart 随机替换） | 手工启动的代码回退值也是 `change-me-now`，不可用于外网绑定 |
| `SESSION_COOKIE_NAME` | `rapid_inbox_session` | HttpOnly 管理员会话 Cookie 名称 |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | HTTP 监听地址与端口；非回环绑定必须置于可信 HTTPS 反向代理后 |
| `HTTP_MAX_REQUEST_BODY_BYTES` | `1048576` | ASGI 请求体上限，同时约束 `Content-Length` 与 streamed/chunked body；最大可配 64 MiB |
| `HTTP_REQUEST_BODY_TIMEOUT_SECONDS` | `15` | 完整接收单个 HTTP 请求体的总时限，抵御慢速分块上传 |
| `HTTP_BODY_MEMORY_BUDGET_BYTES` | `268435456` | 每进程所有已缓冲 HTTP 请求体的共享字节预算，必须不小于单请求上限 |
| `HTTP_CONCURRENCY_LIMIT` | `1000` | 每进程 HTTP/WebSocket 总准入上限；quickstart 同时传给 Uvicorn `--limit-concurrency`，应用中间件也执行 |
| `HTTP_LIVE_CONNECTION_LIMIT` | `256` | 每个 HTTP 进程共享的管理 SSE 与公共邮箱 WebSocket 长连接上限，超限返回 503/1013 |
| `DATABASE_WRITE_QUEUE_CAPACITY` / `DATABASE_WRITE_MAX_WAITERS` | `256` / `1024` | SQLite 单写 actor 已接管请求与等待请求的双重上限，超限快速返回 503 |
| `DATABASE_READ_POOL_SIZE` / `DATABASE_READ_QUEUE_CAPACITY` / `DATABASE_READ_MAX_WAITERS` / `DATABASE_READ_TIMEOUT_SECONDS` | `1` / `256` / `1024` / `5` | API v2 专用只读 actor、已接管请求与等待请求上限和端到端读时限。短查询的 Python 行物化受 GIL 约束，默认单 actor 实测更快；仅在长查询压测确认收益后增加连接数。维护会先排空请求并由 owner 线程关闭连接；这些数值均按 HTTP 进程分别计算 |
| `SMTP_HOST` / `SMTP_PORT` | `0.0.0.0` / `25` | SMTP 监听地址与端口 |
| `MAX_MESSAGE_SIZE_BYTES` | `52428800` | 单封邮件最大体积，Python 与 C++ 共用 |
| `MAX_RECIPIENTS_PER_MESSAGE` | `20` | 单封邮件最大 canonical 收件人数 |
| `SMTP_IDLE_TIMEOUT_SECONDS` | `30` | SMTP 会话空闲断开时间 |
| `SMTP_MAX_CONCURRENT_CONNECTIONS` | `1024` | Python SMTP 并发连接上限；非回环监听不允许配置为 `0` |
| `SMTP_CONNECTION_RATE_LIMIT_COUNT` | `60000` | Python/C++ SMTP 每 IP 建连次数；保留高突发吞吐的同时约束连接抖动状态。来源 IP 状态使用摊销 O(1) 过期/LRU，按并发上限的 4 倍分配且硬封顶 65536 项，IPv6 地址轮换不能造成无界内存或全表扫描 |
| `SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS` | `60` | Python/C++ SMTP 每 IP 建连滑窗 |
| `SMTP_CLOSE_AFTER_DATA` | `true` | Python SMTP 完成一封 DATA 后是否关闭连接 |
| `PARSE_WORKER_COUNT` | `4` | Python 恢复/兼容解析 worker 数量 |
| `PARSE_QUEUE_MAX_MESSAGES` | `10000` | Python MIME 队列的 queued + active 消息预算 |
| `PARSE_QUEUE_MAX_BYTES` | `536870912` | Python MIME 队列的 queued + active 原文预算；必须不小于 `MAX_MESSAGE_SIZE_BYTES` |
| `MESSAGE_PREVIEW_BODY_BYTES` | `131072` | 公共/管理详情中 text 与 HTML 各自最多读取的 UTF-8 源字节数；响应包含原始大小与截断标记，最大可配 16 MiB |
| `MESSAGE_PREVIEW_HEADERS_BYTES` | `65536` | 详情允许反序列化的邮件头 JSON 上限；超限返回空列表及截断标记，最大可配 1 MiB |
| `MESSAGE_PREVIEW_INLINE_ITEM_BYTES` | `65536` | HTML CID 预览中单个内联图片的源字节预算；超限保留原 CID，不影响附件下载 |
| `MESSAGE_PREVIEW_INLINE_TOTAL_BYTES` | `262144` | 单次 HTML 预览可聚合的全部内联图片源字节预算；单项预算不得超过总预算 |
| `FSYNC_STORAGE_WRITES` | `false` | Python 文件写入是否执行文件和目录 fsync |
| `INGRESS_MODE` | `managed_only` | `managed_only` 或 `managed_plus_catchall` |
| `CATCH_ALL_PUBLIC_WEB_ENABLED` | `false` | 任意域策略是否允许公共 Web 查阅 |
| `CATCH_ALL_PUBLIC_API_ENABLED` | `false` | 任意域策略是否允许公共 API 查阅 |
| `CATCH_ALL_RETENTION_DAYS` | `0` | 任意域投递保留天数；`0` 表示不自动过期 |
| `RETENTION_CLEANUP_INTERVAL_SECONDS` | `30` | 后台清理调度间隔 |
| `SMTP_SESSION_RETENTION_SECONDS` | `86400` | 已结束 SMTP 会话保留时间 |
| `EMPTY_MAILBOX_RETENTION_SECONDS` | `86400` | 无投递空邮箱保留时间 |
| `METRIC_RETENTION_SECONDS` | `604800` | 邮件指标 bucket 保留时间 |
| `AUDIT_RETENTION_DAYS` | `90` | 审计日志保留天数 |
| `CLEANUP_BATCH_SIZE` / `FILE_GC_BATCH_SIZE` | `1000` / `500` | 每轮数据库清理和文件 GC 上限 |
| `MAINTENANCE_RUN_RETENTION_DAYS` | `30` | 已完成/失败维护运行记录的保留天数 |
| `QUARANTINE_RETENTION_DAYS` | `30` | quarantine 取证文件保留天数 |
| `ORPHAN_ARTIFACT_GRACE_SECONDS` | `86400` | 扫描无引用 raw/text/html/附件前的最小文件年龄，避免清理在途产物 |
| `ARTIFACT_SWEEP_BATCH_SIZE` | `500` | quarantine 与 orphan 扫描每轮各自最多检查的文件数；pass 会跨清理轮次续跑 |
| `DISK_WARNING_THRESHOLD_PERCENT` | `85` | 仪表盘磁盘使用率告警阈值 |
| `LOG_LEVEL` / `LOG_FORMAT` | `INFO` / `json` | 日志级别和 `json` / `text` 格式 |
| `REQUEST_LOG_ENABLED` | `true` | 是否记录不含查询串的结构化 HTTP 访问日志 |
| `METRICS_ENABLED` / `METRICS_TOKEN` | `true` / 空 | Prometheus 指标开关与令牌；非回环绑定启用指标时令牌必填，否则拒绝启动 |
| `API_CURSOR_SECRET` | 空（quickstart 随机填充） | API v2 cursor HMAC 密钥；手工外网部署必须配置至少 32 个字符 |
| `READINESS_MIN_FREE_DISK_BYTES` | `67108864` | readiness 所需最小可用磁盘空间 |
| `ADMIN_TOKEN` / `PUBLIC_API_KEY` | 未启用 | v1 兼容令牌；仅显式配置非默认随机值时启用 |

预览预算是单请求上限（CID 图片转成 data URL 后还会有约 4/3 的 Base64 膨胀）；正文与内联预算同时调大时，最坏并发内存约随 `HTTP_CONCURRENCY_LIMIT` 成比例增长。生产环境应按可用内存联合设置这些值，完整内容继续通过流式原始邮件与附件下载获取。

C++ `rapid-inbox-ingestd` 专用热路径配置：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SMTP_MAX_CONNECTIONS` | `1024` | C++ SMTP 同时连接上限 |
| `SMTP_MAX_LINE_LENGTH` | `1000` | SMTP 命令/数据行上限 |
| `SMTP_LISTEN_BACKLOG` | `1024` | C++ SMTP 内核监听 backlog；`SMTP_HOST=::` 可监听 IPv6 |
| `SMTP_CONNECTION_RATE_LIMIT_COUNT` / `SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS` | `60000` / `60` | C++ 与 Python 共用的单 IP 建连滑窗；可按边缘滥用风险调低 |
| `INGEST_QUEUE_MAX_MESSAGES` | `10000` | 队列中 reservation、排队和处理中邮件总预算 |
| `INGEST_QUEUE_MAX_BYTES` | `536870912` | 字节预算；必须不小于 `MAX_MESSAGE_SIZE_BYTES` |
| `INGEST_RESERVATION_CHUNK_BYTES` | `65536` | DATA 按块增长的字节 reservation，避免每连接预占整封上限 |
| `INGEST_BATCH_MAX_MESSAGES` | `250` | 单次 group commit 最大邮件数 |
| `INGEST_FLUSH_INTERVAL_MS` | `5` | 等待同批邮件到达的最长时间 |
| `INGEST_SQLITE_BUSY_TIMEOUT_MS` | `5000` | SQLite 锁等待时间 |
| `INGEST_WORKER_COUNT` | `4` | 并行 MIME/文件处理 worker 数量 |
| `INGEST_MAX_RETRIES` | `3` | 单封失败后的有限重试次数，之后写入 quarantine |
| `DOMAIN_RELOAD_INTERVAL_MS` | `1000` | 从 SQLite 热刷新域规则的间隔 |
| `INGEST_DURABLE_ACK` | `true` | 在 SMTP `250` 前写入 raw + pending manifest |
| `INGEST_STORAGE_FSYNC` | `false` | ACK 前 fsync 文件和目录；开启后抗掉电但降低吞吐 |

</details>

## 基本使用

1. 启动服务并登录 `/admin/login`
2. 选择收件模式：添加一个或多个托管域名，或在系统设置中启用任意域模式
3. 为确需公开查阅的域名/任意域策略显式打开 Web 或 API；默认保持私有
4. 将测试邮件投递到任意匹配邮箱地址，例如 `code@example.com`
5. 在管理后台查看私有邮件，或通过已授权的公开页面/API 查阅
6. 为自动化客户端签发最小 scope、最小域授权的 API Key

### 收件模式与公开边界

- `managed_only`：只接收数据库中已启用、且 exact/subdomain 规则匹配的域名。
- `managed_plus_catchall`：托管域仍按最长后缀优先匹配，其余合法域名落到系统维护的
  `*` 策略；对已到达本 SMTP 服务的投递无需逐个预注册域名。
- 任意域模式不会改变公网 DNS 路由，也不能接收 MX 未指向本服务的第三方域名邮件。
  真实公网收件仍需为自己控制的域配置 A/MX，并确保 TCP 25 可达；测试也可让上游中继直接投递到本机。
- 新建托管域和 `*` 策略的公共 Web/API 开关默认都是关闭的。新邮箱的
  `public_enabled` 默认开启，但它只是域级开关下的二级门控；显式公开域后可再逐邮箱关闭。
  SMTP 能接收不代表匿名用户能查看；私有邮件始终可由有权限的管理员或服务 Key 查阅。
- 同一封邮件投递到多个收件人时按 canonical 邮箱去重。只有全部投递都到期后，共享的
  message/raw/附件才会进入最终清理。
- 新建更具体的托管后缀或修改 plus/case canonical 规则时，既有 catch-all/父域邮箱会在同一
  写事务中创建持久 `domain_rehome_jobs`。后台每批最多处理 1000 个邮箱并独立提交，批间允许
  SMTP writer 插队；取消、进程退出或暂时失败后会从 cursor 续跑。迁移只会向更具体的托管规则
  单向提升、合并重复投递并重算汇总；托管邮箱不会因规则停用而降回 `*`。

### 公开页面

```text
GET  /
GET  /mail/{mailbox_address}
GET  /mail/{mailbox_address}/{delivery_id}
GET  /mail/{mailbox_address}/{delivery_id}/raw
GET  /mail/{mailbox_address}/{delivery_id}/attachments/{attachment_id}
```

### 公开 API 示例

```bash
curl \
  -H "X-API-Key: <your-public-api-key>" \
  "http://127.0.0.1:8000/api/v1/public/mailboxes/code@example.com/messages"
```

支持 `limit`、兼容旧版的 `offset`，并返回 `next_cursor`。新集成建议使用 `next_cursor` 翻页：

```bash
curl \
  -H "X-API-Key: <your-public-api-key>" \
  "http://127.0.0.1:8000/api/v1/public/mailboxes/code@example.com/messages?limit=20&cursor=<next_cursor>"
```

新集成也可直接使用 v2 公开邮箱资源。`public` Key 通过 Bearer 认证，并继续受域授权模式、
邮箱 glob 和域/邮箱公共 API 开关约束：

```bash
curl \
  -H "Authorization: Bearer <ri_public_...>" \
  "http://127.0.0.1:8000/api/v2/public/mailboxes/code@example.com/messages?limit=20"
```

### API v2（推荐用于新集成）

`/api/v2` 接受 `admin`、`service` 和 `public` Key，但只接受标准 Bearer Header；查询参数或
`X-API-Key` 中的凭据会被拒绝。`public` Key 只能调用 `/me` 和持有 `public.read` 所允许的公开
邮箱资源；`service` / `admin` Key 仍按 kind、scope、域和邮箱授权逐层收窄。JSON 响应使用严格
envelope，文件下载使用对应媒体类型，错误使用
`application/problem+json`；列表 cursor 与
资源/筛选条件绑定并由 `API_CURSOR_SECRET` 做 HMAC 签名。轮换该密钥会立即使旧 cursor 失效。
OpenAPI 可从 `/docs` 或 `/openapi.json` 查看。

```bash
curl \
  -H "Authorization: Bearer <ri_service_...>" \
  "http://127.0.0.1:8000/api/v2/messages?limit=50"
```

当前 v2 覆盖 principal、公开邮箱的列表/验证码/详情/raw/附件、域名与 DNS 检查、邮箱、
邮件详情/raw/附件/重解析/删除、SMTP 会话与事件、审计、仪表盘、手动清理/清空、系统设置、
API Key 和管理员完整生命周期。管理实时流与跨邮件批量删除仍由 `/api/v1/*` 提供。新代码
应优先使用 v2 已覆盖的资源，并以 OpenAPI 中实际存在的 operation 为准。

所有 v2 cursor 都有 2048 字节输入上限、HMAC 签名并绑定资源与筛选条件；保留的 v1 公开邮箱
cursor 也绑定调用凭据和邮箱。v1 域列表与 SMTP events 使用硬分页，批量删除单次最多 1000 个
delivery ID。v2 API Key 列表还限制单请求最多扫描 5000 个候选；低权主体过滤掉大量历史 Key 时
会返回 continuation cursor，而不会为凑满一页无限扫描。

### 权限模型

后台会话使用三种角色：

- `viewer`：读取运行状态、域名、邮箱、邮件、SMTP、审计、设置、Key 元数据和管理员元数据。
- `operator`：在 viewer 基础上增加域名、邮箱和邮件处理写权限。
- `superadmin`：增加系统设置、API Key、管理员、密码重置和会话撤销等高风险权限；系统阻止
  删除或降级最后一个启用的 superadmin。

API Key 先按 kind 限制可选 scope：`public` 仅可使用 `public.read`，`service` 面向业务资源，
`admin` 才能持有 Key/管理员/系统写权限。域授权必须显式选择：

- `none`：不授予任何域；
- `selected`：只授予列出的域 ID；空列表仍表示拒绝；
- `all`：当前及未来所有域。

邮箱 glob、允许的 IP CIDR、Header/Query 传输方式、每分钟限额、到期时间和吊销状态会继续
收窄权限。新 Key 的空域列表不再隐式代表全域。Key 的创建、修改、轮换、吊销与删除会在同一个
SQLite writer 事务内重新读取调用者和目标权限，避免排队期间策略变化产生授权竞态。
管理员账号委派同样在写事务内重新授权：创建可登录账号必须同时具备
`admins.write` 与 `admins.credentials.write`，重置密码和撤销会话分别受
`admins.credentials.write` / `admins.sessions.write` 约束，且目标角色的有效 scope
不得超过调用者本身。

域名、邮箱和邮件端点会按 `selected` 授权过滤资源。仪表盘/实时状态、SMTP 会话、审计、
系统设置、维护和管理员等全局资源不能安全地按域切分，v1/v2 的相关端点因此要求
`domain_grant_mode=all`；v2 API Key 生命周期也要求全域授权。v1 的受限 Key 管理仅能操作不超出
调用者 scope/域授权的 Key。
`selected` 域凭据可以修改获授权域的一般策略，但域名标识 `root_domain` 的变更会改变授权边界，
因此只允许 `domain_grant_mode=all` 的主体执行；服务层会在同一写事务内再次确认这一条件。

## 日志、健康检查与指标

- `/health/live`：进程存活探针；`/health/ready`：同时检查运行时、后台任务、SQLite、存储目录
  和最小可用磁盘；`/version`：应用版本、推荐 API `v2` 以及受支持的 `v1`/`v2` 列表。
- `/metrics`：Prometheus 文本指标，包括按路由聚合的请求数/延迟、in-flight、后台任务、
  readiness、进程 CPU/内存和 uptime。设置 `METRICS_TOKEN` 后使用 Bearer 或
  `X-Metrics-Token`；关闭 `METRICS_ENABLED` 时返回 404。非回环绑定且指标开启时若未配置
  Token，进程会拒绝启动；只有回环开发环境允许无 Token 访问。
- HTTP 响应携带安全的 `X-Request-ID`。访问日志只记录路由模板，不记录查询串，避免兼容
  Query Key 被日志泄露；`LOG_FORMAT=json` 适合日志采集器，`text` 适合本地排障。格式化与输出
  由容量 4096 的独立线程处理，慢日志 sink 不阻塞事件循环；队列满/输出失败/关闭超时会丢弃并由
  `rapid_inbox_log_records_dropped_total` 按固定原因计数。
- `/admin` 仪表盘通过短 TTL 共享快照异步采集数据库/磁盘状态，展示 RPS、P95、收件/投递/
  拒绝/解析失败、SMTP、解析队列、DB/WAL、磁盘、后台任务和最近清理结果。域/邮箱/邮件/Key/
  审计等总量由事务触发器维护在单行计数器中；流量指标使用分钟桶，24 小时查询最多读取约
  1441 个桶，不会随当天邮件总量线性扫描。

## 性能边界与部署拓扑

Rapid Inbox 的高吞吐目标是单机本地磁盘架构，不是无限横向扩展承诺：

- SQLite 使用 WAL，允许并发读取，但同一时刻仍只有一个写事务。ingestd 可并行解析/写文件，
  SQLite group commit 由互斥锁短时串行；Python 变更也受 `DatabaseWriter` 写锁串行保护。
- `/api/v2` 使用 Runtime 私有的持久只读 actor 和 `mode=ro/query_only` 连接；准入、等待和
  deadline 均有界，维护时先 drain/close。默认单 actor 是本机短查询实测最优值，盲目增加线程
  会放大 Python 行物化的 GIL/futex 竞争；写连接仍显式使用 `synchronous=FULL`。
- `/api/v2` 的 SQLite 热路径由专用 actor 卸载，raw/附件保持流式文件响应；Dashboard 另用约
  1.5 秒共享缓存和防击穿锁。高并发新集成应优先使用 v2，保留的 v1 路由仍是兼容面。
  这些优化不会消除磁盘 IOPS 与 SQLite 单写者上限。
- API Key 鉴权使用进程内、约 2 秒的有界缓存；热命中不切换默认线程池，miss 才异步读库。
  本进程内 Key 变更用提交后 epoch 主动失效，selected 域授权为保持 FK 级 fail-closed 不缓存。
  `last_used_at` 最多约每 30 秒落库一次，因此它是运维信号，不是逐请求审计流水。
- API Key 速率限制使用每 Key 固定内存 token bucket：桶容量等于每分钟额度，并在 60 秒内
  均匀补充，因此允许不超过桶容量的短时突发。状态仍属于单 HTTP 进程；运行 N 个 worker 时
  总可用额度大约变为单进程额度的 N 倍，需要严格全局额度时必须由反向代理/网关执行。
- 一个数据目录建议只运行一个 ingestd 和一个 HTTP 进程。不要把 SQLite WAL 放在不保证 POSIX
  锁语义的网络文件系统，也不要让多个主机直接共享同一个 `app.db`。
- C++/Python SMTP 当前都不实现或宣告 STARTTLS。公网需要传输加密时，应在经过验证的 SMTP
  代理终止 TLS，或仅在可信网络中暴露收件端口；HTTP 管理端同样应置于 HTTPS 反向代理后。

调整 worker、batch、queue、fsync 和 HTTP 并发前，应使用仓库压测脚本在实际磁盘上测量吞吐、
P95/P99、WAL 增长、`451` 比例、恢复时间和掉电要求，并给队列与磁盘预留明确上限。

## 数据与保留策略

Rapid Inbox 使用 SQLite 保存结构化数据，邮件内容拆分保存在本地目录：

```text
storage/
├── app.db           # SQLite 索引与元数据
├── raw/             # 原始 EML
├── text/            # 解析后的纯文本
├── html/            # 解析后的 HTML
├── attachments/     # 附件
├── manifests/       # 启动恢复所需 manifest
├── quarantine/      # 无法持久化/校验的任务与 manifest
└── tmp/             # 临时文件
```

邮件不再使用全局“10 分钟”硬编码保留期。托管域通过 `retention_days` 决定每个投递的
`expires_at`；未设置时不自动过期。任意域使用 `CATCH_ALL_RETENTION_DAYS`，默认 `0` 同样表示
不自动过期。修改域策略只影响之后新建的投递，不会追溯改写历史到期时间。

清理任务按 `CLEANUP_BATCH_SIZE` 分批删除到期投递。仅当一封邮件的所有投递都已到期，才删除
message 元数据，并在同一数据库事务里登记 raw、正文、manifest 和附件的 `file_gc_tasks`；
实际文件删除在事务外执行，失败会记录原因并指数退避重试。因此数据库提交与文件系统故障之间
不会静默遗留一半状态。SMTP 会话、空邮箱、指标 bucket 和审计记录分别使用自己的保留配置。

每轮清理还会删除超过 `QUARANTINE_RETENTION_DAYS` 的 quarantine 文件，并按
`ARTIFACT_SWEEP_BATCH_SIZE` 增量扫描无数据库、manifest、quarantine、file-GC 或在途收件引用的
raw/text/html/附件。扫描 pass 会跨轮次从上次位置续跑；文件至少老于
`ORPHAN_ARTIFACT_GRACE_SECONDS` 才有资格删除，降低与正在落盘/恢复文件竞态的风险。已结束的
维护记录按 `MAINTENANCE_RUN_RETENTION_DAYS` 分批清理。

Python 解析队列同时限制消息数和 raw 字节，预算覆盖排队及 active worker。邮件一旦完成 raw、
manifest 和数据库持久化，即使队列暂满仍保持 SMTP 成功响应；后台 pending 扫描会按旧消息优先
重新入队，避免为了内存背压丢弃已经确认的邮件。

管理/API `DELETE` 会立即将目标投递标记为已删除，并把 `expires_at` 设为当前时间。下一轮清理
将硬删除投递；如果 message 已无其它投递，其元数据和文件会继续通过 file-GC outbox 回收。
因此 DELETE 的查阅可见性立即生效，磁盘释放是可重试的异步过程；可调用
`POST /api/v2/maintenance/cleanup` 加速处理。

删除整邮箱邮件时不会在一个长事务里更新全部历史投递。服务会在最终授权事务中冻结邮箱当前
删除代次和最大 delivery rowid、先把邮箱提升到下一代，再创建持久 `mailbox_bulk_delete_jobs`；
后台每批最多处理 1000 条，作业可在取消、失败或重启后续跑。作业创建之后到达或迁入的投递会
继承新代次，即使 retention 删除最高 rowid 后 SQLite 复用了旧 rowid，也不会被旧作业误删。
授权线性化点是作业创建：在此之前撤销域或邮箱权限会原子拒绝，已经获准并持久化的作业则按原范围完成。

同一 `STORAGE_ROOT` 只允许一个 C++ ingestd。进程启动时持有
`.ingestd.instance.lock` 的内核文件锁，正常退出或崩溃都会自动释放；锁文件会有意保留，是否占用
以 OS 锁为准。SQLite writer 在批次间复用单连接和 prepared statements，SQLite 错误、数据库文件
替换或维护 drained ACK 都会使会话失效并在下一批安全重建。

启动恢复会核对 manifest 与 raw 文件大小/SHA-256，从中恢复未提交的域策略、邮件、投递和解析
结果；全量历史、永久失败重试和同时间戳水位路径通过临时磁盘 SQLite 分批处理，不随历史数量占用
Python 堆。单个 manifest 和每个解码/回放批次都限制为 16 MiB；缺失域策略、损坏或越界的 manifest
会 fail-closed 移入 quarantine，不会推断公开权限，也不会阻断其它邮件恢复。管理后台「清除所有邮件」
会创建跨进程 `.maintenance.lock`，让 C++ ingestd 暂时返回 `421/451`，再停止解析、清空邮件表、
原子移走 raw/text/html/attachments/manifests/tmp 并压缩 SQLite；域名、管理员、API Key、
审计/维护记录与 quarantine 保留，便于取证后单独处置。过期 heartbeat 仅在其 PID 已确认退出时
放行；仍存活或无法验证的状态必须等待匹配 drained ACK 或超时失败。

不要在服务运行时用 `mv`/`os.replace`/备份恢复直接替换 `app.db`。一个进程内的 reader 虽会识别
inode 变化，但 Python writer、C++ ingestd、其它 HTTP 进程以及 `-wal`/`-shm` sidecar 不可能靠单个
连接池原子切换。恢复数据库时必须先停止所有 HTTP/SMTP 进程，按 SQLite 备份流程处理主文件与
sidecar，完成完整性检查后再整体启动。

## 升级与不兼容变更

本轮重构按新安全模型设计，不承诺旧调用语义：

- 新建域名默认 `public_web_enabled=false`、`public_api_enabled=false`；需要公开时必须显式开启。
- API Key 空域列表不再代表全域。旧 Key 会迁移为 `selected`（存在 grants）或 fail-closed 的
  `none`；需要未来域访问时显式改为 `all`。
- API v2 使用 `Authorization: Bearer`、严格字段和 cursor，不接受 Query Key，也不保证 v1
  response shape 兼容；外网部署现在必须提供稳定的 `API_CURSOR_SECRET`。
- 邮件过期改为投递级 `expires_at`；`retention_days=NULL/0` 表示不自动过期，不再套用旧的
  全局 10 分钟规则。
- C++ ingestd 默认 durable ACK。若曾依赖“内存入队即 250”的极低延迟，可显式关闭，但需要
  接受异常退出丢信风险。

升级前应备份 `storage/`，在维护窗口停止旧 HTTP/SMTP 进程，仅启动一个新实例完成 SQLite
轻量迁移，再启动 ingestd。不要让新旧二进制同时写同一数据库；升级后重新检查域公开开关、
Key 的 `domain_grant_mode`、保留期、Metrics Token 和 fsync 选择。

## 开发

```bash
# 安装
python3 -m venv .venv
.venv/bin/pip install -c constraints-dev.txt -e ".[dev]"

# 运行全部测试
.venv/bin/pytest

# C++ ingestd 测试
cmake -S cpp/ingestd -B cpp/ingestd/build
cmake --build cpp/ingestd/build
ctest --test-dir cpp/ingestd/build --output-on-failure

# 指定测试文件
.venv/bin/pytest tests/test_admin_api.py tests/test_public_routes.py
```

### SMTP / HTTP 压测

可使用内置脚本批量投递验证码邮件并采样 C++ ingestd / Python HTTP 的 CPU 与内存：

```bash
.venv/bin/python tools/smtp_stress_test.py \
  --to code@example.com \
  --count 5000 \
  --concurrency 100 \
  --json-output .rapid-inbox-run/smtp-stress.json
```

默认会等待并核对 SQLite 入库/解析数量；只测 SMTP ACK 时可加 `--no-db-check`。HTTP 工具只允许
GET/HEAD，Bearer 建议通过环境变量传递，避免进入 shell 历史：

```bash
RAPID_INBOX_API_TOKEN='<ri_service_...>' \
  .venv/bin/python tools/http_stress_test.py \
  --url http://127.0.0.1:8000/api/v2/domains \
  --count 5000 \
  --concurrency 100 \
  --json-output .rapid-inbox-run/http-stress.json
```

两者都会报告吞吐、P50/P95/P99、失败数；完整参数以实际脚本帮助为准：

```bash
.venv/bin/python tools/smtp_stress_test.py --help
.venv/bin/python tools/http_stress_test.py --help
```

项目依赖在 `pyproject.toml` 中固定到精确版本，`constraints-dev.txt` 保存一组经过验证的开发依赖解析结果。已有虚拟环境拉取新代码后，建议重新执行安装命令，确保入口脚本和依赖版本一致。

## 安全提醒

- 不要在公开环境使用默认管理员密码；优先停用兼容令牌，必须使用时配置独立高熵值
- 任意域模式会接受互联网上任何合法域的投递，但默认仍是私有查阅；不要为方便而全局打开公共 Web/API
- 非回环绑定启用 `/metrics` 时必须设置 `METRICS_TOKEN`，否则服务会拒绝启动；也可关闭指标端点
- API Key token bucket 是单 HTTP 进程内状态；多进程部署还应在可信反向代理执行全局限流
- SMTP 端口 `25` 在部分系统中需要管理员权限，生产部署建议通过反向代理、端口映射或专用服务账户处理
- 公开收件箱适合测试和临时场景，不建议用于接收敏感长期邮件
- `.env`、`storage/`、数据库和邮件落盘文件不应提交到 Git

安全问题请优先查看 [SECURITY.md](SECURITY.md)。

## 贡献

欢迎提交 Issue、修复和改进。开始前建议先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)，里面包含开发流程、测试方式和提交 PR 的注意事项。

## 许可证

Rapid Inbox 基于 [MIT License](LICENSE) 发布。

<div align="center">

<sub>Built with ❤ for local-first email workflows</sub>

</div>
