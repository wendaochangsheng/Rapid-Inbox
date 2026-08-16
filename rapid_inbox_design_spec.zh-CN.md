[English](rapid_inbox_design_spec.md) | **简体中文**

# Rapid Inbox 早期设计记录（SQLite / Python）

> [!IMPORTANT]
> 本文保存项目最初的产品设计和任务拆分，用于追溯决策，不是当前版本的接口、安全或部署契约。
> 实现已经演进为 C++ ingestd 与 Python HTTP 控制面的双进程架构；当前行为以
> [README.zh-CN.md](README.zh-CN.md)、[SECURITY.zh-CN.md](SECURITY.zh-CN.md)、数据库 schema、测试和代码为准。

## 1. 项目目标

实现一个“只收件、不发件”的极速邮箱系统，支持：

1. 通过 SMTP 接收邮件。
2. 管理员后台动态添加可接收邮件的域名。
3. 支持根域、二级子域、三级及更深子域作为收件目标域名。
4. 域和邮箱显式开启公共 Web 后，前台可无需密码通过 URL 访问邮箱，例如：
   - `https://adb.com/mail/xxx@adb.com`
5. 前台可浏览某邮箱下的所有邮件、查看正文、下载附件、查看原始邮件。
6. 管理后台可查看 SMTP 活动和历史会话，以及域名、邮箱、邮件、API Key、审计日志、系统设置；
   早期同进程方案以实时事件为目标，当前分进程边界见 README。
7. 提供完整 HTTP API：
   - 管理员 API
   - 普通访问 API
8. 使用 SQLite 保存元数据；原始邮件与正文/附件存文件系统。
9. 系统以“极简热路径、快速回 250”为最高优先级。
10. 不做任何发件行为。
11. 不做垃圾邮件过滤、杀毒、SPF/DKIM/DMARC 拒收。
12. 不因为内容可疑而丢弃邮件；仅在“域名不允许 / 协议错误 / 超过尺寸上限 / 本地存储失败”时拒收。

## 2. 非目标

1. 不实现 IMAP / POP3。
2. 不实现发信 SMTP Submission。
3. 不实现企业级多节点集群。
4. 不实现复杂反垃圾策略。
5. 不实现通用私人邮箱、端到端加密或企业邮件套件；公开收件箱仅用于显式开启的测试/临时场景。

## 3. 总体设计结论

### 3.1 选型

- **语言**：Python 3.10+
- **SMTP 收件层**：当前默认使用 C++20 `rapid-inbox-ingestd`，`aiosmtpd` 保留为开发/兼容模式
- **HTTP/API 层**：FastAPI
- **数据库**：SQLite
- **模板引擎**：Jinja2（服务端渲染）
- **反向代理**：Nginx / Caddy
- **后台实时流**：SSE（Server-Sent Events）
- **原始邮件存储**：本地文件系统（`.eml`）
- **附件存储**：本地文件系统

### 3.2 架构原则

1. **SMTP 热路径只做三件事**：
   - 判定目标域是否允许
   - 将原始邮件字节流落盘
   - 成功后立即回 `250`
2. **解析 MIME / 提取附件 / 生成预览 / 写复杂索引** 全部异步完成。
3. **SQLite 只存元数据与索引**，大对象（raw mail、正文文件、附件）不直接存 SQLite。
4. **所有写操作经过单一写队列**，避免 SQLite 多写冲突。
5. **前台与 API 直接读取元数据索引**，不走 IMAP。
6. **多收件人共用一份原始邮件**，每个邮箱使用 delivery 记录关联。

## 4. 逻辑架构

### 4.1 组件

#### A. SMTP Ingress

职责：
- 监听 25 端口
- 接收 SMTP 会话
- 校验收件人目标域名是否在允许范围
- 将原始邮件保存为 `.eml`
- 创建占位 message + delivery 记录
- 推送异步解析任务

#### B. Ingest / Parse Worker

职责：
- 读取原始 `.eml`
- 解析头、文本正文、HTML 正文、附件
- 更新 message 详情
- 生成预览字段
- 抽取附件文件

#### C. HTTP / Public Frontend

职责：
- 提供公开邮箱列表页
- 提供邮件详情页
- 提供原始邮件下载
- 提供附件下载

#### D. Admin Backend

职责：
- 域名管理
- 邮箱/邮件管理
- API Key 管理
- 审计日志
- 系统设置
- 实时 SMTP 会话监控

#### E. DB Writer

职责：
- 串行化所有 SQLite 写操作
- 为 SMTP、Worker、Admin 提供统一写入口

#### F. Live Event Bus

职责：
- 维护内存态 live sessions
- 向后台 SSE 推送实时连接事件

## 5. 域名与子域支持规则

### 5.1 域名管理模型

后台添加的不是“单个邮箱”，而是“域规则”。

每条域规则包含：
- `root_domain_ascii`：例如 `adb.com`
- `accept_exact`：是否接收 `*@adb.com`
- `accept_subdomains`：是否接收 `*@x.adb.com`、`*@y.x.adb.com`
- `public_web_enabled`
- `public_api_enabled`
- `is_active`
- `plus_addressing_mode`：`keep` / `strip`
- `local_part_case_sensitive`：默认 `false`

### 5.2 匹配规则

对于收件地址 `local@sub.a.adb.com`：

1. 先将域名转为 **lowercase + IDNA ASCII**。
2. 做“最长后缀匹配”。
3. 若命中规则：
   - 完全相等时要求 `accept_exact = true`
   - 属于其子域时要求 `accept_subdomains = true`
4. 若同时命中多个规则，选择 **最长 root_domain**。

示例：

- 已配置 `adb.com` 且 `accept_subdomains = true`
  - 接收 `a@adb.com`
  - 接收 `a@x.adb.com`
  - 接收 `a@y.x.adb.com`
- 已配置 `x.adb.com`，优先级高于 `adb.com`
  - `a@b.x.adb.com` 先归属于 `x.adb.com`

### 5.3 必须补充的 DNS 说明

应用层支持“子域收件” ≠ DNS 自动支持“所有子域投递到你这台机器”。

后台添加 `adb.com` 以后，管理员页面必须展示建议 DNS：

- 根域邮件：
  - `adb.com MX 10 mx1.mail-host.example`
- 子域邮件：
  - `*.adb.com MX 10 mx1.mail-host.example`
- MX 目标主机：
  - `mx1.mail-host.example A <server_ip>`
  - `mx1.mail-host.example AAAA <server_ipv6>`（可选）

> 设计要求：管理后台必须有“DNS 检查”功能，告诉管理员当前域是否真的具备接收根域邮件与子域邮件的 DNS 条件。

### 5.4 Wildcard DNS 的产品说明

文档与后台提示中必须明确：

1. `*.adb.com` 主要覆盖不存在的子名字；根域 `adb.com` 仍需单独配置。
2. 现有精确记录、委派（zone cut）或更具体名字，可能让 wildcard 结果与预期不同。
3. 因此后台不能只显示“已添加域名”，还要显示“DNS 是否真的会把该域/子域邮件路由到本系统”。

## 6. 邮箱模型

### 6.1 虚拟邮箱

系统不要求先创建邮箱再收信。

只要目标域命中已启用规则，任何 `local-part@domain` 都是**虚拟邮箱**：
- 首次收到邮件时自动创建 mailbox 元数据
- 或首次访问 `/mail/<address>` 时惰性创建空 mailbox 记录

### 6.2 邮箱 URL

前台公开路由：

- `GET /mail/{mailbox_address}`
- `GET /mail/{mailbox_address}/{delivery_id}`

要求：
- 逻辑上支持 `xxx@adb.com`
- 实际客户端请求时允许 `%40` 编码
- 前端页面中统一使用已编码 URL 生成链接

### 6.3 对不存在邮箱的行为

- 若 `domain` 受本系统管理：
  - 返回 200
  - 页面显示“暂无邮件”
- 若 `domain` 不受管理：
  - 返回 404

## 7. SMTP 收件流程（核心）

### 7.1 状态机

SMTP 会话只处理：
- `EHLO/HELO`
- `MAIL FROM`
- `RCPT TO`
- `DATA`
- `QUIT`
- `RSET`

### 7.2 RCPT 策略

`RCPT TO` 阶段：
- 仅检查收件人目标域是否命中允许规则
- 不检查邮箱是否预创建
- 不做垃圾判定
- 不做发件人域白名单
- 不做 SPF/DKIM/DMARC 拒收

### 7.3 DATA 热路径

1. 生成 `smtp_session_id`
2. 生成 `message_id`
3. 将完整 `envelope.content` 原样写入临时文件
4. `flush + fsync`
5. 原子重命名到最终 `.eml`
6. 将“占位消息 + delivery + mailbox upsert”写入 DB 写队列
7. 将“解析任务”写入内存任务队列
8. 返回 `250 queued as <message_id>`

### 7.4 失败处理

- 域不允许：`550`
- 邮件过大：`552`
- 本地写盘失败：`451`
- SQLite 占位写失败：
  - SMTP 不应因此丢失已落盘邮件
  - 通过 recovery scanner 补录
- 解析失败：
  - 邮件仍显示在邮箱中
  - `parse_status = failed`
  - 可后台重试 reparse

### 7.5 占位策略

为实现“收件后尽快可见”：

在异步完整解析前，先插入一条占位消息：
- `subject = NULL`
- `from_addr = envelope_from`
- `parse_status = pending`
- `text_preview = NULL`

前台列表显示：
- 收件时间
- `from`（可先显示 envelope_from）
- 解析中标记

完整解析后再更新为真实头字段。

## 8. 数据一致性与恢复

### 8.1 原子写盘

文件保存必须使用：
- `.part` 临时文件
- `fsync`
- `os.replace()` 原子替换

### 8.2 恢复扫描器

启动时必须执行：

1. 扫描 `storage/raw/` 中的 `.eml`
2. 对不存在 DB 记录的 raw 文件，补建 message/delivery
3. 对 `parse_status = pending/failed` 可重试解析
4. 清理陈旧 `.part`

### 8.3 去重策略

不对“内容相同”的邮件做自动去重。

原因：
- 同一封原始邮件可能本来就被重复投递
- 不应因 hash 相同而吞掉第二次到达

`sha256` 仅用于：
- 完整性校验
- 运维比对

## 9. 存储设计

### 9.1 文件布局

```text
/data/rapid-inbox/
  app.db
  app.db-wal
  app.db-shm
  raw/YYYY/MM/DD/<message_id>.eml
  text/YYYY/MM/DD/<message_id>.txt
  html/YYYY/MM/DD/<message_id>.html
  attachments/<message_id>/<attachment_id>-<safe_name>
  tmp/
  logs/
```

### 9.2 SQLite PRAGMA 要求

启动时设置：

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;
```

说明：
- 默认使用 `WAL`
- 默认使用 `FULL`，降低 SQLite 提交在掉电时回滚或损坏的风险；端到端持久性仍取决于
  durable ACK、文件/目录 fsync、文件系统和存储硬件
- 若用户接受极小的掉电回滚风险，可切换 `NORMAL` 换更低延迟

## 10. SQLite 表设计

> 详见配套 `sqlite_schema.sql`。

### 10.1 admins

管理员账号。

### 10.2 admin_sessions

后台网页登录会话，使用 cookie session。

### 10.3 domains

域规则表。

### 10.4 mailboxes

虚拟邮箱表。

一条邮箱记录对应一个“规范化邮箱地址”，例如：
- `xxx@adb.com`
- `xxx@1.adb.com`

### 10.5 smtp_sessions

SMTP 连接摘要表。

### 10.6 smtp_events

SMTP 历史事件表（非热路径必需，但后台排障需要）。

### 10.7 messages

原始消息对象表；一封 raw 邮件只保存一次。

### 10.8 message_deliveries

消息与邮箱的投递关系表。

如果一封邮件同时投递给：
- `a@adb.com`
- `b@adb.com`

则：
- `messages` 只有 1 行
- `message_deliveries` 有 2 行

### 10.9 attachments

附件索引表。

### 10.10 api_keys / api_key_scopes / api_key_domain_grants / api_key_mailbox_grants

API Key、权限范围、资源绑定。

### 10.11 audit_logs

审计日志。

### 10.12 system_settings

全局设置键值表。

## 11. 权限模型

### 11.1 管理后台身份模型

后台 UI 使用：
- 管理员用户名/密码登录
- 登录成功后签发 session cookie

### 11.2 API Key 模型

API Key 采用：
- `prefix + secret` 形式
- 数据库只存 `secret_hash`
- 创建时仅显示一次明文 key

建议格式：
- `ri_admin_<prefix>_<secret>`
- `ri_public_<prefix>_<secret>`

### 11.3 API Key 类型

- `admin`
- `service`
- `public`

### 11.4 Scope 列表

最小 scope 集：

- `system.read`
- `system.write`
- `domains.read`
- `domains.write`
- `mailboxes.read`
- `mailboxes.write`
- `messages.read`
- `messages.write`
- `attachments.read`
- `live.read`
- `audit.read`
- `apikeys.read`
- `apikeys.write`
- `public.read`

### 11.5 资源限制

API Key 除 scope 外，还要支持资源绑定：

- 域级授权
- 邮箱级授权

示例：
- 某 key 只有 `public.read`
- 且只允许 `adb.com`
- 或只允许 `foo@adb.com`

### 11.6 Key 校验顺序

1. 根据前缀快速查 key
2. 校验 hash
3. 校验状态/过期时间
4. 校验源 IP 白名单（如有）
5. 校验 scope
6. 校验 domain/mailbox 绑定
7. 记录 `last_used_at` / `last_used_ip`

## 12. HTTP 路由设计

## 12.1 Public HTML

### 邮箱页

- `GET /mail/{mailbox_address}`
  - 功能：邮箱邮件列表
  - 无需登录

### 邮件详情页

- `GET /mail/{mailbox_address}/{delivery_id}`
  - 功能：查看邮件详情
  - 无需登录

### 原始邮件

- `GET /mail/{mailbox_address}/{delivery_id}/raw`
  - 功能：下载 raw `.eml`

### 附件下载

- `GET /mail/{mailbox_address}/{delivery_id}/attachments/{attachment_id}`

## 12.2 Public API

> 设计决策：HTML 页面匿名；JSON API 默认要求 API Key。这样前台仍然“无密码访问”，同时 API 保持权限模型完整。

### 获取邮箱邮件列表

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages`
- Header: `X-API-Key: ...`
- Query:
  - `limit`
  - `cursor`

返回：

```json
{
  "mailbox": "xxx@adb.com",
  "items": [
    {
      "delivery_id": "...",
      "message_id": "...",
      "received_at": "2026-04-18T20:00:00Z",
      "from_addr": "sender@example.com",
      "subject": "Hello",
      "has_attachments": true,
      "parse_status": "parsed"
    }
  ],
  "next_cursor": null
}
```

### 获取邮件详情

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}`

### 下载原始邮件

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/raw`

### 下载附件

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/attachments/{attachment_id}`

## 12.3 Admin API

### 域名管理

- `GET /api/v1/admin/domains`
- `POST /api/v1/admin/domains`
- `GET /api/v1/admin/domains/{id}`
- `PATCH /api/v1/admin/domains/{id}`
- `DELETE /api/v1/admin/domains/{id}`
- `POST /api/v1/admin/domains/{id}/dns-check`

`POST /api/v1/admin/domains` 请求示例：

```json
{
  "root_domain": "adb.com",
  "accept_exact": true,
  "accept_subdomains": true,
  "public_web_enabled": true,
  "public_api_enabled": true,
  "plus_addressing_mode": "keep",
  "local_part_case_sensitive": false,
  "max_message_size_bytes": 52428800,
  "retention_days": null
}
```

### 邮箱管理

- `GET /api/v1/admin/mailboxes`
- `GET /api/v1/admin/mailboxes/{id}`
- `PATCH /api/v1/admin/mailboxes/{id}`
- `DELETE /api/v1/admin/mailboxes/{id}`
- `GET /api/v1/admin/mailboxes/{id}/messages`

### 邮件管理

- `GET /api/v1/admin/messages`
- `GET /api/v1/admin/messages/{message_id}`
- `POST /api/v1/admin/messages/{message_id}/reparse`
- `DELETE /api/v1/admin/deliveries/{delivery_id}`
- `POST /api/v1/admin/deliveries/bulk-delete`

### SMTP 会话 / 实时信息

- `GET /api/v1/admin/smtp/sessions`
- `GET /api/v1/admin/smtp/sessions/{session_id}`
- `GET /api/v1/admin/live/smtp/stream`

`/live/smtp/stream` 使用 SSE。

### API Key 管理

- `GET /api/v1/admin/api-keys`
- `POST /api/v1/admin/api-keys`
- `GET /api/v1/admin/api-keys/{id}`
- `PATCH /api/v1/admin/api-keys/{id}`
- `POST /api/v1/admin/api-keys/{id}/rotate`
- `POST /api/v1/admin/api-keys/{id}/revoke`

### 审计与设置

- `GET /api/v1/admin/audit-logs`
- `GET /api/v1/admin/settings`
- `PATCH /api/v1/admin/settings`

## 13. 管理后台页面

### 13.1 页面清单

- `/admin/login`
- `/admin`
- `/admin/domains`
- `/admin/domains/{id}`
- `/admin/mailboxes`
- `/admin/messages`
- `/admin/live`
- `/admin/api-keys`
- `/admin/audit`
- `/admin/settings`

### 13.2 首页仪表盘

显示：
- 当前活跃 SMTP 会话数
- 1 分钟 / 5 分钟收件速率
- 待解析队列长度
- 近 24 小时接收数
- 近 24 小时失败数
- 磁盘占用
- 域名数 / 邮箱数 / 邮件数

### 13.3 实时连接面板

SSE 推送字段：

```json
{
  "type": "rcpt_accepted",
  "session_id": "...",
  "ts": "2026-04-18T20:00:00Z",
  "remote_ip": "198.2.180.169",
  "helo": "mail180-169.suw31.mandrillapp.com",
  "mail_from": "bounce@mandrillapp.com",
  "rcpt_to": "xxx@adb.com",
  "tls_used": true,
  "state": "rcpt"
}
```

事件类型：
- `connected`
- `ehlo`
- `mail_from`
- `rcpt_accepted`
- `rcpt_rejected`
- `data_begin`
- `queued`
- `disconnected`
- `error`

## 14. 前台页面行为

### 14.1 邮箱列表页

展示：
- 收件时间
- 发件人
- 主题
- 附件标记
- 解析状态
- 分页

### 14.2 邮件详情页

展示：
- Header 摘要
- 文本正文
- HTML 正文
- 原始邮件下载
- 附件列表

### 14.3 HTML 邮件渲染安全

必须满足：
- 不把原始 HTML 直接插入主站 DOM
- 使用独立渲染路由 + sandbox iframe，或先做严格清洗
- 默认不自动加载远程图片
- 允许本地 CID 附件重写后显示

## 15. 性能目标（产品要求）

以下为实现目标，不是保证值：

1. 小邮件（<= 100KB）`DATA` 结束到返回 `250`：
   - 空闲机器中位数目标 < 250ms
2. 已接收邮件在前台列表可见：
   - 目标 < 1s
3. 后台实时连接事件延迟：
   - 目标 < 500ms

### 15.1 性能关键点

1. SMTP 热路径禁止：
   - MIME 深解析
   - 附件提取
   - HTML 清洗
   - DNS 远程查询
   - 复杂 SQL 搜索
2. 域规则需加载到内存。
3. 使用单写队列避免 SQLite 写锁抖动。
4. 前台读取使用短事务和只读连接。

## 16. 运营与安全补充要求

### 16.1 即使“不做反垃圾”，仍必须有的系统保护

这些不是垃圾过滤，而是系统自保：

- 最大消息大小限制（可配置）
- 单连接空闲超时
- 并发连接上限
- 单消息最大收件人数上限
- 单 IP 短时连接上限（只防止打死服务，不用于判垃圾）
- 磁盘占用告警

### 16.2 审计要求

必须记录：
- 域名新增/修改/删除
- API Key 创建/轮换/吊销
- 管理员登录/登出
- 邮件删除/批量删除
- 设置变更

### 16.3 备份要求

至少备份：
- SQLite 数据库文件
- `-wal` / `-shm` 文件（若在线冷拷贝）
- 原始邮件目录
- 附件目录

### 16.4 隐私与产品说明

后台与首页必须明确提示：

> 新建域和任意域策略默认私有。只有管理员显式开启域级公共 Web/API 后，对应邮箱公开位才会生效；
> 公开收件箱仅适用于测试、临时和展示场景，不适用于私人通信或长期敏感信息。

## 17. 建议代码目录（初始规划）

```text
app/
  main.py
  config.py
  models.py
  schemas.py
  deps.py
  db/
    connection.py
    writer.py
    schema.sql
    migrations.py
  smtp/
    server.py
    handler.py
    matcher.py
    live_state.py
  ingest/
    queue.py
    parser.py
    storage.py
    recovery.py
  http/
    public_views.py
    admin_views.py
    public_api.py
    admin_api.py
    sse.py
  auth/
    passwords.py
    sessions.py
    api_keys.py
    permissions.py
  services/
    domains.py
    mailboxes.py
    messages.py
    dns_check.py
    attachments.py
    audit.py
  templates/
    public/
    admin/
  static/
```

## 18. 实现顺序（初始任务分解）

### Phase 1：最小可用收件链路

1. 建 SQLite schema
2. 完成 domain matcher
3. 完成 SMTP 收件与 raw 落盘
4. 完成 mailbox/message/delivery 占位写入
5. 完成前台邮箱列表与邮件详情页

### Phase 2：后台与 API

6. 管理员登录
7. 域名 CRUD
8. 管理员 API
9. Public API
10. API Key / scope / 资源绑定

### Phase 3：实时与运维

11. SMTP 实时连接 SSE
12. 历史 SMTP 会话查询
13. DNS 检查页面
14. 审计日志
15. Recovery scanner

### Phase 4：邮件细节

16. MIME 解析
17. 附件提取
18. HTML 安全渲染
19. 批量删除 / reparse / 下载 raw

## 19. 验收标准

1. 添加 `adb.com` 后，可接收 `foo@adb.com`。
2. 开启 `accept_subdomains` 后，可接收 `foo@x.adb.com`、`foo@y.x.adb.com`。
3. 显式开启域级公共 Web 后，访问 `https://adb.com/mail/foo@adb.com` 可查看该邮箱邮件。
4. 无需前台登录即可浏览已显式公开的邮箱。
5. HTTP 与 Python SMTP 同进程时，管理后台可实时看到 SMTP 连接与收件事件；分进程数据面只保证
   已提交历史可查询，跨进程实时通知不属于当前实现。
6. 管理员 API Key 可按 scope 与域/邮箱限制权限。
7. Public API 可按 read-only key 获取公开邮箱消息。
8. 在 durable ACK 模式下，邮件原始文件落盘成功后才返回 `250`。
9. SQLite 写压力上来时，前台读仍可继续。
10. 程序异常重启后，恢复扫描器可以找回已落盘但未入库的邮件。

## 20. 初始设计约束（历史记录，非当前强制契约）

以下条目记录早期方案，不能替代当前代码、配置、README、SECURITY 或测试。实现已经演进为
C++ ingestd 与 Python 控制面双进程架构；贡献者修改行为时应先更新当前契约和回归测试。

1. **前台 HTML：仅对显式开启公共 Web 的域/邮箱匿名开放**
2. **Public JSON API：要求 API Key**
3. **管理员 UI：账号密码 + session cookie**
4. **管理员 API：要求 API Key**
5. **SMTP 仅做收件，不做发件**
6. **SQLite 只存索引；大文件放磁盘**
7. **收件热路径严禁做重处理**
8. **初始计划：占位入库 + 异步解析；当前实现以实际数据面批处理顺序为准**
9. **新建域和任意域策略默认私有；管理员可分别开启域级 Web/API，并继续按邮箱关闭公开位**
10. **企业级多节点扩展不是当前目标；若改变该边界，必须重新设计并明确存储、事件和迁移契约**
