# 安全策略

Rapid Inbox 会处理邮件内容、附件、API Key、管理员会话和本地数据库。请不要通过公开 Issue 报告安全漏洞。

## 支持版本

当前主要维护 `main` 分支。早期版本没有长期支持承诺，建议部署方及时跟进最新修复。

| 版本 | 支持状态 |
| --- | --- |
| `main` | ✅ 持续维护 |
| `0.1.x` | ⚠️ 仅关键安全修复 |
| `< 0.1.0` | ❌ 不再维护 |

## 安全边界

Rapid Inbox 是收件和查阅系统，不是完整的公网 MTA、垃圾邮件过滤器或恶意附件沙箱。启用
`managed_plus_catchall` 后，任何能连接到本 SMTP 服务的发送者都可向任意合法 RCPT 域投递，
可能造成磁盘、解析 CPU 和队列压力。该模式不会改变 DNS/MX，不能截获未路由到本机的邮件。
生产部署必须结合防火墙、云安全组、连接限额、磁盘监控和上游反滥用策略。

只应接收自己控制或已获明确授权的域和邮件。不得使用 Rapid Inbox 拦截第三方邮件、实施钓鱼或
凭据收集、发送垃圾邮件、批量滥用第三方账号，或规避第三方服务规则。本项目只收件、不发件，
也不是 open relay；部署者仍须遵守适用法律、上游服务条款和数据保留义务。

SMTP ingestd 当前不负责公网 TLS 终止。需要保护传输内容时，应将 SMTP 暴露在可信网络，或使用
经过验证的 TLS SMTP 代理；HTTP 管理端必须通过 HTTPS 反向代理提供。

“SMTP 接收成功”和“允许公共查阅”是两条独立边界。新建域及任意域策略默认关闭公共 Web/API，
邮箱公开位默认启用但只在域开关之下生效。显式公开域后，除非再按邮箱关闭，其 Web 页面可匿名
查阅；公共 API 仍要求具有 `public.read` 的有效 API Key。不要使用公开邮箱接收密码重置、生产
密钥或长期敏感信息。“临时邮箱”不表示默认自动销毁；未配置投递保留期时邮件不会自动过期，
部署者必须按自身数据最小化和合规要求设置保留期、备份与删除流程。

## 报告安全问题

请通过邮件联系维护者：

```text
wendao@ofoco.cn
```

邮件中建议包含：

- **影响范围**：受影响的组件、接口或部署场景
- **复现步骤**：尽量清晰的最小复现流程
- **受影响版本**：版本号或提交哈希
- **可能的利用方式**：潜在攻击路径或危害等级
- **修复建议**：如果你已有方向

### 响应时效

| 阶段 | 目标时间 |
| --- | --- |
| 确认收到报告 | 3 个工作日内 |
| 初步评估与回复 | 7 个工作日内 |
| 修复或缓解方案 | 根据严重程度协商 |
| 公开披露 | 修复发布后协调时间窗口 |

修复可用后，会公开说明影响和处理方式，并在合理范围内致谢报告者。

## 部署检查清单

- 首次启动后立即修改 bootstrap 密码。`HOST` 配为非回环地址时会拒绝已知默认密码/兼容令牌，但这不能替代
  独立高熵凭据和定期轮换。quickstart 默认绑定 `127.0.0.1`，显式外网绑定会输出 TLS 反代警告；
  不要在 `HOST` 仍为回环值时用 Uvicorn `--host` 单独覆盖为外网地址。
- quickstart 会先由 Python 完成 SQLite schema 和轻量迁移，成功后才启动 ingestd；初始化失败时不得
  绕过脚本手工抢先启动写入进程。升级期间也应停止旧进程，只允许一个迁移者操作数据库。
- 默认 `INGESTD_VERSION=latest` 是可变指针。发布资产旁的 SHA-256 校验能发现下载损坏或不匹配，
  但不能固定版本；生产部署应指定实际存在、已经审核的 release tag，或从固定源码提交本地构建。
- 优先使用后台签发的 API Key。v2 只使用 `Authorization: Bearer`；关闭 v1 Key 的 Query 传输，
  避免凭据进入浏览历史、Referer、代理和日志。
- API Key 使用最小 kind/scope，并明确选择 `none`、`selected` 或 `all` 域授权。空域列表是拒绝，
  不应为了绕过 403 直接授予 `all`。继续用邮箱 glob 和 IP CIDR 收窄访问。
- 仪表盘/实时状态、SMTP 会话、审计、系统设置、维护和管理员等全局资源要求 `all`
  域授权；不要为了读取全局端点而扩大一把本应只访问 `selected` 业务资源的 Key。
- 管理后台使用 `viewer` / `operator` / `superadmin` 分工。系统会保护最后一个启用的
  superadmin；管理员 API Key 还应拆分 `admins.write`、`admins.credentials.write` 与
  `admins.sessions.write`，任何可登录账号的目标角色都不能超过调用者自身权限。
- HTTP 放在可信反向代理后并启用 TLS。应由 Uvicorn/ASGI 层通过受信代理 IP 校验后再更新
  `scope.scheme`/客户地址；应用不直接信任原始 `X-Forwarded-Proto`。不要将 Uvicorn
  `--forwarded-allow-ips='*'` 暴露在不可信网络，否则 Secure/HSTS/同源判断与 IP allowlist 可被欺骗。
- 非回环绑定启用 `/metrics` 时必须设置独立 `METRICS_TOKEN`，否则服务会拒绝启动；不需要指标时
  关闭 `METRICS_ENABLED`。只有回环开发环境允许空 Token，live/ready 探针也不应被当作管理员认证接口。
- 非回环部署必须配置至少 32 个字符的随机 `API_CURSOR_SECRET`。它用于签名 v2 cursor，必须按
  密钥保护；轮换会使所有由旧密钥签发的 cursor 失效，但不会影响数据库内容。
- API Key 每分钟限额使用单 HTTP 进程内 token bucket，允许桶容量范围内的短时突发。多 worker/
  多实例部署必须在网关增加全局限流，并统一监控 401/403/429。
- 公网 SMTP 必须保持有限并发和有限建连滑窗。默认 Python 并发上限为 1024、共享每 IP
  建连上限为每分钟 60000；非回环 Python SMTP 监听会拒绝显式的无限并发配置。
- Key 委派同时收窄 scope、域、邮箱、IP CIDR、到期时间、速率与传输方式；受限父 Key 不能创建
  或更新出任意 IP、永不过期、不限速或新增 query 传输的子 Key。所有 Key 写操作会在单一数据库
  事务内重新加载调用者与目标策略；不要在自定义扩展中拆开“授权读取”和实际 rotate/update。
- 为 SMTP 设置连接、行长、收件人数、消息体积和字节队列上限；监控 `451`、磁盘告警、解析积压
  和 quarantine。Python 解析还应设置 `PARSE_QUEUE_MAX_MESSAGES` / `PARSE_QUEUE_MAX_BYTES`，且字节
  预算不得小于单封邮件上限。任意域模式尤其需要容量配额和滥用响应方案。
- 保持 `HTTP_MAX_REQUEST_BODY_BYTES` 为业务确需的最小值；它同时限制 Content-Length 和 chunked
  请求；同时设置 `HTTP_REQUEST_BODY_TIMEOUT_SECONDS`、`HTTP_BODY_MEMORY_BUDGET_BYTES` 和
  `HTTP_CONCURRENCY_LIMIT`；quickstart 会把并发值同步传给 Uvicorn。它们仍不能替代反向代理的
  连接数、header、速率和超时限制。
- 用 `HTTP_LIVE_CONNECTION_LIMIT` 约束每个进程内的管理 SSE 与公共邮箱 WebSocket；多进程或
  多实例部署仍应在反向代理设置全局长连接上限、握手速率和空闲超时。
- SQLite 写 actor 使用 `DATABASE_WRITE_QUEUE_CAPACITY` 与 `DATABASE_WRITE_MAX_WAITERS` 双界限；
  503 表示控制面已过载，应退避重试，而不是无界排队或立即放大重试流量。

## 内容与存储安全

- 邮件 HTML 通过 sandbox iframe 和严格 CSP 展示，但邮件正文和附件仍属于不可信输入。不要在
  服务主机直接打开附件；下载端应结合杀毒、内容检测和独立工作站策略。
- `.env`、`storage/`、SQLite、备份、日志、真实邮件样本和 API Key 都不得提交到 Git。默认目录/
  文件权限会收紧到 `0700/0600`，仍应使用专用低权限账户及加密磁盘/备份。
- `INGEST_DURABLE_ACK=true` 在 SMTP 250 前保存 raw + manifest；只有同时开启
  `INGEST_STORAGE_FSYNC=true` 才以掉电持久性为目标。关闭 durable ACK 会引入已确认邮件丢失风险。
  若域在 RCPT 后并发改名/删除，最终事务会拒绝跨租户改投；已 durable ACK 的 artifact 会保留到
  quarantine 供取证，域 tombstone 会阻止 recovery 用陈旧策略复活该域。
- 清空邮件和 SQLite 压缩依赖 `.maintenance.lock` 与 ingestd 协调。不要手工删除锁文件，除非已
  确认没有维护进程和所有 SMTP/HTTP 写入者都已停止。过期 status 文件并不等于进程死亡；实现仅
  在 PID 已可靠判死时清理，否则必须 fail-closed 等待 drained ACK。
- 每个 `STORAGE_ROOT` 只能运行一个 C++ ingestd；`.ingestd.instance.lock` 使用进程级 OS 锁并在
  崩溃时自动释放。该文件会长期保留以避免 unlink/recreate 竞态，不要通过“文件存在”判断进程
  存活，也不要在 ingestd 运行时删除或替换它。
- 不支持在线替换 SQLite 主文件。恢复备份前必须停止全部 HTTP worker 与 ingestd，并按 SQLite
  流程一致处理 `app.db`、`-wal`、`-shm`；否则不同连接可能同时访问新旧 inode 或错误 sidecar。
- 恢复 manifest 的单文件和单批解码预算均为 16 MiB。超限或缺少持久域策略的收据会进入
  quarantine 而不会自动公开或复活域；持续出现此类文件应按异常收件/版本漂移告警。
- 清理采用数据库 file-GC outbox 和重试。持续增长的 `file_gc_pending` 或 quarantine 不是可忽略
  的正常状态，应排查权限、磁盘、文件损坏和路径配置。
- quarantine 与孤立 artifact 会按独立保留期/age gate 增量清理。调小
  `QUARANTINE_RETENTION_DAYS` 或 `ORPHAN_ARTIFACT_GRACE_SECONDS` 会缩短取证和竞态缓冲时间；变更前
  应确认备份、恢复扫描与收件负载。

## Web 与日志

- 管理会话 Cookie 为 HttpOnly/SameSite=Lax，HTTPS 下使用 Secure；已认证的管理写表单要求同源
  `Origin` 或 `Referer`，登录允许缺少这两个头的非浏览器客户端，但会拒绝显式跨源请求。反向代理
  必须将当前版本发布在站点根路径 `/`（不配置 ASGI `root_path`/URL 子路径）、保留正确 Host，并让
  Uvicorn 仅从受信代理更新 scheme，否则可能破坏路由、安全 Cookie、HSTS 或同源判断。
- Request ID 只接受受限字符和长度；结构化访问日志记录路由模板而非原始查询串。自定义日志或
  上游代理仍可能记录完整 URL，部署方需要单独确认脱敏策略。内置日志使用有界异步队列，
  `rapid_inbox_log_records_dropped_total` 非零表示 sink 阻塞、队列过载或关闭未能及时刷新，应告警。
- 审计日志记录管理员/API Key 变更。应将 JSON 日志输出到访问受控、具备保留和告警策略的日志
  系统，并确保只有具备 `audit.read` 的主体和受权运维人员能读取。

## 凭据泄露响应

1. 立即吊销或轮换受影响 API Key；重置管理员密码会撤销其它会话，也可单独执行会话撤销。
2. 检查审计日志、Request ID、来源 IP、Key 最近使用信息和反向代理日志。
3. 若 `.env`、数据库或备份泄露，同时轮换所有相关凭据，不要只修改显示名称。
4. 检查公共域/邮箱开关、`domain_grant_mode` 和任意域设置是否被扩大。
5. 保存取证副本后再清理邮件或 quarantine，并按上方渠道协调披露。
