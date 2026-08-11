# 更新日志

本项目遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/) 和 [语义化版本](https://semver.org/lang/zh-CN/) 的思路记录重要变化。当前处于 `0.x` 阶段，接口和数据结构可能继续调整。

## [Unreleased]

### 新增

- 两种收件模式：只接收已配置域名的 `managed_only`，以及通过系统 `*` 策略接收已到达本服务的任意合法域投递的 `managed_plus_catchall`；任意域默认私有，可独立开启公共 Web/API 与保留期。
- 管理员 RBAC 与完整账号生命周期：`viewer`、`operator`、`superadmin`，支持创建、停用、密码重置、会话撤销，并保护最后一个启用的 superadmin。
- API Key 显式域授权模式 `none/selected/all`、邮箱 glob、kind/scope 校验、IP CIDR、传输方式、限速、到期、轮换、吊销和删除。
- `/api/v2` 新 API：`admin` / `service` / `public` Key 均使用 Bearer-only 认证，提供公开邮箱、域名/DNS、邮箱/邮件、SMTP 会话/事件、仪表盘、系统设置、维护、API Key、管理员和审计资源；使用严格 Pydantic schema、统一 JSON envelope、RFC 9457 风格 Problem Details、HMAC 签名 cursor 和稳定 operation ID，raw/附件保持文件响应。
- 结构化 JSON/text 日志、安全 Request ID、按路由模板记录的 HTTP 日志、Prometheus `/metrics`、`/health/live`、`/health/ready` 和 `/version`；版本端点标记推荐 `v2` 并列出受支持的 `v1`/`v2`。
- 缓存化运维仪表盘：HTTP RPS/P95、独立邮件/投递/拒绝/解析失败、SMTP/解析队列、SQLite/WAL、磁盘、后台任务和清理状态。
- 持久化域归属迁移与整邮箱删除作业：邮箱删除使用代次隔离加固定 rowid frontier，每批最多
  1000 行，支持失败/取消/重启续跑；即使物理清理导致 SQLite 复用 rowid，也不会误删作业创建后的新投递。
- 投递级保留策略和文件 GC outbox：分批删除、失败持久化、指数退避重试；独立清理 SMTP 会话、空邮箱、指标和审计日志。
- C++ `rapid-inbox-ingestd` durable ACK、SIZE/8BITMIME/PIPELINING/SMTPUTF8 与严格 ESMTP 参数校验、IPv4/IPv6 监听、有界单 IP 建连滑窗、跨进程维护锁、域规则热刷新、任意域 fallback、毒任务 quarantine 和 per-domain 投递到期时间。
- 打分制验证码识别，覆盖中英日韩西多语言、分组数字、字母数字和 HTML 场景。
- `quickstart.sh` 一键启动与 GitHub Actions ingestd 二进制发布流程；新增 SMTP 与只读 HTTP 高并发压测脚本。

### 性能与可靠性

- ingestd 在 `DATA` 前只预留消息槽，正文按可配置大块增长字节 reservation；预算覆盖 reservation、排队和处理中批次，既避免每行加锁和无界内存，也不再让小邮件连接预占整封上限。进入 `DATA` 后的大小/字节压力会消费到终止点并只返回一次 `552/451`，保持 PIPELINING 帧同步。
- ingestd 域规则改为带 generation 的不可变共享快照；长连接仅在有效 `MAIL` 边界热切换，`RCPT` 无锁复用事务快照。域匹配由逐规则线性扫描改为精确哈希与无临时分配的最长后缀哈希查找。
- 默认在 SMTP `250` 前原子写入 raw + pending manifest；可选文件/目录 fsync。SQLite 元数据异步 group commit，异常退出后由 manifest 重建。
- MIME/附件处理使用多 worker，SQLite 事务短时串行；批次失败会二分隔离，健康邮件不再被毒邮件反复拖累。
- API Key 鉴权使用有界短 TTL 缓存并在变更时失效；selected 域授权保持即时 fail-closed；`last_used_at` 写入节流，限流改为有界固定内存 token bucket。
- API v2 的全域/无域 Key 命中热缓存时不再为每个请求调度默认线程池；缓存 miss 才异步读取 SQLite，selected 域授权仍坚持逐次 fail-closed 查询。
- Python 解析队列新增消息数与 raw 字节双重预算，active/queued 统一计数；队列压力不否定已持久化邮件，周期 pending 扫描会公平补入队。
- 管理 SSE 与公共邮箱 WebSocket 共享每进程长连接准入上限，避免慢连接耗尽 HTTP 文件描述符与任务容量。
- HTTP 总并发、请求体总接收时限、共享 body 字节预算、SQLite writer 等待者和密码任务等待者均改为有界准入，过载快速失败并提示退避。
- API v2 SQLite 读取改为 Runtime 私有的持久只读 actor：连接、已接管请求和等待者分别有硬上限，端到端 deadline/取消可中断长查询，维护会排空并关闭 owner 连接后再 checkpoint/VACUUM；fatal 状态进入 readiness。
- Python 兼容 SMTP 默认限制 1024 个并发连接，共享建连滑窗默认限制为每 IP 每分钟 60000 次；非回环 SMTP 监听拒绝显式配置为无限并发。
- quickstart 在启动任何服务进程前显式完成 SQLite schema/轻量迁移，失败即整体退出；Uvicorn 同步应用 `HTTP_CONCURRENCY_LIMIT`。
- quarantine 与孤立 raw/text/html/附件清理使用跨批次续跑的持久 iterator pass，不再每轮从目录树根重复扫描同一前缀；已结束维护记录另行分批清理。
- Dashboard 的数据库和磁盘采集移出事件循环，并用短 TTL 与防击穿锁让 HTML/API 共用快照；
  大表总量改由单行事务计数器读取，收件/投递/拒绝/解析失败改为 C++ 批内聚合的分钟桶，24 小时
  查询成本固定在约 1441 个桶，不再每 1.5 秒扫描当天全部邮件。
- Python 短命读连接不再重复设置数据库级 `journal_mode`/`synchronous`，避免每请求触发 WAL 初始化；写连接仍显式使用 `FULL`。
- C++ SQLite writer 跨批次复用单连接和 persistent prepared statements；失败、数据库替换与维护
  握手会安全关闭并按需重建会话。
- 启动 recovery 使用临时磁盘 SQLite 分批 spool 全量历史、永久失败重试与同 mtime 水位路径，
  Python 堆不再随历史 manifest 数量增长，同时避免粗粒度时间戳漏扫新收件。
- 公共/管理详情对正文、headers 与 CID 图片实施独立硬预算，完整 raw/附件继续通过
  `FileResponse` 流式下载；同步日志格式化和 stderr I/O 移入容量 4096 的独立队列线程。
- recovery 校验 raw 大小与 SHA-256；单文件、扫描批次和磁盘 spool 回放页均以 16 MiB 字节预算切分，已完成邮件在读取 JSON 前过滤；无效 manifest 移入 quarantine，不再阻断其它邮件恢复。
- 域规则变更的历史邮箱归属迁移改为持久化 job 与每批 1000 行的独立事务，批间允许 SMTP writer 插队；非路由字段更新不再扫描历史邮箱。
- API v2 受限消息列表改由获授权的 mailbox/delivery 候选驱动，稀疏 selected-domain、mailbox glob 与 mailbox ID 查询不再扫描全局消息时间线。
- v2 API Key 列表使用固定 1000–5000 行扫描预算和最后扫描位置 continuation cursor；兼容 v1 的
  域列表、SMTP events 与批量删除也加入硬分页/1000-ID 上限，公开邮箱 cursor 改为绑定主体和邮箱的 HMAC 签名。
- SMTP 会话、审计与 file-GC 稳态清理查询按现有索引顺序执行；新 GC tombstone 与到期重试使用两个有界索引流公平合并，避免全表扫描或重试饥饿。
- HTTP 安全头/同源守卫改为直接 ASGI 中间件，压测工具为每个 worker 复用独立连接池；进程 RSS 指标读取当前驻留页而非继承的历史峰值。

### 安全

- 新建域名和任意域策略默认关闭公共 Web/API；邮箱公开位默认启用但仅作为域开关下的二级门控。SMTP 可接收不再隐含匿名可查。
- 新 API Key 的空域列表不再隐式表示全域；作用域、域和邮箱约束逐层收窄。
- API Key 子委派新增父级 IP 网络、到期时间、限速和 header/query 传输方式的 containment 校验。
- API Key create/update/rotate/revoke/delete 在同一个 writer 事务内重载调用者与目标策略并再次
  校验 containment，关闭授权读取与密钥轮换之间的 TOCTOU 提权窗口。
- v1/v2 的全局仪表盘、SMTP、审计、系统、维护和管理员等资源要求 `all` 域授权；域/邮箱/邮件仍可按 `selected` 授权过滤。
- 管理员创建、角色变更、密码重置与会话撤销新增独立 credentials/session scope 和事务内委派 containment；低权限 Key 不能创建或接管权限更高的可登录账号。
- `selected` 域主体不能通过修改 `root_domain` 把已有授权 ID 搬到新租户；域标识变更要求事务内重新确认 all-domain 授权。
- 域创建的授权、域行和 rehome job，以及域删除的授权、routing tombstone 和删除本身，均在 `BEGIN IMMEDIATE` 内完成；排队期间撤销或缩权不会留下半成品。
- 系统设置、clear-all、邮箱公开/隐藏与删除、邮件删除/重解析均在最终 `BEGIN IMMEDIATE` 事务中
  重新加载会话或 API Key；等待 writer、maintenance drain 或预检之后撤权会 fail-closed 且原子回滚。
- `HOST` 配为非回环地址时拒绝使用默认 bootstrap/兼容凭据；首次管理员必须修改密码。
- 管理会话 Cookie 使用 HttpOnly/SameSite，HTTPS 下启用 Secure/HSTS。
- ASGI 请求体同时限制声明长度和 streamed/chunked 实际字节，默认 1 MiB、最大可配 64 MiB，超限返回 413 并关闭连接。
- 访问日志不记录查询串；Metrics 支持独立令牌，非回环绑定启用指标但未配置令牌时拒绝启动；HTML 邮件使用 sandbox iframe 和严格 CSP。
- quickstart 下载预编译 ingestd 时校验发布的 SHA-256，不执行校验不匹配的归档。
- quickstart 的 HTTP 默认监听改为 `127.0.0.1`；显式外网绑定会警告必须使用可信 HTTPS 反向代理。可变 `latest` 下载会提示版本漂移，生产部署应固定已审核 tag 或源码提交。
- 清空邮件通过 `.maintenance.lock` 与 ingestd 协调，避免文件移动/数据库压缩期间继续接收。
- 过期 heartbeat 只有在 PID 被可靠判定已退出时才允许维护继续；存活、无权限验证或损坏状态均
  fail-closed 等待匹配 drained ACK。
- C++ ingestd 通过 `.ingestd.instance.lock` 的内核文件锁强制每个 storage root 单实例，进程崩溃
  自动释放，避免多个实例覆盖 heartbeat/drained ACK。

### 不兼容变更

- 域名公共 Web/API 默认值从开启改为关闭，升级后应显式复核公开边界。
- API Key 空 grants 变为 fail-closed；需要所有当前及未来域时必须设置 `domain_grant_mode=all`。
- 邮件清理改为投递级 `expires_at`；`retention_days=NULL/0` 表示不自动过期，不再使用旧的全局 10 分钟规则。
- API v2 只接受 Authorization Bearer，使用严格字段、统一 envelope 和 cursor，不保证 v1 response shape。
- C++ ingestd 默认 durable ACK；关闭后才恢复“仅内存入队即 250”的旧语义。

### 修复

- Python SMTP per-IP 限流状态改用访问 LRU 与 accepted-time 过期索引，摊销 O(1) 清理并按并发上限的四倍分配、硬封顶 65,536 个来源，避免 IPv6 地址轮换造成 O(N²) 扫描和无界增长。
- 缺失持久 `domain_policy` 的 structured recovery manifest 现在严格 fail-closed 并进入 quarantine，不再用公开默认值复活历史私密域和邮箱。
- API Key 缓存增加提交后失效 epoch；与 rotate/revoke/delete 并发的冷读取不能在失效之后重新填入旧密钥或旧 usage policy。
- 移除管理员表单的 Origin/Referer 强制校验，避免 HTTPS 终止代理改写协议或 Host 后误拦登录及后台操作。
- 修复重复 canonical 收件人产生重复投递、超长附件文件名、存储路径越界和维护期间收件竞态。
- 新建更具体托管域或修改 canonical 策略时，历史 catch-all/父域邮箱会在事务内单向提升并安全
  合并重复投递；旧域 Key 不再继续读取已经归入子域的邮箱。
- 修复大正文/内联附件并发详情请求造成的内存放大，以及慢 stderr 将 asyncio 请求线程串行阻塞。
- 修复 C++ SMTP 将 null reverse-path 当成未执行 MAIL、无法解析 ESMTP 参数，以及 DATA 超限提前回复导致后续命令错位的问题；C++/Python 同步执行严格邮箱、域名和长度边界。
- C++ SMTP 的 `VRFY` 现在固定返回不披露信息的 `252`，不会回显用户输入；长连接不再无限沿用已经禁用或已修改大小/保留期的旧域策略。
- Python/C++ 在最终写事务重新确认每个 RCPT 的域身份；rename/delete 不再把已接收邮件挂到新租户，陈旧 durable manifest 由 tombstone 引导到 quarantine，同域已 ACK 的 C++ 在途邮件仍可安全完成。
- 管理/API `DELETE` 现在将投递立即到期，后台清理会硬删除记录并通过 file-GC outbox 可重试地释放磁盘文件。
- 修复批量写入中毒任务可能影响健康同批邮件的问题，并补充确定性隔离测试。

## [0.1.0]

### 新增

- SMTP 收件、公开收件箱、管理后台和 HTTP API 的基础能力。
- 本地 SQLite 与磁盘文件持久化。
- 域名、邮箱、消息、附件、API Key、审计和系统设置管理。
- 启动恢复、邮件解析、HTML 预览和实时收件更新。
