# rapid-inbox-ingestd

[English](README.md) | **简体中文**

`rapid-inbox-ingestd` 是 Rapid Inbox 的主要 SMTP 数据面。它负责接收邮件、持久化可恢复回执、
执行 MIME 解析和验证码提取、写入邮件制品，并提交 Python HTTP/管理进程共用的 SQLite schema。

Python SMTP 监听器仍可用于开发。不要让两种 SMTP 实现在同一个端口上运行。

## 构建与测试

```bash
cmake -S cpp/ingestd -B cpp/ingestd/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/ingestd/build --parallel
ctest --test-dir cpp/ingestd/build --output-on-failure
```

依赖 SQLite 3.20 或更高版本、OpenSSL、ICU 和 libunistring。二进制与 Python 服务必须使用相同的
`STORAGE_ROOT`、`DATABASE_PATH` 和 schema 版本。

## 运行

```bash
SMTP_HOST=127.0.0.1 SMTP_PORT=2525 \
  cpp/ingestd/build/rapid-inbox-ingestd --base-dir .
```

进程会加载 `BASE_DIR/.env`，真实环境变量优先。收到 SIGINT/SIGTERM 后，进程停止接收客户端、
关闭活动会话、排空已排队任务，并在退出前 join writer/domain-refresh 线程。

## 接收与提交流水线

1. 双栈服务器宣告 `SIZE`、`8BITMIME`、`PIPELINING` 和 `SMTPUTF8`；接受空反向路径、校验
   ESMTP 参数，并执行连接/频率、空闲超时、行长度、收件人数、邮箱/域语法和邮件大小限制。
   `VRFY` 固定返回 `252`，绝不泄露或回显邮箱数据。
2. 接受 `DATA` 前只预留一个消息槽，不预留整封邮件的字节预算。正文到达时按
   `INGEST_RESERVATION_CHUNK_BYTES` 分块增加 reservation，在允许数百封小邮件并发的同时避免
   每行获取队列锁。成功 push 后释放未使用的分块字节；已排队和处理中任务保留精确的实际字节数。
3. 启用 durable ACK 时，SMTP 会话会在返回 `250 queued` 前原子写入 raw EML 和 pending
   recovery manifest。
4. Worker 最多收集 `INGEST_BATCH_MAX_MESSAGES` 封邮件并等待不超过
   `INGEST_FLUSH_INTERVAL_MS`，并行解析 MIME，然后写入 text/HTML 和附件制品。
5. pending manifest 通常会被原子替换成最终 parsed/failed manifest，使恢复流程也能获得已完成的
   解析结果。如果该 JSON 会超过 Python recovery 的 16 MiB 单 manifest 上限，ingestd 会保留
   有界 pending manifest，由恢复器重新解析已持久化的 raw EML。
6. SQLite 写入通过一个延迟打开的连接串行执行短 `BEGIN IMMEDIATE` group commit。Writer 会在批次间
   保留 persistent prepared statement 集；任何 SQLite 失败后丢弃整个 session，并在下一事务前检测
   数据库 inode 是否已被替换。消息 ID 和确定性的附件 ID 使重试保持幂等。

普通 MIME 解析失败属于有效的消息结果，以 `parse_status=failed` 保存。基础设施或不变量失败会进行
有界重试；多消息批次失败时会拆分，避免毒任务阻塞同批健康任务。永久失败的单任务会在
`storage/quarantine/` 下留下记录。

消息槽耗尽时，SMTP 会在缓冲正文前临时返回 `451`。如果在 `354` 之后超过字节容量或大小上限，
ingestd 会消费到终止点并只在那里返回一次 `451` 或 `552`，从而保持 PIPELINING 帧同步。
规范化后重复的收件人只接受一次，不消耗额外交付槽。

## 持久性语义

SQLite 元数据有意允许在 SMTP 确认后提交；pending manifest 是持久恢复回执。Python 启动时，
recovery scanner 会验证 raw 大小和 SHA-256，并重建缺失的域、消息和投递状态。

每个 SQLite 批次都会在该事务中从域表重新解析全部收件人。新建的更具体规则可以取得归属，但 rename、
delete 或 fallback 到其他租户属于 `policy conflict`，数据库事务会回滚。由于 durable ACK 可能早于
该批次，已经确认但发生冲突的任务会保留 raw 和 manifest 进入普通有界重试路径，并生成明确的
quarantine 记录；恢复器遵守已持久化的 rename/delete tombstone，绝不会从陈旧 manifest 重新创建
已退役域。在域标识不变时，禁用域或编辑 flags/size 可以让已确认的在途任务依据其 RCPT snapshot
完成，但不能把任务重定向到其他所有者。

| 配置 | SMTP `250` 的含义 |
| --- | --- |
| `INGEST_DURABLE_ACK=true`, `INGEST_STORAGE_FSYNC=false` | ACK 前 raw + pending manifest 已完成原子 rename。可防护 ingestd 进程崩溃，但页缓存内容不保证跨掉电保存。 |
| `INGEST_DURABLE_ACK=true`, `INGEST_STORAGE_FSYNC=true` | ACK 前 raw + manifest 文件及目录项已 fsync。这是最强模式，延迟更高。 |
| `INGEST_DURABLE_ACK=false` | 任务只进入有界内存队列。崩溃或 `kill -9` 可能丢失已确认邮件。 |

部署实例应保持 durable ACK 开启。如果部署要求确认结果能够承受主机掉电，请启用 storage fsync，
并在选择 batch/worker 参数前对实际文件系统做基准测试。

## 域策略与维护

域规则和策略快照每隔 `DOMAIN_RELOAD_INTERVAL_MS` 从 SQLite 重新加载。Reload 会发布带 generation
标签的不可变快照。长连接只在每个有效 `MAIL` 事务边界比较 generation，并在此处采用新快照；因此
`RCPT` 处理保持无锁，一个事务也不会混用新旧策略。精确规则使用规范化哈希索引，子域使用无分配的
最长后缀哈希查找，而不是扫描每个已配置域。活动的 `root_domain_ascii='*'` 行是
`managed_plus_catchall` 的 fallback，绝不会覆盖更具体的托管域。该策略只接受已经到达 ingestd 的
SMTP 连接中的任意合法 RCPT 域；它不会配置 DNS/MX，也不会拦截其他 MTA。

每个已排队收件人携带自己的策略快照，包括公开标志、规范化方式、大小限制和保留天数。因此后续域编辑
不能静默改变已经确认的邮件。

Python 维护流程会创建 `storage/.maintenance.lock`。ingestd 先冻结新的队列 reservation，对新连接
返回 `421`、对进行中的事务返回 `451`，然后等待所有已接受/在途任务完成。它会在原子写入带有精确
maintenance token 的 `.maintenance.drained.json` 前关闭 persistent SQLite 连接。Python 在执行
破坏性操作前验证该 token，因此陈旧确认无法授权之后的清理任务。

一个 storage root 只能由一个 ingestd 持有。启动时会在读取域或发布共享 heartbeat 前，对
`storage/.ingestd.instance.lock` 获取非阻塞内核 `flock`。竞争进程会带清晰错误启动失败；正常退出和
进程崩溃都会自动释放所有权。锁文件本身会故意保留以避免 unlink/recreate 竞态，未尝试 OS 锁前不能
把文件是否存在当作所有权信号。

ingestd 每 500 ms 原子刷新 `storage/.ingestd.status.json`。Heartbeat 包含 `instance_id`、`pid`、
`updated_at`、maintenance `token`、`queue_messages`、`queue_bytes`、`active_connections` 和
`max_connections`。活动数精确跟踪已注册的 SMTP socket，并在优雅关停移除状态文件前回到零。

## 日志

C++ 数据面与 Python 控制面使用相同的 `LOG_LEVEL` 和 `LOG_FORMAT` 设置。部署默认使用 JSON；
每条记录都是线程安全的 stderr 单行，包含毫秒 UTC `ts`、`level`、`event`、`service`、`pid` 和带类型
的事件字段。`text` 用于本地排障。

常规的逐连接和逐消息生命周期事件使用 `DEBUG`，避免日志成为接收路径瓶颈。启动、维护和关闭使用
`INFO`；容量/存储失败、重试、批次隔离和 quarantine 使用 `WARNING` 或更高等级。过载时，重复的
连接限制、维护和队列容量拒绝日志会被限速。SMTP 命令行、消息正文、envelope 地址、凭据、授权值和
maintenance token 永远不会输出。类似凭据的结构化字段名会由 logger 防御性脱敏。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SMTP_HOST` / `SMTP_PORT` | `127.0.0.1` / `25` | IPv4/IPv6/主机名监听地址与端口；`::` 启用 IPv6 wildcard socket |
| `MAX_MESSAGE_SIZE_BYTES` | `52428800` | 全局邮件大小上限 |
| `MAX_RECIPIENTS_PER_MESSAGE` | `20` | 最大唯一规范化收件人数 |
| `SMTP_IDLE_TIMEOUT_SECONDS` | `30` | 客户端接收超时 |
| `SMTP_MAX_CONNECTIONS` | `1024` | 并发连接上限 |
| `SMTP_MAX_LINE_LENGTH` | `1000` | SMTP 命令/数据行最大长度 |
| `SMTP_LISTEN_BACKLOG` | `1024` | 内核 listen backlog |
| `SMTP_CONNECTION_RATE_LIMIT_COUNT` | `60000` | 每 peer 滑动窗口建连额度；`0` 表示关闭 |
| `SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS` | `60` | 每 peer 建连窗口 |
| `INGEST_QUEUE_MAX_MESSAGES` | `10000` | 已预留、排队和处理中消息总预算 |
| `INGEST_QUEUE_MAX_BYTES` | `536870912` | 总字节预算；必须至少容纳一封最大邮件 |
| `INGEST_RESERVATION_CHUNK_BYTES` | `65536` | DATA 增量字节 reservation 的首选分块大小 |
| `INGEST_BATCH_MAX_MESSAGES` | `250` | group-commit 最大批次 |
| `INGEST_FLUSH_INTERVAL_MS` | `5` | 最大批处理等待时间 |
| `INGEST_SQLITE_BUSY_TIMEOUT_MS` | `5000` | SQLite 锁等待时间 |
| `INGEST_WORKER_COUNT` | `4` | 解析器/制品 worker 数 |
| `INGEST_MAX_RETRIES` | `3` | 进入 quarantine 前的单任务重试次数 |
| `DOMAIN_RELOAD_INTERVAL_MS` | `1000` | 规则/策略刷新间隔 |
| `INGEST_DURABLE_ACK` | `true` | ACK 前持久化 raw + pending manifest |
| `INGEST_STORAGE_FSYNC` | `false` | durable ACK 前 fsync 文件和目录 |
| `LOG_LEVEL` | `INFO` | `DEBUG`、`INFO`、`WARNING`、`ERROR` 或 `CRITICAL` |
| `LOG_FORMAT` | `json` | stderr 上的单行 `json` 或 `text` 输出 |

配置解析严格执行：格式错误的布尔值/整数和越界值会使启动失败，而不会静默回退。特别是
`INGEST_QUEUE_MAX_BYTES` 不能小于 `MAX_MESSAGE_SIZE_BYTES`。
