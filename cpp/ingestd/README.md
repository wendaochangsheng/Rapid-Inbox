# rapid-inbox-ingestd

`rapid-inbox-ingestd` is Rapid Inbox's primary SMTP data plane. It accepts
mail, persists a recoverable receipt, performs MIME parsing and verification-code
extraction, writes message artifacts, and commits the shared SQLite schema used
by the Python HTTP/admin process.

The Python SMTP listener remains available for development. Do not run both
SMTP implementations against the same port.

## Build and test

```bash
cmake -S cpp/ingestd -B cpp/ingestd/build -DCMAKE_BUILD_TYPE=Release
cmake --build cpp/ingestd/build --parallel
ctest --test-dir cpp/ingestd/build --output-on-failure
```

Required libraries are SQLite 3.20 or newer, OpenSSL, ICU and libunistring. The
binary and Python service must use the same `STORAGE_ROOT`, `DATABASE_PATH`, and
schema version.

## Run

```bash
SMTP_HOST=127.0.0.1 SMTP_PORT=2525 \
  cpp/ingestd/build/rapid-inbox-ingestd --base-dir .
```

The process loads `BASE_DIR/.env`; real environment variables take precedence.
SIGINT/SIGTERM stops accepting clients, closes active sessions, drains queued
jobs, and joins writer/domain-refresh threads before exit.

## Receive and commit pipeline

1. The dual-stack server advertises `SIZE`, `8BITMIME`, `PIPELINING`, and
   `SMTPUTF8`; it accepts null reverse paths, validates ESMTP parameters, and
   enforces connection/rate, idle-timeout, line-length, recipient-count,
   mailbox/domain-syntax, and message-size limits. `VRFY` returns a fixed `252`
   response and never discloses or echoes mailbox data.
2. Before accepting `DATA`, it reserves one message slot but no whole-message
   byte budget. As the body arrives it grows the reservation in
   `INGEST_RESERVATION_CHUNK_BYTES` chunks, avoiding a queue lock per line while
   allowing hundreds of concurrent small messages. Successful push releases
   unused chunk bytes; queued and in-flight jobs retain their exact actual bytes.
3. With durable ACK enabled, the SMTP session atomically writes the raw EML and
   a pending recovery manifest before returning `250 queued`.
4. Workers collect up to `INGEST_BATCH_MAX_MESSAGES` for at most
   `INGEST_FLUSH_INTERVAL_MS`, parse MIME in parallel, and write text/HTML and
   attachment artifacts.
5. The pending manifest is normally atomically replaced with the final
   parsed/failed manifest, so recovery also has the completed parse result. If
   that JSON would exceed Python recovery's 16 MiB per-manifest limit, ingestd
   keeps a bounded pending manifest and recovery reparses the durable raw EML.
6. SQLite writes are serialized into short `BEGIN IMMEDIATE` group commits on
   one lazily opened connection. The writer keeps a persistent prepared
   statement set between batches, drops the complete session after any SQLite
   failure, and detects database inode replacement before the next transaction.
   Message IDs and deterministic attachment IDs make retries idempotent.

Ordinary MIME parse failures are valid message results and are stored with
`parse_status=failed`. Infrastructure or invariant failures receive bounded
retries; a failed multi-message batch is split so a poison job cannot block its
healthy peers. A permanently failing singleton gets a record under
`storage/quarantine/`.

When message slots are exhausted, SMTP returns a temporary `451` before
buffering a body. If byte capacity or the size ceiling is crossed after `354`,
ingestd consumes through the terminating dot and emits exactly one `451` or
`552` there, preserving PIPELINING framing. Canonically duplicate recipients
are accepted once and do not consume additional delivery slots.

## Durability semantics

SQLite metadata is intentionally allowed to commit after SMTP acknowledgement;
the pending manifest is the durable recovery receipt. On Python startup, the
recovery scanner validates the raw size and SHA-256 and reconstructs missing
domain/message/delivery state.

Every SQLite batch re-resolves all recipients from the domain rows in that
transaction. A newly-created more-specific rule may take ownership, but a
rename, delete, or fallback to a different tenant is a `policy conflict` and
the database transaction is rolled back. Because durable ACK can precede that
batch, an already-acknowledged conflict keeps its raw and manifest for the
normal bounded retry path and receives an explicit quarantine record; recovery
honors persisted rename/delete tombstones and never recreates the retired
domain from that stale manifest. Disabling or editing flags/size on the same
unchanged domain identity may finish an already-acknowledged in-flight job from
its RCPT snapshot, but cannot redirect it to another owner.

| Configuration | Meaning of SMTP `250` |
| --- | --- |
| `INGEST_DURABLE_ACK=true`, `INGEST_STORAGE_FSYNC=false` | Raw + pending manifest were atomically renamed before ACK. This protects against ingestd process crashes, but page-cache contents are not guaranteed across power loss. |
| `INGEST_DURABLE_ACK=true`, `INGEST_STORAGE_FSYNC=true` | Raw + manifest files and directory entries were fsynced before ACK. This is the strongest mode and has higher latency. |
| `INGEST_DURABLE_ACK=false` | The job only entered the bounded in-memory queue. A crash or `kill -9` can lose already-acknowledged mail. |

Keep durable ACK enabled for deployed instances. Enable storage fsync when the deployment
requires acknowledgement to survive host power loss, and benchmark the actual
filesystem before choosing batch/worker values.

## Domain policy and maintenance

Domain rules and policy snapshots are reloaded from SQLite every
`DOMAIN_RELOAD_INTERVAL_MS`. Reloads publish an immutable, generation-tagged
snapshot. Long-lived SMTP connections compare only the generation at each valid
`MAIL` transaction boundary and adopt a new snapshot there; `RCPT` processing
therefore stays lock-free and a transaction cannot mix old and new policies.
Exact rules use a normalized hash index and subdomains use allocation-free,
longest-suffix hash lookups rather than scanning every configured domain. An
active `root_domain_ascii='*'` row is the fallback for
`managed_plus_catchall`; it never overrides a more specific managed domain.
This policy accepts arbitrary valid RCPT domains only for SMTP connections that
already reached ingestd; it does not configure DNS/MX or intercept other MTAs.

Each queued recipient carries its policy snapshot, including public flags,
canonicalization, size limit, and retention days. Later domain edits therefore
cannot silently change a message that was already acknowledged.

The Python maintenance flow creates `storage/.maintenance.lock`. Ingestd first
freezes new queue reservations, rejects new connections with `421` and
in-progress transactions with `451`, and waits for all accepted/in-flight jobs
to finish. It closes its persistent SQLite connection before atomically writing
`.maintenance.drained.json` with the exact maintenance token. Python verifies
that token before destructive work, so a stale acknowledgement cannot authorize
a later cleanup run.

Exactly one ingestd may own a storage root. Startup takes a non-blocking kernel
`flock` on `storage/.ingestd.instance.lock` before reading domains or publishing
the shared heartbeat. A competing process fails startup with a clear error;
normal exit and process crashes both release ownership automatically. The lock
file itself intentionally remains in place to avoid unlink/recreate races and
must not be used as an ownership signal without attempting the OS lock.

Ingestd atomically refreshes `storage/.ingestd.status.json` every 500 ms. The
heartbeat includes `instance_id`, `pid`, `updated_at`, maintenance `token`,
`queue_messages`, `queue_bytes`, `active_connections`, and `max_connections`.
The active count tracks registered SMTP sockets exactly and returns to zero
before graceful shutdown removes the status file.

## Logging

The C++ data plane uses the same `LOG_LEVEL` and `LOG_FORMAT` settings as the
Python control plane. JSON is the deployment default; each record is one
thread-safe stderr line with a millisecond UTC `ts`, `level`, `event`, `service`,
`pid`, and typed event fields. `text` is intended for local troubleshooting.

Routine per-connection and per-message lifecycle events are `DEBUG` to avoid
turning logging into a receive-path bottleneck. Startup, maintenance, and
shutdown are `INFO`; capacity/storage failures, retries, batch isolation, and
quarantine are `WARNING` or higher. Repeated connection-limit, maintenance, and
queue-capacity rejection logs are rate-limited on overload. SMTP command lines,
message bodies, envelope addresses, credentials, authorization values, and
maintenance tokens are never emitted. Credential-like structured field names
are defensively redacted by the logger.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `SMTP_HOST` / `SMTP_PORT` | `127.0.0.1` / `25` | IPv4/IPv6/hostname listen address and port; `::` enables an IPv6 wildcard socket |
| `MAX_MESSAGE_SIZE_BYTES` | `52428800` | Global message-size ceiling |
| `MAX_RECIPIENTS_PER_MESSAGE` | `20` | Maximum unique canonical recipients |
| `SMTP_IDLE_TIMEOUT_SECONDS` | `30` | Client receive timeout |
| `SMTP_MAX_CONNECTIONS` | `1024` | Concurrent connection ceiling |
| `SMTP_MAX_LINE_LENGTH` | `1000` | Maximum SMTP command/data line |
| `SMTP_LISTEN_BACKLOG` | `1024` | Kernel listen backlog |
| `SMTP_CONNECTION_RATE_LIMIT_COUNT` | `60000` | Per-peer sliding-window connection allowance; `0` disables it |
| `SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS` | `60` | Per-peer connection window |
| `INGEST_QUEUE_MAX_MESSAGES` | `10000` | Total reserved, queued, and in-flight message budget |
| `INGEST_QUEUE_MAX_BYTES` | `536870912` | Total byte budget; must be at least one maximum-size message |
| `INGEST_RESERVATION_CHUNK_BYTES` | `65536` | Preferred incremental DATA byte reservation |
| `INGEST_BATCH_MAX_MESSAGES` | `250` | Maximum group-commit batch |
| `INGEST_FLUSH_INTERVAL_MS` | `5` | Maximum batching wait |
| `INGEST_SQLITE_BUSY_TIMEOUT_MS` | `5000` | SQLite lock wait |
| `INGEST_WORKER_COUNT` | `4` | Parser/artifact workers |
| `INGEST_MAX_RETRIES` | `3` | Singleton retry count before quarantine |
| `DOMAIN_RELOAD_INTERVAL_MS` | `1000` | Rule/policy refresh interval |
| `INGEST_DURABLE_ACK` | `true` | Persist raw + pending manifest before ACK |
| `INGEST_STORAGE_FSYNC` | `false` | Fsync files and directories before durable ACK |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL` |
| `LOG_FORMAT` | `json` | Single-line `json` or `text` output on stderr |

Configuration parsing is strict: malformed booleans/integers and out-of-range
values fail startup instead of silently falling back. In particular,
`INGEST_QUEUE_MAX_BYTES` cannot be smaller than `MAX_MESSAGE_SIZE_BYTES`.
