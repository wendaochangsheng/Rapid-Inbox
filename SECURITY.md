**English** | [简体中文](SECURITY.zh-CN.md)

# Security Policy

Rapid Inbox handles email content, attachments, API Keys, administrator sessions, and a local database. Please do not report security vulnerabilities through public Issues.

## Supported versions

The `main` branch is currently the primary maintained version. Early releases have no long-term support commitment, and deployers should keep up with the latest fixes.

| Version | Support status |
| --- | --- |
| `main` | ✅ Actively maintained |
| `0.1.x` | ⚠️ Critical security fixes only |
| `< 0.1.0` | ❌ No longer maintained |

## Security boundaries

Rapid Inbox is an email receiving and viewing system, not a complete public-facing MTA, spam filter, or malicious-attachment sandbox. When `managed_plus_catchall` is enabled, any sender that can connect to this SMTP service can deliver to any syntactically valid RCPT domain, which can put pressure on disk space, parsing CPU, and queues. This mode does not change DNS/MX records and cannot intercept email that is not routed to this host. Production deployments must combine firewalls, cloud security groups, connection limits, disk monitoring, and upstream anti-abuse controls.

Only receive domains and email that you control or are explicitly authorized to receive. Do not use Rapid Inbox to intercept third-party email, conduct phishing or credential collection, send spam, abuse third-party accounts at scale, or evade third-party service rules. This project receives email only and does not send email; it is not an open relay. Deployers remain responsible for complying with applicable laws, upstream terms of service, and data-retention obligations.

SMTP ingestd does not currently terminate public-facing TLS. To protect data in transit, expose SMTP only on a trusted network or use a verified TLS SMTP proxy. The HTTP administration interface must be served through an HTTPS reverse proxy.

"SMTP accepted the message" and "allow public access" are separate boundaries. New domains and the any-domain policy disable public Web/API access by default. The mailbox public flag is enabled by default, but it only takes effect under the domain-level switch. After a domain is explicitly made public, its Web pages can be viewed anonymously unless access is disabled for an individual mailbox; the public API still requires a valid API Key with `public.read`. Do not use public mailboxes to receive password resets, production secrets, or long-lived sensitive information. "Temporary mailbox" does not mean that data is automatically destroyed by default: messages do not expire automatically unless a delivery retention period is configured. Deployers must configure retention periods, backups, and deletion procedures according to their own data-minimization and compliance requirements.

## Reporting a security issue

Contact the maintainer by email:

```text
wendao@ofoco.cn
```

Please include:

- **Impact**: affected components, interfaces, or deployment scenarios
- **Reproduction steps**: the clearest minimal reproduction you can provide
- **Affected version**: a version number or commit hash
- **Potential exploitation**: possible attack paths or severity
- **Suggested fix**: if you already have an approach in mind

### Response targets

| Stage | Target time |
| --- | --- |
| Acknowledge receipt | Within 3 business days |
| Initial assessment and response | Within 7 business days |
| Fix or mitigation | Coordinated according to severity |
| Public disclosure | A coordinated window after the fix is released |

Once a fix is available, the impact and remediation will be described publicly, and reporters will be credited where reasonably possible.

## Deployment checklist

- Use `./docker-deploy.sh` as the primary deployment path, or `deploy/system/install.sh` for the secondary native systemd path. Both generate high-entropy bootstrap, cursor, and metrics secrets and keep the HTTP management port on host loopback by default. Change the bootstrap password immediately after first login. `quickstart.sh` is only a local evaluation/development launcher.
- The Docker deployment intentionally supervises Python HTTP and C++ ingestd inside one non-root container and PID namespace. The maintenance protocol records ingestd's OS PID and Python verifies it with `kill(pid, 0)`; splitting the processes across isolated PID namespaces can falsely classify a live writer as dead. Never scale the Compose `app` service or point multiple containers/hosts at one data volume.
- Supported deployment flows let Python complete SQLite schema migration and readiness before exposing ingestd. During upgrades, stop all old writers and allow only one migrator. If initialization fails, do not bypass the gate and manually start SMTP.
- Deploy from an existing, reviewed source tag or commit. Docker and systemd build that checkout. The local-only quickstart fallback still defaults to mutable `INGESTD_VERSION=latest`; release SHA-256 verification detects corruption or mismatch but does not pin a version.
- Docker configuration is stored in `.rapid-inbox-docker/rapid-inbox.env` and mail data in a named volume. Protect and back up both while stopped. `docker compose down -v` deletes the volume; the supported `./docker-deploy.sh down` retains it. Use a local filesystem with reliable POSIX locks, not NFS or an equivalent network volume.
- Prefer API Keys issued through the administration interface. v2 only uses `Authorization: Bearer`; disable Query transport for v1 Keys to prevent credentials from entering browser history, Referer headers, proxies, and logs.
- Give API Keys the minimum kind and scopes, and explicitly select `none`, `selected`, or `all` domain authorization. An empty domain list means deny; do not grant `all` merely to bypass a 403. Continue to narrow access with mailbox globs and IP CIDRs.
- Global resources such as the dashboard/live status, SMTP sessions, audit records, system settings, maintenance, and administrators require `all` domain authorization. Do not broaden a Key intended to access only `selected` business resources merely to read global endpoints.
- Use `viewer`, `operator`, and `superadmin` roles to separate administration duties. The system protects the last enabled superadmin. Administrator API Keys should also separate `admins.write`, `admins.credentials.write`, and `admins.sessions.write`; the target role of any account that can log in cannot exceed the caller's own permissions.
- Place HTTP behind a trusted reverse proxy and enable TLS. The Uvicorn/ASGI layer should update `scope.scheme` and the client address only after validating a trusted proxy IP; the application does not directly trust a raw `X-Forwarded-Proto` header. Do not expose Uvicorn with `--forwarded-allow-ips='*'` on an untrusted network, or Secure/HSTS/origin checks and IP allowlists can be spoofed.
- When `/metrics` is enabled on a non-loopback binding, a separate `METRICS_TOKEN` is mandatory; otherwise, the service refuses to start. Disable `METRICS_ENABLED` if metrics are not needed. An empty token is allowed only in a loopback development environment, and live/ready probes must not be treated as administrator-authentication endpoints.
- Non-loopback deployments must configure a random `API_CURSOR_SECRET` of at least 32 characters. It signs v2 cursors and must be protected as a secret. Rotation invalidates all cursors signed with the old secret but does not affect database contents.
- Per-minute API Key limits use an in-process token bucket and permit short bursts up to the bucket capacity. Multi-worker or multi-instance deployments must add global rate limiting at the gateway and monitor 401/403/429 responses consistently.
- Public-facing SMTP must maintain bounded concurrency and a bounded connection-rate window. The default Python concurrency limit is 1024, and the shared per-IP connection limit defaults to 60000 per minute. A non-loopback Python SMTP listener rejects an explicitly unbounded concurrency configuration.
- `/health/ready` validates the Python control plane, SQLite, and storage; it does not prove SMTP protocol usability. Deployment acceptance must also verify the SMTP banner and `EHLO`/`NOOP`/`QUIT`, as the supported Docker healthcheck and systemd verifier do.
- Key delegation narrows scopes, domains, mailboxes, IP CIDRs, expiration, rate limits, and transports together. A restricted parent Key cannot create or update a child Key with arbitrary IPs, no expiration, no rate limit, or newly enabled query transport. Every Key write operation reloads caller and target policies in a single database transaction. Do not separate the authorization read from the actual rotate/update operation in custom extensions.
- Configure SMTP limits for connections, line length, recipient count, message size, and byte queues. Monitor `451` responses, disk alerts, parsing backlog, and quarantine. Python parsing must also set `PARSE_QUEUE_MAX_MESSAGES` and `PARSE_QUEUE_MAX_BYTES`, and the byte budget must not be smaller than the single-message limit. Any-domain mode especially requires capacity quotas and an abuse-response plan.
- Keep `HTTP_MAX_REQUEST_BODY_BYTES` at the minimum required by the application. It limits both Content-Length and chunked requests. Also configure `HTTP_REQUEST_BODY_TIMEOUT_SECONDS`, `HTTP_BODY_MEMORY_BUDGET_BYTES`, and `HTTP_CONCURRENCY_LIMIT`; supported launchers pass the concurrency value to Uvicorn. These settings still do not replace reverse-proxy limits for connections, headers, rate, and timeouts.
- Use `HTTP_LIVE_CONNECTION_LIMIT` to constrain administration/public WebSockets and the deprecated administration SSE compatibility stream within each process. Supported launchers also cap forbidden inbound WebSocket application messages at 16 KiB with a one-message queue. Internet-facing deployments must use WSS, preserve `Upgrade`/`Connection` at the trusted reverse proxy, and set global long-lived-connection limits, handshake rates, frame sizes, and idle timeouts there. Cookie-authenticated administration WebSockets require the exact same-origin `Origin`; API Keys belong only in handshake headers, never query strings.
- The SQLite write actor applies both `DATABASE_WRITE_QUEUE_CAPACITY` and `DATABASE_WRITE_MAX_WAITERS`. A 503 means that the control plane is overloaded; back off and retry instead of queueing without bounds or immediately amplifying retry traffic.

## Content and storage security

- Email HTML is displayed through a sandboxed iframe and a strict CSP, but message bodies and attachments are still untrusted input. Do not open attachments directly on the service host. Downloading clients should use antivirus scanning, content inspection, and isolated-workstation policies.
- Never commit `.env`, `.rapid-inbox-docker/`, `storage/`, SQLite files, backups, logs, real email samples, or API Keys to Git. Default directory and file permissions are tightened, and supported deployments use a dedicated low-privilege identity, but encrypted disks/backups and host access controls are still required.
- `INGEST_DURABLE_ACK=true` stores raw + manifest before the SMTP 250 response. Power-loss durability is a goal only when `INGEST_STORAGE_FSYNC=true` is also enabled. Disabling durable ACK introduces a risk of losing acknowledged messages. If a domain is concurrently renamed or deleted after RCPT, the final transaction rejects cross-tenant rerouting. Artifacts that already received a durable ACK remain in quarantine for forensics, and a domain tombstone prevents recovery from reviving the domain with stale policy.
- Clearing messages and compacting SQLite coordinate with ingestd through `.maintenance.lock`. Do not manually remove the lock file unless you have confirmed that no maintenance process is running and all SMTP/HTTP writers have stopped. A stale status file does not mean the process is dead. The implementation removes it only when the PID is reliably known to be dead; otherwise, it fails closed and waits for a drained ACK.
- Only one C++ ingestd may run for each `STORAGE_ROOT`. `.ingestd.instance.lock` uses a process-level OS lock and is released automatically after a crash. The file remains in place to avoid unlink/recreate races. Do not use file existence to determine whether the process is alive, and do not delete or replace it while ingestd is running.
- Replacing the primary SQLite file online is unsupported. Before restoring a backup, stop all HTTP workers and ingestd, and handle `app.db`, `-wal`, and `-shm` consistently according to SQLite procedures. Otherwise, different connections can simultaneously access old and new inodes or the wrong sidecar files.
- Recovery manifests have a 16 MiB decoding budget per file and per batch. Receipts that exceed the limit or lack a durable domain policy go to quarantine instead of automatically becoming public or reviving a domain. Recurring files of this kind should trigger alerts for anomalous ingestion or version drift.
- Cleanup uses a database file-GC outbox and retries. Sustained growth in `file_gc_pending` or quarantine is not an ignorable normal state; investigate permissions, disk status, file corruption, and path configuration.
- Quarantine and orphaned artifacts are cleaned incrementally under independent retention periods and age gates. Reducing `QUARANTINE_RETENTION_DAYS` or `ORPHAN_ARTIFACT_GRACE_SECONDS` shortens the forensic and race-buffer windows. Confirm backups, recovery scans, and ingestion load before changing them.

## Web and logging

- Administration session cookies are HttpOnly/SameSite=Lax and use Secure under HTTPS. Authenticated administration write forms require a same-origin `Origin` or `Referer`. Login allows non-browser clients that omit both headers but rejects an explicitly cross-origin request. The reverse proxy must publish the current version at the site root `/` (with no ASGI `root_path` or URL subpath), preserve the correct Host, and allow Uvicorn to update the scheme only from trusted proxies; otherwise, routing, security cookies, HSTS, or origin checks can break.
- Request IDs accept only restricted characters and lengths. Structured access logs record route templates rather than raw query strings. Custom logs or upstream proxies might still record full URLs, so deployers must verify their own redaction policies. Built-in logging uses a bounded asynchronous queue. A nonzero `rapid_inbox_log_records_dropped_total` means the sink is blocked, the queue is overloaded, or shutdown failed to flush promptly, and should trigger an alert.
- Audit logs record administrator and API Key changes. Send JSON logs to an access-controlled logging system with retention and alerting policies, and ensure that only principals with `audit.read` and authorized operators can read them.

## Credential exposure response

1. Immediately revoke or rotate affected API Keys. Resetting an administrator password revokes other sessions; sessions can also be revoked individually.
2. Review audit logs, Request IDs, source IPs, Key last-used information, and reverse-proxy logs.
3. If `.env`, the database, or backups were exposed, rotate all related credentials rather than changing only display names.
4. Check whether public domain/mailbox switches, `domain_grant_mode`, or any-domain settings were broadened.
5. Preserve forensic copies before cleaning messages or quarantine, then coordinate disclosure through the channel above.
