**English** | [简体中文](CHANGELOG.zh-CN.md)

# Changelog

This project records notable changes in the spirit of [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/). The project is currently in the `0.x` stage, and interfaces and data structures may continue to change.

## [Unreleased]

### Added

- Added the primary one-command Docker Compose deployment: a multi-stage source build, non-root and
  read-only application container, private generated credentials, a persistent named volume,
  migration/readiness gating, combined HTTP/SMTP health checks, and graceful supervision of the
  Python and C++ processes in one PID namespace.
- Added the secondary native Debian/Ubuntu systemd installer with a dedicated service account,
  versioned releases, hardened HTTP/SMTP units, build-before-downtime updates, consistent SQLite
  backups, rollback handling, protocol acceptance, and data-preserving uninstall behavior.
- Added a SQLite-backed public-mailbox live-event outbox and bounded HTTP tailer. Committed delivery
  changes from C++ ingestd or standalone Python SMTP now update open public inboxes over WebSocket
  without a manual refresh; cursor gaps force a full-page resynchronization, and tailer task health
  participates in readiness.

### Changed

- Docker Compose is now the recommended deployment path, native systemd is the secondary path, and
  `quickstart.sh` is documented as a local evaluation/development foreground launcher.
- Made project-facing Markdown bilingual: standard filenames now contain English, complete
  Simplified Chinese versions use the `.zh-CN.md` suffix, and GitHub Issue/PR templates present
  both languages in one file.
- Clarified the project's use cases and its boundaries for single-node deployment, data retention, authorized use, public access, platform compatibility, and Alpha maturity; distinguished currently implemented capabilities from the Roadmap.
- Corrected historical documentation drift concerning default public access, C++ rate-limit defaults, release versions, and Python/C++ test coverage.
- Registered the localized verification-code regression with CTest and made the ingestd build job run the Python/C++ cross-process integration tests.

### Security

- Administration write forms require an Origin or Referer matching the trusted ASGI scheme/Host. Login remains compatible with non-browser clients that omit source headers, but rejects an explicitly cross-origin source.
- When a C++ parsing result exceeds the 16 MiB recovery-manifest budget, it falls back to a bounded pending receipt and the Python recovery process reparses the persisted raw message, avoiding a mismatch between durable ACK and the cross-language recovery limit.

## [0.1.0] - 2026-08-16

### Added

- Two ingestion modes: `managed_only`, which accepts only configured domains, and `managed_plus_catchall`, which uses the system `*` policy to accept deliveries to any syntactically valid domain that reach this service. Any-domain mail is private by default and can independently enable public Web/API access and retention periods.
- Administrator RBAC and a complete account lifecycle: `viewer`, `operator`, and `superadmin`, with account creation, disabling, password reset, and session revocation, while protecting the last enabled superadmin.
- Explicit API Key domain authorization modes `none/selected/all`, mailbox globs, kind/scope validation, IP CIDRs, transports, rate limits, expiration, rotation, revocation, and deletion.
- A new `/api/v2` API: `admin`, `service`, and `public` Keys all use Bearer-only authentication. It provides resources for public mailboxes, domains/DNS, mailboxes/messages, SMTP sessions/events, dashboards, system settings, maintenance, API Keys, administrators, and audit records. It uses strict Pydantic schemas, a unified JSON envelope, RFC 9457-style Problem Details, HMAC-signed cursors, and stable operation IDs; raw messages and attachments remain file responses.
- Structured JSON/text logs, secure Request IDs, HTTP logs recorded by route template, Prometheus `/metrics`, `/health/live`, `/health/ready`, and `/version`. The version endpoint marks `v2` as recommended and lists supported `v1`/`v2` versions.
- A cached operations dashboard covering HTTP RPS/P95, separate message/delivery/rejection/parse-failure metrics, SMTP/parsing queues, SQLite/WAL, disk, background tasks, and cleanup status.
- Persistent domain-ownership migration and whole-mailbox deletion jobs. Mailbox deletion uses generation isolation plus a fixed rowid frontier, processes at most 1000 rows per batch, and can resume after failure, cancellation, or restart. It does not mistakenly delete new deliveries created after the job, even if physical cleanup lets SQLite reuse rowids.
- Delivery-level retention policies and a file-GC outbox: batched deletion, persistent failures, and exponential-backoff retries, with separate cleanup of SMTP sessions, empty mailboxes, metrics, and audit logs.
- C++ `rapid-inbox-ingestd` durable ACK, SIZE/8BITMIME/PIPELINING/SMTPUTF8, strict ESMTP parameter validation, IPv4/IPv6 listeners, a bounded per-IP connection-rate window, a cross-process maintenance lock, hot-reloaded domain rules, any-domain fallback, poison-task quarantine, and per-domain delivery expiration times.
- Scored verification-code recognition covering Chinese, English, Japanese, Korean, and Spanish; grouped digits; alphanumeric codes; and HTML scenarios.
- A one-command `quickstart.sh` startup flow and a GitHub Actions ingestd binary-release workflow, plus SMTP and read-only HTTP high-concurrency benchmark scripts.

### Performance and reliability

- Before `DATA`, ingestd reserves only a message slot. Message bodies grow their byte reservation in configurable large blocks. The budget covers reservations, queued work, and in-flight batches, avoiding per-line locking and unbounded memory without making small-message connections reserve the entire per-message limit. After entering `DATA`, size/byte pressure consumes through the terminator and returns only one `552/451`, preserving PIPELINING frame synchronization.
- ingestd domain rules use generation-tagged immutable shared snapshots. Long-lived connections switch snapshots only at a valid `MAIL` boundary, while `RCPT` reuses the transaction snapshot without locks. Domain matching changed from a linear scan of every rule to exact hashes and longest-suffix hash lookup without temporary allocations.
- By default, raw + pending manifest are atomically written before the SMTP `250`; file/directory fsync is optional. SQLite metadata uses asynchronous group commit and is rebuilt from manifests after an abnormal exit.
- MIME/attachment processing uses multiple workers while SQLite transactions are briefly serialized. Failed batches are split recursively, so healthy messages are no longer repeatedly delayed by a poison message.
- API Key authentication uses a bounded short-TTL cache that is invalidated on changes. Authorization for `selected` domains remains immediately fail-closed. `last_used_at` writes are throttled, and rate limiting uses a bounded, fixed-memory token bucket.
- API v2 Keys with all-domain or no-domain authorization no longer schedule work in the default thread pool for every hot-cache request. Only cache misses read SQLite asynchronously; `selected` domain authorization still performs a fail-closed query for every request.
- The Python parsing queue has dual limits for message count and raw bytes, with unified active/queued accounting. Queue pressure does not reject already persisted messages, and periodic pending scans feed the queue fairly.
- Administration SSE and public-mailbox WebSockets share a per-process admission limit for long-lived connections, preventing slow connections from exhausting HTTP file descriptors and task capacity.
- HTTP total concurrency, total request-body receive time, shared body-byte budget, SQLite-writer waiters, and password-task waiters all use bounded admission. Overload fails quickly and asks clients to back off.
- API v2 SQLite reads use a persistent read-only actor private to each Runtime. Connections, admitted requests, and waiters each have hard limits; end-to-end deadlines and cancellation can interrupt long queries. Maintenance drains and closes the owner connection before checkpoint/VACUUM, and a fatal state affects readiness.
- The Python compatibility SMTP server defaults to 1024 concurrent connections and a shared per-IP connection-rate window of 60000 per minute. A non-loopback SMTP listener rejects an explicitly unbounded concurrency configuration.
- quickstart explicitly completes SQLite schema setup and lightweight migrations before starting any service process, and exits entirely on failure. It also applies `HTTP_CONCURRENCY_LIMIT` to Uvicorn.
- Quarantine and orphaned raw/text/html/attachment cleanup use a persistent iterator pass that resumes across batches instead of restarting at the same directory-tree prefix every cycle. Finished maintenance records are cleaned separately in batches.
- Dashboard database and disk collection moved out of the event loop. A short TTL and single-flight lock let HTML/API callers share a snapshot. Large-table totals are read from single-row transactional counters; ingestion/delivery/rejection/parse-failure data comes from minute buckets aggregated within C++ batches, so a 24-hour query has a fixed cost of about 1441 buckets instead of scanning every message from the day every 1.5 seconds.
- Short-lived Python read connections no longer repeatedly set database-level `journal_mode`/`synchronous`, avoiding WAL initialization on every request. Write connections still explicitly use `FULL`.
- The C++ SQLite writer reuses one connection and persistent prepared statements across batches. Failures, database replacement, and maintenance handshakes close the session safely and rebuild it when needed.
- Startup recovery uses a temporary on-disk SQLite database to batch-spool complete history, permanent-failure retries, and paths at the same mtime watermark. The Python heap no longer grows with historical manifest count, while newly received mail is not missed on coarse-grained filesystems.
- Public and administration detail views apply independent hard budgets to bodies, headers, and CID images. Complete raw messages and attachments continue to stream through `FileResponse`. Synchronous log formatting and stderr I/O run in a separate bounded queue thread with capacity 4096.
- Recovery verifies raw-message size and SHA-256. Per-file, scan-batch, and disk-spool replay pages are each split under a 16 MiB byte budget. Completed messages are filtered before JSON is read, and invalid manifests move to quarantine instead of blocking other message recovery.
- Historical mailbox-ownership migration after domain-rule changes uses a persistent job and independent transactions of 1000 rows per batch, allowing the SMTP writer to proceed between batches. Changes to non-routing fields no longer scan historical mailboxes.
- Restricted API v2 message lists are driven by authorized mailbox/delivery candidates. Sparse `selected`-domain, mailbox-glob, and mailbox-ID queries no longer scan the global message timeline.
- API v2 Key lists use a fixed 1000-5000-row scan budget and a continuation cursor containing the last scanned position. Compatibility v1 domain lists, SMTP events, and bulk deletion also gained hard pagination/1000-ID limits. Public-mailbox cursors are now HMAC-signed and bound to the principal and mailbox.
- Steady-state cleanup queries for SMTP sessions, audit records, and file-GC follow existing index order. New GC tombstones and expired retries use two bounded index streams merged fairly, avoiding full-table scans and retry starvation.
- HTTP security headers and the external-access guard use direct ASGI middleware. Benchmark workers each reuse a separate connection pool. Process RSS metrics read current resident pages rather than an inherited historical peak.

### Security

- New domains and the any-domain policy disable public Web/API access by default. The mailbox public flag is enabled by default but acts only as a secondary gate under the domain switch. SMTP acceptance no longer implies anonymous access.
- An empty domain list for a new API Key no longer implicitly means all domains; scope, domain, and mailbox constraints narrow access layer by layer.
- API Key child delegation gained containment checks for parent IP networks, expiration time, rate limits, and header/query transports.
- API Key create/update/rotate/revoke/delete operations reload caller and target policies in the same writer transaction and validate containment again, closing the TOCTOU privilege-escalation window between authorization reads and Key rotation.
- Global dashboard, SMTP, audit, system, maintenance, administrator, and similar resources in v1/v2 require `all` domain authorization. Domain, mailbox, and message resources can still be filtered through `selected` authorization.
- Administrator creation, role changes, password reset, and session revocation gained separate credential/session scopes and transactional delegation containment. A lower-privileged Key cannot create or take over an account that can log in with greater privileges.
- A `selected`-domain principal cannot move an already authorized ID to a new tenant by changing `root_domain`. Domain-identity changes require all-domain authorization to be reconfirmed in the transaction.
- Authorization, the domain row, and the rehome job for domain creation, as well as authorization, the routing tombstone, and deletion itself for domain deletion, all complete inside `BEGIN IMMEDIATE`. Revocation or narrowing while queued cannot leave partial state.
- System settings, clear-all, mailbox public/private changes and deletion, and message deletion/reparse all reload the session or API Key inside the final `BEGIN IMMEDIATE` transaction. Revocation while waiting for the writer, maintenance drain, or preflight fails closed and rolls back atomically.
- When `HOST` is set to a non-loopback address, default bootstrap and compatibility credentials are rejected. The first administrator must change the password.
- Administration session cookies use HttpOnly/SameSite and enable Secure/HSTS under HTTPS.
- ASGI request bodies limit both declared lengths and actual streamed/chunked bytes, defaulting to 1 MiB and configurable up to 64 MiB. Oversized requests return 413 and close the connection.
- Access logs omit query strings. Metrics support a separate token, and a non-loopback service refuses to start when metrics are enabled without a token. HTML email uses a sandboxed iframe and a strict CSP.
- quickstart verifies the released SHA-256 when downloading a prebuilt ingestd and does not execute an archive whose checksum does not match.
- quickstart's default HTTP listener changed to `127.0.0.1`. Explicit external binding warns that a trusted HTTPS reverse proxy is required. Mutable `latest` downloads warn about version drift; production deployments should pin a reviewed tag or source commit.
- Clearing messages coordinates with ingestd through `.maintenance.lock`, preventing continued ingestion while files are moved or the database is compacted.
- An expired heartbeat allows maintenance to proceed only when the PID is reliably known to have exited. A live PID, an unverifiable PID due to permissions, or corrupted state all fail closed and wait for the matching drained ACK.
- C++ ingestd uses a kernel file lock on `.ingestd.instance.lock` to enforce one instance per storage root. It is released automatically after a crash, preventing multiple instances from overwriting heartbeat/drained ACK state.

### Breaking changes

- The default for domain public Web/API access changed from enabled to disabled. Review public-access boundaries explicitly after upgrading.
- Empty API Key grants now fail closed. To authorize all current and future domains, set `domain_grant_mode=all`.
- Message cleanup now uses delivery-level `expires_at`. `retention_days=NULL/0` means no automatic expiration; the old global 10-minute rule is no longer used.
- API v2 accepts only Authorization Bearer, uses strict fields, a unified envelope, and cursors, and does not guarantee the v1 response shape.
- C++ ingestd enables durable ACK by default. Disabling it restores the old "250 after in-memory enqueue only" semantics.

### Fixed

- Python SMTP per-IP rate-limit state now uses an access LRU and an accepted-time expiration index for amortized O(1) cleanup. It allocates four times the concurrency limit and has a hard cap of 65,536 sources, preventing IPv6 address rotation from causing O(N²) scans and unbounded growth.
- Structured recovery manifests missing a persisted `domain_policy` now fail closed and move to quarantine instead of reviving historically private domains and mailboxes with public defaults.
- The API Key cache gained a post-commit invalidation epoch. A cold read concurrent with rotate/revoke/delete cannot repopulate an old credential or usage policy after invalidation.
- Removed mandatory Origin/Referer validation from administration forms, avoiding false rejection of login and administration operations when an HTTPS-terminating proxy rewrites the protocol or Host.
- Fixed duplicate deliveries from repeated canonical recipients, overly long attachment filenames, storage-path traversal, and ingestion races during maintenance.
- When a more specific managed domain is created or a canonical policy changes, historical catch-all/parent-domain mailboxes are promoted in one direction within transactions and duplicate deliveries are merged safely. Keys for the old domain can no longer read mailboxes that have moved into a child domain.
- Fixed memory amplification from concurrent detail requests for large bodies/inline attachments and event-loop serialization when stderr is slow.
- Fixed C++ SMTP treating a null reverse-path as though `MAIL` had not run, failing to parse ESMTP parameters, and replying too early after a `DATA` size violation, which desynchronized subsequent commands. C++ and Python now enforce the same strict mailbox, domain, and length boundaries.
- C++ SMTP `VRFY` now always returns a non-disclosing `252` and never echoes user input. Long-lived connections no longer keep using indefinitely a stale domain policy whose domain was disabled or whose size/retention settings changed.
- Python/C++ reconfirm every RCPT's domain identity in the final write transaction. Rename/delete can no longer attach accepted mail to a new tenant; tombstones direct stale durable manifests to quarantine, while already-ACKed in-flight C++ mail for the same domain can still complete safely.
- Administration/API `DELETE` now immediately expires deliveries. Background cleanup hard-deletes records and releases files through a retryable file-GC outbox.
- Fixed a poison task in a batch potentially affecting healthy messages in the same batch, and added deterministic isolation tests.

[Unreleased]: https://github.com/wendaochangsheng/Rapid-Inbox/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/wendaochangsheng/Rapid-Inbox/releases/tag/v0.1.0
