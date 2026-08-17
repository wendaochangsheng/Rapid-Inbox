**English** | [简体中文](rapid_inbox_design_spec.zh-CN.md)

# Rapid Inbox Early Design Record (SQLite / Python)

> [!IMPORTANT]
> This document preserves the project's original product design and task breakdown for decision traceability. It is not the contract for the current interfaces, security model, or deployment model.
> The implementation has evolved into a two-process architecture with the C++ ingestd and Python HTTP control plane. For current behavior, refer to
> [README.md](README.md), [SECURITY.md](SECURITY.md), the database schema, tests, and code.

## 1. Project Goals

Build a high-speed, receive-only email system that supports:

1. Receiving email over SMTP.
2. Dynamically adding domains that can receive email through the admin console.
3. Using root domains, second-level subdomains, third-level subdomains, and deeper subdomains as recipient domains.
4. Allowing passwordless mailbox access by URL after public Web access is explicitly enabled for the domain and mailbox, for example:
   - `https://adb.com/mail/xxx@adb.com`
5. Letting the public frontend browse all messages in a mailbox, view message bodies, download attachments, and view raw messages.
6. Letting the admin console inspect SMTP activity and historical sessions, as well as domains, mailboxes, messages, API keys, audit logs, and system settings;
   the current two-process design persists public-mailbox delivery notifications in SQLite and tails them in HTTP for WebSocket updates. The authenticated administration WebSocket exposes process-local connection/command telemetry, while separate C++ ingress contributes only committed outbox deliveries mapped to `delivery_committed`.
7. Providing a complete HTTP API:
   - Admin API
   - General-access API
8. Storing metadata in SQLite and raw messages plus bodies/attachments in the filesystem.
9. Giving the minimal hot path and a fast 250 response the highest priority.
10. Never sending email.
11. Not performing spam filtering, antivirus scanning, or SPF/DKIM/DMARC-based rejection.
12. Not discarding messages because their content appears suspicious; messages are rejected only when the domain is not allowed, the protocol is invalid, the size limit is exceeded, or local storage fails.

## 2. Non-Goals

1. Implementing IMAP / POP3.
2. Implementing SMTP Submission for outbound mail.
3. Implementing an enterprise-grade multi-node cluster.
4. Implementing sophisticated anti-spam policies.
5. Implementing a general-purpose private mailbox, end-to-end encryption, or an enterprise mail suite; public inboxes are only for explicitly enabled testing or temporary scenarios.

## 3. Overall Design Decisions

### 3.1 Technology Choices

- **Language**: Python 3.10+
- **SMTP receiving layer**: currently defaults to the C++20 `rapid-inbox-ingestd`; `aiosmtpd` remains available for development and compatibility modes
- **HTTP/API layer**: FastAPI
- **Database**: SQLite
- **Template engine**: Jinja2 (server-side rendering)
- **Reverse proxy**: Nginx / Caddy
- **Admin real-time stream**: authenticated WebSocket; the former SSE route is deprecated compatibility only
- **Public-mailbox real-time stream**: WebSocket backed by a SQLite live-event outbox
- **Raw message storage**: local filesystem (`.eml`)
- **Attachment storage**: local filesystem

### 3.2 Architectural Principles

1. **The SMTP hot path does only three things**:
   - Determines whether the recipient domain is allowed
   - Persists the raw message byte stream
   - Returns `250` immediately after success
2. **MIME parsing / attachment extraction / preview generation / complex index writes** all happen asynchronously.
3. **SQLite stores only metadata and indexes**; large objects (raw messages, body files, and attachments) are not stored directly in SQLite.
4. **All writes pass through a single writer queue** to avoid contention between SQLite writers.
5. **The frontend and API read metadata indexes directly** without using IMAP.
6. **Multiple recipients share one raw message**, with a delivery record linking each mailbox.

## 4. Logical Architecture

### 4.1 Components

#### A. SMTP Ingress

Responsibilities:
- Listen on port 25
- Receive SMTP sessions
- Verify that recipient domains match an allowed rule
- Save raw messages as `.eml` files
- Create placeholder message and delivery records
- Enqueue asynchronous parsing tasks

#### B. Ingest / Parse Worker

Responsibilities:
- Read raw `.eml` files
- Parse headers, text bodies, HTML bodies, and attachments
- Update message details
- Generate preview fields
- Extract attachment files

#### C. HTTP / Public Frontend

Responsibilities:
- Provide the public mailbox listing page
- Provide the message detail page
- Provide raw message downloads
- Provide attachment downloads
- Push newly committed mailbox deliveries over WebSocket
- Reload the current mailbox view when a WebSocket cursor gap requires resynchronization

#### D. Admin Backend

Responsibilities:
- Domain management
- Mailbox/message management
- API key management
- Audit logs
- System settings
- Real-time SMTP session monitoring over an authenticated WebSocket for in-process ingress; committed session history and delivery notifications for separate ingress

#### E. DB Writer

Responsibilities:
- Serialize all SQLite write operations
- Provide a unified write entry point for SMTP, Worker, and Admin

#### F. Live Event Bus

Responsibilities:
- Persist cross-process public-mailbox delivery events in SQLite
- Tail committed delivery events into bounded per-HTTP live state
- Push matching mailbox updates over WebSocket
- Keep connection-level SMTP telemetry in memory for the administration WebSocket
- Map committed `mailbox_delivery` outbox events, including those produced by separate/C++ ingress, to administration `delivery_committed` events; parse-update events advance the cursor without claiming cross-process command telemetry

## 5. Domain and Subdomain Support Rules

### 5.1 Domain Management Model

The admin console adds a "domain rule," not an individual mailbox.

Each domain rule contains:
- `root_domain_ascii`: for example, `adb.com`
- `accept_exact`: whether to accept `*@adb.com`
- `accept_subdomains`: whether to accept `*@x.adb.com` and `*@y.x.adb.com`
- `public_web_enabled`
- `public_api_enabled`
- `is_active`
- `plus_addressing_mode`: `keep` / `strip`
- `local_part_case_sensitive`: defaults to `false`

### 5.2 Matching Rules

For the recipient address `local@sub.a.adb.com`:

1. First convert the domain to **lowercase + IDNA ASCII**.
2. Perform a longest-suffix match.
3. If a rule matches:
   - An exact match requires `accept_exact = true`
   - A subdomain match requires `accept_subdomains = true`
4. If multiple rules match, select the **longest root_domain**.

Examples:

- With `adb.com` configured and `accept_subdomains = true`:
  - Accept `a@adb.com`
  - Accept `a@x.adb.com`
  - Accept `a@y.x.adb.com`
- With `x.adb.com` configured, it takes priority over `adb.com`:
  - `a@b.x.adb.com` is associated with `x.adb.com` first

### 5.3 Required DNS Explanation

Application-level support for "subdomain receiving" does not mean DNS automatically supports delivering every subdomain to this server.

After an administrator adds `adb.com`, the admin page must show the recommended DNS records:

- Root-domain mail:
  - `adb.com MX 10 mx1.mail-host.example`
- Subdomain mail:
  - `*.adb.com MX 10 mx1.mail-host.example`
- MX target host:
  - `mx1.mail-host.example A <server_ip>`
  - `mx1.mail-host.example AAAA <server_ipv6>` (optional)

> Design requirement: the admin console must provide a "DNS check" feature that tells administrators whether the current domain actually has the DNS configuration required to receive mail for the root domain and its subdomains.

### 5.4 Product Explanation for Wildcard DNS

The documentation and admin guidance must make the following clear:

1. `*.adb.com` mainly covers subdomain names that do not otherwise exist; the root domain `adb.com` still requires a separate record.
2. Existing exact records, delegations (zone cuts), or more specific names can make wildcard results differ from expectations.
3. Therefore, the admin console must show not only that a domain has been added, but also whether DNS will actually route mail for that domain and its subdomains to this system.

## 6. Mailbox Model

### 6.1 Virtual Mailboxes

The system does not require a mailbox to be created before it can receive mail.

As long as the recipient domain matches an enabled rule, any `local-part@domain` is a **virtual mailbox**:
- Create mailbox metadata automatically when the first message arrives
- Or lazily create an empty mailbox record when `/mail/<address>` is first visited

### 6.2 Mailbox URLs

Public frontend routes:

- `GET /mail/{mailbox_address}`
- `GET /mail/{mailbox_address}/{delivery_id}`

Requirements:
- Logically support `xxx@adb.com`
- Allow `%40` encoding in actual client requests
- Consistently generate links with encoded URLs in frontend pages

### 6.3 Behavior for a Nonexistent Mailbox

- If the `domain` is managed by this system:
  - Return 200
  - Show "No messages yet" on the page
- If the `domain` is not managed:
  - Return 404

## 7. SMTP Receiving Flow (Core)

### 7.1 State Machine

An SMTP session handles only:
- `EHLO/HELO`
- `MAIL FROM`
- `RCPT TO`
- `DATA`
- `QUIT`
- `RSET`

### 7.2 RCPT Policy

During `RCPT TO`:

- Only check whether the recipient domain matches an allowed rule
- Do not check whether the mailbox was pre-created
- Do not classify spam
- Do not apply a sender-domain allowlist
- Do not reject based on SPF/DKIM/DMARC

### 7.3 DATA Hot Path

1. Generate `smtp_session_id`
2. Generate `message_id`
3. Write the complete `envelope.content` unchanged to a temporary file
4. `flush + fsync`
5. Atomically rename it to the final `.eml` file
6. Submit the "placeholder message + delivery + mailbox upsert" to the DB writer queue
7. Submit the "parsing task" to the in-memory task queue
8. Return `250 queued as <message_id>`

### 7.4 Failure Handling

- Domain not allowed: `550`
- Message too large: `552`
- Local disk write failure: `451`
- SQLite placeholder write failure:
  - SMTP must not lose a message that has already been persisted
  - Backfill it through the recovery scanner
- Parsing failure:
  - The message remains visible in the mailbox
  - `parse_status = failed`
  - Reparse can be retried from the admin console

### 7.5 Placeholder Strategy

To make a message visible as soon as possible after receipt:

Insert a placeholder message before full asynchronous parsing:
- `subject = NULL`
- `from_addr = envelope_from`
- `parse_status = pending`
- `text_preview = NULL`

The frontend list displays:
- Receipt time
- `from` (initially, it can display envelope_from)
- A parsing-in-progress indicator

After full parsing, update these fields with the actual header values.

## 8. Data Consistency and Recovery

### 8.1 Atomic Disk Writes

File persistence must use:
- A `.part` temporary file
- `fsync`
- Atomic replacement with `os.replace()`

### 8.2 Recovery Scanner

The following must run at startup:

1. Scan for `.eml` files under `storage/raw/`
2. Backfill message/delivery records for raw files that have no database record
3. Optionally retry parsing records with `parse_status = pending/failed`
4. Clean up stale `.part` files

### 8.3 Deduplication Strategy

Do not automatically deduplicate messages with identical content.

Reasons:
- The same raw message may legitimately be delivered more than once
- A second arrival must not be swallowed because it has the same hash

`sha256` is used only for:
- Integrity verification
- Operational comparison

## 9. Storage Design

### 9.1 File Layout

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

### 9.2 SQLite PRAGMA Requirements

Set at startup:

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;
```

Notes:
- Use `WAL` by default
- Use `FULL` by default to reduce the risk of a SQLite commit being rolled back or corrupted after power loss; end-to-end durability still depends on
  durable ACK, file/directory fsync, the filesystem, and storage hardware
- Users who accept a very small risk of rollback after power loss can switch to `NORMAL` for lower latency

## 10. SQLite Table Design

> See the accompanying `sqlite_schema.sql` for details.

### 10.1 admins

Administrator accounts.

### 10.2 admin_sessions

Admin Web login sessions using cookie-based sessions.

### 10.3 domains

Domain rules.

### 10.4 mailboxes

Virtual mailboxes.

Each mailbox record corresponds to a normalized mailbox address, for example:
- `xxx@adb.com`
- `xxx@1.adb.com`

### 10.5 smtp_sessions

SMTP connection summaries.

### 10.6 smtp_events

Historical SMTP events (not required on the hot path, but needed for admin troubleshooting).

### 10.7 messages

Raw message objects; each raw message is stored only once.

### 10.8 message_deliveries

Delivery relationships between messages and mailboxes.

If one message is delivered to both:
- `a@adb.com`
- `b@adb.com`

Then:
- `messages` has only 1 row
- `message_deliveries` has 2 rows

### 10.8.1 mailbox_live_events

Transactional outbox for public-mailbox WebSocket notifications. Delivery inserts and parse-state
changes write this table in the same SQLite transaction as the source row. It retains at most one
delivery event and the latest parse-update event per delivery, and delivery deletion removes both
events even when legacy maintenance code has temporarily disabled foreign-key enforcement.

### 10.9 attachments

Attachment index.

### 10.10 api_keys / api_key_scopes / api_key_domain_grants / api_key_mailbox_grants

API keys, permission scopes, and resource bindings.

### 10.11 audit_logs

Audit logs.

### 10.12 system_settings

Global key-value settings.

## 11. Authorization Model

### 11.1 Admin Console Identity Model

The admin UI uses:
- Administrator username/password login
- A session cookie issued after successful login

### 11.2 API Key Model

An API key uses:
- A `prefix + secret` format
- Only `secret_hash` is stored in the database
- The plaintext key is shown only once, when it is created

Recommended formats:
- `ri_admin_<prefix>_<secret>`
- `ri_public_<prefix>_<secret>`

### 11.3 API Key Types

- `admin`
- `service`
- `public`

### 11.4 Scope List

Minimum scope set:

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

### 11.5 Resource Restrictions

In addition to scopes, an API key must support resource bindings:

- Domain-level authorization
- Mailbox-level authorization

Example:
- A key has only `public.read`
- And it is restricted to `adb.com`
- Or it is restricted to `foo@adb.com`

### 11.6 Key Validation Order

1. Look up the key quickly by prefix
2. Verify the hash
3. Verify status/expiration
4. Verify the source IP allowlist (if present)
5. Verify the scope
6. Verify domain/mailbox bindings
7. Record `last_used_at` / `last_used_ip`

## 12. HTTP Route Design

## 12.1 Public HTML

### Mailbox Page

- `GET /mail/{mailbox_address}`
  - Function: mailbox message list
  - No login required

### Message Detail Page

- `GET /mail/{mailbox_address}/{delivery_id}`
  - Function: view message details
  - No login required

### Raw Message

- `GET /mail/{mailbox_address}/{delivery_id}/raw`
  - Function: download the raw `.eml`

### Attachment Download

- `GET /mail/{mailbox_address}/{delivery_id}/attachments/{attachment_id}`

## 12.2 Public API

> Design decision: HTML pages are anonymous; the JSON API requires an API key by default. This keeps the frontend passwordless while preserving the complete authorization model for the API.

### List Mailbox Messages

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages`
- Header: `X-API-Key: ...`
- Query:
  - `limit`
  - `cursor`

Response:

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

### Get Message Details

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}`

### Download Raw Message

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/raw`

### Download Attachment

- `GET /api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/attachments/{attachment_id}`

## 12.3 Admin API

### Domain Management

- `GET /api/v1/admin/domains`
- `POST /api/v1/admin/domains`
- `GET /api/v1/admin/domains/{id}`
- `PATCH /api/v1/admin/domains/{id}`
- `DELETE /api/v1/admin/domains/{id}`
- `POST /api/v1/admin/domains/{id}/dns-check`

Example `POST /api/v1/admin/domains` request:

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

### Mailbox Management

- `GET /api/v1/admin/mailboxes`
- `GET /api/v1/admin/mailboxes/{id}`
- `PATCH /api/v1/admin/mailboxes/{id}`
- `DELETE /api/v1/admin/mailboxes/{id}`
- `GET /api/v1/admin/mailboxes/{id}/messages`

### Message Management

- `GET /api/v1/admin/messages`
- `GET /api/v1/admin/messages/{message_id}`
- `POST /api/v1/admin/messages/{message_id}/reparse`
- `DELETE /api/v1/admin/deliveries/{delivery_id}`
- `POST /api/v1/admin/deliveries/bulk-delete`

### SMTP Sessions / Live Information

- `GET /api/v1/admin/smtp/sessions`
- `GET /api/v1/admin/smtp/sessions/{session_id}`
- `WebSocket /api/v1/admin/live/smtp/ws?after_cursor={cursor}` (primary administration live interface)
- `GET /api/v1/admin/live/smtp/stream` (deprecated SSE compatibility route)

The administration UI uses the server-only `/live/smtp/ws` WebSocket. Every data or control JSON
message carries a `generation:sequence` cursor, and reconnects pass the newest cursor through
`after_cursor`. A ring overrun or generation change detected on an established stream emits a `gap`
message and then continues from the oldest available event. A reconnect whose supplied generation is
already stale falls back to the current ring or committed history. An internal event that is not
exposed to administrators emits a cursor-only control message so reconnects do not replay it.

Cookie-authenticated handshakes require exactly one `Origin` whose HTTP(S) scheme, host, and port
match the effective WebSocket URL (`ws` maps to `http`, and `wss` maps to `https`). Header
`X-API-Key` authentication requires `live.read` and a global grant;
credentials in the query string are rejected. Authentication/policy failures and client application
frames delivered to the route close with code `1008`; supported launchers reject larger inbound
messages at 16 KiB before application processing. The shared live-connection limit closes excess
WebSockets with `1013`, and the administration page uses `1000` for normal unload. Internet-facing
deployments must use WSS and preserve WebSocket upgrade headers at the trusted reverse proxy. The old
`/live/smtp/stream` SSE endpoint remains only as a deprecated compatibility route.

### API Key Management

- `GET /api/v1/admin/api-keys`
- `POST /api/v1/admin/api-keys`
- `GET /api/v1/admin/api-keys/{id}`
- `PATCH /api/v1/admin/api-keys/{id}`
- `POST /api/v1/admin/api-keys/{id}/rotate`
- `POST /api/v1/admin/api-keys/{id}/revoke`

### Audit and Settings

- `GET /api/v1/admin/audit-logs`
- `GET /api/v1/admin/settings`
- `PATCH /api/v1/admin/settings`

## 13. Admin Console Pages

### 13.1 Page List

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

### 13.2 Home Dashboard

Displays:
- Current active SMTP session count
- 1-minute / 5-minute receipt rate
- Pending parsing queue length
- Number received in the last 24 hours
- Number of failures in the last 24 hours
- Disk usage
- Domain count / mailbox count / message count

### 13.3 Live Connection Panel

Example JSON message pushed over the administration WebSocket:

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
  "state": "rcpt",
  "cursor": "generation:42"
}
```

Process-local data event types:
- `connect`
- `rcpt_accepted`
- `rcpt_rejected`
- `queued`
- `disconnect`
- `error`

Cross-process and control event types:
- `delivery_committed`: maps only a committed `mailbox_delivery` from the SQLite outbox and uses
  `source: committed_outbox`; C++/separate ingress does not provide live connection or command events
- `gap`: reports `ring_overrun` or `generation_changed` before replay continues from the oldest
  available event
- `cursor`: advances resume state when an internal event, such as a parse-only delivery update, is not
  rendered in the administration feed

## 14. Public Frontend Behavior

### 14.1 Mailbox List Page

Displays:
- Receipt time
- Sender
- Subject
- Attachment indicator
- Parsing status
- Pagination
- Live insertion and update of newly committed deliveries over WebSocket
- Full-page resynchronization when the WebSocket cursor has a gap

### 14.2 Message Detail Page

Displays:
- Header summary
- Text body
- HTML body
- Raw message download
- Attachment list

### 14.3 HTML Message Rendering Security

The following requirements must be met:
- Do not insert raw HTML directly into the main site's DOM
- Use a separate rendering route with a sandboxed iframe, or sanitize it strictly first
- Do not load remote images automatically by default
- Allow rewritten local CID attachments to be displayed

## 15. Performance Targets (Product Requirements)

The following are implementation targets, not guarantees:

1. For a small message (<= 100KB), time from the end of `DATA` to the `250` response:
   - Median target < 250ms on an idle machine
2. Time from the SQLite delivery commit until it is visible in an already-open public mailbox:
   - Target < 1s
3. Latency for a live connection event in the admin console:
   - Target < 500ms

### 15.1 Performance Critical Points

1. The SMTP hot path must not perform:
   - Deep MIME parsing
   - Attachment extraction
   - HTML sanitization
   - Remote DNS queries
   - Complex SQL searches
2. Domain rules must be loaded into memory.
3. Use a single writer queue to avoid SQLite write-lock contention.
4. Frontend reads should use short transactions and read-only connections.

## 16. Operations and Security Supplementary Requirements

### 16.1 System Protections Required Even Without Anti-Spam

These protect the system itself; they are not spam filtering:

- Maximum message size limit (configurable)
- Per-connection idle timeout
- Concurrent connection limit
- Maximum recipients per message
- Short-term per-IP connection limit (only to prevent service exhaustion, not for spam classification)
- Disk usage alerting

### 16.2 Audit Requirements

The following must be recorded:
- Domain creation/modification/deletion
- API key creation/rotation/revocation
- Administrator login/logout
- Message deletion/bulk deletion
- Settings changes

### 16.3 Backup Requirements

At minimum, back up:
- SQLite database file
- `-wal` / `-shm` files (when making an online cold copy)
- Raw message directory
- Attachment directory

### 16.4 Privacy and Product Explanation

The admin console and home page must clearly show:

> New domains and the any-domain policy are private by default. A corresponding mailbox becomes public only after an administrator explicitly enables domain-level public Web/API access; public inboxes are intended only for testing, temporary, and demonstration scenarios, not for private communication or long-term sensitive information.

## 17. Suggested Code Directory (Initial Plan)

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
    live.py
    sse.py  # deprecated compatibility wrapper
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

## 18. Implementation Order (Initial Task Breakdown)

### Phase 1: Minimum Viable Receiving Path

1. Create the SQLite schema
2. Complete the domain matcher
3. Complete SMTP receiving and raw persistence
4. Complete placeholder writes for mailbox/message/delivery
5. Complete the frontend mailbox list and message detail pages

### Phase 2: Admin and API

6. Administrator login
7. Domain CRUD
8. Admin API
9. Public API
10. API keys / scopes / resource bindings

### Phase 3: Real-Time and Operations

11. Authenticated SMTP live-connection WebSocket and durable public-mailbox WebSocket delivery events
12. Historical SMTP session queries
13. DNS check page
14. Audit logs
15. Recovery scanner

### Phase 4: Message Details

16. MIME parsing
17. Attachment extraction
18. Secure HTML rendering
19. Bulk deletion / reparse / raw download

## 19. Acceptance Criteria

1. After adding `adb.com`, the system can receive `foo@adb.com`.
2. After enabling `accept_subdomains`, the system can receive `foo@x.adb.com` and `foo@y.x.adb.com`.
3. After explicitly enabling domain-level public Web access, visiting `https://adb.com/mail/foo@adb.com` shows that mailbox's messages.
4. A publicly enabled mailbox can be browsed without a frontend login.
5. Committed mailbox deliveries from embedded Python SMTP, standalone Python SMTP, and C++ ingestd appear in an already-open public mailbox over WebSocket without a manual refresh. Cursor gaps trigger a full-page resynchronization. The administration UI uses `/api/v1/admin/live/smtp/ws`, resumes with `after_cursor`, and reports gaps without pretending that history is command telemetry. Connection/command events remain process-local; separate C++ ingress contributes only committed `delivery_committed` events and historical session data.
6. An administrator API key can restrict permissions by scope and by domain/mailbox.
7. The Public API can retrieve messages from a public mailbox using a read-only key.
8. In durable ACK mode, the raw message file is persisted successfully before `250` is returned.
9. Frontend reads can continue while SQLite write pressure increases.
10. After an abnormal restart, the recovery scanner can recover messages that were persisted but not inserted into the database.

## 20. Initial Design Constraints (Historical Record, Not Current Mandatory Contract)

The following entries record the early design and cannot replace the current code, configuration, README, SECURITY, or tests. The implementation has evolved into a two-process C++ ingestd and Python control-plane architecture; contributors must update the current contract and regression tests before changing behavior.

1. **Public HTML: anonymous access only for domains/mailboxes explicitly enabled for public Web access**
2. **Public JSON API: requires an API key**
3. **Admin UI: username/password + session cookie**
4. **Admin API: requires an API key**
5. **SMTP receives only; it does not send**
6. **SQLite stores indexes only; large files go on disk**
7. **Heavy processing is forbidden on the receiving hot path**
8. **Initial plan: placeholder insertion + asynchronous parsing; the current implementation follows the actual data-plane batch order**
9. **New domains and the any-domain policy are private by default; administrators can separately enable domain-level Web/API access and continue to disable public access per mailbox**
10. **Enterprise-grade multi-node expansion is not a current goal; changing this boundary requires redesigning and documenting storage, event, and migration contracts**
