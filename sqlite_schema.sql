PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
PRAGMA synchronous = FULL;

BEGIN;

CREATE TABLE IF NOT EXISTS admins (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'superadmin' CHECK (role IN ('superadmin', 'operator', 'viewer')),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    must_change_password INTEGER NOT NULL DEFAULT 0 CHECK (must_change_password IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_login_at TEXT,
    last_login_ip TEXT
);

CREATE TABLE IF NOT EXISTS admin_sessions (
    id TEXT PRIMARY KEY,
    admin_id INTEGER NOT NULL,
    session_token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    last_seen_at TEXT,
    last_ip TEXT,
    user_agent TEXT,
    FOREIGN KEY (admin_id) REFERENCES admins(id) ON DELETE CASCADE
);

CREATE TRIGGER IF NOT EXISTS prevent_last_active_superadmin_update
BEFORE UPDATE OF role, is_active ON admins
WHEN OLD.role = 'superadmin'
    AND OLD.is_active = 1
    AND (NEW.role <> 'superadmin' OR NEW.is_active <> 1)
    AND (SELECT COUNT(*) FROM admins WHERE role = 'superadmin' AND is_active = 1) <= 1
BEGIN
    SELECT RAISE(ABORT, 'cannot remove last active superadmin');
END;

CREATE TRIGGER IF NOT EXISTS prevent_last_active_superadmin_delete
BEFORE DELETE ON admins
WHEN OLD.role = 'superadmin'
    AND OLD.is_active = 1
    AND (SELECT COUNT(*) FROM admins WHERE role = 'superadmin' AND is_active = 1) <= 1
BEGIN
    SELECT RAISE(ABORT, 'cannot remove last active superadmin');
END;

CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin_active
ON admin_sessions(admin_id, revoked_at, expires_at);

CREATE TABLE IF NOT EXISTS domains (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    root_domain_ascii TEXT NOT NULL UNIQUE,
    root_domain_unicode TEXT,
    accept_exact INTEGER NOT NULL DEFAULT 1 CHECK (accept_exact IN (0, 1)),
    accept_subdomains INTEGER NOT NULL DEFAULT 1 CHECK (accept_subdomains IN (0, 1)),
    public_web_enabled INTEGER NOT NULL DEFAULT 0 CHECK (public_web_enabled IN (0, 1)),
    public_api_enabled INTEGER NOT NULL DEFAULT 0 CHECK (public_api_enabled IN (0, 1)),
    is_active INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
    is_hidden INTEGER NOT NULL DEFAULT 0 CHECK (is_hidden IN (0, 1)),
    local_part_case_sensitive INTEGER NOT NULL DEFAULT 0 CHECK (local_part_case_sensitive IN (0, 1)),
    plus_addressing_mode TEXT NOT NULL DEFAULT 'keep' CHECK (plus_addressing_mode IN ('keep', 'strip')),
    max_message_size_bytes INTEGER NOT NULL DEFAULT 52428800,
    retention_days INTEGER,
    dns_status TEXT NOT NULL DEFAULT 'unknown' CHECK (dns_status IN ('unknown', 'ok', 'warning', 'error')),
    dns_last_checked_at TEXT,
    dns_details_json TEXT,
    notes TEXT,
    created_by_admin_id INTEGER,
    updated_by_admin_id INTEGER,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (created_by_admin_id) REFERENCES admins(id) ON DELETE SET NULL,
    FOREIGN KEY (updated_by_admin_id) REFERENCES admins(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS mailboxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_id INTEGER NOT NULL,
    local_part_canonical TEXT NOT NULL,
    rcpt_domain_ascii TEXT NOT NULL,
    address_canonical TEXT NOT NULL UNIQUE,
    address_display TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    latest_message_at TEXT,
    message_count INTEGER NOT NULL DEFAULT 0,
    public_enabled INTEGER NOT NULL DEFAULT 1 CHECK (public_enabled IN (0, 1)),
    is_hidden INTEGER NOT NULL DEFAULT 0 CHECK (is_hidden IN (0, 1)),
    notes TEXT,
    -- A mailbox clear increments this value before any page is processed.
    -- Deliveries accepted afterwards inherit the new generation and therefore
    -- cannot be consumed by the already-persisted clear job, even if SQLite
    -- later reuses a deleted rowid.
    bulk_delete_generation INTEGER NOT NULL DEFAULT 0
        CHECK (bulk_delete_generation >= 0),
    FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_mailboxes_domain ON mailboxes(domain_id);
CREATE INDEX IF NOT EXISTS idx_mailboxes_domain_local ON mailboxes(domain_id, rcpt_domain_ascii, local_part_canonical);
CREATE INDEX IF NOT EXISTS idx_mailboxes_latest_message ON mailboxes(latest_message_at DESC);
CREATE INDEX IF NOT EXISTS idx_mailboxes_latest_sort
ON mailboxes(COALESCE(latest_message_at, '') DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_mailboxes_stale_empty
ON mailboxes(last_seen_at ASC, id ASC)
WHERE message_count = 0 AND latest_message_at IS NULL;

CREATE TABLE IF NOT EXISTS smtp_sessions (
    id TEXT PRIMARY KEY,
    remote_ip TEXT NOT NULL,
    remote_port INTEGER,
    local_ip TEXT,
    local_port INTEGER,
    helo_name TEXT,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed', 'error')),
    tls_used INTEGER NOT NULL DEFAULT 0 CHECK (tls_used IN (0, 1)),
    tls_cipher TEXT,
    tls_protocol TEXT,
    connect_at TEXT NOT NULL,
    disconnect_at TEXT,
    first_command_at TEXT,
    last_command_at TEXT,
    message_count INTEGER NOT NULL DEFAULT 0,
    rcpt_accepted_count INTEGER NOT NULL DEFAULT 0,
    rcpt_rejected_count INTEGER NOT NULL DEFAULT 0,
    bytes_received INTEGER NOT NULL DEFAULT 0,
    last_mail_from TEXT,
    last_rcpt_to_sample TEXT,
    result_code INTEGER,
    result_message TEXT,
    close_reason TEXT
);

CREATE INDEX IF NOT EXISTS idx_smtp_sessions_connect_at ON smtp_sessions(connect_at DESC);
CREATE INDEX IF NOT EXISTS idx_smtp_sessions_connect_id
ON smtp_sessions(connect_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_smtp_sessions_remote_ip ON smtp_sessions(remote_ip, connect_at DESC);
CREATE INDEX IF NOT EXISTS idx_smtp_sessions_status ON smtp_sessions(status, connect_at DESC);
CREATE INDEX IF NOT EXISTS idx_smtp_sessions_retention_time
ON smtp_sessions(COALESCE(disconnect_at, last_command_at, connect_at), id);

CREATE TABLE IF NOT EXISTS smtp_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    ts TEXT NOT NULL,
    payload_json TEXT,
    FOREIGN KEY (session_id) REFERENCES smtp_sessions(id) ON DELETE CASCADE,
    UNIQUE (session_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_smtp_events_session_ts ON smtp_events(session_id, ts ASC);
CREATE INDEX IF NOT EXISTS idx_smtp_events_type_ts ON smtp_events(event_type, ts DESC);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    smtp_session_id TEXT,
    raw_path TEXT NOT NULL UNIQUE,
    raw_sha256 TEXT NOT NULL,
    raw_size_bytes INTEGER NOT NULL,
    envelope_from TEXT,
    message_id_header TEXT,
    subject TEXT,
    from_name TEXT,
    from_addr TEXT,
    reply_to TEXT,
    date_header TEXT,
    received_at TEXT NOT NULL,
    indexed_at TEXT,
    parse_status TEXT NOT NULL DEFAULT 'pending' CHECK (parse_status IN ('pending', 'parsed', 'failed')),
    parse_error TEXT,
    has_text INTEGER NOT NULL DEFAULT 0 CHECK (has_text IN (0, 1)),
    has_html INTEGER NOT NULL DEFAULT 0 CHECK (has_html IN (0, 1)),
    has_attachments INTEGER NOT NULL DEFAULT 0 CHECK (has_attachments IN (0, 1)),
    attachment_count INTEGER NOT NULL DEFAULT 0,
    text_preview TEXT,
    text_body_path TEXT,
    html_body_path TEXT,
    headers_json TEXT,
    verification_code TEXT,
    is_deleted_globally INTEGER NOT NULL DEFAULT 0 CHECK (is_deleted_globally IN (0, 1)),
    FOREIGN KEY (smtp_session_id) REFERENCES smtp_sessions(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_received_at ON messages(received_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_received_id ON messages(received_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_parse_status ON messages(parse_status, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_parse_received_id
ON messages(parse_status, received_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_messages_message_id_header ON messages(message_id_header);
CREATE INDEX IF NOT EXISTS idx_messages_from_addr ON messages(from_addr, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_messages_subject ON messages(subject);
CREATE INDEX IF NOT EXISTS idx_messages_raw_sha256 ON messages(raw_sha256);
CREATE INDEX IF NOT EXISTS idx_messages_text_body_path
ON messages(text_body_path)
WHERE text_body_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_messages_html_body_path
ON messages(html_body_path)
WHERE html_body_path IS NOT NULL;

CREATE TABLE IF NOT EXISTS message_deliveries (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    mailbox_id INTEGER NOT NULL,
    rcpt_to TEXT NOT NULL,
    delivered_at TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'deleted', 'hidden')),
    deleted_at TEXT,
    expires_at TEXT,
    notes TEXT,
    -- -1 is an insertion sentinel for compatibility with direct SQL writers;
    -- the bootstrap trigger below replaces it with the mailbox's current
    -- generation in the same statement transaction.
    mailbox_generation INTEGER NOT NULL DEFAULT -1
        CHECK (mailbox_generation >= -1),
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    FOREIGN KEY (mailbox_id) REFERENCES mailboxes(id) ON DELETE RESTRICT,
    UNIQUE (message_id, mailbox_id)
);

CREATE INDEX IF NOT EXISTS idx_message_deliveries_mailbox_time ON message_deliveries(mailbox_id, delivered_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_mailbox_status_time_id
ON message_deliveries(mailbox_id, status, delivered_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_message ON message_deliveries(message_id);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_status_time ON message_deliveries(status, delivered_at DESC);
CREATE INDEX IF NOT EXISTS idx_message_deliveries_rcpt_to ON message_deliveries(rcpt_to, delivered_at DESC);
-- initialize_database installs the generation-aware partial paging index after
-- applying lightweight column migrations. Keeping it out of this base script
-- lets existing databases add mailbox_generation before SQLite parses it.

CREATE TRIGGER IF NOT EXISTS message_deliveries_fill_mailbox_generation
AFTER INSERT ON message_deliveries
WHEN NEW.mailbox_generation = -1
BEGIN
    UPDATE message_deliveries
    SET mailbox_generation = (
        SELECT bulk_delete_generation
        FROM mailboxes
        WHERE id = NEW.mailbox_id
    )
    WHERE rowid = NEW.rowid;
END;

-- Cross-process mailbox updates are recorded in the same transaction as the
-- delivery change. The HTTP process tails this compact outbox and fans events
-- out to its in-memory WebSocket subscribers; SMTP writers don't need an IPC
-- dependency or process-specific SQLite functions.
CREATE TABLE IF NOT EXISTS mailbox_live_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('mailbox_delivery', 'mailbox_delivery_updated')),
    delivery_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (delivery_id) REFERENCES message_deliveries(id) ON DELETE CASCADE,
    UNIQUE (event_type, delivery_id)
);

CREATE INDEX IF NOT EXISTS idx_mailbox_live_events_delivery_id
ON mailbox_live_events(delivery_id, id);

CREATE TRIGGER IF NOT EXISTS mailbox_live_events_after_delivery_insert
AFTER INSERT ON message_deliveries
WHEN NEW.status = 'active'
BEGIN
    INSERT INTO mailbox_live_events (event_type, delivery_id, created_at)
    VALUES ('mailbox_delivery', NEW.id, NEW.delivered_at);
END;

CREATE TRIGGER IF NOT EXISTS mailbox_live_events_after_message_parse_update
AFTER UPDATE OF parse_status, indexed_at ON messages
WHEN OLD.parse_status IS NOT NEW.parse_status
    OR OLD.indexed_at IS NOT NEW.indexed_at
BEGIN
    INSERT OR REPLACE INTO mailbox_live_events (event_type, delivery_id, created_at)
    SELECT 'mailbox_delivery_updated', delivery.id,
           COALESCE(NEW.indexed_at, NEW.received_at)
    FROM message_deliveries AS delivery
    WHERE delivery.message_id = NEW.id
      AND delivery.status = 'active';
END;

-- Keep databases migrated to this schema compatible with older clear-all
-- code, which temporarily disables foreign keys and does not know this table.
CREATE TRIGGER IF NOT EXISTS mailbox_live_events_after_delivery_delete
AFTER DELETE ON message_deliveries
BEGIN
    DELETE FROM mailbox_live_events
    WHERE delivery_id = OLD.id;
END;

CREATE TABLE IF NOT EXISTS mail_metric_buckets (
    bucket_ts TEXT PRIMARY KEY,
    received INTEGER NOT NULL DEFAULT 0,
    deliveries INTEGER NOT NULL DEFAULT 0,
    parse_failures INTEGER NOT NULL DEFAULT 0,
    rejected INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS attachments (
    id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL,
    part_index INTEGER NOT NULL,
    filename TEXT,
    safe_filename TEXT,
    content_type TEXT,
    content_disposition TEXT,
    content_id TEXT,
    storage_path TEXT NOT NULL,
    sha256 TEXT,
    size_bytes INTEGER NOT NULL,
    is_inline INTEGER NOT NULL DEFAULT 0 CHECK (is_inline IN (0, 1)),
    created_at TEXT NOT NULL,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE,
    UNIQUE (message_id, part_index)
);

CREATE INDEX IF NOT EXISTS idx_attachments_message ON attachments(message_id);
CREATE INDEX IF NOT EXISTS idx_attachments_sha256 ON attachments(sha256);
CREATE INDEX IF NOT EXISTS idx_attachments_storage_path
ON attachments(storage_path);

CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    public_id TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    kind TEXT NOT NULL CHECK (kind IN ('admin', 'service', 'public')),
    key_prefix TEXT NOT NULL UNIQUE,
    secret_hash TEXT NOT NULL UNIQUE,
    owner_admin_id INTEGER,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'revoked', 'expired', 'disabled')),
    domain_grant_mode TEXT NOT NULL DEFAULT 'none'
        CHECK (domain_grant_mode IN ('none', 'selected', 'all')),
    allow_header INTEGER NOT NULL DEFAULT 1 CHECK (allow_header IN (0, 1)),
    allow_query INTEGER NOT NULL DEFAULT 0 CHECK (allow_query IN (0, 1)),
    rate_limit_per_min INTEGER NOT NULL DEFAULT 3600,
    allowed_ip_cidrs TEXT,
    expires_at TEXT,
    last_used_at TEXT,
    last_used_ip TEXT,
    revoked_at TEXT,
    created_by_admin_id INTEGER,
    created_at TEXT NOT NULL,
    FOREIGN KEY (owner_admin_id) REFERENCES admins(id) ON DELETE SET NULL,
    FOREIGN KEY (created_by_admin_id) REFERENCES admins(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_api_keys_status ON api_keys(status, expires_at);
CREATE INDEX IF NOT EXISTS idx_api_keys_kind ON api_keys(kind, status);
CREATE INDEX IF NOT EXISTS idx_api_keys_created_id
ON api_keys(created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS api_key_scopes (
    api_key_id INTEGER NOT NULL,
    scope TEXT NOT NULL,
    PRIMARY KEY (api_key_id, scope),
    FOREIGN KEY (api_key_id) REFERENCES api_keys(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS api_key_domain_grants (
    api_key_id INTEGER NOT NULL,
    domain_id INTEGER NOT NULL,
    PRIMARY KEY (api_key_id, domain_id),
    FOREIGN KEY (api_key_id) REFERENCES api_keys(id) ON DELETE CASCADE,
    FOREIGN KEY (domain_id) REFERENCES domains(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS api_key_mailbox_grants (
    api_key_id INTEGER NOT NULL,
    mailbox_pattern TEXT NOT NULL,
    PRIMARY KEY (api_key_id, mailbox_pattern),
    FOREIGN KEY (api_key_id) REFERENCES api_keys(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    actor_type TEXT NOT NULL CHECK (actor_type IN ('admin', 'api_key', 'system', 'anonymous')),
    actor_ref TEXT,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_ref TEXT,
    status TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    ip TEXT,
    user_agent TEXT,
    details_json TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_id
ON audit_logs(created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_actor ON audit_logs(actor_type, actor_ref, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource ON audit_logs(resource_type, resource_ref, created_at DESC);

CREATE TABLE IF NOT EXISTS system_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS file_gc_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_path TEXT NOT NULL UNIQUE,
    reason TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    next_attempt_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_file_gc_tasks_next_attempt
ON file_gc_tasks(next_attempt_at, id);

CREATE TABLE IF NOT EXISTS maintenance_runs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    details_json TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_maintenance_runs_kind_started
ON maintenance_runs(kind, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_maintenance_runs_status_finished
ON maintenance_runs(status, finished_at, id);

CREATE TABLE IF NOT EXISTS domain_rehome_jobs (
    id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    candidate_root_domain TEXT,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    cursor_mailbox_id INTEGER NOT NULL DEFAULT 0,
    max_mailbox_id INTEGER NOT NULL,
    mailboxes_scanned INTEGER NOT NULL DEFAULT 0,
    mailboxes_rehomed INTEGER NOT NULL DEFAULT 0,
    deliveries_moved INTEGER NOT NULL DEFAULT 0,
    deliveries_deduplicated INTEGER NOT NULL DEFAULT 0,
    destination_domain_ids_json TEXT NOT NULL DEFAULT '[]',
    marks_ownership_upgrade INTEGER NOT NULL DEFAULT 0
        CHECK (marks_ownership_upgrade IN (0, 1)),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_domain_rehome_jobs_status_created
ON domain_rehome_jobs(status, created_at, id);

CREATE TABLE IF NOT EXISTS mailbox_bulk_delete_jobs (
    id TEXT PRIMARY KEY,
    mailbox_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    cursor_delivery_rowid INTEGER NOT NULL DEFAULT 0,
    max_delivery_rowid INTEGER NOT NULL,
    target_generation INTEGER NOT NULL DEFAULT 0
        CHECK (target_generation >= 0),
    deleted_count INTEGER NOT NULL DEFAULT 0,
    deleted_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    error TEXT,
    FOREIGN KEY (mailbox_id) REFERENCES mailboxes(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mailbox_bulk_delete_jobs_incomplete_mailbox
ON mailbox_bulk_delete_jobs(mailbox_id)
WHERE status IN ('pending', 'running', 'failed');

CREATE INDEX IF NOT EXISTS idx_mailbox_bulk_delete_jobs_status_created
ON mailbox_bulk_delete_jobs(status, created_at, id);

-- Dashboard totals live in one cache-hot row so a status refresh never scans
-- an unbounded application table.  These counters are maintained by triggers
-- because administrative and recovery writes can originate from either the
-- Python process, the C++ ingest daemon, or an operator's migration script.
CREATE TABLE IF NOT EXISTS dashboard_counters (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    domains INTEGER NOT NULL DEFAULT 0 CHECK (domains >= 0),
    mailboxes INTEGER NOT NULL DEFAULT 0 CHECK (mailboxes >= 0),
    messages INTEGER NOT NULL DEFAULT 0 CHECK (messages >= 0),
    api_keys INTEGER NOT NULL DEFAULT 0 CHECK (api_keys >= 0),
    audit_logs INTEGER NOT NULL DEFAULT 0 CHECK (audit_logs >= 0),
    pending_messages INTEGER NOT NULL DEFAULT 0 CHECK (pending_messages >= 0),
    failed_messages INTEGER NOT NULL DEFAULT 0 CHECK (failed_messages >= 0)
) WITHOUT ROWID;

INSERT OR IGNORE INTO dashboard_counters (
    singleton_id,
    domains,
    mailboxes,
    messages,
    api_keys,
    audit_logs,
    pending_messages,
    failed_messages
)
SELECT
    1,
    (SELECT COUNT(*) FROM domains),
    (SELECT COUNT(*) FROM mailboxes),
    (SELECT COUNT(*) FROM messages),
    (SELECT COUNT(*) FROM api_keys),
    (SELECT COUNT(*) FROM audit_logs),
    (SELECT COUNT(*) FROM messages WHERE parse_status = 'pending'),
    (SELECT COUNT(*) FROM messages WHERE parse_status = 'failed');

CREATE TRIGGER IF NOT EXISTS dashboard_domains_insert
AFTER INSERT ON domains
BEGIN
    UPDATE dashboard_counters SET domains = domains + 1 WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_domains_delete
AFTER DELETE ON domains
BEGIN
    UPDATE dashboard_counters SET domains = MAX(domains - 1, 0) WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_mailboxes_insert
AFTER INSERT ON mailboxes
BEGIN
    UPDATE dashboard_counters SET mailboxes = mailboxes + 1 WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_mailboxes_delete
AFTER DELETE ON mailboxes
BEGIN
    UPDATE dashboard_counters SET mailboxes = MAX(mailboxes - 1, 0) WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_messages_insert
AFTER INSERT ON messages
BEGIN
    UPDATE dashboard_counters
    SET messages = messages + 1,
        pending_messages = pending_messages + (NEW.parse_status = 'pending'),
        failed_messages = failed_messages + (NEW.parse_status = 'failed')
    WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_messages_delete
AFTER DELETE ON messages
BEGIN
    UPDATE dashboard_counters
    SET messages = MAX(messages - 1, 0),
        pending_messages = MAX(pending_messages - (OLD.parse_status = 'pending'), 0),
        failed_messages = MAX(failed_messages - (OLD.parse_status = 'failed'), 0)
    WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_messages_parse_status_update
AFTER UPDATE OF parse_status ON messages
WHEN OLD.parse_status <> NEW.parse_status
BEGIN
    UPDATE dashboard_counters
    SET pending_messages = MAX(
            pending_messages
            - (OLD.parse_status = 'pending')
            + (NEW.parse_status = 'pending'),
            0
        ),
        failed_messages = MAX(
            failed_messages
            - (OLD.parse_status = 'failed')
            + (NEW.parse_status = 'failed'),
            0
        )
    WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_api_keys_insert
AFTER INSERT ON api_keys
BEGIN
    UPDATE dashboard_counters SET api_keys = api_keys + 1 WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_api_keys_delete
AFTER DELETE ON api_keys
BEGIN
    UPDATE dashboard_counters SET api_keys = MAX(api_keys - 1, 0) WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_audit_logs_insert
AFTER INSERT ON audit_logs
BEGIN
    UPDATE dashboard_counters SET audit_logs = audit_logs + 1 WHERE singleton_id = 1;
END;

CREATE TRIGGER IF NOT EXISTS dashboard_audit_logs_delete
AFTER DELETE ON audit_logs
BEGIN
    UPDATE dashboard_counters SET audit_logs = MAX(audit_logs - 1, 0) WHERE singleton_id = 1;
END;

COMMIT;
