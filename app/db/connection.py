from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "sqlite_schema.sql"


@contextmanager
def connect_database(
    database_path: Path,
    *,
    durable_writes: bool = False,
) -> Iterator[sqlite3.Connection]:
    connection = sqlite3.connect(database_path, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    apply_pragmas(connection, durable_writes=durable_writes)
    try:
        yield connection
    except BaseException:
        connection.rollback()
        raise
    else:
        connection.commit()
    finally:
        connection.close()


def apply_pragmas(connection: sqlite3.Connection, *, durable_writes: bool = False) -> None:
    # journal_mode is database-persistent and is established by the schema
    # bootstrap. Reapplying it on every short-lived read connection forces
    # SQLite to reopen WAL state and is disproportionately expensive under a
    # high-concurrency HTTP workload.
    connection.execute("PRAGMA foreign_keys = ON;")
    connection.execute("PRAGMA busy_timeout = 5000;")
    # Python's SQLite default is FULL, but mutation connections set it
    # explicitly so a custom SQLite build cannot silently weaken durability.
    # Read connections avoid this PRAGMA because merely setting it also forces
    # WAL initialization on every request.
    if durable_writes:
        connection.execute("PRAGMA synchronous = FULL;")


def initialize_database(database_path: Path) -> None:
    database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    _chmod_private(database_path.parent, directory=True)
    schema = SCHEMA_PATH.read_text(encoding="utf-8")
    with connect_database(database_path, durable_writes=True) as connection:
        connection.executescript(schema)
        _apply_lightweight_migrations(connection)
    _chmod_private(database_path)
    _chmod_private(Path(f"{database_path}-wal"))
    _chmod_private(Path(f"{database_path}-shm"))


def _chmod_private(path: Path, *, directory: bool = False) -> None:
    if not path.exists():
        return
    try:
        path.chmod(0o700 if directory else 0o600)
    except OSError:
        return


def _column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row["name"]) for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _apply_lightweight_migrations(connection: sqlite3.Connection) -> None:
    admin_columns = _column_names(connection, "admins")
    if "must_change_password" not in admin_columns:
        connection.execute(
            """
            ALTER TABLE admins
            ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0
                CHECK (must_change_password IN (0, 1))
            """
        )
    message_columns = _column_names(connection, "messages")
    if "verification_code" not in message_columns:
        connection.execute(
            """
            ALTER TABLE messages
            ADD COLUMN verification_code TEXT
            """
        )
    delivery_columns = _column_names(connection, "message_deliveries")
    if "expires_at" not in delivery_columns:
        connection.execute(
            """
            ALTER TABLE message_deliveries
            ADD COLUMN expires_at TEXT
            """
        )
    mailbox_columns = _column_names(connection, "mailboxes")
    if "bulk_delete_generation" not in mailbox_columns:
        connection.execute(
            """
            ALTER TABLE mailboxes
            ADD COLUMN bulk_delete_generation INTEGER NOT NULL DEFAULT 0
                CHECK (bulk_delete_generation >= 0)
            """
        )
    delivery_columns = _column_names(connection, "message_deliveries")
    if "mailbox_generation" not in delivery_columns:
        connection.execute(
            """
            ALTER TABLE message_deliveries
            ADD COLUMN mailbox_generation INTEGER NOT NULL DEFAULT -1
                CHECK (mailbox_generation >= -1)
            """
        )
        connection.execute(
            """
            UPDATE message_deliveries
            SET mailbox_generation = COALESCE(
                (
                    SELECT mailbox.bulk_delete_generation
                    FROM mailboxes AS mailbox
                    WHERE mailbox.id = message_deliveries.mailbox_id
                ),
                0
            )
            WHERE mailbox_generation = -1
            """
        )
    bulk_job_columns = _column_names(connection, "mailbox_bulk_delete_jobs")
    if "target_generation" not in bulk_job_columns:
        connection.execute(
            """
            ALTER TABLE mailbox_bulk_delete_jobs
            ADD COLUMN target_generation INTEGER NOT NULL DEFAULT 0
                CHECK (target_generation >= 0)
            """
        )

    # An old persisted job used only a rowid frontier.  Advance its mailbox to
    # the next generation before startup can resume the job, while leaving the
    # old deliveries and the job target at generation zero.
    connection.execute(
        """
        UPDATE mailboxes
        SET bulk_delete_generation = MAX(
            bulk_delete_generation,
            COALESCE(
                (
                    SELECT MAX(job.target_generation + 1)
                    FROM mailbox_bulk_delete_jobs AS job
                    WHERE job.mailbox_id = mailboxes.id
                      AND job.status IN ('pending', 'running', 'failed')
                ),
                bulk_delete_generation
            )
        )
        WHERE EXISTS (
            SELECT 1
            FROM mailbox_bulk_delete_jobs AS job
            WHERE job.mailbox_id = mailboxes.id
              AND job.status IN ('pending', 'running', 'failed')
        )
        """
    )
    # Replace the pre-generation hot index instead of maintaining two partial
    # indexes for every delivery insert. SQLite appends rowid to the new index,
    # preserving ordered page scans within one mailbox generation.
    connection.execute(
        "DROP INDEX IF EXISTS idx_message_deliveries_active_mailbox_rowid"
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_deliveries_active_mailbox_generation_rowid
        ON message_deliveries(mailbox_id, mailbox_generation)
        WHERE status = 'active'
        """
    )
    connection.execute(
        """
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
        END
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_deliveries_expires_at
        ON message_deliveries(expires_at)
        WHERE expires_at IS NOT NULL
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_deliveries_expiry_scan
        ON message_deliveries(expires_at ASC, id ASC)
        WHERE expires_at IS NOT NULL
        """
    )
    api_key_columns = _column_names(connection, "api_keys")
    if "domain_grant_mode" not in api_key_columns:
        connection.execute(
            """
            ALTER TABLE api_keys
            ADD COLUMN domain_grant_mode TEXT NOT NULL DEFAULT 'none'
                CHECK (domain_grant_mode IN ('none', 'selected', 'all'))
            """
        )
        connection.execute(
            """
            UPDATE api_keys
            SET domain_grant_mode = 'selected'
            WHERE EXISTS (
                SELECT 1
                FROM api_key_domain_grants AS grants
                WHERE grants.api_key_id = api_keys.id
            )
            """
        )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin_active
        ON admin_sessions(admin_id, revoked_at, expires_at)
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS mail_metric_buckets (
            bucket_ts TEXT PRIMARY KEY,
            received INTEGER NOT NULL DEFAULT 0,
            deliveries INTEGER NOT NULL DEFAULT 0,
            parse_failures INTEGER NOT NULL DEFAULT 0,
            rejected INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    metric_columns = _column_names(connection, "mail_metric_buckets")
    added_received = "received" not in metric_columns
    added_rejected = "rejected" not in metric_columns
    if added_received:
        connection.execute(
            "ALTER TABLE mail_metric_buckets ADD COLUMN received INTEGER NOT NULL DEFAULT 0"
        )
    if added_rejected:
        connection.execute(
            "ALTER TABLE mail_metric_buckets ADD COLUMN rejected INTEGER NOT NULL DEFAULT 0"
        )
    _backfill_mail_metric_buckets(
        connection,
        backfill_received=added_received,
        backfill_rejected=added_rejected,
    )


def _backfill_mail_metric_buckets(
    connection: sqlite3.Connection,
    *,
    backfill_received: bool,
    backfill_rejected: bool,
) -> None:
    existing = connection.execute("SELECT COUNT(*) AS count FROM mail_metric_buckets").fetchone()
    was_empty = existing is None or int(existing["count"]) == 0
    if was_empty:
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            )
            SELECT
                substr(delivered_at, 1, 16) || ':00Z' AS bucket_ts,
                0 AS received,
                COUNT(*) AS deliveries,
                0 AS parse_failures,
                0 AS rejected
            FROM message_deliveries
            WHERE status = 'active'
            GROUP BY bucket_ts
            ON CONFLICT(bucket_ts) DO UPDATE SET
                deliveries = mail_metric_buckets.deliveries + excluded.deliveries
            """
        )
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            )
            SELECT
                substr(received_at, 1, 16) || ':00Z' AS bucket_ts,
                0 AS received,
                0 AS deliveries,
                COUNT(*) AS parse_failures,
                0 AS rejected
            FROM messages
            WHERE parse_status = 'failed'
            GROUP BY bucket_ts
            ON CONFLICT(bucket_ts) DO UPDATE SET
                parse_failures = mail_metric_buckets.parse_failures + excluded.parse_failures
            """
        )
    if backfill_received or was_empty:
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            )
            SELECT
                substr(received_at, 1, 16) || ':00Z' AS bucket_ts,
                COUNT(*) AS received,
                0 AS deliveries,
                0 AS parse_failures,
                0 AS rejected
            FROM messages
            GROUP BY bucket_ts
            ON CONFLICT(bucket_ts) DO UPDATE SET
                received = excluded.received
            """
        )
    if backfill_rejected or was_empty:
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            )
            SELECT
                substr(connect_at, 1, 16) || ':00Z' AS bucket_ts,
                0 AS received,
                0 AS deliveries,
                0 AS parse_failures,
                SUM(rcpt_rejected_count) AS rejected
            FROM smtp_sessions
            WHERE rcpt_rejected_count > 0
            GROUP BY bucket_ts
            ON CONFLICT(bucket_ts) DO UPDATE SET
                rejected = excluded.rejected
            """
        )
