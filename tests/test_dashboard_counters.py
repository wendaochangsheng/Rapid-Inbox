from __future__ import annotations

import re
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import app.runtime as runtime_module
import app.services.dashboard as dashboard_module
from app.db.connection import connect_database, initialize_database
from app.services.dashboard import DashboardService


_STAMP = "2026-07-15T12:34:00Z"


def _counter_values(connection) -> dict[str, int]:
    row = connection.execute(
        "SELECT * FROM dashboard_counters WHERE singleton_id = 1"
    ).fetchone()
    assert row is not None
    return {
        key: int(row[key])
        for key in (
            "domains",
            "mailboxes",
            "messages",
            "api_keys",
            "audit_logs",
            "pending_messages",
            "failed_messages",
        )
    }


def _actual_values(connection) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM domains) AS domains,
            (SELECT COUNT(*) FROM mailboxes) AS mailboxes,
            (SELECT COUNT(*) FROM messages) AS messages,
            (SELECT COUNT(*) FROM api_keys) AS api_keys,
            (SELECT COUNT(*) FROM audit_logs) AS audit_logs,
            (
                SELECT COUNT(*) FROM messages WHERE parse_status = 'pending'
            ) AS pending_messages,
            (
                SELECT COUNT(*) FROM messages WHERE parse_status = 'failed'
            ) AS failed_messages
        """
    ).fetchone()
    assert row is not None
    return {key: int(row[key]) for key in row.keys()}


def _assert_counters_match(connection) -> dict[str, int]:
    counters = _counter_values(connection)
    assert counters == _actual_values(connection)
    return counters


def _insert_domain(connection, root_domain: str = "counter.example") -> int:
    cursor = connection.execute(
        """
        INSERT INTO domains (root_domain_ascii, created_at, updated_at)
        VALUES (?, ?, ?)
        """,
        (root_domain, _STAMP, _STAMP),
    )
    return int(cursor.lastrowid)


def _insert_mailbox(connection, domain_id: int, address: str = "box@counter.example") -> int:
    local_part, rcpt_domain = address.split("@", 1)
    cursor = connection.execute(
        """
        INSERT INTO mailboxes (
            domain_id,
            local_part_canonical,
            rcpt_domain_ascii,
            address_canonical,
            address_display,
            first_seen_at,
            last_seen_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (domain_id, local_part, rcpt_domain, address, address, _STAMP, _STAMP),
    )
    return int(cursor.lastrowid)


def _insert_message(
    connection,
    message_id: str,
    *,
    parse_status: str,
    received_at: str = _STAMP,
) -> None:
    connection.execute(
        """
        INSERT INTO messages (
            id,
            raw_path,
            raw_sha256,
            raw_size_bytes,
            received_at,
            parse_status
        ) VALUES (?, ?, ?, 1, ?, ?)
        """,
        (
            message_id,
            f"raw/{message_id}.eml",
            f"sha256-{message_id}",
            received_at,
            parse_status,
        ),
    )


def test_dashboard_counters_follow_direct_sql_mutations(tmp_path) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        assert _assert_counters_match(connection) == {
            "domains": 0,
            "mailboxes": 0,
            "messages": 0,
            "api_keys": 0,
            "audit_logs": 0,
            "pending_messages": 0,
            "failed_messages": 0,
        }

        domain_id = _insert_domain(connection)
        mailbox_id = _insert_mailbox(connection, domain_id)
        _insert_message(connection, "message-pending", parse_status="pending")
        _insert_message(connection, "message-failed", parse_status="failed")
        _insert_message(connection, "message-parsed", parse_status="parsed")
        connection.execute(
            """
            INSERT INTO api_keys (
                public_id,
                name,
                kind,
                key_prefix,
                secret_hash,
                created_at
            ) VALUES ('key-counter', 'Counter key', 'service', 'ric_counter', 'hash-counter', ?)
            """,
            (_STAMP,),
        )
        connection.execute(
            """
            INSERT INTO audit_logs (
                actor_type,
                action,
                resource_type,
                status,
                created_at
            ) VALUES ('system', 'counter.test', 'dashboard', 'success', ?)
            """,
            (_STAMP,),
        )

        assert _assert_counters_match(connection) == {
            "domains": 1,
            "mailboxes": 1,
            "messages": 3,
            "api_keys": 1,
            "audit_logs": 1,
            "pending_messages": 1,
            "failed_messages": 1,
        }

        connection.execute(
            "UPDATE messages SET parse_status = 'failed' WHERE id = 'message-pending'"
        )
        connection.execute(
            "UPDATE messages SET parse_status = 'parsed' WHERE id = 'message-failed'"
        )
        assert _assert_counters_match(connection)["pending_messages"] == 0
        assert _counter_values(connection)["failed_messages"] == 1

        connection.execute("DELETE FROM messages WHERE id = 'message-pending'")
        connection.execute("DELETE FROM api_keys")
        connection.execute("DELETE FROM audit_logs")
        assert _assert_counters_match(connection)["failed_messages"] == 0

        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes WHERE id = ?", (mailbox_id,))
        connection.execute("DELETE FROM domains WHERE id = ?", (domain_id,))
        assert _assert_counters_match(connection) == {
            "domains": 0,
            "mailboxes": 0,
            "messages": 0,
            "api_keys": 0,
            "audit_logs": 0,
            "pending_messages": 0,
            "failed_messages": 0,
        }


def test_dashboard_aggregates_minute_buckets_over_bounded_windows(
    tmp_path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    frozen_now = datetime(2026, 7, 15, 12, 34, 45, tzinfo=timezone.utc)

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return frozen_now.replace(tzinfo=None)
            return frozen_now.astimezone(tz)

    monkeypatch.setattr(dashboard_module, "datetime", FrozenDateTime)

    with connect_database(database_path) as connection:
        connection.executemany(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            ) VALUES (?, ?, ?, ?, ?)
            """,
            [
                ("2026-07-15T12:34:00Z", 10, 11, 1, 2),
                ("2026-07-15T12:33:00Z", 2, 3, 0, 5),
                ("2026-07-15T12:29:00Z", 5, 7, 2, 11),
                ("2026-07-15T12:28:00Z", 13, 17, 3, 19),
                ("2026-07-14T12:34:00Z", 23, 29, 5, 31),
                ("2026-07-14T12:33:00Z", 100, 100, 100, 100),
            ],
        )

    snapshot = DashboardService(
        SimpleNamespace(settings=SimpleNamespace(database_path=database_path))
    )._database_snapshot()

    assert snapshot["ok"] is True
    assert snapshot["received_last_minute"] == 12
    assert snapshot["received_last_five_minutes"] == 17
    assert snapshot["received_last_day"] == 53
    assert snapshot["deliveries_last_minute"] == 14
    assert snapshot["deliveries_last_day"] == 67
    assert snapshot["parse_failures_last_day"] == 11
    assert snapshot["rejected_last_day"] == 68


def test_legacy_metric_bucket_migration_adds_and_backfills_columns_once(tmp_path) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        domain_id = _insert_domain(connection, "migration.example")
        mailbox_id = _insert_mailbox(connection, domain_id, "box@migration.example")
        connection.execute(
            """
            INSERT INTO smtp_sessions (
                id, remote_ip, connect_at, rcpt_rejected_count
            ) VALUES ('smtp-migration-1', '127.0.0.1', ?, 4)
            """,
            (_STAMP,),
        )
        _insert_message(connection, "migration-failed", parse_status="failed")
        _insert_message(connection, "migration-parsed", parse_status="parsed")
        connection.executemany(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (?, ?, ?, 'box@migration.example', ?)
            """,
            [
                ("delivery-failed", "migration-failed", mailbox_id, _STAMP),
                ("delivery-parsed", "migration-parsed", mailbox_id, _STAMP),
            ],
        )
        connection.execute("DROP TABLE mail_metric_buckets")
        connection.execute(
            """
            CREATE TABLE mail_metric_buckets (
                bucket_ts TEXT PRIMARY KEY,
                deliveries INTEGER NOT NULL DEFAULT 0,
                parse_failures INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (bucket_ts, deliveries, parse_failures)
            VALUES (?, 9, 7)
            """,
            (_STAMP,),
        )

    initialize_database(database_path)
    with connect_database(database_path) as connection:
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(mail_metric_buckets)").fetchall()
        }
        row = connection.execute(
            "SELECT * FROM mail_metric_buckets WHERE bucket_ts = ?",
            (_STAMP,),
        ).fetchone()
        assert columns == {
            "bucket_ts",
            "received",
            "deliveries",
            "parse_failures",
            "rejected",
        }
        assert dict(row) == {
            "bucket_ts": _STAMP,
            "deliveries": 9,
            "parse_failures": 7,
            "received": 2,
            "rejected": 4,
        }

        _insert_message(connection, "post-migration-message", parse_status="parsed")
        connection.execute(
            """
            INSERT INTO smtp_sessions (
                id, remote_ip, connect_at, rcpt_rejected_count
            ) VALUES ('smtp-migration-2', '127.0.0.2', ?, 20)
            """,
            (_STAMP,),
        )

    initialize_database(database_path)
    with connect_database(database_path) as connection:
        row = connection.execute(
            "SELECT * FROM mail_metric_buckets WHERE bucket_ts = ?",
            (_STAMP,),
        ).fetchone()
        assert dict(row) == {
            "bucket_ts": _STAMP,
            "deliveries": 9,
            "parse_failures": 7,
            "received": 2,
            "rejected": 4,
        }


@pytest.mark.asyncio
async def test_clear_all_mail_resets_cached_mail_totals_and_metric_buckets(
    runtime,
    sample_email_bytes: bytes,
) -> None:
    await runtime.create_domain("clear-counter.example")
    accepted = await runtime.accept_message(
        rcpt_tos=["box@clear-counter.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    assert accepted.startswith("250 queued as msg_")
    await runtime.drain_parser_queue()

    result = await runtime.clear_all_mail()

    assert result["messages"] == 1
    with connect_database(runtime.settings.database_path) as connection:
        counters = _assert_counters_match(connection)
        metric_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM mail_metric_buckets"
            ).fetchone()["count"]
        )
    assert counters["messages"] == 0
    assert counters["mailboxes"] == 0
    assert counters["pending_messages"] == 0
    assert counters["failed_messages"] == 0
    assert metric_count == 0


@pytest.mark.asyncio
async def test_retention_deletions_keep_dashboard_counters_consistent(
    runtime,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    monkeypatch.setattr(runtime_module, "utc_now", lambda: "2030-01-01T00:00:00Z")
    domain = await runtime.create_domain("retention-counter.example")
    await runtime.domains.update_domain(domain["id"], {"retention_days": 1})
    accepted = await runtime.accept_message(
        rcpt_tos=["box@retention-counter.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    assert accepted.startswith("250 queued as msg_")
    await runtime.drain_parser_queue()

    monkeypatch.setattr(runtime_module, "utc_now", lambda: "2030-01-02T00:00:00Z")
    result = await runtime.cleanup_expired_messages()

    assert result["messages"] == 1
    with connect_database(runtime.settings.database_path) as connection:
        counters = _assert_counters_match(connection)
    assert counters["messages"] == 0
    assert counters["pending_messages"] == 0
    assert counters["failed_messages"] == 0


def test_dashboard_database_snapshot_avoids_unbounded_count_queries(
    tmp_path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    statements: list[str] = []
    original_connect_database = dashboard_module.connect_database

    @contextmanager
    def traced_connect_database(path):
        with original_connect_database(path) as connection:
            connection.set_trace_callback(statements.append)
            yield connection

    monkeypatch.setattr(
        dashboard_module,
        "connect_database",
        traced_connect_database,
    )

    snapshot = DashboardService(
        SimpleNamespace(settings=SimpleNamespace(database_path=database_path))
    )._database_snapshot()

    assert snapshot["ok"] is True
    sql = "\n".join(statements).lower()
    assert "from dashboard_counters as counters" in sql
    assert "from mail_metric_buckets" in sql
    for table_name in ("messages", "api_keys", "audit_logs"):
        assert re.search(
            rf"count\s*\(\s*\*\s*\)\s+from\s+(?:main\.)?{table_name}\b",
            sql,
        ) is None
