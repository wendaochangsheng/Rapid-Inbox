from __future__ import annotations

import asyncio
import json
from email.message import EmailMessage

import pytest

import app.runtime as runtime_module
import app.services.messages as messages_module
from app.auth.api_keys import ApiKeyAuthorizationError
from app.config import Settings
from app.ingest.queue import ParseTask
from app.runtime import RapidInboxRuntime
from conftest import connect_database


def _attachment_email_bytes() -> bytes:
    message = EmailMessage()
    message["From"] = "Sender <sender@example.com>"
    message["To"] = "Foo <foo@adb.com>"
    message["Subject"] = "Expiring message"
    message["Message-ID"] = "<expiring@example.com>"
    message.set_content("This message should expire.")
    message.add_attachment(
        b"attachment-body",
        maintype="text",
        subtype="plain",
        filename="report.txt",
    )
    return message.as_bytes()


class _RecordingConnection:
    def __init__(self, connection) -> None:
        self.connection = connection
        self.execute_calls = 0
        self.executemany_calls = 0
        self.max_bind_count = 0

    def execute(self, statement, parameters=()):
        self.execute_calls += 1
        self.max_bind_count = max(self.max_bind_count, len(parameters))
        return self.connection.execute(statement, parameters)

    def executemany(self, statement, parameter_rows):
        rows = list(parameter_rows)
        self.executemany_calls += 1
        if rows:
            self.max_bind_count = max(
                self.max_bind_count,
                max(len(parameters) for parameters in rows),
            )
        return self.connection.executemany(statement, rows)


@pytest.mark.asyncio
async def test_message_retention_deletes_expired_mail_rows_and_files(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        domain = await runtime.create_domain("adb.com")
        await runtime.domains.update_domain(domain["id"], {"retention_days": 1})
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()

        with connect_database(settings.database_path) as connection:
            message_row = connection.execute(
                """
                SELECT id, raw_path, received_at, text_body_path, html_body_path
                FROM messages
                """
            ).fetchone()
            attachment_row = connection.execute("SELECT storage_path FROM attachments").fetchone()
            mailbox_row = connection.execute("SELECT id, message_count FROM mailboxes").fetchone()

        storage_paths = [
            message_row["raw_path"],
            message_row["text_body_path"],
            message_row["html_body_path"],
            attachment_row["storage_path"],
            runtime.storage.manifest_path(message_row["id"], message_row["received_at"]),
        ]
        existing_paths = [runtime.storage.resolve(path) for path in storage_paths if path]
        assert all(path.is_file() for path in existing_paths)
        assert mailbox_row["message_count"] == 1

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T19:59:59Z")
        not_yet_expired = await runtime.cleanup_expired_messages()
        assert not_yet_expired["messages"] == 0

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T20:00:00Z")
        expired = await runtime.cleanup_expired_messages()

        assert expired["messages"] == 1
        assert expired["deliveries"] == 1
        assert expired["attachments"] == 1
        assert expired["mailboxes"] == 0
        assert expired["files"] == len(existing_paths)
        assert not any(path.exists() for path in existing_paths)

        with connect_database(settings.database_path) as connection:
            counts = {
                "messages": connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"],
                "deliveries": connection.execute("SELECT COUNT(*) AS count FROM message_deliveries").fetchone()["count"],
                "attachments": connection.execute("SELECT COUNT(*) AS count FROM attachments").fetchone()["count"],
                "metrics": connection.execute(
                    "SELECT COALESCE(SUM(deliveries), 0) AS count FROM mail_metric_buckets"
                ).fetchone()["count"],
            }
            mailbox = connection.execute(
                "SELECT message_count, latest_message_at FROM mailboxes WHERE id = ?",
                (mailbox_row["id"],),
            ).fetchone()

        assert counts == {"messages": 0, "deliveries": 0, "attachments": 0, "metrics": 1}
        assert dict(mailbox) == {"message_count": 0, "latest_message_at": None}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_retention_deletes_empty_mailboxes_after_ten_minutes(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        empty_mailbox_retention_seconds=600,
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        with connect_database(settings.database_path) as connection:
            domain_id = connection.execute(
                "SELECT id FROM domains WHERE root_domain_ascii = 'adb.com'"
            ).fetchone()["id"]
            connection.executemany(
                """
                INSERT INTO mailboxes (
                    domain_id,
                    local_part_canonical,
                    rcpt_domain_ascii,
                    address_canonical,
                    address_display,
                    first_seen_at,
                    last_seen_at,
                    latest_message_at,
                    message_count
                ) VALUES (?, ?, 'adb.com', ?, ?, ?, ?, NULL, 0)
                """,
                [
                    (
                        domain_id,
                        "old-empty",
                        "old-empty@adb.com",
                        "old-empty@adb.com",
                        "2026-04-18T20:00:00Z",
                        "2026-04-18T20:00:00Z",
                    ),
                    (
                        domain_id,
                        "fresh-empty",
                        "fresh-empty@adb.com",
                        "fresh-empty@adb.com",
                        "2026-04-18T20:00:01Z",
                        "2026-04-18T20:00:01Z",
                    ),
                ],
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:10:00Z")
        expired = await runtime.cleanup_expired_messages()

        assert expired["mailboxes"] == 1

        with connect_database(settings.database_path) as connection:
            remaining = {
                str(row["address_canonical"])
                for row in connection.execute("SELECT address_canonical FROM mailboxes").fetchall()
            }

        assert remaining == {"fresh-empty@adb.com"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_retention_deletes_stale_smtp_sessions_without_dropping_active_connection(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        smtp_session_retention_seconds=600,
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        allowed, reason = await runtime.register_smtp_connection("smtp_active", "127.0.0.1")
        assert allowed is True
        assert reason is None

        with connect_database(settings.database_path) as connection:
            for session_id, status, ts in (
                ("smtp_closed_old", "closed", "2026-04-18T20:00:00Z"),
                ("smtp_error_old", "error", "2026-04-18T20:00:00Z"),
                ("smtp_inactive_open_old", "open", "2026-04-18T20:00:00Z"),
                ("smtp_active", "open", "2026-04-18T20:00:00Z"),
                ("smtp_closed_fresh", "closed", "2026-04-18T20:00:01Z"),
            ):
                connection.execute(
                    """
                    INSERT INTO smtp_sessions (
                        id,
                        remote_ip,
                        status,
                        connect_at,
                        disconnect_at,
                        last_command_at
                    ) VALUES (?, '127.0.0.1', ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        status,
                        ts,
                        ts if status != "open" else None,
                        ts,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO smtp_events (session_id, seq, event_type, ts, payload_json)
                    VALUES (?, 1, 'connect', ?, '{}')
                    """,
                    (session_id, ts),
                )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:10:00Z")
        expired = await runtime.cleanup_expired_messages()

        assert expired["smtp_sessions"] == 3

        with connect_database(settings.database_path) as connection:
            remaining_sessions = {
                str(row["id"])
                for row in connection.execute("SELECT id FROM smtp_sessions ORDER BY id").fetchall()
            }
            remaining_events = {
                str(row["session_id"])
                for row in connection.execute("SELECT session_id FROM smtp_events ORDER BY session_id").fetchall()
            }

        assert remaining_sessions == {"smtp_active", "smtp_closed_fresh"}
        assert remaining_events == {"smtp_active", "smtp_closed_fresh"}
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_start_marks_previous_open_smtp_sessions_as_orphaned(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    await runtime.stop()

    with connect_database(settings.database_path) as connection:
        connection.execute(
            """
            INSERT INTO smtp_sessions (
                id,
                remote_ip,
                status,
                connect_at,
                last_command_at
            ) VALUES ('smtp_orphaned', '127.0.0.1', 'open', ?, ?)
            """,
            ("2026-04-18T20:00:00Z", "2026-04-18T20:00:01Z"),
        )

    restarted_runtime = RapidInboxRuntime(settings)
    await restarted_runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT status, disconnect_at, close_reason
                FROM smtp_sessions
                WHERE id = 'smtp_orphaned'
                """
            ).fetchone()

        assert row["status"] == "error"
        assert row["disconnect_at"] is not None
        assert row["close_reason"] == "runtime restarted before disconnect"
    finally:
        await restarted_runtime.stop()


@pytest.mark.asyncio
async def test_metric_bucket_retention_runs_without_expired_mail_or_sessions(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        metric_retention_seconds=48 * 60 * 60,
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO mail_metric_buckets (bucket_ts, deliveries, parse_failures)
                VALUES (?, ?, ?)
                """,
                [
                    ("2026-04-18T19:59:59Z", 1, 0),
                    ("2026-04-18T20:00:00Z", 2, 0),
                    ("2026-04-20T19:59:00Z", 3, 1),
                ],
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-20T20:00:00Z")
        expired = await runtime.cleanup_expired_messages()

        assert expired["messages"] == 0
        assert expired["smtp_sessions"] == 0
        assert expired["metric_buckets"] == 1

        with connect_database(settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT bucket_ts, deliveries, parse_failures
                FROM mail_metric_buckets
                ORDER BY bucket_ts
                """
            ).fetchall()

        assert [dict(row) for row in rows] == [
            {"bucket_ts": "2026-04-18T20:00:00Z", "deliveries": 2, "parse_failures": 0},
            {"bucket_ts": "2026-04-20T19:59:00Z", "deliveries": 3, "parse_failures": 1},
        ]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_deleted_expired_manifest_is_not_recovered_on_restart(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        domain = await runtime.create_domain("adb.com")
        await runtime.domains.update_domain(domain["id"], {"retention_days": 1})
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T20:00:00Z")
        await runtime.cleanup_expired_messages()
        assert not list(settings.manifests_dir.rglob("*.json"))
    finally:
        await runtime.stop()

    restarted = RapidInboxRuntime(settings)
    monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:05:00Z")
    await restarted.start()
    try:
        with connect_database(settings.database_path) as connection:
            message_count = connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"]
    finally:
        await restarted.stop()

    assert message_count == 0


@pytest.mark.asyncio
async def test_maintenance_runs_record_running_success_and_failure(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        operation_started = asyncio.Event()
        release_operation = asyncio.Event()

        async def controlled_cleanup() -> dict[str, int]:
            operation_started.set()
            await release_operation.wait()
            return {**runtime._empty_retention_result(), "audit_logs": 3}

        monkeypatch.setattr(runtime, "_cleanup_expired_messages_operation", controlled_cleanup)
        cleanup_task = asyncio.create_task(runtime.cleanup_expired_messages())
        await operation_started.wait()

        with connect_database(settings.database_path) as connection:
            running = connection.execute(
                """
                SELECT id, kind, status, started_at, finished_at, details_json, error
                FROM maintenance_runs
                ORDER BY rowid DESC
                LIMIT 1
                """
            ).fetchone()

        assert running["kind"] == "retention_cleanup"
        assert running["status"] == "running"
        assert running["started_at"] is not None
        assert running["finished_at"] is None
        assert running["details_json"] is None
        assert running["error"] is None

        release_operation.set()
        result = await cleanup_task
        assert result["audit_logs"] == 3

        with connect_database(settings.database_path) as connection:
            succeeded = connection.execute(
                """
                SELECT status, finished_at, details_json, error
                FROM maintenance_runs
                WHERE id = ?
                """,
                (running["id"],),
            ).fetchone()

        assert succeeded["status"] == "succeeded"
        assert succeeded["finished_at"] is not None
        assert json.loads(succeeded["details_json"])["audit_logs"] == 3
        assert succeeded["error"] is None

        async def failed_cleanup() -> dict[str, int]:
            raise RuntimeError("forced cleanup failure")

        monkeypatch.setattr(runtime, "_cleanup_expired_messages_operation", failed_cleanup)
        with pytest.raises(RuntimeError, match="forced cleanup failure"):
            await runtime.cleanup_expired_messages()

        with connect_database(settings.database_path) as connection:
            failed = connection.execute(
                """
                SELECT status, finished_at, details_json, error
                FROM maintenance_runs
                ORDER BY rowid DESC
                LIMIT 1
                """
            ).fetchone()

        assert failed["status"] == "failed"
        assert failed["finished_at"] is not None
        assert failed["details_json"] is None
        assert failed["error"] == "RuntimeError: forced cleanup failure"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_manual_cleanup_reauthorizes_again_in_final_delete_transaction(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("cleanup-final-auth.example")
        queued = await runtime.accept_message(
            rcpt_tos=["box@cleanup-final-auth.example"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()
        message_id = queued.removeprefix("250 queued as ")
        with connect_database(settings.database_path, durable_writes=True) as connection:
            connection.execute(
                """
                UPDATE message_deliveries
                SET expires_at = '2000-01-01T00:00:00Z'
                WHERE message_id = ?
                """,
                (message_id,),
            )

        key = await runtime.api_keys.create_key(
            name="cleanup-final-transaction",
            kind="admin",
            scopes=["system.write"],
            domain_ids=[],
            domain_grant_mode="all",
            mailbox_patterns=[],
        )
        principal = runtime.api_keys.authenticate_plain_text(key["plain_text"])
        real_snapshot = runtime._expiration_batch_snapshot

        def revoke_after_admission(now: str, limit: int):
            with connect_database(settings.database_path, durable_writes=True) as connection:
                connection.execute(
                    "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                    (key["id"],),
                )
            return real_snapshot(now, limit)

        monkeypatch.setattr(runtime, "_expiration_batch_snapshot", revoke_after_admission)
        with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
            await runtime.cleanup_expired_messages(
                authorization_principal=principal,
            )

        with connect_database(settings.database_path) as connection:
            message = connection.execute(
                "SELECT id FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            delivery = connection.execute(
                """
                SELECT status, expires_at
                FROM message_deliveries
                WHERE message_id = ?
                """,
                (message_id,),
            ).fetchone()
            run = connection.execute(
                """
                SELECT status, error
                FROM maintenance_runs
                WHERE kind = 'retention_cleanup'
                ORDER BY rowid DESC
                LIMIT 1
                """
            ).fetchone()

        assert message is not None
        assert dict(delivery) == {
            "status": "active",
            "expires_at": "2000-01-01T00:00:00Z",
        }
        assert run["status"] == "failed"
        assert "no longer active" in str(run["error"])
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_file_gc_failures_use_bounded_exponential_backoff(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        gc_directory = settings.raw_dir / "gc-target-directory"
        gc_directory.mkdir(parents=True)
        storage_path = gc_directory.relative_to(settings.storage_root).as_posix()
        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                INSERT INTO file_gc_tasks (
                    storage_path, reason, attempts, created_at, updated_at
                ) VALUES (?, 'test', 0, ?, ?)
                """,
                (storage_path, "2026-04-18T20:00:00Z", "2026-04-18T20:00:00Z"),
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        first = await runtime._drain_file_gc(10)
        assert first == {"file_gc_deleted": 0, "file_gc_failed": 1, "file_gc_pending": 1}

        with connect_database(settings.database_path) as connection:
            after_first = connection.execute(
                "SELECT attempts, last_error, next_attempt_at FROM file_gc_tasks"
            ).fetchone()
        assert after_first["attempts"] == 1
        assert after_first["last_error"].startswith("OSError: GC target is not a regular file")
        assert after_first["next_attempt_at"] == "2026-04-18T20:00:30Z"

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:29Z")
        skipped = await runtime._drain_file_gc(10)
        assert skipped == {"file_gc_deleted": 0, "file_gc_failed": 0, "file_gc_pending": 1}

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:30Z")
        second = await runtime._drain_file_gc(10)
        assert second == {"file_gc_deleted": 0, "file_gc_failed": 1, "file_gc_pending": 1}
        with connect_database(settings.database_path) as connection:
            after_second = connection.execute(
                "SELECT attempts, next_attempt_at FROM file_gc_tasks"
            ).fetchone()
        assert dict(after_second) == {
            "attempts": 2,
            "next_attempt_at": "2026-04-18T20:01:30Z",
        }
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_file_gc_due_loader_fairly_interleaves_new_and_retry_tasks(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO file_gc_tasks (
                    storage_path, reason, attempts, next_attempt_at,
                    created_at, updated_at
                ) VALUES (?, 'test', ?, ?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """,
                [
                    ("raw/pending-1.eml", 0, None),
                    ("raw/pending-2.eml", 0, None),
                    ("raw/pending-3.eml", 0, None),
                    ("raw/retry-1.eml", 1, "2026-01-01T00:00:00Z"),
                    ("raw/retry-2.eml", 2, "2026-01-02T00:00:00Z"),
                    ("raw/retry-3.eml", 3, "2026-01-03T00:00:00Z"),
                    ("raw/future.eml", 1, "2030-01-01T00:00:00Z"),
                ],
            )

        rows = runtime._load_due_file_gc_tasks("2026-01-10T00:00:00Z", 4)

        assert [row["storage_path"] for row in rows] == [
            "raw/pending-1.eml",
            "raw/retry-1.eml",
            "raw/pending-2.eml",
            "raw/retry-2.eml",
        ]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_shared_message_honors_each_domain_retention_before_file_gc(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        short_domain = await runtime.create_domain("short-retention.example")
        long_domain = await runtime.create_domain("long-retention.example")
        await runtime.domains.update_domain(short_domain["id"], {"retention_days": 1})
        await runtime.domains.update_domain(long_domain["id"], {"retention_days": 2})

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        response = await runtime.accept_message(
            rcpt_tos=[
                "short@short-retention.example",
                "long@long-retention.example",
            ],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")

        with connect_database(settings.database_path) as connection:
            message = connection.execute(
                "SELECT raw_path, received_at FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            deliveries = connection.execute(
                """
                SELECT mb.address_canonical, d.expires_at
                FROM message_deliveries AS d
                JOIN mailboxes AS mb ON mb.id = d.mailbox_id
                WHERE d.message_id = ?
                ORDER BY mb.address_canonical
                """,
                (message_id,),
            ).fetchall()

        raw_path = runtime.storage.resolve(message["raw_path"])
        manifest_path = runtime.storage.resolve(
            runtime.storage.manifest_path(message_id, message["received_at"])
        )
        assert raw_path.is_file()
        assert manifest_path.is_file()
        assert {row["expires_at"] for row in deliveries} == {
            "2026-04-19T20:00:00Z",
            "2026-04-20T20:00:00Z",
        }

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T20:00:00Z")
        first_cleanup = await runtime.cleanup_expired_messages()
        assert first_cleanup["deliveries"] == 1
        assert first_cleanup["messages"] == 0
        assert first_cleanup["files"] == 0
        assert raw_path.is_file()
        assert manifest_path.is_file()

        with connect_database(settings.database_path) as connection:
            remaining = connection.execute(
                """
                SELECT mb.address_canonical
                FROM message_deliveries AS d
                JOIN mailboxes AS mb ON mb.id = d.mailbox_id
                WHERE d.message_id = ?
                """,
                (message_id,),
            ).fetchall()
            mailbox_counts = {
                str(row["address_canonical"]): int(row["message_count"])
                for row in connection.execute(
                    "SELECT address_canonical, message_count FROM mailboxes"
                ).fetchall()
            }
        assert [row["address_canonical"] for row in remaining] == ["long@long-retention.example"]
        assert mailbox_counts == {
            "long@long-retention.example": 1,
            "short@short-retention.example": 0,
        }

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-20T20:00:00Z")
        second_cleanup = await runtime.cleanup_expired_messages()
        assert second_cleanup["deliveries"] == 1
        assert second_cleanup["messages"] == 1
        assert second_cleanup["attachments"] == 1
        assert second_cleanup["files"] > 0
        assert not raw_path.exists()
        assert not manifest_path.exists()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_audit_cleanup_uses_configured_retention_and_strict_cutoff(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        audit_retention_days=1,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO audit_logs (
                    actor_type, actor_ref, action, resource_type, resource_ref,
                    status, created_at
                ) VALUES ('system', 'retention-test', ?, 'test', NULL, 'success', ?)
                """,
                [
                    ("old", "2026-04-19T19:59:59Z"),
                    ("boundary", "2026-04-19T20:00:00Z"),
                    ("fresh", "2026-04-20T19:59:59Z"),
                ],
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-20T20:00:00Z")
        result = await runtime.cleanup_expired_messages()
        assert result["audit_logs"] == 1

        with connect_database(settings.database_path) as connection:
            actions = [
                str(row["action"])
                for row in connection.execute("SELECT action FROM audit_logs ORDER BY id").fetchall()
            ]
        assert actions == ["boundary", "fresh"]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_soft_deleted_delivery_is_scheduled_for_physical_cleanup(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-20T20:00:00Z")
        monkeypatch.setattr(messages_module, "utc_now", lambda: "2026-04-20T20:00:00Z")
        await runtime.create_domain("adb.com")
        response = await runtime.accept_message(
            rcpt_tos=["delete-me@adb.com"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")

        with connect_database(settings.database_path) as connection:
            delivery = connection.execute(
                "SELECT id FROM message_deliveries WHERE message_id = ?",
                (message_id,),
            ).fetchone()
            raw_path = connection.execute(
                "SELECT raw_path FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()["raw_path"]

        await runtime.messages.soft_delete_delivery(str(delivery["id"]))
        with connect_database(settings.database_path) as connection:
            deleted = connection.execute(
                "SELECT status, deleted_at, expires_at FROM message_deliveries WHERE id = ?",
                (delivery["id"],),
            ).fetchone()
        assert dict(deleted) == {
            "status": "deleted",
            "deleted_at": "2026-04-20T20:00:00Z",
            "expires_at": "2026-04-20T20:00:00Z",
        }

        result = await runtime.cleanup_expired_messages()
        assert result["deliveries"] == 1
        assert result["messages"] == 1
        assert not runtime.storage.resolve(str(raw_path)).exists()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_pending_file_gc_tombstone_prevents_manifest_resurrection_after_restart(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        domain = await runtime.create_domain("retired.example")
        await runtime.domains.update_domain(domain["id"], {"retention_days": 1})
        response = await runtime.accept_message(
            rcpt_tos=["box@retired.example"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")

        with connect_database(settings.database_path) as connection:
            message = connection.execute(
                "SELECT raw_path, received_at FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        raw_path = runtime.storage.resolve(str(message["raw_path"]))
        manifest_path = runtime.storage.resolve(
            runtime.storage.manifest_path(message_id, str(message["received_at"]))
        )

        async def leave_tombstones_pending(_limit: int) -> dict[str, int]:
            with connect_database(settings.database_path) as connection:
                pending = int(
                    connection.execute("SELECT COUNT(*) AS count FROM file_gc_tasks").fetchone()["count"]
                )
            return {"file_gc_deleted": 0, "file_gc_failed": 0, "file_gc_pending": pending}

        monkeypatch.setattr(runtime, "_drain_file_gc", leave_tombstones_pending)
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T20:00:00Z")
        result = await runtime.cleanup_expired_messages()
        assert result["messages"] == 1
        assert result["file_gc_pending"] >= 2
        assert raw_path.is_file()
        assert manifest_path.is_file()
    finally:
        await runtime.stop()

    restarted = RapidInboxRuntime(settings)
    await restarted.start()
    try:
        with connect_database(settings.database_path) as connection:
            message_count = int(connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"])
            delivery_count = int(
                connection.execute("SELECT COUNT(*) AS count FROM message_deliveries").fetchone()["count"]
            )
        assert message_count == 0
        assert delivery_count == 0
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_periodic_recovery_waits_for_retention_file_gc_linearization_lock(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        gc_started = asyncio.Event()
        release_gc = asyncio.Event()
        recovery_started = asyncio.Event()

        async def controlled_gc(_limit: int) -> dict[str, int]:
            gc_started.set()
            await release_gc.wait()
            return {"file_gc_deleted": 0, "file_gc_failed": 0, "file_gc_pending": 0}

        async def controlled_recovery(*, incremental: bool) -> set[str]:
            assert incremental is True
            recovery_started.set()
            return set()

        monkeypatch.setattr(runtime, "_drain_file_gc", controlled_gc)
        monkeypatch.setattr(runtime.recovery, "_recover_manifests", controlled_recovery)

        cleanup_task = asyncio.create_task(runtime.cleanup_expired_messages())
        await asyncio.wait_for(gc_started.wait(), timeout=1)
        recovery_task = asyncio.create_task(runtime.recovery.recover_missing_manifests(incremental=True))
        await asyncio.sleep(0)
        assert not recovery_started.is_set()

        release_gc.set()
        await asyncio.wait_for(cleanup_task, timeout=1)
        await asyncio.wait_for(recovery_task, timeout=1)
        assert recovery_started.is_set()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_retention_waits_for_parser_from_exact_expiry_order_batch(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        cleanup_batch_size=1,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    release_parser = asyncio.Event()
    cleanup_task = None
    try:
        domain = await runtime.create_domain("batch-order.example")
        now = "2026-04-20T20:00:00Z"
        monkeypatch.setattr(runtime_module, "utc_now", lambda: now)
        with connect_database(settings.database_path) as connection:
            mailbox_id = connection.execute(
                """
                INSERT INTO mailboxes (
                    domain_id,
                    local_part_canonical,
                    rcpt_domain_ascii,
                    address_canonical,
                    address_display,
                    first_seen_at,
                    last_seen_at,
                    latest_message_at,
                    message_count
                ) VALUES (?, 'batch', 'batch-order.example', ?, ?, ?, ?, ?, 2)
                """,
                (
                    domain["id"],
                    "batch@batch-order.example",
                    "batch@batch-order.example",
                    "2026-04-01T00:00:00Z",
                    "2026-04-02T00:00:00Z",
                    "2026-04-02T00:00:00Z",
                ),
            ).lastrowid
            connection.executemany(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
                ) VALUES (?, ?, ?, 1, ?, 'parsed')
                """,
                [
                    (
                        "msg_received_first",
                        "raw/msg_received_first.eml",
                        "a" * 64,
                        "2026-04-01T00:00:00Z",
                    ),
                    (
                        "msg_expires_first",
                        "raw/msg_expires_first.eml",
                        "b" * 64,
                        "2026-04-02T00:00:00Z",
                    ),
                ],
            )
            connection.executemany(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at, status, expires_at
                ) VALUES (?, ?, ?, 'batch@batch-order.example', ?, 'active', ?)
                """,
                [
                    (
                        "delivery_received_first",
                        "msg_received_first",
                        mailbox_id,
                        "2026-04-01T00:00:00Z",
                        "2026-04-20T19:00:00Z",
                    ),
                    (
                        "delivery_expires_first",
                        "msg_expires_first",
                        mailbox_id,
                        "2026-04-02T00:00:00Z",
                        "2026-04-20T18:00:00Z",
                    ),
                ],
            )

        parser_started = asyncio.Event()

        async def held_parser(task: ParseTask) -> None:
            assert task.message_id == "msg_expires_first"
            parser_started.set()
            await release_parser.wait()

        runtime.parse_queue._worker = held_parser
        assert runtime.parse_queue.try_enqueue(ParseTask("msg_expires_first", 1)) is True
        await asyncio.wait_for(parser_started.wait(), timeout=1)

        wait_started = asyncio.Event()
        original_wait = runtime.parse_queue.wait_until_not_active

        async def tracked_wait(predicate) -> None:
            assert predicate("msg_expires_first") is True
            assert predicate("msg_received_first") is False
            wait_started.set()
            await original_wait(predicate)

        monkeypatch.setattr(runtime.parse_queue, "wait_until_not_active", tracked_wait)
        cleanup_task = asyncio.create_task(runtime.cleanup_expired_messages())
        await asyncio.wait_for(wait_started.wait(), timeout=1)
        await asyncio.sleep(0.05)
        assert cleanup_task.done() is False

        with connect_database(settings.database_path) as connection:
            assert connection.execute(
                "SELECT 1 FROM messages WHERE id = 'msg_expires_first'"
            ).fetchone() is not None

        release_parser.set()
        result = await asyncio.wait_for(cleanup_task, timeout=2)
        assert result["deliveries"] == 1
        assert result["messages"] == 1

        with connect_database(settings.database_path) as connection:
            remaining = [
                str(row["id"])
                for row in connection.execute("SELECT id FROM messages ORDER BY id").fetchall()
            ]
        assert remaining == ["msg_received_first"]

        def temp_counts(connection):
            return (
                int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM _retention_delivery_batch"
                    ).fetchone()["count"]
                ),
                int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM _retention_orphan_message_batch"
                    ).fetchone()["count"]
                ),
                int(
                    connection.execute(
                        "SELECT COUNT(*) AS count FROM _retention_active_smtp_session"
                    ).fetchone()["count"]
                ),
            )

        assert await runtime.writer.execute(temp_counts) == (0, 0, 0)
    finally:
        release_parser.set()
        if cleanup_task is not None and not cleanup_task.done():
            await cleanup_task
        await runtime.stop()


@pytest.mark.asyncio
async def test_large_retention_batch_has_bounded_sql_binds_and_revalidates_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    row_count = 1_101
    now = "2026-04-20T20:00:00Z"
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        cleanup_batch_size=1_200,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: now)
        domain = await runtime.create_domain("large-batch.example")
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO mailboxes (
                    id,
                    domain_id,
                    local_part_canonical,
                    rcpt_domain_ascii,
                    address_canonical,
                    address_display,
                    first_seen_at,
                    last_seen_at,
                    latest_message_at,
                    message_count
                ) VALUES (?, ?, ?, 'large-batch.example', ?, ?, ?, ?, ?, 1)
                """,
                [
                    (
                        10_000 + index,
                        domain["id"],
                        f"box-{index:04d}",
                        f"box-{index:04d}@large-batch.example",
                        f"box-{index:04d}@large-batch.example",
                        "2026-04-19T00:00:00Z",
                        "2026-04-20T19:59:59Z",
                        "2026-04-20T19:59:59Z",
                    )
                    for index in range(row_count)
                ],
            )
            connection.executemany(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
                ) VALUES (?, ?, ?, 1, '2026-04-19T00:00:00Z', 'parsed')
                """,
                [
                    (
                        f"msg_bulk_{index:04d}",
                        f"raw/bulk/msg_bulk_{index:04d}.eml",
                        f"{index:064x}",
                    )
                    for index in range(row_count)
                ],
            )
            connection.executemany(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at, status, expires_at
                ) VALUES (?, ?, ?, ?, '2026-04-19T00:00:00Z', 'active', ?)
                """,
                [
                    (
                        f"delivery_bulk_{index:04d}",
                        f"msg_bulk_{index:04d}",
                        10_000 + index,
                        f"box-{index:04d}@large-batch.example",
                        "2026-04-20T19:59:59Z",
                    )
                    for index in range(row_count)
                ],
            )

        delivery_batch, candidate_message_ids = runtime._expiration_batch_snapshot(
            now,
            settings.cleanup_batch_size,
        )
        assert len(delivery_batch) == row_count
        assert len(candidate_message_ids) == row_count

        with runtime._smtp_connection_lock:
            runtime._active_smtp_connections.update(
                {
                    f"smtp_active_{index:04d}": "127.0.0.1"
                    for index in range(row_count)
                }
            )

        with connect_database(settings.database_path) as connection:
            # Both mutations occur after the read snapshot.  The writer must
            # retain each row: one no longer has the same status and the other
            # no longer expires in this batch.
            connection.execute(
                """
                UPDATE message_deliveries
                SET status = 'hidden'
                WHERE id = 'delivery_bulk_0000'
                """
            )
            connection.execute(
                """
                UPDATE message_deliveries
                SET expires_at = '2026-04-21T00:00:00Z'
                WHERE id = 'delivery_bulk_0001'
                """
            )
            # Production invokes the batch through the dedicated writer,
            # which starts from autocommit. Commit the synthetic cross-process
            # mutations before calling the transaction-owning batch directly.
            connection.commit()

            recorded = _RecordingConnection(connection)
            result = runtime._expire_delivery_batch(
                recorded,
                now,
                settings.cleanup_batch_size,
                delivery_batch,
            )
            first_call_execute_count = recorded.execute_calls

            assert result["deliveries"] == row_count - 2
            assert result["messages"] == row_count - 2
            assert recorded.max_bind_count <= 4
            assert first_call_execute_count < 40
            assert recorded.executemany_calls >= 3

            remaining_delivery_ids = {
                str(row["id"])
                for row in connection.execute(
                    "SELECT id FROM message_deliveries ORDER BY id"
                ).fetchall()
            }
            assert remaining_delivery_ids == {
                "delivery_bulk_0000",
                "delivery_bulk_0001",
            }
            assert int(
                connection.execute(
                    "SELECT COALESCE(SUM(message_count), 0) AS count FROM mailboxes"
                ).fetchone()["count"]
            ) == 2
            assert int(
                connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"]
            ) == 2

            for table_name in (
                "_retention_delivery_batch",
                "_retention_orphan_message_batch",
                "_retention_active_smtp_session",
            ):
                assert int(
                    connection.execute(
                        f"SELECT COUNT(*) AS count FROM {table_name}"
                    ).fetchone()["count"]
                ) == 0

            connection.commit()
            second = runtime._expire_delivery_batch(
                recorded,
                now,
                settings.cleanup_batch_size,
                [],
            )
            assert second["deliveries"] == 0
            assert second["messages"] == 0
            assert recorded.execute_calls - first_call_execute_count < 40
            assert recorded.max_bind_count <= 4
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_pending_parse_page_and_enqueue_are_linearized_with_retention(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    release_page = asyncio.Event()
    scan_task = None
    cleanup_task = None
    try:
        await runtime._stop_pending_parse_scan_loop()
        await runtime.parse_queue.stop(discard_pending=True)
        runtime._last_manifest_recovery_at = float("inf")
        now = "2026-04-20T20:00:00Z"
        monkeypatch.setattr(runtime_module, "utc_now", lambda: now)
        domain = await runtime.create_domain("scan-race.example")

        with connect_database(settings.database_path) as connection:
            mailbox_id = connection.execute(
                """
                INSERT INTO mailboxes (
                    domain_id,
                    local_part_canonical,
                    rcpt_domain_ascii,
                    address_canonical,
                    address_display,
                    first_seen_at,
                    last_seen_at,
                    latest_message_at,
                    message_count
                ) VALUES (?, 'race', 'scan-race.example', ?, ?, ?, ?, ?, 1)
                """,
                (
                    domain["id"],
                    "race@scan-race.example",
                    "race@scan-race.example",
                    "2026-04-19T00:00:00Z",
                    "2026-04-20T19:59:59Z",
                    "2026-04-19T00:00:00Z",
                ),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
                ) VALUES (
                    'msg_scan_race',
                    'raw/msg_scan_race.eml',
                    ?,
                    1,
                    '2026-04-19T00:00:00Z',
                    'pending'
                )
                """,
                ("c" * 64,),
            )
            connection.execute(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at, status, expires_at
                ) VALUES (
                    'delivery_scan_race',
                    'msg_scan_race',
                    ?,
                    'race@scan-race.example',
                    '2026-04-19T00:00:00Z',
                    'active',
                    '2026-04-20T19:59:59Z'
                )
                """,
                (mailbox_id,),
            )

        page_read_started = asyncio.Event()

        async def paused_stale_page() -> list[ParseTask]:
            page_read_started.set()
            await release_page.wait()
            return [ParseTask("msg_scan_race", 1)]

        monkeypatch.setattr(runtime, "_next_pending_parse_task_page", paused_stale_page)
        scan_task = asyncio.create_task(runtime.requeue_pending_messages_for_parse())
        await asyncio.wait_for(page_read_started.wait(), timeout=1)
        assert runtime._mail_store_lock.locked() is True

        cleanup_task = asyncio.create_task(runtime.cleanup_expired_messages())
        await asyncio.sleep(0.05)
        assert cleanup_task.done() is False

        release_page.set()
        assert await asyncio.wait_for(scan_task, timeout=1) == 1
        result = await asyncio.wait_for(cleanup_task, timeout=2)
        assert result["dropped_parse_tasks"] == 1
        assert result["deliveries"] == 1
        assert result["messages"] == 1
        assert runtime.parse_queue.contains("msg_scan_race") is False
    finally:
        release_page.set()
        if scan_task is not None and not scan_task.done():
            await scan_task
        if cleanup_task is not None and not cleanup_task.done():
            await cleanup_task
        await runtime.stop()


@pytest.mark.asyncio
async def test_reparse_waits_for_retention_linearization_and_fails_after_delete(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    release_gc = asyncio.Event()
    cleanup_task = None
    reparse_task = None
    try:
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
        domain = await runtime.create_domain("reparse-race.example")
        await runtime.domains.update_domain(domain["id"], {"retention_days": 1})
        response = await runtime.accept_message(
            rcpt_tos=["race@reparse-race.example"],
            envelope_from="sender@example.com",
            content=_attachment_email_bytes(),
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")

        gc_started = asyncio.Event()

        async def held_gc(_limit: int) -> dict[str, int]:
            gc_started.set()
            await release_gc.wait()
            return {
                "file_gc_deleted": 0,
                "file_gc_failed": 0,
                "file_gc_pending": 1,
            }

        monkeypatch.setattr(runtime, "_drain_file_gc", held_gc)
        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-19T20:00:00Z")
        cleanup_task = asyncio.create_task(runtime.cleanup_expired_messages())
        await asyncio.wait_for(gc_started.wait(), timeout=1)

        with connect_database(settings.database_path) as connection:
            assert connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone() is None

        reparse_task = asyncio.create_task(runtime.reparse_message(message_id))
        await asyncio.sleep(0.05)
        assert reparse_task.done() is False
        assert runtime.parse_queue.contains(message_id) is False

        release_gc.set()
        cleanup_result = await asyncio.wait_for(cleanup_task, timeout=2)
        assert cleanup_result["messages"] == 1
        with pytest.raises(LookupError, match="message not found"):
            await asyncio.wait_for(reparse_task, timeout=1)
        assert runtime.parse_queue.contains(message_id) is False
    finally:
        release_gc.set()
        if cleanup_task is not None and not cleanup_task.done():
            await cleanup_task
        if reparse_task is not None and not reparse_task.done():
            try:
                await reparse_task
            except LookupError:
                pass
        await runtime.stop()


@pytest.mark.asyncio
async def test_file_gc_does_not_follow_symlink_or_delete_its_target(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        outside_target = tmp_path / "outside-target.eml"
        outside_target.write_bytes(b"must survive file GC")
        symlink_path = settings.raw_dir / "gc-symlink.eml"
        symlink_path.symlink_to(outside_target)
        storage_path = symlink_path.relative_to(settings.storage_root).as_posix()
        timestamp = "2026-04-18T20:00:00Z"

        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                INSERT INTO file_gc_tasks (
                    storage_path, reason, attempts, created_at, updated_at
                ) VALUES (?, 'symlink-regression', 0, ?, ?)
                """,
                (storage_path, timestamp, timestamp),
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: timestamp)
        result = await runtime._drain_file_gc(10)

        assert result == {
            "file_gc_deleted": 0,
            "file_gc_failed": 1,
            "file_gc_pending": 1,
        }
        assert symlink_path.is_symlink()
        assert outside_target.read_bytes() == b"must survive file GC"

        with connect_database(settings.database_path) as connection:
            task = connection.execute(
                """
                SELECT attempts, last_error, next_attempt_at
                FROM file_gc_tasks
                WHERE storage_path = ?
                """,
                (storage_path,),
            ).fetchone()
        assert task["attempts"] == 1
        assert task["last_error"] == "OSError: GC target is not a regular file"
        assert task["next_attempt_at"] == "2026-04-18T20:00:30Z"
    finally:
        await runtime.stop()
