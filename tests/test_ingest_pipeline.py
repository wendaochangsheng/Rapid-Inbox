from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import stat
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.db.connection import connect_database, initialize_database
from app.ingest.storage import FileStorage
from app.runtime import RapidInboxRuntime


def test_cleanup_stale_parts_removes_legacy_visible_temp_files_without_touching_final_attachment_part_filename(
    tmp_path,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    storage = FileStorage(settings)

    legacy_raw_temp_path = storage.resolve("raw/2026/04/18/msg_1.eml.part")
    legacy_raw_temp_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_raw_temp_path.write_bytes(b"legacy raw temp")

    final_attachment_path = storage.resolve("attachments/msg_1/att_1-report.part")
    final_attachment_path.parent.mkdir(parents=True, exist_ok=True)
    final_attachment_path.write_bytes(b"final attachment")

    stale_hidden_part_path = storage.resolve("attachments/msg_1/.att_1-report.part.part")
    stale_hidden_part_path.write_bytes(b"stale temp data")
    stale_cpp_temp_path = storage.resolve("manifests/2026/04/18/.msg_1.json.tmp.A1B2C3")
    stale_cpp_temp_path.parent.mkdir(parents=True, exist_ok=True)
    stale_cpp_temp_path.write_bytes(b"stale C++ temp data")
    os.utime(legacy_raw_temp_path, (0, 0))
    os.utime(stale_hidden_part_path, (0, 0))
    os.utime(stale_cpp_temp_path, (0, 0))

    storage.cleanup_stale_parts()

    assert not legacy_raw_temp_path.exists()
    assert final_attachment_path.exists()
    assert not stale_hidden_part_path.exists()
    assert not stale_cpp_temp_path.exists()


def test_cleanup_stale_parts_preserves_recent_cross_process_temp_file(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    storage = FileStorage(settings)
    active_part_path = storage.resolve("raw/2026/04/18/.msg_active.eml.part")
    active_part_path.parent.mkdir(parents=True, exist_ok=True)
    active_part_path.write_bytes(b"still being written by ingestd")
    active_cpp_temp_path = storage.resolve("raw/2026/04/18/.msg_active.eml.tmp.A1B2C3")
    active_cpp_temp_path.write_bytes(b"still being written by C++ ingestd")

    storage.cleanup_stale_parts()

    assert active_part_path.read_bytes() == b"still being written by ingestd"
    assert active_cpp_temp_path.read_bytes() == b"still being written by C++ ingestd"


def test_write_raw_message_fsyncs_created_directory_chain(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        fsync_storage_writes=True,
    )
    storage = FileStorage(settings)

    fsynced_dirs: list[str] = []
    real_fsync = os.fsync

    def recording_fsync(fd: int) -> None:
        if stat.S_ISDIR(os.fstat(fd).st_mode):
            fsynced_dirs.append(os.readlink(f"/proc/self/fd/{fd}"))
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", recording_fsync)

    storage.write_raw_message("msg_1", "2026-04-18T20:00:00Z", b"raw body")

    expected_dirs = {
        str(settings.storage_root),
        str(settings.storage_root / "raw"),
        str(settings.storage_root / "raw" / "2026"),
        str(settings.storage_root / "raw" / "2026" / "04"),
        str(settings.storage_root / "raw" / "2026" / "04" / "18"),
    }
    assert expected_dirs.issubset(set(fsynced_dirs))


def test_storage_rejects_paths_outside_storage_root(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    storage = FileStorage(settings)

    with pytest.raises(ValueError):
        storage.resolve("../outside.eml")
    with pytest.raises(ValueError):
        storage.resolve(str(tmp_path / "outside.eml"))


def test_storage_writes_private_files_and_directories(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    storage = FileStorage(settings)

    raw_path, _, _ = storage.write_raw_message("msg_1", "2026-04-18T20:00:00Z", b"raw body")
    resolved = storage.resolve(raw_path)

    assert stat.S_IMODE(resolved.stat().st_mode) == 0o600
    assert stat.S_IMODE((settings.storage_root / "raw").stat().st_mode) == 0o700
    assert stat.S_IMODE((settings.storage_root / "raw" / "2026").stat().st_mode) == 0o700
    assert stat.S_IMODE((settings.storage_root / "raw" / "2026" / "04" / "18").stat().st_mode) == 0o700


@pytest.mark.asyncio
async def test_accept_message_writes_manifest_for_recovery(tmp_path, sample_email_bytes: bytes) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        await runtime.ensure_smtp_session(
            "smtp_test_1",
            SimpleNamespace(peer=("127.0.0.1", 2525), host_name="localhost", ssl=None),
        )
        response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com", "bar@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
            smtp_session_id="smtp_test_1",
        )
        await runtime.drain_parser_queue()

        manifest_paths = list(settings.manifests_dir.rglob("*.json"))
        message_id = response.removeprefix("250 queued as ")
        assert response.startswith("250 queued as ")
        assert len(manifest_paths) == 1

        manifest = json.loads(manifest_paths[0].read_text(encoding="utf-8"))
        assert manifest["message_id"] == message_id
        assert manifest["smtp_session_id"] == "smtp_test_1"
        assert manifest["envelope_from"] == "sender@example.com"
        assert manifest["rcpt_tos"] == ["foo@adb.com", "bar@adb.com"]
        assert manifest["raw_sha256"] == hashlib.sha256(sample_email_bytes).hexdigest()
        assert manifest["raw_path"].endswith(".eml")
        assert manifest["raw_size_bytes"] == len(sample_email_bytes)
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_accept_message_does_not_publish_manifest_when_raw_write_fails(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")

        def fail_write_raw_message(*args, **kwargs):
            raise RuntimeError("raw write failed")

        monkeypatch.setattr(runtime.storage, "write_raw_message", fail_write_raw_message)

        with pytest.raises(RuntimeError, match="raw write failed"):
            await runtime.accept_message(
                rcpt_tos=["foo@adb.com"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )

        manifest_paths = list(settings.manifests_dir.rglob("*.json"))
        assert manifest_paths == []
        assert not list(settings.raw_dir.rglob("*.eml"))

        with connect_database(settings.database_path) as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()

        assert row["count"] == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_accept_message_publishes_raw_before_manifest(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        calls: list[str] = []
        real_write_raw = runtime.storage.write_raw_message
        real_write_manifest = runtime.storage.write_manifest

        def tracked_write_raw(*args, **kwargs):
            calls.append("raw")
            return real_write_raw(*args, **kwargs)

        def tracked_write_manifest(message_id, received_at, payload):
            calls.append("manifest")
            assert runtime.storage.resolve(str(payload["raw_path"])).is_file()
            return real_write_manifest(message_id, received_at, payload)

        monkeypatch.setattr(runtime.storage, "write_raw_message", tracked_write_raw)
        monkeypatch.setattr(runtime.storage, "write_manifest", tracked_write_manifest)

        response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )

        assert response.startswith("250 queued as ")
        assert calls == ["raw", "manifest"]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_placeholder_insert_and_parse_roundtrip(tmp_path, sample_email_bytes: bytes) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")

        result = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()

        mailbox = await runtime.get_mailbox_view("foo@adb.com")
        detail = await runtime.get_delivery_detail("foo@adb.com", mailbox["items"][0]["delivery_id"])

        assert result.startswith("250 queued as ")
        assert mailbox["items"][0]["parse_status"] == "parsed"
        assert mailbox["message_count"] == 1
        assert detail["subject"] == "Hello Rapid Inbox"
        assert detail["text_body"].startswith("Hello from tests.")
        assert detail["html_body"].startswith("<html>")
        assert detail["from_addr"] == "sender@example.com"
        assert any(settings.raw_dir.rglob("*.eml"))
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_python_fallback_parser_persists_verification_code(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        result = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="noreply@openai.com",
            content=(
                b"From: OpenAI <noreply@openai.com>\r\n"
                b"To: foo@adb.com\r\n"
                b"Subject: Your OpenAI verification code\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"\r\n"
                b"Your verification code is 654321.\r\n"
            ),
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = result.removeprefix("250 queued as ")
    with connect_database(settings.database_path) as connection:
        row = connection.execute(
            "SELECT parse_status, verification_code FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()

    assert row["parse_status"] == "parsed"
    assert row["verification_code"] == "654321"


def test_initialize_database_adds_verification_code_to_existing_messages_table(tmp_path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    database_path.parent.mkdir(parents=True)
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            CREATE TABLE messages (
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
                parse_status TEXT NOT NULL DEFAULT 'pending',
                parse_error TEXT,
                has_text INTEGER NOT NULL DEFAULT 0,
                has_html INTEGER NOT NULL DEFAULT 0,
                has_attachments INTEGER NOT NULL DEFAULT 0,
                attachment_count INTEGER NOT NULL DEFAULT 0,
                text_preview TEXT,
                text_body_path TEXT,
                html_body_path TEXT,
                headers_json TEXT,
                is_deleted_globally INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        connection.commit()

    initialize_database(database_path)

    with connect_database(database_path) as connection:
        columns = {str(row["name"]) for row in connection.execute("PRAGMA table_info(messages)").fetchall()}
    assert "verification_code" in columns


@pytest.mark.asyncio
async def test_runtime_parses_pending_messages_written_by_external_ingest_process(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        received_at = "2026-04-18T20:00:00Z"
        message_id = "msg_external_ingest_pending"
        raw_path, raw_sha256, raw_size_bytes = runtime.storage.write_raw_message(
            message_id,
            received_at,
            sample_email_bytes,
        )

        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                INSERT INTO messages (
                    id,
                    raw_path,
                    raw_sha256,
                    raw_size_bytes,
                    envelope_from,
                    from_addr,
                    received_at,
                    parse_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    message_id,
                    raw_path,
                    raw_sha256,
                    raw_size_bytes,
                    "sender@example.com",
                    "sender@example.com",
                    received_at,
                ),
            )
            connection.commit()

        deadline = asyncio.get_running_loop().time() + 2
        row = None
        while asyncio.get_running_loop().time() < deadline:
            with connect_database(settings.database_path) as connection:
                row = connection.execute(
                    """
                    SELECT parse_status, subject, text_preview
                    FROM messages
                    WHERE id = ?
                    """,
                    (message_id,),
                ).fetchone()
            if row is not None and row["parse_status"] == "parsed":
                break
            await asyncio.sleep(0.05)

        assert row is not None
        assert row["parse_status"] == "parsed"
        assert row["subject"] == "Hello Rapid Inbox"
        assert row["text_preview"].startswith("Hello from tests.")
    finally:
        await runtime.stop()
