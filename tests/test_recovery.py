from __future__ import annotations

import asyncio
import json
from pathlib import Path
import sqlite3
import threading
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.ingest.queue import ParseQueue, ParseTask
from app.ingest.recovery import (
    FAILED_REPARSE_BATCH_SIZE,
    FULL_MANIFEST_SCAN_BATCH_SIZE,
    RecoveryScanner,
)
from app.runtime import RapidInboxRuntime
from conftest import connect_database


@pytest.mark.asyncio
async def test_parse_queue_continues_after_worker_exception() -> None:
    seen: list[str] = []
    calls = 0

    async def worker(task: ParseTask) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        seen.append(task.message_id)

    queue = ParseQueue(worker)
    await queue.start()
    try:
        await queue.enqueue(ParseTask(message_id="msg_one", raw_size_bytes=1))
        await queue.enqueue(ParseTask(message_id="msg_two", raw_size_bytes=1))
        await asyncio.wait_for(queue.drain(), timeout=2)
    finally:
        await queue.stop()

    assert seen == ["msg_two"]


@pytest.mark.asyncio
async def test_missing_raw_file_does_not_stop_later_parse_tasks(
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
        await runtime.ensure_smtp_session(
            "smtp_missing_raw",
            SimpleNamespace(peer=("127.0.0.1", 2525), host_name="localhost", ssl=None),
        )
        original_read_bytes = runtime.storage.read_bytes
        call_count = 0

        def flaky_read_bytes(relative_path: str) -> bytes:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise FileNotFoundError(relative_path)
            return original_read_bytes(relative_path)

        monkeypatch.setattr(runtime.storage, "read_bytes", flaky_read_bytes)

        first_response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
            smtp_session_id="smtp_missing_raw",
        )
        second_response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
            smtp_session_id="smtp_missing_raw",
        )
        await asyncio.wait_for(runtime.drain_parser_queue(), timeout=2)
    finally:
        await runtime.stop()

    first_message_id = first_response.removeprefix("250 queued as ")
    second_message_id = second_response.removeprefix("250 queued as ")

    with connect_database(settings.database_path) as connection:
        rows = {
            str(row["id"]): row
            for row in connection.execute(
                """
                SELECT id, parse_status, parse_error
                FROM messages
                WHERE id IN (?, ?)
                """,
                (first_message_id, second_message_id),
            ).fetchall()
        }

    assert rows[first_message_id]["parse_status"] == "failed"
    assert rows[first_message_id]["parse_error"]
    assert rows[second_message_id]["parse_status"] == "parsed"
    assert rows[second_message_id]["parse_error"] is None


@pytest.mark.asyncio
async def test_recovery_scanner_rebuilds_missing_message_and_delivery(tmp_path, sample_email_bytes: bytes) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        await runtime.ensure_smtp_session(
            "smtp_recover_1",
            SimpleNamespace(peer=("127.0.0.1", 2525), host_name="localhost", ssl=None),
        )
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
            smtp_session_id="smtp_recover_1",
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        mailbox = await repaired.get_mailbox_view("foo@adb.com")
        await repaired.drain_parser_queue()
        assert mailbox["message_count"] == 1
        assert mailbox["items"][0]["parse_status"] in {"pending", "parsed"}
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_scanner_restores_parsed_manifest_without_reparse(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        response = await runtime.accept_message(
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

    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parsed"] = {
        "status": "parsed",
        "message_id_header": None,
        "subject": "Recovered parsed subject",
        "from_name": "OpenAI",
        "from_addr": "noreply@openai.com",
        "reply_to": None,
        "date_header": None,
        "has_text": True,
        "has_html": False,
        "has_attachments": False,
        "attachment_count": 0,
        "text_preview": "Your verification code is 654321.",
        "text_body_path": None,
        "html_body_path": None,
        "headers_json": [["Subject", "Recovered parsed subject"]],
        "verification_code": "654321",
        "attachments": [],
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.commit()

    recovered = RapidInboxRuntime(settings)
    await recovered.start()
    try:
        await recovered.drain_parser_queue()
    finally:
        await recovered.stop()

    with connect_database(settings.database_path) as connection:
        row = connection.execute(
            "SELECT parse_status, subject, verification_code FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()

    assert row["parse_status"] == "parsed"
    assert row["subject"] == "Recovered parsed subject"
    assert row["verification_code"] == "654321"


@pytest.mark.asyncio
async def test_recovery_scanner_restores_failed_manifest_without_reparse(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="noreply@example.com",
            content=(
                b"Subject: Broken\r\n"
                b"Content-Type: text/plain; charset=utf-8\r\n"
                b"\r\n"
                b"Body\r\n"
            ),
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["parsed"] = {
        "status": "failed",
        "parse_error": "invalid multipart boundary",
    }
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.commit()

    recovered = RapidInboxRuntime(settings)
    await recovered.start()
    try:
        await recovered.drain_parser_queue()
    finally:
        await recovered.stop()

    with connect_database(settings.database_path) as connection:
        row = connection.execute(
            "SELECT parse_status, parse_error, verification_code FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()

    assert row["parse_status"] == "failed"
    assert row["parse_error"] == "invalid multipart boundary"
    assert row["verification_code"] is None


@pytest.mark.asyncio
async def test_startup_incremental_manifest_scan_skips_consistent_messages(
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
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    async def fail_recover_from_manifest(_manifest) -> None:
        raise AssertionError("a consistent message must not be replayed")

    restarted = RapidInboxRuntime(settings)
    monkeypatch.setattr(restarted, "recover_from_manifest", fail_recover_from_manifest)
    await restarted.start()
    try:
        mailbox = await restarted.get_mailbox_view("foo@adb.com")
        assert mailbox["message_count"] == 1
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_recovery_scanner_rebuilds_mailbox_bounds_from_multiple_deliveries(
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
        await runtime.create_domain("adb.com")
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender1@example.com",
            content=sample_email_bytes,
        )
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender2@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    manifest_paths = sorted(settings.manifests_dir.rglob("*.json"))
    assert len(manifest_paths) == 2

    later_received_at = "2026-04-18T20:05:01Z"
    earlier_received_at = "2026-04-18T20:00:01Z"
    for manifest_path, received_at in zip(manifest_paths, [later_received_at, earlier_received_at], strict=True):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["received_at"] = received_at
        manifest_path.write_text(json.dumps(manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        await repaired.drain_parser_queue()
        mailbox = await repaired.get_mailbox_view("foo@adb.com")
        assert mailbox["message_count"] == 2
        assert len(mailbox["items"]) == 2

        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT first_seen_at, last_seen_at, latest_message_at, message_count
                FROM mailboxes
                WHERE address_canonical = ?
                """,
                ("foo@adb.com",),
            ).fetchone()

        assert row["first_seen_at"] == earlier_received_at
        assert row["last_seen_at"] == later_received_at
        assert row["latest_message_at"] == later_received_at
        assert row["message_count"] == 2
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_scanner_skips_bad_manifests_and_recovers_legacy_manifest_for_inactive_domain(
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
        created_domain = await runtime.create_domain("adb.com")
        response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["recipients"][0]["address_canonical"] == "foo@adb.com"
    assert manifest["recipients"][0]["domain_id"] == created_domain["id"]
    assert manifest["recipients"][0]["root_domain_ascii"] == "adb.com"
    assert manifest["rcpt_tos"] == ["foo@adb.com"]
    manifest.pop("recipients")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    (settings.manifests_dir / "broken.json").write_text("{not valid json", encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("UPDATE domains SET is_active = 0 WHERE root_domain_ascii = ?", ("adb.com",))
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        await repaired.drain_parser_queue()
    finally:
        await repaired.stop()

    with connect_database(settings.database_path) as connection:
        mailbox = connection.execute(
            """
            SELECT first_seen_at, last_seen_at, latest_message_at, message_count
            FROM mailboxes
            WHERE address_canonical = ?
            """,
            ("foo@adb.com",),
        ).fetchone()
        delivery = connection.execute(
            """
            SELECT d.message_id, d.rcpt_to, d.delivered_at
            FROM message_deliveries AS d
            WHERE d.message_id = ?
            """,
            (message_id,),
        ).fetchone()
        message = connection.execute(
            """
            SELECT parse_status, parse_error
            FROM messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()

    assert mailbox["message_count"] == 1
    assert mailbox["first_seen_at"] == manifest["received_at"]
    assert mailbox["last_seen_at"] == manifest["received_at"]
    assert mailbox["latest_message_at"] == manifest["received_at"]
    assert delivery["rcpt_to"] == "foo@adb.com"
    assert delivery["delivered_at"] == manifest["received_at"]
    assert message["parse_status"] == "parsed"
    assert message["parse_error"] is None


@pytest.mark.asyncio
async def test_recovery_scanner_skips_legacy_manifest_when_domain_row_missing(
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
        await runtime.create_domain("adb.com")
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.pop("recipients")
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("adb.com",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        assert repaired.list_domains() == []
        with pytest.raises(LookupError):
            await repaired.get_mailbox_view("foo@adb.com")
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_quarantines_recipient_manifest_without_domain_policy_fail_closed(
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
        await runtime.create_domain(
            "private-recovery.example",
            public_web_enabled=False,
            public_api_enabled=False,
        )
        response = await runtime.accept_message(
            rcpt_tos=["secret@private-recovery.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob(f"{message_id}.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["recipients"][0].pop("domain_policy")
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    raw_path = settings.storage_root / str(manifest["raw_path"])
    assert raw_path.exists()

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries WHERE message_id = ?", (message_id,))
        connection.execute("DELETE FROM messages WHERE id = ?", (message_id,))
        connection.execute("DELETE FROM mailboxes WHERE address_canonical = ?", ("secret@private-recovery.example",))
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("private-recovery.example",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        with connect_database(settings.database_path) as connection:
            assert connection.execute(
                "SELECT 1 FROM domains WHERE root_domain_ascii = ?",
                ("private-recovery.example",),
            ).fetchone() is None
            assert connection.execute(
                "SELECT 1 FROM mailboxes WHERE address_canonical = ?",
                ("secret@private-recovery.example",),
            ).fetchone() is None
            assert connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone() is None

        for surface in ("web", "api"):
            with pytest.raises(LookupError):
                await repaired.get_mailbox_view(
                    "secret@private-recovery.example",
                    surface=surface,
                )

        assert not manifest_path.exists()
        quarantined = list(
            (settings.storage_root / "quarantine" / "manifests").glob(
                f"{message_id}*.json"
            )
        )
        assert len(quarantined) == 1
        assert raw_path.exists()
    finally:
        await repaired.stop()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("plus_addressing_mode", "invalid-mode"),
        ("dns_status", "broken"),
    ],
)
@pytest.mark.asyncio
async def test_recovery_scanner_skips_invalid_domain_policy_values(
    tmp_path,
    sample_email_bytes: bytes,
    field: str,
    value: object,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["recipients"][0]["domain_policy"][field] = value
    manifest_path.write_text(json.dumps(manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("adb.com",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        assert repaired.list_domains() == []
        with pytest.raises(LookupError):
            await repaired.get_mailbox_view("foo@adb.com")
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_scanner_requeues_failed_message_on_startup(
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
        await runtime.create_domain("adb.com")
        response = await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = response.removeprefix("250 queued as ")
    with connect_database(settings.database_path) as connection:
        connection.execute(
            """
            UPDATE messages
            SET parse_status = 'failed',
                parse_error = ?
            WHERE id = ?
            """,
            ("forced failure", message_id),
        )
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        await repaired.drain_parser_queue()
    finally:
        await repaired.stop()

    with connect_database(settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT parse_status, parse_error
            FROM messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()

    assert row["parse_status"] == "parsed"
    assert row["parse_error"] is None


@pytest.mark.asyncio
async def test_recovery_scanner_restores_older_legacy_manifest_after_newer_manifest_recreates_domain(
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
        await runtime.create_domain(
            "adb.com",
            accept_exact=True,
            accept_subdomains=False,
            plus_addressing_mode="strip",
            local_part_case_sensitive=True,
        )
        await runtime.accept_message(
            rcpt_tos=["Foo+Tag@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.accept_message(
            rcpt_tos=["Foo+Tag@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    manifest_paths = sorted(settings.manifests_dir.rglob("*.json"))
    assert len(manifest_paths) == 2

    legacy_manifest_path = manifest_paths[0]
    legacy_manifest = json.loads(legacy_manifest_path.read_text(encoding="utf-8"))
    legacy_manifest.pop("recipients")
    legacy_manifest["received_at"] = "2026-04-17T20:00:00Z"
    legacy_target_path = settings.manifests_dir / "2026" / "04" / "17" / legacy_manifest_path.name
    legacy_target_path.parent.mkdir(parents=True, exist_ok=True)
    legacy_manifest_path.rename(legacy_target_path)
    legacy_target_path.write_text(json.dumps(legacy_manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("adb.com",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        await repaired.drain_parser_queue()
        mailbox = await repaired.get_mailbox_view("Foo+Tag@adb.com")

        assert mailbox["message_count"] == 2
        assert len(mailbox["items"]) == 2
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_scanner_recreates_deleted_domain_from_manifest_metadata(
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
        await runtime.create_domain(
            "adb.com",
            accept_exact=True,
            accept_subdomains=False,
            public_web_enabled=False,
            public_api_enabled=False,
            plus_addressing_mode="strip",
            local_part_case_sensitive=True,
        )
        with connect_database(settings.database_path) as connection:
            connection.execute("UPDATE domains SET is_hidden = 1 WHERE root_domain_ascii = ?", ("adb.com",))
            connection.commit()
        response = await runtime.accept_message(
            rcpt_tos=["Foo+Tag@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    policy = manifest["recipients"][0]["domain_policy"]
    assert policy["accept_exact"] is True
    assert policy["accept_subdomains"] is False
    assert policy["plus_addressing_mode"] == "strip"
    assert policy["local_part_case_sensitive"] is True
    assert policy["public_web_enabled"] is False
    assert policy["public_api_enabled"] is False
    assert policy["is_hidden"] is True
    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("adb.com",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        mailbox = await repaired.get_mailbox_view("Foo+Tag@adb.com")
        await repaired.drain_parser_queue()
        mailbox_after_parse = await repaired.get_mailbox_view("Foo+Tag@adb.com")

        assert mailbox["message_count"] == 1
        assert mailbox["items"][0]["message_id"] == message_id
        assert mailbox_after_parse["items"][0]["parse_status"] == "parsed"
    finally:
        await repaired.stop()

    with connect_database(settings.database_path) as connection:
        domain = connection.execute(
            """
            SELECT id, root_domain_ascii, is_active
                 , plus_addressing_mode, local_part_case_sensitive, public_web_enabled, public_api_enabled, is_hidden
            FROM domains
            WHERE root_domain_ascii = ?
            """,
            ("adb.com",),
        ).fetchone()
        delivery = connection.execute(
            """
            SELECT d.message_id, d.rcpt_to
            FROM message_deliveries AS d
            WHERE d.message_id = ?
            """,
            (message_id,),
        ).fetchone()

    assert domain["root_domain_ascii"] == "adb.com"
    assert domain["is_active"] == 1
    assert domain["plus_addressing_mode"] == "strip"
    assert domain["local_part_case_sensitive"] == 1
    assert domain["public_web_enabled"] == 0
    assert domain["public_api_enabled"] == 0
    assert domain["is_hidden"] == 1
    assert delivery["rcpt_to"] == "Foo+Tag@adb.com"


@pytest.mark.asyncio
async def test_recovery_scanner_uses_latest_policy_snapshot_for_deleted_domain(
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
        await runtime.create_domain(
            "adb.com",
            accept_exact=True,
            accept_subdomains=False,
            plus_addressing_mode="keep",
            local_part_case_sensitive=False,
        )
        await runtime.accept_message(
            rcpt_tos=["Foo+Tag@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )

        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                UPDATE domains
                SET plus_addressing_mode = 'strip',
                    local_part_case_sensitive = 1
                WHERE root_domain_ascii = ?
                """,
                ("adb.com",),
            )
            connection.commit()
        runtime.domains.reload()

        await runtime.accept_message(
            rcpt_tos=["Foo+Tag@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    manifest_paths = sorted(settings.manifests_dir.rglob("*.json"))
    assert len(manifest_paths) == 2

    old_manifest_path = None
    new_manifest_path = None
    for manifest_path in manifest_paths:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        policy = manifest["recipients"][0]["domain_policy"]
        if policy["plus_addressing_mode"] == "keep":
            old_manifest_path = manifest_path
            old_manifest = manifest
        else:
            new_manifest_path = manifest_path
            new_manifest = manifest

    assert old_manifest_path is not None
    assert new_manifest_path is not None

    assert isinstance(old_manifest["recovery_order_ns"], int)
    assert isinstance(new_manifest["recovery_order_ns"], int)
    same_second = "2026-04-18T20:00:00Z"
    old_manifest["received_at"] = same_second
    new_manifest["received_at"] = same_second
    old_manifest["recovery_order_ns"] = 1_000_000_000
    new_manifest["recovery_order_ns"] = 2_000_000_000
    target_dir = settings.manifests_dir / "2026" / "04" / "18"
    old_target_path = target_dir / "z-old-policy.json"
    new_target_path = target_dir / "a-new-policy.json"
    target_dir.mkdir(parents=True, exist_ok=True)
    old_manifest_path.rename(old_target_path)
    if new_manifest_path != new_target_path:
        new_manifest_path.rename(new_target_path)
    old_target_path.write_text(json.dumps(old_manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")
    new_target_path.write_text(json.dumps(new_manifest, sort_keys=True, ensure_ascii=False), encoding="utf-8")

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute("DELETE FROM domains WHERE root_domain_ascii = ?", ("adb.com",))
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        with connect_database(settings.database_path) as connection:
            domain = connection.execute(
                """
                SELECT plus_addressing_mode, local_part_case_sensitive
                FROM domains
                WHERE root_domain_ascii = ?
                """,
                ("adb.com",),
            ).fetchone()
            message_count = connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()
            delivery_count = connection.execute("SELECT COUNT(*) AS count FROM message_deliveries").fetchone()

        assert domain["plus_addressing_mode"] == "strip"
        assert domain["local_part_case_sensitive"] == 1
        assert message_count["count"] == 2
        assert delivery_count["count"] == 2
    finally:
        await repaired.stop()


@pytest.mark.asyncio
async def test_recovery_does_not_reuse_historical_domain_id_owned_by_another_root(
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
        created_domain = await runtime.create_domain("historical.example")
        response = await runtime.accept_message(
            rcpt_tos=["box@historical.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
    finally:
        await runtime.stop()

    historical_id = int(created_domain["id"])
    message_id = response.removeprefix("250 queued as ")
    manifest_path = next(settings.manifests_dir.rglob("*.json"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["recipients"][0]["domain_id"] == historical_id

    with connect_database(settings.database_path) as connection:
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.execute(
            """
            UPDATE domains
            SET root_domain_ascii = 'unrelated.example',
                root_domain_unicode = 'unrelated.example'
            WHERE id = ?
            """,
            (historical_id,),
        )
        connection.commit()

    repaired = RapidInboxRuntime(settings)
    await repaired.start()
    try:
        await repaired.drain_parser_queue()
    finally:
        await repaired.stop()

    with connect_database(settings.database_path) as connection:
        domains = {
            str(row["root_domain_ascii"]): int(row["id"])
            for row in connection.execute(
                "SELECT id, root_domain_ascii FROM domains"
            ).fetchall()
        }
        delivery = connection.execute(
            """
            SELECT m.address_canonical, d.root_domain_ascii, md.message_id
            FROM message_deliveries AS md
            JOIN mailboxes AS m ON m.id = md.mailbox_id
            JOIN domains AS d ON d.id = m.domain_id
            WHERE md.message_id = ?
            """,
            (message_id,),
        ).fetchone()

    assert domains["unrelated.example"] == historical_id
    assert domains["historical.example"] != historical_id
    assert delivery["address_canonical"] == "box@historical.example"
    assert delivery["root_domain_ascii"] == "historical.example"


@pytest.mark.asyncio
@pytest.mark.parametrize("policy_change", ["rename", "delete"])
async def test_recovery_quarantines_manifest_for_explicitly_retired_domain_identity(
    tmp_path,
    sample_email_bytes: bytes,
    policy_change: str,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        domain = await runtime.create_domain("retired-policy.example")
        response = await runtime.accept_message(
            rcpt_tos=["box@retired-policy.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        message_id = response.removeprefix("250 queued as ")
        with connect_database(settings.database_path) as connection:
            connection.execute("DELETE FROM message_deliveries WHERE message_id = ?", (message_id,))
            connection.execute("DELETE FROM messages WHERE id = ?", (message_id,))
            connection.execute("DELETE FROM mailboxes WHERE domain_id = ?", (domain["id"],))
            connection.commit()

        if policy_change == "rename":
            await runtime.domains.update_domain(
                domain["id"],
                {"root_domain": "retired-policy-renamed.example"},
            )
        else:
            await runtime.domains.delete_domain(domain["id"])
    finally:
        await runtime.stop()

    raw_path = next(settings.raw_dir.rglob("*.eml"))
    restarted = RapidInboxRuntime(settings)
    await restarted.start()
    try:
        with connect_database(settings.database_path) as connection:
            assert connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone() is None
            assert connection.execute(
                "SELECT 1 FROM domains WHERE root_domain_ascii = 'retired-policy.example'"
            ).fetchone() is None
        quarantined = list(
            (settings.storage_root / "quarantine" / "manifests").glob("*.json")
        )
        assert len(quarantined) == 1
        assert raw_path.exists()
    finally:
        await restarted.stop()


@pytest.mark.asyncio
async def test_full_recovery_scans_large_manifest_and_database_history_in_bounded_batches(
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
        await runtime.create_domain("bulk.example")
        match = runtime.domains.match_address("box@bulk.example")
        assert match is not None
        await runtime._ensure_mailbox_exists(match)
        with connect_database(settings.database_path) as connection:
            mailbox_id = int(
                connection.execute(
                    "SELECT id FROM mailboxes WHERE address_canonical = 'box@bulk.example'"
                ).fetchone()["id"]
            )

            message_rows = []
            delivery_rows = []
            manifest_dir = settings.manifests_dir / "bulk"
            manifest_dir.mkdir(parents=True, exist_ok=True)
            for index in range(FULL_MANIFEST_SCAN_BATCH_SIZE * 2 + 37):
                message_id = f"msg_bulk_{index:05d}"
                raw_path = f"raw/bulk/{message_id}.eml"
                received_at = "2026-04-18T20:00:00Z"
                message_rows.append(
                    (
                        message_id,
                        raw_path,
                        "0" * 64,
                        1,
                        received_at,
                        "parsed",
                    )
                )
                delivery_rows.append(
                    (
                        f"dlv_bulk_{index:05d}",
                        message_id,
                        mailbox_id,
                        "box@bulk.example",
                        received_at,
                    )
                )
                (manifest_dir / f"{message_id}.json").write_text(
                    json.dumps(
                        {
                            "message_id": message_id,
                            "received_at": received_at,
                            "raw_path": raw_path,
                            "raw_sha256": "0" * 64,
                            "raw_size_bytes": 1,
                            "rcpt_tos": ["box@bulk.example"],
                        },
                        separators=(",", ":"),
                    ),
                    encoding="utf-8",
                )
            connection.executemany(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                message_rows,
            )
            connection.executemany(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                delivery_rows,
            )
            connection.commit()

        scanner = runtime.recovery
        subset_sizes: list[int] = []
        real_subset_lookup = scanner._complete_message_ids_subset

        def record_subset_lookup(message_ids: set[str]) -> set[str]:
            subset_sizes.append(len(message_ids))
            return real_subset_lookup(message_ids)

        monkeypatch.setattr(scanner, "_complete_message_ids_subset", record_subset_lookup)
        spool = await asyncio.to_thread(scanner._scan_manifest_files, False)
        try:
            assert spool.connection.execute(
                "SELECT COUNT(*) AS count FROM recovery_manifests"
            ).fetchone()["count"] == 0
        finally:
            spool.connection.close()

        assert sum(subset_sizes) == FULL_MANIFEST_SCAN_BATCH_SIZE * 2 + 37
        assert max(subset_sizes) <= FULL_MANIFEST_SCAN_BATCH_SIZE
        assert len(subset_sizes) == 3
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_recovery_rejects_oversized_manifest_before_json_decode_and_quarantines(
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
        manifest_path = settings.manifests_dir / "oversized.json"
        manifest_path.write_bytes(b"{" + (b"x" * 128) + b"}")
        monkeypatch.setattr("app.ingest.recovery.MAX_RECOVERY_MANIFEST_BYTES", 64)

        def unexpected_json_decode(_payload):
            raise AssertionError("oversized manifest reached json.loads")

        monkeypatch.setattr("app.ingest.recovery.json.loads", unexpected_json_decode)
        result = await asyncio.to_thread(
            runtime.recovery._scan_selected_manifest_files,
            [(manifest_path, manifest_path.stat().st_mtime_ns)],
            [],
        )

        assert result["policy"] == []
        assert result["legacy"] == []
        assert not manifest_path.exists()
        quarantined = list(
            (settings.storage_root / "quarantine" / "manifests").glob(
                "oversized*.json"
            )
        )
        assert len(quarantined) == 1
        assert quarantined[0].stat().st_size == 130
    finally:
        await runtime.stop()


def test_full_manifest_scan_batches_are_bounded_by_encoded_file_bytes(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    manifest_paths = [settings.manifests_dir / f"large-{index}.json" for index in range(5)]
    for manifest_path in manifest_paths:
        manifest_path.write_bytes(b"x" * 60)

    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    batches: list[list[Path]] = []

    def capture_batch(_connection, selected_paths):
        batches.append([path for path, _mtime_ns in selected_paths])

    monkeypatch.setattr("app.ingest.recovery.MAX_RECOVERY_MANIFEST_BYTES", 80)
    monkeypatch.setattr("app.ingest.recovery.MAX_RECOVERY_MANIFEST_BATCH_BYTES", 100)
    monkeypatch.setattr(scanner, "_spool_full_scan_batch", capture_batch)

    spool = scanner._build_full_recovery_spool()
    try:
        assert [path for batch in batches for path in batch] == manifest_paths
        assert len(batches) == len(manifest_paths)
        assert all(
            sum(path.stat().st_size for path in batch) <= 100
            for batch in batches
        )
    finally:
        spool.connection.close()
        scanner._periodic_state.close()


def test_full_manifest_replay_page_is_bounded_by_spooled_json_bytes(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE recovery_manifests (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            manifest_path TEXT NOT NULL,
            manifest_json TEXT NOT NULL
        )
        """
    )
    payloads = [
        json.dumps(
            {"index": index, "padding": "x" * 30},
            separators=(",", ":"),
        )
        for index in range(3)
    ]
    encoded_bytes = len(payloads[0].encode("utf-8"))
    connection.executemany(
        "INSERT INTO recovery_manifests (manifest_path, manifest_json) VALUES (?, ?)",
        [(f"manifest-{index}.json", payload) for index, payload in enumerate(payloads)],
    )
    monkeypatch.setattr("app.ingest.recovery.MAX_RECOVERY_MANIFEST_BYTES", 1_000)
    monkeypatch.setattr(
        "app.ingest.recovery.MAX_RECOVERY_MANIFEST_BATCH_BYTES",
        encoded_bytes * 2,
    )

    try:
        first_page = scanner._read_full_manifest_page(connection, 0)
        second_page = scanner._read_full_manifest_page(connection, first_page[-1][0])

        assert len(first_page) == 2
        assert len(second_page) == 1
        assert sum(len(payloads[sequence - 1].encode("utf-8")) for sequence, _, _ in first_page) <= encoded_bytes * 2
        assert sum(len(payloads[sequence - 1].encode("utf-8")) for sequence, _, _ in second_page) <= encoded_bytes * 2
    finally:
        connection.close()
        scanner._periodic_state.close()


@pytest.mark.asyncio
async def test_recovery_prefilters_complete_message_by_manifest_filename_before_read(
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
        await runtime.create_domain("complete-prefilter.example")
        response = await runtime.accept_message(
            rcpt_tos=["box@complete-prefilter.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")
        manifest_path = next(settings.manifests_dir.rglob(f"{message_id}.json"))

        def unexpected_read(_path):
            raise AssertionError("complete message manifest was opened")

        monkeypatch.setattr(runtime.recovery, "_read_manifest", unexpected_read)
        result = await asyncio.to_thread(
            runtime.recovery._scan_selected_manifest_files,
            [(manifest_path, manifest_path.stat().st_mtime_ns)],
            [],
        )

        assert result["policy"] == []
        assert result["legacy"] == []
        assert manifest_path.exists()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_failed_startup_reparse_pages_large_database_history(
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
        total = FAILED_REPARSE_BATCH_SIZE * 2 + 41
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes,
                    received_at, parse_status, parse_error
                ) VALUES (?, ?, ?, ?, ?, 'failed', 'historical failure')
                """,
                [
                    (
                        f"msg_failed_{index:05d}",
                        f"raw/failed/msg_failed_{index:05d}.eml",
                        "0" * 64,
                        1,
                        "2026-04-18T20:00:00Z",
                    )
                    for index in range(total)
                ],
            )
            connection.commit()

        cutoff = await runtime.recovery_reparse_rowid_cutoff()
        page_sizes: list[int] = []
        queued: list[str] = []
        real_find_page = runtime.find_failed_reparse_page

        async def record_page(**kwargs):
            tasks, cursor = await real_find_page(**kwargs)
            page_sizes.append(len(tasks))
            return tasks, cursor

        async def capture_task(task: ParseTask) -> bool:
            queued.append(task.message_id)
            return True

        monkeypatch.setattr(runtime, "find_failed_reparse_page", record_page)
        monkeypatch.setattr(runtime, "enqueue_recovery_parse_task", capture_task)
        await runtime.recovery._requeue_unparsed_messages(max_rowid=cutoff)

        assert len(queued) == total
        assert len(set(queued)) == total
        assert max(page_sizes) <= FAILED_REPARSE_BATCH_SIZE
        assert page_sizes == [FAILED_REPARSE_BATCH_SIZE, FAILED_REPARSE_BATCH_SIZE, 41]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_periodic_recovery_uses_watermark_and_quarantines_new_bad_manifest(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    monkeypatch.setattr("app.ingest.recovery.MANIFEST_RECOVERY_STABILITY_SECONDS", 0.0)
    await runtime.start()
    try:
        await runtime.create_domain("watermark.example")
        await runtime.accept_message(
            rcpt_tos=["box@watermark.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()

        scanner = runtime.recovery
        main_thread_id = threading.get_ident()
        reads: list[tuple[str, int]] = []
        real_read_manifest = scanner._read_manifest

        def counting_read_manifest(path):
            reads.append((str(path), threading.get_ident()))
            return real_read_manifest(path)

        monkeypatch.setattr(scanner, "_read_manifest", counting_read_manifest)

        await scanner.recover_missing_manifests()
        # The durable filename is the message ID, so completed rows are
        # filtered in SQLite before their JSON receipt is opened.
        assert reads == []

        await scanner.recover_missing_manifests(incremental=True)
        assert reads == []

        bad_manifest = settings.manifests_dir / "new-broken.json"
        bad_manifest.write_text("{not valid json", encoding="utf-8")
        await scanner.recover_missing_manifests(incremental=True)

        assert len(reads) == 1
        assert reads[-1][0] == str(bad_manifest)
        assert reads[-1][1] != main_thread_id
        assert not bad_manifest.exists()
        quarantined = list((settings.storage_root / "quarantine" / "manifests").glob("new-broken*.json"))
        assert len(quarantined) == 1

        await scanner.recover_missing_manifests(incremental=True)
        assert len(reads) == 1
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_stop_waits_for_cancelled_manifest_scan_thread_before_closing_state(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    await runtime._stop_pending_parse_scan_loop()

    scan_entered = threading.Event()
    release_scan = threading.Event()
    scan_exited = threading.Event()
    scan_errors: list[BaseException] = []
    stop_task: asyncio.Task[None] | None = None

    def blocking_scan(incremental: bool):
        assert incremental is True
        try:
            runtime.recovery._periodic_state.execute("SELECT 1").fetchone()
            scan_entered.set()
            if not release_scan.wait(timeout=5):
                raise TimeoutError("test did not release manifest scan")
            # This access occurs after the asyncio waiter has been cancelled.
            # It must still succeed until the worker is about to return.
            runtime.recovery._periodic_state.execute("SELECT 1").fetchone()
        except BaseException as exc:
            scan_errors.append(exc)
        finally:
            scan_exited.set()
        return {
            "policy": [],
            "legacy": [],
            "retry_paths": [],
            "watermark_paths": [],
        }

    async def pending_scan_was_detached() -> None:
        while runtime._pending_parse_scan_task is not None:
            await asyncio.sleep(0)

    monkeypatch.setattr(runtime.recovery, "_scan_manifest_files", blocking_scan)
    scan_task = asyncio.create_task(
        runtime.recovery.recover_missing_manifests(incremental=True)
    )
    runtime._pending_parse_scan_task = scan_task

    try:
        assert await asyncio.wait_for(
            asyncio.to_thread(scan_entered.wait, 2),
            timeout=3,
        )
        stop_task = asyncio.create_task(runtime.stop())
        await asyncio.wait_for(pending_scan_was_detached(), timeout=2)

        assert scan_task.cancelling()
        assert not scan_task.done()
        assert not stop_task.done()
        assert runtime.recovery._periodic_state_closed is False

        release_scan.set()
        await asyncio.wait_for(asyncio.shield(stop_task), timeout=5)

        assert scan_exited.is_set()
        assert scan_errors == []
        assert runtime.recovery._periodic_state_closed is True
        assert scan_task.cancelled()
    finally:
        release_scan.set()
        if stop_task is None:
            await runtime.stop()
        elif not stop_task.done():
            await asyncio.wait_for(asyncio.shield(stop_task), timeout=5)


def test_incremental_manifest_selection_continues_after_one_stat_race(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    paths = [settings.manifests_dir / name for name in ("a.json", "b.json", "c.json")]
    for path in paths:
        path.write_text("{}", encoding="utf-8")

    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    monkeypatch.setattr(scanner, "_recent_manifest_directories", lambda: [settings.manifests_dir])
    mtimes = {"a.json": 100, "c.json": 50}

    def racing_stat(path):
        if path.name == "b.json":
            raise FileNotFoundError(path)
        return mtimes[path.name]

    monkeypatch.setattr(scanner, "_manifest_mtime_ns", racing_stat)
    selected, watermark_paths = scanner._select_manifest_paths(incremental=True)

    assert {path.name for path, _mtime_ns in selected} == {"a.json", "c.json"}
    assert selected == watermark_paths


def test_incremental_watermark_drains_equal_mtime_batch_boundary(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    paths = [settings.manifests_dir / f"manifest-{index}.json" for index in range(5)]
    for path in paths:
        path.write_text("{}", encoding="utf-8")

    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    monkeypatch.setattr(scanner, "_recent_manifest_directories", lambda: [settings.manifests_dir])
    monkeypatch.setattr(scanner, "_manifest_mtime_ns", lambda _path: 100)
    monkeypatch.setattr("app.ingest.recovery.PERIODIC_MANIFEST_SCAN_BATCH_SIZE", 3)

    first, first_watermark = scanner._select_manifest_paths(incremental=True)
    assert [path.name for path, _mtime_ns in first] == [
        "manifest-0.json",
        "manifest-1.json",
        "manifest-2.json",
    ]
    scanner._advance_periodic_watermark(first_watermark)

    second, second_watermark = scanner._select_manifest_paths(incremental=True)
    assert [path.name for path, _mtime_ns in second] == [
        "manifest-3.json",
        "manifest-4.json",
    ]
    scanner._advance_periodic_watermark(second_watermark)
    assert scanner._select_manifest_paths(incremental=True) == ([], [])


def test_full_scan_watermark_detects_new_path_with_same_mtime(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    old_paths = [settings.manifests_dir / f"old-{index}.json" for index in range(3)]
    for path in old_paths:
        path.write_text("{}", encoding="utf-8")

    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    monkeypatch.setattr(scanner, "_recent_manifest_directories", lambda: [settings.manifests_dir])
    monkeypatch.setattr(scanner, "_manifest_mtime_ns", lambda _path: 100)
    scanner._reset_full_scan_watermark_paths()
    scanner._record_full_scan_watermark_paths([(path, 100) for path in old_paths], 100)
    scanner._advance_full_periodic_watermark(100)

    new_path = settings.manifests_dir / "new-but-equal-mtime.json"
    new_path.write_text("{}", encoding="utf-8")
    selected, watermark_paths = scanner._select_manifest_paths(incremental=True)

    assert selected == [(new_path, 100)]
    assert watermark_paths == selected


def test_periodic_retry_index_is_disk_backed_and_rotates_large_history(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    retry_paths = [settings.manifests_dir / f"legacy-{index}.json" for index in range(5_000)]
    scanner._schedule_periodic_retries(retry_paths)
    monkeypatch.setattr(scanner, "_manifest_mtime_ns", lambda _path: 100)

    assert scanner._periodic_retry_count() == len(retry_paths)
    assert not hasattr(scanner, "_periodic_retry_paths")
    assert not hasattr(scanner, "_periodic_retry_queue")

    seen: set[Path] = set()
    for _iteration in range(5):
        selected = scanner._take_periodic_retries(1_000)
        assert len(selected) == 1_000
        seen.update(path for path, _mtime_ns in selected)

    assert seen == set(retry_paths)


def test_incremental_retry_rotation_reserves_capacity_for_fresh_manifests(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    fresh_paths = [settings.manifests_dir / f"fresh-{index}.json" for index in range(4)]
    retry_paths = [settings.manifests_dir / f"retry-{index}.json" for index in range(4)]
    for path in fresh_paths + retry_paths:
        path.write_text("{}", encoding="utf-8")

    scanner = RecoveryScanner(SimpleNamespace(settings=settings))
    for path in retry_paths:
        scanner._schedule_periodic_retry(path)
    monkeypatch.setattr(scanner, "_recent_manifest_directories", lambda: [settings.manifests_dir])
    monkeypatch.setattr(scanner, "_manifest_mtime_ns", lambda _path: 100)
    monkeypatch.setattr("app.ingest.recovery.PERIODIC_MANIFEST_SCAN_BATCH_SIZE", 4)

    seen_retries: set[str] = set()
    for _iteration in range(4):
        selected, watermark_paths = scanner._select_manifest_paths(incremental=True)
        selected_names = {path.name for path, _mtime_ns in selected}
        retry_names = {name for name in selected_names if name.startswith("retry-")}
        fresh_names = {name for name in selected_names if name.startswith("fresh-")}
        assert len(retry_names) == 1
        assert len(fresh_names) == 3
        assert {path.name for path, _mtime_ns in watermark_paths} == fresh_names
        seen_retries.update(retry_names)

    assert seen_retries == {path.name for path in retry_paths}


@pytest.mark.asyncio
async def test_unexpected_replay_failure_does_not_advance_incremental_watermark(
    tmp_path,
    monkeypatch,
) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(_mail_store_lock=asyncio.Lock())

    async def fail_replay(_manifest):
        raise RuntimeError("database unavailable")

    async def enqueue(_message_id, *, raw_size_bytes=None):
        return True

    runtime.recover_from_manifest = fail_replay
    runtime.enqueue_message_for_parse = enqueue
    scanner = RecoveryScanner(runtime)
    scan_result = {
        "policy": [],
        "legacy": [(manifest_path, {"message_id": "msg_retry", "raw_size_bytes": 1})],
        "watermark_paths": [(manifest_path, 123)],
    }
    monkeypatch.setattr(scanner, "_scan_manifest_files", lambda _incremental: scan_result)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await scanner.recover_missing_manifests(incremental=True)
    assert scanner._periodic_watermark_ns == -1

    async def successful_replay(_manifest):
        return True

    runtime.recover_from_manifest = successful_replay
    await scanner.recover_missing_manifests(incremental=True)
    assert scanner._periodic_watermark_ns == 123


@pytest.mark.asyncio
async def test_startup_replay_value_error_is_pinned_for_periodic_retry(tmp_path, monkeypatch) -> None:
    manifest_path = tmp_path / "legacy.json"
    manifest_path.write_text("{}", encoding="utf-8")
    runtime = SimpleNamespace(_mail_store_lock=asyncio.Lock())
    replay_attempts = 0

    async def replay_after_domain_repair(_manifest):
        nonlocal replay_attempts
        replay_attempts += 1
        if replay_attempts == 1:
            raise ValueError("domain missing")
        return True

    async def enqueue(_message_id, *, raw_size_bytes=None):
        return True

    runtime.recover_from_manifest = replay_after_domain_repair
    runtime.enqueue_message_for_parse = enqueue
    scanner = RecoveryScanner(runtime)
    scan_result = {
        "policy": [],
        "legacy": [(manifest_path, {"message_id": "msg_legacy", "raw_size_bytes": 1})],
        "watermark_paths": [(manifest_path, 123)],
    }
    monkeypatch.setattr(scanner, "_scan_manifest_files", lambda _incremental: scan_result)

    await scanner.recover_missing_manifests()
    assert scanner._has_periodic_retry(manifest_path)
    assert scanner._periodic_watermark_ns == 123

    await scanner.recover_missing_manifests(incremental=True)
    assert replay_attempts == 2
    assert not scanner._has_periodic_retry(manifest_path)
    assert scanner._periodic_retry_count() == 0


@pytest.mark.asyncio
async def test_periodic_recovery_defers_manifest_while_original_python_commit_is_blocked(
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
        await runtime.create_domain("inflight.example")
        artifact_published = threading.Event()
        writer_blocked = asyncio.Event()
        release_writer = asyncio.Event()
        real_write_artifacts = runtime._write_accept_artifacts
        real_writer_execute = runtime.writer.execute
        should_block_writer = True

        def capture_artifact_publish(message_id, received_at, manifest_payload, content) -> None:
            real_write_artifacts(message_id, received_at, manifest_payload, content)
            artifact_published.set()

        async def block_original_metadata_commit(operation):
            nonlocal should_block_writer
            if artifact_published.is_set() and should_block_writer:
                should_block_writer = False
                writer_blocked.set()
                await release_writer.wait()
            return await real_writer_execute(operation)

        monkeypatch.setattr(runtime, "_write_accept_artifacts", capture_artifact_publish)
        monkeypatch.setattr(runtime.writer, "execute", block_original_metadata_commit)
        monkeypatch.setattr("app.ingest.recovery.MANIFEST_RECOVERY_STABILITY_SECONDS", 0.0)

        accept_task = asyncio.create_task(
            runtime.accept_message(
                rcpt_tos=["box@inflight.example"],
                envelope_from="sender@example.com",
                content=sample_email_bytes,
            )
        )
        await asyncio.wait_for(writer_blocked.wait(), timeout=2)

        manifest_path = next(settings.manifests_dir.rglob("*.json"))
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        message_id = str(manifest["message_id"])
        assert message_id in runtime.active_mail_accept_message_ids()

        await runtime.recovery.recover_missing_manifests(incremental=True)
        with connect_database(settings.database_path) as connection:
            assert connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone() is None
        assert runtime.recovery._has_periodic_retry(manifest_path)

        release_writer.set()
        response = await asyncio.wait_for(accept_task, timeout=2)
        assert response == f"250 queued as {message_id}"
        await runtime.recovery.recover_missing_manifests(incremental=True)
        assert not runtime.recovery._has_periodic_retry(manifest_path)
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_fresh_ingestd_queue_defers_recovery_without_losing_startup_full_scan(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    runtime = SimpleNamespace(settings=settings, _mail_store_lock=asyncio.Lock())
    scanner = RecoveryScanner(runtime)
    calls: list[bool] = []

    async def record_scan(*, incremental: bool) -> set[str]:
        calls.append(incremental)
        return set()

    monkeypatch.setattr(scanner, "_recover_manifests", record_scan)
    status_path = settings.storage_root / ".ingestd.status.json"
    status_path.write_text(json.dumps({"queue_messages": 1}), encoding="utf-8")

    assert await scanner.recover_missing_manifests(incremental=True) == set()
    assert calls == []
    assert scanner._startup_full_scan_complete is False

    status_path.write_text(json.dumps({"queue_messages": 0}), encoding="utf-8")
    await scanner.recover_missing_manifests(incremental=True)
    await scanner.recover_missing_manifests(incremental=True)
    assert calls == [False, True]
    assert scanner._startup_full_scan_complete is True
