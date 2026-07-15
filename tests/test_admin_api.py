from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from app.config import Settings
from app.db.connection import connect_database
from app.ingest.parser import ParsedMessage
from app.ingest.queue import ParseTask
from app.main import create_app
from app.services.settings import SettingsService
from app.smtp.handler import RapidInboxHandler
import app.http.admin_api as admin_api_module


@pytest.mark.asyncio
async def test_admin_domain_api_creates_and_lists_domains(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        admin_token="admin-secret",
    )
    app = create_app(settings=settings)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            created = await client.post(
                "/api/v1/admin/domains",
                headers={"X-API-Key": settings.admin_token},
                json={"root_domain": "adb.com", "accept_subdomains": True},
            )
            listed = await client.get(
                "/api/v1/admin/domains",
                headers={"X-API-Key": settings.admin_token},
            )

        assert created.status_code == 201
        assert created.json()["root_domain_ascii"] == "adb.com"
        assert listed.status_code == 200
        assert listed.json()["items"][0]["root_domain_ascii"] == "adb.com"


@pytest.mark.asyncio
async def test_admin_sync_domain_read_does_not_block_event_loop(
    admin_client,
    runtime,
    monkeypatch,
) -> None:
    original_domains_page = admin_api_module._domains_page
    started = threading.Event()

    def slow_domains_page(*args, **kwargs):
        started.set()
        time.sleep(0.2)
        return original_domains_page(*args, **kwargs)

    monkeypatch.setattr(admin_api_module, "_domains_page", slow_domains_page)
    request_task = asyncio.create_task(admin_client.get("/api/v1/admin/domains"))

    assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1.0)
    assert not request_task.done()

    response = await request_task
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_admin_domain_list_is_sql_paginated_and_hard_limited(admin_client, runtime) -> None:
    for root_domain in ("paged-a.example", "paged-b.example", "paged-c.example"):
        await runtime.create_domain(root_domain)

    first = await admin_client.get("/api/v1/admin/domains", params={"limit": 2, "offset": 0})
    second = await admin_client.get("/api/v1/admin/domains", params={"limit": 2, "offset": 2})
    oversized = await admin_client.get("/api/v1/admin/domains", params={"limit": 201})

    assert first.status_code == 200
    assert first.json()["limit"] == 2
    assert first.json()["offset"] == 0
    assert first.json()["total_count"] == 3
    assert len(first.json()["items"]) == 2
    assert second.status_code == 200
    assert len(second.json()["items"]) == 1
    assert oversized.status_code == 422


@pytest.mark.asyncio
async def test_admin_bulk_delete_rejects_too_many_ids_before_resource_queries(
    admin_client,
    monkeypatch,
) -> None:
    checked_ids: list[str] = []

    async def track_delivery_grant(_request, _admin, delivery_id: str) -> str:
        checked_ids.append(delivery_id)
        return "message-id"

    monkeypatch.setattr(admin_api_module, "_require_delivery_grant", track_delivery_grant)
    response = await admin_client.post(
        "/api/v1/admin/messages/bulk-delete",
        json={"delivery_ids": [f"delivery-{index}" for index in range(1001)]},
    )

    assert response.status_code == 422
    assert "maximum of 1000" in response.json()["detail"]
    assert checked_ids == []


@pytest.mark.asyncio
async def test_admin_smtp_session_events_are_paginated_and_hard_limited(
    admin_client,
    runtime,
) -> None:
    session_id = "smtp_v1_event_page"
    await runtime.ensure_smtp_session(
        session_id,
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    for index in range(3):
        await runtime.record_smtp_event(session_id, "command", {"index": index})

    page = await admin_client.get(
        f"/api/v1/admin/smtp-sessions/{session_id}",
        params={"event_limit": 1, "event_offset": 1},
    )
    oversized = await admin_client.get(
        f"/api/v1/admin/smtp-sessions/{session_id}",
        params={"event_limit": 1001},
    )

    assert page.status_code == 200
    assert page.json()["events_total_count"] >= 3
    assert page.json()["events_limit"] == 1
    assert page.json()["events_offset"] == 1
    assert len(page.json()["events"]) == 1
    assert oversized.status_code == 422


@pytest.mark.asyncio
async def test_admin_api_supports_message_reparse_and_settings_update(admin_client, runtime, seeded_message) -> None:
    reparse = await admin_client.post(f"/api/v1/admin/messages/{seeded_message.message_id}/reparse")
    settings_response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={"max_recipients_per_message": "25"},
    )
    audit = await admin_client.get("/api/v1/admin/audit-logs")

    assert reparse.status_code == 202
    assert settings_response.status_code == 200
    assert audit.status_code == 200
    assert any(item["action"] == "settings.update" for item in audit.json()["items"])
    assert all("details_json" not in item for item in audit.json()["items"])


@pytest.mark.asyncio
async def test_admin_message_detail_replaces_invalid_utf8_preview(
    admin_client,
    runtime,
    seeded_message,
) -> None:
    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            "UPDATE messages SET text_preview = CAST(? AS TEXT) WHERE id = ?",
            (b"legacy-\xe4\xb8", seeded_message.message_id),
        )

    response = await admin_client.get(
        f"/api/v1/admin/messages/{seeded_message.message_id}"
    )

    assert response.status_code == 200
    assert response.json()["text_preview"] == "legacy-\ufffd"


@pytest.mark.asyncio
async def test_live_sse_api_key_enforces_ip_restrictions(app_client, runtime) -> None:
    key = await runtime.api_keys.create_key(
        name="live-ip-restricted",
        kind="admin",
        scopes=["live.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
        allowed_ip_cidrs=["203.0.113.0/24"],
    )

    response = await app_client.get(
        "/api/v1/admin/live/smtp/stream",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "api key ip not allowed"


@pytest.mark.asyncio
async def test_admin_api_mutation_succeeds_when_audit_logging_fails(admin_client, runtime) -> None:
    async def failing_audit_log(*args, **kwargs):
        raise RuntimeError("audit unavailable")

    runtime.audit.log = failing_audit_log

    response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={"max_recipients_per_message": "33"},
    )

    assert response.status_code == 200
    assert response.json()["max_recipients_per_message"] == 33


@pytest.mark.asyncio
async def test_admin_api_can_update_mailbox_visibility(admin_client, runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = runtime.mailboxes.list_mailboxes()["items"][0]
    response = await admin_client.patch(
        f"/api/v1/admin/mailboxes/{mailbox['id']}",
        json={"public_enabled": False, "is_hidden": True},
    )
    public_response = await admin_client.get("/mail/foo@adb.com")

    updated = runtime.mailboxes.get_mailbox(mailbox["id"])

    assert response.status_code == 200
    assert response.json()["public_enabled"] is False
    assert response.json()["is_hidden"] is True
    assert updated["public_enabled"] is False
    assert updated["is_hidden"] is True
    assert public_response.status_code == 404


@pytest.mark.asyncio
async def test_admin_api_can_create_and_revoke_api_keys_through_http(admin_client, runtime) -> None:
    await runtime.create_domain("adb.com")

    created = await admin_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "read-domains",
            "kind": "admin",
            "scopes": ["domains.read"],
        },
    )
    created_payload = created.json()
    auth_response = await admin_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": created_payload["plain_text"]},
    )
    revoked = await admin_client.post(f"/api/v1/admin/api-keys/{created_payload['id']}/revoke")
    revoked_response = await admin_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": created_payload["plain_text"]},
    )

    assert created.status_code == 201
    assert created_payload["status"] == "active"
    assert auth_response.status_code == 200
    assert revoked.status_code == 200
    assert revoked.json()["status"] == "revoked"
    assert revoked_response.status_code == 401


@pytest.mark.asyncio
async def test_admin_api_can_update_api_key_permissions_and_grants(admin_client, runtime) -> None:
    primary_domain = await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.create_domain("other.com", public_web_enabled=True, public_api_enabled=True)
    key = await runtime.api_keys.create_key(
        name="patch-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[primary_domain["id"]],
        mailbox_patterns=[],
    )

    denied_before = await admin_client.get(
        "/api/v1/public/mailboxes/foo@other.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )
    updated = await admin_client.patch(
        f"/api/v1/admin/api-keys/{key['id']}",
        json={
            "name": "patch-public-updated",
            "scopes": ["public.read"],
            "grant_all_domains": True,
            "allow_query": True,
            "rate_limit_per_min": 0,
        },
    )
    allowed_after = await admin_client.get(
        "/api/v1/public/mailboxes/foo@other.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )
    query_context = runtime.api_keys.authenticate_query(key["plain_text"])

    assert denied_before.status_code == 403
    assert updated.status_code == 200
    assert updated.json()["name"] == "patch-public-updated"
    assert updated.json()["domain_grant_mode"] == "all"
    assert updated.json()["domain_ids"] == []
    assert updated.json()["allow_query"] is True
    assert allowed_after.status_code == 200
    assert query_context.domain_ids == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query_string",
    [
        "?limit=-1",
        "?limit=1001",
        "?offset=-1",
    ],
)
async def test_admin_api_rejects_invalid_audit_pagination(admin_client, query_string: str) -> None:
    response = await admin_client.get(f"/api/v1/admin/audit-logs{query_string}")

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_admin_api_rejects_invalid_domain_payload(admin_client) -> None:
    response = await admin_client.post(
        "/api/v1/admin/domains",
        json={
            "root_domain": "bad.adb.com",
            "plus_addressing_mode": "bogus",
        },
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_admin_api_read_endpoints_accept_read_scopes(admin_client, runtime) -> None:
    await runtime.create_domain("adb.com")
    read_only_key = await runtime.api_keys.create_key(
        name="fixture-admin-read",
        kind="admin",
        scopes=["domains.read", "system.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
    )

    domains_response = await admin_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": read_only_key["plain_text"]},
    )
    settings_response = await admin_client.get(
        "/api/v1/admin/settings",
        headers={"X-API-Key": read_only_key["plain_text"]},
    )

    assert domains_response.status_code == 200
    domain = domains_response.json()["items"][0]
    assert domain["root_domain_ascii"] == "adb.com"
    assert isinstance(domain["accept_exact"], bool)
    assert isinstance(domain["public_web_enabled"], bool)
    assert isinstance(domain["is_active"], bool)
    assert settings_response.status_code == 200
    assert settings_response.json()["max_message_size_bytes"] == runtime.settings.max_message_size_bytes


@pytest.mark.asyncio
async def test_admin_mailbox_service_normalizes_boolean_flags(runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_mailbox_booleans",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_mailbox_booleans",
    )
    await runtime.drain_parser_queue()

    mailbox = runtime.mailboxes.list_mailboxes()["items"][0]

    assert mailbox["address_canonical"] == "foo@adb.com"
    assert isinstance(mailbox["public_enabled"], bool)
    assert isinstance(mailbox["is_hidden"], bool)
    assert mailbox["public_enabled"] is True
    assert mailbox["is_hidden"] is False


@pytest.mark.asyncio
async def test_admin_api_reparse_failure_clears_stale_parsed_fields(runtime, monkeypatch) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Attachment Mail\r\n"
        b"Message-ID: <attachment@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary99\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Attachment body.\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary99--\r\n"
    )
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_reparse_failure",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
        smtp_session_id="smtp_reparse_failure",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    message_id = mailbox["items"][0]["message_id"]

    with connect_database(runtime.settings.database_path) as connection:
        attachment_row = connection.execute(
            """
            SELECT storage_path
            FROM attachments
            WHERE message_id = ?
            """,
            (message_id,),
        ).fetchone()
    attachment_path = runtime.storage.resolve(attachment_row["storage_path"])
    assert attachment_path.exists() is True

    def failing_parse_message(*args, **kwargs):
        raise ValueError("parse failed")

    monkeypatch.setattr(runtime.parser, "parse_message", failing_parse_message)

    await runtime.messages.reparse_message(message_id)
    await runtime.drain_parser_queue()

    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT
                parse_status,
                parse_error,
                message_id_header,
                subject,
                from_name,
                from_addr,
                reply_to,
                date_header,
                has_text,
                has_html,
                has_attachments,
                attachment_count,
                text_preview,
                text_body_path,
                html_body_path,
                headers_json
            FROM messages
            WHERE id = ?
            """,
            (message_id,),
        ).fetchone()
        attachment_count = connection.execute(
            "SELECT COUNT(*) AS count FROM attachments WHERE message_id = ?",
            (message_id,),
        ).fetchone()

    assert attachment_path.exists() is False
    assert row["parse_status"] == "failed"
    assert row["parse_error"] == "parse failed"
    assert row["message_id_header"] is None
    assert row["subject"] is None
    assert row["from_name"] is None
    assert row["from_addr"] is None
    assert row["reply_to"] is None
    assert row["date_header"] is None
    assert row["has_text"] == 0
    assert row["has_html"] == 0
    assert row["has_attachments"] == 0
    assert row["attachment_count"] == 0
    assert row["text_preview"] is None
    assert row["text_body_path"] is None
    assert row["html_body_path"] is None
    assert row["headers_json"] is None
    assert attachment_count["count"] == 0


@pytest.mark.asyncio
async def test_admin_api_parse_failure_keeps_attachment_files_when_db_write_fails(runtime, monkeypatch) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Attachment Mail\r\n"
        b"Message-ID: <attachment-dberror@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary99\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Attachment body.\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary99--\r\n"
    )
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_reparse_db_failure",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
        smtp_session_id="smtp_reparse_db_failure",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    message_id = mailbox["items"][0]["message_id"]

    with connect_database(runtime.settings.database_path) as connection:
        attachment_row = connection.execute(
            """
            SELECT storage_path
            FROM attachments
            WHERE message_id = ?
            """,
            (message_id,),
        ).fetchone()
    attachment_path = runtime.storage.resolve(attachment_row["storage_path"])
    assert attachment_path.exists() is True

    def failing_parse_message(*args, **kwargs):
        raise ValueError("parse failed")

    async def failing_execute(operation):
        with connect_database(runtime.settings.database_path) as connection:
            operation(connection)
            connection.rollback()
        raise RuntimeError("db write failed")

    monkeypatch.setattr(runtime.parser, "parse_message", failing_parse_message)
    monkeypatch.setattr(runtime.writer, "execute", failing_execute)

    with pytest.raises(RuntimeError, match="db write failed"):
        await runtime._parse_message(ParseTask(message_id=message_id, raw_size_bytes=1))

    assert attachment_path.exists() is True


@pytest.mark.asyncio
async def test_admin_api_successful_reparse_replaces_attachment_files(runtime) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Attachment Mail\r\n"
        b"Message-ID: <attachment-success@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary99\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Attachment body.\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary99--\r\n"
    )
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_reparse_success",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
        smtp_session_id="smtp_reparse_success",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    message_id = mailbox["items"][0]["message_id"]

    with connect_database(runtime.settings.database_path) as connection:
        attachment_row = connection.execute(
            """
            SELECT storage_path
            FROM attachments
            WHERE message_id = ?
            """,
            (message_id,),
        ).fetchone()
    old_attachment_path = runtime.storage.resolve(attachment_row["storage_path"])
    assert old_attachment_path.exists() is True

    def successful_parse_message(*args, **kwargs):
        return ParsedMessage(
            message_id_header="<reparsed@example.com>",
            subject="Reparsed Attachment Mail",
            from_name="Sender",
            from_addr="sender@example.com",
            reply_to=None,
            date_header="Sat, 18 Apr 2026 20:00:00 +0000",
            has_text=True,
            has_html=False,
            has_attachments=False,
            attachment_count=0,
            text_preview="Reparsed attachment body.",
            text_body_path=None,
            html_body_path=None,
            headers_json="[]",
            attachments=[],
        )

    runtime.parser.parse_message = successful_parse_message

    await runtime.messages.reparse_message(message_id)
    await runtime.drain_parser_queue()

    with connect_database(runtime.settings.database_path) as connection:
        attachment_count = connection.execute(
            "SELECT COUNT(*) AS count FROM attachments WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        row = connection.execute(
            "SELECT parse_status, subject FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()

    assert old_attachment_path.exists() is False
    assert attachment_count["count"] == 0
    assert row["parse_status"] == "parsed"
    assert row["subject"] == "Reparsed Attachment Mail"


@pytest.mark.asyncio
async def test_admin_api_attachment_cleanup_errors_do_not_stop_later_reparses(runtime, monkeypatch) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Attachment Mail\r\n"
        b"Message-ID: <attachment-cleanup-error@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary99\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Attachment body.\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary99--\r\n"
    )
    second_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Plain Mail\r\n"
        b"Message-ID: <plain-cleanup-error@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:05:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Plain message body.\r\n"
    )
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_cleanup_error",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
        smtp_session_id="smtp_cleanup_error",
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=second_email_bytes,
        smtp_session_id="smtp_cleanup_error",
    )
    await runtime.drain_parser_queue()

    with connect_database(runtime.settings.database_path) as connection:
        first_message_id = connection.execute(
            "SELECT id FROM messages WHERE subject = ?",
            ("Attachment Mail",),
        ).fetchone()["id"]
        second_message_id = connection.execute(
            "SELECT id FROM messages WHERE subject = ?",
            ("Plain Mail",),
        ).fetchone()["id"]
        first_attachment_row = connection.execute(
            "SELECT storage_path FROM attachments WHERE message_id = ?",
            (first_message_id,),
        ).fetchone()
    first_attachment_path = runtime.storage.resolve(first_attachment_row["storage_path"])

    original_unlink = Path.unlink

    def failing_unlink(self, missing_ok: bool = False):  # type: ignore[override]
        if self == first_attachment_path:
            raise RuntimeError("cleanup failed")
        return original_unlink(self, missing_ok=missing_ok)

    def reparsed_message(message_id: str) -> ParsedMessage:
        return ParsedMessage(
            message_id_header=f"<reparsed-{message_id}@example.com>",
            subject=f"reparsed-{message_id}",
            from_name="Sender",
            from_addr="sender@example.com",
            reply_to=None,
            date_header="Sat, 18 Apr 2026 20:00:00 +0000",
            has_text=True,
            has_html=False,
            has_attachments=False,
            attachment_count=0,
            text_preview=f"reparsed-{message_id}",
            text_body_path=None,
            html_body_path=None,
            headers_json="[]",
            attachments=[],
        )

    monkeypatch.setattr(Path, "unlink", failing_unlink)
    monkeypatch.setattr(runtime.parser, "parse_message", lambda message_id, raw_content, received_at: reparsed_message(message_id))

    await runtime.messages.reparse_message(first_message_id)
    await runtime.messages.reparse_message(second_message_id)
    await asyncio.wait_for(runtime.drain_parser_queue(), timeout=2)

    with connect_database(runtime.settings.database_path) as connection:
        first_row = connection.execute(
            "SELECT subject FROM messages WHERE id = ?",
            (first_message_id,),
        ).fetchone()
        second_row = connection.execute(
            "SELECT subject FROM messages WHERE id = ?",
            (second_message_id,),
        ).fetchone()

    assert first_attachment_path.exists() is True
    assert first_row["subject"] == f"reparsed-{first_message_id}"
    assert second_row["subject"] == f"reparsed-{second_message_id}"


@pytest.mark.asyncio
async def test_admin_api_settings_update_applies_live_message_size_limit(admin_client, runtime) -> None:
    await runtime.update_settings({"max_message_size_bytes": 10})
    await runtime.create_domain("adb.com")
    handler = RapidInboxHandler(runtime)
    session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None)
    envelope = SimpleNamespace(
        rcpt_tos=[],
        mail_from="sender@example.com",
        content=(
            b"From: Sender <sender@example.com>\r\n"
            b"To: Foo <foo@adb.com>\r\n"
            b"Subject: Live settings\r\n"
            b"\r\n"
            b"hello world\r\n"
        ),
    )

    await handler.handle_RCPT(None, session, envelope, "foo@adb.com", [])
    rejected = await handler.handle_DATA(None, session, envelope)
    settings_response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={"max_message_size_bytes": "100"},
    )
    accepted = await handler.handle_DATA(None, session, envelope)

    await runtime.drain_parser_queue()

    assert rejected == "552 message too large"
    assert settings_response.status_code == 200
    assert runtime.settings.max_message_size_bytes == 100
    assert accepted.startswith("250 queued as ")


@pytest.mark.asyncio
async def test_admin_api_settings_reload_from_database_on_restart(tmp_path) -> None:
    storage_root = tmp_path / "storage"
    database_path = storage_root / "app.db"
    settings = Settings(
        storage_root=storage_root,
        database_path=database_path,
    )
    app = create_app(settings=settings)

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            admin_key = await app.state.runtime.api_keys.create_key(
                name="restart-admin",
                kind="admin",
                scopes=["system.write"],
                domain_ids=[],

                domain_grant_mode="all",
                mailbox_patterns=[],
            )
            response = await client.patch(
                "/api/v1/admin/settings",
                headers={"X-API-Key": admin_key["plain_text"]},
                json={"max_message_size_bytes": "10"},
            )

        assert response.status_code == 200

    restarted_settings = Settings(
        storage_root=storage_root,
        database_path=database_path,
    )
    restarted_app = create_app(settings=restarted_settings)

    async with restarted_app.router.lifespan_context(restarted_app):
        assert restarted_app.state.runtime.settings.max_message_size_bytes == 10
        await restarted_app.state.runtime.create_domain("adb.com")
        handler = RapidInboxHandler(restarted_app.state.runtime)
        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None)
        envelope = SimpleNamespace(
            rcpt_tos=[],
            mail_from="sender@example.com",
            content=b"01234567890",
        )

        await handler.handle_RCPT(None, session, envelope, "foo@adb.com", [])
        result = await handler.handle_DATA(None, session, envelope)

        assert result == "552 message too large"


@pytest.mark.asyncio
async def test_admin_api_settings_allowlist_rejects_unsupported_keys_and_filters_echo(admin_client, runtime) -> None:
    rejected = await admin_client.patch(
        "/api/v1/admin/settings",
        json={"admin_token": "super-secret"},
    )

    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            """
            INSERT OR REPLACE INTO system_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            """,
            ("admin_token", "super-secret", "2026-04-18T00:00:00Z"),
        )

    listed = await admin_client.get("/api/v1/admin/settings")

    assert rejected.status_code == 422
    assert "admin_token" not in listed.json()
    assert set(listed.json()) == SettingsService.SUPPORTED_SETTINGS


@pytest.mark.asyncio
async def test_admin_api_rejects_fractional_settings_values(admin_client) -> None:
    response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={"max_recipients_per_message": 12.5},
    )

    assert response.status_code == 422


@pytest.mark.asyncio
async def test_admin_api_allows_zero_to_disable_smtp_connection_limits(admin_client, runtime) -> None:
    response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={
            "smtp_max_concurrent_connections": 0,
            "smtp_connection_rate_limit_count": 0,
        },
    )

    assert response.status_code == 200
    assert response.json()["smtp_max_concurrent_connections"] == 0
    assert response.json()["smtp_connection_rate_limit_count"] == 0
    assert runtime.settings.smtp_max_concurrent_connections == 0
    assert runtime.settings.smtp_connection_rate_limit_count == 0


@pytest.mark.asyncio
async def test_admin_api_mirrors_domain_mailbox_message_session_and_key_rotate(
    admin_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    created = await admin_client.post(
        "/api/v1/admin/domains",
        json={"root_domain": "mirror.test", "accept_subdomains": True},
    )
    domain_id = created.json()["id"]
    patched = await admin_client.patch(
        f"/api/v1/admin/domains/{domain_id}",
        json={"public_api_enabled": False, "accept_exact": False},
    )
    domain_detail = await admin_client.get(f"/api/v1/admin/domains/{domain_id}")

    assert created.status_code == 201
    assert patched.status_code == 200
    assert patched.json()["public_api_enabled"] is False
    assert patched.json()["accept_exact"] is False
    assert domain_detail.status_code == 200
    assert domain_detail.json()["dns_recommendations"]

    await admin_client.patch(
        f"/api/v1/admin/domains/{domain_id}",
        json={"public_api_enabled": True, "accept_exact": True},
    )
    await runtime.ensure_smtp_session(
        "smtp_admin_mirror",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@mirror.test"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_admin_mirror",
    )
    await runtime.drain_parser_queue()

    mailboxes = await admin_client.get("/api/v1/admin/mailboxes?q=foo@mirror.test")
    mailbox_id = mailboxes.json()["items"][0]["id"]
    mailbox_detail = await admin_client.get(f"/api/v1/admin/mailboxes/{mailbox_id}")
    messages = await admin_client.get(f"/api/v1/admin/messages?mailbox_id={mailbox_id}")
    message_id = messages.json()["items"][0]["id"]
    message_detail = await admin_client.get(f"/api/v1/admin/messages/{message_id}")
    raw = await admin_client.get(f"/api/v1/admin/messages/{message_id}/raw")
    session_list = await admin_client.get("/api/v1/admin/smtp-sessions")
    session_detail = await admin_client.get("/api/v1/admin/smtp-sessions/smtp_admin_mirror")
    delivery_id = message_detail.json()["deliveries"][0]["delivery_id"]
    deleted = await admin_client.delete(f"/api/v1/admin/messages/{message_id}/deliveries/{delivery_id}")
    audit = await admin_client.get("/api/v1/admin/audit-logs?action=deliveries.delete")

    assert mailboxes.status_code == 200
    assert mailbox_detail.status_code == 200
    assert mailbox_detail.json()["deliveries"][0]["delivery_id"] == delivery_id
    assert messages.status_code == 200
    assert message_detail.status_code == 200
    assert message_detail.json()["subject"] == "Hello Rapid Inbox"
    assert raw.status_code == 200
    assert raw.headers["content-type"] == "message/rfc822"
    assert session_list.status_code == 200
    assert session_detail.status_code == 200
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] == 1
    assert audit.status_code == 200
    assert audit.json()["items"][0]["action"] == "deliveries.delete"

    key = await runtime.api_keys.create_key(
        name="rotate-me",
        kind="admin",
        scopes=["domains.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    key_detail = await admin_client.get(f"/api/v1/admin/api-keys/{key['id']}")
    rotated = await admin_client.post(f"/api/v1/admin/api-keys/{key['id']}/rotate")
    old_response = await admin_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": key["plain_text"]},
    )
    new_response = await admin_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": rotated.json()["plain_text"]},
    )

    assert key_detail.status_code == 200
    assert "plain_text" not in key_detail.json()
    assert rotated.status_code == 200
    assert rotated.json()["plain_text"] != key["plain_text"]
    assert old_response.status_code == 401
    assert new_response.status_code == 200


@pytest.mark.asyncio
async def test_smtp_runtime_protection_settings_apply_to_handler(
    admin_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    await runtime.create_domain("adb.com")
    settings_response = await admin_client.patch(
        "/api/v1/admin/settings",
        json={
            "smtp_max_concurrent_connections": 1,
            "smtp_connection_rate_limit_count": 1,
            "smtp_connection_rate_limit_window_seconds": 60,
            "smtp_idle_timeout_seconds": 120,
            "disk_warning_threshold_percent": 90,
        },
    )
    handler = RapidInboxHandler(runtime)
    first_session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None)
    first_envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)
    second_session = SimpleNamespace(peer=("127.0.0.2", 2526), host_name="pytest", ssl=None)
    second_envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)

    first = await handler.handle_RCPT(None, first_session, first_envelope, "foo@adb.com", [])
    second = await handler.handle_RCPT(None, second_session, second_envelope, "bar@adb.com", [])

    assert settings_response.status_code == 200
    assert runtime.settings.smtp_idle_timeout_seconds == 120
    assert first == "250 OK"
    assert second == "421 concurrent connection limit exceeded"
