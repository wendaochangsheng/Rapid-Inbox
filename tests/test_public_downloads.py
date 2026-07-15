from __future__ import annotations

import asyncio
import threading
import time

import pytest

import app.runtime as runtime_module


def _attachment_message(subject: str = "Attachment Test") -> bytes:
    return (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        + f"Subject: {subject}\r\n".encode()
        + f"Message-ID: <{subject.lower().replace(' ', '-')}@example.com>\r\n".encode()
        + b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary-hotpath\r\n"
        b"\r\n"
        b"--boundary-hotpath\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Body text.\r\n"
        b"\r\n"
        b"--boundary-hotpath\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary-hotpath--\r\n"
    )


async def _assert_timer_runs_while(task: asyncio.Task) -> None:
    await asyncio.sleep(0.02)
    assert not task.done(), "synchronous read blocked the event loop until completion"
    await task


@pytest.mark.asyncio
async def test_public_message_routes_serve_raw_attachment_and_html_frame(app_client, seeded_message) -> None:
    raw_response = await app_client.get(f"/mail/foo@adb.com/{seeded_message.delivery_id}/raw")
    html_response = await app_client.get(f"/mail/foo@adb.com/{seeded_message.delivery_id}/html")
    api_response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{seeded_message.delivery_id}/raw",
        headers={"X-API-Key": seeded_message.public_api_key},
    )

    assert raw_response.status_code == 200
    assert raw_response.headers["content-type"] == "message/rfc822"
    assert "sandbox" in html_response.text
    assert "Content-Security-Policy" in html_response.text
    assert "about:srcdoc" in html_response.text
    assert api_response.status_code == 200


@pytest.mark.asyncio
async def test_public_message_routes_serve_attachments(app_client, runtime) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Attachment Test\r\n"
        b"Message-ID: <attachment@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundary99\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Body text.\r\n"
        b"\r\n"
        b"--boundary99\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundary99--\r\n"
    )

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    public_key = await runtime.api_keys.create_key(
        name="attachment-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    detail = await runtime.get_delivery_detail("foo@adb.com", delivery_id)
    attachment_id = detail["attachments"][0]["id"]

    raw_response = await app_client.get(f"/mail/foo@adb.com/{delivery_id}/attachments/{attachment_id}")
    api_response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/attachments/{attachment_id}",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert raw_response.status_code == 200
    assert raw_response.content.startswith(b"attachment contents")
    assert raw_response.headers["content-disposition"].startswith("attachment;")
    assert raw_response.headers["x-content-type-options"] == "nosniff"
    assert api_response.status_code == 200
    assert api_response.content.startswith(b"attachment contents")
    assert api_response.headers["content-disposition"].startswith("attachment;")
    assert api_response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.asyncio
async def test_public_attachment_routes_allow_inline_only_for_safe_raster_images(app_client, runtime) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Inline Allowlist Test\r\n"
        b"Message-ID: <inline-allowlist@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/related; boundary=boundaryallow\r\n"
        b"\r\n"
        b"--boundaryallow\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Body text.\r\n"
        b"\r\n"
        b"--boundaryallow\r\n"
        b"Content-Type: image/png\r\n"
        b"Content-Disposition: inline; filename=\"hero.png\"\r\n"
        b"Content-ID: <hero-png>\r\n"
        b"\r\n"
        b"png-bytes\r\n"
        b"\r\n"
        b"--boundaryallow\r\n"
        b"Content-Type: image/svg+xml\r\n"
        b"Content-Disposition: inline; filename=\"evil.svg\"\r\n"
        b"Content-ID: <hero-svg>\r\n"
        b"\r\n"
        b"<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>\r\n"
        b"\r\n"
        b"--boundaryallow--\r\n"
    )

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    public_key = await runtime.api_keys.create_key(
        name="inline-allowlist-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    detail = await runtime.get_delivery_detail("foo@adb.com", delivery_id)
    png_attachment_id = next(attachment["id"] for attachment in detail["attachments"] if attachment["content_type"] == "image/png")
    svg_attachment_id = next(attachment["id"] for attachment in detail["attachments"] if attachment["content_type"] == "image/svg+xml")

    png_response = await app_client.get(f"/mail/foo@adb.com/{delivery_id}/attachments/{png_attachment_id}")
    svg_response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/attachments/{svg_attachment_id}",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert png_response.status_code == 200
    assert png_response.headers["content-disposition"].startswith("inline;")
    assert png_response.headers["x-content-type-options"] == "nosniff"
    assert svg_response.status_code == 200
    assert svg_response.headers["content-disposition"].startswith("attachment;")
    assert svg_response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mailbox_updates",
    [
        {"public_enabled": False},
        {"is_hidden": True},
    ],
)
async def test_public_download_routes_respect_mailbox_visibility_flags(
    app_client,
    runtime,
    mailbox_updates: dict[str, bool],
) -> None:
    attachment_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Visibility Test\r\n"
        b"Message-ID: <visibility@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=boundaryvis\r\n"
        b"\r\n"
        b"--boundaryvis\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Body text.\r\n"
        b"\r\n"
        b"--boundaryvis\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b'Content-Disposition: attachment; filename="report.txt"\r\n'
        b"\r\n"
        b"attachment contents\r\n"
        b"\r\n"
        b"--boundaryvis--\r\n"
    )

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=attachment_email_bytes,
    )
    await runtime.drain_parser_queue()
    public_key = await runtime.api_keys.create_key(
        name="download-visibility-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    mailbox = runtime.mailboxes.list_mailboxes()["items"][0]
    mailbox_view = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = mailbox_view["items"][0]["delivery_id"]
    detail = await runtime.get_delivery_detail("foo@adb.com", delivery_id)
    attachment_id = detail["attachments"][0]["id"]
    await runtime.mailboxes.update_mailbox(mailbox["id"], mailbox_updates)

    web_raw = await app_client.get(f"/mail/foo@adb.com/{delivery_id}/raw")
    api_raw = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/raw",
        headers={"X-API-Key": public_key["plain_text"]},
    )
    web_attachment = await app_client.get(f"/mail/foo@adb.com/{delivery_id}/attachments/{attachment_id}")
    api_attachment = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/attachments/{attachment_id}",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert web_raw.status_code == 404
    assert api_raw.status_code == 404
    assert web_attachment.status_code == 404
    assert api_attachment.status_code == 404


@pytest.mark.asyncio
async def test_public_web_routes_respect_public_web_enabled_flag(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("web-disabled.adb.com", public_web_enabled=False, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@web-disabled.adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@web-disabled.adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    public_key = await runtime.api_keys.create_key(
        name="web-disabled-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@web-disabled.adb.com"],
    )

    web_response = await app_client.get(f"/mail/foo@web-disabled.adb.com/{delivery_id}/raw")
    api_response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@web-disabled.adb.com/messages/{delivery_id}/raw",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert web_response.status_code == 404
    assert api_response.status_code == 200


@pytest.mark.asyncio
async def test_public_api_routes_respect_public_api_enabled_flag(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("api-disabled.adb.com", public_web_enabled=True, public_api_enabled=False)
    await runtime.accept_message(
        rcpt_tos=["foo@api-disabled.adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@api-disabled.adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    public_key = await runtime.api_keys.create_key(
        name="api-disabled-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@api-disabled.adb.com"],
    )

    web_response = await app_client.get(f"/mail/foo@api-disabled.adb.com/{delivery_id}/raw")
    api_response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@api-disabled.adb.com/messages/{delivery_id}/raw",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert web_response.status_code == 200
    assert api_response.status_code == 404


@pytest.mark.asyncio
async def test_public_html_frame_rewrites_cid_references_to_attachment_routes(app_client, runtime) -> None:
    cid_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: CID Rewrite\r\n"
        b"Message-ID: <cid@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/related; boundary=boundarycid\r\n"
        b"\r\n"
        b"--boundarycid\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><img src=\"cid:hero-image\" alt=\"Hero\"></body></html>\r\n"
        b"\r\n"
        b"--boundarycid\r\n"
        b"Content-Type: image/png\r\n"
        b"Content-Disposition: inline; filename=\"hero.png\"\r\n"
        b"Content-ID: <hero-image>\r\n"
        b"\r\n"
        b"png-bytes\r\n"
        b"\r\n"
        b"--boundarycid--\r\n"
    )

    await runtime.create_domain("cid.adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@cid.adb.com"],
        envelope_from="sender@example.com",
        content=cid_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@cid.adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    detail = await runtime.get_delivery_detail("foo@cid.adb.com", delivery_id)
    attachment_id = detail["attachments"][0]["id"]

    html_response = await app_client.get(f"/mail/foo@cid.adb.com/{delivery_id}/html")

    assert html_response.status_code == 200
    assert "data:image/png;base64," in html_response.text
    assert f"/mail/foo@cid.adb.com/{delivery_id}/attachments/{attachment_id}" not in html_response.text
    assert "Content-Security-Policy" in html_response.text
    assert "about:srcdoc" in html_response.text


@pytest.mark.asyncio
async def test_public_html_frame_rewrites_cid_references_without_attachment_filename(app_client, runtime) -> None:
    cid_email_bytes = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: CID Rewrite No Filename\r\n"
        b"Message-ID: <cid-nofilename@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/related; boundary=boundarycidnofile\r\n"
        b"\r\n"
        b"--boundarycidnofile\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><img src=\"cid:hero-image\" alt=\"Hero\"></body></html>\r\n"
        b"\r\n"
        b"--boundarycidnofile\r\n"
        b"Content-Type: image/png\r\n"
        b"Content-Disposition: inline\r\n"
        b"Content-ID: <hero-image>\r\n"
        b"\r\n"
        b"png-bytes\r\n"
        b"\r\n"
        b"--boundarycidnofile--\r\n"
    )

    await runtime.create_domain("cid-nofilename.adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@cid-nofilename.adb.com"],
        envelope_from="sender@example.com",
        content=cid_email_bytes,
    )
    await runtime.drain_parser_queue()

    mailbox = await runtime.get_mailbox_view("foo@cid-nofilename.adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]
    detail = await runtime.get_delivery_detail("foo@cid-nofilename.adb.com", delivery_id)
    attachment = detail["attachments"][0]

    html_response = await app_client.get(f"/mail/foo@cid-nofilename.adb.com/{delivery_id}/html")

    assert html_response.status_code == 200
    assert attachment["safe_filename"].endswith(".png")
    assert attachment["safe_filename"] != "attachment.bin"
    assert "data:image/png;base64," in html_response.text
    assert "Content-Security-Policy" in html_response.text
    assert "about:srcdoc" in html_response.text


@pytest.mark.asyncio
async def test_public_sqlite_and_body_reads_do_not_block_event_loop_timer(
    runtime,
    monkeypatch,
    sample_email_bytes: bytes,
) -> None:
    await runtime.create_domain("hotpath.adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@hotpath.adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@hotpath.adb.com")
    delivery_id = str(mailbox["items"][0]["delivery_id"])

    real_connect_database = runtime_module.connect_database
    sqlite_entered = threading.Event()

    def slow_connect_database(*args, **kwargs):
        sqlite_entered.set()
        time.sleep(0.15)
        return real_connect_database(*args, **kwargs)

    monkeypatch.setattr(runtime_module, "connect_database", slow_connect_database)
    list_task = asyncio.create_task(
        runtime.messages.get_public_mailbox_view(
            "foo@hotpath.adb.com",
            surface="web",
        )
    )
    while not sqlite_entered.is_set():
        await asyncio.sleep(0)
    await _assert_timer_runs_while(list_task)

    monkeypatch.setattr(runtime_module, "connect_database", real_connect_database)
    real_read_text_preview = runtime.storage.read_text_preview
    body_read_entered = threading.Event()

    def slow_read_text_preview(relative_path, max_bytes):
        body_read_entered.set()
        time.sleep(0.15)
        return real_read_text_preview(relative_path, max_bytes)

    monkeypatch.setattr(runtime.storage, "read_text_preview", slow_read_text_preview)
    detail_task = asyncio.create_task(
        runtime.messages.get_public_delivery_detail(
            "foo@hotpath.adb.com",
            delivery_id,
            surface="web",
        )
    )
    while not body_read_entered.is_set():
        await asyncio.sleep(0)
    await _assert_timer_runs_while(detail_task)


@pytest.mark.asyncio
async def test_public_raw_and_attachment_downloads_never_load_message_bodies(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    message_bytes = _attachment_message("No Body Download")
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=message_bytes,
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = str(mailbox["items"][0]["delivery_id"])
    detail = await runtime.get_delivery_detail("foo@adb.com", delivery_id)
    attachment_id = str(detail["attachments"][0]["id"])
    public_key = await runtime.api_keys.create_key(
        name="body-free-download",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    def body_read_forbidden(*_args, **_kwargs):
        raise AssertionError("download path must not read text/html bodies")

    monkeypatch.setattr(runtime.storage, "read_text", body_read_forbidden)
    monkeypatch.setattr(runtime.storage, "read_text_preview", body_read_forbidden)
    monkeypatch.setattr(runtime, "_get_delivery_detail_sync", body_read_forbidden)
    bearer = {"Authorization": f"Bearer {public_key['plain_text']}"}
    v1_key = {"X-API-Key": public_key["plain_text"]}
    responses = [
        await app_client.get(f"/mail/foo@adb.com/{delivery_id}/raw"),
        await app_client.get(f"/mail/foo@adb.com/{delivery_id}/attachments/{attachment_id}"),
        await app_client.get(
            f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/raw",
            headers=v1_key,
        ),
        await app_client.get(
            f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/attachments/{attachment_id}",
            headers=v1_key,
        ),
        await app_client.get(
            f"/api/v2/public/mailboxes/foo@adb.com/messages/{delivery_id}/raw",
            headers=bearer,
        ),
        await app_client.get(
            f"/api/v2/public/mailboxes/foo@adb.com/messages/{delivery_id}/attachments/{attachment_id}",
            headers=bearer,
        ),
    ]

    assert [response.status_code for response in responses] == [200] * 6
    assert responses[0].content == message_bytes
    assert responses[2].content == message_bytes
    assert responses[4].content == message_bytes
    assert responses[1].content.startswith(b"attachment contents")
    assert responses[3].content.startswith(b"attachment contents")
    assert responses[5].content.startswith(b"attachment contents")


@pytest.mark.asyncio
async def test_public_downloads_fail_closed_before_resolving_cross_mailbox_paths(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com", "bar@adb.com"],
        envelope_from="sender@example.com",
        content=_attachment_message("Cross Mailbox"),
    )
    await runtime.drain_parser_queue()
    foo_view = await runtime.get_mailbox_view("foo@adb.com")
    bar_view = await runtime.get_mailbox_view("bar@adb.com")
    bar_delivery_id = str(bar_view["items"][0]["delivery_id"])
    detail = await runtime.get_delivery_detail("foo@adb.com", str(foo_view["items"][0]["delivery_id"]))
    attachment_id = str(detail["attachments"][0]["id"])
    public_key = await runtime.api_keys.create_key(
        name="foo-only-download",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    resolved_paths: list[str] = []

    def forbidden_resolve(relative_path: str):
        resolved_paths.append(relative_path)
        raise AssertionError("unauthorized resource path was resolved")

    monkeypatch.setattr(runtime.storage, "resolve", forbidden_resolve)
    v1_key = {"X-API-Key": public_key["plain_text"]}
    bearer = {"Authorization": f"Bearer {public_key['plain_text']}"}
    responses = [
        await app_client.get(
            f"/api/v1/public/mailboxes/foo@adb.com/messages/{bar_delivery_id}/raw",
            headers=v1_key,
        ),
        await app_client.get(
            f"/api/v1/public/mailboxes/foo@adb.com/messages/{bar_delivery_id}/attachments/{attachment_id}",
            headers=v1_key,
        ),
        await app_client.get(
            f"/api/v2/public/mailboxes/foo@adb.com/messages/{bar_delivery_id}/raw",
            headers=bearer,
        ),
        await app_client.get(
            f"/api/v2/public/mailboxes/foo@adb.com/messages/{bar_delivery_id}/attachments/{attachment_id}",
            headers=bearer,
        ),
    ]

    assert [response.status_code for response in responses] == [404] * 4
    assert resolved_paths == []
