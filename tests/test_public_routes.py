from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from email.policy import SMTP
from itertools import count

import httpx
import pytest

from app.config import default_settings
import app.http.public_views as public_views_module
import app.runtime as runtime_module
from app.main import create_app
from app.services.messages import MessageService


def _mail_bytes(subject: str, message_id: str, body: str) -> bytes:
    return (
        "From: Sender <sender@example.com>\r\n"
        "To: Foo <foo@adb.com>\r\n"
        f"Subject: {subject}\r\n"
        f"Message-ID: <{message_id}>\r\n"
        "Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        f"{body}\r\n"
    ).encode("utf-8")


@pytest.mark.asyncio
async def test_public_template_rendering_does_not_block_event_loop(app_client, monkeypatch) -> None:
    original_render = public_views_module._render_template
    started = threading.Event()

    def slow_render(*args, **kwargs):
        started.set()
        time.sleep(0.2)
        return original_render(*args, **kwargs)

    monkeypatch.setattr(public_views_module, "_render_template", slow_render)
    request_task = asyncio.create_task(app_client.get("/"))

    assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1.0)
    assert not request_task.done()
    await asyncio.sleep(0)
    response = await request_task

    assert response.status_code == 200


def _rich_mail_bytes(
    *,
    subject: str,
    message_id: str,
    from_addr: str,
    body: str,
    subtype: str = "plain",
) -> bytes:
    message = EmailMessage()
    message["From"] = from_addr
    message["To"] = "Foo <foo@adb.com>"
    message["Subject"] = subject
    message["Message-ID"] = f"<{message_id}>"
    message["Date"] = "Sat, 18 Apr 2026 20:00:00 +0000"
    if subtype == "html":
        message.set_content(body, subtype="html")
    else:
        message.set_content(body)
    return message.as_bytes(policy=SMTP)


def _patch_sequenced_utc_now(monkeypatch) -> None:
    base = datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc)
    ticks = count()

    monkeypatch.setattr(
        runtime_module,
        "utc_now",
        lambda: (base + timedelta(seconds=next(ticks))).isoformat().replace("+00:00", "Z"),
    )


@pytest.mark.asyncio
async def test_public_home_page_exposes_mailbox_entry_point(app_client) -> None:
    response = await app_client.get("/")

    assert response.status_code == 200
    assert "一个地址" in response.text
    assert "公开邮件" in response.text
    assert "立即进入" in response.text


@pytest.mark.asyncio
async def test_mailbox_page_and_public_api_show_received_message(tmp_path, sample_email_bytes: bytes) -> None:
    settings = default_settings(tmp_path)
    app = create_app(settings=settings)

    async with app.router.lifespan_context(app):
        runtime = app.state.runtime
        await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        mailbox = await runtime.get_mailbox_view("foo@adb.com")
        delivery_id = mailbox["items"][0]["delivery_id"]
        public_key = await runtime.api_keys.create_key(
            name="public-route-fixture",
            kind="public",
            scopes=["public.read"],
            domain_ids=[],

            domain_grant_mode="all",
            mailbox_patterns=["foo@adb.com"],
        )

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            page = await client.get("/mail/foo@adb.com")
            detail = await client.get(f"/mail/foo@adb.com/{delivery_id}")
            api = await client.get(
                "/api/v1/public/mailboxes/foo@adb.com/messages",
                headers={"X-API-Key": public_key["plain_text"]},
            )

        assert page.status_code == 200
        assert "Hello Rapid Inbox" in page.text
        assert detail.status_code == 200
        assert "sender@example.com" in detail.text
        assert api.status_code == 200
        assert api.json()["items"][0]["delivery_id"] == delivery_id


@pytest.mark.asyncio
async def test_public_message_page_displays_shanghai_time(app_client, runtime, monkeypatch, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]

    response = await app_client.get(f"/mail/foo@adb.com/{delivery_id}")

    assert response.status_code == 200
    assert "2026-04-19 04:00:00" in response.text


@pytest.mark.asyncio
async def test_public_mailbox_page_exposes_pagination_links(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Oldest", "oldest@example.com", "oldest"),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Middle", "middle@example.com", "middle"),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Newest", "newest@example.com", "newest"),
    )
    await runtime.drain_parser_queue()

    first_page = await app_client.get("/mail/foo@adb.com?limit=1&offset=0")
    second_page = await app_client.get("/mail/foo@adb.com?limit=1&offset=1")

    assert first_page.status_code == 200
    assert "Newest" in first_page.text
    assert "?limit=1&offset=1" in first_page.text
    assert "?limit=1&offset=2" in first_page.text
    assert 'aria-label="第 3 页"' in first_page.text
    assert second_page.status_code == 200
    assert "Middle" in second_page.text
    assert "?limit=1&offset=0" in second_page.text


@pytest.mark.asyncio
async def test_public_mailbox_page_defaults_to_twenty_results(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    for index in range(21):
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=_mail_bytes(f"Subject {index:02d}", f"default-{index:02d}@example.com", f"body-{index:02d}"),
        )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert "?limit=20&offset=20" in response.text
    assert "Subject 20" in response.text
    assert "Subject 00" not in response.text


@pytest.mark.asyncio
async def test_public_mailbox_api_returns_pagination_metadata(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Oldest", "oldest-api@example.com", "oldest"),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Middle", "middle-api@example.com", "middle"),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Newest", "newest-api@example.com", "newest"),
    )
    await runtime.drain_parser_queue()
    public_key = await runtime.api_keys.create_key(
        name="pagination-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    first_page = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages?limit=1&offset=0",
        headers={"X-API-Key": public_key["plain_text"]},
    )
    second_page = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages?limit=1&offset=1",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert first_page.status_code == 200
    assert first_page.json()["limit"] == 1
    assert first_page.json()["offset"] == 0
    assert first_page.json()["next_offset"] == 1
    assert first_page.json()["previous_offset"] is None
    assert first_page.json()["has_next"] is True
    assert first_page.json()["has_previous"] is False
    assert first_page.json()["items"][0]["subject"] == "Newest"
    assert second_page.status_code == 200
    assert second_page.json()["offset"] == 1
    assert second_page.json()["previous_offset"] == 0
    assert second_page.json()["has_previous"] is True
    assert second_page.json()["items"][0]["subject"] == "Middle"


@pytest.mark.asyncio
async def test_public_mailbox_api_supports_delivery_cursor_pagination(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    for subject in ("Oldest", "Middle", "Newest"):
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=_mail_bytes(subject, f"cursor-{subject.lower()}@example.com", subject),
        )
    await runtime.drain_parser_queue()
    public_key = await runtime.api_keys.create_key(
        name="cursor-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    first_page = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages?limit=1",
        headers={"X-API-Key": public_key["plain_text"]},
    )
    cursor = first_page.json()["next_cursor"]
    second_page = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages?limit=1&cursor={cursor}",
        headers={"X-API-Key": public_key["plain_text"]},
    )
    encoded, signature = cursor.split(".", 1)
    tampered = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        params={
            "limit": 1,
            "cursor": f"{encoded}.{'A' if signature[0] != 'A' else 'B'}{signature[1:]}",
        },
        headers={"X-API-Key": public_key["plain_text"]},
    )
    cross_mailbox = await app_client.get(
        "/api/v1/public/mailboxes/bar@adb.com/messages",
        params={"limit": 1, "cursor": cursor},
        headers={"X-API-Key": public_key["plain_text"]},
    )
    oversized = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        params={"cursor": "A" * 2049},
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert first_page.status_code == 200
    assert first_page.json()["items"][0]["subject"] == "Newest"
    assert first_page.json()["pagination"]["mode"] == "offset"
    assert cursor
    assert "." in cursor
    assert second_page.status_code == 200
    assert second_page.json()["pagination"]["mode"] == "cursor"
    assert second_page.json()["items"][0]["subject"] == "Middle"
    assert tampered.status_code == 422
    assert cross_mailbox.status_code == 422
    assert oversized.status_code == 422


@pytest.mark.asyncio
async def test_public_mailbox_api_hides_soft_deleted_delivery_detail(app_client, runtime, seeded_message) -> None:
    await runtime.messages.soft_delete_delivery(seeded_message.delivery_id)

    detail = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{seeded_message.delivery_id}",
        headers={"X-API-Key": seeded_message.public_api_key},
    )

    assert detail.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mailbox_updates",
    [
        {"public_enabled": False},
        {"is_hidden": True},
    ],
)
async def test_public_mailbox_routes_respect_mailbox_visibility_flags(
    app_client,
    runtime,
    sample_email_bytes: bytes,
    mailbox_updates: dict[str, bool],
) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    public_key = await runtime.api_keys.create_key(
        name="visibility-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )

    mailbox = runtime.mailboxes.list_mailboxes()["items"][0]
    await runtime.mailboxes.update_mailbox(mailbox["id"], mailbox_updates)

    web_response = await app_client.get("/mail/foo@adb.com")
    api_response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert web_response.status_code == 404
    assert api_response.status_code == 404


@pytest.mark.asyncio
async def test_public_mailbox_page_shows_copy_button_for_openai_verification_code(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="noreply@openai.com",
        content=_rich_mail_bytes(
            subject="Your OpenAI verification code",
            message_id="openai-otp@example.com",
            from_addr="OpenAI <noreply@openai.com>",
            body="Your OpenAI verification code is 654321.\nUse this code to verify your email.\n",
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert "复制验证码" in response.text
    assert "654321" in response.text


@pytest.mark.asyncio
async def test_public_mailbox_page_ignores_numbers_without_verification_keywords(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_rich_mail_bytes(
            subject="Order update",
            message_id="non-otp@example.com",
            from_addr="Store <sender@example.com>",
            body="Order 123456 has shipped and will arrive tomorrow.\n",
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert 'data-code="123456"' not in response.text
    assert "验证码 123456" not in response.text


@pytest.mark.asyncio
async def test_public_mailbox_page_ignores_mail_with_multiple_candidate_codes(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_rich_mail_bytes(
            subject="Verification code candidates",
            message_id="multi-otp@example.com",
            from_addr="Example <sender@example.com>",
            body="Your verification code could be 123456 or 654321 depending on region.\n",
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert 'data-code="123456"' not in response.text
    assert 'data-code="654321"' not in response.text
    assert "验证码 123456" not in response.text
    assert "验证码 654321" not in response.text


@pytest.mark.asyncio
async def test_public_mailbox_page_extracts_html_openai_verification_code(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="noreply@openai.com",
        content=_rich_mail_bytes(
            subject="Verify your email",
            message_id="html-openai-otp@example.com",
            from_addr="OpenAI <noreply@openai.com>",
            subtype="html",
            body=(
                "<html><body><h1>Verify your email</h1>"
                "<p>Your OpenAI verification code</p>"
                "<table><tr><td>482951</td></tr></table>"
                "</body></html>"
            ),
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert "复制验证码" in response.text
    assert "482951" in response.text


@pytest.mark.asyncio
async def test_public_mailbox_page_extracts_chatgpt_login_code_from_css_heavy_openai_html(app_client, runtime) -> None:
    noisy_css = " ".join(
        f".rule-{index} {{ font-family: Sohne; background-image: url(https://cdn.openai.com/font-{index}.woff2); }}"
        for index in range(20)
    )
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="noreply@tm.openai.com",
        content=_rich_mail_bytes(
            subject="Your temporary ChatGPT login code",
            message_id="chatgpt-login-code@example.com",
            from_addr="OpenAI <noreply@tm.openai.com>",
            subtype="html",
            body=(
                "<html><head><style>"
                f"{noisy_css}"
                "</style></head><body>"
                "<p>Enter this temporary verification code to continue:</p>"
                "<p>138349</p>"
                "</body></html>"
            ),
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert 'class="btn btn-primary copy-code-btn"' in response.text
    assert "138349" in response.text


@pytest.mark.asyncio
async def test_public_api_lists_mailbox_verification_codes(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    public_key = await runtime.api_keys.create_key(
        name="public-code-list",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="noreply@openai.com",
        content=_rich_mail_bytes(
            subject="Your OpenAI verification code",
            message_id="code-list@example.com",
            from_addr="OpenAI <noreply@openai.com>",
            body="Your verification code is 654321.",
        ),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/verification-codes",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["mailbox"] == "foo@adb.com"
    assert payload["items"][0]["verification_code"] == "654321"
    assert payload["items"][0]["received_at"]


@pytest.mark.asyncio
async def test_public_api_gets_single_message_verification_code(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    public_key = await runtime.api_keys.create_key(
        name="public-code-detail",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="noreply@openai.com",
        content=_rich_mail_bytes(
            subject="Your OpenAI verification code",
            message_id="code-detail@example.com",
            from_addr="OpenAI <noreply@openai.com>",
            body="Your verification code is 482951.",
        ),
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    delivery_id = mailbox["items"][0]["delivery_id"]

    response = await app_client.get(
        f"/api/v1/public/mailboxes/foo@adb.com/messages/{delivery_id}/verification-code",
        headers={"X-API-Key": public_key["plain_text"]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["delivery_id"] == delivery_id
    assert payload["verification_code"] == "482951"


@pytest.mark.asyncio
async def test_public_mailbox_page_includes_websocket_bootstrap_on_first_page(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Bootstrap", "bootstrap@example.com", "bootstrap"),
    )
    await runtime.drain_parser_queue()

    response = await app_client.get("/mail/foo@adb.com")

    assert response.status_code == 200
    assert "/mail/foo@adb.com/ws?after_cursor=" in response.text
    assert 'id="mail-list"' in response.text
    assert 'data-live-enabled="true"' in response.text


@pytest.mark.asyncio
async def test_public_mailbox_live_events_include_new_delivery_and_parse_update(runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    _, cursor = runtime.live_state.snapshot_state()
    _, seq_text = cursor.rsplit(":", 1)
    last_seq = int(seq_text)

    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=_mail_bytes("Live Subject", "live@example.com", "live body"),
    )
    await runtime.drain_parser_queue()

    events = [
        event
        for event in runtime.live_state.snapshot_since(last_seq)
        if event.get("type") in {"mailbox_delivery", "mailbox_delivery_updated"}
    ]
    inserted = events[0]
    updated = events[-1]
    item = await MessageService(runtime).get_public_mailbox_item(
        "foo@adb.com",
        str(updated["delivery_id"]),
        surface="web",
    )

    assert inserted["type"] == "mailbox_delivery"
    assert str(inserted["delivery_id"]).startswith("dlv_")
    assert inserted["parse_status"] == "pending"
    assert updated["type"] == "mailbox_delivery_updated"
    assert updated["delivery_id"] == inserted["delivery_id"]
    assert item["parse_status"] == "parsed"
    assert item["subject"] == "Live Subject"
