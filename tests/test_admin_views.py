from __future__ import annotations

from datetime import datetime, timedelta, timezone
from itertools import count
import re
from types import SimpleNamespace

import httpx
import pytest

from app.db.connection import connect_database
import app.runtime as runtime_module


def _patch_sequenced_utc_now(monkeypatch) -> None:
    base = datetime(2026, 4, 18, 20, 0, 0, tzinfo=timezone.utc)
    ticks = count()

    monkeypatch.setattr(
        runtime_module,
        "utc_now",
        lambda: (base + timedelta(seconds=next(ticks))).isoformat().replace("+00:00", "Z"),
    )


async def _login_and_change_initial_password(app_client, runtime, *, new_password: str = "new-admin-password") -> None:
    login_response = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        follow_redirects=True,
    )
    assert login_response.status_code == 200
    response = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": new_password,
            "confirm_password": new_password,
        },
        follow_redirects=True,
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_admin_login_and_dashboard_page_flow(app_client, runtime) -> None:
    response = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        follow_redirects=True,
    )

    assert response.status_code == 200
    assert "调整运行参数、修改管理员密码或清除全部邮件数据" in response.text
    assert 'aria-label="会话操作"' in response.text
    assert "当前账号仍在使用初始密码" in response.text
    assert "修改管理员密码" in response.text


@pytest.mark.asyncio
async def test_admin_login_accepts_missing_origin_header(app_fixture, runtime) -> None:
    app, _ = app_fixture
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/admin/login",
            data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/settings?force_password_change=1"
    assert response.cookies.get(runtime.settings.session_cookie_name) is not None
    assert response.headers["cache-control"] == "private, no-store"


@pytest.mark.asyncio
async def test_admin_login_accepts_reverse_proxy_origin_mismatch(app_fixture, runtime) -> None:
    app, _ = app_fixture
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        response = await client.post(
            "/admin/login",
            headers={"Origin": "https://forwarded-preview.example"},
            data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/settings?force_password_change=1"
    assert response.cookies.get(runtime.settings.session_cookie_name) is not None
    assert response.headers["cache-control"] == "private, no-store"


@pytest.mark.asyncio
async def test_dashboard_current_smtp_sessions_uses_runtime_active_connections(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)
    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            """
            INSERT INTO smtp_sessions (
                id,
                remote_ip,
                status,
                connect_at,
                last_command_at
            ) VALUES ('smtp_stale_open', '127.0.0.1', 'open', ?, ?)
            """,
            ("2026-04-18T20:00:00Z", "2026-04-18T20:00:00Z"),
        )

    response = await app_client.get("/admin")

    assert response.status_code == 200
    assert re.search(
        r"当前 SMTP 会话[\s\S]*?<span class=\"stat-tile__value\">0</span>",
        response.text,
    )


@pytest.mark.asyncio
async def test_bootstrap_admin_must_change_password_before_other_admin_pages(app_client, runtime) -> None:
    await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        follow_redirects=True,
    )

    blocked = await app_client.get("/admin/domains")
    await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "new-admin-password",
            "confirm_password": "new-admin-password",
        },
    )
    allowed = await app_client.get("/admin/domains")

    assert blocked.status_code == 303
    assert blocked.headers["location"] == "/admin/settings?force_password_change=1"
    assert allowed.status_code == 200
    assert "新增域名" in allowed.text


@pytest.mark.asyncio
async def test_admin_logout_revokes_session_and_clears_cookie(app_client, runtime) -> None:
    cookie_name = runtime.settings.session_cookie_name

    await _login_and_change_initial_password(app_client, runtime)
    assert app_client.cookies.get(cookie_name) is not None

    response = await app_client.post("/admin/logout")

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/login"
    assert app_client.cookies.get(cookie_name) is None

    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT revoked_at
            FROM admin_sessions
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            """
        ).fetchone()

    assert row["revoked_at"] is not None

    redirect = await app_client.get("/admin")
    assert redirect.status_code == 303
    assert redirect.headers["location"] == "/admin/login"


@pytest.mark.asyncio
async def test_admin_form_posts_do_not_enforce_origin(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)

    response = await app_client.post(
        "/admin/logout",
        headers={"Origin": "https://evil.example"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/login"


@pytest.mark.asyncio
async def test_admin_security_does_not_trust_direct_forwarded_proto(app_client, runtime) -> None:
    login = await app_client.post(
        "/admin/login",
        headers={"X-Forwarded-Proto": "https"},
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
    )
    assert login.status_code == 303
    assert "secure" not in login.headers["set-cookie"].lower()

    logout = await app_client.post(
        "/admin/logout",
        headers={
            "Origin": "https://testserver",
            "X-Forwarded-Proto": "https",
        },
    )
    assert logout.status_code == 303
    assert logout.headers["location"] == "/admin/login"


@pytest.mark.asyncio
async def test_admin_pages_redirect_unauthenticated_users_to_login(app_client) -> None:
    response = await app_client.get("/admin")

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/login"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["x-frame-options"] == "SAMEORIGIN"
    assert response.headers["referrer-policy"] == "same-origin"


@pytest.mark.asyncio
async def test_admin_live_placeholder_route_is_not_exposed(app_client) -> None:
    response = await app_client.get("/admin/live")

    assert response.status_code == 404


@pytest.mark.asyncio
async def test_admin_live_page_uses_cursor_based_stream_url(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)

    response = await app_client.get("/admin/live")

    assert response.status_code == 200
    assert "实时监控中枢" in response.text
    assert "after_cursor=" in response.text
    assert "msg.innerHTML = msgHtml" not in response.text
    assert "appendTextSpan" in response.text


@pytest.mark.asyncio
async def test_admin_login_rejects_invalid_credentials_with_error(app_client) -> None:
    response = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": "not-the-password"},
    )

    assert response.status_code == 401
    assert "用户名或密码不正确。" in response.text


@pytest.mark.asyncio
async def test_admin_login_rejects_oversized_password_before_authentication(app_client, runtime, monkeypatch) -> None:
    async def unexpected_authenticate(*_args, **_kwargs):
        raise AssertionError("oversized password reached authentication service")

    monkeypatch.setattr(runtime.auth, "authenticate_admin", unexpected_authenticate)
    response = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": "x" * 1025},
    )

    assert response.status_code == 400
    assert "用户名或密码格式无效" in response.text


@pytest.mark.asyncio
async def test_admin_login_rate_limit_blocks_repeated_failures(app_client) -> None:
    for _ in range(5):
        response = await app_client.post(
            "/admin/login",
            data={"username": "admin", "password": "not-the-password"},
        )
        assert response.status_code == 401

    blocked = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": "not-the-password"},
    )

    assert blocked.status_code == 429
    assert "登录失败次数过多" in blocked.text


@pytest.mark.asyncio
async def test_admin_domains_page_can_create_domain_via_form(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.post(
        "/admin/domains",
        data={
            "root_domain": "mail.adb.com",
            "accept_exact": "1",
            "accept_subdomains": "1",
            "public_web_enabled": "1",
            "public_api_enabled": "1",
            "plus_addressing_mode": "keep",
            "local_part_case_sensitive": "0",
            "is_active": "1",
            "max_message_size_bytes": "2048",
            "retention_days": "7",
        },
    )

    created = runtime.domains.list_domains()[0]
    created_detail = runtime.domains.get_domain(created["id"])
    updated_response = await app_client.post(
        f"/admin/domains/{created['id']}",
        data={
            "root_domain": "renamed-mail.adb.com",
            "accept_exact": "1",
            "accept_subdomains": "1",
            "public_web_enabled": "1",
            "public_api_enabled": "1",
            "plus_addressing_mode": "keep",
            "local_part_case_sensitive": "0",
            "is_active": "1",
            "max_message_size_bytes": "2048",
            "retention_days": "7",
            "notes": "updated through admin page",
        },
    )
    updated_detail = runtime.domains.get_domain(created["id"])
    detail_page = await app_client.get(f"/admin/domains/{created['id']}")
    missing_confirmation = await app_client.post(
        f"/admin/domains/{created['id']}/delete",
        data={},
    )
    deleted_response = await app_client.post(
        f"/admin/domains/{created['id']}/delete",
        data={"confirm": "delete-domain"},
    )

    assert response.status_code == 303
    assert response.headers["location"] == f"/admin/domains/{created['id']}"
    assert created["root_domain_ascii"] == "mail.adb.com"
    assert created_detail["accept_subdomains"] is True
    assert created_detail["public_web_enabled"] is True
    assert created_detail["max_message_size_bytes"] == 2048
    assert created_detail["retention_days"] == 7
    assert updated_response.status_code == 303
    assert updated_detail["root_domain_ascii"] == "renamed-mail.adb.com"
    assert updated_detail["notes"] == "updated through admin page"
    assert detail_page.status_code == 200
    assert 'class="danger-action"' in detail_page.text
    assert 'value="delete-domain" required data-danger-confirm' in detail_page.text
    assert "永久删除 renamed-mail.adb.com" in detail_page.text
    assert "onsubmit=\"return confirm('确认删除该域名" not in detail_page.text
    assert missing_confirmation.status_code == 422
    assert deleted_response.status_code == 303
    assert deleted_response.headers["location"] == "/admin/domains"
    with pytest.raises(LookupError):
        runtime.domains.get_domain(created["id"])


@pytest.mark.asyncio
async def test_catch_all_domain_detail_routes_management_to_settings(app_client, runtime) -> None:
    await runtime.system_settings.update_settings({"ingress_mode": "managed_plus_catchall"})
    catch_all = next(
        item for item in runtime.domains.list_domains() if item["root_domain_ascii"] == "*"
    )
    await _login_and_change_initial_password(app_client, runtime)

    response = await app_client.get(f"/admin/domains/{catch_all['id']}")

    assert response.status_code == 200
    assert "在系统设置中统一管理" in response.text
    assert 'href="/admin/settings#runtime"' in response.text
    assert f'action="/admin/domains/{catch_all["id"]}/delete"' not in response.text
    assert f'action="/admin/domains/{catch_all["id"]}/dns-check"' not in response.text
    assert 'name="root_domain"' not in response.text


@pytest.mark.asyncio
async def test_admin_delete_action_is_separate_from_password_reset(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)
    created = await app_client.post(
        "/admin/admins",
        data={
            "username": "layout-operator",
            "display_name": "Layout Operator",
            "password": "strong-layout-password",
            "role": "operator",
            "is_active": "1",
        },
    )
    operator = next(
        item for item in runtime.auth.list_admins()["items"] if item["username"] == "layout-operator"
    )
    page = await app_client.get("/admin/admins")
    delete_form = re.search(
        rf'<form class="admin-delete-form"[^>]*action="/admin/admins/{operator["id"]}/delete"[^>]*>(.*?)</form>',
        page.text,
        re.DOTALL,
    )

    assert created.status_code == 303
    assert page.status_code == 200
    assert delete_form is not None
    assert 'name="password"' not in delete_form.group(1)


@pytest.mark.asyncio
async def test_admin_mailboxes_page_can_toggle_visibility_via_form(app_client, runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.ensure_smtp_session(
        "smtp_clear_all",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="localhost", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_clear_all",
    )
    await runtime.drain_parser_queue()
    await runtime.live_state.publish(
        {
            "type": "queued",
            "ts": "2026-04-18T20:00:00Z",
            "session_id": "smtp_clear_all",
            "message_id": "fixture",
        }
    )

    await _login_and_change_initial_password(app_client, runtime)
    mailbox = runtime.mailboxes.list_mailboxes()["items"][0]
    message = runtime.messages.list_messages()["items"][0]
    detail_page = await app_client.get(f"/admin/mailboxes/{mailbox['id']}")
    message_detail_page = await app_client.get(f"/admin/messages/{message['id']}")
    response = await app_client.post(
        f"/admin/mailboxes/{mailbox['id']}",
        data={
            "public_enabled": "0",
            "is_hidden": "1",
        },
    )

    updated = runtime.mailboxes.get_mailbox(mailbox["id"])

    assert response.status_code == 303
    assert response.headers["location"] == "/admin/mailboxes?limit=20&offset=0"
    assert updated["public_enabled"] is False
    assert updated["is_hidden"] is True
    assert 'data-bulk-delete-form' in detail_page.text
    assert 'data-bulk-item' in detail_page.text
    assert 'disabled data-bulk-submit' in detail_page.text
    assert "未选择时会删除" not in detail_page.text
    assert 'data-bulk-delete-form' in message_detail_page.text
    assert 'disabled data-bulk-submit' in message_detail_page.text
    assert "未选择时会删除" not in message_detail_page.text


@pytest.mark.asyncio
async def test_invalid_utf8_preview_does_not_break_public_or_admin_pages(
    app_client,
    runtime,
    seeded_message,
) -> None:
    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            "UPDATE messages SET text_preview = CAST(? AS TEXT) WHERE id = ?",
            (b"legacy-\xe4\xb8", seeded_message.message_id),
        )

    public_view = await runtime.messages.get_public_mailbox_view(
        "foo@adb.com",
        surface="web",
    )
    public_item = await runtime.messages.get_public_mailbox_item(
        "foo@adb.com",
        seeded_message.delivery_id,
        surface="web",
    )
    public_page = await app_client.get("/mail/foo@adb.com")

    assert public_page.status_code == 200
    assert "text_preview" not in public_view["items"][0]
    assert "text_preview" not in public_item

    await _login_and_change_initial_password(app_client, runtime)
    admin_page = await app_client.get(f"/admin/messages/{seeded_message.message_id}")

    assert admin_page.status_code == 200
    assert seeded_message.message_id in admin_page.text


@pytest.mark.asyncio
async def test_admin_mailboxes_filter_form_accepts_empty_values(app_client, runtime) -> None:
    domain = await runtime.create_domain("adb.com")
    await _login_and_change_initial_password(app_client, runtime)

    blank_filters = await app_client.get(
        "/admin/mailboxes?q=&domain_id=&public_enabled=&is_hidden=&limit=20"
    )
    domain_filter = await app_client.get(
        f"/admin/mailboxes?q=&domain_id={domain['id']}&public_enabled=&is_hidden=&limit=20"
    )

    assert blank_filters.status_code == 200
    assert domain_filter.status_code == 200
    assert f'value="{domain["id"]}" selected' in domain_filter.text


@pytest.mark.asyncio
async def test_admin_api_keys_page_can_create_and_revoke_via_form(app_client, runtime) -> None:
    await _login_and_change_initial_password(app_client, runtime)
    created = await app_client.post(
        "/admin/api-keys",
        data={
            "name": "html-admin-key",
            "kind": "admin",
            "scopes": ["domains.read"],
        },
    )
    match = re.search(r"(ri_admin_[a-f0-9]+_[A-Za-z0-9_-]+)", created.text)

    assert created.status_code in {200, 201}
    assert match is not None

    plain_text = match.group(1)
    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT id, status
            FROM api_keys
            WHERE name = ?
            """,
            ("html-admin-key",),
        ).fetchone()

    assert row["status"] == "active"

    revoked = await app_client.post(f"/admin/api-keys/{row['id']}/revoke", follow_redirects=True)

    with connect_database(runtime.settings.database_path) as connection:
        after = connection.execute(
            """
            SELECT status
            FROM api_keys
            WHERE id = ?
            """,
            (row["id"],),
        ).fetchone()

    denied = await app_client.get(
        "/api/v1/admin/domains",
        headers={"X-API-Key": plain_text},
    )

    assert revoked.status_code == 200
    assert after["status"] == "revoked"
    assert denied.status_code == 401


@pytest.mark.asyncio
async def test_admin_settings_page_can_clear_all_mail(app_client, runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_clear_all",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="localhost", ssl=None),
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_clear_all",
    )
    await runtime.drain_parser_queue()
    await runtime.live_state.publish(
        {
            "type": "queued",
            "ts": "2026-04-18T20:00:00Z",
            "session_id": "smtp_clear_all",
            "message_id": "fixture",
        }
    )

    assert any(path.is_file() for path in runtime.settings.raw_dir.rglob("*"))
    assert any(path.is_file() for path in runtime.settings.manifests_dir.rglob("*"))

    await _login_and_change_initial_password(app_client, runtime)
    settings_page = await app_client.get("/admin/settings")
    missing_confirmation = await app_client.post("/admin/settings/clear-mail", data={})
    response = await app_client.post(
        "/admin/settings/clear-mail",
        data={"confirm": "clear-all-mail"},
    )

    assert settings_page.status_code == 200
    assert "清除所有邮件" in settings_page.text
    assert "近期网络会话" in settings_page.text
    assert 'class="danger-action"' in settings_page.text
    assert 'value="clear-all-mail" required data-danger-confirm' in settings_page.text
    assert 'disabled data-no-confirm' in settings_page.text
    assert "onsubmit=\"return confirm('确认清除" not in settings_page.text
    assert missing_confirmation.status_code == 422
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        "/admin/settings?mail_cleared=1&cleared_messages=1&cleared_mailboxes=1&cleared_sessions=1"
    )
    assert "database_size_before_bytes=" in response.headers["location"]
    assert "database_size_after_bytes=" in response.headers["location"]

    with connect_database(runtime.settings.database_path) as connection:
        counts = {
            "domains": connection.execute("SELECT COUNT(*) AS count FROM domains").fetchone()["count"],
            "messages": connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"],
            "deliveries": connection.execute("SELECT COUNT(*) AS count FROM message_deliveries").fetchone()["count"],
            "mailboxes": connection.execute("SELECT COUNT(*) AS count FROM mailboxes").fetchone()["count"],
            "attachments": connection.execute("SELECT COUNT(*) AS count FROM attachments").fetchone()["count"],
            "smtp_sessions": connection.execute("SELECT COUNT(*) AS count FROM smtp_sessions").fetchone()["count"],
            "smtp_events": connection.execute("SELECT COUNT(*) AS count FROM smtp_events").fetchone()["count"],
        }
        audit = connection.execute(
            "SELECT action, details_json FROM audit_logs WHERE action = ?",
            ("mail.clear_all",),
        ).fetchone()

    assert counts == {
        "domains": 1,
        "messages": 0,
        "deliveries": 0,
        "mailboxes": 0,
        "attachments": 0,
        "smtp_sessions": 0,
        "smtp_events": 0,
    }
    assert audit is not None
    assert '"messages": 1' in audit["details_json"]
    assert '"smtp_sessions": 1' in audit["details_json"]
    assert runtime.live_state.snapshot() == []
    assert not any(path.is_file() for path in runtime.settings.raw_dir.rglob("*"))
    assert not any(path.is_file() for path in runtime.settings.manifests_dir.rglob("*"))

    public_mailbox = await app_client.get("/mail/foo@adb.com")
    assert public_mailbox.status_code == 200
    assert "暂无邮件" in public_mailbox.text

    with connect_database(runtime.settings.database_path) as connection:
        mailbox_count = connection.execute("SELECT COUNT(*) AS count FROM mailboxes").fetchone()["count"]
        message_count = connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"]

    assert mailbox_count == 0
    assert message_count == 0


@pytest.mark.asyncio
async def test_clear_all_mail_vacuums_sqlite_database(runtime) -> None:
    large_headers_json = "x" * 2_000_000
    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            """
            INSERT INTO messages (
                id,
                raw_path,
                raw_sha256,
                raw_size_bytes,
                received_at,
                headers_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "large-message",
                "raw/large-message.eml",
                "0" * 64,
                1,
                "2026-04-18T20:00:00Z",
                large_headers_json,
            ),
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    result = await runtime.clear_all_mail()

    assert result["messages"] == 1
    assert result["database_vacuumed"] == 1
    assert result["database_size_after_bytes"] < result["database_size_before_bytes"]


@pytest.mark.asyncio
async def test_admin_settings_page_can_change_admin_password(app_client, runtime) -> None:
    await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        follow_redirects=True,
    )

    settings_page = await app_client.get("/admin/settings")
    rejected = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": "wrong-password",
            "new_password": "new-admin-password",
            "confirm_password": "new-admin-password",
        },
    )
    changed = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "new-admin-password",
            "confirm_password": "new-admin-password",
        },
    )

    assert settings_page.status_code == 200
    assert "修改管理员密码" in settings_page.text
    assert rejected.status_code == 400
    assert "当前密码不正确" in rejected.text
    assert changed.status_code == 303
    assert changed.headers["location"] == "/admin/settings?password_changed=1"

    with connect_database(runtime.settings.database_path) as connection:
        audit = connection.execute(
            "SELECT action, resource_type FROM audit_logs WHERE action = ?",
            ("admin.password_change",),
        ).fetchone()

    assert audit is not None
    assert audit["resource_type"] == "admin"

    await app_client.post("/admin/logout")
    old_login = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
    )
    new_login = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": "new-admin-password"},
        follow_redirects=True,
    )

    assert old_login.status_code == 401
    assert new_login.status_code == 200
    assert "域名管理" in new_login.text


@pytest.mark.asyncio
async def test_admin_password_change_rejects_default_or_unchanged_password(app_client, runtime) -> None:
    await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
        follow_redirects=True,
    )

    default_password = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": runtime.settings.bootstrap_admin_password,
            "confirm_password": runtime.settings.bootstrap_admin_password,
        },
    )
    changed = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "new-admin-password",
            "confirm_password": "new-admin-password",
        },
    )
    unchanged = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": "new-admin-password",
            "new_password": "new-admin-password",
            "confirm_password": "new-admin-password",
        },
    )

    assert default_password.status_code == 400
    assert "默认初始密码" in default_password.text
    assert changed.status_code == 303
    assert unchanged.status_code == 400
    assert "不能与当前密码相同" in unchanged.text


@pytest.mark.asyncio
async def test_admin_password_change_rejects_oversized_password(app_client, runtime) -> None:
    await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
    )

    response = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "x" * 1025,
            "confirm_password": "x" * 1025,
        },
    )

    assert response.status_code == 400
    assert "不能超过 1024 个字符" in response.text


@pytest.mark.asyncio
async def test_admin_api_keys_page_uses_checkbox_scopes_and_domain_hints(app_client, runtime) -> None:
    await runtime.create_domain("adb.com")
    await _login_and_change_initial_password(app_client, runtime)

    response = await app_client.get("/admin/api-keys")

    assert response.status_code == 200
    assert 'type="checkbox" name="scopes" value="public.read"' in response.text
    assert 'type="text" name="scopes"' not in response.text
    assert 'name="domain_grant_mode"' in response.text
    assert 'value="all"' in response.text
    assert "选择授权域名" in response.text
    assert "授权所有可用域名" in response.text
    assert "公开邮件读取" in response.text
    assert "adb.com" in response.text
    assert 'id="create-key-form" class="drawer__form"' in response.text
    assert ".drawer__form" in response.text
    assert "min-height: 0;" in response.text
    assert "-webkit-overflow-scrolling: touch;" in response.text


@pytest.mark.asyncio
async def test_admin_api_keys_form_without_domain_grants_allows_public_mailbox_access(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await _login_and_change_initial_password(app_client, runtime)

    created = await app_client.post(
        "/admin/api-keys",
        data={
            "name": "html-public-no-domain",
            "kind": "public",
            "scopes": ["public.read"],
        },
    )
    match = re.search(r"(ri_public_[a-f0-9]+_[A-Za-z0-9_-]+)", created.text)

    assert created.status_code in {200, 201}
    assert match is not None

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": match.group(1)},
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_admin_api_keys_page_can_edit_permissions_and_domain_grants(app_client, runtime) -> None:
    primary_domain = await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.create_domain("other.com", public_web_enabled=True, public_api_enabled=True)
    key = await runtime.api_keys.create_key(
        name="html-public-edit",
        kind="public",
        scopes=["public.read"],
        domain_ids=[primary_domain["id"]],
        mailbox_patterns=[],
    )
    await _login_and_change_initial_password(app_client, runtime)

    denied_before = await app_client.get(
        "/api/v1/public/mailboxes/foo@other.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )
    edit_page = await app_client.get(f"/admin/api-keys/{key['id']}")
    updated = await app_client.post(
        f"/admin/api-keys/{key['id']}",
        data={
            "name": "html-public-edited",
            "kind": "public",
            "status": "active",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "mailbox_patterns": "",
            "allow_header": "1",
            "rate_limit_per_min": "3600",
            "allowed_ip_cidrs": "",
            "expires_at": "",
        },
    )
    allowed_after = await app_client.get(
        "/api/v1/public/mailboxes/foo@other.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            """
            SELECT name
            FROM api_keys
            WHERE id = ?
            """,
            (key["id"],),
        ).fetchone()
        domain_grants = connection.execute(
            "SELECT COUNT(*) AS count FROM api_key_domain_grants WHERE api_key_id = ?",
            (key["id"],),
        ).fetchone()

    assert denied_before.status_code == 403
    assert edit_page.status_code == 200
    assert "保存修改" in edit_page.text
    assert "授权所有可用域名" in edit_page.text
    assert updated.status_code == 303
    assert updated.headers["location"] == f"/admin/api-keys/{key['id']}?updated=1"
    assert allowed_after.status_code == 200
    assert row["name"] == "html-public-edited"
    assert domain_grants["count"] == 0


@pytest.mark.asyncio
async def test_admin_mailboxes_page_paginates_results(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com")
    for index in range(21):
        await runtime.accept_message(
            rcpt_tos=[f"user{index:02d}@adb.com"],
            envelope_from="sender@example.com",
            content=(
                "From: Sender <sender@example.com>\r\n"
                f"To: User <user{index:02d}@adb.com>\r\n"
                f"Subject: Mailbox {index:02d}\r\n"
                f"Message-ID: <mailbox-{index:02d}@example.com>\r\n"
                "Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
                "MIME-Version: 1.0\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                "\r\n"
                "Mailbox pagination test\r\n"
            ).encode("utf-8"),
        )
    await runtime.drain_parser_queue()

    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.get("/admin/mailboxes")

    assert response.status_code == 200
    assert "?limit=20&offset=20" in response.text
    assert "user20@adb.com" in response.text
    assert "user00@adb.com" not in response.text


@pytest.mark.asyncio
async def test_admin_messages_page_paginates_results(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    await runtime.create_domain("adb.com")
    for index in range(21):
        await runtime.accept_message(
            rcpt_tos=["foo@adb.com"],
            envelope_from="sender@example.com",
            content=(
                "From: Sender <sender@example.com>\r\n"
                "To: Foo <foo@adb.com>\r\n"
                f"Subject: Message {index:02d}\r\n"
                f"Message-ID: <message-{index:02d}@example.com>\r\n"
                "Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
                "MIME-Version: 1.0\r\n"
                "Content-Type: text/plain; charset=utf-8\r\n"
                "\r\n"
                "Messages pagination test\r\n"
            ).encode("utf-8"),
        )
    await runtime.drain_parser_queue()

    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.get("/admin/messages")

    assert response.status_code == 200
    assert "?limit=20&offset=20" in response.text
    assert "Message 20" in response.text
    assert "Message 00" not in response.text


@pytest.mark.asyncio
async def test_admin_messages_page_shows_recipients(app_client, runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com", "bar@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.get("/admin/messages")

    assert response.status_code == 200
    assert "收件人:" in response.text
    assert "bar@adb.com, foo@adb.com" in response.text


@pytest.mark.asyncio
async def test_admin_messages_page_displays_shanghai_time(app_client, runtime, monkeypatch, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-18T20:00:00Z")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.get("/admin/messages")

    assert response.status_code == 200
    assert "2026-04-19 04:00:00" in response.text


@pytest.mark.asyncio
async def test_admin_api_keys_and_audit_pages_paginate_results(app_client, runtime) -> None:
    for index in range(21):
        await runtime.api_keys.create_key(
            name=f"paged-key-{index:02d}",
            kind="admin",
            scopes=["domains.read"],
            domain_ids=[],

            domain_grant_mode="all",
            mailbox_patterns=[],
        )
        await runtime.audit.log(
            "admin",
            "fixture",
            f"fixture.action.{index:02d}",
            "system_settings",
            None,
            "success",
        )

    await _login_and_change_initial_password(app_client, runtime)
    api_keys_response = await app_client.get("/admin/api-keys")
    audit_response = await app_client.get("/admin/audit")

    assert api_keys_response.status_code == 200
    assert "?limit=20&offset=20" in api_keys_response.text
    assert "paged-key-20" in api_keys_response.text
    assert "paged-key-00" not in api_keys_response.text

    assert audit_response.status_code == 200
    assert "?limit=20&offset=20" in audit_response.text
    assert "fixture.action.20" in audit_response.text
    assert "fixture.action.00" not in audit_response.text


@pytest.mark.asyncio
async def test_admin_live_page_limits_stream_items_and_paginates_sessions(app_client, runtime, monkeypatch) -> None:
    _patch_sequenced_utc_now(monkeypatch)

    for index in range(21):
        session_id = f"smtp_session_{index:02d}"
        await runtime.ensure_smtp_session(
            session_id,
            type("Session", (), {"peer": ("127.0.0.1", 2500 + index), "host_name": f"mx{index:02d}", "ssl": None})(),
        )
        await runtime.close_smtp_session(session_id, status="closed", close_reason="fixture")

    for index in range(25):
        await runtime.live_state.publish(
            {
                "type": "queued" if index % 2 else "rcpt_accepted",
                "ts": f"2026-04-18T20:00:{index:02d}Z",
                "session_id": f"smtp_session_{index:02d}",
                "message_id": f"msg_{index:02d}",
            }
        )

    await _login_and_change_initial_password(app_client, runtime)
    response = await app_client.get("/admin/live")

    assert response.status_code == 200
    assert response.text.count('data-event-type="') == 20
    assert "?limit=20&offset=20" in response.text
    assert "smtp_session_20" in response.text
    assert "smtp_session_00" not in response.text
