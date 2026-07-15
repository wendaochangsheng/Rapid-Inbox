from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.db.connection import connect_database


@pytest.mark.asyncio
async def test_public_key_without_domain_grants_cannot_read_current_domains(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    key = await runtime.api_keys.create_key(
        name="public-read",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        mailbox_patterns=[],
    )

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_public_key_without_domain_grants_cannot_read_later_domains(app_client, runtime) -> None:
    key = await runtime.api_keys.create_key(
        name="public-read-all-future",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        mailbox_patterns=[],
    )
    await runtime.create_domain("later.adb.com", public_web_enabled=True, public_api_enabled=True)

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@later.adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_seeded_message_public_key_can_read_seeded_mailbox(app_client, seeded_message) -> None:
    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": seeded_message.public_api_key},
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["delivery_id"] == seeded_message.delivery_id


@pytest.mark.asyncio
async def test_mailbox_only_public_key_can_read_mailbox(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_mailbox_only",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="mailbox-only",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_mailbox_only",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["delivery_id"] == mailbox["items"][0]["delivery_id"]


@pytest.mark.asyncio
async def test_mailbox_only_public_key_uses_canonical_mailbox_address(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_canonical_mailbox",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="canonical-mailbox",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_canonical_mailbox",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")

    response = await app_client.get(
        "/api/v1/public/mailboxes/FOO@ADB.COM/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["delivery_id"] == mailbox["items"][0]["delivery_id"]


@pytest.mark.asyncio
async def test_public_key_context_is_cleared_after_request(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_context_cleanup",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="context-cleanup",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_context_cleanup",
    )
    await runtime.drain_parser_queue()

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 200

    mailbox = await runtime.get_mailbox_view("bar@adb.com")
    assert mailbox["mailbox"] == "bar@adb.com"
    assert mailbox["message_count"] == 0


@pytest.mark.asyncio
async def test_public_key_records_request_ip(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_record_ip",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="record-ip",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_record_ip",
    )
    await runtime.drain_parser_queue()

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 200

    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            "SELECT last_used_ip FROM api_keys WHERE id = ?",
            (key["id"],),
        ).fetchone()

    assert row["last_used_ip"] == "127.0.0.1"


@pytest.mark.asyncio
async def test_public_key_ip_restriction_blocks_disallowed_client_ip(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_ip_restriction",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="ip-restricted",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
        allowed_ip_cidrs=["203.0.113.0/24"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_ip_restriction",
    )
    await runtime.drain_parser_queue()

    response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_public_key_rate_limit_blocks_repeat_requests(app_client, runtime, sample_email_bytes) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_rate_limit",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="rate-limited",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
        rate_limit_per_min=1,
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_rate_limit",
    )
    await runtime.drain_parser_queue()

    first_response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )
    second_response = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert first_response.status_code == 200
    assert second_response.status_code == 429


@pytest.mark.asyncio
async def test_query_key_auth_respects_allow_query(runtime) -> None:
    disabled_key = await runtime.api_keys.create_key(
        name="query-disabled",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    enabled_key = await runtime.api_keys.create_key(
        name="query-enabled",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
        allow_query=True,
    )

    with pytest.raises(LookupError):
        runtime.api_keys.authenticate_query(disabled_key["plain_text"])

    context = runtime.api_keys.authenticate_query(enabled_key["plain_text"])

    assert context.kind == "public"
    assert context.public_id == enabled_key["public_id"]


@pytest.mark.asyncio
async def test_public_api_query_key_cannot_use_header_only_key(app_client, runtime) -> None:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    header_only_key = await runtime.api_keys.create_key(
        name="query-disabled-http",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
        allow_header=True,
        allow_query=False,
    )
    query_key = await runtime.api_keys.create_key(
        name="query-enabled-http",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],

        domain_grant_mode="all",
        mailbox_patterns=[],
        allow_query=True,
    )

    rejected = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        params={"api_key": header_only_key["plain_text"]},
    )
    accepted = await app_client.get(
        "/api/v1/public/mailboxes/foo@adb.com/messages",
        params={"api_key": query_key["plain_text"]},
    )

    assert rejected.status_code == 401
    assert accepted.status_code == 200


@pytest.mark.asyncio
async def test_admin_api_mailbox_grants_filter_lists_and_shared_messages(
    app_client,
    runtime,
    sample_email_bytes,
) -> None:
    await runtime.create_domain("restricted.example")
    shared_response = await runtime.accept_message(
        rcpt_tos=["allowed@restricted.example", "secret@restricted.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    allowed_response = await runtime.accept_message(
        rcpt_tos=["allowed@restricted.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()

    key = await runtime.api_keys.create_key(
        name="mailbox-scoped-admin",
        kind="admin",
        scopes=["mailboxes.read", "messages.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["allowed@restricted.example"],
    )
    app_client.headers["X-API-Key"] = key["plain_text"]

    mailboxes = await app_client.get("/api/v1/admin/mailboxes")
    assert mailboxes.status_code == 200
    assert [item["address_canonical"] for item in mailboxes.json()["items"]] == [
        "allowed@restricted.example"
    ]
    assert mailboxes.json()["total_count"] == 1

    stored_mailboxes = {
        item["address_canonical"]: item
        for item in runtime.mailboxes.list_mailboxes(limit=10)["items"]
    }
    assert (
        await app_client.get(
            f"/api/v1/admin/mailboxes/{stored_mailboxes['allowed@restricted.example']['id']}"
        )
    ).status_code == 200
    assert (
        await app_client.get(
            f"/api/v1/admin/mailboxes/{stored_mailboxes['secret@restricted.example']['id']}"
        )
    ).status_code == 403

    messages = await app_client.get("/api/v1/admin/messages")
    assert messages.status_code == 200
    assert [item["id"] for item in messages.json()["items"]] == [
        allowed_response.removeprefix("250 queued as ")
    ]
    shared_message_id = shared_response.removeprefix("250 queued as ")
    assert (await app_client.get(f"/api/v1/admin/messages/{shared_message_id}")).status_code == 403


@pytest.mark.asyncio
async def test_selected_domain_key_cannot_read_global_admin_surfaces(app_client, runtime) -> None:
    domain = await runtime.create_domain("selected-global-guard.example")
    key = await runtime.api_keys.create_key(
        name="selected-global-guard",
        kind="admin",
        scopes=[
            "system.read",
            "audit.read",
            "smtp.read",
            "live.read",
            "admins.read",
        ],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    app_client.headers["X-API-Key"] = key["plain_text"]

    for path in (
        "/api/v1/admin/dashboard/metrics",
        "/api/v1/admin/smtp-sessions",
        "/api/v1/admin/audit-logs",
        "/api/v1/admin/settings",
        "/api/v1/admin/admins",
        "/api/v1/admin/live/smtp/stream",
    ):
        response = await app_client.get(path)
        assert response.status_code == 403, path

    v2_audit = await app_client.get(
        "/api/v2/audit-events",
        headers={"Authorization": f"Bearer {key['plain_text']}"},
    )
    assert v2_audit.status_code == 403
    assert v2_audit.json()["code"] == "global_grant_required"


@pytest.mark.asyncio
async def test_v1_selected_domain_key_can_edit_and_delete_but_cannot_rename_domain(
    app_client,
    runtime,
) -> None:
    domain = await runtime.create_domain("selected-v1-update.example")
    key = await runtime.api_keys.create_key(
        name="selected-v1-domain-editor",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    headers = {"X-API-Key": key["plain_text"]}

    policy_update = await app_client.patch(
        f"/api/v1/admin/domains/{domain['id']}",
        headers=headers,
        json={"notes": "selected v1 policy update"},
    )
    rename_denied = await app_client.patch(
        f"/api/v1/admin/domains/{domain['id']}",
        headers=headers,
        json={"root_domain": "renamed-selected-v1-update.example"},
    )
    delete_allowed = await app_client.delete(
        f"/api/v1/admin/domains/{domain['id']}",
        headers=headers,
    )

    assert policy_update.status_code == 200
    assert policy_update.json()["notes"] == "selected v1 policy update"
    assert rename_denied.status_code == 403
    assert delete_allowed.status_code == 200
    assert delete_allowed.json()["domain"]["root_domain_ascii"] == "selected-v1-update.example"
    with pytest.raises(LookupError):
        runtime.domains.get_domain(domain["id"])


@pytest.mark.asyncio
async def test_v1_domain_delete_rejects_principal_narrowed_after_request_authentication(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    domain = await runtime.create_domain("stale-delete-v1.example")
    key = await runtime.api_keys.create_key(
        name="stale-v1-domain-deleter",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    original_delete = runtime.domains.delete_domain

    async def narrow_before_delete(domain_id: int, *, authorization_principal=None):
        await runtime.api_keys.update_key(key["id"], scopes=["domains.read"])
        return await original_delete(
            domain_id,
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(runtime.domains, "delete_domain", narrow_before_delete)
    response = await app_client.delete(
        f"/api/v1/admin/domains/{domain['id']}",
        headers={"X-API-Key": key["plain_text"]},
    )

    assert response.status_code == 403
    assert runtime.domains.get_domain(domain["id"])["root_domain_ascii"] == "stale-delete-v1.example"


@pytest.mark.asyncio
async def test_v1_settings_reject_principal_narrowed_after_request_authentication(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    original_limit = int(runtime.settings.max_recipients_per_message)
    key = await runtime.api_keys.create_key(
        name="stale-v1-settings-writer",
        kind="admin",
        scopes=["system.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    real_update = runtime.system_settings.update_settings

    async def narrow_before_update(payload, *, authorization_principal=None):
        await runtime.api_keys.update_key(key["id"], scopes=["system.read"])
        return await real_update(
            payload,
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(runtime.system_settings, "update_settings", narrow_before_update)
    response = await app_client.patch(
        "/api/v1/admin/settings",
        headers={"X-API-Key": key["plain_text"]},
        json={"max_recipients_per_message": original_limit + 1},
    )

    assert response.status_code == 403
    assert int(runtime.settings.max_recipients_per_message) == original_limit


@pytest.mark.asyncio
async def test_v1_admin_creation_requires_credentials_and_contained_role(app_client, runtime) -> None:
    write_only = await runtime.api_keys.create_key(
        name="v1-admin-write-only",
        kind="admin",
        scopes=["admins.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    missing_credentials = await app_client.post(
        "/api/v1/admin/admins",
        headers={"X-API-Key": write_only["plain_text"]},
        json={
            "username": "forbidden-superadmin-v1",
            "password": "forbidden-superadmin-password",
            "role": "superadmin",
        },
    )

    bounded = await runtime.api_keys.create_key(
        name="v1-bounded-admin-manager",
        kind="admin",
        scopes=["admins.write", "admins.credentials.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    role_exceeds_actor = await app_client.post(
        "/api/v1/admin/admins",
        headers={"X-API-Key": bounded["plain_text"]},
        json={
            "username": "forbidden-viewer-v1",
            "password": "forbidden-viewer-password",
            "role": "viewer",
        },
    )

    assert missing_credentials.status_code == 403
    assert role_exceeds_actor.status_code == 403
    usernames = {item["username"] for item in runtime.auth.list_admins()["items"]}
    assert "forbidden-superadmin-v1" not in usernames
    assert "forbidden-viewer-v1" not in usernames


@pytest.mark.asyncio
async def test_v1_mailbox_scoped_key_cannot_delegate_broader_api_key(app_client, runtime) -> None:
    parent = await runtime.api_keys.create_key(
        name="mailbox-scoped-key-manager",
        kind="admin",
        scopes=["api_keys.read", "api_keys.write", "public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["team*@delegation.example"],
    )
    app_client.headers["X-API-Key"] = parent["plain_text"]

    unrestricted = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "unrestricted-child",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "mailbox_patterns": [],
        },
    )
    unrelated = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "unrelated-child",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "mailbox_patterns": ["secret@delegation.example"],
        },
    )
    unprovable_glob = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "unprovable-glob-child",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "mailbox_patterns": ["team?@delegation.example"],
        },
    )
    literal_child = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "literal-child",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "mailbox_patterns": ["team1@delegation.example"],
        },
    )

    assert unrestricted.status_code == 403
    assert unrelated.status_code == 403
    assert unprovable_glob.status_code == 403
    assert literal_child.status_code == 201
    assert literal_child.json()["mailbox_patterns"] == ["team1@delegation.example"]


@pytest.mark.asyncio
async def test_v1_api_key_delegation_preserves_parent_operational_policy(app_client, runtime) -> None:
    parent = await runtime.api_keys.create_key(
        name="bounded-key-manager",
        kind="admin",
        scopes=["api_keys.read", "api_keys.write", "public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
        rate_limit_per_min=10,
        allowed_ip_cidrs=["127.0.0.0/8"],
        expires_at="2099-01-01T00:00:00Z",
    )
    app_client.headers["X-API-Key"] = parent["plain_text"]

    over_broad = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "over-broad-v1",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "rate_limit_per_min": 0,
            "allowed_ip_cidrs": [],
            "expires_at": None,
        },
    )
    compliant = await app_client.post(
        "/api/v1/admin/api-keys",
        json={
            "name": "contained-v1",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "rate_limit_per_min": 5,
            "allowed_ip_cidrs": ["127.2.0.0/16"],
            "expires_at": "2098-01-01T00:00:00Z",
        },
    )
    widened = await app_client.patch(
        f"/api/v1/admin/api-keys/{compliant.json()['id']}",
        json={"rate_limit_per_min": 11},
    )
    query_transport = await app_client.patch(
        f"/api/v1/admin/api-keys/{compliant.json()['id']}",
        json={"allow_query": True},
    )

    assert over_broad.status_code == 403
    assert compliant.status_code == 201
    assert compliant.json()["rate_limit_per_min"] == 5
    assert widened.status_code == 403
    assert query_transport.status_code == 403


@pytest.mark.asyncio
async def test_v1_mailbox_scoped_key_cannot_manage_broader_existing_key(app_client, runtime) -> None:
    parent = await runtime.api_keys.create_key(
        name="mailbox-scoped-key-manager",
        kind="admin",
        scopes=["api_keys.read", "api_keys.write", "public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["allowed@delegation.example"],
    )
    hidden_target = await runtime.api_keys.create_key(
        name="broader-existing-key",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    app_client.headers["X-API-Key"] = parent["plain_text"]

    listed = await app_client.get("/api/v1/admin/api-keys")
    assert listed.status_code == 200
    assert hidden_target["id"] not in {item["id"] for item in listed.json()["items"]}

    requests = (
        await app_client.get(f"/api/v1/admin/api-keys/{hidden_target['id']}"),
        await app_client.patch(
            f"/api/v1/admin/api-keys/{hidden_target['id']}",
            json={"name": "attempted-update"},
        ),
        await app_client.post(f"/api/v1/admin/api-keys/{hidden_target['id']}/rotate"),
        await app_client.post(f"/api/v1/admin/api-keys/{hidden_target['id']}/revoke"),
        await app_client.delete(f"/api/v1/admin/api-keys/{hidden_target['id']}"),
    )
    assert [response.status_code for response in requests] == [403, 403, 403, 403, 403]

    unchanged = runtime.api_keys.get_key(hidden_target["id"])
    assert unchanged["name"] == "broader-existing-key"
    assert unchanged["status"] == "active"
