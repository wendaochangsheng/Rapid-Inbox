from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.db.connection import connect_database
from app.db.read_pool import SQLiteReadPoolOverloadedError
from app.main import create_app
from app.auth.permissions import ROLE_SCOPES
import app.http.api_v2 as api_v2_module


async def _create_v2_key(
    runtime,
    *,
    scopes: list[str],
    kind: str = "admin",
    domain_grant_mode: str = "all",
    domain_ids: list[int] | None = None,
    mailbox_patterns: list[str] | None = None,
    rate_limit_per_min: int = 3600,
) -> dict:
    return await runtime.api_keys.create_key(
        name="v2-test-key",
        kind=kind,
        scopes=scopes,
        domain_ids=domain_ids or [],
        mailbox_patterns=mailbox_patterns or [],
        domain_grant_mode=domain_grant_mode,
        rate_limit_per_min=rate_limit_per_min,
    )


def _bearer(key: dict) -> dict[str, str]:
    return {"Authorization": f"Bearer {key['plain_text']}"}


def test_v2_openapi_declares_bearer_security_and_strict_models() -> None:
    schema = create_app().openapi()

    assert schema["components"]["securitySchemes"]["BearerAuth"]["type"] == "http"
    assert schema["components"]["securitySchemes"]["BearerAuth"]["scheme"] == "bearer"
    assert schema["paths"]["/api/v2/me"]["get"]["security"] == [{"BearerAuth": []}]
    assert schema["components"]["schemas"]["DomainCreate"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["ProblemDetails"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["DashboardStatusOut"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["DashboardIngestdOut"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["PublicMessageDetailOut"]["additionalProperties"] is False
    assert schema["components"]["schemas"]["SmtpSessionDetailOut"]["additionalProperties"] is False
    assert "message/rfc822" in schema["paths"]["/api/v2/messages/{message_id}/raw"]["get"]["responses"]["200"][
        "content"
    ]
    public_raw_operation = schema["paths"][
        "/api/v2/public/mailboxes/{mailbox_address}/messages/{delivery_id}/raw"
    ]["get"]
    assert "message/rfc822" in public_raw_operation["responses"]["200"]["content"]
    assert schema["paths"]["/api/v2/api-keys"]["post"]["operationId"] == "createV2ApiKey"
    public_list_operation = schema["paths"]["/api/v2/public/mailboxes/{mailbox_address}/messages"]["get"]
    assert public_list_operation["operationId"] == "listV2PublicMailboxMessages"
    assert schema["paths"]["/api/v2/smtp-sessions"]["get"]["operationId"] == "listV2SmtpSessions"
    assert schema["paths"]["/api/v2/domains/{domain_id}/dns-check"]["post"]["operationId"] == "runV2DomainDnsCheck"
    assert schema["paths"]["/api/v2/maintenance/clear-all"]["post"]["operationId"] == "runV2MaintenanceClearAll"
    read_unavailable_response = schema["paths"]["/api/v2/domains"]["get"]["responses"]["503"]
    assert read_unavailable_response["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ProblemDetails"
    )
    assert public_list_operation["security"] == [{"BearerAuth": []}]
    assert "Cache-Control" in schema["paths"]["/api/v2/api-keys"]["post"]["responses"]["201"]["headers"]
    rotate_response = schema["paths"]["/api/v2/api-keys/{api_key_id}/rotate"]["post"]["responses"]["200"]
    assert "Cache-Control" in rotate_response["headers"]

    me_parameters = schema["paths"]["/api/v2/me"]["get"].get("parameters", [])
    assert "api_key" not in {parameter["name"] for parameter in me_parameters}


@pytest.mark.asyncio
async def test_v2_accepts_only_bearer_and_returns_problem_details(app_client, runtime) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.read"])

    x_api_key = await app_client.get("/api/v2/me", headers={"X-API-Key": key["plain_text"]})
    query_key = await app_client.get("/api/v2/me", params={"api_key": key["plain_text"]})
    accepted = await app_client.get("/api/v2/me", headers=_bearer(key))

    assert x_api_key.status_code == 401
    assert x_api_key.headers["content-type"].startswith("application/problem+json")
    assert x_api_key.json()["code"] == "authentication_required"
    assert x_api_key.json()["request_id"] == x_api_key.headers["x-request-id"]
    assert query_key.status_code == 400
    assert query_key.json()["code"] == "query_credentials_not_allowed"
    assert accepted.status_code == 200
    assert accepted.json()["data"]["domain_grant_mode"] == "all"


@pytest.mark.asyncio
async def test_v2_hot_bearer_authentication_does_not_schedule_database_lookup(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.read"], rate_limit_per_min=0)
    first = await app_client.get("/api/v2/me", headers=_bearer(key))
    assert first.status_code == 200

    def unexpected_fallback(*_args, **_kwargs):
        raise AssertionError("hot API v2 authentication used the cold database path")

    monkeypatch.setattr(runtime.api_keys, "authenticate_plain_text", unexpected_fallback)
    second = await app_client.get("/api/v2/me", headers=_bearer(key))

    assert second.status_code == 200
    assert second.json()["data"]["id"] == first.json()["data"]["id"]


@pytest.mark.asyncio
async def test_v2_read_overload_returns_retryable_problem_details(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.read"], rate_limit_per_min=0)

    async def overloaded(_query, _params=()):
        raise SQLiteReadPoolOverloadedError("test admission queue is full")

    monkeypatch.setattr(runtime.read_pool, "fetch_all", overloaded)
    response = await app_client.get("/api/v2/domains", headers=_bearer(key))

    assert response.status_code == 503
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.headers["retry-after"] == "1"
    assert response.json()["code"] == "database_read_overloaded"
    assert response.json()["request_id"] == response.headers["x-request-id"]


@pytest.mark.asyncio
async def test_v2_forbids_extra_fields_and_uses_stable_validation_error(app_client, runtime) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.write"])

    response = await app_client.post(
        "/api/v2/domains",
        headers=_bearer(key),
        json={"root_domain": "strict-v2.example", "unexpected": True},
    )

    assert response.status_code == 422
    payload = response.json()
    assert payload["code"] == "validation_error"
    assert payload["type"] == "urn:rapid-inbox:problem:validation_error"
    assert payload["errors"]
    assert payload["request_id"] == response.headers["x-request-id"]


@pytest.mark.asyncio
async def test_v2_domain_create_accepts_retention_policy(app_client, runtime) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.write"])

    response = await app_client.post(
        "/api/v2/domains",
        headers=_bearer(key),
        json={"root_domain": "retained-v2.example", "retention_days": 14},
    )

    assert response.status_code == 201
    assert response.json()["data"]["retention_days"] == 14


@pytest.mark.asyncio
async def test_v2_settings_schema_rejects_values_above_runtime_limits(app_client, runtime) -> None:
    key = await _create_v2_key(runtime, scopes=["system.write"])

    response = await app_client.patch(
        "/api/v2/system/settings",
        headers=_bearer(key),
        json={"max_recipients_per_message": 10_001, "cleanup_batch_size": 1_000_001},
    )

    assert response.status_code == 422
    assert response.json()["code"] == "validation_error"


@pytest.mark.asyncio
async def test_v2_domain_keyset_and_selected_grants_are_fail_closed(app_client, runtime) -> None:
    first = await runtime.create_domain("a-v2.example")
    second = await runtime.create_domain("b-v2.example")
    all_key = await _create_v2_key(runtime, scopes=["domains.read"])

    first_page = await app_client.get("/api/v2/domains", headers=_bearer(all_key), params={"limit": 1})
    cursor = first_page.json()["page"]["next_cursor"]
    second_page = await app_client.get(
        "/api/v2/domains",
        headers=_bearer(all_key),
        params={"limit": 1, "cursor": cursor},
    )

    assert first_page.status_code == 200
    assert set(first_page.json()) == {"data", "page", "request_id"}
    assert "total_count" not in first_page.json()["page"]
    assert cursor
    assert second_page.status_code == 200
    assert first_page.json()["data"][0]["id"] != second_page.json()["data"][0]["id"]

    selected_key = await _create_v2_key(
        runtime,
        scopes=["domains.read", "domains.write"],
        domain_grant_mode="selected",
        domain_ids=[first["id"]],
    )
    selected_list = await app_client.get("/api/v2/domains", headers=_bearer(selected_key))
    hidden_detail = await app_client.get(f"/api/v2/domains/{second['id']}", headers=_bearer(selected_key))
    create_denied = await app_client.post(
        "/api/v2/domains",
        headers=_bearer(selected_key),
        json={"root_domain": "forbidden-create.example"},
    )
    policy_update = await app_client.patch(
        f"/api/v2/domains/{first['id']}",
        headers=_bearer(selected_key),
        json={"notes": "selected policy update"},
    )
    rename_denied = await app_client.patch(
        f"/api/v2/domains/{first['id']}",
        headers=_bearer(selected_key),
        json={"root_domain": "renamed-a-v2.example"},
    )
    delete_allowed = await app_client.delete(
        f"/api/v2/domains/{first['id']}",
        headers=_bearer(selected_key),
    )

    assert [item["id"] for item in selected_list.json()["data"]] == [first["id"]]
    assert hidden_detail.status_code == 404
    assert hidden_detail.json()["code"] == "domain_not_found"
    assert create_denied.status_code == 403
    assert create_denied.json()["code"] == "global_grant_required"
    assert policy_update.status_code == 200
    assert policy_update.json()["data"]["notes"] == "selected policy update"
    assert rename_denied.status_code == 403
    assert rename_denied.json()["code"] == "global_grant_required"
    assert delete_allowed.status_code == 200
    assert delete_allowed.json()["data"] == {"id": first["id"], "deleted": True}
    with pytest.raises(LookupError):
        runtime.domains.get_domain(first["id"])

    encoded, signature = cursor.split(".", 1)
    tampered_signature = ("A" if signature[0] != "A" else "B") + signature[1:]
    tampered = await app_client.get(
        "/api/v2/domains",
        headers=_bearer(all_key),
        params={"limit": 1, "cursor": f"{encoded}.{tampered_signature}"},
    )
    assert tampered.status_code == 400
    assert tampered.json()["code"] == "invalid_cursor"


@pytest.mark.asyncio
async def test_v2_domain_create_rejects_principal_revoked_after_request_authentication(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    key = await _create_v2_key(runtime, scopes=["domains.write"])
    original_create = runtime.create_domain

    async def revoke_before_create(root_domain: str, **kwargs):
        await runtime.api_keys.revoke_key(key["id"])
        return await original_create(root_domain, **kwargs)

    monkeypatch.setattr(runtime, "create_domain", revoke_before_create)
    response = await app_client.post(
        "/api/v2/domains",
        headers=_bearer(key),
        json={"root_domain": "revoked-create-v2.example"},
    )

    assert response.status_code == 403
    assert response.json()["code"] == "authorization_changed"
    assert "revoked-create-v2.example" not in {
        item["root_domain_ascii"] for item in runtime.domains.list_domains()
    }


@pytest.mark.asyncio
async def test_v2_message_and_mailbox_filters_reject_partial_multi_domain_access(
    app_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    first = await runtime.create_domain("messages-a-v2.example")
    second = await runtime.create_domain("messages-b-v2.example")
    await runtime.ensure_smtp_session(
        "smtp_v2_permissions",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    multi_domain_result = await runtime.accept_message(
        rcpt_tos=["foo@messages-a-v2.example", "bar@messages-b-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes.replace(b"Hello Rapid Inbox", b"V2 multi-domain"),
        smtp_session_id="smtp_v2_permissions",
    )
    allowed_result = await runtime.accept_message(
        rcpt_tos=["only@messages-a-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes.replace(b"Hello Rapid Inbox", b"V2 allowed"),
        smtp_session_id="smtp_v2_permissions",
    )
    await runtime.drain_parser_queue()
    multi_domain_message = str(multi_domain_result).rsplit(" ", 1)[-1]
    allowed_message = str(allowed_result).rsplit(" ", 1)[-1]

    key = await _create_v2_key(
        runtime,
        scopes=["mailboxes.read", "messages.read", "messages.write"],
        domain_grant_mode="selected",
        domain_ids=[first["id"]],
    )
    mailboxes = await app_client.get("/api/v2/mailboxes", headers=_bearer(key))
    messages = await app_client.get("/api/v2/messages", headers=_bearer(key))
    hidden_detail = await app_client.get(f"/api/v2/messages/{multi_domain_message}", headers=_bearer(key))
    hidden_reparse = await app_client.post(
        f"/api/v2/messages/{multi_domain_message}/reparse",
        headers=_bearer(key),
    )
    allowed_detail = await app_client.get(f"/api/v2/messages/{allowed_message}", headers=_bearer(key))

    mailbox_addresses = {item["address_canonical"] for item in mailboxes.json()["data"]}
    message_ids = {item["id"] for item in messages.json()["data"]}
    assert mailboxes.status_code == 200
    assert "bar@messages-b-v2.example" not in mailbox_addresses
    assert "foo@messages-a-v2.example" in mailbox_addresses
    assert multi_domain_message not in message_ids
    assert allowed_message in message_ids
    assert hidden_detail.status_code == 404
    assert hidden_reparse.status_code == 404
    assert allowed_detail.status_code == 200
    assert all(
        delivery["mailbox"].endswith("@messages-a-v2.example")
        for delivery in allowed_detail.json()["data"]["deliveries"]
    )
    assert second["id"] not in [item["domain_id"] for item in mailboxes.json()["data"]]


@pytest.mark.asyncio
async def test_v2_global_endpoints_and_admin_crud(app_client, runtime) -> None:
    key = await _create_v2_key(
        runtime,
        scopes=sorted(ROLE_SCOPES["superadmin"]),
    )
    created = await app_client.post(
        "/api/v2/admins",
        headers=_bearer(key),
        json={
            "username": "v2-viewer",
            "password": "v2-viewer-password",
            "role": "viewer",
            "must_change_password": False,
        },
    )
    admin_id = created.json()["data"]["id"]
    updated = await app_client.patch(
        f"/api/v2/admins/{admin_id}",
        headers=_bearer(key),
        json={"role": "operator"},
    )
    reset = await app_client.post(
        f"/api/v2/admins/{admin_id}/password",
        headers=_bearer(key),
        json={"password": "v2-reset-password", "must_change_password": True},
    )
    revoked = await app_client.post(
        f"/api/v2/admins/{admin_id}/sessions/revoke",
        headers=_bearer(key),
    )
    listed = await app_client.get("/api/v2/admins", headers=_bearer(key))
    settings = await app_client.get("/api/v2/system/settings", headers=_bearer(key))
    audit = await app_client.get("/api/v2/audit-events", headers=_bearer(key))
    deleted = await app_client.delete(f"/api/v2/admins/{admin_id}", headers=_bearer(key))

    assert created.status_code == 201
    assert updated.json()["data"]["role"] == "operator"
    assert reset.json()["data"]["must_change_password"] is True
    assert revoked.json()["data"] == {"admin_id": admin_id, "revoked_sessions": 0}
    assert admin_id in {item["id"] for item in listed.json()["data"]}
    assert settings.status_code == 200
    assert settings.json()["data"]["ingress_mode"] in {"managed_only", "managed_plus_catchall"}
    assert audit.status_code == 200
    assert any(item["action"] == "admins.create" for item in audit.json()["data"])
    assert deleted.json()["data"] == {"id": admin_id, "deleted": True}


@pytest.mark.asyncio
async def test_v2_admin_creation_requires_credentials_and_contained_role(app_client, runtime) -> None:
    write_only = await _create_v2_key(runtime, scopes=["admins.write"])
    missing_credentials = await app_client.post(
        "/api/v2/admins",
        headers=_bearer(write_only),
        json={
            "username": "forbidden-superadmin-v2",
            "password": "forbidden-superadmin-password",
            "role": "superadmin",
        },
    )

    bounded = await _create_v2_key(
        runtime,
        scopes=["admins.write", "admins.credentials.write"],
    )
    role_exceeds_actor = await app_client.post(
        "/api/v2/admins",
        headers=_bearer(bounded),
        json={
            "username": "forbidden-viewer-v2",
            "password": "forbidden-viewer-password",
            "role": "viewer",
        },
    )

    assert missing_credentials.status_code == 403
    assert missing_credentials.json()["code"] == "insufficient_scope"
    assert role_exceeds_actor.status_code == 403
    assert role_exceeds_actor.json()["code"] == "admin_delegation_forbidden"
    usernames = {item["username"] for item in runtime.auth.list_admins()["items"]}
    assert "forbidden-superadmin-v2" not in usernames
    assert "forbidden-viewer-v2" not in usernames


@pytest.mark.asyncio
async def test_v2_global_endpoints_reject_selected_domain_key(app_client, runtime) -> None:
    domain = await runtime.create_domain("selected-global-v2.example")
    key = await _create_v2_key(
        runtime,
        scopes=["audit.read", "system.read", "admins.read"],
        domain_grant_mode="selected",
        domain_ids=[domain["id"]],
    )

    for path in ("/api/v2/audit-events", "/api/v2/system/settings", "/api/v2/admins"):
        response = await app_client.get(path, headers=_bearer(key))
        assert response.status_code == 403
        assert response.json()["code"] == "global_grant_required"


@pytest.mark.asyncio
async def test_v2_resource_mutations_and_file_downloads(app_client, runtime) -> None:
    domain = await runtime.create_domain("resources-v2.example")
    disposable_domain = await runtime.create_domain("delete-v2.example")
    await runtime.ensure_smtp_session(
        "smtp_v2_resources",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    attachment_email = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: One <one@resources-v2.example>\r\n"
        b"Subject: V2 attachment\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=v2-boundary\r\n\r\n"
        b"--v2-boundary\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nhello\r\n"
        b"--v2-boundary\r\nContent-Type: application/octet-stream\r\n"
        b"Content-Disposition: attachment; filename=proof.txt\r\n"
        b"Content-Transfer-Encoding: base64\r\n\r\ncHJvb2YtYnl0ZXM=\r\n"
        b"--v2-boundary--\r\n"
    )
    accepted = await runtime.accept_message(
        rcpt_tos=["one@resources-v2.example", "two@resources-v2.example"],
        envelope_from="sender@example.com",
        content=attachment_email,
        smtp_session_id="smtp_v2_resources",
    )
    mailbox_message = await runtime.accept_message(
        rcpt_tos=["box@resources-v2.example"],
        envelope_from="sender@example.com",
        content=attachment_email.replace(b"V2 attachment", b"V2 mailbox delete"),
        smtp_session_id="smtp_v2_resources",
    )
    await runtime.drain_parser_queue()
    message_id = str(accepted).rsplit(" ", 1)[-1]
    mailbox_message_id = str(mailbox_message).rsplit(" ", 1)[-1]
    detail = runtime.messages.get_admin_message_detail(message_id)
    attachment_id = str(detail["attachments"][0]["id"])
    first_delivery_id = str(detail["deliveries"][0]["delivery_id"])
    box_mailbox = next(
        item
        for item in runtime.mailboxes.list_mailboxes(limit=20)["items"]
        if item["address_canonical"] == "box@resources-v2.example"
    )
    key = await _create_v2_key(
        runtime,
        scopes=["domains.write", "mailboxes.read", "mailboxes.write", "messages.read", "messages.write"],
        domain_grant_mode="selected",
        domain_ids=[domain["id"], disposable_domain["id"]],
    )

    raw = await app_client.get(f"/api/v2/messages/{message_id}/raw", headers=_bearer(key))
    attachment = await app_client.get(
        f"/api/v2/messages/{message_id}/attachments/{attachment_id}",
        headers=_bearer(key),
    )
    mailbox_update = await app_client.patch(
        f"/api/v2/mailboxes/{box_mailbox['id']}",
        headers=_bearer(key),
        json={"public_enabled": True, "notes": "managed by API v2"},
    )
    delivery_delete = await app_client.delete(
        f"/api/v2/messages/{message_id}/deliveries/{first_delivery_id}",
        headers=_bearer(key),
    )
    message_delete = await app_client.delete(f"/api/v2/messages/{message_id}", headers=_bearer(key))
    mailbox_delete = await app_client.delete(
        f"/api/v2/mailboxes/{box_mailbox['id']}",
        headers=_bearer(key),
    )
    domain_delete = await app_client.delete(
        f"/api/v2/domains/{disposable_domain['id']}",
        headers=_bearer(key),
    )

    assert raw.status_code == 200
    assert raw.headers["content-type"].startswith("message/rfc822")
    assert raw.content == attachment_email
    assert attachment.status_code == 200
    assert attachment.content == b"proof-bytes"
    assert "proof.txt" in attachment.headers["content-disposition"]
    assert mailbox_update.json()["data"]["public_enabled"] is True
    assert mailbox_update.json()["data"]["notes"] == "managed by API v2"
    assert delivery_delete.json()["data"]["affected"] == 1
    assert message_delete.json()["data"]["affected"] == 1
    assert mailbox_delete.json()["data"]["affected"] == 1
    assert mailbox_message_id
    assert domain_delete.json()["data"] == {"id": disposable_domain["id"], "deleted": True}


@pytest.mark.asyncio
async def test_v2_api_key_lifecycle_and_delegation_are_fail_closed(app_client, runtime) -> None:
    manager = await _create_v2_key(
        runtime,
        scopes=["api_keys.read", "api_keys.write", "messages.read", "public.read"],
    )
    created = await app_client.post(
        "/api/v2/api-keys",
        headers=_bearer(manager),
        json={
            "name": "v2-created-service",
            "kind": "service",
            "scopes": ["messages.read"],
            "domain_grant_mode": "all",
        },
    )
    created_payload = created.json()["data"]
    api_key_id = created_payload["api_key"]["id"]
    secret = created_payload["secret"]
    listed = await app_client.get("/api/v2/api-keys", headers=_bearer(manager), params={"limit": 1})
    next_page = await app_client.get(
        "/api/v2/api-keys",
        headers=_bearer(manager),
        params={"limit": 1, "cursor": listed.json()["page"]["next_cursor"]},
    )
    detail = await app_client.get(f"/api/v2/api-keys/{api_key_id}", headers=_bearer(manager))
    updated = await app_client.patch(
        f"/api/v2/api-keys/{api_key_id}",
        headers=_bearer(manager),
        json={"name": "v2-updated-service", "rate_limit_per_min": 1234},
    )
    rotated = await app_client.post(f"/api/v2/api-keys/{api_key_id}/rotate", headers=_bearer(manager))
    over_delegated = await app_client.post(
        "/api/v2/api-keys",
        headers=_bearer(manager),
        json={
            "name": "forbidden-super-key",
            "kind": "admin",
            "scopes": ["system.write"],
            "domain_grant_mode": "all",
        },
    )
    revoked = await app_client.post(f"/api/v2/api-keys/{api_key_id}/revoke", headers=_bearer(manager))
    deleted = await app_client.delete(f"/api/v2/api-keys/{api_key_id}", headers=_bearer(manager))
    missing = await app_client.get(f"/api/v2/api-keys/{api_key_id}", headers=_bearer(manager))

    assert created.status_code == 201
    assert created.headers["cache-control"] == "private, no-store"
    assert secret.startswith("ri_service_")
    assert listed.status_code == 200
    assert listed.json()["page"]["limit"] == 1
    assert next_page.status_code == 200
    assert listed.json()["data"][0]["id"] != next_page.json()["data"][0]["id"]
    assert detail.json()["data"]["name"] == "v2-created-service"
    assert updated.json()["data"]["name"] == "v2-updated-service"
    assert updated.json()["data"]["rate_limit_per_min"] == 1234
    assert rotated.json()["data"]["secret"].startswith("ri_service_")
    assert rotated.json()["data"]["secret"] != secret
    assert rotated.headers["cache-control"] == "private, no-store"
    assert over_delegated.status_code == 403
    assert over_delegated.json()["code"] == "api_key_delegation_denied"
    assert revoked.json()["data"]["status"] == "revoked"
    assert deleted.json()["data"] == {"id": api_key_id, "deleted": True}
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_v2_api_key_rotate_closes_authorization_toctou(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    manager = await _create_v2_key(
        runtime,
        scopes=["api_keys.read", "api_keys.write", "public.read"],
    )
    target = await _create_v2_key(runtime, scopes=["public.read"])
    original_prefix = target["key_prefix"]
    authorization_read = asyncio.Event()
    release_rotation = asyncio.Event()
    real_authorized_api_key = api_v2_module._authorized_api_key

    async def pause_after_authorization(request, principal, api_key_id):
        authorized = await real_authorized_api_key(request, principal, api_key_id)
        authorization_read.set()
        await release_rotation.wait()
        return authorized

    monkeypatch.setattr(api_v2_module, "_authorized_api_key", pause_after_authorization)
    rotate_task = asyncio.create_task(
        app_client.post(
            f"/api/v2/api-keys/{target['id']}/rotate",
            headers=_bearer(manager),
        )
    )
    await asyncio.wait_for(authorization_read.wait(), timeout=2)
    await runtime.api_keys.update_key(target["id"], scopes=["system.write"])
    release_rotation.set()
    response = await asyncio.wait_for(rotate_task, timeout=2)

    assert response.status_code == 404
    assert response.json()["code"] == "api_key_not_found"
    current = runtime.api_keys.get_key(target["id"])
    assert current["key_prefix"] == original_prefix
    assert current["scopes"] == ["system.write"]


@pytest.mark.asyncio
async def test_v2_api_key_delegation_preserves_parent_operational_policy(app_client, runtime) -> None:
    parent = await _create_v2_key(
        runtime,
        scopes=["api_keys.read", "api_keys.write", "public.read"],
        rate_limit_per_min=10,
    )
    parent = await runtime.api_keys.update_key(
        parent["id"],
        allowed_ip_cidrs=["127.0.0.0/8"],
        expires_at="2099-01-01T00:00:00Z",
    )
    parent["plain_text"] = (await runtime.api_keys.rotate_key(parent["id"]))["plain_text"]

    over_broad = await app_client.post(
        "/api/v2/api-keys",
        headers=_bearer(parent),
        json={
            "name": "over-broad-policy",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "rate_limit_per_min": 0,
            "allowed_ip_cidrs": [],
            "expires_at": None,
        },
    )
    compliant = await app_client.post(
        "/api/v2/api-keys",
        headers=_bearer(parent),
        json={
            "name": "contained-policy",
            "kind": "public",
            "scopes": ["public.read"],
            "domain_grant_mode": "all",
            "rate_limit_per_min": 5,
            "allowed_ip_cidrs": ["127.1.0.0/16"],
            "expires_at": "2098-01-01T00:00:00Z",
        },
    )
    child_id = compliant.json()["data"]["api_key"]["id"]
    widened = await app_client.patch(
        f"/api/v2/api-keys/{child_id}",
        headers=_bearer(parent),
        json={"allowed_ip_cidrs": ["0.0.0.0/0"]},
    )
    perpetual = await app_client.patch(
        f"/api/v2/api-keys/{child_id}",
        headers=_bearer(parent),
        json={"expires_at": None},
    )

    assert over_broad.status_code == 403
    assert over_broad.json()["code"] == "api_key_delegation_denied"
    assert compliant.status_code == 201
    assert widened.status_code == 403
    assert perpetual.status_code == 403


@pytest.mark.asyncio
async def test_v2_api_key_list_scan_budget_returns_continuation_through_invisible_rows(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    manager = await _create_v2_key(runtime, scopes=["api_keys.read"])

    def insert_hidden_keys(connection) -> None:
        connection.execute("BEGIN IMMEDIATE")
        for index in range(1100):
            cursor = connection.execute(
                """
                INSERT INTO api_keys (
                    public_id, name, kind, key_prefix, secret_hash,
                    domain_grant_mode, created_at
                ) VALUES (?, ?, 'public', ?, ?, 'none', '2099-01-01T00:00:00Z')
                """,
                (
                    f"hidden-public-{index}",
                    f"Hidden public {index}",
                    f"hidden-prefix-{index}",
                    f"hidden-hash-{index}",
                ),
            )
            connection.execute(
                "INSERT INTO api_key_scopes (api_key_id, scope) VALUES (?, 'public.read')",
                (int(cursor.lastrowid),),
            )

    await runtime.writer.execute(insert_hidden_keys)

    hydrated_batch_sizes: list[int] = []
    real_hydrate = api_v2_module._hydrate_api_key_rows

    def counting_hydrate(connection, rows):
        hydrated_batch_sizes.append(len(rows))
        return real_hydrate(connection, rows)

    monkeypatch.setattr(api_v2_module, "_hydrate_api_key_rows", counting_hydrate)
    first = await app_client.get(
        "/api/v2/api-keys",
        headers=_bearer(manager),
        params={"limit": 1},
    )

    assert first.status_code == 200
    assert first.json()["data"] == []
    assert first.json()["page"]["has_more"] is True
    assert first.json()["page"]["next_cursor"]
    assert sum(hydrated_batch_sizes) == api_v2_module.API_KEY_SCAN_MIN_ROWS
    assert max(hydrated_batch_sizes) <= api_v2_module.API_KEY_SCAN_BATCH_MAX_ROWS
    assert len(hydrated_batch_sizes) <= 8

    hydrated_batch_sizes.clear()
    second = await app_client.get(
        "/api/v2/api-keys",
        headers=_bearer(manager),
        params={"limit": 1, "cursor": first.json()["page"]["next_cursor"]},
    )

    assert second.status_code == 200
    assert [item["id"] for item in second.json()["data"]] == [manager["id"]]
    assert second.json()["page"]["has_more"] is False
    assert sum(hydrated_batch_sizes) == 101


@pytest.mark.asyncio
async def test_v2_dashboard_and_cleanup_are_typed_global_operations(app_client, runtime) -> None:
    key = await _create_v2_key(runtime, scopes=["system.read", "system.write"])

    dashboard = await app_client.get("/api/v2/dashboard/status", headers=_bearer(key))
    cleanup = await app_client.post("/api/v2/maintenance/cleanup", headers=_bearer(key))

    assert dashboard.status_code == 200
    dashboard_data = dashboard.json()["data"]
    assert dashboard_data["health"]["status"] in {"ok", "warning", "danger"}
    assert dashboard_data["database"]["ok"] is True
    assert dashboard_data["ingestd"]["state"] == "missing"
    assert dashboard_data["ingestd"]["online"] is False
    assert dashboard_data["ingestd"]["queue_messages"] is None
    assert set(dashboard_data["totals"]) == {"domains", "mailboxes", "messages", "api_keys", "audit_logs"}
    assert cleanup.status_code == 200
    assert cleanup.json()["data"]["operation"] == "cleanup_expired_messages"
    assert "deliveries" in cleanup.json()["data"]["result"]


@pytest.mark.asyncio
async def test_v2_public_mailbox_is_bearer_only_keyset_paginated_and_downloadable(
    app_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    domain = await runtime.create_domain(
        "public-v2.example",
        public_api_enabled=True,
        public_web_enabled=False,
    )
    other_domain = await runtime.create_domain(
        "other-public-v2.example",
        public_api_enabled=True,
    )
    attachment_email = (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@public-v2.example>\r\n"
        b"Subject: Public V2 attachment\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/mixed; boundary=public-v2-boundary\r\n\r\n"
        b"--public-v2-boundary\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n"
        b"Verification code: 654321\r\n"
        b"--public-v2-boundary\r\nContent-Type: application/octet-stream\r\n"
        b"Content-Disposition: attachment; filename=public-proof.txt\r\n"
        b"Content-Transfer-Encoding: base64\r\n\r\ncHVibGljLXByb29m\r\n"
        b"--public-v2-boundary--\r\n"
    )
    first_result = await runtime.accept_message(
        rcpt_tos=["foo@public-v2.example", "other@other-public-v2.example"],
        envelope_from="sender@example.com",
        content=attachment_email,
    )
    await runtime.accept_message(
        rcpt_tos=["foo@public-v2.example", "bar@public-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes.replace(b"Hello Rapid Inbox", b"Public V2 second"),
    )
    await runtime.drain_parser_queue()
    first_message_id = str(first_result).rsplit(" ", 1)[-1]
    await runtime.writer.execute(
        lambda connection: connection.execute(
            "UPDATE messages SET verification_code = ? WHERE id = ?",
            ("654321", first_message_id),
        )
    )
    mailbox = await runtime.get_mailbox_view("foo@public-v2.example")
    first_item = next(item for item in mailbox["items"] if item["message_id"] == first_message_id)
    first_delivery_id = str(first_item["delivery_id"])
    first_detail = runtime.messages.get_admin_message_detail(first_message_id)
    attachment_id = str(first_detail["attachments"][0]["id"])
    public_key = await _create_v2_key(
        runtime,
        kind="public",
        scopes=["public.read"],
        domain_grant_mode="selected",
        domain_ids=[domain["id"]],
        mailbox_patterns=["foo@public-v2.example"],
    )

    me = await app_client.get("/api/v2/me", headers=_bearer(public_key))
    first_page = await app_client.get(
        "/api/v2/public/mailboxes/foo@public-v2.example/messages",
        headers=_bearer(public_key),
        params={"limit": 1},
    )
    cursor = first_page.json()["page"]["next_cursor"]
    second_page = await app_client.get(
        "/api/v2/public/mailboxes/foo@public-v2.example/messages",
        headers=_bearer(public_key),
        params={"limit": 1, "cursor": cursor},
    )
    detail = await app_client.get(
        f"/api/v2/public/mailboxes/foo@public-v2.example/messages/{first_delivery_id}",
        headers=_bearer(public_key),
    )
    codes = await app_client.get(
        "/api/v2/public/mailboxes/foo@public-v2.example/verification-codes",
        headers=_bearer(public_key),
    )
    code = await app_client.get(
        f"/api/v2/public/mailboxes/foo@public-v2.example/messages/{first_delivery_id}/verification-code",
        headers=_bearer(public_key),
    )
    raw = await app_client.get(
        f"/api/v2/public/mailboxes/foo@public-v2.example/messages/{first_delivery_id}/raw",
        headers=_bearer(public_key),
    )
    attachment = await app_client.get(
        f"/api/v2/public/mailboxes/foo@public-v2.example/messages/{first_delivery_id}/attachments/{attachment_id}",
        headers=_bearer(public_key),
    )
    mailbox_denied = await app_client.get(
        "/api/v2/public/mailboxes/bar@public-v2.example/messages",
        headers=_bearer(public_key),
    )
    domain_denied = await app_client.get(
        "/api/v2/public/mailboxes/other@other-public-v2.example/messages",
        headers=_bearer(public_key),
    )
    encoded, signature = cursor.split(".", 1)
    tampered = await app_client.get(
        "/api/v2/public/mailboxes/foo@public-v2.example/messages",
        headers=_bearer(public_key),
        params={"limit": 1, "cursor": f"{encoded}.{'A' if signature[0] != 'A' else 'B'}{signature[1:]}"},
    )

    assert me.status_code == 200
    assert me.json()["data"]["kind"] == "public"
    assert first_page.status_code == 200
    assert cursor
    assert second_page.status_code == 200
    assert first_page.json()["data"][0]["delivery_id"] != second_page.json()["data"][0]["delivery_id"]
    assert detail.status_code == 200
    assert detail.json()["data"]["mailbox"] == "foo@public-v2.example"
    assert "storage_path" not in str(detail.json())
    assert any(item["verification_code"] == "654321" for item in codes.json()["data"])
    assert code.json()["data"]["verification_code"] == "654321"
    assert raw.content == attachment_email
    assert raw.headers["cache-control"] == "private, no-store"
    assert attachment.content == b"public-proof"
    assert attachment.headers["x-content-type-options"] == "nosniff"
    assert mailbox_denied.status_code == 404
    assert domain_denied.status_code == 404
    assert tampered.status_code == 400
    assert other_domain["id"] not in me.json()["data"]["domain_ids"]

    for method, path in (
        ("GET", "/api/v2/domains"),
        ("GET", "/api/v2/messages"),
        ("GET", "/api/v2/smtp-sessions"),
        ("POST", "/api/v2/maintenance/clear-all"),
    ):
        denied = await app_client.request(method, path, headers=_bearer(public_key))
        assert denied.status_code == 403
        assert denied.json()["code"] == "insufficient_scope"


@pytest.mark.asyncio
async def test_v2_public_mailbox_enforces_domain_and_mailbox_public_flags(
    app_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    enabled = await runtime.create_domain("visibility-v2.example", public_api_enabled=True)
    await runtime.create_domain("disabled-api-v2.example", public_api_enabled=False)
    await runtime.accept_message(
        rcpt_tos=["foo@visibility-v2.example", "foo@disabled-api-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    key = await _create_v2_key(
        runtime,
        kind="public",
        scopes=["public.read"],
        domain_grant_mode="all",
    )

    enabled_response = await app_client.get(
        "/api/v2/public/mailboxes/foo@visibility-v2.example/messages",
        headers=_bearer(key),
    )
    domain_disabled = await app_client.get(
        "/api/v2/public/mailboxes/foo@disabled-api-v2.example/messages",
        headers=_bearer(key),
    )
    mailbox = next(
        item
        for item in runtime.mailboxes.list_mailboxes(domain_id=enabled["id"])["items"]
        if item["address_canonical"] == "foo@visibility-v2.example"
    )
    await runtime.mailboxes.update_mailbox(mailbox["id"], {"public_enabled": False})
    mailbox_disabled = await app_client.get(
        "/api/v2/public/mailboxes/foo@visibility-v2.example/messages",
        headers=_bearer(key),
    )

    assert enabled_response.status_code == 200
    assert domain_disabled.status_code == 404
    assert mailbox_disabled.status_code == 404


@pytest.mark.asyncio
async def test_v2_public_read_is_rate_limited_once_per_request(
    app_client,
    runtime,
    sample_email_bytes: bytes,
) -> None:
    await runtime.create_domain("rate-public-v2.example", public_api_enabled=True)
    await runtime.accept_message(
        rcpt_tos=["foo@rate-public-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    key = await _create_v2_key(
        runtime,
        kind="public",
        scopes=["public.read"],
        domain_grant_mode="all",
        rate_limit_per_min=1,
    )
    path = "/api/v2/public/mailboxes/foo@rate-public-v2.example/messages"

    first = await app_client.get(path, headers=_bearer(key))
    second = await app_client.get(path, headers=_bearer(key))

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.json()["code"] == "rate_limit_exceeded"


@pytest.mark.asyncio
async def test_v2_smtp_sessions_and_events_use_signed_keyset_cursors(app_client, runtime) -> None:
    for session_id, port in (("smtp_v2_c", 2503), ("smtp_v2_b", 2502), ("smtp_v2_a", 2501)):
        await runtime.ensure_smtp_session(
            session_id,
            SimpleNamespace(peer=("192.0.2.10", port), host_name=f"{session_id}.example", ssl=None),
        )
    await runtime.record_smtp_event("smtp_v2_a", "connect", {"peer": "192.0.2.10"})
    await runtime.record_smtp_event("smtp_v2_a", "helo", {"helo": "mx.example"})
    key = await _create_v2_key(runtime, scopes=["smtp.read"])

    first_page = await app_client.get(
        "/api/v2/smtp-sessions",
        headers=_bearer(key),
        params={"limit": 1},
    )
    cursor = first_page.json()["page"]["next_cursor"]
    second_page = await app_client.get(
        "/api/v2/smtp-sessions",
        headers=_bearer(key),
        params={"limit": 1, "cursor": cursor},
    )
    detail = await app_client.get(
        "/api/v2/smtp-sessions/smtp_v2_a",
        headers=_bearer(key),
        params={"event_limit": 1},
    )
    event_cursor = detail.json()["data"]["events_page"]["next_cursor"]
    next_events = await app_client.get(
        "/api/v2/smtp-sessions/smtp_v2_a/events",
        headers=_bearer(key),
        params={"limit": 1, "cursor": event_cursor},
    )
    encoded, signature = cursor.split(".", 1)
    tampered = await app_client.get(
        "/api/v2/smtp-sessions",
        headers=_bearer(key),
        params={"limit": 1, "cursor": f"{encoded}.{'A' if signature[0] != 'A' else 'B'}{signature[1:]}"},
    )
    cross_session_cursor = await app_client.get(
        "/api/v2/smtp-sessions/smtp_v2_b/events",
        headers=_bearer(key),
        params={"limit": 1, "cursor": event_cursor},
    )

    assert first_page.status_code == 200
    assert cursor
    assert first_page.json()["data"][0]["id"] != second_page.json()["data"][0]["id"]
    assert detail.status_code == 200
    assert detail.json()["data"]["events"][0]["event_type"] == "connect"
    assert detail.json()["data"]["events"][0]["payload"] == {"peer": "192.0.2.10"}
    assert event_cursor
    assert next_events.json()["data"][0]["event_type"] == "helo"
    assert tampered.status_code == 400
    assert cross_session_cursor.status_code == 400

    selected_domain = await runtime.create_domain("smtp-selected-v2.example")
    selected_key = await _create_v2_key(
        runtime,
        scopes=["smtp.read"],
        domain_grant_mode="selected",
        domain_ids=[selected_domain["id"]],
    )
    denied = await app_client.get("/api/v2/smtp-sessions", headers=_bearer(selected_key))
    assert denied.status_code == 403
    assert denied.json()["code"] == "global_grant_required"


@pytest.mark.asyncio
async def test_v2_domain_dns_check_and_clear_all_are_authorized_and_typed(
    app_client,
    runtime,
    monkeypatch,
    sample_email_bytes: bytes,
) -> None:
    first = await runtime.create_domain("dns-v2.example")
    second = await runtime.create_domain("dns-hidden-v2.example")

    async def fake_dns_check(_self, root_domain: str) -> dict:
        assert root_domain == "dns-v2.example"
        return {"status": "ok", "mx_records": ["mx.dns-v2.example"]}

    monkeypatch.setattr("app.http.api_v2.DnsCheckService.run_dns_check", fake_dns_check)
    selected = await _create_v2_key(
        runtime,
        scopes=["domains.write"],
        domain_grant_mode="selected",
        domain_ids=[first["id"]],
    )
    checked = await app_client.post(
        f"/api/v2/domains/{first['id']}/dns-check",
        headers=_bearer(selected),
    )
    denied_check = await app_client.post(
        f"/api/v2/domains/{second['id']}/dns-check",
        headers=_bearer(selected),
    )

    assert checked.status_code == 200
    assert checked.json()["data"]["dns_status"] == "ok"
    assert checked.json()["data"]["dns_details"]["mx_records"] == ["mx.dns-v2.example"]
    assert denied_check.status_code == 404

    await runtime.system_settings.update_settings({"ingress_mode": "managed_plus_catchall"})
    catch_all = next(
        item
        for item in runtime.domains.list_domains()
        if item["root_domain_ascii"] == "*"
    )
    global_dns_key = await _create_v2_key(runtime, scopes=["domains.write"])
    catch_all_check = await app_client.post(
        f"/api/v2/domains/{catch_all['id']}/dns-check",
        headers=_bearer(global_dns_key),
    )
    assert catch_all_check.status_code == 422
    assert catch_all_check.json()["code"] == "dns_check_not_supported"

    await runtime.accept_message(
        rcpt_tos=["foo@dns-v2.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    global_key = await _create_v2_key(runtime, scopes=["system.write"])
    clear = await app_client.post("/api/v2/maintenance/clear-all", headers=_bearer(global_key))
    selected_system_key = await _create_v2_key(
        runtime,
        scopes=["system.write"],
        domain_grant_mode="selected",
        domain_ids=[first["id"]],
    )
    denied_clear = await app_client.post(
        "/api/v2/maintenance/clear-all",
        headers=_bearer(selected_system_key),
    )

    assert clear.status_code == 200
    assert clear.json()["data"]["operation"] == "clear_all_mail"
    assert clear.json()["data"]["result"]["messages"] == 1
    assert denied_clear.status_code == 403
    assert denied_clear.json()["code"] == "global_grant_required"


@pytest.mark.asyncio
async def test_v2_system_mutations_reject_principal_revoked_after_request_authentication(
    app_client,
    runtime,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    original_limit = int(runtime.settings.max_recipients_per_message)
    settings_key = await _create_v2_key(runtime, scopes=["system.write"])
    real_update_settings = runtime.system_settings.update_settings

    async def revoke_before_settings_write(payload, *, authorization_principal=None):
        await runtime.api_keys.revoke_key(settings_key["id"])
        return await real_update_settings(
            payload,
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(
        runtime.system_settings,
        "update_settings",
        revoke_before_settings_write,
    )
    settings_response = await app_client.patch(
        "/api/v2/system/settings",
        headers=_bearer(settings_key),
        json={"max_recipients_per_message": original_limit + 1},
    )
    monkeypatch.setattr(runtime.system_settings, "update_settings", real_update_settings)

    assert settings_response.status_code == 403
    assert settings_response.json()["code"] == "authorization_changed"
    assert int(runtime.settings.max_recipients_per_message) == original_limit

    await runtime.create_domain("revoked-v2-clear.example")
    queued = await runtime.accept_message(
        rcpt_tos=["box@revoked-v2-clear.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    message_id = queued.removeprefix("250 queued as ")
    clear_key = await _create_v2_key(runtime, scopes=["system.write"])
    real_clear_all = runtime.clear_all_mail

    async def revoke_before_clear(*, authorization_principal=None):
        await runtime.api_keys.revoke_key(clear_key["id"])
        return await real_clear_all(
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(runtime, "clear_all_mail", revoke_before_clear)
    clear_response = await app_client.post(
        "/api/v2/maintenance/clear-all",
        headers=_bearer(clear_key),
    )

    assert clear_response.status_code == 403
    assert clear_response.json()["code"] == "authorization_changed"
    assert runtime.messages.get_admin_message_detail(message_id)["id"] == message_id

    with connect_database(
        runtime.settings.database_path,
        durable_writes=True,
    ) as connection:
        connection.execute(
            """
            UPDATE message_deliveries
            SET expires_at = '2000-01-01T00:00:00Z'
            WHERE message_id = ?
            """,
            (message_id,),
        )

    cleanup_key = await _create_v2_key(runtime, scopes=["system.write"])
    real_cleanup = runtime.cleanup_expired_messages

    async def revoke_before_cleanup(*, authorization_principal=None):
        await runtime.api_keys.revoke_key(cleanup_key["id"])
        return await real_cleanup(
            authorization_principal=authorization_principal,
        )

    monkeypatch.setattr(runtime, "cleanup_expired_messages", revoke_before_cleanup)
    cleanup_response = await app_client.post(
        "/api/v2/maintenance/cleanup",
        headers=_bearer(cleanup_key),
    )

    assert cleanup_response.status_code == 403
    assert cleanup_response.json()["code"] == "authorization_changed"
    assert runtime.messages.get_admin_message_detail(message_id)["id"] == message_id
