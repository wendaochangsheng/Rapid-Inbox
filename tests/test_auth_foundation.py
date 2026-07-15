from __future__ import annotations

import asyncio
import sqlite3
import threading

import pytest
from fastapi import HTTPException

from app.auth.api_keys import ApiKeyAuthorizationError, ApiKeyService
from app.auth.permissions import (
    PermissionContext,
    PermissionDenied,
    ROLE_SCOPES,
    delegated_api_key_policy_is_within_principal,
    ensure_mailbox_access,
    require_admin_role_scope,
    role_permission_context,
)
from app.auth.sessions import DUMMY_PASSWORD_HASH
from app.db.connection import connect_database
from app.db.writer import DatabaseWriter
from app.services.domains import domain_routing_tombstone_key


def test_role_scopes_keep_high_risk_mutations_superadmin_only() -> None:
    assert "messages.read" in ROLE_SCOPES["viewer"]
    assert "messages.write" not in ROLE_SCOPES["viewer"]

    assert "messages.write" in ROLE_SCOPES["operator"]
    assert "system.write" not in ROLE_SCOPES["operator"]
    assert "api_keys.write" not in ROLE_SCOPES["operator"]
    assert "admins.write" not in ROLE_SCOPES["operator"]

    assert "system.write" in ROLE_SCOPES["superadmin"]
    assert "api_keys.write" in ROLE_SCOPES["superadmin"]
    assert "admins.write" in ROLE_SCOPES["superadmin"]


def test_role_permission_context_and_scope_guard() -> None:
    viewer = role_permission_context({"id": 7, "username": "reader", "role": "viewer"})

    assert viewer.kind == "admin"
    assert viewer.domain_grant_mode == "all"
    assert require_admin_role_scope(viewer, "messages.read") is viewer
    with pytest.raises(PermissionDenied):
        require_admin_role_scope(viewer, "messages.write")


def test_selected_empty_domain_grants_are_denied_and_globs_are_bounded() -> None:
    selected_empty = PermissionContext(
        scopes=("public.read",),
        domain_ids=(),
        mailbox_patterns=(),
        domain_grant_mode="selected",
    )
    with pytest.raises(PermissionDenied):
        ensure_mailbox_access(selected_empty, "foo@adb.com", 1, "public.read")

    selected = PermissionContext(
        scopes=("public.read",),
        domain_ids=(1,),
        mailbox_patterns=("*@adb.com", "support@*"),
        domain_grant_mode="selected",
    )
    ensure_mailbox_access(selected, "foo@adb.com", 1, "public.read")
    ensure_mailbox_access(selected, "support@example.com", 1, "public.read")
    with pytest.raises(PermissionDenied):
        ensure_mailbox_access(selected, "foo@example.com", 1, "public.read")


def test_delegated_api_key_policy_cannot_widen_parent_limits() -> None:
    parent = PermissionContext(
        scopes=("api_keys.write",),
        domain_ids=(),
        mailbox_patterns=(),
        domain_grant_mode="all",
        kind="admin",
        rate_limit_per_min=10,
        allowed_ip_cidrs=("127.0.0.0/8", "2001:db8::/32"),
        expires_at="2099-01-01T00:00:00Z",
        allow_header=True,
        allow_query=False,
    )
    base = {
        "rate_limit_per_min": 5,
        "allowed_ip_cidrs": ["127.1.0.0/16", "2001:db8:1::/48"],
        "expires_at": "2098-01-01T00:00:00Z",
        "allow_header": True,
        "allow_query": False,
    }

    assert delegated_api_key_policy_is_within_principal(parent, base)
    assert not delegated_api_key_policy_is_within_principal(
        parent, {**base, "rate_limit_per_min": 0}
    )
    assert not delegated_api_key_policy_is_within_principal(
        parent, {**base, "allowed_ip_cidrs": []}
    )
    assert not delegated_api_key_policy_is_within_principal(
        parent, {**base, "allowed_ip_cidrs": ["0.0.0.0/0"]}
    )
    assert not delegated_api_key_policy_is_within_principal(
        parent, {**base, "expires_at": None}
    )
    assert not delegated_api_key_policy_is_within_principal(
        parent, {**base, "allow_query": True}
    )


@pytest.mark.asyncio
async def test_api_key_domain_modes_stay_fail_closed_after_grant_cascade(runtime) -> None:
    domain = await runtime.create_domain("selected.example")
    key = await runtime.api_keys.create_key(
        name="selected-domain",
        kind="public",
        scopes=["public.read"],
        domain_ids=[domain["id"]],
        mailbox_patterns=[],
        domain_grant_mode="selected",
    )

    context = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    ensure_mailbox_access(context, "box@selected.example", domain["id"], "public.read")

    await runtime.domains.delete_domain(domain["id"])
    context_after_delete = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    assert context_after_delete.domain_grant_mode == "selected"
    assert context_after_delete.domain_ids == ()
    with pytest.raises(PermissionDenied):
        ensure_mailbox_access(
            context_after_delete,
            "box@another.example",
            domain["id"] + 1,
            "public.read",
        )


@pytest.mark.asyncio
async def test_api_key_scope_kind_validation_and_safe_rotation(runtime) -> None:
    fail_closed_key = await runtime.api_keys.create_key(
        name="implicit-empty-grants",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        mailbox_patterns=[],
    )
    assert fail_closed_key["domain_grant_mode"] == "none"

    with pytest.raises(ValueError, match="cannot use scope"):
        await runtime.api_keys.create_key(
            name="public-admin-scope",
            kind="public",
            scopes=["messages.read"],
            domain_ids=[],
            mailbox_patterns=[],
            domain_grant_mode="none",
        )
    with pytest.raises(ValueError, match="invalid api key scope"):
        await runtime.api_keys.create_key(
            name="unknown-scope",
            kind="admin",
            scopes=["everything.write"],
            domain_ids=[],
            mailbox_patterns=[],
            domain_grant_mode="none",
        )
    with pytest.raises(ValueError, match="invalid mailbox pattern"):
        await runtime.api_keys.create_key(
            name="ambiguous-mailbox-glob",
            kind="public",
            scopes=["public.read"],
            domain_ids=[],
            mailbox_patterns=["[!a]*@example.com"],
            domain_grant_mode="all",
        )

    key = await runtime.api_keys.create_key(
        name="revoked-key",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        mailbox_patterns=["*@example.com"],
        domain_grant_mode="all",
    )
    context = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    ensure_mailbox_access(context, "foo@example.com", 999, "public.read")
    with pytest.raises(PermissionDenied):
        ensure_mailbox_access(context, "foo@elsewhere.example", 999, "public.read")
    await runtime.api_keys.revoke_key(key["id"])
    with pytest.raises(ValueError, match="only active"):
        await runtime.api_keys.rotate_key(key["id"])
    assert runtime.api_keys.get_key(key["id"])["status"] == "revoked"


@pytest.mark.asyncio
async def test_api_key_expiration_is_normalized_and_malformed_values_fail_closed(runtime) -> None:
    normalized = await runtime.api_keys.create_key(
        name="future-expiry",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
        expires_at="2031-01-01T01:00:00+01:00",
    )
    assert normalized["expires_at"] == "2031-01-01T00:00:00Z"
    assert runtime.api_keys.authenticate_plain_text(normalized["plain_text"]).api_key_id == normalized["id"]

    with pytest.raises(ValueError, match="expires_at"):
        await runtime.api_keys.create_key(
            name="naive-expiry",
            kind="service",
            scopes=["system.read"],
            domain_ids=[],
            mailbox_patterns=[],
            domain_grant_mode="all",
            expires_at="2031-01-01T00:00:00",
        )

    expired = await runtime.api_keys.create_key(
        name="expired-key",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
        expires_at="2000-01-01T00:00:00Z",
    )
    with pytest.raises(LookupError, match="expired api key"):
        runtime.api_keys.authenticate_plain_text(expired["plain_text"])
    with pytest.raises(ValueError, match="expired api keys"):
        await runtime.api_keys.rotate_key(expired["id"])

    malformed = await runtime.api_keys.create_key(
        name="malformed-persisted-expiry",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )
    with connect_database(runtime.settings.database_path) as connection:
        connection.execute(
            "UPDATE api_keys SET expires_at = 'not-a-timestamp' WHERE id = ?",
            (malformed["id"],),
        )
    runtime.api_keys._invalidate_key_cache(malformed["id"])
    with pytest.raises(LookupError, match="expired api key"):
        runtime.api_keys.authenticate_plain_text(malformed["plain_text"])


@pytest.mark.asyncio
async def test_admin_crud_and_last_superadmin_guard(runtime) -> None:
    bootstrap = runtime.auth.list_admins()["items"][0]
    viewer = await runtime.auth.create_admin(
        username="reader",
        password="reader-password-1",
        role="viewer",
        display_name="Reader",
    )
    assert runtime.auth.get_admin(viewer["id"])["role"] == "viewer"

    updated = await runtime.auth.update_admin(viewer["id"], role="operator", is_active=False)
    assert updated["role"] == "operator"
    assert updated["is_active"] is False

    with pytest.raises(ValueError, match="last active superadmin"):
        await runtime.auth.update_admin(bootstrap["id"], is_active=False)
    with pytest.raises(ValueError, match="last active superadmin"):
        await runtime.auth.delete_admin(bootstrap["id"])
    with connect_database(runtime.settings.database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError, match="last active superadmin"):
            connection.execute(
                "UPDATE admins SET role = 'operator' WHERE id = ?",
                (bootstrap["id"],),
            )

    second_superadmin = await runtime.auth.create_admin(
        username="root-two",
        password="second-superadmin-password",
        role="superadmin",
    )
    demoted = await runtime.auth.update_admin(bootstrap["id"], role="operator")
    assert demoted["role"] == "operator"
    await runtime.auth.update_admin(viewer["id"], role="superadmin", is_active=True)
    deleted = await runtime.auth.delete_admin(second_superadmin["id"])
    assert deleted["username"] == "root-two"


@pytest.mark.asyncio
async def test_admin_delegation_is_contained_and_reloads_api_key_in_transaction(runtime) -> None:
    bootstrap = runtime.auth.list_admins()["items"][0]
    bounded_key = await runtime.api_keys.create_key(
        name="bounded-admin-manager",
        kind="admin",
        scopes=["admins.write", "admins.credentials.write"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )
    bounded = runtime.api_keys.authenticate_plain_text(bounded_key["plain_text"])

    with pytest.raises(ApiKeyAuthorizationError, match="role exceeds"):
        await runtime.auth.create_admin(
            username="forbidden-viewer",
            password="forbidden-viewer-password",
            role="viewer",
            authorization_principal=bounded,
        )
    with pytest.raises(ApiKeyAuthorizationError, match="role exceeds"):
        await runtime.auth.update_admin(
            bootstrap["id"],
            display_name="should-not-change",
            authorization_principal=bounded,
        )
    with pytest.raises(ApiKeyAuthorizationError, match="role exceeds"):
        await runtime.auth.reset_admin_password(
            bootstrap["id"],
            "should-not-become-the-password",
            authorization_principal=bounded,
        )
    assert {item["username"] for item in runtime.auth.list_admins()["items"]}.isdisjoint(
        {"forbidden-viewer"}
    )
    assert runtime.auth.get_admin(bootstrap["id"])["display_name"] != "should-not-change"

    full_key = await runtime.api_keys.create_key(
        name="revoked-admin-manager",
        kind="admin",
        scopes=sorted(ROLE_SCOPES["superadmin"]),
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )
    stale_full_context = runtime.api_keys.authenticate_plain_text(full_key["plain_text"])
    await runtime.api_keys.update_key(full_key["id"], scopes=["admins.write"])

    with pytest.raises(ApiKeyAuthorizationError, match="admins.credentials.write"):
        await runtime.auth.create_admin(
            username="stale-key-created-admin",
            password="stale-key-created-password",
            role="viewer",
            authorization_principal=stale_full_context,
        )
    assert "stale-key-created-admin" not in {
        item["username"] for item in runtime.auth.list_admins()["items"]
    }


@pytest.mark.asyncio
async def test_admin_mutation_reloads_human_session_role_in_transaction(runtime) -> None:
    actor = await runtime.auth.create_admin(
        username="human-session-actor",
        password="human-session-actor-password",
        role="superadmin",
    )
    session = await runtime.auth.create_session(
        admin_id=actor["id"],
        ip="127.0.0.1",
        user_agent="pytest",
    )
    stale_context = role_permission_context(
        await runtime.auth.get_session_admin(session["token"], ip="127.0.0.1")
    )
    await runtime.auth.update_admin(actor["id"], role="operator")

    with pytest.raises(ApiKeyAuthorizationError, match="admins.write"):
        await runtime.auth.create_admin(
            username="stale-human-created-admin",
            password="stale-human-created-password",
            role="viewer",
            authorization_principal=stale_context,
        )
    assert "stale-human-created-admin" not in {
        item["username"] for item in runtime.auth.list_admins()["items"]
    }


@pytest.mark.asyncio
async def test_password_change_preserves_current_session_and_revokes_others(runtime) -> None:
    admin = await runtime.auth.create_admin(
        username="session-owner",
        password="original-password",
        role="operator",
        must_change_password=False,
    )
    current = await runtime.auth.create_session(admin_id=admin["id"], ip="127.0.0.1", user_agent="current")
    other = await runtime.auth.create_session(admin_id=admin["id"], ip="127.0.0.2", user_agent="other")

    await runtime.auth.change_admin_password(
        admin["id"],
        "original-password",
        "replacement-password",
        current_session_id=current["id"],
    )

    assert (await runtime.auth.get_session_admin(current["token"]))["id"] == admin["id"]
    with pytest.raises(LookupError, match="session not found"):
        await runtime.auth.get_session_admin(other["token"])
    assert (await runtime.auth.authenticate_admin("session-owner", "replacement-password"))["id"] == admin["id"]

    await runtime.auth.reset_admin_password(admin["id"], "reset-password-2")
    with pytest.raises(LookupError, match="session not found"):
        await runtime.auth.get_session_admin(current["token"])


@pytest.mark.asyncio
async def test_unknown_admin_uses_dummy_password_hash(runtime, monkeypatch) -> None:
    from app.auth import sessions as sessions_module

    observed_hashes: list[str] = []
    real_verify_password = sessions_module.verify_password

    def recording_verify(password: str, stored_hash: str) -> bool:
        observed_hashes.append(stored_hash)
        return real_verify_password(password, stored_hash)

    monkeypatch.setattr(sessions_module, "verify_password", recording_verify)
    with pytest.raises(LookupError, match="invalid admin credentials"):
        await runtime.auth.authenticate_admin("does-not-exist", "irrelevant-password")
    assert observed_hashes == [DUMMY_PASSWORD_HASH]


@pytest.mark.asyncio
async def test_session_last_seen_write_is_throttled(runtime, monkeypatch) -> None:
    admin = await runtime.auth.create_admin(
        username="touch-test",
        password="touch-test-password",
        role="viewer",
    )
    session = await runtime.auth.create_session(admin_id=admin["id"], ip="127.0.0.1", user_agent="pytest")

    async def unexpected_write(*args, **kwargs):
        raise AssertionError("a fresh session must not update last_seen_at")

    monkeypatch.setattr(runtime.auth.writer, "execute", unexpected_write)
    loaded = await runtime.auth.get_session_admin(session["token"], ip="127.0.0.1")
    assert loaded["session_last_seen_at"] == session["last_seen_at"]


@pytest.mark.asyncio
async def test_schema_has_explicit_grant_mode_and_delivery_expiration(runtime) -> None:
    with connect_database(runtime.settings.database_path) as connection:
        api_key_columns = {
            str(row["name"]): row
            for row in connection.execute("PRAGMA table_info(api_keys)").fetchall()
        }
        delivery_columns = {
            str(row["name"]): row
            for row in connection.execute("PRAGMA table_info(message_deliveries)").fetchall()
        }
        indexes = {
            str(row["name"])
            for row in connection.execute("PRAGMA index_list(message_deliveries)").fetchall()
        }

    assert api_key_columns["domain_grant_mode"]["dflt_value"] == "'none'"
    assert "expires_at" in delivery_columns
    assert "idx_message_deliveries_expires_at" in indexes


@pytest.mark.asyncio
async def test_api_key_authentication_cache_is_bounded_and_invalidated(runtime, monkeypatch) -> None:
    key = await runtime.api_keys.create_key(
        name="cached-service-key",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )
    load_count = 0
    real_load = runtime.api_keys._load_authentication_record

    def counting_load(kind: str, key_prefix: str):
        nonlocal load_count
        load_count += 1
        return real_load(kind, key_prefix)

    monkeypatch.setattr(runtime.api_keys, "_load_authentication_record", counting_load)

    first = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    second = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    assert first.api_key_id == second.api_key_id == key["id"]
    assert load_count == 1

    await runtime.api_keys.update_key(key["id"], name="cached-service-key-updated")
    refreshed = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    assert refreshed.name == "cached-service-key-updated"
    assert load_count == 2

    await runtime.api_keys.revoke_key(key["id"])
    with pytest.raises(LookupError, match="inactive api key"):
        runtime.api_keys.authenticate_plain_text(key["plain_text"])
    assert load_count == 3


@pytest.mark.asyncio
async def test_api_key_usage_writes_are_throttled_without_weakening_rate_limit(runtime, monkeypatch) -> None:
    service = ApiKeyService(
        runtime.settings.database_path,
        DatabaseWriter(runtime.settings.database_path),
    )
    key = await service.create_key(
        name="usage-throttle",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
        rate_limit_per_min=2,
    )
    context = service.authenticate_plain_text(key["plain_text"])
    write_count = 0
    real_execute = service.writer.execute

    async def counting_execute(operation):
        nonlocal write_count
        write_count += 1
        return await real_execute(operation)

    monkeypatch.setattr(service.writer, "execute", counting_execute)

    await service.record_usage(context, ip="127.0.0.1")
    await service.record_usage(context, ip="127.0.0.1")
    with pytest.raises(HTTPException) as exc_info:
        await service.record_usage(context, ip="127.0.0.1")

    assert exc_info.value.status_code == 429
    assert write_count == 1


@pytest.mark.asyncio
async def test_api_key_rate_limit_and_usage_persistence_are_concurrency_safe(runtime, monkeypatch) -> None:
    service = ApiKeyService(
        runtime.settings.database_path,
        DatabaseWriter(runtime.settings.database_path),
    )
    request_limit = 24
    key = await service.create_key(
        name="concurrent-usage-throttle",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
        rate_limit_per_min=request_limit,
    )
    context = service.authenticate_plain_text(key["plain_text"])
    write_count = 0
    real_execute = service.writer.execute

    async def counting_execute(operation):
        nonlocal write_count
        write_count += 1
        return await real_execute(operation)

    monkeypatch.setattr(service.writer, "execute", counting_execute)

    async def record_once() -> int:
        try:
            await service.record_usage(context, ip="127.0.0.1")
        except HTTPException as exc:
            return exc.status_code
        return 200

    statuses = await asyncio.gather(*(record_once() for _ in range(100)))

    assert statuses.count(200) == request_limit
    assert statuses.count(429) == 100 - request_limit
    assert write_count == 1
    assert len(service._usage_buckets) == 1
    assert not hasattr(service, "_usage_windows")


@pytest.mark.asyncio
async def test_api_key_pattern_and_cidr_limits_stay_below_sql_expression_limits(runtime) -> None:
    service = runtime.api_keys

    with pytest.raises(ValueError, match="too many mailbox patterns"):
        await service.create_key(
            name="too-many-patterns",
            kind="service",
            scopes=["messages.read"],
            domain_ids=[],
            domain_grant_mode="all",
            mailbox_patterns=[f"user-{index}@example.com" for index in range(101)],
        )

    with pytest.raises(ValueError, match="too many allowed IP networks"):
        await service.create_key(
            name="too-many-networks",
            kind="service",
            scopes=["messages.read"],
            domain_ids=[],
            domain_grant_mode="all",
            mailbox_patterns=[],
            allowed_ip_cidrs=[f"10.{index // 256}.{index % 256}.0/24" for index in range(101)],
        )


@pytest.mark.asyncio
async def test_api_key_mutations_reauthorize_actor_and_target_inside_writer_transaction(runtime) -> None:
    actor = await runtime.api_keys.create_key(
        name="bounded-key-manager",
        kind="admin",
        scopes=["api_keys.write", "public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    stale_actor_context = runtime.api_keys.authenticate_plain_text(actor["plain_text"])
    target = await runtime.api_keys.create_key(
        name="bounded-child",
        kind="admin",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )

    # A forbidden update is tentatively applied in the writer transaction, then
    # rejected and rolled back when the resulting policy exceeds the actor.
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.api_keys.update_key(
            target["id"],
            scopes=["system.write"],
            authorization_principal=stale_actor_context,
        )
    assert runtime.api_keys.get_key(target["id"])["scopes"] == ["public.read"]

    # Simulate a superadmin expanding the target after an HTTP authorization
    # read but before the bounded actor's queued mutation reaches the writer.
    expanded = await runtime.api_keys.update_key(target["id"], scopes=["system.write"])
    original_prefix = expanded["key_prefix"]
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.api_keys.rotate_key(
            target["id"],
            authorization_principal=stale_actor_context,
        )
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.api_keys.revoke_key(
            target["id"],
            authorization_principal=stale_actor_context,
        )
    current = runtime.api_keys.get_key(target["id"])
    assert current["key_prefix"] == original_prefix
    assert current["status"] == "active"

    await runtime.api_keys.revoke_key(target["id"])
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.api_keys.delete_key(
            target["id"],
            authorization_principal=stale_actor_context,
        )
    assert runtime.api_keys.get_key(target["id"])["status"] == "revoked"

    second_target = await runtime.api_keys.create_key(
        name="second-bounded-child",
        kind="admin",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    # The request context is stale after the actor itself is narrowed. The
    # writer reloads the actor and refuses to honor its old management scope.
    await runtime.api_keys.update_key(actor["id"], scopes=["public.read"])
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.api_keys.rotate_key(
            second_target["id"],
            authorization_principal=stale_actor_context,
        )


@pytest.mark.asyncio
async def test_domain_updates_reauthorize_scope_and_grants_inside_writer_transaction(
    runtime,
    monkeypatch,
) -> None:
    domain = await runtime.create_domain("selected-update.example")
    actor = await runtime.api_keys.create_key(
        name="selected-domain-editor",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    stale_actor = runtime.api_keys.authenticate_plain_text(actor["plain_text"])
    real_authorize = runtime.api_keys.transaction_authorization_principal

    def observe_transaction(connection, principal, **kwargs):
        assert connection.in_transaction
        return real_authorize(connection, principal, **kwargs)

    monkeypatch.setattr(
        runtime.api_keys,
        "transaction_authorization_principal",
        observe_transaction,
    )

    updated = await runtime.domains.update_domain(
        domain["id"],
        {"notes": "selected grant may edit policy"},
        authorization_principal=stale_actor,
    )
    assert updated["notes"] == "selected grant may edit policy"

    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.domains.update_domain(
            domain["id"],
            {"root_domain": "renamed-selected-update.example"},
            authorization_principal=stale_actor,
        )
    assert runtime.domains.get_domain(domain["id"])["root_domain_ascii"] == "selected-update.example"

    # The request context still contains the old selected grant, but the writer
    # reloads the key after its grant is narrowed and rolls the policy write back.
    await runtime.api_keys.update_key(
        actor["id"],
        domain_ids=[],
        domain_grant_mode="selected",
    )
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.domains.update_domain(
            domain["id"],
            {"public_api_enabled": True},
            authorization_principal=stale_actor,
        )
    assert runtime.domains.get_domain(domain["id"])["public_api_enabled"] is False

    scope_actor = await runtime.api_keys.create_key(
        name="scope-narrowed-domain-editor",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    stale_scope_actor = runtime.api_keys.authenticate_plain_text(scope_actor["plain_text"])
    await runtime.api_keys.update_key(scope_actor["id"], scopes=["domains.read"])
    with pytest.raises(ApiKeyAuthorizationError):
        await runtime.domains.update_domain(
            domain["id"],
            {"public_web_enabled": True},
            authorization_principal=stale_scope_actor,
        )
    assert runtime.domains.get_domain(domain["id"])["public_web_enabled"] is False

    global_actor = await runtime.api_keys.create_key(
        name="global-domain-editor",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    renamed = await runtime.domains.update_domain(
        domain["id"],
        {"root_domain": "renamed-selected-update.example"},
        authorization_principal=runtime.api_keys.authenticate_plain_text(global_actor["plain_text"]),
    )
    assert renamed["root_domain_ascii"] == "renamed-selected-update.example"


@pytest.mark.asyncio
async def test_domain_create_delete_reauthorize_after_cross_process_key_change(runtime) -> None:
    async def run_behind_writer_latch(call_factory, external_change):
        entered = threading.Event()
        release = threading.Event()
        queued = asyncio.Event()
        real_writer = runtime.domains._writer

        class ObservedDomainWriter:
            async def execute(self, operation):
                submitted = asyncio.create_task(real_writer.execute(operation))
                while not submitted.done():
                    # The blocker below prevents execution. Inspecting by
                    # operation identity proves this exact domain transaction,
                    # rather than an unrelated background write, is queued.
                    with real_writer._queue.mutex:
                        is_queued = any(
                            getattr(item, "operation", None) is operation
                            for item in real_writer._queue.queue
                        )
                    if is_queued:
                        queued.set()
                        break
                    await asyncio.sleep(0.001)
                return await submitted

        def blocker(_connection) -> None:
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("writer latch was not released")

        blocker_task = asyncio.create_task(runtime.writer.execute(blocker))
        assert await asyncio.wait_for(
            asyncio.to_thread(entered.wait, 2),
            timeout=3,
        )
        runtime.domains._writer = ObservedDomainWriter()
        mutation_task = asyncio.create_task(call_factory())
        try:
            await asyncio.wait_for(queued.wait(), timeout=3)
            await asyncio.to_thread(external_change)
        finally:
            release.set()
            await asyncio.wait_for(blocker_task, timeout=3)
            runtime.domains._writer = real_writer
        return await asyncio.wait_for(mutation_task, timeout=3)

    create_actor = await runtime.api_keys.create_key(
        name="cross-process-domain-creator",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    stale_create_actor = runtime.api_keys.authenticate_plain_text(
        create_actor["plain_text"]
    )

    def narrow_create_actor() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "DELETE FROM api_key_scopes WHERE api_key_id = ?",
                (create_actor["id"],),
            )
            connection.execute(
                "INSERT INTO api_key_scopes (api_key_id, scope) VALUES (?, 'domains.read')",
                (create_actor["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="domains.write"):
        await run_behind_writer_latch(
            lambda: runtime.domains.create_domain(
                "blocked-cross-process-create.example",
                authorization_principal=stale_create_actor,
            ),
            narrow_create_actor,
        )

    with connect_database(runtime.settings.database_path) as connection:
        created_row = connection.execute(
            "SELECT id FROM domains WHERE root_domain_ascii = ?",
            ("blocked-cross-process-create.example",),
        ).fetchone()
        create_job = connection.execute(
            "SELECT id FROM domain_rehome_jobs WHERE candidate_root_domain = ?",
            ("blocked-cross-process-create.example",),
        ).fetchone()
    assert created_row is None
    assert create_job is None

    doomed = await runtime.create_domain("blocked-cross-process-delete.example")
    delete_actor = await runtime.api_keys.create_key(
        name="cross-process-domain-deleter",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[doomed["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    stale_delete_actor = runtime.api_keys.authenticate_plain_text(
        delete_actor["plain_text"]
    )

    def revoke_delete_actor() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (delete_actor["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await run_behind_writer_latch(
            lambda: runtime.domains.delete_domain(
                doomed["id"],
                authorization_principal=stale_delete_actor,
            ),
            revoke_delete_actor,
        )

    assert runtime.domains.get_domain(doomed["id"])["root_domain_ascii"] == (
        "blocked-cross-process-delete.example"
    )
    with connect_database(runtime.settings.database_path) as connection:
        tombstone = connection.execute(
            "SELECT value FROM system_settings WHERE key = ?",
            (
                domain_routing_tombstone_key(
                    doomed["id"],
                    "blocked-cross-process-delete.example",
                ),
            ),
        ).fetchone()
    assert tombstone is None


@pytest.mark.asyncio
async def test_settings_update_reauthorizes_after_queued_cross_process_revocation(runtime) -> None:
    actor = await runtime.api_keys.create_key(
        name="cross-process-settings-writer",
        kind="admin",
        scopes=["system.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    stale_actor = runtime.api_keys.authenticate_plain_text(actor["plain_text"])
    setting_name = "max_recipients_per_message"
    original_live_value = int(getattr(runtime.settings, setting_name))
    target_value = original_live_value + 1
    with connect_database(runtime.settings.database_path) as connection:
        original_row = connection.execute(
            "SELECT value FROM system_settings WHERE key = ?",
            (setting_name,),
        ).fetchone()
    original_persisted_value = None if original_row is None else str(original_row["value"])

    entered = threading.Event()
    release = threading.Event()
    queued = asyncio.Event()
    real_writer = runtime.writer
    real_service_runtime = runtime.system_settings._runtime

    def blocker(_connection) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("writer latch was not released")

    class ObservedWriter:
        async def execute(self, operation):
            submitted = asyncio.create_task(real_writer.execute(operation))
            while not submitted.done():
                with real_writer._queue.mutex:
                    is_queued = any(
                        getattr(item, "operation", None) is operation
                        for item in real_writer._queue.queue
                    )
                if is_queued:
                    queued.set()
                    break
                await asyncio.sleep(0.001)
            return await submitted

    class SettingsRuntimeProxy:
        writer = ObservedWriter()

        def __getattr__(self, name):
            return getattr(real_service_runtime, name)

    blocker_task = asyncio.create_task(real_writer.execute(blocker))
    assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), timeout=3)
    runtime.system_settings._runtime = SettingsRuntimeProxy()
    update_task = asyncio.create_task(
        runtime.system_settings.update_settings(
            {setting_name: target_value},
            authorization_principal=stale_actor,
        )
    )
    try:
        await asyncio.wait_for(queued.wait(), timeout=3)
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (actor["id"],),
            )
    finally:
        release.set()
        await asyncio.wait_for(blocker_task, timeout=3)
        runtime.system_settings._runtime = real_service_runtime

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await asyncio.wait_for(update_task, timeout=3)

    with connect_database(runtime.settings.database_path) as connection:
        persisted = connection.execute(
            "SELECT value FROM system_settings WHERE key = ?",
            (setting_name,),
        ).fetchone()
    persisted_value = None if persisted is None else str(persisted["value"])
    assert persisted_value == original_persisted_value
    assert int(getattr(runtime.settings, setting_name)) == original_live_value
