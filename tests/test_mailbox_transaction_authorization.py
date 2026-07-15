from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from app.auth.api_keys import ApiKeyAuthorizationError
from app.db.connection import connect_database


def _seed_mailbox(runtime, *, domain: str) -> dict[str, Any]:
    address = f"bulk@{domain}"
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        cursor = connection.execute(
            """
            INSERT INTO domains (root_domain_ascii, created_at, updated_at)
            VALUES (?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (domain,),
        )
        domain_id = int(cursor.lastrowid)
        cursor = connection.execute(
            """
            INSERT INTO mailboxes (
                domain_id, local_part_canonical, rcpt_domain_ascii,
                address_canonical, address_display, first_seen_at, last_seen_at,
                latest_message_at, message_count, public_enabled
            ) VALUES (
                ?, 'bulk', ?, ?, ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                '2026-01-01T00:00:00Z', 2, 0
            )
            """,
            (domain_id, domain, address, address),
        )
        mailbox_id = int(cursor.lastrowid)
        connection.executemany(
            """
            INSERT INTO messages (
                id, raw_path, raw_sha256, raw_size_bytes, received_at
            ) VALUES (?, ?, ?, 1, '2026-01-01T00:00:00Z')
            """,
            (
                (f"message-{index}-{domain}", f"raw/{index}-{domain}.eml", f"sha-{index}-{domain}")
                for index in (1, 2)
            ),
        )
        delivery_ids = [f"delivery-{index}-{domain}" for index in (1, 2)]
        connection.executemany(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (?, ?, ?, ?, '2026-01-01T00:00:00Z')
            """,
            (
                (
                    delivery_id,
                    f"message-{index}-{domain}",
                    mailbox_id,
                    address,
                )
                for index, delivery_id in zip((1, 2), delivery_ids, strict=True)
            ),
        )
    return {
        "domain_id": domain_id,
        "mailbox_id": mailbox_id,
        "address": address,
        "delivery_ids": delivery_ids,
    }


async def _run_behind_writer_latch(
    runtime,
    call_factory: Callable[[], Coroutine[Any, Any, Any]],
    external_change: Callable[[], None],
) -> Any:
    entered = threading.Event()
    release = threading.Event()
    queued = asyncio.Event()
    real_writer = runtime.writer

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

    def blocker(_connection) -> None:
        entered.set()
        if not release.wait(timeout=5):
            raise TimeoutError("writer latch was not released")

    blocker_task = asyncio.create_task(real_writer.execute(blocker))
    assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), timeout=3)
    runtime.writer = ObservedWriter()
    mutation_task = asyncio.create_task(call_factory())
    try:
        await asyncio.wait_for(queued.wait(), timeout=3)
        await asyncio.to_thread(external_change)
    finally:
        release.set()
        await asyncio.wait_for(blocker_task, timeout=3)
        runtime.writer = real_writer
    return await asyncio.wait_for(mutation_task, timeout=5)


@pytest.mark.asyncio
async def test_mailbox_update_reauthorizes_after_queued_key_revocation(runtime) -> None:
    seeded = _seed_mailbox(runtime, domain="mailbox-update-auth.example")
    key = await runtime.api_keys.create_key(
        name="queued-mailbox-updater",
        kind="admin",
        scopes=["mailboxes.write"],
        domain_ids=[seeded["domain_id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[seeded["address"]],
    )
    stale_principal = runtime.api_keys.authenticate_plain_text(key["plain_text"])

    def revoke_key() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (key["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await _run_behind_writer_latch(
            runtime,
            lambda: runtime.mailboxes.update_mailbox(
                seeded["mailbox_id"],
                {"public_enabled": True},
                authorization_principal=stale_principal,
            ),
            revoke_key,
        )

    assert runtime.mailboxes.get_mailbox(seeded["mailbox_id"])["public_enabled"] is False


@pytest.mark.asyncio
async def test_bulk_delete_job_reauthorizes_domain_grant_before_creation(runtime) -> None:
    seeded = _seed_mailbox(runtime, domain="mailbox-job-auth.example")
    key = await runtime.api_keys.create_key(
        name="queued-mailbox-clearer",
        kind="admin",
        scopes=["mailboxes.write"],
        domain_ids=[seeded["domain_id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[seeded["address"]],
    )
    stale_principal = runtime.api_keys.authenticate_plain_text(key["plain_text"])

    def remove_domain_grant() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "DELETE FROM api_key_domain_grants WHERE api_key_id = ?",
                (key["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="domain grant"):
        await _run_behind_writer_latch(
            runtime,
            lambda: runtime.mailboxes.soft_delete_mailbox_deliveries(
                seeded["mailbox_id"],
                authorization_principal=stale_principal,
            ),
            remove_domain_grant,
        )

    with connect_database(runtime.settings.database_path) as connection:
        job_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM mailbox_bulk_delete_jobs WHERE mailbox_id = ?",
                (seeded["mailbox_id"],),
            ).fetchone()["count"]
        )
        active_count = int(
            connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM message_deliveries
                WHERE mailbox_id = ? AND status = 'active'
                """,
                (seeded["mailbox_id"],),
            ).fetchone()["count"]
        )
    assert job_count == 0
    assert active_count == 2


@pytest.mark.asyncio
async def test_mailbox_pattern_is_rechecked_for_update_and_explicit_delete(runtime) -> None:
    seeded = _seed_mailbox(runtime, domain="mailbox-pattern-auth.example")
    key = await runtime.api_keys.create_key(
        name="mailbox-pattern-editor",
        kind="admin",
        scopes=["mailboxes.write"],
        domain_ids=[seeded["domain_id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[seeded["address"]],
    )
    stale_principal = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        connection.execute(
            "DELETE FROM api_key_mailbox_grants WHERE api_key_id = ?",
            (key["id"],),
        )
        connection.execute(
            """
            INSERT INTO api_key_mailbox_grants (api_key_id, mailbox_pattern)
            VALUES (?, 'other@mailbox-pattern-auth.example')
            """,
            (key["id"],),
        )

    with pytest.raises(ApiKeyAuthorizationError, match="mailbox grant"):
        await runtime.mailboxes.update_mailbox(
            seeded["mailbox_id"],
            {"is_hidden": True},
            authorization_principal=stale_principal,
        )
    with pytest.raises(ApiKeyAuthorizationError, match="mailbox grant"):
        await runtime.mailboxes.soft_delete_mailbox_deliveries(
            seeded["mailbox_id"],
            delivery_ids=[seeded["delivery_ids"][0]],
            authorization_principal=stale_principal,
        )

    mailbox = runtime.mailboxes.get_mailbox(seeded["mailbox_id"])
    assert mailbox["is_hidden"] is False
    with connect_database(runtime.settings.database_path) as connection:
        statuses = connection.execute(
            "SELECT status FROM message_deliveries WHERE mailbox_id = ? ORDER BY id",
            (seeded["mailbox_id"],),
        ).fetchall()
    assert [row["status"] for row in statuses] == ["active", "active"]

    # Trusted internal maintenance remains an explicit, unrestricted path.
    updated = await runtime.mailboxes.update_mailbox(
        seeded["mailbox_id"],
        {"is_hidden": True},
    )
    deleted = await runtime.mailboxes.soft_delete_mailbox_deliveries(
        seeded["mailbox_id"],
        delivery_ids=[seeded["delivery_ids"][0]],
    )
    assert updated["is_hidden"] is True
    assert deleted["deleted"] == 1


@pytest.mark.asyncio
async def test_all_mailbox_http_surfaces_forward_principal_and_map_final_denial(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    seeded = _seed_mailbox(runtime, domain="mailbox-http-auth.example")
    key = await runtime.api_keys.create_key(
        name="mailbox-http-editor",
        kind="admin",
        scopes=["mailboxes.write"],
        domain_ids=[seeded["domain_id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[seeded["address"]],
    )

    login = await app_client.post(
        "/admin/login",
        data={
            "username": "admin",
            "password": runtime.settings.bootstrap_admin_password,
        },
        follow_redirects=True,
    )
    assert login.status_code == 200
    password_change = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "mailbox-http-new-password",
            "confirm_password": "mailbox-http-new-password",
        },
        follow_redirects=True,
    )
    assert password_change.status_code == 200

    update_principals = []
    delete_principals = []

    async def denied_update(
        mailbox_id,
        payload,
        *,
        authorization_principal=None,
    ):
        assert mailbox_id == seeded["mailbox_id"]
        update_principals.append(authorization_principal)
        raise ApiKeyAuthorizationError("injected final mailbox denial")

    async def denied_delete(
        mailbox_id,
        *,
        delivery_ids=None,
        authorization_principal=None,
    ):
        assert mailbox_id == seeded["mailbox_id"]
        delete_principals.append(authorization_principal)
        raise ApiKeyAuthorizationError("injected final mailbox denial")

    monkeypatch.setattr(runtime.mailboxes, "update_mailbox", denied_update)
    monkeypatch.setattr(
        runtime.mailboxes,
        "soft_delete_mailbox_deliveries",
        denied_delete,
    )

    v1_headers = {"X-API-Key": key["plain_text"]}
    v1_update = await app_client.patch(
        f"/api/v1/admin/mailboxes/{seeded['mailbox_id']}",
        headers=v1_headers,
        json={"public_enabled": True},
    )
    v1_delete = await app_client.delete(
        f"/api/v1/admin/mailboxes/{seeded['mailbox_id']}",
        headers=v1_headers,
    )

    v2_headers = {"Authorization": f"Bearer {key['plain_text']}"}
    v2_update = await app_client.patch(
        f"/api/v2/mailboxes/{seeded['mailbox_id']}",
        headers=v2_headers,
        json={"public_enabled": True},
    )
    v2_delete = await app_client.delete(
        f"/api/v2/mailboxes/{seeded['mailbox_id']}",
        headers=v2_headers,
    )

    page_update = await app_client.post(
        f"/admin/mailboxes/{seeded['mailbox_id']}",
        data={"public_enabled": "1"},
    )
    page_delete = await app_client.post(
        f"/admin/mailboxes/{seeded['mailbox_id']}/delete-deliveries",
        data={},
    )

    assert v1_update.status_code == 403
    assert v1_delete.status_code == 403
    assert v2_update.status_code == 403
    assert v2_delete.status_code == 403
    assert v2_update.json()["code"] == "authorization_changed"
    assert v2_delete.json()["code"] == "authorization_changed"
    assert page_update.status_code == 403
    assert page_delete.status_code == 403

    assert len(update_principals) == 3
    assert len(delete_principals) == 3
    assert all(principal is not None for principal in update_principals)
    assert all(principal is not None for principal in delete_principals)
    assert update_principals[0].api_key_id == key["id"]
    assert update_principals[1].api_key_id == key["id"]
    assert update_principals[2].admin_session_id is not None
    assert delete_principals[0].api_key_id == key["id"]
    assert delete_principals[1].api_key_id == key["id"]
    assert delete_principals[2].admin_session_id is not None
