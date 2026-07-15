from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from app.auth.api_keys import ApiKeyAuthorizationError
from app.db.connection import connect_database
from app.services.dns_check import DnsCheckService


async def _run_behind_domain_writer_latch(
    runtime,
    call_factory: Callable[[], Coroutine[Any, Any, Any]],
    external_change: Callable[[], None],
) -> Any:
    entered = threading.Event()
    release = threading.Event()
    queued = asyncio.Event()
    real_writer = runtime.domains._writer

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
    runtime.domains._writer = ObservedWriter()
    mutation_task = asyncio.create_task(call_factory())
    try:
        await asyncio.wait_for(queued.wait(), timeout=3)
        await asyncio.to_thread(external_change)
    finally:
        release.set()
        await asyncio.wait_for(blocker_task, timeout=3)
        runtime.domains._writer = real_writer
    return await asyncio.wait_for(mutation_task, timeout=5)


@pytest.mark.asyncio
async def test_dns_result_reauthorizes_and_rejects_stale_domain_policy(runtime) -> None:
    domain = await runtime.create_domain("dns-final-auth.example")
    key = await runtime.api_keys.create_key(
        name="queued-dns-writer",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    stale_principal = runtime.api_keys.authenticate_plain_text(key["plain_text"])
    details = {
        "domain_id": domain["id"],
        "root_domain_ascii": domain["root_domain_ascii"],
        "checked_at": "2026-07-15T00:00:00Z",
        "status": "ok",
        "mx_records": ["mx.dns-final-auth.example"],
    }

    def revoke_key() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (key["id"],),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await _run_behind_domain_writer_latch(
            runtime,
            lambda: runtime.domains.record_dns_check(
                domain["id"],
                expected_root_domain_ascii=domain["root_domain_ascii"],
                checked_at=details["checked_at"],
                details=details,
                authorization_principal=stale_principal,
            ),
            revoke_key,
        )
    assert runtime.domains.get_domain(domain["id"])["dns_status"] == "unknown"

    renamed = await runtime.domains.update_domain(
        domain["id"],
        {"root_domain": "renamed-dns-final-auth.example"},
    )
    with pytest.raises(ValueError, match="domain changed"):
        await runtime.domains.record_dns_check(
            domain["id"],
            expected_root_domain_ascii=domain["root_domain_ascii"],
            checked_at=details["checked_at"],
            details=details,
        )
    assert runtime.domains.get_domain(domain["id"])["dns_status"] == "unknown"

    current_details = {
        **details,
        "root_domain_ascii": renamed["root_domain_ascii"],
        "checked_at": "2026-07-15T00:01:00Z",
    }
    stored = await runtime.domains.record_dns_check(
        domain["id"],
        expected_root_domain_ascii=renamed["root_domain_ascii"],
        checked_at=current_details["checked_at"],
        details=current_details,
    )
    assert stored["dns_status"] == "ok"
    assert stored["dns_last_checked_at"] == "2026-07-15T00:01:00Z"


@pytest.mark.asyncio
async def test_all_dns_http_surfaces_forward_principal_and_map_final_denial(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    domain = await runtime.create_domain("dns-http-auth.example")
    key = await runtime.api_keys.create_key(
        name="dns-http-writer",
        kind="admin",
        scopes=["domains.write"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
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
            "new_password": "dns-http-new-password",
            "confirm_password": "dns-http-new-password",
        },
        follow_redirects=True,
    )
    assert password_change.status_code == 200

    async def fake_dns_check(_self, root_domain: str) -> dict[str, Any]:
        assert root_domain == domain["root_domain_ascii"]
        return {"status": "ok", "mx_records": [f"mx.{root_domain}"]}

    principals = []

    async def denied_record(
        domain_id,
        *,
        expected_root_domain_ascii,
        checked_at,
        details,
        authorization_principal=None,
    ):
        assert domain_id == domain["id"]
        assert expected_root_domain_ascii == domain["root_domain_ascii"]
        assert details["status"] == "ok"
        principals.append(authorization_principal)
        raise ApiKeyAuthorizationError("injected final DNS denial")

    monkeypatch.setattr(DnsCheckService, "run_dns_check", fake_dns_check)
    monkeypatch.setattr(runtime.domains, "record_dns_check", denied_record)

    v1 = await app_client.post(
        f"/api/v1/admin/domains/{domain['id']}/dns-check",
        headers={"X-API-Key": key["plain_text"]},
    )
    v2 = await app_client.post(
        f"/api/v2/domains/{domain['id']}/dns-check",
        headers={"Authorization": f"Bearer {key['plain_text']}"},
    )
    page = await app_client.post(f"/admin/domains/{domain['id']}/dns-check")

    assert v1.status_code == 403
    assert v2.status_code == 403
    assert v2.json()["code"] == "authorization_changed"
    assert page.status_code == 403
    assert len(principals) == 3
    assert principals[0].api_key_id == key["id"]
    assert principals[1].api_key_id == key["id"]
    assert principals[2].admin_session_id is not None
