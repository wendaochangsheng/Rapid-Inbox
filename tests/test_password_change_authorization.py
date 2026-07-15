from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Coroutine
from typing import Any

import pytest

from app.auth.api_keys import ApiKeyAuthorizationError
from app.auth.passwords import hash_password, verify_password
from app.db.connection import connect_database
from app.ingest.storage import utc_now


async def _run_behind_auth_writer_latch(
    runtime,
    call_factory: Callable[[], Coroutine[Any, Any, Any]],
    external_change: Callable[[], None],
) -> Any:
    entered = threading.Event()
    release = threading.Event()
    queued = asyncio.Event()
    real_writer = runtime.auth.writer

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
        if not release.wait(timeout=10):
            raise TimeoutError("writer latch was not released")

    blocker_task = asyncio.create_task(real_writer.execute(blocker))
    assert await asyncio.wait_for(asyncio.to_thread(entered.wait, 2), timeout=3)
    runtime.auth.writer = ObservedWriter()
    mutation_task = asyncio.create_task(call_factory())
    try:
        await asyncio.wait_for(queued.wait(), timeout=10)
        await asyncio.to_thread(external_change)
    finally:
        release.set()
        await asyncio.wait_for(blocker_task, timeout=3)
        runtime.auth.writer = real_writer
    return await asyncio.wait_for(mutation_task, timeout=10)


def _password_hash(runtime, admin_id: int) -> str:
    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute(
            "SELECT password_hash FROM admins WHERE id = ?",
            (admin_id,),
        ).fetchone()
    assert row is not None
    return str(row["password_hash"])


@pytest.mark.asyncio
async def test_login_rejects_password_changed_after_verification(runtime) -> None:
    admin = await runtime.auth.create_admin(
        username="login-proof-owner",
        password="login-proof-original",
        role="viewer",
        must_change_password=False,
    )
    competing_hash = await asyncio.to_thread(
        hash_password,
        "login-proof-reset-password",
    )

    def reset_password() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE admins SET password_hash = ? WHERE id = ?",
                (competing_hash, admin["id"]),
            )

    with pytest.raises(LookupError, match="invalid admin credentials"):
        await _run_behind_auth_writer_latch(
            runtime,
            lambda: runtime.auth.authenticate_admin(
                "login-proof-owner",
                "login-proof-original",
            ),
            reset_password,
        )
    assert _password_hash(runtime, admin["id"]) == competing_hash

    second = await runtime.auth.create_admin(
        username="session-proof-owner",
        password="session-proof-original",
        role="viewer",
        must_change_password=False,
    )
    authenticated = await runtime.auth.authenticate_admin(
        "session-proof-owner",
        "session-proof-original",
    )
    proof = str(authenticated["_password_hash_proof"])
    reset_hash = await asyncio.to_thread(
        hash_password,
        "session-proof-reset-password",
    )
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        connection.execute(
            "UPDATE admins SET password_hash = ? WHERE id = ?",
            (reset_hash, second["id"]),
        )

    with pytest.raises(LookupError, match="admin not found"):
        await runtime.auth.create_session(
            admin_id=second["id"],
            ip="127.0.0.1",
            user_agent="pytest",
            expected_password_hash=proof,
        )
    with connect_database(runtime.settings.database_path) as connection:
        session_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM admin_sessions WHERE admin_id = ?",
                (second["id"],),
            ).fetchone()["count"]
        )
    assert session_count == 0


@pytest.mark.asyncio
async def test_login_rejects_username_changed_after_verification(runtime) -> None:
    admin = await runtime.auth.create_admin(
        username="login-rename-owner",
        password="login-rename-original",
        role="viewer",
        must_change_password=False,
    )

    def rename_admin() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE admins SET username = ? WHERE id = ?",
                ("login-rename-owner-new", admin["id"]),
            )

    with pytest.raises(LookupError, match="invalid admin credentials"):
        await _run_behind_auth_writer_latch(
            runtime,
            lambda: runtime.auth.authenticate_admin(
                "login-rename-owner",
                "login-rename-original",
            ),
            rename_admin,
        )

    authenticated = await runtime.auth.authenticate_admin(
        "login-rename-owner-new",
        "login-rename-original",
    )
    assert authenticated["id"] == admin["id"]
    assert authenticated["username"] == "login-rename-owner-new"


@pytest.mark.asyncio
async def test_password_change_rejects_revoked_session_and_concurrent_password_change(
    runtime,
) -> None:
    current_password = runtime.settings.bootstrap_admin_password
    admin = await runtime.auth.authenticate_admin(
        runtime.settings.bootstrap_admin_username,
        current_password,
    )
    session = await runtime.auth.create_session(
        admin_id=admin["id"],
        ip="127.0.0.1",
        user_agent="pytest",
    )
    original_hash = _password_hash(runtime, admin["id"])

    def revoke_session() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE admin_sessions SET revoked_at = ? WHERE id = ?",
                ("2026-07-15T00:00:00Z", session["id"]),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="session is no longer active"):
        await _run_behind_auth_writer_latch(
            runtime,
            lambda: runtime.auth.change_admin_password(
                admin["id"],
                current_password,
                "revoked-session-password",
                current_session_id=session["id"],
            ),
            revoke_session,
        )
    unchanged_hash = _password_hash(runtime, admin["id"])
    assert unchanged_hash == original_hash
    assert verify_password(current_password, unchanged_hash)
    assert not verify_password("revoked-session-password", unchanged_hash)

    competing_hash = await asyncio.to_thread(
        hash_password,
        "competing-password-change",
    )

    def commit_competing_password() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE admins SET password_hash = ? WHERE id = ?",
                (competing_hash, admin["id"]),
            )

    with pytest.raises(LookupError, match="invalid admin credentials"):
        await _run_behind_auth_writer_latch(
            runtime,
            lambda: runtime.auth.change_admin_password(
                admin["id"],
                current_password,
                "stale-overwrite-password",
            ),
            commit_competing_password,
        )
    final_hash = _password_hash(runtime, admin["id"])
    assert final_hash == competing_hash
    assert verify_password("competing-password-change", final_hash)
    assert not verify_password("stale-overwrite-password", final_hash)


@pytest.mark.asyncio
async def test_password_change_rejects_session_that_expires_while_queued(
    runtime,
) -> None:
    current_password = runtime.settings.bootstrap_admin_password
    admin = await runtime.auth.authenticate_admin(
        runtime.settings.bootstrap_admin_username,
        current_password,
    )
    session = await runtime.auth.create_session(
        admin_id=admin["id"],
        ip="127.0.0.1",
        user_agent="pytest",
    )
    original_hash = _password_hash(runtime, admin["id"])

    def expire_session() -> None:
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE admin_sessions SET expires_at = ? WHERE id = ?",
                (utc_now(), session["id"]),
            )

    with pytest.raises(ApiKeyAuthorizationError, match="session is no longer active"):
        await _run_behind_auth_writer_latch(
            runtime,
            lambda: runtime.auth.change_admin_password(
                admin["id"],
                current_password,
                "expired-session-password",
                current_session_id=session["id"],
            ),
            expire_session,
        )

    unchanged_hash = _password_hash(runtime, admin["id"])
    assert unchanged_hash == original_hash
    assert verify_password(current_password, unchanged_hash)
    assert not verify_password("expired-session-password", unchanged_hash)


@pytest.mark.asyncio
async def test_password_change_page_maps_final_session_denial_to_403(
    app_client,
    runtime,
    monkeypatch,
) -> None:
    login = await app_client.post(
        "/admin/login",
        data={
            "username": "admin",
            "password": runtime.settings.bootstrap_admin_password,
        },
        follow_redirects=True,
    )
    assert login.status_code == 200

    async def denied_change(*args, **kwargs):
        assert kwargs["current_session_id"]
        raise ApiKeyAuthorizationError("injected revoked session")

    monkeypatch.setattr(runtime.auth, "change_admin_password", denied_change)
    response = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "denied-session-password",
            "confirm_password": "denied-session-password",
        },
    )
    assert response.status_code == 403
    assert response.json()["detail"] == "administrator authorization changed"
