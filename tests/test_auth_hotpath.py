from __future__ import annotations

import asyncio
import threading
import time

import pytest

from app.auth import sessions as sessions_module
from app.auth.sessions import (
    LOGIN_FAILURE_MAX_KEYS,
    MAX_ADMIN_PASSWORD_LENGTH,
    AuthenticationOverloadedError,
)


@pytest.mark.asyncio
async def test_password_verification_does_not_block_event_loop(runtime, monkeypatch) -> None:
    def slow_rejection(_password: str, _stored_hash: str) -> bool:
        time.sleep(0.1)
        return False

    monkeypatch.setattr(sessions_module, "verify_password", slow_rejection)
    authentication = asyncio.create_task(
        runtime.auth.authenticate_admin("missing-admin", "wrong-password")
    )
    started = time.perf_counter()
    await asyncio.sleep(0.01)
    timer_elapsed = time.perf_counter() - started

    with pytest.raises(LookupError, match="invalid admin credentials"):
        await authentication

    assert timer_elapsed < 0.06


@pytest.mark.asyncio
async def test_password_work_isolated_from_default_executor(runtime, monkeypatch) -> None:
    started_count = 0
    started_lock = threading.Lock()
    workers_started = threading.Event()

    def slow_rejection(_password: str, _stored_hash: str) -> bool:
        nonlocal started_count
        with started_lock:
            started_count += 1
            if started_count >= 4:
                workers_started.set()
        time.sleep(0.05)
        return False

    monkeypatch.setattr(sessions_module, "verify_password", slow_rejection)
    attempts = [
        asyncio.create_task(
            runtime.auth.authenticate_admin(
                f"missing-{index}",
                "wrong-password",
                ip=f"192.0.2.{index}",
            )
        )
        for index in range(32)
    ]
    while not workers_started.is_set():
        await asyncio.sleep(0.001)

    probe_started = time.perf_counter()
    await asyncio.wait_for(asyncio.to_thread(time.monotonic), timeout=0.1)
    assert time.perf_counter() - probe_started < 0.1

    results = await asyncio.gather(*attempts, return_exceptions=True)
    assert all(isinstance(result, LookupError) for result in results)


@pytest.mark.asyncio
async def test_api_key_cold_usage_policy_read_does_not_block_event_loop(runtime, monkeypatch) -> None:
    created = await runtime.api_keys.create_key(
        name="cold-policy",
        kind="service",
        scopes=["system.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    context = runtime.api_keys.authenticate_plain_text(created["plain_text"])
    with runtime.api_keys._auth_cache_lock:
        runtime.api_keys._usage_policy_cache.clear()

    original_load = runtime.api_keys._load_usage_policy

    def slow_load(api_key_id: int):
        time.sleep(0.1)
        return original_load(api_key_id)

    monkeypatch.setattr(runtime.api_keys, "_load_usage_policy", slow_load)
    usage = asyncio.create_task(runtime.api_keys.record_usage(context, ip="127.0.0.1"))
    started = time.perf_counter()
    await asyncio.sleep(0.01)
    timer_elapsed = time.perf_counter() - started

    await usage
    assert timer_elapsed < 0.06


@pytest.mark.asyncio
async def test_api_key_hot_authentication_uses_cache_without_database_io(runtime, monkeypatch) -> None:
    created = await runtime.api_keys.create_key(
        name="hot-auth-cache",
        kind="service",
        scopes=["domains.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    plain_text = created["plain_text"]
    expected = runtime.api_keys.authenticate_plain_text(
        plain_text,
        request_ip="127.0.0.1",
    )

    def unexpected_load(_kind: str, _prefix: str):
        raise AssertionError("hot API key authentication reached SQLite")

    monkeypatch.setattr(runtime.api_keys, "_load_authentication_record", unexpected_load)
    cached = runtime.api_keys.authenticate_plain_text_cached(
        plain_text,
        request_ip="127.0.0.1",
    )

    assert cached == expected


@pytest.mark.asyncio
async def test_selected_domain_key_stays_off_hot_authentication_cache(runtime) -> None:
    domain = await runtime.create_domain("selected-cache.example")
    created = await runtime.api_keys.create_key(
        name="selected-cache",
        kind="service",
        scopes=["domains.read"],
        domain_ids=[domain["id"]],
        domain_grant_mode="selected",
        mailbox_patterns=[],
    )
    plain_text = created["plain_text"]

    runtime.api_keys.authenticate_plain_text(plain_text, request_ip="127.0.0.1")

    assert runtime.api_keys.authenticate_plain_text_cached(
        plain_text,
        request_ip="127.0.0.1",
    ) is None


@pytest.mark.asyncio
async def test_cold_auth_loader_cannot_repopulate_cache_after_revocation(runtime, monkeypatch) -> None:
    created = await runtime.api_keys.create_key(
        name="auth-cache-revocation-race",
        kind="service",
        scopes=["domains.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    loaded = threading.Event()
    release = threading.Event()
    real_load = runtime.api_keys._load_authentication_record

    def delayed_load(kind: str, key_prefix: str):
        record = real_load(kind, key_prefix)
        loaded.set()
        assert release.wait(timeout=2)
        return record

    monkeypatch.setattr(runtime.api_keys, "_load_authentication_record", delayed_load)
    authentication = asyncio.create_task(
        asyncio.to_thread(
            runtime.api_keys.authenticate_plain_text,
            created["plain_text"],
            request_ip="127.0.0.1",
        )
    )
    try:
        await asyncio.wait_for(asyncio.to_thread(loaded.wait), timeout=1)
        await runtime.api_keys.revoke_key(created["id"])
        release.set()
        context = await asyncio.wait_for(authentication, timeout=1)

        # The in-flight lookup linearized before revoke, but its stale record
        # and usage policy must not survive the post-commit invalidation.
        assert context.api_key_id == created["id"]
        assert runtime.api_keys.authenticate_plain_text_cached(
            created["plain_text"],
            request_ip="127.0.0.1",
        ) is None
        with runtime.api_keys._auth_cache_lock:
            assert created["id"] not in runtime.api_keys._usage_policy_cache
    finally:
        release.set()
        await asyncio.gather(authentication, return_exceptions=True)


@pytest.mark.asyncio
async def test_authentication_service_close_is_idempotent_and_rejects_new_work(runtime) -> None:
    await runtime.auth.close()
    await runtime.auth.close()

    with pytest.raises(RuntimeError, match="authentication service is closed"):
        await runtime.auth.authenticate_admin("missing", "wrong", ip="203.0.113.1")


@pytest.mark.asyncio
async def test_oversized_login_password_is_rejected_before_password_work(runtime, monkeypatch) -> None:
    def unexpected_verify(_password: str, _stored_hash: str) -> bool:
        raise AssertionError("oversized password reached PBKDF2 verification")

    monkeypatch.setattr(sessions_module, "verify_password", unexpected_verify)

    with pytest.raises(LookupError, match="invalid admin credentials"):
        await runtime.auth.authenticate_admin(
            "admin",
            "x" * (MAX_ADMIN_PASSWORD_LENGTH + 1),
            ip="203.0.113.2",
        )


@pytest.mark.asyncio
async def test_authentication_close_drains_owned_password_work_and_rejects_new_work(runtime, monkeypatch) -> None:
    started = threading.Event()
    release = threading.Event()

    def slow_rejection(_password: str, _stored_hash: str) -> bool:
        started.set()
        release.wait(timeout=2)
        return False

    monkeypatch.setattr(sessions_module, "verify_password", slow_rejection)
    authentication = asyncio.create_task(
        runtime.auth.authenticate_admin("missing-before-close", "wrong-password")
    )
    await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)

    close_task = asyncio.create_task(runtime.auth.close())
    while runtime.auth._password_executor is not None:
        await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="authentication service is closed"):
        await runtime.auth.authenticate_admin("missing-after-close", "wrong-password")

    release.set()
    with pytest.raises(LookupError, match="invalid admin credentials"):
        await authentication
    await asyncio.wait_for(close_task, timeout=1)


@pytest.mark.asyncio
async def test_cancelled_authentication_close_does_not_strand_executor(runtime, monkeypatch) -> None:
    started = threading.Event()
    release = threading.Event()

    def slow_rejection(_password: str, _stored_hash: str) -> bool:
        started.set()
        release.wait(timeout=2)
        return False

    monkeypatch.setattr(sessions_module, "verify_password", slow_rejection)
    authentication = asyncio.create_task(
        runtime.auth.authenticate_admin("missing-during-close", "wrong-password")
    )
    await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=1)

    interrupted_close = asyncio.create_task(runtime.auth.close())
    while runtime.auth._password_executor is not None:
        await asyncio.sleep(0)
    interrupted_close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await interrupted_close

    release.set()
    with pytest.raises(LookupError, match="invalid admin credentials"):
        await authentication
    await asyncio.wait_for(runtime.auth.close(), timeout=1)


@pytest.mark.asyncio
async def test_password_work_waiter_queue_is_bounded(runtime) -> None:
    runtime.auth._password_slots = asyncio.Semaphore(0)
    runtime.auth._password_waiter_limit = 2
    waiters = [
        asyncio.create_task(runtime.auth._run_password_work(lambda: None))
        for _ in range(2)
    ]
    await asyncio.sleep(0)

    with pytest.raises(AuthenticationOverloadedError, match="work queue is full"):
        await runtime.auth._run_password_work(lambda: None)

    for waiter in waiters:
        waiter.cancel()
    await asyncio.gather(*waiters, return_exceptions=True)
    assert runtime.auth._password_waiters == 0


def test_login_failure_tracking_is_bounded_and_has_ip_wide_limit(runtime) -> None:
    for index in range(LOGIN_FAILURE_MAX_KEYS + 500):
        runtime.auth.record_login_failure(f"attacker-{index}", ip=f"192.0.2.{index % 250}")

    assert len(runtime.auth._login_failures) <= LOGIN_FAILURE_MAX_KEYS
    assert len(runtime.auth._login_ip_failures) <= LOGIN_FAILURE_MAX_KEYS
    assert max(map(len, runtime.auth._login_failures)) <= 128 + 1 + 64

    for index in range(50):
        runtime.auth.record_login_failure(f"rotating-name-{index}", ip="198.51.100.10")
    with pytest.raises(PermissionError, match="too many login attempts"):
        runtime.auth.assert_login_allowed("another-name", ip="198.51.100.10")
