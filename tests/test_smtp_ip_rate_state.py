from __future__ import annotations

from time import perf_counter

import pytest

from app import runtime as runtime_module
from app.config import Settings
from app.runtime import (
    SMTP_IP_RATE_STATE_MAX_ENTRIES,
    RapidInboxRuntime,
)


def _settings(tmp_path, *, rate_limit: int, max_connections: int = 100_000) -> Settings:
    return Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        smtp_max_concurrent_connections=max_connections,
        smtp_connection_rate_limit_count=rate_limit,
        smtp_connection_rate_limit_window_seconds=60,
    )


def _ipv6(index: int) -> str:
    return f"2001:db8:{index >> 16:x}::{index & 0xffff:x}"


@pytest.mark.asyncio
async def test_python_smtp_rate_state_handles_ten_thousand_rotating_ipv6_sources_in_linear_time(
    tmp_path,
) -> None:
    runtime = RapidInboxRuntime(
        _settings(tmp_path, rate_limit=1, max_connections=3_000)
    )
    started = perf_counter()
    try:
        for index in range(10_000):
            session_id = f"smtp-churn-{index}"
            allowed, reason = await runtime.register_smtp_connection(session_id, _ipv6(index))
            assert allowed is True
            assert reason is None
            assert await runtime.release_smtp_connection(session_id) is True

        elapsed = perf_counter() - started
        assert len(runtime._smtp_ip_windows) == 10_000
        assert len(runtime._smtp_ip_expiry_order) == 10_000
        assert runtime.active_smtp_connection_count() == 0
        # This catches the former all-map scan on every connection without
        # imposing a tight microbenchmark threshold on slower CI machines.
        assert elapsed < 5.0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_python_smtp_rate_state_hard_caps_at_65536_and_evicts_lru(tmp_path) -> None:
    runtime = RapidInboxRuntime(_settings(tmp_path, rate_limit=1))
    capacity = runtime._smtp_ip_window_capacity()
    assert capacity == SMTP_IP_RATE_STATE_MAX_ENTRIES == 65_536
    try:
        for index in range(capacity):
            session_id = f"smtp-cap-{index}"
            allowed, _reason = await runtime.register_smtp_connection(session_id, _ipv6(index))
            assert allowed is True
            await runtime.release_smtp_connection(session_id)

        # A rejected access refreshes the first source's LRU position without
        # extending its accepted-time rate window.
        touched_ip = _ipv6(0)
        allowed, reason = await runtime.register_smtp_connection("smtp-cap-touch", touched_ip)
        assert allowed is False
        assert reason == "per-ip connection rate limit exceeded"

        newest_ip = _ipv6(capacity)
        allowed, _reason = await runtime.register_smtp_connection("smtp-cap-new", newest_ip)
        assert allowed is True
        await runtime.release_smtp_connection("smtp-cap-new")

        assert len(runtime._smtp_ip_windows) == capacity
        assert len(runtime._smtp_ip_expiry_order) == capacity
        assert touched_ip in runtime._smtp_ip_windows
        assert _ipv6(1) not in runtime._smtp_ip_windows
        assert newest_ip in runtime._smtp_ip_windows
        assert runtime.active_smtp_connection_count() == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_python_smtp_rate_state_expires_in_accept_order_without_full_scan(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = RapidInboxRuntime(_settings(tmp_path, rate_limit=10, max_connections=1024))
    now = 0.0
    monkeypatch.setattr(runtime_module, "monotonic", lambda: now)
    try:
        for index in range(3):
            now = float(index)
            session_id = f"smtp-expiry-{index}"
            allowed, _reason = await runtime.register_smtp_connection(session_id, _ipv6(index))
            assert allowed is True
            await runtime.release_smtp_connection(session_id)

        assert list(runtime._smtp_ip_expiry_order) == [_ipv6(0), _ipv6(1), _ipv6(2)]
        now = 62.0
        allowed, _reason = await runtime.register_smtp_connection("smtp-expiry-fresh", _ipv6(99))
        assert allowed is True

        assert list(runtime._smtp_ip_windows) == [_ipv6(99)]
        assert list(runtime._smtp_ip_expiry_order) == [_ipv6(99)]
        assert runtime.active_smtp_connection_count() == 1
        assert await runtime.release_smtp_connection("smtp-expiry-fresh") is True
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_python_smtp_per_ip_window_preserves_large_legal_burst_and_exact_boundary(
    tmp_path,
    monkeypatch,
) -> None:
    burst_size = 10_000
    runtime = RapidInboxRuntime(_settings(tmp_path, rate_limit=burst_size))
    now = 100.0
    monkeypatch.setattr(runtime_module, "monotonic", lambda: now)
    remote_ip = "2001:db8::b"
    try:
        for index in range(burst_size):
            session_id = f"smtp-burst-{index}"
            allowed, reason = await runtime.register_smtp_connection(session_id, remote_ip)
            assert allowed is True
            assert reason is None
            await runtime.release_smtp_connection(session_id)

        rejected, reason = await runtime.register_smtp_connection("smtp-burst-rejected", remote_ip)
        assert rejected is False
        assert reason == "per-ip connection rate limit exceeded"
        assert len(runtime._smtp_ip_windows[remote_ip]) == burst_size
        assert runtime.active_smtp_connection_count() == 0

        # Timestamps at the inclusive cutoff leave the exact sliding window.
        now += 60.0
        allowed, reason = await runtime.register_smtp_connection("smtp-burst-reset", remote_ip)
        assert allowed is True
        assert reason is None
        assert list(runtime._smtp_ip_windows[remote_ip]) == [now]
        assert runtime.active_smtp_connection_count() == 1
        assert await runtime.release_smtp_connection("smtp-burst-reset") is True
    finally:
        await runtime.stop()
