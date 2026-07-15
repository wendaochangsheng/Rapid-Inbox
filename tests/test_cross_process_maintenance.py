from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from pathlib import Path

import pytest

import app.ingest.storage as storage_module
from app.auth.api_keys import ApiKeyAuthorizationError
from app.config import Settings
from app.ingest.storage import FileStorage
from app.runtime import RapidInboxRuntime
from conftest import connect_database


def _write_json_atomic(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(f".{path.name}.test-part")
    temporary.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


def test_maintenance_lease_uses_private_unique_token_and_accepts_matching_ack(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    storage = FileStorage(settings)
    lease = storage.begin_maintenance("test")
    try:
        lock_payload = json.loads(lease.path.read_text(encoding="utf-8"))
        assert lock_payload["token"] == lease.token
        assert len(lease.token) == 32
        assert lease.path.stat().st_mode & 0o777 == 0o600

        _write_json_atomic(
            settings.storage_root / storage_module.INGEST_STATUS_FILENAME,
            {
                "instance_id": "ingest_test",
                "pid": os.getpid(),
                "updated_at": "2026-07-15T00:00:00Z",
                "token": lease.token,
                "queue_messages": 0,
                "queue_bytes": 0,
            },
        )
        _write_json_atomic(
            settings.storage_root / storage_module.MAINTENANCE_DRAINED_FILENAME,
            {"instance_id": "ingest_test", "token": lease.token},
        )

        storage.wait_for_ingestd_drain(
            lease,
            timeout_seconds=0.2,
            heartbeat_fresh_seconds=5,
            poll_seconds=0.005,
        )
    finally:
        storage.end_maintenance(lease)

    assert not lease.path.exists()
    assert not (settings.storage_root / storage_module.MAINTENANCE_DRAINED_FILENAME).exists()


def test_stale_dead_ingest_status_does_not_block_maintenance(tmp_path, monkeypatch) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    storage = FileStorage(settings)
    status_path = settings.storage_root / storage_module.INGEST_STATUS_FILENAME
    _write_json_atomic(
        status_path,
        {"instance_id": "dead", "pid": 999_999, "queue_messages": 10},
    )
    old_timestamp = time.time() - 60
    os.utime(status_path, (old_timestamp, old_timestamp))
    monkeypatch.setattr(storage, "_status_process_is_alive", lambda _status: False)

    lease = storage.begin_maintenance("test-stale")
    try:
        started = time.monotonic()
        storage.wait_for_ingestd_drain(
            lease,
            timeout_seconds=0.2,
            heartbeat_fresh_seconds=1,
            poll_seconds=0.005,
        )
        assert time.monotonic() - started < 0.1
    finally:
        storage.end_maintenance(lease)


def test_stale_live_or_unverifiable_ingest_status_fails_closed(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    settings.ensure_directories()
    storage = FileStorage(settings)
    status_path = settings.storage_root / storage_module.INGEST_STATUS_FILENAME
    _write_json_atomic(
        status_path,
        {
            "instance_id": "stalled-but-live",
            "pid": os.getpid(),
            "queue_messages": 10,
            "queue_bytes": 1024,
        },
    )
    old_timestamp = time.time() - 60
    os.utime(status_path, (old_timestamp, old_timestamp))

    lease = storage.begin_maintenance("test-stalled-live")
    try:
        with pytest.raises(TimeoutError, match="ingestd to drain"):
            storage.wait_for_ingestd_drain(
                lease,
                timeout_seconds=0.05,
                heartbeat_fresh_seconds=1,
                poll_seconds=0.005,
            )
    finally:
        storage.end_maintenance(lease)


@pytest.mark.asyncio
async def test_clear_mail_aborts_on_fresh_ingest_without_matching_ack(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("handshake-timeout.example")
        response = await runtime.accept_message(
            rcpt_tos=["box@handshake-timeout.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        message_id = response.removeprefix("250 queued as ")
        _write_json_atomic(
            settings.storage_root / storage_module.INGEST_STATUS_FILENAME,
            {
                "instance_id": "stuck-ingestd",
                "pid": 999999,
                "updated_at": "2026-07-15T00:00:00Z",
                "token": None,
                "queue_messages": 1,
                "queue_bytes": len(sample_email_bytes),
            },
        )
        monkeypatch.setattr(storage_module, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 0.15)
        monkeypatch.setattr(storage_module, "INGEST_STATUS_FRESH_SECONDS", 10.0)
        monkeypatch.setattr(storage_module, "MAINTENANCE_DRAIN_POLL_SECONDS", 0.01)

        with pytest.raises(TimeoutError, match="ingestd to drain"):
            await runtime.clear_all_mail()

        with connect_database(settings.database_path) as connection:
            message = connection.execute(
                "SELECT raw_path FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            maintenance = connection.execute(
                """
                SELECT status, error
                FROM maintenance_runs
                WHERE kind = 'clear_all_mail'
                ORDER BY rowid DESC
                LIMIT 1
                """
            ).fetchone()
        assert message is not None
        assert runtime.storage.resolve(message["raw_path"]).is_file()
        assert dict(maintenance) == {
            "status": "failed",
            "error": "TimeoutError: timed out waiting for ingestd to drain for maintenance",
        }
        assert not (settings.storage_root / storage_module.MAINTENANCE_LOCK_FILENAME).exists()
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_clear_mail_waits_for_matching_cross_process_ack(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    stop_heartbeat = threading.Event()
    observed_token: list[str] = []
    try:
        await runtime.create_domain("handshake-success.example")
        await runtime.accept_message(
            rcpt_tos=["box@handshake-success.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        monkeypatch.setattr(storage_module, "MAINTENANCE_DRAIN_TIMEOUT_SECONDS", 2.0)
        monkeypatch.setattr(storage_module, "INGEST_STATUS_FRESH_SECONDS", 1.0)
        monkeypatch.setattr(storage_module, "MAINTENANCE_DRAIN_POLL_SECONDS", 0.01)

        status_path = settings.storage_root / storage_module.INGEST_STATUS_FILENAME
        lock_path = settings.storage_root / storage_module.MAINTENANCE_LOCK_FILENAME
        drained_path = settings.storage_root / storage_module.MAINTENANCE_DRAINED_FILENAME

        def simulated_ingestd() -> None:
            while not stop_heartbeat.is_set():
                _write_json_atomic(
                    status_path,
                    {
                        "instance_id": "simulated-ingestd",
                        "pid": os.getpid(),
                        "updated_at": "2026-07-15T00:00:00Z",
                        "token": None,
                        "queue_messages": 0,
                        "queue_bytes": 0,
                    },
                )
                if lock_path.exists():
                    try:
                        token = str(json.loads(lock_path.read_text(encoding="utf-8"))["token"])
                    except (OSError, KeyError, json.JSONDecodeError):
                        time.sleep(0.005)
                        continue
                    observed_token.append(token)
                    _write_json_atomic(
                        drained_path,
                        {"instance_id": "simulated-ingestd", "token": token},
                    )
                    return
                time.sleep(0.01)

        heartbeat_thread = threading.Thread(target=simulated_ingestd, daemon=True)
        heartbeat_thread.start()
        for _ in range(100):
            if status_path.exists():
                break
            await asyncio.sleep(0.005)

        result = await runtime.clear_all_mail()
        heartbeat_thread.join(timeout=2)

        assert observed_token and len(observed_token[0]) == 32
        assert result["messages"] == 1
        with connect_database(settings.database_path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 0
    finally:
        stop_heartbeat.set()
        await runtime.stop()


@pytest.mark.asyncio
async def test_clear_mail_revoked_while_waiting_for_drain_fails_closed_and_releases_lock(
    runtime,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    await runtime.create_domain("revoked-clear-all.example")
    response = await runtime.accept_message(
        rcpt_tos=["box@revoked-clear-all.example"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
    )
    await runtime.drain_parser_queue()
    message_id = response.removeprefix("250 queued as ")
    actor = await runtime.api_keys.create_key(
        name="revoked-clear-all-actor",
        kind="admin",
        scopes=["system.write"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
    )
    stale_actor = runtime.api_keys.authenticate_plain_text(actor["plain_text"])
    drain_entered = threading.Event()
    release_drain = threading.Event()

    def paused_drain(_lease, **_kwargs) -> None:
        drain_entered.set()
        if not release_drain.wait(timeout=5):
            raise TimeoutError("test drain latch was not released")

    monkeypatch.setattr(runtime.storage, "wait_for_ingestd_drain", paused_drain)
    clear_task = asyncio.create_task(
        runtime.clear_all_mail(authorization_principal=stale_actor)
    )
    try:
        assert await asyncio.wait_for(
            asyncio.to_thread(drain_entered.wait, 2),
            timeout=3,
        )
        lock_path = runtime.settings.storage_root / storage_module.MAINTENANCE_LOCK_FILENAME
        assert lock_path.exists()
        with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
            connection.execute(
                "UPDATE api_keys SET status = 'revoked' WHERE id = ?",
                (actor["id"],),
            )
    finally:
        release_drain.set()

    with pytest.raises(ApiKeyAuthorizationError, match="no longer active"):
        await asyncio.wait_for(clear_task, timeout=5)

    with connect_database(runtime.settings.database_path) as connection:
        message = connection.execute(
            "SELECT raw_path FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        maintenance = connection.execute(
            """
            SELECT status, error
            FROM maintenance_runs
            WHERE kind = 'clear_all_mail'
            ORDER BY rowid DESC
            LIMIT 1
            """
        ).fetchone()
        running_count = int(
            connection.execute(
                "SELECT COUNT(*) FROM maintenance_runs WHERE status = 'running'"
            ).fetchone()[0]
        )
    assert message is not None
    assert runtime.storage.resolve(str(message["raw_path"])).is_file()
    assert dict(maintenance)["status"] == "failed"
    assert "ApiKeyAuthorizationError" in str(maintenance["error"])
    assert running_count == 0
    assert not lock_path.exists()
    assert runtime._mail_maintenance_active is False
    assert runtime.parse_queue.is_running

    probe_lease = runtime.storage.begin_maintenance("post-authorization-failure-probe")
    runtime.storage.end_maintenance(probe_lease)
