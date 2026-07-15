from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
from pathlib import Path

import pytest

from app.config import Settings
from app.db.read_pool import (
    SQLiteReadPool,
    SQLiteReadPoolClosedError,
    SQLiteReadPoolForkedError,
    SQLiteReadPoolOverloadedError,
    SQLiteReadPoolPausedError,
    SQLiteReadPoolTimeoutError,
)
from app.runtime import RapidInboxRuntime


def _create_probe_database(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    try:
        connection.execute("CREATE TABLE probe (value TEXT NOT NULL)")
        connection.execute("INSERT INTO probe (value) VALUES (?)", (value,))
        connection.commit()
    finally:
        connection.close()


async def _wait_until(predicate, *, attempts: int = 3000) -> None:
    for _ in range(attempts):
        if predicate():
            return
        await asyncio.sleep(0.001)
    raise AssertionError("condition was not reached")


def _read_threads() -> set[int]:
    return {
        id(thread)
        for thread in threading.enumerate()
        if thread.name.startswith("rapid-inbox-db-read-")
    }


def test_constructor_is_threadless_and_start_is_explicit(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "threadless")
    before = _read_threads()
    pool = SQLiteReadPool(database_path, max_connections=2)
    assert _read_threads() == before

    pool.start()
    assert len(_read_threads() - before) == 2
    assert pool.active_connection_count == 0
    asyncio.run(pool.close())
    assert _read_threads() == before


@pytest.mark.asyncio
async def test_worker_reuses_owner_connection_and_resets_read_transaction(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "first")
    pool = SQLiteReadPool(database_path, max_connections=1)
    real_connect = sqlite3.connect
    opened: list[tuple[int, sqlite3.Connection]] = []

    def recording_connect(*args, **kwargs):
        connection = real_connect(*args, **kwargs)
        opened.append((threading.get_ident(), connection))
        return connection

    monkeypatch.setattr("app.db.read_pool.sqlite3.connect", recording_connect)
    pool.start()
    try:
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "first"}
        # Fully materialized SELECTs must not leave a snapshot transaction on
        # the persistent actor connection for the next request.
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "first"}
        assert await pool._submit(lambda connection: connection.in_transaction) is False
        assert len(opened) == 1
        assert opened[0][0] != threading.get_ident()

        assert await pool.fetch_one("PRAGMA foreign_keys") == {"foreign_keys": 1}
        assert await pool.fetch_one("PRAGMA busy_timeout") == {"timeout": 5000}
        assert await pool.fetch_one("PRAGMA query_only") == {"query_only": 1}
        assert len(opened) == 1
    finally:
        await pool.close()
    assert pool.active_connection_count == 0


@pytest.mark.asyncio
async def test_actor_count_hard_limits_concurrent_connections(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "bounded")
    pool = SQLiteReadPool(
        database_path,
        max_connections=3,
        queue_capacity=24,
        max_waiters=24,
    )
    release = threading.Event()
    state_lock = threading.Lock()
    active = 0
    entered = 0
    peak = 0

    def blocking_read(connection: sqlite3.Connection) -> dict[str, str]:
        nonlocal active, entered, peak
        row = connection.execute("SELECT value FROM probe").fetchone()
        with state_lock:
            active += 1
            entered += 1
            peak = max(peak, active)
        try:
            if not release.wait(timeout=3.0):
                raise TimeoutError("test did not release read actors")
            return dict(row)
        finally:
            with state_lock:
                active -= 1

    pool.start()
    tasks = [asyncio.create_task(pool._submit(blocking_read)) for _ in range(24)]
    try:
        await _wait_until(lambda: entered == 3)
        assert pool.active_connection_count == 3
        assert peak == 3
        release.set()
        assert all(result == {"value": "bounded"} for result in await asyncio.gather(*tasks))
        assert peak <= pool.max_connections
        assert pool.active_connection_count <= pool.max_connections
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        await pool.close()


@pytest.mark.asyncio
async def test_admission_is_bounded_and_cancelled_owner_releases_only_on_completion(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "admission")
    pool = SQLiteReadPool(
        database_path,
        max_connections=1,
        queue_capacity=1,
        max_waiters=1,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking_read(connection: sqlite3.Connection) -> str:
        value = str(connection.execute("SELECT value FROM probe").fetchone()[0])
        entered.set()
        if not release.wait(timeout=3.0):
            raise TimeoutError("test did not release admitted read")
        return value

    pool.start()
    owner = asyncio.create_task(pool._submit(blocking_read))
    try:
        await _wait_until(entered.is_set)
        waiter = asyncio.create_task(pool.fetch_one("SELECT value FROM probe"))
        await _wait_until(lambda: len(pool._admission_waiters) == 1)
        with pytest.raises(SQLiteReadPoolOverloadedError):
            await pool.fetch_one("SELECT value FROM probe")

        owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await owner
        assert pool.outstanding_count == 1
        assert not waiter.done()

        release.set()
        assert await waiter == {"value": "admission"}
        await _wait_until(lambda: pool.outstanding_count == 0)
    finally:
        release.set()
        await pool.close()


@pytest.mark.asyncio
async def test_read_deadline_includes_bounded_admission_wait(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "deadline")
    pool = SQLiteReadPool(
        database_path,
        max_connections=1,
        queue_capacity=1,
        max_waiters=1,
        timeout_seconds=0.03,
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking_read(connection: sqlite3.Connection) -> str:
        value = str(connection.execute("SELECT value FROM probe").fetchone()[0])
        entered.set()
        release.wait(timeout=2.0)
        return value

    pool.start()
    owner = asyncio.create_task(pool._submit(blocking_read))
    try:
        await _wait_until(entered.is_set)
        with pytest.raises(SQLiteReadPoolTimeoutError, match="during admission"):
            await pool.fetch_one("SELECT value FROM probe")
        assert pool.outstanding_count == 1
        assert not pool._admission_waiters or all(
            waiter.state != "waiting" for waiter in pool._admission_waiters
        )
    finally:
        release.set()
        await asyncio.gather(owner, return_exceptions=True)
        await pool.close()


@pytest.mark.asyncio
async def test_pause_is_atomic_drains_owner_closes_and_rejects_racing_reads(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    replacement_path = tmp_path / "replacement.db"
    _create_probe_database(database_path, "before-maintenance")
    _create_probe_database(replacement_path, "after-maintenance")
    pool = SQLiteReadPool(database_path, max_connections=1)
    entered = threading.Event()
    release = threading.Event()

    def blocking_read(connection: sqlite3.Connection) -> str:
        value = str(connection.execute("SELECT value FROM probe").fetchone()[0])
        entered.set()
        if not release.wait(timeout=3.0):
            raise TimeoutError("test did not release maintenance read")
        return value

    pool.start()
    owner = asyncio.create_task(pool._submit(blocking_read))
    try:
        await _wait_until(entered.is_set)
        pause = asyncio.create_task(pool.pause_and_drain())
        await _wait_until(lambda: pool._state == "pausing")
        with pytest.raises(SQLiteReadPoolPausedError):
            await pool.fetch_one("SELECT value FROM probe")
        assert not pause.done()

        release.set()
        assert await owner == "before-maintenance"
        await pause
        assert pool.active_connection_count == 0

        os.replace(replacement_path, database_path)
        pool.resume()
        assert await pool.fetch_one("SELECT value FROM probe") == {
            "value": "after-maintenance"
        }
    finally:
        release.set()
        await pool.close()


@pytest.mark.asyncio
async def test_external_inode_replacement_discards_idle_old_snapshot(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    replacement_path = tmp_path / "replacement.db"
    _create_probe_database(database_path, "old")
    _create_probe_database(replacement_path, "new")
    pool = SQLiteReadPool(database_path, max_connections=1)
    pool.start()
    try:
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "old"}
        old_inode = database_path.stat().st_ino
        os.replace(replacement_path, database_path)
        assert database_path.stat().st_ino != old_inode
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "new"}
        assert pool.active_connection_count == 1
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_sqlite_error_discards_connection_and_retries_only_once(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "recovered")
    pool = SQLiteReadPool(database_path, max_connections=1)
    real_connect = sqlite3.connect
    connect_count = 0
    attempts: list[sqlite3.Connection] = []

    def counting_connect(*args, **kwargs):
        nonlocal connect_count
        connect_count += 1
        return real_connect(*args, **kwargs)

    def fail_once(connection: sqlite3.Connection) -> dict[str, str]:
        attempts.append(connection)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("simulated transient read failure")
        return dict(connection.execute("SELECT value FROM probe").fetchone())

    monkeypatch.setattr("app.db.read_pool.sqlite3.connect", counting_connect)
    pool.start()
    try:
        assert await pool._submit(fail_once) == {"value": "recovered"}
        assert len(attempts) == 2
        assert attempts[0] is not attempts[1]
        assert connect_count == 2

        failed_attempts = 0

        def always_fails(_connection: sqlite3.Connection) -> None:
            nonlocal failed_attempts
            failed_attempts += 1
            raise sqlite3.OperationalError("persistent read failure")

        with pytest.raises(sqlite3.OperationalError, match="persistent read failure"):
            await pool._submit(always_fails)
        assert failed_attempts == 2
        assert pool.active_connection_count == 0
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_long_query_has_hard_deadline_and_connection_recovers(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "timeout")
    pool = SQLiteReadPool(
        database_path,
        max_connections=1,
        timeout_seconds=0.02,
    )
    pool.start()
    try:
        with pytest.raises(SQLiteReadPoolTimeoutError, match="deadline exceeded"):
            await pool.fetch_one(
                """
                WITH RECURSIVE counter(value) AS (
                    VALUES (0)
                    UNION ALL
                    SELECT value + 1 FROM counter WHERE value < 100000000
                )
                SELECT SUM(value) AS total FROM counter
                """
            )
        assert pool.outstanding_count == 0
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "timeout"}
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_cancelled_sql_query_is_interrupted_but_worker_releases_capacity(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "cancelled")
    pool = SQLiteReadPool(database_path, max_connections=1, timeout_seconds=5)
    pool.start()
    query = asyncio.create_task(
        pool.fetch_one(
            """
            WITH RECURSIVE counter(value) AS (
                VALUES (0)
                UNION ALL
                SELECT value + 1 FROM counter WHERE value < 100000000
            )
            SELECT SUM(value) AS total FROM counter
            """
        )
    )
    try:
        await _wait_until(lambda: pool.outstanding_count == 1)
        query.cancel()
        with pytest.raises(asyncio.CancelledError):
            await query
        await _wait_until(lambda: pool.outstanding_count == 0)
        assert await pool.fetch_one("SELECT value FROM probe") == {"value": "cancelled"}
    finally:
        query.cancel()
        await pool.close()


@pytest.mark.asyncio
async def test_fatal_actor_fails_active_queued_and_waiting_reads_without_leaking_capacity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "fatal")
    pool = SQLiteReadPool(
        database_path,
        max_connections=1,
        queue_capacity=2,
        max_waiters=1,
    )
    entered = threading.Event()
    release = threading.Event()

    def fatal_run(_worker, _slot, _request):
        entered.set()
        if not release.wait(timeout=3.0):
            raise TimeoutError("test did not release fatal actor")
        raise RuntimeError("deterministic actor failure")

    monkeypatch.setattr(pool, "_run_request", fatal_run)
    pool.start()
    active = asyncio.create_task(pool.fetch_one("SELECT value FROM probe"))
    await _wait_until(entered.is_set)
    queued = asyncio.create_task(pool.fetch_one("SELECT value FROM probe"))
    await _wait_until(lambda: pool.outstanding_count == 2)
    waiting = asyncio.create_task(pool.fetch_one("SELECT value FROM probe"))
    await _wait_until(lambda: len(pool._admission_waiters) == 1)

    release.set()
    results = await asyncio.gather(active, queued, waiting, return_exceptions=True)
    assert all(
        isinstance(result, RuntimeError) and "deterministic actor failure" in str(result)
        for result in results
    )
    await _wait_until(lambda: pool.closed)
    assert pool.outstanding_count == 0
    assert pool.active_connection_count == 0
    with pytest.raises(RuntimeError, match="deterministic actor failure"):
        await pool.close()


@pytest.mark.asyncio
async def test_shutdown_is_idempotent_closes_actors_and_rejects_new_reads(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "closed")
    pool = SQLiteReadPool(database_path, max_connections=2)
    pool.start()

    assert await pool.fetch_one("SELECT value FROM probe") == {"value": "closed"}
    assert pool.active_connection_count == 1
    await asyncio.gather(pool.close(), pool.close())
    await pool.close()

    assert pool.closed is True
    assert pool.active_connection_count == 0
    assert all(worker.thread is not None and not worker.thread.is_alive() for worker in pool._workers)
    with pytest.raises(SQLiteReadPoolClosedError, match="not started or is closing"):
        await pool.fetch_one("SELECT value FROM probe")


@pytest.mark.asyncio
async def test_shutdown_cancellation_still_drains_owner_and_joins_actor(tmp_path: Path) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "shutdown")
    pool = SQLiteReadPool(database_path, max_connections=1)
    entered = threading.Event()
    release = threading.Event()

    def blocking_read(connection: sqlite3.Connection) -> str:
        value = str(connection.execute("SELECT value FROM probe").fetchone()[0])
        entered.set()
        if not release.wait(timeout=3.0):
            raise TimeoutError("test did not release shutdown read")
        return value

    pool.start()
    read = asyncio.create_task(pool._submit(blocking_read))
    await _wait_until(entered.is_set)
    close = asyncio.create_task(pool.close())
    await _wait_until(lambda: pool._state == "closing")
    close.cancel()
    await asyncio.sleep(0)
    assert not close.done()

    release.set()
    assert await read == "shutdown"
    await close
    assert pool.closed
    assert all(worker.thread is not None and not worker.thread.is_alive() for worker in pool._workers)


@pytest.mark.asyncio
async def test_runtime_clear_all_drains_readers_before_vacuum_and_resumes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    entered = threading.Event()
    release = threading.Event()
    compacted = threading.Event()
    original_compact = runtime._compact_mail_database

    def blocking_read(connection: sqlite3.Connection) -> int:
        count = int(connection.execute("SELECT COUNT(*) FROM messages").fetchone()[0])
        entered.set()
        if not release.wait(timeout=5.0):
            raise TimeoutError("test did not release read before VACUUM")
        return count

    def checking_compact(connection: sqlite3.Connection) -> dict[str, int]:
        assert runtime.read_pool.active_connection_count == 0
        compacted.set()
        return original_compact(connection)

    monkeypatch.setattr(runtime, "_compact_mail_database", checking_compact)
    await runtime.start()
    read = asyncio.create_task(runtime.read_pool._submit(blocking_read))
    clear: asyncio.Task[dict[str, object]] | None = None
    try:
        await _wait_until(entered.is_set)
        clear = asyncio.create_task(runtime.clear_all_mail())
        await _wait_until(lambda: runtime.read_pool._state == "pausing")
        assert not compacted.is_set()
        assert runtime.operational_state()["ok"] is False
        assert runtime.operational_state()["database_read_pool"]["state"] == "pausing"

        release.set()
        assert await read == 0
        result = await clear
        assert compacted.is_set()
        assert result["database_checkpoint_busy_before"] == 0
        assert runtime.read_pool._state == "open"
        assert runtime.operational_state()["database_read_pool"]["ok"] is True
        assert await runtime.read_pool.fetch_one("SELECT COUNT(*) AS count FROM messages") == {
            "count": 0
        }
    finally:
        release.set()
        await asyncio.gather(read, return_exceptions=True)
        if clear is not None:
            await asyncio.gather(clear, return_exceptions=True)
        await runtime.stop()


@pytest.mark.asyncio
async def test_reader_fatal_marks_not_ready_and_runtime_still_closes_writer(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)

    def fatal_run(_worker, _slot, _request):
        raise RuntimeError("runtime reader actor failed")

    await runtime.start()
    monkeypatch.setattr(runtime.read_pool, "_run_request", fatal_run)
    with pytest.raises(RuntimeError, match="runtime reader actor failed"):
        await runtime.read_pool.fetch_one("SELECT 1 AS value")
    await _wait_until(lambda: runtime.read_pool.closed)

    operational = runtime.operational_state()
    readiness = await runtime.observability.readiness.check(runtime, force=True)
    assert operational["ok"] is False
    assert operational["database_read_pool"]["ok"] is False
    assert readiness["status"] == "not_ready"
    assert readiness["checks"]["runtime"]["database_read_pool"]["state"] == "closed"

    with pytest.raises(RuntimeError, match="runtime reader actor failed"):
        await runtime.stop()
    assert runtime.writer._state == "closed"
    assert runtime.writer._thread is None or not runtime.writer._thread.is_alive()


def test_started_pool_fails_fast_after_fork_boundary(tmp_path: Path, monkeypatch) -> None:
    database_path = tmp_path / "app.db"
    _create_probe_database(database_path, "fork")
    pool = SQLiteReadPool(database_path, max_connections=1)
    pool.start()
    owner_pid = os.getpid()
    try:
        monkeypatch.setattr("app.db.read_pool.os.getpid", lambda: owner_pid + 1)
        with pytest.raises(SQLiteReadPoolForkedError, match="after fork"):
            asyncio.run(pool.fetch_one("SELECT value FROM probe"))
    finally:
        monkeypatch.setattr("app.db.read_pool.os.getpid", lambda: owner_pid)
        asyncio.run(pool.close())


def test_separate_pools_are_isolated_across_event_loops(tmp_path: Path) -> None:
    first_path = tmp_path / "first.db"
    second_path = tmp_path / "second.db"
    _create_probe_database(first_path, "first-loop")
    _create_probe_database(second_path, "second-loop")
    first_pool = SQLiteReadPool(first_path, max_connections=1)
    second_pool = SQLiteReadPool(second_path, max_connections=1)
    first_pool.start()
    second_pool.start()

    try:
        assert asyncio.run(first_pool.fetch_one("SELECT value FROM probe")) == {
            "value": "first-loop"
        }
        assert asyncio.run(second_pool.fetch_one("SELECT value FROM probe")) == {
            "value": "second-loop"
        }
        assert first_pool.active_connection_count == 1
        assert second_pool.active_connection_count == 1
    finally:
        asyncio.run(first_pool.close())
        asyncio.run(second_pool.close())


def test_one_pool_serves_multiple_event_loops_without_loop_owned_state(tmp_path: Path) -> None:
    database_path = tmp_path / "shared.db"
    _create_probe_database(database_path, "shared-loop")
    pool = SQLiteReadPool(database_path, max_connections=1)
    pool.start()
    barrier = threading.Barrier(3)
    results: list[dict[str, str]] = []
    errors: list[BaseException] = []
    result_lock = threading.Lock()

    def read_from_own_loop() -> None:
        try:
            barrier.wait(timeout=2.0)
            result = asyncio.run(pool.fetch_one("SELECT value FROM probe"))
            with result_lock:
                results.append(result or {})
        except BaseException as exc:  # noqa: BLE001 - test captures thread failures
            with result_lock:
                errors.append(exc)

    threads = [
        threading.Thread(target=read_from_own_loop, name=f"read-loop-{index}")
        for index in range(2)
    ]
    try:
        for thread in threads:
            thread.start()
        barrier.wait(timeout=2.0)
        for thread in threads:
            thread.join(timeout=3.0)
        assert all(not thread.is_alive() for thread in threads)
        assert errors == []
        assert results == [{"value": "shared-loop"}, {"value": "shared-loop"}]
    finally:
        asyncio.run(pool.close())
