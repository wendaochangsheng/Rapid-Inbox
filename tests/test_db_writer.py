from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from app.config import default_settings
from app.db.connection import connect_database, initialize_database
from app.db.writer import (
    DatabaseWriter,
    DatabaseWriterClosedError,
    DatabaseWriterOverloadedError,
)
from app.runtime import RapidInboxRuntime


def _count_probe_rows(database_path: Path) -> int:
    with connect_database(database_path) as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM writer_probe").fetchone()
    return int(row["count"])


async def _wait_for_thread_event(event: threading.Event) -> None:
    while not event.is_set():
        await asyncio.sleep(0.001)


def _database_writer_thread_ids() -> set[int]:
    return {
        id(thread)
        for thread in threading.enumerate()
        if thread.name == "rapid-inbox-db-writer"
    }


def test_writer_construction_and_create_app_do_not_start_actor_threads(tmp_path: Path) -> None:
    before = _database_writer_thread_ids()
    writer = DatabaseWriter(tmp_path / "standalone.db")
    assert writer._thread is None
    assert _database_writer_thread_ids() == before

    # Importing app.main also constructs its module-level ASGI app.  Neither
    # that preload path nor an additional create_app() may create OS threads.
    from app.main import create_app

    assert _database_writer_thread_ids() == before
    app = create_app(settings=default_settings(tmp_path / "app"))
    assert app.state.runtime.writer._thread is None
    assert _database_writer_thread_ids() == before

    asyncio.run(writer.close())
    asyncio.run(app.state.runtime.writer.close())


def test_first_concurrent_submissions_start_exactly_one_actor_thread(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute("CREATE TABLE writer_probe (value TEXT NOT NULL)")

    writer = DatabaseWriter(database_path, queue_capacity=2)
    real_thread_start = threading.Thread.start
    start_count = 0
    count_lock = threading.Lock()

    def counting_thread_start(thread: threading.Thread) -> None:
        nonlocal start_count
        if thread.name == "rapid-inbox-db-writer":
            with count_lock:
                start_count += 1
        real_thread_start(thread)

    monkeypatch.setattr(threading.Thread, "start", counting_thread_start)
    barrier = threading.Barrier(8)
    errors: list[BaseException] = []

    def submit(index: int) -> None:
        try:
            barrier.wait(timeout=2.0)
            asyncio.run(_insert_probe_row(writer, str(index)))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    submitters = [
        threading.Thread(target=submit, args=(index,), name=f"writer-submitter-{index}")
        for index in range(8)
    ]
    try:
        for thread in submitters:
            thread.start()
        for thread in submitters:
            thread.join(timeout=3.0)

        assert all(not thread.is_alive() for thread in submitters)
        assert errors == []
        assert start_count == 1
        assert writer._thread is not None
        assert _count_probe_rows(database_path) == 8
    finally:
        asyncio.run(writer.close())


@pytest.mark.asyncio
async def test_unsubmitted_writer_close_is_threadless_and_idempotent(tmp_path: Path) -> None:
    writer = DatabaseWriter(tmp_path / "missing-parent" / "app.db")
    assert writer._thread is None

    await asyncio.gather(writer.close(), writer.close())
    await writer.close()

    assert writer._thread is None
    with pytest.raises(DatabaseWriterClosedError, match="closing or closed"):
        await writer.execute(lambda connection: connection.execute("SELECT 1"))


@pytest.mark.asyncio
async def test_database_writer_rejects_beyond_bounded_waiter_capacity(tmp_path: Path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    writer = DatabaseWriter(database_path, queue_capacity=1, max_waiters=2)
    started = threading.Event()
    release = threading.Event()

    def blocking_write(connection: sqlite3.Connection) -> None:
        started.set()
        assert release.wait(timeout=2)
        connection.execute("CREATE TABLE IF NOT EXISTS writer_probe (value TEXT NOT NULL)")

    owner = asyncio.create_task(writer.execute(blocking_write))
    await asyncio.wait_for(_wait_for_thread_event(started), timeout=1)
    waiters = [
        asyncio.create_task(
            writer.execute(
                lambda connection, index=index: connection.execute(
                    "INSERT INTO writer_probe (value) VALUES (?)",
                    (str(index),),
                )
            )
        )
        for index in range(2)
    ]
    await asyncio.sleep(0.02)

    with pytest.raises(DatabaseWriterOverloadedError, match="admission queue is full"):
        await writer.execute(lambda connection: connection.execute("SELECT 1"))

    release.set()
    await asyncio.gather(owner, *waiters)
    await writer.close()
    assert _count_probe_rows(database_path) == 2


def test_short_lived_read_connections_keep_wal_and_safety_pragmas(tmp_path: Path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        assert str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal"
        assert int(connection.execute("PRAGMA foreign_keys").fetchone()[0]) == 1
        assert int(connection.execute("PRAGMA busy_timeout").fetchone()[0]) == 5000
        assert int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 2


def test_new_domain_schema_is_private_by_default(tmp_path: Path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        columns = {
            str(row["name"]): str(row["dflt_value"])
            for row in connection.execute("PRAGMA table_info(domains)").fetchall()
        }

    assert columns["public_web_enabled"] == "0"
    assert columns["public_api_enabled"] == "0"


async def _insert_probe_row(writer: DatabaseWriter, value: str) -> None:
    def operation(connection: sqlite3.Connection) -> None:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS writer_probe (
                value TEXT NOT NULL
            )
            """
        )
        connection.execute("INSERT INTO writer_probe (value) VALUES (?)", (value,))

    await writer.execute(operation)


def test_database_writer_supports_calls_from_different_event_loops(tmp_path: Path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    writer = DatabaseWriter(database_path)

    started = threading.Event()
    thread_error: list[BaseException] = []

    async def hold_writer_lock() -> None:
        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS writer_probe (
                    value TEXT NOT NULL
                )
                """
            )
            connection.execute("INSERT INTO writer_probe (value) VALUES (?)", ("thread-loop",))
            started.set()
            time.sleep(0.2)

        await writer.execute(operation)

    def run_first_loop() -> None:
        try:
            asyncio.run(hold_writer_lock())
        except BaseException as exc:  # noqa: BLE001
            thread_error.append(exc)

    thread = threading.Thread(target=run_first_loop)
    thread.start()
    assert started.wait(timeout=2.0)

    asyncio.run(asyncio.wait_for(_insert_probe_row(writer, "main-loop"), timeout=1.0))
    thread.join()
    assert thread_error == []
    assert _count_probe_rows(database_path) == 2
    asyncio.run(writer.close())


@pytest.mark.asyncio
async def test_database_writer_does_not_starve_default_executor_and_serializes_all_work(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute("CREATE TABLE writer_probe (value TEXT NOT NULL)")

    writer = DatabaseWriter(database_path, queue_capacity=64)
    state_lock = threading.Lock()
    first_started = threading.Event()
    active = 0
    max_active = 0

    def slow_insert(connection: sqlite3.Connection, value: str) -> None:
        nonlocal active, max_active
        with state_lock:
            active += 1
            max_active = max(max_active, active)
        first_started.set()
        try:
            time.sleep(0.02)
            connection.execute("INSERT INTO writer_probe (value) VALUES (?)", (value,))
        finally:
            with state_lock:
                active -= 1

    async def submit(index: int) -> None:
        operation = lambda connection: slow_insert(connection, str(index))
        if index % 7 == 0:
            await writer.execute_maintenance(operation)
        else:
            await writer.execute(operation)

    writes = [asyncio.create_task(submit(index)) for index in range(64)]
    try:
        await asyncio.wait_for(_wait_for_thread_event(first_started), timeout=1.0)

        # With the old to_thread + mutex design these probes sat behind all 64
        # writer calls in the default executor for well over half a second.
        probe_started = time.monotonic()
        await asyncio.wait_for(
            asyncio.gather(*(asyncio.to_thread(time.monotonic) for _ in range(8))),
            timeout=0.4,
        )
        assert time.monotonic() - probe_started < 0.4

        await asyncio.wait_for(asyncio.gather(*writes), timeout=5.0)
        assert max_active == 1
        assert active == 0
        assert _count_probe_rows(database_path) == 64
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_catastrophic_worker_failure_closes_admission_before_draining_owned_work(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    writer = DatabaseWriter(database_path, queue_capacity=2, max_waiters=2)
    worker_entered = threading.Event()
    release_worker = threading.Event()
    queued_operation_ran = threading.Event()
    waiting_operation_ran = threading.Event()

    def catastrophic_run_request(
        _connection: sqlite3.Connection,
        _request,
    ) -> None:
        worker_entered.set()
        assert release_worker.wait(timeout=3.0)
        raise RuntimeError("catastrophic writer failure")

    monkeypatch.setattr(writer, "_run_request", catastrophic_run_request)

    first = asyncio.create_task(writer.execute(lambda _connection: None))
    second = None
    waiting = None
    try:
        await asyncio.wait_for(_wait_for_thread_event(worker_entered), timeout=1.0)
        second = asyncio.create_task(
            writer.execute(lambda _connection: queued_operation_ran.set())
        )

        # The first request is active and the second owns the remaining queue
        # capacity.  A third caller must therefore be an admission waiter when
        # the worker crashes.
        waiting = asyncio.create_task(
            writer.execute(lambda _connection: waiting_operation_ran.set())
        )
        for _ in range(1000):
            with writer._state_lock:
                waiting_count = sum(
                    waiter.state == "waiting"
                    for waiter in writer._admission_waiters
                )
            if waiting_count == 1:
                break
            await asyncio.sleep(0.001)
        assert waiting_count == 1

        release_worker.set()
        first_result, second_result, waiting_result = await asyncio.wait_for(
            asyncio.gather(first, second, waiting, return_exceptions=True),
            timeout=2.0,
        )

        assert isinstance(first_result, RuntimeError)
        assert str(first_result) == "catastrophic writer failure"
        assert isinstance(second_result, RuntimeError)
        assert str(second_result) == "catastrophic writer failure"
        assert isinstance(waiting_result, DatabaseWriterClosedError)
        assert queued_operation_ran.is_set() is False
        assert waiting_operation_ran.is_set() is False

        # Request futures can be completed just before the worker's finalizer
        # publishes the terminal actor state.  close() observes that terminal
        # future and must surface the same catastrophic failure without
        # hanging.
        with pytest.raises(RuntimeError, match="catastrophic writer failure"):
            await asyncio.wait_for(writer.close(), timeout=1.0)
        with writer._state_lock:
            assert writer._state == "closed"
            assert writer._outstanding == 0
            assert not writer._admission_waiters
    finally:
        release_worker.set()
        for task in (first, second, waiting):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first, second, waiting) if task is not None),
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_cancelled_owned_write_is_committed_and_close_drains_then_fails_fast(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute("CREATE TABLE writer_probe (value TEXT NOT NULL)")

    writer = DatabaseWriter(database_path, queue_capacity=2)
    first_started = threading.Event()
    release_first = threading.Event()
    second_started = threading.Event()
    release_second = threading.Event()

    def first(connection: sqlite3.Connection) -> None:
        first_started.set()
        assert release_first.wait(timeout=3.0)
        connection.execute("INSERT INTO writer_probe (value) VALUES ('first')")

    def second(connection: sqlite3.Connection) -> None:
        second_started.set()
        assert release_second.wait(timeout=3.0)
        connection.execute("INSERT INTO writer_probe (value) VALUES ('second')")

    first_task = asyncio.create_task(writer.execute(first))
    cancelled_owner = None
    waiting_submitter = None
    close_task = None
    try:
        await asyncio.wait_for(_wait_for_thread_event(first_started), timeout=1.0)

        # Capacity is now owned by one running and one queued transaction.
        cancelled_owner = asyncio.create_task(writer.execute(second))
        await asyncio.sleep(0.02)
        cancelled_owner.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_owner

        # This request is still waiting for admission.  close() must reject it
        # while preserving and draining the cancelled caller's owned write.
        waiting_submitter = asyncio.create_task(
            writer.execute(
                lambda connection: connection.execute(
                    "INSERT INTO writer_probe (value) VALUES ('not-owned')"
                )
            )
        )
        await asyncio.sleep(0.02)
        close_task = asyncio.create_task(writer.close())

        with pytest.raises(DatabaseWriterClosedError, match="closing or closed"):
            await asyncio.wait_for(waiting_submitter, timeout=0.5)
        with pytest.raises(DatabaseWriterClosedError, match="closing or closed"):
            await asyncio.wait_for(
                writer.execute(lambda connection: connection.execute("SELECT 1")),
                timeout=0.1,
            )
        assert not close_task.done()

        release_first.set()
        await asyncio.wait_for(_wait_for_thread_event(second_started), timeout=1.0)
        assert not close_task.done()
        release_second.set()

        await asyncio.wait_for(first_task, timeout=1.0)
        await asyncio.wait_for(close_task, timeout=1.0)
        await writer.close()
        assert _count_probe_rows(database_path) == 2
    finally:
        release_first.set()
        release_second.set()
        if not first_task.done():
            await first_task
        if close_task is None or not close_task.done():
            await writer.close()


@pytest.mark.asyncio
async def test_writer_rolls_back_failed_transaction_and_keeps_full_durability(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "storage" / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute("CREATE TABLE writer_probe (value TEXT NOT NULL)")

    writer = DatabaseWriter(database_path)

    def failing_write(connection: sqlite3.Connection) -> None:
        assert int(connection.execute("PRAGMA synchronous").fetchone()[0]) == 2
        connection.execute("INSERT INTO writer_probe (value) VALUES ('rolled-back')")
        raise ValueError("abort transaction")

    try:
        await writer.execute(lambda connection: connection.execute("PRAGMA synchronous = NORMAL"))
        with pytest.raises(ValueError, match="abort transaction"):
            await writer.execute(failing_write)
        await _insert_probe_row(writer, "committed")
        assert _count_probe_rows(database_path) == 1
    finally:
        await writer.close()


@pytest.mark.asyncio
async def test_runtime_stop_closes_writer_and_is_idempotent(tmp_path: Path) -> None:
    runtime = RapidInboxRuntime(default_settings(tmp_path))
    await runtime.start()
    write_started = threading.Event()
    release_write = threading.Event()

    def slow_write(connection: sqlite3.Connection) -> None:
        write_started.set()
        assert release_write.wait(timeout=3.0)
        connection.execute("CREATE TABLE writer_probe (value TEXT NOT NULL)")
        connection.execute("INSERT INTO writer_probe VALUES ('drained')")

    write_task = asyncio.create_task(runtime.writer.execute(slow_write))
    stop_task = None
    try:
        await asyncio.wait_for(_wait_for_thread_event(write_started), timeout=1.0)
        stop_task = asyncio.create_task(runtime.stop())
        await asyncio.sleep(0.05)
        assert not stop_task.done()

        release_write.set()
        await asyncio.wait_for(write_task, timeout=1.0)
        await asyncio.wait_for(stop_task, timeout=1.0)
        await runtime.stop()

        assert _count_probe_rows(runtime.settings.database_path) == 1
        with pytest.raises(DatabaseWriterClosedError, match="closing or closed"):
            await runtime.writer.execute(lambda connection: connection.execute("SELECT 1"))
        with pytest.raises(RuntimeError, match="cannot be restarted"):
            await runtime.start()
    finally:
        release_write.set()
        if not write_task.done():
            await write_task
        if stop_task is None or not stop_task.done():
            await runtime.stop()
