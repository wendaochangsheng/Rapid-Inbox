from __future__ import annotations

import asyncio
import time

import pytest

from app.config import Settings
from app.ingest.queue import ParseQueue, ParseTask
from app.runtime import RapidInboxRuntime
from conftest import connect_database


@pytest.mark.asyncio
async def test_parse_queue_runs_configured_workers_concurrently() -> None:
    started: list[str] = []
    both_started = asyncio.Event()
    release_workers = asyncio.Event()

    async def worker(task: ParseTask) -> None:
        started.append(task.message_id)
        if len(started) == 2:
            both_started.set()
        await release_workers.wait()

    queue = ParseQueue(worker, worker_count=2)
    await queue.start()
    try:
        await queue.enqueue(ParseTask(message_id="msg_one", raw_size_bytes=1))
        await queue.enqueue(ParseTask(message_id="msg_two", raw_size_bytes=1))
        await asyncio.wait_for(both_started.wait(), timeout=2)
        release_workers.set()
        await asyncio.wait_for(queue.drain(), timeout=2)
    finally:
        release_workers.set()
        await queue.stop()

    assert set(started) == {"msg_one", "msg_two"}


@pytest.mark.asyncio
async def test_parse_queue_waits_only_for_matching_active_tasks() -> None:
    started_old = asyncio.Event()
    release_old = asyncio.Event()

    async def worker(task: ParseTask) -> None:
        if task.message_id == "msg_old":
            started_old.set()
            await release_old.wait()

    queue = ParseQueue(worker, worker_count=2)
    await queue.start()
    try:
        await queue.enqueue(ParseTask(message_id="msg_old", raw_size_bytes=1))
        await asyncio.wait_for(started_old.wait(), timeout=2)

        waiter = asyncio.create_task(queue.wait_until_not_active(lambda message_id: message_id == "msg_old"))
        await asyncio.sleep(0)
        assert waiter.done() is False

        release_old.set()
        await asyncio.wait_for(waiter, timeout=2)
        await asyncio.wait_for(queue.drain(), timeout=2)
    finally:
        release_old.set()
        await queue.stop()


@pytest.mark.asyncio
async def test_parse_queue_stop_can_discard_pending_tasks() -> None:
    started: list[str] = []
    worker_started = asyncio.Event()

    async def worker(task: ParseTask) -> None:
        started.append(task.message_id)
        worker_started.set()
        await asyncio.sleep(60)

    queue = ParseQueue(worker, worker_count=1)
    await queue.start()
    await queue.enqueue(ParseTask(message_id="msg_active", raw_size_bytes=1))
    await queue.enqueue(ParseTask(message_id="msg_pending", raw_size_bytes=1))
    await asyncio.wait_for(worker_started.wait(), timeout=2)

    await queue.stop(discard_pending=True, timeout=0.01)

    assert started == ["msg_active"]
    assert queue.is_running is False


@pytest.mark.asyncio
async def test_parse_source_database_lookup_does_not_block_event_loop(runtime, monkeypatch) -> None:
    def slow_missing_lookup(_message_id: str):
        time.sleep(0.2)
        return None

    monkeypatch.setattr(runtime, "_load_parse_message_source", slow_missing_lookup)
    started_at = time.monotonic()
    parse_task = asyncio.create_task(
        runtime._parse_message(ParseTask(message_id="msg_missing", raw_size_bytes=1))
    )

    await asyncio.sleep(0.02)
    timer_elapsed = time.monotonic() - started_at
    await parse_task

    assert timer_elapsed < 0.1


@pytest.mark.asyncio
async def test_parse_queue_bounds_messages_and_bytes_across_active_and_pending_tasks() -> None:
    worker_started = asyncio.Event()
    release_worker = asyncio.Event()

    async def worker(_task: ParseTask) -> None:
        worker_started.set()
        await release_worker.wait()

    queue = ParseQueue(worker, worker_count=1, max_messages=2, max_bytes=10)
    await queue.start()
    try:
        assert queue.try_enqueue(ParseTask(message_id="msg_active", raw_size_bytes=6)) is True
        await asyncio.wait_for(worker_started.wait(), timeout=2)

        assert queue.active_messages == 1
        assert queue.reserved_messages == 1
        assert queue.reserved_bytes == 6
        assert queue.try_enqueue(ParseTask(message_id="msg_too_large", raw_size_bytes=5)) is False
        assert queue.try_enqueue(ParseTask(message_id="msg_pending", raw_size_bytes=4)) is True
        assert queue.try_enqueue(ParseTask(message_id="msg_count_full", raw_size_bytes=0)) is False
        assert queue.try_enqueue(ParseTask(message_id="msg_active", raw_size_bytes=1)) is False
        assert queue.reserved_messages == 2
        assert queue.reserved_bytes == 10
        assert queue.queued_messages == 1
        assert queue.remove_pending(lambda task: task.message_id == "msg_pending") == 1
        assert queue.reserved_messages == 1
        assert queue.reserved_bytes == 6
        assert queue.try_enqueue(ParseTask(message_id="msg_pending", raw_size_bytes=4)) is True

        release_worker.set()
        await asyncio.wait_for(queue.drain(), timeout=2)
    finally:
        release_worker.set()
        await queue.stop()

    assert queue.reserved_messages == 0
    assert queue.reserved_bytes == 0


@pytest.mark.asyncio
async def test_parse_queue_clear_timeout_and_restart_release_all_reservations() -> None:
    active_started = asyncio.Event()
    release_active = asyncio.Event()
    completed: list[str] = []

    async def worker(task: ParseTask) -> None:
        if task.message_id == "msg_active":
            active_started.set()
            await release_active.wait()
        completed.append(task.message_id)

    queue = ParseQueue(worker, worker_count=1, max_messages=2, max_bytes=10)
    await queue.start()
    assert queue.try_enqueue(ParseTask(message_id="msg_active", raw_size_bytes=6)) is True
    assert queue.try_enqueue(ParseTask(message_id="msg_pending", raw_size_bytes=4)) is True
    await asyncio.wait_for(active_started.wait(), timeout=2)

    await queue.stop(discard_pending=True, timeout=0.01)

    assert queue.is_running is False
    assert queue.reserved_messages == 0
    assert queue.reserved_bytes == 0
    # A rejected ID and a discarded pending ID must not leave a stale
    # duplicate marker after shutdown.
    assert queue.try_enqueue(ParseTask(message_id="msg_pending", raw_size_bytes=10)) is True
    assert queue.clear_pending() == 1
    assert queue.reserved_messages == 0
    assert queue.reserved_bytes == 0

    await queue.start()
    try:
        assert queue.try_enqueue(ParseTask(message_id="msg_after_restart", raw_size_bytes=10)) is True
        await asyncio.wait_for(queue.drain(), timeout=2)
    finally:
        release_active.set()
        await queue.stop()

    assert completed == ["msg_after_restart"]
    assert queue.reserved_messages == 0
    assert queue.reserved_bytes == 0


@pytest.mark.asyncio
async def test_durable_message_skipped_when_parse_queue_is_full_is_eventually_requeued(
    tmp_path,
    sample_email_bytes: bytes,
    monkeypatch,
) -> None:
    # Force the first scan page to contain only the already-active message.
    # The next keyset page must still reach the durable skipped message.
    monkeypatch.setattr("app.runtime.PENDING_PARSE_SCAN_BATCH_SIZE", 1)
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        parse_queue_max_messages=1,
    )
    runtime = RapidInboxRuntime(settings)
    first_parse_started = asyncio.Event()
    release_first_parse = asyncio.Event()
    original_worker = runtime.parse_queue._worker

    async def block_first_parse(task: ParseTask) -> None:
        if not first_parse_started.is_set():
            first_parse_started.set()
            await release_first_parse.wait()
        await original_worker(task)

    runtime.parse_queue._worker = block_first_parse
    await runtime.start()
    try:
        await runtime.create_domain("bounded.example")
        first_response = await runtime.accept_message(
            rcpt_tos=["first@bounded.example"],
            envelope_from="sender@example.net",
            content=sample_email_bytes,
        )
        await asyncio.wait_for(first_parse_started.wait(), timeout=2)
        second_response = await runtime.accept_message(
            rcpt_tos=["second@bounded.example"],
            envelope_from="sender@example.net",
            content=sample_email_bytes,
        )
        second_message_id = second_response.removeprefix("250 queued as ")

        assert first_response.startswith("250 queued as ")
        assert second_response.startswith("250 queued as ")
        assert runtime.parse_queue.contains(second_message_id) is False
        with connect_database(settings.database_path) as connection:
            second_row = connection.execute(
                "SELECT parse_status FROM messages WHERE id = ?",
                (second_message_id,),
            ).fetchone()
        assert second_row["parse_status"] == "pending"

        await asyncio.sleep(0.6)
        assert runtime.parse_queue.contains(second_message_id) is False
        release_first_parse.set()
        for _attempt in range(60):
            with connect_database(settings.database_path) as connection:
                status = connection.execute(
                    "SELECT parse_status FROM messages WHERE id = ?",
                    (second_message_id,),
                ).fetchone()["parse_status"]
            if status == "parsed":
                break
            await asyncio.sleep(0.05)
        assert status == "parsed"
    finally:
        release_first_parse.set()
        await runtime.stop()
