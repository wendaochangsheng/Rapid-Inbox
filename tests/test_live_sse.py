from __future__ import annotations

import asyncio
from contextlib import suppress
import threading
import time

import pytest

from app.http.sse import stream_smtp_live_events
from app.smtp.live_state import LiveState

class GuardedEvents:
    def __init__(self) -> None:
        self.items = [{"type": "seed", "seq": 1}]
        self.iterating = threading.Event()
        self.concurrent_append = False

    def append(self, item: dict[str, object]) -> None:
        if self.iterating.is_set():
            self.concurrent_append = True
        self.items.append(item)

    def __iter__(self):
        self.iterating.set()
        try:
            for item in list(self.items):
                time.sleep(0.01)
                yield item
        finally:
            self.iterating.clear()


def test_live_state_snapshot_and_publish_are_thread_safe() -> None:
    state = LiveState()
    guarded = GuardedEvents()
    state._events = guarded  # type: ignore[attr-defined]

    snapshot_result: list[dict[str, object]] = []
    snapshot_error: list[BaseException] = []
    publish_error: list[BaseException] = []

    def run_snapshot() -> None:
        try:
            snapshot_result.extend(state.snapshot())
        except BaseException as exc:  # noqa: BLE001
            snapshot_error.append(exc)

    def run_publish() -> None:
        try:
            asyncio.run(state.publish({"type": "published"}))
        except BaseException as exc:  # noqa: BLE001
            publish_error.append(exc)

    snapshot_thread = threading.Thread(target=run_snapshot)
    snapshot_thread.start()
    assert guarded.iterating.wait(timeout=1)

    publish_thread = threading.Thread(target=run_publish)
    publish_thread.start()
    snapshot_thread.join(timeout=1)
    publish_thread.join(timeout=1)

    assert not snapshot_thread.is_alive()
    assert not publish_thread.is_alive()
    assert snapshot_error == []
    assert publish_error == []
    assert guarded.concurrent_append is False
    assert snapshot_result[0]["type"] == "seed"
    assert snapshot_result[0]["seq"] == 1


@pytest.mark.asyncio
async def test_live_sse_stream_emits_recent_rcpt_event(runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_live_1",
    )

    stream = stream_smtp_live_events(runtime, poll_interval=0.01)
    try:
        event = await asyncio.wait_for(anext(stream), timeout=0.05)
    finally:
        await stream.aclose()

    assert "rcpt_accepted" in event or "queued" in event
    assert "id:" in event


@pytest.mark.asyncio
async def test_live_sse_stream_skips_initial_history_after_cursor(runtime, sample_email_bytes: bytes) -> None:
    await runtime.create_domain("adb.com")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_live_1",
    )

    _, cursor = runtime.live_state.snapshot_state()
    stream = stream_smtp_live_events(runtime, after_cursor=cursor, poll_interval=0.01)
    pending_event: asyncio.Task[str] | None = None
    try:
        pending_event = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.05)
        assert not pending_event.done()

        await runtime.live_state.publish(
            {"type": "queued", "session_id": "smtp_live_2", "ts": "2026-04-18T20:00:00Z"}
        )
        follow_up = await asyncio.wait_for(pending_event, timeout=0.2)
    finally:
        if pending_event is not None and not pending_event.done():
            pending_event.cancel()
            with suppress(asyncio.CancelledError):
                await pending_event
        await stream.aclose()

    assert '"session_id": "smtp_live_2"' in follow_up
    assert '"type": "queued"' in follow_up
    assert "id:" in follow_up


@pytest.mark.asyncio
async def test_live_sse_stream_falls_back_to_initial_history_for_stale_generation(
    runtime, sample_email_bytes: bytes
) -> None:
    await runtime.create_domain("adb.com")
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_live_1",
    )

    stream = stream_smtp_live_events(runtime, after_cursor="deadbeef:99", poll_interval=0.01)
    try:
        event = await asyncio.wait_for(anext(stream), timeout=0.05)
    finally:
        await stream.aclose()

    assert "rcpt_accepted" in event or "queued" in event
    assert "id:" in event


@pytest.mark.asyncio
async def test_live_sse_stream_prefers_last_event_id_over_after_cursor(runtime) -> None:
    await runtime.create_domain("adb.com")
    await runtime.live_state.publish(
        {"type": "queued", "session_id": "smtp_live_1", "ts": "2026-04-18T20:00:00Z"}
    )

    generation = runtime.live_state.generation
    stream = stream_smtp_live_events(
        runtime,
        after_cursor=f"{generation}:0",
        last_event_id=f"{generation}:1",
        poll_interval=0.01,
    )
    pending_event: asyncio.Task[str] | None = None
    try:
        pending_event = asyncio.create_task(anext(stream))
        await asyncio.sleep(0.05)
        assert not pending_event.done()

        await runtime.live_state.publish(
            {"type": "queued", "session_id": "smtp_live_2", "ts": "2026-04-18T20:00:01Z"}
        )
        follow_up = await asyncio.wait_for(pending_event, timeout=0.2)
    finally:
        if pending_event is not None and not pending_event.done():
            pending_event.cancel()
            with suppress(asyncio.CancelledError):
                await pending_event
        await stream.aclose()

    assert '"session_id": "smtp_live_2"' in follow_up
    assert '"session_id": "smtp_live_1"' not in follow_up
    assert "id:" in follow_up


@pytest.mark.asyncio
async def test_live_sse_reports_ring_overrun_before_replaying_available_events(runtime) -> None:
    runtime.live_state = LiveState(max_events=2)
    generation = runtime.live_state.generation
    for index in range(3):
        await runtime.live_state.publish(
            {
                "type": "queued",
                "session_id": f"smtp_ring_{index}",
                "ts": "2026-04-18T20:00:00Z",
            }
        )

    stream = stream_smtp_live_events(
        runtime,
        after_cursor=f"{generation}:0",
        poll_interval=0.01,
    )
    try:
        gap = await asyncio.wait_for(anext(stream), timeout=0.1)
        first_available = await asyncio.wait_for(anext(stream), timeout=0.1)
    finally:
        await stream.aclose()

    assert "event: gap" in gap
    assert '"reason": "ring_overrun"' in gap
    assert '"oldest_available_seq": 2' in gap
    assert '"session_id": "smtp_ring_1"' in first_available


@pytest.mark.asyncio
async def test_live_sse_reports_generation_change_and_resumes_after_clear(runtime) -> None:
    runtime.live_state = LiveState(max_events=2)
    await runtime.live_state.publish(
        {"type": "queued", "session_id": "before-clear", "ts": "2026-04-18T20:00:00Z"}
    )
    _, cursor = runtime.live_state.snapshot_state()
    stream = stream_smtp_live_events(runtime, after_cursor=cursor, poll_interval=0.01)
    pending_gap = asyncio.create_task(anext(stream))
    try:
        await asyncio.sleep(0.02)
        runtime.live_state.clear()
        await runtime.live_state.publish(
            {"type": "queued", "session_id": "after-clear", "ts": "2026-04-18T20:00:01Z"}
        )
        gap = await asyncio.wait_for(pending_gap, timeout=0.2)
        resumed = await asyncio.wait_for(anext(stream), timeout=0.1)
    finally:
        if not pending_gap.done():
            pending_gap.cancel()
            with suppress(asyncio.CancelledError):
                await pending_gap
        await stream.aclose()

    assert "event: gap" in gap
    assert '"reason": "generation_changed"' in gap
    assert '"session_id": "after-clear"' in resumed
