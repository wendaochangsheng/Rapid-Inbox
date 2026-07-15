from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.http.sse import stream_smtp_live_events


class _QuietLiveState:
    generation = "test-generation"

    def snapshot_state(self):
        return [], "test-generation:0"

    def snapshot_stream_window(self, _generation: str, _last_seq: int):
        return self.generation, [], None, 1, 0


@pytest.mark.asyncio
async def test_smtp_live_stream_stops_quietly_when_cancelled() -> None:
    runtime = SimpleNamespace(live_state=_QuietLiveState())

    async def consume_stream() -> None:
        async for _event in stream_smtp_live_events(
            runtime,
            poll_interval=60,
            after_cursor="test-generation:0",
        ):
            pass

    task = asyncio.create_task(consume_stream())
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.wait_for(task, timeout=2)
