"""Deprecated compatibility wrapper for the former admin SSE endpoint."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator

from app.http.live import LIVE_SMTP_EVENT_TYPES, iter_smtp_live_events


LIVE_SSE_EVENT_TYPES = LIVE_SMTP_EVENT_TYPES


def encode_sse(event: dict[str, object], *, event_id: str | None = None) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event['type']}")
    lines.append(f"data: {json.dumps(event, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


async def stream_smtp_live_events(
    runtime,
    *,
    poll_interval: float = 0.25,
    history_limit: int = 25,
    after_cursor: str | None = None,
    last_event_id: str | None = None,
) -> AsyncIterator[str]:
    resume_cursor = last_event_id if last_event_id is not None else after_cursor
    async for event in iter_smtp_live_events(
        runtime,
        poll_interval=poll_interval,
        history_limit=history_limit,
        after_cursor=resume_cursor,
    ):
        cursor = str(event.get("cursor") or "")
        yield encode_sse(event, event_id=cursor or None)
