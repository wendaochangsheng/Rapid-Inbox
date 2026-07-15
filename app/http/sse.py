from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from app.db.connection import connect_database


LIVE_SSE_EVENT_TYPES: tuple[str, ...] = (
    "connect",
    "rcpt_accepted",
    "rcpt_rejected",
    "queued",
    "disconnect",
    "error",
    "gap",
)


def encode_sse(event: dict[str, object], *, event_id: str | None = None) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event['type']}")
    lines.append(f"data: {json.dumps(event, ensure_ascii=False)}")
    return "\n".join(lines) + "\n\n"


def smtp_live_snapshot(runtime, *, history_limit: int = 25) -> list[dict[str, Any]]:
    events, _ = runtime.live_state.snapshot_state()
    if events:
        return events[-history_limit:]
    return _recent_message_events(runtime, limit=history_limit)


def _parse_live_cursor(cursor: str | None) -> tuple[str, int] | None:
    if cursor is None:
        return None
    try:
        generation, seq_text = cursor.rsplit(":", 1)
        if not generation:
            return None
        seq = int(seq_text)
    except (AttributeError, ValueError):
        return None
    return generation, seq


async def stream_smtp_live_events(
    runtime,
    *,
    poll_interval: float = 0.25,
    history_limit: int = 25,
    after_cursor: str | None = None,
    last_event_id: str | None = None,
) -> AsyncIterator[str]:
    live_state = runtime.live_state
    resume_cursor = last_event_id if last_event_id is not None else after_cursor
    parsed_cursor = _parse_live_cursor(resume_cursor)
    stream_generation = live_state.generation
    generation_matches = parsed_cursor is not None and parsed_cursor[0] == stream_generation

    if generation_matches:
        last_seq = max(parsed_cursor[1], 0)
        replay_initial = False
    else:
        replay_initial = True
        last_seq = 0

    try:
        if replay_initial:
            events, cursor = live_state.snapshot_state()
            parsed_snapshot_cursor = _parse_live_cursor(cursor)
            if parsed_snapshot_cursor is not None:
                stream_generation = parsed_snapshot_cursor[0]
            smtp_events = [event for event in events if str(event.get("type") or "") in LIVE_SSE_EVENT_TYPES]
            if smtp_events:
                for event in smtp_events:
                    seq = int(event.get("seq", 0))
                    yield encode_sse(event, event_id=f"{stream_generation}:{seq}")
                last_seq = parsed_snapshot_cursor[1] if parsed_snapshot_cursor is not None else 0
            else:
                history_events = await asyncio.to_thread(
                    _recent_message_events,
                    runtime,
                    limit=history_limit,
                )
                history_count = len(history_events)
                for index, event in enumerate(history_events):
                    history_seq = -(history_count - index)
                    yield encode_sse(event, event_id=f"{stream_generation}:{history_seq}")
                last_seq = 0

        while True:
            (
                current_generation,
                raw_events,
                gap_reason,
                oldest_seq,
                latest_seq,
            ) = live_state.snapshot_stream_window(stream_generation, last_seq)
            if gap_reason is not None:
                previous_generation = stream_generation
                previous_seq = last_seq
                stream_generation = current_generation
                last_seq = max(oldest_seq - 1, 0)
                yield encode_sse(
                    {
                        "type": "gap",
                        "reason": gap_reason,
                        "previous_generation": previous_generation,
                        "previous_seq": previous_seq,
                        "oldest_available_seq": oldest_seq,
                        "latest_available_seq": latest_seq,
                    },
                    event_id=f"{stream_generation}:{last_seq}",
                )
            if raw_events:
                last_seq = int(raw_events[-1].get("seq", last_seq))
                for event in raw_events:
                    if str(event.get("type") or "") not in LIVE_SSE_EVENT_TYPES:
                        continue
                    yield encode_sse(event, event_id=f"{stream_generation}:{event['seq']}")
                continue
            await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        return


def count_smtp_sessions(runtime) -> int:
    with connect_database(runtime.settings.database_path) as connection:
        row = connection.execute("SELECT COUNT(*) AS count FROM smtp_sessions").fetchone()
    return 0 if row is None else int(row["count"])


def smtp_sessions_page(
    runtime,
    *,
    limit: int = 25,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    """Fetch an SMTP session page and its count in one worker-side DB pass."""
    with connect_database(runtime.settings.database_path) as connection:
        rows = _recent_smtp_session_rows(connection, limit=limit, offset=offset)
        count_row = connection.execute("SELECT COUNT(*) AS count FROM smtp_sessions").fetchone()
    return _normalize_smtp_sessions(rows), 0 if count_row is None else int(count_row["count"])


def recent_smtp_sessions(runtime, *, limit: int = 25, offset: int = 0) -> list[dict[str, Any]]:
    with connect_database(runtime.settings.database_path) as connection:
        rows = _recent_smtp_session_rows(connection, limit=limit, offset=offset)

    return _normalize_smtp_sessions(rows)


def _recent_smtp_session_rows(connection, *, limit: int, offset: int):
    return connection.execute(
        """
        SELECT
            id,
            remote_ip,
            remote_port,
            helo_name,
            status,
            tls_used,
            connect_at,
            disconnect_at,
            first_command_at,
            last_command_at,
            message_count,
            rcpt_accepted_count,
            rcpt_rejected_count,
            bytes_received,
            last_mail_from,
            last_rcpt_to_sample,
            result_code,
            result_message,
            close_reason
        FROM smtp_sessions
        ORDER BY connect_at DESC, id DESC
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    ).fetchall()


def _normalize_smtp_sessions(rows) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for row in rows:
        session = dict(row)
        session["tls_used"] = bool(session["tls_used"])
        sessions.append(session)
    return sessions


def _recent_message_events(runtime, *, limit: int = 25) -> list[dict[str, Any]]:
    with connect_database(runtime.settings.database_path) as connection:
        rows = connection.execute(
            """
            SELECT
                d.id AS delivery_id,
                d.rcpt_to,
                d.delivered_at,
                m.id AS message_id,
                m.smtp_session_id,
                m.envelope_from
            FROM message_deliveries AS d
            JOIN messages AS m ON m.id = d.message_id
            ORDER BY d.delivered_at DESC, d.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    events: list[dict[str, Any]] = []
    seen_messages: set[str] = set()
    for row in rows:
        session_id = row["smtp_session_id"] or row["message_id"]
        events.append(
            {
                "type": "rcpt_accepted",
                "session_id": session_id,
                "delivery_id": row["delivery_id"],
                "message_id": row["message_id"],
                "rcpt_to": row["rcpt_to"],
                "mail_from": row["envelope_from"],
                "ts": row["delivered_at"],
                "source": "history",
            }
        )
        if row["message_id"] in seen_messages:
            continue
        seen_messages.add(row["message_id"])
        events.append(
            {
                "type": "queued",
                "session_id": session_id,
                "message_id": row["message_id"],
                "mail_from": row["envelope_from"],
                "ts": row["delivered_at"],
                "source": "history",
            }
        )
    return events
