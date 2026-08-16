from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from app.db.connection import apply_pragmas, connect_database, initialize_database


NOW = "2026-08-16T12:00:00Z"


def _seed_mailbox_and_message(
    connection: sqlite3.Connection,
    *,
    message_id: str = "msg-live",
    parse_status: str = "pending",
) -> int:
    domain = connection.execute(
        "SELECT id FROM domains WHERE root_domain_ascii = 'live.example'"
    ).fetchone()
    if domain is None:
        domain_id = int(
            connection.execute(
                """
                INSERT INTO domains (root_domain_ascii, created_at, updated_at)
                VALUES ('live.example', ?, ?)
                """,
                (NOW, NOW),
            ).lastrowid
        )
    else:
        domain_id = int(domain["id"])
    mailbox = connection.execute(
        """
        INSERT INTO mailboxes (
            domain_id, local_part_canonical, rcpt_domain_ascii,
            address_canonical, address_display, first_seen_at, last_seen_at,
            latest_message_at, message_count
        ) VALUES (?, 'box', 'live.example', 'box@live.example',
                  'box@live.example', ?, ?, ?, 1)
        """,
        (domain_id, NOW, NOW, NOW),
    )
    connection.execute(
        """
        INSERT INTO messages (
            id, raw_path, raw_sha256, raw_size_bytes, subject, from_addr,
            received_at, indexed_at, parse_status
        ) VALUES (?, ?, ?, 1, 'Live subject', 'sender@example.com', ?, ?, ?)
        """,
        (
            message_id,
            f"raw/{message_id}.eml",
            f"sha256-{message_id}",
            NOW,
            NOW if parse_status != "pending" else None,
            parse_status,
        ),
    )
    return int(mailbox.lastrowid)


def test_mailbox_live_outbox_rolls_back_with_delivery_and_parse_transactions(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(connection)

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    apply_pragmas(connection, durable_writes=True)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES ('dlv-live', 'msg-live', ?, 'box@live.example', ?)
            """,
            (mailbox_id, NOW),
        )
        assert connection.execute(
            "SELECT event_type FROM mailbox_live_events"
        ).fetchone()["event_type"] == "mailbox_delivery"
        connection.rollback()

        assert connection.execute(
            "SELECT COUNT(*) AS count FROM message_deliveries"
        ).fetchone()["count"] == 0
        assert connection.execute(
            "SELECT COUNT(*) AS count FROM mailbox_live_events"
        ).fetchone()["count"] == 0

        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES ('dlv-live', 'msg-live', ?, 'box@live.example', ?)
            """,
            (mailbox_id, NOW),
        )
        connection.commit()

        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            UPDATE messages
            SET parse_status = 'parsed', indexed_at = ?
            WHERE id = 'msg-live'
            """,
            (NOW,),
        )
        assert [
            str(row["event_type"])
            for row in connection.execute(
                "SELECT event_type FROM mailbox_live_events ORDER BY id"
            ).fetchall()
        ] == ["mailbox_delivery", "mailbox_delivery_updated"]
        connection.rollback()

        assert connection.execute(
            "SELECT parse_status FROM messages WHERE id = 'msg-live'"
        ).fetchone()["parse_status"] == "pending"
        assert [
            str(row["event_type"])
            for row in connection.execute(
                "SELECT event_type FROM mailbox_live_events ORDER BY id"
            ).fetchall()
        ] == ["mailbox_delivery"]

        connection.execute(
            """
            UPDATE messages
            SET parse_status = 'parsed', indexed_at = ?
            WHERE id = 'msg-live'
            """,
            (NOW,),
        )
        connection.commit()
        first_update_id = int(
            connection.execute(
                """
                SELECT id
                FROM mailbox_live_events
                WHERE event_type = 'mailbox_delivery_updated'
                """
            ).fetchone()["id"]
        )
        connection.execute(
            """
            UPDATE messages
            SET indexed_at = '2026-08-16T12:00:01Z'
            WHERE id = 'msg-live'
            """
        )
        connection.commit()
        update_rows = connection.execute(
            """
            SELECT id
            FROM mailbox_live_events
            WHERE event_type = 'mailbox_delivery_updated'
            """
        ).fetchall()
        assert len(update_rows) == 1
        assert int(update_rows[0]["id"]) > first_update_id
    finally:
        connection.close()


def test_delivery_delete_trigger_supports_pre_outbox_clear_all_code(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(connection, parse_status="parsed")
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES ('dlv-live', 'msg-live', ?, 'box@live.example', ?)
            """,
            (mailbox_id, NOW),
        )
        assert connection.execute(
            "SELECT COUNT(*) AS count FROM mailbox_live_events"
        ).fetchone()["count"] == 1

    connection = sqlite3.connect(database_path)
    connection.row_factory = sqlite3.Row
    apply_pragmas(connection, durable_writes=True)
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN IMMEDIATE")
        # This is the relevant deletion order used before the outbox existed.
        connection.execute("DELETE FROM message_deliveries")
        connection.execute("DELETE FROM messages")
        connection.execute("DELETE FROM mailboxes")
        connection.commit()

        assert connection.execute(
            "SELECT COUNT(*) AS count FROM mailbox_live_events"
        ).fetchone()["count"] == 0
        assert connection.execute("PRAGMA foreign_key_check").fetchone() is None
    finally:
        connection.close()


def test_initialize_database_adds_live_outbox_to_existing_database(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute("DROP TRIGGER mailbox_live_events_after_delivery_delete")
        connection.execute("DROP TRIGGER mailbox_live_events_after_message_parse_update")
        connection.execute("DROP TRIGGER mailbox_live_events_after_delivery_insert")
        connection.execute("DROP TABLE mailbox_live_events")

    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        objects = {
            (str(row["type"]), str(row["name"]))
            for row in connection.execute(
                """
                SELECT type, name
                FROM sqlite_schema
                WHERE name IN (
                    'mailbox_live_events',
                    'idx_mailbox_live_events_delivery_id',
                    'mailbox_live_events_after_delivery_insert',
                    'mailbox_live_events_after_message_parse_update',
                    'mailbox_live_events_after_delivery_delete'
                )
                """
            ).fetchall()
        }
        assert objects == {
            ("table", "mailbox_live_events"),
            ("index", "idx_mailbox_live_events_delivery_id"),
            ("trigger", "mailbox_live_events_after_delivery_insert"),
            ("trigger", "mailbox_live_events_after_message_parse_update"),
            ("trigger", "mailbox_live_events_after_delivery_delete"),
        }

        mailbox_id = _seed_mailbox_and_message(
            connection,
            message_id="msg-upgraded",
            parse_status="parsed",
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'dlv-upgraded', 'msg-upgraded', ?, 'box@live.example', ?
            )
            """,
            (mailbox_id, NOW),
        )
        assert dict(
            connection.execute(
                """
                SELECT event_type, delivery_id
                FROM mailbox_live_events
                """
            ).fetchone()
        ) == {
            "event_type": "mailbox_delivery",
            "delivery_id": "dlv-upgraded",
        }


@pytest.mark.asyncio
async def test_runtime_tails_delivery_from_independent_sqlite_writer(runtime) -> None:
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(
            connection,
            message_id="msg-external",
            parse_status="parsed",
        )

    _, cursor = runtime.live_state.snapshot_state()
    last_seq = int(cursor.rsplit(":", 1)[1])
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'dlv-external', 'msg-external', ?, 'box@live.example', ?
            )
            """,
            (mailbox_id, NOW),
        )

    deadline = asyncio.get_running_loop().time() + 2.0
    matching_event = None
    while matching_event is None:
        matching_event = next(
            (
                event
                for event in runtime.live_state.snapshot_since(last_seq)
                if event.get("delivery_id") == "dlv-external"
            ),
            None,
        )
        if matching_event is not None:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("HTTP runtime did not tail the external delivery event")
        await asyncio.sleep(0.01)

    assert matching_event["type"] == "mailbox_delivery"
    assert matching_event["message_id"] == "msg-external"
    assert matching_event["mailbox"] == "box@live.example"
    assert matching_event["parse_status"] == "parsed"
    assert int(matching_event["outbox_id"]) > 0


@pytest.mark.asyncio
async def test_runtime_stop_cancels_mailbox_live_outbox_tailer(runtime) -> None:
    task = runtime._mailbox_live_event_task

    assert task is not None
    assert runtime.operational_state()["tasks"]["mailbox_live_events"] is True

    await asyncio.wait_for(runtime.stop(), timeout=2.0)

    assert task.done()
    assert runtime._mailbox_live_event_task is None
    assert runtime.operational_state()["tasks"]["mailbox_live_events"] is False
    assert runtime.read_pool.closed is True
    assert runtime.read_pool.active_connection_count == 0


@pytest.mark.asyncio
async def test_mailbox_live_tailer_drains_multiple_pages_in_order(runtime) -> None:
    await runtime._stop_mailbox_live_event_loop()
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        domain_id = int(
            connection.execute(
                """
                INSERT INTO domains (root_domain_ascii, created_at, updated_at)
                VALUES ('batch-live.example', ?, ?)
                """,
                (NOW, NOW),
            ).lastrowid
        )
        mailbox_id = int(
            connection.execute(
                """
                INSERT INTO mailboxes (
                    domain_id, local_part_canonical, rcpt_domain_ascii,
                    address_canonical, address_display, first_seen_at, last_seen_at,
                    latest_message_at, message_count
                ) VALUES (?, 'box', 'batch-live.example', 'box@batch-live.example',
                          'box@batch-live.example', ?, ?, ?, 600)
                """,
                (domain_id, NOW, NOW, NOW),
            ).lastrowid
        )
        connection.executemany(
            """
            INSERT INTO messages (
                id, raw_path, raw_sha256, raw_size_bytes, received_at,
                indexed_at, parse_status
            ) VALUES (?, ?, ?, 1, ?, ?, 'parsed')
            """,
            (
                (
                    f"msg-batch-{index:04d}",
                    f"raw/msg-batch-{index:04d}.eml",
                    f"sha256-batch-{index:04d}",
                    NOW,
                    NOW,
                )
                for index in range(600)
            ),
        )
        connection.executemany(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (?, ?, ?, 'box@batch-live.example', ?)
            """,
            (
                (
                    f"dlv-batch-{index:04d}",
                    f"msg-batch-{index:04d}",
                    mailbox_id,
                    NOW,
                )
                for index in range(600)
            ),
        )

    published = await runtime._publish_pending_mailbox_live_events()

    assert published == 600
    assert runtime._mailbox_live_event_cursor == 600
    retained_events = runtime.live_state.snapshot()
    assert len(retained_events) == 200
    assert [int(event["outbox_id"]) for event in retained_events] == list(
        range(401, 601)
    )


@pytest.mark.asyncio
async def test_delivery_retention_cascade_does_not_rewind_live_generation(runtime) -> None:
    await runtime._stop_mailbox_live_event_loop()
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(
            connection,
            message_id="msg-retained-generation",
            parse_status="parsed",
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'dlv-retained-generation', 'msg-retained-generation', ?,
                'box@live.example', ?
            )
            """,
            (mailbox_id, NOW),
        )

    assert await runtime._publish_pending_mailbox_live_events() == 1
    generation = runtime.live_state.generation
    assert runtime._mailbox_live_event_cursor == 1

    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        connection.execute(
            "DELETE FROM message_deliveries WHERE id = 'dlv-retained-generation'"
        )
        assert connection.execute(
            "SELECT COUNT(*) AS count FROM mailbox_live_events"
        ).fetchone()["count"] == 0

    assert await runtime._publish_pending_mailbox_live_events() == 0
    assert runtime._mailbox_live_event_cursor == 1
    assert runtime.live_state.generation == generation


@pytest.mark.asyncio
async def test_database_identity_change_resets_cursor_and_replays_replacement_outbox(
    runtime,
    monkeypatch,
) -> None:
    await runtime._stop_mailbox_live_event_loop()
    runtime._mailbox_live_event_cursor = 99
    runtime._mailbox_live_event_database_identity = (1, 1)
    previous_generation = runtime.live_state.generation

    monkeypatch.setattr(
        runtime,
        "_current_mailbox_live_event_database_identity",
        lambda: (2, 2),
    )

    async def replacement_page(after_id: int, limit: int):
        assert after_id == 0
        assert limit > 0
        return [
            {
                "outbox_high_water": 1,
                "id": 1,
                "event_type": "mailbox_delivery",
                "delivery_id": "dlv-replacement",
                "created_at": NOW,
                "delivered_at": NOW,
                "delivery_status": "active",
                "message_id": "msg-replacement",
                "parse_status": "parsed",
                "mailbox": "box@live.example",
            }
        ]

    monkeypatch.setattr(runtime, "_load_mailbox_live_event_page", replacement_page)

    assert await runtime._publish_pending_mailbox_live_events() == 1
    assert runtime._mailbox_live_event_cursor == 1
    assert runtime._mailbox_live_event_database_identity == (2, 2)
    assert runtime.live_state.generation != previous_generation
    assert runtime.live_state.snapshot()[-1]["delivery_id"] == "dlv-replacement"


@pytest.mark.asyncio
async def test_clear_all_cancels_loaded_outbox_page_before_new_generation(
    runtime,
    monkeypatch,
) -> None:
    await runtime._stop_mailbox_live_event_loop()
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(
            connection,
            message_id="msg-before-clear",
            parse_status="parsed",
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'dlv-before-clear', 'msg-before-clear', ?, 'box@live.example', ?
            )
            """,
            (mailbox_id, NOW),
        )
        first_outbox_id = int(
            connection.execute(
                "SELECT MAX(id) AS id FROM mailbox_live_events"
            ).fetchone()["id"]
        )

    page_loaded = asyncio.Event()
    release_loaded_page = asyncio.Event()
    original_load = runtime._load_mailbox_live_event_page

    async def blocked_load(after_id: int, limit: int):
        rows = await original_load(after_id, limit)
        page_loaded.set()
        await release_loaded_page.wait()
        return rows

    monkeypatch.setattr(runtime, "_load_mailbox_live_event_page", blocked_load)
    old_generation = runtime.live_state.generation
    runtime._mailbox_live_event_task = asyncio.create_task(
        runtime._publish_pending_mailbox_live_events()
    )
    await asyncio.wait_for(page_loaded.wait(), timeout=1.0)

    result = await asyncio.wait_for(runtime.clear_all_mail(), timeout=5.0)
    release_loaded_page.set()

    assert result["messages"] == 1
    assert runtime.live_state.generation != old_generation
    assert runtime.live_state.snapshot() == []
    assert runtime._mailbox_live_event_cursor == 0

    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        mailbox_id = _seed_mailbox_and_message(
            connection,
            message_id="msg-after-clear",
            parse_status="parsed",
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'dlv-after-clear', 'msg-after-clear', ?, 'box@live.example', ?
            )
            """,
            (mailbox_id, NOW),
        )

    deadline = asyncio.get_running_loop().time() + 2.0
    after_clear_event = None
    while after_clear_event is None:
        after_clear_event = next(
            (
                event
                for event in runtime.live_state.snapshot()
                if event.get("delivery_id") == "dlv-after-clear"
            ),
            None,
        )
        if after_clear_event is not None:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("tailer did not resume after clear-all")
        await asyncio.sleep(0.01)

    assert int(after_clear_event["outbox_id"]) > first_outbox_id
