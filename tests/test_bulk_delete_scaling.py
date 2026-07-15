from __future__ import annotations

import sqlite3

import pytest

from app.db.connection import connect_database


@pytest.mark.asyncio
async def test_bulk_delivery_delete_uses_bounded_sql_and_refreshes_all_mailboxes(runtime) -> None:
    domain = await runtime.create_domain("bulk-delete.example")
    message_id = "msg_bulk_delete_scaling"
    delivered_at = "2026-07-15T00:00:00Z"
    delivery_count = 1_101

    def seed(connection: sqlite3.Connection) -> list[str]:
        connection.execute(
            """
            INSERT INTO messages (
                id, raw_path, raw_sha256, raw_size_bytes, received_at, parse_status
            ) VALUES (?, ?, ?, 1, ?, 'parsed')
            """,
            (message_id, f"raw/{message_id}.eml", "0" * 64, delivered_at),
        )
        connection.executemany(
            """
            INSERT INTO mailboxes (
                domain_id, local_part_canonical, rcpt_domain_ascii,
                address_canonical, address_display, first_seen_at,
                last_seen_at, latest_message_at, message_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                (
                    int(domain["id"]),
                    f"box-{index}",
                    "bulk-delete.example",
                    f"box-{index}@bulk-delete.example",
                    f"box-{index}@bulk-delete.example",
                    delivered_at,
                    delivered_at,
                    delivered_at,
                )
                for index in range(delivery_count)
            ),
        )
        mailbox_rows = connection.execute(
            """
            SELECT id
            FROM mailboxes
            WHERE domain_id = ?
            ORDER BY id ASC
            """,
            (int(domain["id"]),),
        ).fetchall()
        delivery_ids = [f"delivery-bulk-{index}" for index in range(delivery_count)]
        connection.executemany(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                (
                    delivery_ids[index],
                    message_id,
                    int(mailbox_rows[index]["id"]),
                    f"box-{index}@bulk-delete.example",
                    delivered_at,
                )
                for index in range(delivery_count)
            ),
        )
        return delivery_ids

    delivery_ids = await runtime.writer.execute(seed)
    result = await runtime.messages.soft_delete_deliveries(delivery_ids)

    assert result["deleted"] == delivery_count
    assert set(result["delivery_ids"]) == set(delivery_ids)
    with connect_database(runtime.settings.database_path) as connection:
        active = connection.execute(
            "SELECT COUNT(*) AS count FROM message_deliveries WHERE status = 'active'"
        ).fetchone()
        stale_summaries = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM mailboxes
            WHERE domain_id = ?
              AND (message_count != 0 OR latest_message_at IS NOT NULL)
            """,
            (int(domain["id"]),),
        ).fetchone()

    assert int(active["count"]) == 0
    assert int(stale_summaries["count"]) == 0
