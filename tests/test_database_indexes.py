from __future__ import annotations

from app.db.connection import connect_database, initialize_database


def _plan(connection, query: str, params: tuple[object, ...] = ()) -> str:
    rows = connection.execute(f"EXPLAIN QUERY PLAN {query}", params).fetchall()
    return "\n".join(str(row["detail"]) for row in rows)


def test_hot_path_queries_use_covering_or_lookup_indexes(tmp_path) -> None:
    database_path = tmp_path / "app.db"
    initialize_database(database_path)

    with connect_database(database_path) as connection:
        text_plan = _plan(
            connection,
            "SELECT text_body_path FROM messages WHERE text_body_path IN (?, ?)",
            ("text/a.txt", "text/b.txt"),
        )
        html_plan = _plan(
            connection,
            "SELECT html_body_path FROM messages WHERE html_body_path IN (?, ?)",
            ("html/a.html", "html/b.html"),
        )
        attachment_plan = _plan(
            connection,
            "SELECT storage_path FROM attachments WHERE storage_path IN (?, ?)",
            ("attachments/a", "attachments/b"),
        )
        expiry_plan = _plan(
            connection,
            """
            SELECT id
            FROM message_deliveries
            WHERE expires_at IS NOT NULL AND expires_at <= ?
            ORDER BY expires_at ASC, id ASC
            LIMIT ?
            """,
            ("2030-01-01T00:00:00Z", 500),
        )
        mailbox_plan = _plan(
            connection,
            """
            SELECT id
            FROM mailboxes
            ORDER BY COALESCE(latest_message_at, '') DESC, id DESC
            LIMIT ?
            """,
            (100,),
        )
        smtp_retention_plan = _plan(
            connection,
            """
            SELECT id
            FROM smtp_sessions
            WHERE COALESCE(disconnect_at, last_command_at, connect_at) <= ?
            ORDER BY COALESCE(disconnect_at, last_command_at, connect_at) ASC, id ASC
            LIMIT ?
            """,
            ("2030-01-01T00:00:00Z", 500),
        )
        audit_retention_plan = _plan(
            connection,
            """
            SELECT id
            FROM audit_logs
            WHERE created_at < ?
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            ("2030-01-01T00:00:00Z", 500),
        )
        gc_pending_plan = _plan(
            connection,
            """
            SELECT id, storage_path, attempts
            FROM file_gc_tasks
            WHERE next_attempt_at IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (500,),
        )
        gc_retry_plan = _plan(
            connection,
            """
            SELECT id, storage_path, attempts
            FROM file_gc_tasks
            WHERE next_attempt_at IS NOT NULL AND next_attempt_at <= ?
            ORDER BY next_attempt_at ASC, id ASC
            LIMIT ?
            """,
            ("2030-01-01T00:00:00Z", 500),
        )
        mailbox_bulk_delete_plan = _plan(
            connection,
            """
            SELECT rowid
            FROM message_deliveries
            WHERE mailbox_id = ?
              AND status = 'active'
              AND mailbox_generation = ?
              AND rowid > ?
              AND rowid <= ?
            ORDER BY rowid ASC
            LIMIT ?
            """,
            (1, 0, 0, 1000, 100),
        )
        api_key_page_plan = _plan(
            connection,
            """
            SELECT id, created_at
            FROM api_keys
            WHERE created_at < ? OR (created_at = ? AND id < ?)
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            ("2030-01-01T00:00:00Z", "2030-01-01T00:00:00Z", 1000, 100),
        )
        legacy_bulk_index = connection.execute(
            """
            SELECT 1
            FROM sqlite_master
            WHERE type = 'index'
              AND name = 'idx_message_deliveries_active_mailbox_rowid'
            """
        ).fetchone()

    assert "idx_messages_text_body_path" in text_plan
    assert "idx_messages_html_body_path" in html_plan
    assert "idx_attachments_storage_path" in attachment_plan
    assert "idx_message_deliveries_expiry_scan" in expiry_plan
    assert "idx_mailboxes_latest_sort" in mailbox_plan
    assert "idx_smtp_sessions_retention_time" in smtp_retention_plan
    assert "idx_audit_logs_created_id" in audit_retention_plan
    assert "idx_file_gc_tasks_next_attempt" in gc_pending_plan
    assert "idx_file_gc_tasks_next_attempt" in gc_retry_plan
    assert "idx_message_deliveries_active_mailbox_generation_rowid" in mailbox_bulk_delete_plan
    assert "idx_api_keys_created_id" in api_key_page_plan
    assert legacy_bulk_index is None
    assert "USE TEMP B-TREE" not in expiry_plan
    assert "USE TEMP B-TREE" not in mailbox_plan
    assert "USE TEMP B-TREE" not in smtp_retention_plan
    assert "USE TEMP B-TREE" not in audit_retention_plan
    assert "USE TEMP B-TREE" not in gc_pending_plan
    assert "USE TEMP B-TREE" not in gc_retry_plan
    assert "USE TEMP B-TREE" not in mailbox_bulk_delete_plan
    assert "USE TEMP B-TREE" not in api_key_page_plan
