from __future__ import annotations

import asyncio
import threading

import pytest

from app.config import Settings
from app.db.connection import connect_database, initialize_database
from app.runtime import RapidInboxRuntime
from app.services.mailboxes import MAILBOX_BULK_DELETE_BATCH_SIZE


def _seed_mailbox_deliveries(
    runtime: RapidInboxRuntime,
    *,
    count: int,
    domain: str,
) -> int:
    with connect_database(runtime.settings.database_path, durable_writes=True) as connection:
        connection.execute(
            """
            INSERT INTO domains (root_domain_ascii, created_at, updated_at)
            VALUES (?, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """,
            (domain,),
        )
        domain_id = int(
            connection.execute(
                "SELECT id FROM domains WHERE root_domain_ascii = ?",
                (domain,),
            ).fetchone()["id"]
        )
        address = f"bulk@{domain}"
        connection.execute(
            """
            INSERT INTO mailboxes (
                domain_id, local_part_canonical, rcpt_domain_ascii,
                address_canonical, address_display, first_seen_at, last_seen_at,
                latest_message_at, message_count
            ) VALUES (
                ?, 'bulk', ?, ?, ?,
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z',
                '2026-01-01T00:00:00Z', ?
            )
            """,
            (domain_id, domain, address, address, count),
        )
        mailbox_id = int(
            connection.execute(
                "SELECT id FROM mailboxes WHERE address_canonical = ?",
                (address,),
            ).fetchone()["id"]
        )
        connection.execute(
            """
            WITH RECURSIVE sequence(value) AS (
                VALUES (1)
                UNION ALL
                SELECT value + 1 FROM sequence WHERE value < ?
            )
            INSERT INTO messages (
                id, raw_path, raw_sha256, raw_size_bytes, received_at
            )
            SELECT
                printf('bulk-message-%06d', value),
                printf('raw/bulk-message-%06d.eml', value),
                printf('bulk-sha-%06d', value),
                1,
                '2026-01-01T00:00:00Z'
            FROM sequence
            """,
            (count,),
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            )
            SELECT
                printf('bulk-delivery-%06d', rowid),
                id,
                ?,
                ?,
                '2026-01-01T00:00:00Z'
            FROM messages
            WHERE id GLOB 'bulk-message-*'
            ORDER BY id ASC
            """,
            (mailbox_id, address),
        )
    return mailbox_id


def test_legacy_incomplete_bulk_delete_job_migrates_generation_boundary(tmp_path) -> None:
    database_path = tmp_path / "legacy.db"
    initialize_database(database_path)
    with connect_database(database_path, durable_writes=True) as connection:
        connection.execute(
            """
            INSERT INTO domains (root_domain_ascii, created_at, updated_at)
            VALUES ('legacy-bulk.example', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
            """
        )
        domain_id = int(connection.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        connection.execute(
            """
            INSERT INTO mailboxes (
                domain_id, local_part_canonical, rcpt_domain_ascii,
                address_canonical, address_display, first_seen_at, last_seen_at,
                latest_message_at, message_count
            ) VALUES (
                ?, 'box', 'legacy-bulk.example', 'box@legacy-bulk.example',
                'box@legacy-bulk.example', '2026-01-01T00:00:00Z',
                '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 1
            )
            """,
            (domain_id,),
        )
        mailbox_id = int(connection.execute("SELECT last_insert_rowid() AS id").fetchone()["id"])
        connection.execute(
            """
            INSERT INTO messages (id, raw_path, raw_sha256, raw_size_bytes, received_at)
            VALUES ('legacy-message', 'raw/legacy-message.eml', 'legacy-sha', 1,
                    '2026-01-01T00:00:00Z')
            """
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'legacy-delivery', 'legacy-message', ?, 'box@legacy-bulk.example',
                '2026-01-01T00:00:00Z'
            )
            """,
            (mailbox_id,),
        )
        connection.execute(
            """
            INSERT INTO mailbox_bulk_delete_jobs (
                id, mailbox_id, status, cursor_delivery_rowid, max_delivery_rowid,
                deleted_count, deleted_at, created_at, updated_at
            ) VALUES (
                'legacy-job', ?, 'running', 0, 1, 0,
                '2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z',
                '2026-01-02T00:00:00Z'
            )
            """,
            (mailbox_id,),
        )
        connection.execute("DROP TRIGGER message_deliveries_fill_mailbox_generation")
        connection.execute(
            "DROP INDEX idx_message_deliveries_active_mailbox_generation_rowid"
        )
        connection.execute(
            "ALTER TABLE mailbox_bulk_delete_jobs DROP COLUMN target_generation"
        )
        connection.execute(
            "ALTER TABLE message_deliveries DROP COLUMN mailbox_generation"
        )
        connection.execute(
            "ALTER TABLE mailboxes DROP COLUMN bulk_delete_generation"
        )

    initialize_database(database_path)
    with connect_database(database_path) as connection:
        mailbox = connection.execute(
            "SELECT bulk_delete_generation FROM mailboxes WHERE id = ?",
            (mailbox_id,),
        ).fetchone()
        delivery = connection.execute(
            """
            SELECT mailbox_generation
            FROM message_deliveries
            WHERE id = 'legacy-delivery'
            """
        ).fetchone()
        job = connection.execute(
            """
            SELECT target_generation
            FROM mailbox_bulk_delete_jobs
            WHERE id = 'legacy-job'
            """
        ).fetchone()
        indexes = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ).fetchall()
        }
        trigger = connection.execute(
            """
            SELECT 1 FROM sqlite_master
            WHERE type = 'trigger'
              AND name = 'message_deliveries_fill_mailbox_generation'
            """
        ).fetchone()
        connection.execute(
            """
            INSERT INTO messages (id, raw_path, raw_sha256, raw_size_bytes, received_at)
            VALUES ('post-migration-message', 'raw/post-migration.eml',
                    'post-migration-sha', 1, '2026-01-03T00:00:00Z')
            """
        )
        connection.execute(
            """
            INSERT INTO message_deliveries (
                id, message_id, mailbox_id, rcpt_to, delivered_at
            ) VALUES (
                'post-migration-delivery', 'post-migration-message', ?,
                'box@legacy-bulk.example', '2026-01-03T00:00:00Z'
            )
            """,
            (mailbox_id,),
        )
        post_migration = connection.execute(
            """
            SELECT mailbox_generation
            FROM message_deliveries
            WHERE id = 'post-migration-delivery'
            """
        ).fetchone()

    assert int(mailbox["bulk_delete_generation"]) == 1
    assert int(delivery["mailbox_generation"]) == 0
    assert int(job["target_generation"]) == 0
    assert "idx_message_deliveries_active_mailbox_generation_rowid" in indexes
    assert "idx_message_deliveries_active_mailbox_rowid" not in indexes
    assert trigger is not None
    assert int(post_migration["mailbox_generation"]) == 1


@pytest.mark.asyncio
async def test_mailbox_bulk_delete_100k_is_bounded_interleavable_and_keeps_new_delivery(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    release_second_batch = asyncio.Event()
    first_batch_committed = asyncio.Event()
    delete_task: asyncio.Task[dict] | None = None
    try:
        mailbox_id = _seed_mailbox_deliveries(
            runtime,
            count=100_000,
            domain="bulk-delete.example",
        )
        authorization_checks = 0
        real_authorize = runtime.api_keys.transaction_authorization_principal

        def observe_authorization(connection, principal, **kwargs):
            nonlocal authorization_checks
            assert connection.in_transaction
            authorization_checks += 1
            return real_authorize(connection, principal, **kwargs)

        monkeypatch.setattr(
            runtime.api_keys,
            "transaction_authorization_principal",
            observe_authorization,
        )
        batches: list[int] = []
        original_process = runtime.mailboxes._process_bulk_delete_job_batch

        def observe_batch(connection, job_id, *, batch_size=MAILBOX_BULK_DELETE_BATCH_SIZE):
            result = original_process(connection, job_id, batch_size=batch_size)
            batches.append(int(result["batch_deleted"]))
            return result

        refresh_count = 0
        original_refresh = runtime._refresh_mailbox_summary_after_message_delete

        def observe_refresh(connection, refreshed_mailbox_id):
            nonlocal refresh_count
            refresh_count += 1
            return original_refresh(connection, refreshed_mailbox_id)

        yield_count = 0

        async def pause_after_first_batch() -> None:
            nonlocal yield_count
            yield_count += 1
            if yield_count == 1:
                first_batch_committed.set()
                await release_second_batch.wait()
            else:
                await asyncio.sleep(0)

        monkeypatch.setattr(runtime.mailboxes, "_process_bulk_delete_job_batch", observe_batch)
        monkeypatch.setattr(runtime, "_refresh_mailbox_summary_after_message_delete", observe_refresh)
        monkeypatch.setattr(
            runtime.mailboxes,
            "_yield_between_bulk_delete_batches",
            pause_after_first_batch,
        )

        delete_task = asyncio.create_task(
            runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)
        )
        await asyncio.wait_for(first_batch_committed.wait(), timeout=10)
        assert batches == [MAILBOX_BULK_DELETE_BATCH_SIZE]

        marker_batch_counts: list[int] = []

        def interleaved_write(connection) -> None:
            marker_batch_counts.append(len(batches))
            connection.execute(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at
                ) VALUES (
                    'bulk-new-message', 'raw/bulk-new-message.eml',
                    'bulk-new-sha', 1, '2026-01-02T00:00:00Z'
                )
                """
            )
            connection.execute(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at
                ) VALUES (
                    'bulk-new-delivery', 'bulk-new-message', ?,
                    'bulk@bulk-delete.example', '2026-01-02T00:00:00Z'
                )
                """,
                (mailbox_id,),
            )
            connection.execute(
                """
                UPDATE mailboxes
                SET message_count = message_count + 1,
                    last_seen_at = '2026-01-02T00:00:00Z',
                    latest_message_at = '2026-01-02T00:00:00Z'
                WHERE id = ?
                """,
                (mailbox_id,),
            )
            connection.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES (
                    '_test_mailbox_bulk_delete_interleave', '1',
                    '2026-01-02T00:00:00Z'
                )
                """
            )

        await asyncio.wait_for(runtime.writer.execute(interleaved_write), timeout=5)
        assert marker_batch_counts == [1]
        assert not delete_task.done()

        release_second_batch.set()
        result = await asyncio.wait_for(delete_task, timeout=60)
        assert result == {
            "deleted": 100_000,
            "delivery_ids": [],
            "delivery_ids_truncated": True,
        }
        assert len(batches) == 100
        assert sum(batches) == 100_000
        assert all(count == MAILBOX_BULK_DELETE_BATCH_SIZE for count in batches)
        assert refresh_count == 1
        # Authorization belongs to the durable job-creation boundary. The 100
        # short page transactions and their recovery path never re-read the
        # original request credential.
        assert authorization_checks == 1

        with connect_database(settings.database_path) as connection:
            old_statuses = connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM message_deliveries
                WHERE id != 'bulk-new-delivery'
                GROUP BY status
                """
            ).fetchall()
            new_delivery = connection.execute(
                """
                SELECT rowid, status, mailbox_generation FROM message_deliveries
                WHERE id = 'bulk-new-delivery'
                """
            ).fetchone()
            mailbox = connection.execute(
                """
                SELECT message_count, latest_message_at, bulk_delete_generation
                FROM mailboxes WHERE id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            job = connection.execute(
                """
                SELECT
                    status, cursor_delivery_rowid, max_delivery_rowid,
                    target_generation, deleted_count, error
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
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
                (
                    mailbox_id,
                    int(job["target_generation"]),
                    0,
                    int(job["max_delivery_rowid"]),
                    MAILBOX_BULK_DELETE_BATCH_SIZE,
                ),
            ).fetchall()

        assert [dict(row) for row in old_statuses] == [
            {"status": "deleted", "count": 100_000}
        ]
        assert dict(mailbox) == {
            "message_count": 1,
            "latest_message_at": "2026-01-02T00:00:00Z",
            "bulk_delete_generation": 1,
        }
        assert dict(job) == {
            "status": "succeeded",
            "cursor_delivery_rowid": 100_000,
            "max_delivery_rowid": 100_000,
            "target_generation": 0,
            "deleted_count": 100_000,
            "error": None,
        }
        assert int(new_delivery["rowid"]) > int(job["max_delivery_rowid"])
        assert new_delivery["status"] == "active"
        assert int(new_delivery["mailbox_generation"]) == 1
        plan_details = [str(row["detail"]) for row in plan]
        assert any(
            "idx_message_deliveries_active_mailbox_generation_rowid" in detail
            and "mailbox_id=? AND mailbox_generation=? AND rowid>? AND rowid<?" in detail
            for detail in plan_details
        )
        assert not any("SCAN message_deliveries" in detail for detail in plan_details)
        assert not any("TEMP B-TREE" in detail for detail in plan_details)

        second = await runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)
        assert second == {
            "deleted": 1,
            "delivery_ids": [],
            "delivery_ids_truncated": True,
        }
        assert refresh_count == 2
        assert authorization_checks == 2
        with connect_database(settings.database_path) as connection:
            retained_jobs = connection.execute(
                """
                SELECT status, deleted_count
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchall()
            mailbox = connection.execute(
                "SELECT message_count, latest_message_at FROM mailboxes WHERE id = ?",
                (mailbox_id,),
            ).fetchone()
        assert [dict(row) for row in retained_jobs] == [
            {"status": "succeeded", "deleted_count": 1}
        ]
        assert dict(mailbox) == {"message_count": 0, "latest_message_at": None}
    finally:
        release_second_batch.set()
        if delete_task is not None and not delete_task.done():
            await delete_task
        await runtime.stop()


@pytest.mark.asyncio
async def test_mailbox_bulk_delete_generation_excludes_reused_rowid(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        mailbox_id = _seed_mailbox_deliveries(
            runtime,
            count=2,
            domain="bulk-delete-rowid-reuse.example",
        )
        job_id = await runtime.writer.execute(
            lambda connection: runtime.mailboxes._create_or_resume_bulk_delete_job(
                connection,
                mailbox_id,
                deleted_at="2026-01-02T00:00:00Z",
            )
        )
        first = await runtime.writer.execute(
            lambda connection: runtime.mailboxes._process_bulk_delete_job_batch(
                connection,
                job_id,
                batch_size=1,
            )
        )
        assert first == {"complete": False, "batch_deleted": 1, "deleted": 1}

        def reuse_frontier_rowid(connection) -> int:
            highest = connection.execute(
                """
                SELECT rowid, id
                FROM message_deliveries
                WHERE mailbox_id = ? AND status = 'active'
                ORDER BY rowid DESC
                LIMIT 1
                """,
                (mailbox_id,),
            ).fetchone()
            assert int(highest["rowid"]) == 2
            connection.execute(
                "DELETE FROM message_deliveries WHERE id = ?",
                (highest["id"],),
            )
            connection.execute(
                """
                INSERT INTO messages (
                    id, raw_path, raw_sha256, raw_size_bytes, received_at
                ) VALUES (
                    'reused-rowid-message', 'raw/reused-rowid-message.eml',
                    'reused-rowid-sha', 1, '2026-01-03T00:00:00Z'
                )
                """
            )
            # Omit mailbox_generation deliberately. The compatibility trigger
            # must inherit the post-job mailbox generation for direct writers.
            cursor = connection.execute(
                """
                INSERT INTO message_deliveries (
                    id, message_id, mailbox_id, rcpt_to, delivered_at
                ) VALUES (
                    'reused-rowid-delivery', 'reused-rowid-message', ?,
                    'bulk@bulk-delete-rowid-reuse.example',
                    '2026-01-03T00:00:00Z'
                )
                """,
                (mailbox_id,),
            )
            return int(cursor.lastrowid)

        reused_rowid = await runtime.writer.execute(reuse_frontier_rowid)
        assert reused_rowid == 2

        completed = await runtime.writer.execute(
            lambda connection: runtime.mailboxes._process_bulk_delete_job_batch(
                connection,
                job_id,
                batch_size=1,
            )
        )
        assert completed == {"complete": True, "batch_deleted": 0, "deleted": 1}

        with connect_database(settings.database_path) as connection:
            delivery = connection.execute(
                """
                SELECT rowid, status, mailbox_generation
                FROM message_deliveries
                WHERE id = 'reused-rowid-delivery'
                """
            ).fetchone()
            mailbox = connection.execute(
                """
                SELECT message_count, bulk_delete_generation
                FROM mailboxes
                WHERE id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            job = connection.execute(
                """
                SELECT status, target_generation, max_delivery_rowid
                FROM mailbox_bulk_delete_jobs
                WHERE id = ?
                """,
                (job_id,),
            ).fetchone()

        assert dict(delivery) == {
            "rowid": 2,
            "status": "active",
            "mailbox_generation": 1,
        }
        assert dict(mailbox) == {
            "message_count": 1,
            "bulk_delete_generation": 1,
        }
        assert dict(job) == {
            "status": "succeeded",
            "target_generation": 0,
            "max_delivery_rowid": 2,
        }
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_cancelled_mailbox_bulk_delete_request_keeps_persisted_job_running(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    first_batch_committed = asyncio.Event()
    release_job = asyncio.Event()
    try:
        mailbox_id = _seed_mailbox_deliveries(
            runtime,
            count=1500,
            domain="cancel-bulk-delete.example",
        )
        yield_count = 0

        async def hold_after_first_batch() -> None:
            nonlocal yield_count
            yield_count += 1
            if yield_count == 1:
                first_batch_committed.set()
                await release_job.wait()
            else:
                await asyncio.sleep(0)

        monkeypatch.setattr(
            runtime.mailboxes,
            "_yield_between_bulk_delete_batches",
            hold_after_first_batch,
        )
        request_task = asyncio.create_task(
            runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)
        )
        await asyncio.wait_for(first_batch_committed.wait(), timeout=10)
        request_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request_task

        with connect_database(settings.database_path) as connection:
            running = connection.execute(
                """
                SELECT status, cursor_delivery_rowid, deleted_count
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
        assert dict(running) == {
            "status": "running",
            "cursor_delivery_rowid": MAILBOX_BULK_DELETE_BATCH_SIZE,
            "deleted_count": MAILBOX_BULK_DELETE_BATCH_SIZE,
        }

        release_job.set()
        await asyncio.wait_for(
            asyncio.gather(*tuple(runtime.mailboxes._bulk_delete_tasks.values())),
            timeout=10,
        )
        with connect_database(settings.database_path) as connection:
            completed = connection.execute(
                """
                SELECT status, cursor_delivery_rowid, deleted_count, error
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            mailbox = connection.execute(
                "SELECT message_count, latest_message_at FROM mailboxes WHERE id = ?",
                (mailbox_id,),
            ).fetchone()
        assert dict(completed) == {
            "status": "succeeded",
            "cursor_delivery_rowid": 1500,
            "deleted_count": 1500,
            "error": None,
        }
        assert dict(mailbox) == {"message_count": 0, "latest_message_at": None}
    finally:
        release_job.set()
        if runtime.mailboxes._bulk_delete_tasks:
            await asyncio.gather(
                *tuple(runtime.mailboxes._bulk_delete_tasks.values()),
                return_exceptions=True,
            )
        await runtime.stop()


@pytest.mark.asyncio
async def test_clear_all_mail_tables_removes_retained_bulk_delete_job(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        mailbox_id = _seed_mailbox_deliveries(
            runtime,
            count=3,
            domain="clear-job-metadata.example",
        )
        result = await runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)
        assert result["deleted"] == 3

        cleared = await runtime.writer.execute(runtime._clear_mail_tables)
        assert cleared["deliveries"] == 3
        assert cleared["mailboxes"] == 1
        with connect_database(settings.database_path) as connection:
            jobs = int(
                connection.execute(
                    "SELECT COUNT(*) AS count FROM mailbox_bulk_delete_jobs"
                ).fetchone()["count"]
            )
            foreign_key_error = connection.execute("PRAGMA foreign_key_check").fetchone()
        assert jobs == 0
        assert foreign_key_error is None
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_failed_mailbox_bulk_delete_resumes_during_next_startup(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    first_runtime = RapidInboxRuntime(settings)
    await first_runtime.start()
    mailbox_id = _seed_mailbox_deliveries(
        first_runtime,
        count=1100,
        domain="resume-bulk-delete.example",
    )

    async def fail_after_first_commit() -> None:
        raise RuntimeError("injected mailbox bulk delete interruption")

    monkeypatch.setattr(
        first_runtime.mailboxes,
        "_yield_between_bulk_delete_batches",
        fail_after_first_commit,
    )
    with pytest.raises(RuntimeError, match="injected mailbox bulk delete interruption"):
        await first_runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)

    with connect_database(settings.database_path) as connection:
        failed = connection.execute(
            """
            SELECT status, cursor_delivery_rowid, deleted_count, error
            FROM mailbox_bulk_delete_jobs
            WHERE mailbox_id = ?
            """,
            (mailbox_id,),
        ).fetchone()
        stale_summary = connection.execute(
            "SELECT message_count FROM mailboxes WHERE id = ?",
            (mailbox_id,),
        ).fetchone()
    assert failed["status"] == "failed"
    assert int(failed["cursor_delivery_rowid"]) == MAILBOX_BULK_DELETE_BATCH_SIZE
    assert int(failed["deleted_count"]) == MAILBOX_BULK_DELETE_BATCH_SIZE
    assert "injected mailbox bulk delete interruption" in str(failed["error"])
    assert int(stale_summary["message_count"]) == 1100
    await first_runtime.stop()

    restarted_runtime = RapidInboxRuntime(settings)
    await restarted_runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            completed = connection.execute(
                """
                SELECT status, cursor_delivery_rowid, deleted_count, error
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            mailbox = connection.execute(
                "SELECT message_count, latest_message_at FROM mailboxes WHERE id = ?",
                (mailbox_id,),
            ).fetchone()
        assert dict(completed) == {
            "status": "succeeded",
            "cursor_delivery_rowid": 1100,
            "deleted_count": 1100,
            "error": None,
        }
        assert dict(mailbox) == {"message_count": 0, "latest_message_at": None}
    finally:
        await restarted_runtime.stop()


@pytest.mark.asyncio
async def test_runtime_stop_waits_for_bulk_delete_page_commit_then_restart_resumes(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    first_runtime = RapidInboxRuntime(settings)
    await first_runtime.start()
    mailbox_id = _seed_mailbox_deliveries(
        first_runtime,
        count=1500,
        domain="shutdown-bulk-delete.example",
    )
    page_updated = threading.Event()
    release_page_commit = threading.Event()
    original_process = first_runtime.mailboxes._process_bulk_delete_job_batch

    def block_first_page_before_commit(
        connection,
        job_id,
        *,
        batch_size=MAILBOX_BULK_DELETE_BATCH_SIZE,
    ):
        result = original_process(connection, job_id, batch_size=batch_size)
        page_updated.set()
        assert release_page_commit.wait(timeout=5)
        return result

    monkeypatch.setattr(
        first_runtime.mailboxes,
        "_process_bulk_delete_job_batch",
        block_first_page_before_commit,
    )
    delete_task = asyncio.create_task(
        first_runtime.mailboxes.soft_delete_mailbox_deliveries(mailbox_id)
    )
    stop_task: asyncio.Task[None] | None = None
    try:
        assert await asyncio.wait_for(
            asyncio.to_thread(page_updated.wait, 2),
            timeout=3,
        )
        stop_task = asyncio.create_task(first_runtime.stop())
        await asyncio.sleep(0.05)
        assert not stop_task.done()

        release_page_commit.set()
        await asyncio.wait_for(asyncio.shield(stop_task), timeout=5)
        delete_result = await asyncio.gather(delete_task, return_exceptions=True)
        assert isinstance(delete_result[0], RuntimeError)
        await first_runtime.stop()

        with connect_database(settings.database_path) as connection:
            paused = connection.execute(
                """
                SELECT status, cursor_delivery_rowid, deleted_count
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
        assert dict(paused) == {
            "status": "running",
            "cursor_delivery_rowid": MAILBOX_BULK_DELETE_BATCH_SIZE,
            "deleted_count": MAILBOX_BULK_DELETE_BATCH_SIZE,
        }
    finally:
        release_page_commit.set()
        if stop_task is None or not stop_task.done():
            await first_runtime.stop()

    restarted_runtime = RapidInboxRuntime(settings)
    await restarted_runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            completed = connection.execute(
                """
                SELECT status, cursor_delivery_rowid, deleted_count, error
                FROM mailbox_bulk_delete_jobs
                WHERE mailbox_id = ?
                """,
                (mailbox_id,),
            ).fetchone()
        assert dict(completed) == {
            "status": "succeeded",
            "cursor_delivery_rowid": 1500,
            "deleted_count": 1500,
            "error": None,
        }
    finally:
        await restarted_runtime.stop()
