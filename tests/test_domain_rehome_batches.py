from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.db.connection import connect_database
from app.runtime import RapidInboxRuntime
from app.services.domains import DOMAIN_REHOME_BATCH_SIZE


@pytest.mark.asyncio
async def test_nonrouting_domain_update_does_not_schedule_rehome(runtime) -> None:
    domain = await runtime.create_domain("nonrouting-update.example")
    with connect_database(runtime.settings.database_path) as connection:
        jobs_before = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM domain_rehome_jobs"
            ).fetchone()["count"]
        )

    updated = await runtime.domains.update_domain(
        domain["id"],
        {
            "public_web_enabled": True,
            "public_api_enabled": True,
            "is_hidden": True,
            "max_message_size_bytes": 1_048_576,
            "retention_days": 7,
            "notes": "policy metadata only",
        },
    )

    with connect_database(runtime.settings.database_path) as connection:
        jobs_after = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM domain_rehome_jobs"
            ).fetchone()["count"]
        )
    assert jobs_after == jobs_before
    assert updated["public_web_enabled"] is True
    assert updated["public_api_enabled"] is True
    assert updated["is_hidden"] is True
    assert updated["max_message_size_bytes"] == 1_048_576
    assert updated["retention_days"] == 7
    assert updated["notes"] == "policy metadata only"


@pytest.mark.asyncio
async def test_domain_rehome_scans_100k_mailboxes_in_bounded_interleavable_transactions(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_plus_catchall",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    release_second_batch = asyncio.Event()
    first_batch_committed = asyncio.Event()
    create_task: asyncio.Task[dict] | None = None
    try:
        with connect_database(settings.database_path, durable_writes=True) as connection:
            catch_all = connection.execute(
                "SELECT id FROM domains WHERE root_domain_ascii = '*'"
            ).fetchone()
            assert catch_all is not None
            connection.execute(
                """
                WITH RECURSIVE sequence(value) AS (
                    VALUES (1)
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value < 100000
                )
                INSERT INTO mailboxes (
                    domain_id,
                    local_part_canonical,
                    rcpt_domain_ascii,
                    address_canonical,
                    address_display,
                    first_seen_at,
                    last_seen_at,
                    latest_message_at,
                    message_count
                )
                SELECT
                    ?,
                    printf('unrelated%06d', value),
                    'unrelated.invalid',
                    printf('unrelated%06d@unrelated.invalid', value),
                    printf('unrelated%06d@unrelated.invalid', value),
                    '2026-01-01T00:00:00Z',
                    '2026-01-01T00:00:00Z',
                    NULL,
                    0
                FROM sequence
                """,
                (int(catch_all["id"]),),
            )

        scanned_pages: list[int] = []
        original_process = runtime.domains._process_rehome_job_batch

        def observe_batch(connection, job_id, *, batch_size=DOMAIN_REHOME_BATCH_SIZE):
            result = original_process(connection, job_id, batch_size=batch_size)
            scanned_pages.append(int(result["scanned"]))
            return result

        yield_count = 0

        async def pause_after_first_batch() -> None:
            nonlocal yield_count
            yield_count += 1
            if yield_count == 1:
                first_batch_committed.set()
                await release_second_batch.wait()
            else:
                await asyncio.sleep(0)

        monkeypatch.setattr(runtime.domains, "_process_rehome_job_batch", observe_batch)
        monkeypatch.setattr(
            runtime.domains,
            "_yield_between_rehome_batches",
            pause_after_first_batch,
        )

        create_task = asyncio.create_task(runtime.create_domain("new-bounded.example"))
        await asyncio.wait_for(first_batch_committed.wait(), timeout=10)
        assert scanned_pages == [DOMAIN_REHOME_BATCH_SIZE]

        marker_batch_count: list[int] = []

        def marker_write(connection) -> None:
            marker_batch_count.append(len(scanned_pages))
            connection.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES ('_test_rehome_interleave', '1', '2026-01-01T00:00:00Z')
                """
            )

        # The marker commits while migration is paused between page transactions.
        await asyncio.wait_for(runtime.writer.execute(marker_write), timeout=5)
        assert marker_batch_count == [1]
        assert not create_task.done()
        release_second_batch.set()
        created = await asyncio.wait_for(create_task, timeout=30)
        assert created["root_domain_ascii"] == "new-bounded.example"

        assert len(scanned_pages) == 100
        assert sum(scanned_pages) == 100_000
        assert all(0 < count <= DOMAIN_REHOME_BATCH_SIZE for count in scanned_pages)
        with connect_database(settings.database_path) as connection:
            job = connection.execute(
                """
                SELECT status, mailboxes_scanned, mailboxes_rehomed
                FROM domain_rehome_jobs
                WHERE reason = 'domain.create'
                  AND candidate_root_domain = 'new-bounded.example'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """
            ).fetchone()
            plan = connection.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT id, domain_id, address_canonical
                FROM mailboxes
                WHERE id > ? AND id <= ?
                ORDER BY id ASC
                LIMIT ?
                """,
                (0, 100_000, DOMAIN_REHOME_BATCH_SIZE),
            ).fetchall()
        assert dict(job) == {
            "status": "succeeded",
            "mailboxes_scanned": 100_000,
            "mailboxes_rehomed": 0,
        }
        assert not any("TEMP B-TREE" in str(row["detail"]) for row in plan)
    finally:
        release_second_batch.set()
        if create_task is not None and not create_task.done():
            await create_task
        await runtime.stop()


@pytest.mark.asyncio
async def test_cancelled_domain_request_leaves_tracked_job_to_completion(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_plus_catchall",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    first_batch_committed = asyncio.Event()
    release_job = asyncio.Event()
    try:
        with connect_database(settings.database_path) as connection:
            catch_all_id = int(
                connection.execute(
                    "SELECT id FROM domains WHERE root_domain_ascii = '*'"
                ).fetchone()["id"]
            )
            connection.execute(
                """
                WITH RECURSIVE sequence(value) AS (
                    VALUES (1)
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value < 1500
                )
                INSERT INTO mailboxes (
                    domain_id, local_part_canonical, rcpt_domain_ascii,
                    address_canonical, address_display, first_seen_at, last_seen_at
                )
                SELECT
                    ?, printf('cancel%04d', value), 'elsewhere.invalid',
                    printf('cancel%04d@elsewhere.invalid', value),
                    printf('cancel%04d@elsewhere.invalid', value),
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
                FROM sequence
                """,
                (catch_all_id,),
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
            runtime.domains,
            "_yield_between_rehome_batches",
            hold_after_first_batch,
        )
        request_task = asyncio.create_task(runtime.create_domain("cancelled-job.example"))
        await asyncio.wait_for(first_batch_committed.wait(), timeout=10)
        request_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request_task

        with connect_database(settings.database_path) as connection:
            running = connection.execute(
                """
                SELECT status, cursor_mailbox_id
                FROM domain_rehome_jobs
                WHERE candidate_root_domain = 'cancelled-job.example'
                """
            ).fetchone()
        assert running is not None
        assert running["status"] == "running"
        assert int(running["cursor_mailbox_id"]) == DOMAIN_REHOME_BATCH_SIZE

        release_job.set()
        await asyncio.wait_for(
            asyncio.gather(*tuple(runtime.domains._rehome_tasks)),
            timeout=10,
        )
        with connect_database(settings.database_path) as connection:
            completed = connection.execute(
                """
                SELECT status, mailboxes_scanned
                FROM domain_rehome_jobs
                WHERE candidate_root_domain = 'cancelled-job.example'
                """
            ).fetchone()
        assert dict(completed) == {
            "status": "succeeded",
            "mailboxes_scanned": 1500,
        }
    finally:
        release_job.set()
        if runtime.domains._rehome_tasks:
            await asyncio.gather(*tuple(runtime.domains._rehome_tasks), return_exceptions=True)
        await runtime.stop()


@pytest.mark.asyncio
async def test_failed_domain_rehome_persists_cursor_and_resumes(
    tmp_path,
    monkeypatch,
) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        ingress_mode="managed_plus_catchall",
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            catch_all_id = int(
                connection.execute(
                    "SELECT id FROM domains WHERE root_domain_ascii = '*'"
                ).fetchone()["id"]
            )
            connection.execute(
                """
                WITH RECURSIVE sequence(value) AS (
                    VALUES (1)
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value < 1100
                )
                INSERT INTO mailboxes (
                    domain_id, local_part_canonical, rcpt_domain_ascii,
                    address_canonical, address_display, first_seen_at, last_seen_at
                )
                SELECT
                    ?, printf('resume%04d', value), 'elsewhere.invalid',
                    printf('resume%04d@elsewhere.invalid', value),
                    printf('resume%04d@elsewhere.invalid', value),
                    '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z'
                FROM sequence
                """,
                (catch_all_id,),
            )

        original_yield = runtime.domains._yield_between_rehome_batches

        async def fail_after_first_commit() -> None:
            raise RuntimeError("injected rehome interruption")

        monkeypatch.setattr(
            runtime.domains,
            "_yield_between_rehome_batches",
            fail_after_first_commit,
        )
        with pytest.raises(RuntimeError, match="injected rehome interruption"):
            await runtime.create_domain("resume-job.example")

        with connect_database(settings.database_path) as connection:
            failed = connection.execute(
                """
                SELECT status, cursor_mailbox_id, mailboxes_scanned, error
                FROM domain_rehome_jobs
                WHERE candidate_root_domain = 'resume-job.example'
                """
            ).fetchone()
        assert failed["status"] == "failed"
        assert int(failed["cursor_mailbox_id"]) == DOMAIN_REHOME_BATCH_SIZE
        assert int(failed["mailboxes_scanned"]) == DOMAIN_REHOME_BATCH_SIZE
        assert "injected rehome interruption" in str(failed["error"])

        monkeypatch.setattr(
            runtime.domains,
            "_yield_between_rehome_batches",
            original_yield,
        )
        await runtime.domains._resume_incomplete_rehome_jobs()
        with connect_database(settings.database_path) as connection:
            completed = connection.execute(
                """
                SELECT status, cursor_mailbox_id, mailboxes_scanned, error
                FROM domain_rehome_jobs
                WHERE candidate_root_domain = 'resume-job.example'
                """
            ).fetchone()
        assert dict(completed) == {
            "status": "succeeded",
            "cursor_mailbox_id": 1100,
            "mailboxes_scanned": 1100,
            "error": None,
        }
    finally:
        await runtime.stop()
