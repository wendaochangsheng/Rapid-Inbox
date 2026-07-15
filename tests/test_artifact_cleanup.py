from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

import app.runtime as runtime_module
from app.config import Settings, default_settings
from app.db.connection import connect_database
from app.runtime import RapidInboxRuntime


def _settings(tmp_path: Path, **overrides: object) -> Settings:
    return Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        retention_cleanup_interval_seconds=86_400,
        **overrides,
    )


def _make_old(path: Path, *, seconds: int = 120) -> None:
    timestamp = time.time() - seconds
    os.utime(path, (timestamp, timestamp), follow_symlinks=False)


def test_artifact_governance_settings_load_defaults_and_environment(tmp_path, monkeypatch) -> None:
    defaults = _settings(tmp_path)
    assert defaults.maintenance_run_retention_days == 30
    assert defaults.quarantine_retention_days == 30
    assert defaults.orphan_artifact_grace_seconds == 86_400
    assert defaults.artifact_sweep_batch_size == 500

    monkeypatch.setenv("MAINTENANCE_RUN_RETENTION_DAYS", "7")
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "8")
    monkeypatch.setenv("ORPHAN_ARTIFACT_GRACE_SECONDS", "900")
    monkeypatch.setenv("ARTIFACT_SWEEP_BATCH_SIZE", "42")
    loaded = default_settings(tmp_path)
    assert loaded.maintenance_run_retention_days == 7
    assert loaded.quarantine_retention_days == 8
    assert loaded.orphan_artifact_grace_seconds == 900
    assert loaded.artifact_sweep_batch_size == 42


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("maintenance_run_retention_days", 0),
        ("quarantine_retention_days", 36_501),
        ("orphan_artifact_grace_seconds", 0),
        ("artifact_sweep_batch_size", 1_000_001),
    ],
)
def test_artifact_governance_settings_reject_unsafe_ranges(
    tmp_path,
    field_name: str,
    value: int,
) -> None:
    with pytest.raises(ValueError, match="invalid"):
        _settings(tmp_path, **{field_name: value})


@pytest.mark.asyncio
async def test_startup_marks_crashed_maintenance_run_failed(tmp_path) -> None:
    settings = _settings(tmp_path)
    first_runtime = RapidInboxRuntime(settings)
    await first_runtime.start()
    await first_runtime.stop()

    with connect_database(settings.database_path) as connection:
        connection.execute(
            """
            INSERT INTO maintenance_runs (id, kind, status, started_at)
            VALUES ('mnt_crashed', 'clear_all_mail', 'running', '2026-01-01T00:00:00Z')
            """
        )

    second_runtime = RapidInboxRuntime(settings)
    await second_runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                "SELECT status, finished_at, error FROM maintenance_runs WHERE id = 'mnt_crashed'"
            ).fetchone()
        assert row["status"] == "failed"
        assert row["finished_at"] is not None
        assert row["error"] == "runtime restarted before maintenance completed"
    finally:
        await second_runtime.stop()


@pytest.mark.asyncio
async def test_maintenance_history_cleanup_is_batched_and_never_deletes_running_run(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(
        tmp_path,
        maintenance_run_retention_days=1,
        cleanup_batch_size=1,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            connection.executemany(
                """
                INSERT INTO maintenance_runs (
                    id, kind, status, started_at, finished_at
                ) VALUES (?, 'test', ?, ?, ?)
                """,
                [
                    (
                        "mnt_old_1",
                        "succeeded",
                        "2026-04-01T00:00:00Z",
                        "2026-04-01T00:00:01Z",
                    ),
                    (
                        "mnt_old_2",
                        "failed",
                        "2026-04-02T00:00:00Z",
                        "2026-04-02T00:00:01Z",
                    ),
                    (
                        "mnt_running",
                        "running",
                        "2026-04-01T00:00:00Z",
                        None,
                    ),
                    (
                        "mnt_recent",
                        "succeeded",
                        "2026-04-19T12:00:00Z",
                        "2026-04-19T12:00:01Z",
                    ),
                ],
            )

        monkeypatch.setattr(runtime_module, "utc_now", lambda: "2026-04-20T00:00:00Z")
        result = await runtime.cleanup_expired_messages()
        assert result["maintenance_runs"] == 1

        with connect_database(settings.database_path) as connection:
            rows = connection.execute(
                "SELECT id, kind, status FROM maintenance_runs ORDER BY id"
            ).fetchall()
        by_id = {str(row["id"]): dict(row) for row in rows}
        assert "mnt_old_1" not in by_id
        assert by_id["mnt_old_2"]["status"] == "failed"
        assert by_id["mnt_running"]["status"] == "running"
        assert by_id["mnt_recent"]["status"] == "succeeded"
        cleanup_runs = [row for row in rows if row["kind"] == "retention_cleanup"]
        assert len(cleanup_runs) == 1
        assert cleanup_runs[0]["status"] == "succeeded"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_quarantine_sweep_honors_age_batch_and_symlink_boundaries(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        quarantine_retention_days=1,
        artifact_sweep_batch_size=1,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        quarantine = settings.storage_root / "quarantine" / "manifests"
        quarantine.mkdir(parents=True)
        old_one = quarantine / "a-old.json"
        old_two = quarantine / "b-old.json"
        young = quarantine / "c-young.json"
        old_one.write_text("old-one", encoding="utf-8")
        old_two.write_text("old-two", encoding="utf-8")
        young.write_text("young", encoding="utf-8")
        _make_old(old_one, seconds=2 * 86_400)
        _make_old(old_two, seconds=2 * 86_400)

        outside = tmp_path / "outside-evidence.json"
        outside.write_text("must survive", encoding="utf-8")
        _make_old(outside, seconds=2 * 86_400)
        outside_link = quarantine / "outside-link.json"
        outside_link.symlink_to(outside)

        first = await runtime.cleanup_expired_messages()
        assert first["quarantine_files_examined"] == 1
        assert first["quarantine_files_deleted"] == 1
        assert first["files"] == 1
        assert sum(path.exists() for path in (old_one, old_two)) == 1

        second = await runtime.cleanup_expired_messages()
        assert second["quarantine_files_examined"] == 1
        assert second["quarantine_files_deleted"] == 1
        assert second["files"] == 1
        assert not old_one.exists()
        assert not old_two.exists()
        assert young.exists()
        assert outside_link.is_symlink()
        assert outside.read_text(encoding="utf-8") == "must survive"
    finally:
        await runtime.stop()


def test_artifact_sweep_resumes_one_iterator_until_the_pass_is_exhausted(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = RapidInboxRuntime(_settings(tmp_path))
    categories = ("attachments", "html", "raw", "text")
    candidates = [
        {
            "storage_path": f"raw/2026/04/18/msg_{index}.eml",
            "stat_signature": (1, index, 0, 0, 0, 0),
        }
        for index in range(5)
    ]
    pass_starts: list[tuple[tuple[str, ...], float]] = []

    def iter_candidates(
        selected_categories: tuple[str, ...],
        cutoff_epoch: float,
    ):
        pass_starts.append((selected_categories, cutoff_epoch))
        return iter(candidates)

    monkeypatch.setattr(runtime, "_iter_old_regular_artifacts", iter_candidates)

    first = runtime._select_artifact_sweep_candidates(
        categories,
        cutoff_epoch=100.0,
        limit=2,
        cursor_name="orphan",
    )
    second = runtime._select_artifact_sweep_candidates(
        categories,
        cutoff_epoch=200.0,
        limit=2,
        cursor_name="orphan",
    )
    assert [candidate["storage_path"] for candidate in first + second] == [
        candidate["storage_path"] for candidate in candidates[:4]
    ]
    assert pass_starts == [(categories, 100.0)]

    final = runtime._select_artifact_sweep_candidates(
        categories,
        cutoff_epoch=300.0,
        limit=2,
        cursor_name="orphan",
    )
    assert final == [candidates[4]]
    assert "orphan" not in runtime._artifact_sweep_iterators
    assert pass_starts == [(categories, 100.0)]

    restarted = runtime._select_artifact_sweep_candidates(
        categories,
        cutoff_epoch=400.0,
        limit=1,
        cursor_name="orphan",
    )
    assert restarted == [candidates[0]]
    assert pass_starts == [(categories, 100.0), (categories, 400.0)]


@pytest.mark.asyncio
async def test_clear_all_resets_artifact_sweep_before_the_next_pass(
    tmp_path,
    monkeypatch,
) -> None:
    runtime = RapidInboxRuntime(_settings(tmp_path))
    await runtime.start()
    try:
        pass_starts = 0
        candidate = {
            "storage_path": "raw/2026/04/18/msg_stale.eml",
            "stat_signature": (1, 2, 3, 4, 5, 6),
        }

        def iter_candidates(_categories: tuple[str, ...], _cutoff_epoch: float):
            nonlocal pass_starts
            pass_starts += 1
            return iter((candidate, candidate))

        monkeypatch.setattr(runtime, "_iter_old_regular_artifacts", iter_candidates)
        assert runtime._select_artifact_sweep_candidates(
            ("raw",),
            cutoff_epoch=100.0,
            limit=1,
            cursor_name="orphan",
        ) == [candidate]
        assert pass_starts == 1
        assert "orphan" in runtime._artifact_sweep_iterators

        await runtime.clear_all_mail()
        assert runtime._artifact_sweep_iterators == {}

        assert runtime._select_artifact_sweep_candidates(
            ("raw",),
            cutoff_epoch=200.0,
            limit=1,
            cursor_name="orphan",
        ) == [candidate]
        assert pass_starts == 2
    finally:
        await runtime.stop()


def test_database_protected_candidates_skip_manifest_tree_scan(tmp_path, monkeypatch) -> None:
    runtime = RapidInboxRuntime(_settings(tmp_path))
    storage_path = "attachments/msg_database/att_file-report.txt"
    candidate = {"storage_path": storage_path, "stat_signature": (1, 2, 3, 4, 5, 6)}
    monkeypatch.setattr(
        runtime,
        "_database_artifact_references",
        lambda paths: set(paths),
    )

    def fail_manifest_scan(_message_ids):
        raise AssertionError("DB-protected attachments must not scan the manifest tree")

    monkeypatch.setattr(runtime, "_matching_manifest_paths", fail_manifest_scan)
    monkeypatch.setattr(runtime, "_matching_quarantine_paths", fail_manifest_scan)

    assert runtime._protected_artifact_paths([candidate]) == {storage_path}


@pytest.mark.asyncio
async def test_orphan_sweep_preserves_database_manifest_tombstone_and_symlinks(
    tmp_path,
    sample_email_bytes,
) -> None:
    settings = _settings(
        tmp_path,
        orphan_artifact_grace_seconds=1,
        artifact_sweep_batch_size=100,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        await runtime.create_domain("protected.example")
        await runtime.accept_message(
            rcpt_tos=["mailbox@protected.example"],
            envelope_from="sender@example.com",
            content=sample_email_bytes,
        )
        await runtime.drain_parser_queue()
        with connect_database(settings.database_path) as connection:
            db_raw_path = str(
                connection.execute(
                    "SELECT raw_path FROM messages LIMIT 1",
                ).fetchone()["raw_path"]
            )

        received_at = "2026-04-18T20:00:00Z"
        manifest_raw_path, _, _ = runtime.storage.write_raw_message(
            "msg_manifest_only",
            received_at,
            b"manifest protected",
        )
        runtime.storage.write_manifest(
            "msg_manifest_only",
            received_at,
            {"message_id": "msg_manifest_only", "raw_path": manifest_raw_path},
        )
        tombstone_raw_path, _, _ = runtime.storage.write_raw_message(
            "msg_tombstone_only",
            received_at,
            b"tombstone protected",
        )
        orphan_raw_path, _, _ = runtime.storage.write_raw_message(
            "msg_orphan",
            received_at,
            b"delete me",
        )
        young_raw_path, _, _ = runtime.storage.write_raw_message(
            "msg_young",
            received_at,
            b"too new",
        )
        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                INSERT INTO file_gc_tasks (
                    storage_path, reason, attempts, next_attempt_at, created_at, updated_at
                ) VALUES (?, 'test', 0, '2999-01-01T00:00:00Z', ?, ?)
                """,
                (tombstone_raw_path, received_at, received_at),
            )

        for storage_path in (
            db_raw_path,
            manifest_raw_path,
            tombstone_raw_path,
            orphan_raw_path,
        ):
            _make_old(runtime.storage.resolve(storage_path))

        outside_file = tmp_path / "outside-message.eml"
        outside_file.write_bytes(b"outside")
        _make_old(outside_file)
        file_link = settings.raw_dir / "outside-file.eml"
        file_link.symlink_to(outside_file)
        outside_directory = tmp_path / "outside-directory"
        outside_directory.mkdir()
        escaped_file = outside_directory / "escaped.eml"
        escaped_file.write_bytes(b"escaped")
        _make_old(escaped_file)
        directory_link = settings.raw_dir / "outside-directory"
        directory_link.symlink_to(outside_directory, target_is_directory=True)

        result = await runtime.cleanup_expired_messages()
        assert result["orphan_artifacts_deleted"] == 1
        assert result["orphan_artifacts_protected"] >= 3
        assert runtime.storage.resolve(db_raw_path).exists()
        assert runtime.storage.resolve(manifest_raw_path).exists()
        assert runtime.storage.resolve(tombstone_raw_path).exists()
        assert not runtime.storage.resolve(orphan_raw_path).exists()
        assert runtime.storage.resolve(young_raw_path).exists()
        assert file_link.is_symlink()
        assert directory_link.is_symlink()
        assert outside_file.read_bytes() == b"outside"
        assert escaped_file.read_bytes() == b"escaped"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_quarantined_bad_manifest_releases_old_raw_for_orphan_cleanup(tmp_path) -> None:
    settings = _settings(
        tmp_path,
        orphan_artifact_grace_seconds=1,
        artifact_sweep_batch_size=10,
    )
    runtime = RapidInboxRuntime(settings)
    await runtime.start()
    try:
        received_at = "2026-04-18T20:00:00Z"
        raw_path, _, _ = runtime.storage.write_raw_message(
            "msg_bad_manifest",
            received_at,
            b"unrecoverable raw",
        )
        manifest_path = runtime.storage.resolve(
            runtime.storage.manifest_path("msg_bad_manifest", received_at)
        )
        manifest_path.parent.mkdir(parents=True)
        manifest_path.write_text("{broken-json", encoding="utf-8")
        _make_old(runtime.storage.resolve(raw_path))
        _make_old(manifest_path)

        await runtime.recovery.recover_missing_manifests(incremental=False)
        assert not manifest_path.exists()
        quarantined = list((settings.storage_root / "quarantine" / "manifests").glob("*.json"))
        assert len(quarantined) == 1
        assert runtime.storage.resolve(raw_path).exists()

        preserved = await runtime.cleanup_expired_messages()
        assert preserved["orphan_artifacts_deleted"] == 0
        assert preserved["orphan_artifacts_protected"] == 1
        assert runtime.storage.resolve(raw_path).exists()

        _make_old(quarantined[0], seconds=31 * 86_400)
        result = await runtime.cleanup_expired_messages()
        assert result["quarantine_files_deleted"] == 1
        assert result["orphan_artifacts_deleted"] == 1
        assert result["files"] == 2
        assert not runtime.storage.resolve(raw_path).exists()
    finally:
        await runtime.stop()
