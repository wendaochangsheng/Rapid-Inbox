from __future__ import annotations

import asyncio
from dataclasses import dataclass
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import stat
from pathlib import Path
from time import time, time_ns
from typing import TYPE_CHECKING, Iterable, Iterator
from uuid import uuid4

from app.db.connection import connect_database
from app.ingest.storage import INGEST_STATUS_FILENAME, INGEST_STATUS_FRESH_SECONDS

if TYPE_CHECKING:
    from app.runtime import RapidInboxRuntime


PERIODIC_MANIFEST_SCAN_BATCH_SIZE = 1000
PERIODIC_MANIFEST_RECENT_YEAR_COUNT = 2
PERIODIC_MANIFEST_RECENT_MONTH_COUNT = 2
PERIODIC_MANIFEST_RECENT_DAY_COUNT = 3
PERIODIC_MANIFEST_RETRY_SHARE = 4
FULL_MANIFEST_SCAN_BATCH_SIZE = 500
FULL_RECOVERY_REPLAY_BATCH_SIZE = 500
FAILED_REPARSE_BATCH_SIZE = 500
# Recovery receipts are attacker-influenced through SMTP headers.  A count-only
# page bound is therefore not a memory bound: hundreds of near-message-sized
# JSON files can otherwise be decoded together during startup.  Keep the
# per-file contract aligned with the C++ writer and admit at most this many
# encoded bytes into one scan/replay page.
MAX_RECOVERY_MANIFEST_BYTES = 16 * 1024 * 1024
MAX_RECOVERY_MANIFEST_BATCH_BYTES = 16 * 1024 * 1024
MANIFEST_RECOVERY_STABILITY_SECONDS = 3.0
INGEST_STATUS_RECOVERY_GUARD_SECONDS = 30.0
INGEST_STATUS_MAX_BYTES = 64 * 1024


@dataclass(slots=True)
class _FullRecoverySpool:
    connection: sqlite3.Connection
    watermark_ns: int


class RecoveryPolicyConflictError(ValueError):
    """A durable receipt targets a domain identity explicitly retired later."""


class RecoveryScanner:
    def __init__(self, runtime: "RapidInboxRuntime") -> None:
        self.runtime = runtime
        self._scan_lock = asyncio.Lock()
        self._periodic_watermark_ns = -1
        # Both collections can grow with the complete manifest history on a
        # coarse-mtime filesystem or when many legacy receipts cannot yet be
        # mapped to a domain.  Keep that state in a private temporary on-disk
        # SQLite database instead of the long-lived Python heap.
        self._periodic_state = sqlite3.connect("", check_same_thread=False)
        self._periodic_state.row_factory = sqlite3.Row
        self._periodic_state.executescript(
            """
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = FILE;

            CREATE TABLE periodic_retries (
                path TEXT PRIMARY KEY,
                rotation_sequence INTEGER NOT NULL
            );
            CREATE INDEX periodic_retries_rotation
                ON periodic_retries(rotation_sequence);

            CREATE TABLE periodic_watermark_paths (
                path TEXT PRIMARY KEY
            );
            CREATE TABLE full_scan_watermark_paths (
                path TEXT PRIMARY KEY
            );
            """
        )
        self._periodic_retry_sequence = 0
        self._full_scan_staged_watermark_ns = -1
        self._periodic_state_closed = False
        self._closing = False
        self._close_task: asyncio.Task[None] | None = None
        self._startup_full_scan_complete = False
        self._scan_requires_stability = False

    async def close(self) -> None:
        """Close disk-backed scan state after every admitted scan has stopped.

        Cancelling an ``asyncio.to_thread`` waiter does not stop its worker
        thread.  Closing the state connection as soon as the asyncio task is
        cancelled can therefore race with a scan still using that connection.
        Serialize close with the scan lock and keep the owned close task alive
        through caller cancellation.
        """

        self._closing = True
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close_periodic_state())

        cancelled = False
        while True:
            try:
                await asyncio.shield(self._close_task)
                break
            except asyncio.CancelledError:
                cancelled = True
                if self._close_task.done():
                    self._close_task.result()
                    break
        if cancelled:
            raise asyncio.CancelledError

    async def _close_periodic_state(self) -> None:
        async with self._scan_lock:
            if self._periodic_state_closed:
                return
            self._periodic_state.close()
            self._periodic_state_closed = True

    async def run(self) -> None:
        # Capture the pre-recovery rowid frontier so manifests that deliberately
        # restore a final ``failed`` parse result are not immediately reparsed.
        # A scalar frontier replaces the previous O(number of restored failures)
        # exclusion set.
        failed_reparse_cutoff = await self.runtime.recovery_reparse_rowid_cutoff()
        await self.recover_missing_manifests()
        await self._requeue_unparsed_messages(max_rowid=failed_reparse_cutoff)

    async def recover_missing_manifests(self, *, incremental: bool = False) -> set[str]:
        """Recover the manifest/DB difference without relying on global DB heuristics.

        A manifest is the receipt left by both ingress implementations before the
        metadata transaction.  Even a mostly healthy database may contain one new
        durable receipt that never reached SQLite. Startup performs a complete
        difference scan; periodic calls use a bounded recent-partition watermark
        so historical receipts are not read and hashed every ten seconds.
        """

        async with self._scan_lock:
            if self._closing:
                raise RuntimeError("RecoveryScanner is closing or closed")
            ingestd_state = await asyncio.to_thread(self._ingestd_recovery_state)
            if ingestd_state == "busy":
                # A live C++ writer owns every manifest in its reservation,
                # queue, and in-flight counters. Replaying while that writer is
                # blocked on SQLite would turn its eventual plain INSERT into a
                # duplicate-key poison job. Leave the cursor untouched and let
                # the next periodic pass retry after the daemon drains.
                return set()
            effective_incremental = incremental and self._startup_full_scan_complete
            # Retention commits file-GC tombstones and unlinks artifacts under
            # this same lock.  A scanner must observe either the live message or
            # its tombstone, never the gap between the metadata delete and file
            # removal where a manifest could otherwise resurrect the message.
            async with self.runtime._mail_store_lock:
                current_ingestd_state = await asyncio.to_thread(self._ingestd_recovery_state)
                if current_ingestd_state == "busy":
                    return set()
                self._scan_requires_stability = bool(
                    effective_incremental
                    or ingestd_state == "idle"
                    or current_ingestd_state == "idle"
                )
                result = await self._recover_manifests(incremental=effective_incremental)
            if not effective_incremental:
                self._startup_full_scan_complete = True
            return result

    async def _recover_manifests(self, *, incremental: bool) -> set[str]:
        scan_result = await self._scan_manifest_files_in_thread(incremental)
        if isinstance(scan_result, _FullRecoverySpool):
            try:
                await self._replay_full_recovery_spool(scan_result.connection)
            finally:
                await asyncio.to_thread(scan_result.connection.close)
            self._advance_full_periodic_watermark(scan_result.watermark_ns)
            return set()

        return await self._replay_scan_result(scan_result)

    async def _scan_manifest_files_in_thread(
        self,
        incremental: bool,
    ) -> dict[str, object] | _FullRecoverySpool:
        """Run a scan without abandoning its worker on task cancellation."""

        worker = asyncio.create_task(
            asyncio.to_thread(self._scan_manifest_files, incremental)
        )
        try:
            return await asyncio.shield(worker)
        except asyncio.CancelledError as cancelled:
            # ``to_thread`` workers cannot be cancelled once running.  Keep
            # this coroutine (and therefore ``_scan_lock``) alive until the
            # worker exits, even if shutdown itself receives repeated cancels.
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break

            if not worker.cancelled():
                try:
                    abandoned_result = worker.result()
                except BaseException:
                    # Cancellation remains the caller-visible outcome, but
                    # retrieving the result consumes any worker exception.
                    pass
                else:
                    # A cancelled startup scan may have completed creation of
                    # its private replay spool.  No caller remains to close it.
                    if isinstance(abandoned_result, _FullRecoverySpool):
                        abandoned_result.connection.close()
            raise cancelled

    async def _replay_scan_result(self, scan_result: dict[str, object]) -> set[str]:
        policy_manifests: list[tuple[Path, dict[str, object]]] = scan_result["policy"]
        legacy_manifests: list[tuple[Path, dict[str, object]]] = scan_result["legacy"]
        watermark_paths: list[tuple[Path, int]] = scan_result["watermark_paths"]
        self._schedule_periodic_retries(scan_result.get("retry_paths", []))
        latest_policy_snapshots: dict[str, dict[str, object]] = {}
        recovered_final_message_ids: set[str] = set()
        for manifest_path, manifest in policy_manifests:
            self._record_latest_policy_snapshots(latest_policy_snapshots, manifest_path, manifest)

        for snapshot in self._sorted_snapshots(latest_policy_snapshots):
            try:
                await self.runtime.recover_domain_snapshot(snapshot)
            except ValueError:
                continue

        for manifest_path, manifest in policy_manifests + legacy_manifests:
            try:
                recovered = await self.runtime.recover_from_manifest(manifest)
            except RecoveryPolicyConflictError as exc:
                self._quarantine_bad_manifest(manifest_path, exc)
                self._discard_periodic_retry(manifest_path)
                continue
            except ValueError:
                # Legacy manifests can remain unrecoverable if the matching domain never reappears.
                # Pin them even during the startup scan: startup also seeds the
                # periodic watermark, so omitting them here would make a later
                # domain repair invisible until the next process restart.
                self._schedule_periodic_retry(manifest_path)
                continue
            self._discard_periodic_retry(manifest_path)
            if recovered is False:
                continue
            message_id = manifest.get("message_id")
            if isinstance(message_id, str) and message_id:
                parsed = manifest.get("parsed")
                if isinstance(parsed, dict) and parsed.get("status") in {"parsed", "failed"}:
                    recovered_final_message_ids.add(message_id)
                else:
                    await self.runtime.enqueue_message_for_parse(
                        message_id,
                        raw_size_bytes=int(manifest["raw_size_bytes"]),
                    )
        # Commit the cursor only after every selected manifest has either been
        # replayed, classified as complete/retired/invalid, or pinned in the
        # explicit retry queue.  Unexpected exceptions leave the cursor intact,
        # so the next pass cannot jump over an incompletely processed batch.
        self._advance_periodic_watermark(watermark_paths)
        return recovered_final_message_ids

    def _scan_manifest_files(self, incremental: bool) -> dict[str, object] | _FullRecoverySpool:
        if not incremental:
            return self._build_full_recovery_spool()
        selected_paths, watermark_paths = self._select_manifest_paths(True)
        return self._scan_selected_manifest_files(selected_paths, watermark_paths)

    def _build_full_recovery_spool(self) -> _FullRecoverySpool:
        """Scan all receipts with bounded RAM and spool replay state to disk.

        ``sqlite3.connect("")`` creates a private temporary on-disk database
        that SQLite removes when the connection closes.  Only one manifest
        batch and one DB-existence result set are resident at a time; the
        complete history is never materialized as Python paths, JSON objects,
        or message-ID sets.
        """

        connection = sqlite3.connect("", check_same_thread=False)
        connection.row_factory = sqlite3.Row
        try:
            connection.executescript(
                """
                PRAGMA journal_mode = OFF;
                PRAGMA synchronous = OFF;
                PRAGMA temp_store = FILE;

                CREATE TABLE recovery_manifests (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    manifest_path TEXT NOT NULL,
                    manifest_json TEXT NOT NULL
                );

                CREATE TABLE recovery_domain_snapshots (
                    root_domain_ascii TEXT PRIMARY KEY,
                    domain_id INTEGER NOT NULL,
                    received_at TEXT NOT NULL,
                    domain_policy_json TEXT NOT NULL,
                    recovery_order_ns INTEGER NOT NULL,
                    manifest_mtime_ns INTEGER NOT NULL
                );
                """
            )
            watermark_ns = -1
            self._reset_full_scan_watermark_paths()
            selected_paths: list[tuple[Path, int]] = []
            selected_bytes = 0
            for manifest_path in self._iter_full_manifest_paths():
                try:
                    mtime_ns = self._manifest_mtime_ns(manifest_path)
                    manifest_bytes = self._manifest_budget_bytes(manifest_path)
                except OSError:
                    continue
                if selected_paths and (
                    len(selected_paths) >= FULL_MANIFEST_SCAN_BATCH_SIZE
                    or selected_bytes + manifest_bytes
                    > MAX_RECOVERY_MANIFEST_BATCH_BYTES
                ):
                    self._record_full_scan_watermark_paths(selected_paths, watermark_ns)
                    self._spool_full_scan_batch(connection, selected_paths)
                    selected_paths = []
                    selected_bytes = 0
                watermark_ns = max(watermark_ns, mtime_ns)
                selected_paths.append((manifest_path, mtime_ns))
                selected_bytes += manifest_bytes
                # An over-limit file is admitted alone so the bounded reader
                # can classify and quarantine it without allocating its body.
                if (
                    len(selected_paths) >= FULL_MANIFEST_SCAN_BATCH_SIZE
                    or selected_bytes >= MAX_RECOVERY_MANIFEST_BATCH_BYTES
                ):
                    self._record_full_scan_watermark_paths(selected_paths, watermark_ns)
                    self._spool_full_scan_batch(connection, selected_paths)
                    selected_paths = []
                    selected_bytes = 0
            if selected_paths:
                self._record_full_scan_watermark_paths(selected_paths, watermark_ns)
                self._spool_full_scan_batch(connection, selected_paths)
            connection.commit()
            return _FullRecoverySpool(connection=connection, watermark_ns=watermark_ns)
        except BaseException:
            connection.close()
            raise

    def _iter_full_manifest_paths(self) -> Iterator[Path]:
        root = self.runtime.settings.manifests_dir
        for directory, directory_names, file_names in os.walk(root, followlinks=False):
            directory_names.sort()
            file_names.sort()
            parent = Path(directory)
            for file_name in file_names:
                if file_name.endswith(".json"):
                    yield parent / file_name

    def _spool_full_scan_batch(
        self,
        connection: sqlite3.Connection,
        selected_paths: list[tuple[Path, int]],
    ) -> None:
        scan_result = self._scan_selected_manifest_files(selected_paths, [])
        self._schedule_periodic_retries(scan_result["retry_paths"])

        selected_mtimes = dict(selected_paths)
        manifests = list(scan_result["policy"]) + list(scan_result["legacy"])
        for manifest_path, manifest in manifests:
            connection.execute(
                """
                INSERT INTO recovery_manifests (manifest_path, manifest_json)
                VALUES (?, ?)
                """,
                (
                    str(manifest_path),
                    json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
                ),
            )

        for manifest_path, manifest in scan_result["policy"]:
            recovery_order = self._manifest_recovery_order_from_mtime(
                selected_mtimes[manifest_path],
                manifest,
            )
            for recipient in manifest["recipients"]:
                if not isinstance(recipient, dict):
                    continue
                domain_policy = recipient.get("domain_policy")
                if domain_policy is None:
                    continue
                connection.execute(
                    """
                    INSERT INTO recovery_domain_snapshots (
                        root_domain_ascii,
                        domain_id,
                        received_at,
                        domain_policy_json,
                        recovery_order_ns,
                        manifest_mtime_ns
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(root_domain_ascii) DO UPDATE SET
                        domain_id = excluded.domain_id,
                        received_at = excluded.received_at,
                        domain_policy_json = excluded.domain_policy_json,
                        recovery_order_ns = excluded.recovery_order_ns,
                        manifest_mtime_ns = excluded.manifest_mtime_ns
                    WHERE excluded.recovery_order_ns > recovery_domain_snapshots.recovery_order_ns
                       OR (
                           excluded.recovery_order_ns = recovery_domain_snapshots.recovery_order_ns
                           AND excluded.manifest_mtime_ns >= recovery_domain_snapshots.manifest_mtime_ns
                       )
                    """,
                    (
                        str(recipient["root_domain_ascii"]),
                        int(recipient["domain_id"]),
                        str(manifest["received_at"]),
                        json.dumps(domain_policy, ensure_ascii=False, separators=(",", ":")),
                        recovery_order[0],
                        recovery_order[1],
                    ),
                )
        # Bound SQLite's rollback state as well as Python memory.
        connection.commit()

    async def _replay_full_recovery_spool(self, connection: sqlite3.Connection) -> None:
        after_root: str | None = None
        while True:
            snapshots = await asyncio.to_thread(
                self._read_full_snapshot_page,
                connection,
                after_root,
            )
            if not snapshots:
                break
            for snapshot in snapshots:
                try:
                    await self.runtime.recover_domain_snapshot(snapshot)
                except ValueError:
                    continue
            after_root = str(snapshots[-1]["root_domain_ascii"])

        after_sequence = 0
        while True:
            rows = await asyncio.to_thread(
                self._read_full_manifest_page,
                connection,
                after_sequence,
            )
            if not rows:
                break
            for sequence, manifest_path, manifest in rows:
                try:
                    recovered = await self.runtime.recover_from_manifest(manifest)
                except RecoveryPolicyConflictError as exc:
                    self._quarantine_bad_manifest(manifest_path, exc)
                    self._discard_periodic_retry(manifest_path)
                    continue
                except ValueError:
                    self._schedule_periodic_retry(manifest_path)
                    continue
                self._discard_periodic_retry(manifest_path)
                if recovered is False:
                    continue
                parsed = manifest.get("parsed")
                if not (
                    isinstance(parsed, dict)
                    and parsed.get("status") in {"parsed", "failed"}
                ):
                    await self.runtime.enqueue_message_for_parse(
                        str(manifest["message_id"]),
                        raw_size_bytes=int(manifest["raw_size_bytes"]),
                    )
            after_sequence = rows[-1][0]

    def _read_full_snapshot_page(
        self,
        connection: sqlite3.Connection,
        after_root: str | None,
    ) -> list[dict[str, object]]:
        if after_root is None:
            rows = connection.execute(
                """
                SELECT *
                FROM recovery_domain_snapshots
                ORDER BY root_domain_ascii ASC
                LIMIT ?
                """,
                (FULL_RECOVERY_REPLAY_BATCH_SIZE,),
            ).fetchall()
        else:
            rows = connection.execute(
                """
                SELECT *
                FROM recovery_domain_snapshots
                WHERE root_domain_ascii > ?
                ORDER BY root_domain_ascii ASC
                LIMIT ?
                """,
                (after_root, FULL_RECOVERY_REPLAY_BATCH_SIZE),
            ).fetchall()
        return [
            {
                "domain_id": int(row["domain_id"]),
                "root_domain_ascii": str(row["root_domain_ascii"]),
                "received_at": str(row["received_at"]),
                "domain_policy": json.loads(str(row["domain_policy_json"])),
            }
            for row in rows
        ]

    def _read_full_manifest_page(
        self,
        connection: sqlite3.Connection,
        after_sequence: int,
    ) -> list[tuple[int, Path, dict[str, object]]]:
        metadata = connection.execute(
            """
            SELECT
                sequence,
                COALESCE(LENGTH(CAST(manifest_json AS BLOB)), 0) AS encoded_bytes
            FROM recovery_manifests
            WHERE sequence > ?
            ORDER BY sequence ASC
            LIMIT ?
            """,
            (after_sequence, FULL_RECOVERY_REPLAY_BATCH_SIZE),
        ).fetchall()
        if not metadata:
            return []

        selected_end = after_sequence
        selected_bytes = 0
        for row in metadata:
            encoded_bytes = int(row["encoded_bytes"])
            if encoded_bytes > MAX_RECOVERY_MANIFEST_BYTES:
                raise ValueError("spooled recovery manifest exceeds the size limit")
            if (
                selected_end != after_sequence
                and selected_bytes + encoded_bytes
                > MAX_RECOVERY_MANIFEST_BATCH_BYTES
            ):
                break
            selected_end = int(row["sequence"])
            selected_bytes += encoded_bytes
            if selected_bytes >= MAX_RECOVERY_MANIFEST_BATCH_BYTES:
                break

        rows = connection.execute(
            """
            SELECT sequence, manifest_path, manifest_json
            FROM recovery_manifests
            WHERE sequence > ? AND sequence <= ?
            ORDER BY sequence ASC
            """,
            (after_sequence, selected_end),
        ).fetchall()
        return [
            (
                int(row["sequence"]),
                Path(str(row["manifest_path"])),
                json.loads(str(row["manifest_json"])),
            )
            for row in rows
        ]

    def _scan_selected_manifest_files(
        self,
        selected_paths: list[tuple[Path, int]],
        watermark_paths: list[tuple[Path, int]],
    ) -> dict[str, object]:
        selected_mtimes = dict(selected_paths)
        retired_manifest_paths = self._file_gc_storage_paths(
            {self._relative_storage_path(path) for path, _mtime_ns in selected_paths}
        )
        # Normal receipts are named after their message ID.  Query those tiny
        # filename keys before opening JSON so a restart does not deserialize
        # the complete historical manifest corpus merely to discover that all
        # corresponding rows and deliveries already exist.
        complete_filename_ids = self._complete_message_ids_subset(
            {manifest_path.stem for manifest_path, _mtime_ns in selected_paths}
        )
        parsed: list[tuple[Path, dict[str, object]]] = []
        for manifest_path, _mtime_ns in selected_paths:
            if self._relative_storage_path(manifest_path) in retired_manifest_paths:
                self._discard_periodic_retry(manifest_path)
                continue
            if manifest_path.stem in complete_filename_ids:
                self._discard_periodic_retry(manifest_path)
                continue
            try:
                manifest = self._read_manifest(manifest_path)
                self.runtime.validate_recovery_manifest(manifest)
            except (json.JSONDecodeError, OSError, ValueError) as exc:
                self._quarantine_bad_manifest(manifest_path, exc)
                continue
            parsed.append((manifest_path, manifest))

        retired_raw_paths = self._file_gc_storage_paths(
            {str(manifest["raw_path"]) for _manifest_path, manifest in parsed}
        )
        if retired_raw_paths:
            active_parsed: list[tuple[Path, dict[str, object]]] = []
            for manifest_path, manifest in parsed:
                if str(manifest["raw_path"]) in retired_raw_paths:
                    self._discard_periodic_retry(manifest_path)
                    continue
                active_parsed.append((manifest_path, manifest))
            parsed = active_parsed

        complete_message_ids = (
            self._complete_message_ids_subset(
                {str(manifest["message_id"]) for _, manifest in parsed}
            )
            if parsed
            else set()
        )

        policy: list[tuple[Path, dict[str, object]]] = []
        legacy: list[tuple[Path, dict[str, object]]] = []
        retry_paths: list[Path] = []
        active_accept_message_ids = self._active_accept_message_ids()
        for manifest_path, manifest in parsed:
            message_id = str(manifest["message_id"])
            if message_id in complete_message_ids:
                self._discard_periodic_retry(manifest_path)
                continue
            if message_id in active_accept_message_ids or (
                self._scan_requires_stability
                and not self._manifest_mtime_is_stable(selected_mtimes[manifest_path])
            ):
                retry_paths.append(manifest_path)
                continue
            try:
                self._validate_raw_artifact(manifest)
            except (OSError, ValueError) as exc:
                self._quarantine_bad_manifest(manifest_path, exc)
                continue
            if self._has_domain_policy(manifest):
                policy.append((manifest_path, manifest))
            else:
                legacy.append((manifest_path, manifest))

        return {
            "policy": policy,
            "legacy": legacy,
            "retry_paths": retry_paths,
            "watermark_paths": watermark_paths,
        }

    def _active_accept_message_ids(self) -> set[str]:
        snapshot = getattr(self.runtime, "active_mail_accept_message_ids", None)
        if snapshot is None:
            return set()
        return {str(message_id) for message_id in snapshot()}

    def _manifest_mtime_is_stable(self, mtime_ns: int) -> bool:
        minimum_age_ns = int(MANIFEST_RECOVERY_STABILITY_SECONDS * 1_000_000_000)
        return time_ns() - int(mtime_ns) >= minimum_age_ns

    def _ingestd_recovery_state(self) -> str:
        settings = getattr(self.runtime, "settings", None)
        if settings is None:
            return "offline"
        status_path = Path(settings.storage_root) / INGEST_STATUS_FILENAME
        try:
            stat_result = status_path.stat()
        except OSError:
            return "offline"
        age_seconds = max(time() - stat_result.st_mtime, 0.0)
        if age_seconds > INGEST_STATUS_RECOVERY_GUARD_SECONDS:
            return "offline"
        if age_seconds > INGEST_STATUS_FRESH_SECONDS:
            return "busy"
        if stat_result.st_size > INGEST_STATUS_MAX_BYTES:
            return "busy"
        try:
            with status_path.open("rb") as heartbeat:
                raw_status = heartbeat.read(INGEST_STATUS_MAX_BYTES + 1)
            payload = json.loads(raw_status)
        except (OSError, UnicodeError, json.JSONDecodeError):
            return "busy"
        if not isinstance(payload, dict):
            return "busy"
        queue_messages = payload.get("queue_messages")
        if (
            not isinstance(queue_messages, int)
            or isinstance(queue_messages, bool)
            or queue_messages < 0
        ):
            return "busy"
        return "busy" if queue_messages > 0 else "idle"

    def _read_manifest(self, manifest_path: Path) -> dict[str, object]:
        with manifest_path.open("rb") as handle:
            file_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(file_stat.st_mode):
                raise ValueError("recovery manifest is not a regular file")
            if file_stat.st_size > MAX_RECOVERY_MANIFEST_BYTES:
                raise ValueError("recovery manifest exceeds the size limit")
            encoded = handle.read(MAX_RECOVERY_MANIFEST_BYTES + 1)
        if len(encoded) > MAX_RECOVERY_MANIFEST_BYTES:
            raise ValueError("recovery manifest exceeds the size limit")
        manifest = json.loads(encoded.decode("utf-8"))
        if not isinstance(manifest, dict):
            raise ValueError("invalid recovery manifest")
        return manifest

    @staticmethod
    def _manifest_budget_bytes(manifest_path: Path) -> int:
        """Return a capped admission charge without reading the file body."""

        size = max(int(manifest_path.stat().st_size), 0)
        return min(size, MAX_RECOVERY_MANIFEST_BYTES + 1)

    def _bound_selected_paths_by_bytes(
        self,
        selected_paths: list[tuple[Path, int]],
        watermark_paths: list[tuple[Path, int]],
    ) -> tuple[list[tuple[Path, int]], list[tuple[Path, int]]]:
        if not selected_paths:
            return [], []
        watermark_set = {path for path, _mtime_ns in watermark_paths}
        bounded: list[tuple[Path, int]] = []
        bounded_watermark: list[tuple[Path, int]] = []
        admitted_bytes = 0
        for item in selected_paths:
            path, _mtime_ns = item
            try:
                manifest_bytes = self._manifest_budget_bytes(path)
            except OSError:
                # Let the ordinary scan path consume the stat/open race.  It
                # will not allocate a body and may safely advance this path's
                # watermark classification.
                manifest_bytes = 0
            if (
                bounded
                and admitted_bytes + manifest_bytes
                > MAX_RECOVERY_MANIFEST_BATCH_BYTES
            ):
                break
            bounded.append(item)
            if path in watermark_set:
                bounded_watermark.append(item)
            admitted_bytes += manifest_bytes
            if admitted_bytes >= MAX_RECOVERY_MANIFEST_BATCH_BYTES:
                break
        return bounded, bounded_watermark

    def _select_manifest_paths(
        self,
        incremental: bool,
    ) -> tuple[list[tuple[Path, int]], list[tuple[Path, int]]]:
        if not incremental:
            raise ValueError("full manifest scans use the bounded recovery spool")

        discovered_paths: dict[Path, int] = {}
        for directory in self._recent_manifest_directories():
            try:
                paths = list(directory.glob("*.json"))
            except OSError:
                continue
            for path in paths:
                try:
                    mtime_ns = self._manifest_mtime_ns(path)
                except OSError:
                    # A concurrently removed entry must not abort the rest of
                    # this directory.  Doing so and then advancing the cursor
                    # over already collected entries could permanently skip an
                    # older manifest that glob had not yielded yet.
                    continue
                discovered_paths[path] = mtime_ns

        retry_paths = self._periodic_retry_subset(discovered_paths)
        equal_watermark_paths = {
            path
            for path, mtime_ns in discovered_paths.items()
            if mtime_ns == self._periodic_watermark_ns and path not in retry_paths
        }
        seen_equal_paths = self._watermark_seen_subset(equal_watermark_paths)
        fresh_candidates = {
            path: mtime_ns
            for path, mtime_ns in discovered_paths.items()
            if path not in retry_paths
            and (
                mtime_ns > self._periodic_watermark_ns
                or (
                    mtime_ns == self._periodic_watermark_ns
                    and path not in seen_equal_paths
                )
            )
        }

        ordered_fresh = sorted(fresh_candidates.items(), key=lambda item: (item[1], str(item[0])))
        if not self._has_periodic_retries():
            fresh = ordered_fresh[:PERIODIC_MANIFEST_SCAN_BATCH_SIZE]
            return self._bound_selected_paths_by_bytes(fresh, fresh)

        if not ordered_fresh:
            retries = self._take_periodic_retries(PERIODIC_MANIFEST_SCAN_BATCH_SIZE)
            return self._bound_selected_paths_by_bytes(retries, [])

        retry_budget = max(1, PERIODIC_MANIFEST_SCAN_BATCH_SIZE // PERIODIC_MANIFEST_RETRY_SHARE)
        retries = self._take_periodic_retries(retry_budget)
        fresh_budget = PERIODIC_MANIFEST_SCAN_BATCH_SIZE - len(retries)
        fresh = ordered_fresh[:fresh_budget]

        # If the fresh side did not use the batch, let retries consume the
        # remainder.  The retry deque rotates, so a permanently bad legacy
        # receipt cannot monopolize every retry slot forever.
        if len(fresh) < fresh_budget:
            retries.extend(
                self._take_periodic_retries(
                    PERIODIC_MANIFEST_SCAN_BATCH_SIZE - len(fresh) - len(retries),
                    exclude={path for path, _mtime_ns in retries},
                )
            )
        return self._bound_selected_paths_by_bytes(fresh + retries, fresh)

    def _schedule_periodic_retry(self, path: Path) -> None:
        self._schedule_periodic_retries((path,))

    def _schedule_periodic_retries(self, paths: Iterable[Path]) -> None:
        rows: list[tuple[str, int]] = []
        for path in paths:
            self._periodic_retry_sequence += 1
            rows.append((str(path), self._periodic_retry_sequence))
        if not rows:
            return
        self._periodic_state.executemany(
            """
            INSERT OR IGNORE INTO periodic_retries (path, rotation_sequence)
            VALUES (?, ?)
            """,
            rows,
        )

    def _discard_periodic_retry(self, path: Path) -> None:
        self._periodic_state.execute(
            "DELETE FROM periodic_retries WHERE path = ?",
            (str(path),),
        )

    def _has_periodic_retry(self, path: Path) -> bool:
        return (
            self._periodic_state.execute(
                "SELECT 1 FROM periodic_retries WHERE path = ?",
                (str(path),),
            ).fetchone()
            is not None
        )

    def _has_periodic_retries(self) -> bool:
        return (
            self._periodic_state.execute("SELECT 1 FROM periodic_retries LIMIT 1").fetchone()
            is not None
        )

    def _periodic_retry_count(self) -> int:
        row = self._periodic_state.execute("SELECT COUNT(*) AS count FROM periodic_retries").fetchone()
        return int(row["count"])

    def _periodic_retry_subset(self, paths: Iterable[Path]) -> set[Path]:
        return self._periodic_path_subset("periodic_retries", paths)

    def _watermark_seen_subset(self, paths: Iterable[Path]) -> set[Path]:
        return self._periodic_path_subset("periodic_watermark_paths", paths)

    def _periodic_path_subset(self, table: str, paths: Iterable[Path]) -> set[Path]:
        path_strings = sorted({str(path) for path in paths})
        found: set[Path] = set()
        for index in range(0, len(path_strings), 500):
            chunk = path_strings[index : index + 500]
            placeholders = ", ".join("?" for _ in chunk)
            rows = self._periodic_state.execute(
                f"SELECT path FROM {table} WHERE path IN ({placeholders})",
                tuple(chunk),
            ).fetchall()
            found.update(Path(str(row["path"])) for row in rows)
        return found

    def _take_periodic_retries(
        self,
        limit: int,
        *,
        exclude: set[Path] | None = None,
    ) -> list[tuple[Path, int]]:
        if limit <= 0 or not self._has_periodic_retries():
            return []
        excluded = exclude or set()
        selected: list[tuple[Path, int]] = []
        inspected = 0
        queue_size = self._periodic_retry_count()
        while inspected < queue_size and len(selected) < limit:
            page_size = min(256, queue_size - inspected)
            rows = self._periodic_state.execute(
                """
                SELECT path
                FROM periodic_retries
                ORDER BY rotation_sequence ASC
                LIMIT ?
                """,
                (page_size,),
            ).fetchall()
            if not rows:
                break
            for row in rows:
                inspected += 1
                path = Path(str(row["path"]))
                try:
                    mtime_ns = self._manifest_mtime_ns(path)
                except OSError:
                    self._discard_periodic_retry(path)
                    continue
                self._periodic_retry_sequence += 1
                self._periodic_state.execute(
                    """
                    UPDATE periodic_retries
                    SET rotation_sequence = ?
                    WHERE path = ?
                    """,
                    (self._periodic_retry_sequence, str(path)),
                )
                if path in excluded:
                    continue
                selected.append((path, mtime_ns))
                if len(selected) >= limit:
                    break
        return selected

    def _recent_manifest_directories(self) -> list[Path]:
        root = self.runtime.settings.manifests_dir
        directories: set[Path] = {root}
        for year in self._latest_child_directories(root, PERIODIC_MANIFEST_RECENT_YEAR_COUNT):
            for month in self._latest_child_directories(year, PERIODIC_MANIFEST_RECENT_MONTH_COUNT):
                directories.update(
                    self._latest_child_directories(month, PERIODIC_MANIFEST_RECENT_DAY_COUNT)
                )
        return sorted(directories)

    def _latest_child_directories(self, parent: Path, limit: int) -> list[Path]:
        try:
            children = [path for path in parent.iterdir() if path.is_dir()]
        except OSError:
            return []
        return sorted(children, key=lambda path: path.name, reverse=True)[:limit]

    def _manifest_mtime_ns(self, manifest_path: Path) -> int:
        return int(manifest_path.stat().st_mtime_ns)

    def _relative_storage_path(self, path: Path) -> str:
        root = self.runtime.settings.storage_root.resolve(strict=False)
        return path.resolve(strict=False).relative_to(root).as_posix()

    def _file_gc_storage_paths(self, storage_paths: set[str]) -> set[str]:
        if not storage_paths:
            return set()
        retired: set[str] = set()
        ordered_paths = sorted(storage_paths)
        with connect_database(self.runtime.settings.database_path) as connection:
            for index in range(0, len(ordered_paths), 500):
                chunk = ordered_paths[index : index + 500]
                placeholders = ", ".join("?" for _ in chunk)
                rows = connection.execute(
                    f"SELECT storage_path FROM file_gc_tasks WHERE storage_path IN ({placeholders})",
                    tuple(chunk),
                ).fetchall()
                retired.update(str(row["storage_path"]) for row in rows)
        return retired

    def _advance_periodic_watermark(self, selected_paths: list[tuple[Path, int]]) -> None:
        if not selected_paths:
            return
        latest_mtime = max(mtime_ns for _, mtime_ns in selected_paths)
        latest_paths = {str(path) for path, mtime_ns in selected_paths if mtime_ns == latest_mtime}
        if latest_mtime > self._periodic_watermark_ns:
            self._periodic_watermark_ns = latest_mtime
            self._periodic_state.execute("DELETE FROM periodic_watermark_paths")
            self._periodic_state.executemany(
                "INSERT OR IGNORE INTO periodic_watermark_paths (path) VALUES (?)",
                ((path,) for path in latest_paths),
            )
        elif latest_mtime == self._periodic_watermark_ns:
            self._periodic_state.executemany(
                "INSERT OR IGNORE INTO periodic_watermark_paths (path) VALUES (?)",
                ((path,) for path in latest_paths),
            )

    def _advance_full_periodic_watermark(self, latest_mtime: int) -> None:
        self._periodic_watermark_ns = latest_mtime
        self._periodic_state.execute("DELETE FROM periodic_watermark_paths")
        if latest_mtime >= 0:
            self._periodic_state.execute(
                """
                INSERT OR IGNORE INTO periodic_watermark_paths (path)
                SELECT path FROM full_scan_watermark_paths
                """
            )
        self._periodic_state.execute("DELETE FROM full_scan_watermark_paths")

    def _reset_full_scan_watermark_paths(self) -> None:
        self._full_scan_staged_watermark_ns = -1
        self._periodic_state.execute("DELETE FROM full_scan_watermark_paths")

    def _record_full_scan_watermark_paths(
        self,
        selected_paths: list[tuple[Path, int]],
        latest_mtime: int,
    ) -> None:
        if latest_mtime < 0:
            return
        if latest_mtime > self._full_scan_staged_watermark_ns:
            self._periodic_state.execute("DELETE FROM full_scan_watermark_paths")
            self._full_scan_staged_watermark_ns = latest_mtime
        elif latest_mtime < self._full_scan_staged_watermark_ns:
            return
        paths = {
            str(path)
            for path, mtime_ns in selected_paths
            if mtime_ns == latest_mtime
        }
        if not paths:
            return
        self._periodic_state.executemany(
            "INSERT OR IGNORE INTO full_scan_watermark_paths (path) VALUES (?)",
            ((path,) for path in paths),
        )

    def _complete_message_ids_subset(self, message_ids: set[str]) -> set[str]:
        if not message_ids:
            return set()
        complete: set[str] = set()
        ordered_ids = sorted(message_ids)
        with connect_database(self.runtime.settings.database_path) as connection:
            for index in range(0, len(ordered_ids), 500):
                chunk = ordered_ids[index : index + 500]
                placeholders = ", ".join("?" for _ in chunk)
                rows = connection.execute(
                    f"""
                    SELECT m.id
                    FROM messages AS m
                    WHERE m.id IN ({placeholders})
                      AND EXISTS (
                          SELECT 1 FROM message_deliveries AS d WHERE d.message_id = m.id
                      )
                    """,
                    tuple(chunk),
                ).fetchall()
                complete.update(str(row["id"]) for row in rows)
        return complete

    async def _requeue_unparsed_messages(self, *, max_rowid: int) -> None:
        after_rowid = 0
        while after_rowid < max_rowid:
            tasks, next_rowid = await self.runtime.find_failed_reparse_page(
                after_rowid=after_rowid,
                max_rowid=max_rowid,
                limit=FAILED_REPARSE_BATCH_SIZE,
            )
            if not tasks:
                break
            for task in tasks:
                # Failed startup work is not covered by the ordinary pending
                # scanner.  Wait for bounded queue capacity so no historical
                # failure is silently dropped when a page is larger than the
                # current message/byte budget.
                await self.runtime.enqueue_recovery_parse_task(task)
            if next_rowid is None or next_rowid <= after_rowid:
                break
            after_rowid = next_rowid

    def _validate_raw_artifact(self, manifest: dict[str, object]) -> None:
        raw_path = self.runtime.storage.resolve(str(manifest["raw_path"]))
        stat = raw_path.stat()
        expected_size = int(manifest["raw_size_bytes"])
        if stat.st_size != expected_size:
            raise ValueError("recovery raw size mismatch")
        digest = hashlib.sha256()
        with raw_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != str(manifest["raw_sha256"]).lower():
            raise ValueError("recovery raw sha256 mismatch")

    def _quarantine_bad_manifest(self, manifest_path, exc: Exception) -> None:
        quarantine_dir = self.runtime.settings.storage_root / "quarantine" / "manifests"
        try:
            quarantine_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            target = quarantine_dir / manifest_path.name
            if target.exists():
                target = quarantine_dir / (
                    f"{manifest_path.stem}-{manifest_path.stat().st_mtime_ns}-{uuid4().hex}.json"
                )
            shutil.move(str(manifest_path), str(target))
        except OSError:
            pass
        policy_conflict = isinstance(exc, RecoveryPolicyConflictError)
        logging.getLogger("rapid_inbox.recovery").error(
            (
                "recovery manifest policy conflict quarantined"
                if policy_conflict
                else "invalid recovery manifest quarantined"
            ),
            extra={
                "event": (
                    "recovery.manifest_policy_conflict"
                    if policy_conflict
                    else "recovery.manifest_invalid"
                ),
                "manifest": str(manifest_path),
                "error": str(exc),
            },
        )

    def _has_domain_policy(self, manifest: dict[str, object]) -> bool:
        recipients = manifest.get("recipients")
        if not isinstance(recipients, list) or not recipients:
            return False
        return any(isinstance(recipient, dict) and recipient.get("domain_policy") is not None for recipient in recipients)

    def _record_latest_policy_snapshots(
        self,
        latest_policy_snapshots: dict[str, dict[str, object]],
        manifest_path,
        manifest: dict[str, object],
    ) -> None:
        recovery_order = self._manifest_recovery_order(manifest_path, manifest)
        for recipient in manifest["recipients"]:
            if not isinstance(recipient, dict):
                continue
            domain_policy = recipient.get("domain_policy")
            if domain_policy is None:
                continue
            domain_id = int(recipient["domain_id"])
            root_domain_ascii = str(recipient["root_domain_ascii"])
            snapshot = {
                "domain_id": domain_id,
                "root_domain_ascii": root_domain_ascii,
                "received_at": str(manifest["received_at"]),
                "domain_policy": domain_policy,
                "_recovery_order": recovery_order,
            }
            current = latest_policy_snapshots.get(root_domain_ascii)
            if current is None or self._recovery_order_key(current) <= recovery_order:
                latest_policy_snapshots[root_domain_ascii] = snapshot

    def _sorted_snapshots(self, latest_policy_snapshots: dict[str, dict[str, object]]) -> list[dict[str, object]]:
        return sorted(
            latest_policy_snapshots.values(),
            key=lambda snapshot: (*self._recovery_order_key(snapshot), int(snapshot["domain_id"])),
        )

    def _recovery_order_key(self, snapshot: dict[str, object]) -> tuple[int, int]:
        order = snapshot["_recovery_order"]
        if not isinstance(order, tuple) or len(order) != 2:
            return (0, 0)
        return (int(order[0]), int(order[1]))

    def _manifest_recovery_order(self, manifest_path, manifest: dict[str, object]) -> tuple[int, int]:
        try:
            mtime_ns = manifest_path.stat().st_mtime_ns
        except OSError as exc:
            raise ValueError("invalid recovery manifest") from exc

        return self._manifest_recovery_order_from_mtime(mtime_ns, manifest)

    def _manifest_recovery_order_from_mtime(
        self,
        mtime_ns: int,
        manifest: dict[str, object],
    ) -> tuple[int, int]:
        recovery_order_ns = manifest.get("recovery_order_ns")
        if recovery_order_ns is None:
            return (mtime_ns, mtime_ns)
        if not isinstance(recovery_order_ns, int) or isinstance(recovery_order_ns, bool) or recovery_order_ns < 0:
            raise ValueError("invalid recovery manifest")
        return (recovery_order_ns, mtime_ns)
