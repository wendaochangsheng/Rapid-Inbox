from __future__ import annotations

import hashlib
import asyncio
import json
import logging
import os
import sqlite3
import stat
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any
from time import monotonic, time_ns
from pathlib import Path

from app.auth import AuthService
from app.auth.api_keys import ApiKeyService, get_active_permission_context
from app.auth.permissions import PermissionContext, ensure_mailbox_access
from app.config import DEFAULT_BOOTSTRAP_ADMIN_PASSWORD, Settings
from app.db.connection import connect_database, initialize_database
from app.db.read_pool import SQLiteReadPool
from app.db.writer import DatabaseWriter
from app.ingest.parser import MessageParser, ParsedMessage
from app.ingest.recovery import RecoveryPolicyConflictError, RecoveryScanner
from app.ingest.queue import ParseQueue, ParseTask
from app.ingest.storage import FileStorage, utc_now
from app.observability import Observability
from app.services.audit import AuditService
from app.services.domains import (
    DomainService,
    domain_routing_tombstone_key,
    match_active_domain,
    promote_mailbox_ownership,
)
from app.services.mailboxes import MailboxService
from app.services.messages import MessageService
from app.services.settings import SettingsService
from app.smtp.live_state import LiveState
from app.smtp.matcher import DomainMatch, DomainMatcher, DomainRule


MESSAGE_RETENTION_CLEANUP_INTERVAL_SECONDS = 30
PENDING_PARSE_SCAN_INTERVAL_SECONDS = 0.5
PENDING_PARSE_SCAN_BATCH_SIZE = 5000
MANIFEST_RECOVERY_SCAN_INTERVAL_SECONDS = 10.0
MAX_MANIFEST_SWEEP_BYTES = 16 * 1024 * 1024
SMTP_IP_RATE_STATE_MIN_ENTRIES = 1024
SMTP_IP_RATE_STATE_MAX_ENTRIES = 65_536


_maintenance_logger = logging.getLogger("rapid_inbox.maintenance")


class _RecipientPolicyChangedError(RuntimeError):
    pass


class RapidInboxRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.observability = Observability(settings)
        self._started = False
        self._stopping = False
        self._stopped_permanently = False
        self._legacy_public_api_key = settings.public_api_key
        self.storage = FileStorage(settings)
        self.writer = DatabaseWriter(
            settings.database_path,
            queue_capacity=settings.database_write_queue_capacity,
            max_waiters=settings.database_write_max_waiters,
        )
        self.read_pool = SQLiteReadPool(
            settings.database_path,
            max_connections=settings.database_read_pool_size,
            queue_capacity=settings.database_read_queue_capacity,
            max_waiters=settings.database_read_max_waiters,
            timeout_seconds=settings.database_read_timeout_seconds,
        )
        self.api_keys = ApiKeyService(settings.database_path, self.writer)
        self.auth = AuthService(settings, self.writer, self.api_keys)
        self.domains = DomainService(settings.database_path, self.writer, self.api_keys)
        self.mailboxes = MailboxService(self)
        self.messages = MessageService(self)
        self.audit = AuditService(self)
        self.system_settings = SettingsService(self)
        self.parser = MessageParser(self.storage)
        self.parse_queue = ParseQueue(
            self._parse_message,
            worker_count=settings.parse_worker_count,
            max_messages=settings.parse_queue_max_messages,
            max_bytes=settings.parse_queue_max_bytes,
        )
        self._mail_store_lock = asyncio.Lock()
        self._mail_maintenance_condition = asyncio.Condition()
        self._mail_maintenance_active = False
        self._active_mail_accepts = 0
        self._active_mail_accept_message_lock = RLock()
        self._active_mail_accept_message_ids: set[str] = set()
        self._smtp_connection_lock = RLock()
        self._active_smtp_connections: dict[str, str] = {}
        # Access-order LRU bounds rotating-source state. A second ordered set
        # tracks last accepted timestamps, allowing expiry to stop at the first
        # fresh bucket instead of scanning every IPv4/IPv6 source per accept.
        self._smtp_ip_windows: OrderedDict[str, deque[float]] = OrderedDict()
        self._smtp_ip_expiry_order: OrderedDict[str, None] = OrderedDict()
        self._retention_cleanup_task: asyncio.Task[None] | None = None
        self._pending_parse_scan_task: asyncio.Task[None] | None = None
        self._pending_parse_scan_cursor: tuple[str, str] | None = None
        self._last_manifest_recovery_at = monotonic()
        self._artifact_sweep_iterators: dict[
            str,
            tuple[tuple[str, ...], float, Any],
        ] = {}
        self.live_state = LiveState()
        self.recovery = RecoveryScanner(self)

    async def start(self) -> None:
        if self._stopped_permanently:
            raise RuntimeError("RapidInboxRuntime cannot be restarted after stop()")
        self._stopping = False
        self.settings.ensure_directories()
        self.storage.cleanup_abandoned_clear_trash()
        self.storage.cleanup_stale_parts()
        initialize_database(self.settings.database_path)
        self.read_pool.start()
        interrupted_runs = await self.writer.execute(self._mark_interrupted_maintenance_runs_failed)
        if interrupted_runs:
            _maintenance_logger.warning(
                "interrupted maintenance runs marked failed",
                extra={
                    "event": "maintenance_interrupted_recovered",
                    "task": "startup_recovery",
                    "outcome": "failed",
                    "count": interrupted_runs,
                },
            )
        self._assert_runtime_secrets_are_safe()
        await self.auth.ensure_bootstrap_admin()
        await self.system_settings.load_persisted_settings()
        await self.writer.execute(self._mark_orphaned_smtp_sessions_closed)
        # Swap the plain config token for a string-like proxy that can validate DB-backed keys too.
        self.settings.public_api_key = self.api_keys.configure_legacy_public_api_key(
            self._legacy_public_api_key,
            enabled=self.settings.legacy_public_api_key_enabled,
        )
        await self.mailboxes.resume_incomplete_bulk_delete_jobs()
        await self.parse_queue.start()
        await self.recovery.run()
        self.domains.reload()
        self.observability.background.register("message_retention")
        self.observability.background.register("pending_parse_scan")
        self._retention_cleanup_task = asyncio.create_task(self._message_retention_loop())
        self._pending_parse_scan_task = asyncio.create_task(self._pending_parse_scan_loop())
        self._started = True

    async def stop(self) -> None:
        """Permanently stop this runtime and drain its database writer."""

        self._stopping = True
        try:
            try:
                await self._stop_pending_parse_scan_loop()
            except asyncio.CancelledError:
                pass
            try:
                await self._stop_message_retention_loop()
            except asyncio.CancelledError:
                pass
            try:
                await self.parse_queue.stop(discard_pending=True, timeout=5.0)
            except asyncio.CancelledError:
                pass
            try:
                await self.mailboxes.close()
            except asyncio.CancelledError:
                pass
            try:
                await self.recovery.close()
            except asyncio.CancelledError:
                pass
            with self._smtp_connection_lock:
                self._active_smtp_connections.clear()
                self._smtp_ip_windows.clear()
                self._smtp_ip_expiry_order.clear()
        finally:
            self._started = False
            self._stopped_permanently = True
            try:
                self.observability.background.stop("message_retention")
                self.observability.background.stop("pending_parse_scan")
            finally:
                close_error: BaseException | None = None
                # Each resource is drained even if an earlier close reports an
                # actor failure. This prevents a failed reader from stranding
                # the writer's non-daemon ownership or pending transactions.
                for close_operation in (
                    self.auth.close,
                    self.read_pool.close,
                    self.writer.close,
                ):
                    try:
                        await self._finish_close_uncancellable(close_operation())
                    except BaseException as exc:
                        if close_error is None:
                            close_error = exc
                if close_error is not None:
                    raise close_error

    @staticmethod
    async def _finish_close_uncancellable(operation: Any) -> Any:
        close_task = asyncio.create_task(operation)
        while True:
            try:
                return await asyncio.shield(close_task)
            except asyncio.CancelledError:
                if close_task.done():
                    return await close_task
                continue

    def operational_state(self) -> dict[str, Any]:
        retention_running = self._retention_cleanup_task is not None and not self._retention_cleanup_task.done()
        pending_scan_running = self._pending_parse_scan_task is not None and not self._pending_parse_scan_task.done()
        parse_queue_running = self.parse_queue.is_running
        read_pool_state = self.read_pool.operational_state()
        ok = bool(
            self._started
            and not self._stopping
            and retention_running
            and pending_scan_running
            and parse_queue_running
            and read_pool_state["ok"]
        )
        return {
            "ok": ok,
            "started": self._started,
            "stopping": self._stopping,
            "parse_queue_running": parse_queue_running,
            "database_read_pool": read_pool_state,
            "tasks": {
                "message_retention": retention_running,
                "pending_parse_scan": pending_scan_running,
            },
        }

    async def create_domain(self, root_domain: str, **kwargs: Any) -> dict[str, Any]:
        return await self.domains.create_domain(root_domain, **kwargs)

    def list_domains(self) -> list[dict[str, Any]]:
        return self.domains.list_domains()

    async def reparse_message(
        self,
        message_id: str,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> None:
        await self.messages.reparse_message(
            message_id,
            authorization_principal=authorization_principal,
        )

    def get_settings(self) -> dict[str, Any]:
        return self.system_settings.get_settings()

    async def update_settings(
        self,
        payload: dict[str, Any],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        return await self.system_settings.update_settings(
            payload,
            authorization_principal=authorization_principal,
        )

    def apply_live_settings(self, updates: dict[str, Any]) -> None:
        for key, value in updates.items():
            if hasattr(self.settings, key):
                setattr(self.settings, key, value)

    async def clear_all_mail(
        self,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        return await self._run_maintenance(
            "clear_all_mail",
            lambda: self._clear_all_mail_operation(
                authorization_principal=authorization_principal,
            ),
            authorization_principal=authorization_principal,
            required_scope="system.write",
            require_global=True,
        )

    async def _clear_all_mail_operation(
        self,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        await self._begin_mail_maintenance()
        try:
            maintenance_lock = await asyncio.to_thread(self.storage.begin_maintenance, "clear-mail")
            try:
                await asyncio.to_thread(
                    self.storage.wait_for_ingestd_drain,
                    maintenance_lock,
                )
                await self.read_pool.pause_and_drain()
                try:
                    async with self._mail_store_lock:
                        dropped_parse_tasks = self.parse_queue.clear_pending()
                        await self.parse_queue.stop()
                        try:
                            result = await self.writer.execute(
                                lambda connection: self._clear_mail_tables(
                                    connection,
                                    authorization_principal=authorization_principal,
                                )
                            )
                            result["moved_storage_directories"] = await asyncio.to_thread(
                                self.storage.clear_mail_data
                            )
                            self._reset_artifact_sweep_iterators()
                            self.live_state.clear()
                            try:
                                result.update(
                                    await self.writer.execute_maintenance(
                                        self._compact_mail_database
                                    )
                                )
                            except sqlite3.Error as exc:
                                result["database_compaction_failed"] = 1
                                result["database_compaction_error"] = str(exc)
                            result["dropped_parse_tasks"] = dropped_parse_tasks
                            return result
                        finally:
                            await self.parse_queue.start()
                finally:
                    self.read_pool.resume()
            finally:
                await asyncio.to_thread(self.storage.end_maintenance, maintenance_lock)
        finally:
            await self._end_mail_maintenance()

    async def _begin_mail_maintenance(self) -> None:
        async with self._mail_maintenance_condition:
            while self._mail_maintenance_active:
                await self._mail_maintenance_condition.wait()
            self._mail_maintenance_active = True
            try:
                while self._active_mail_accepts:
                    await self._mail_maintenance_condition.wait()
            except BaseException:
                self._mail_maintenance_active = False
                self._mail_maintenance_condition.notify_all()
                raise

    async def _end_mail_maintenance(self) -> None:
        async with self._mail_maintenance_condition:
            self._mail_maintenance_active = False
            self._mail_maintenance_condition.notify_all()

    async def _enter_mail_accept(self) -> None:
        async with self._mail_maintenance_condition:
            while self._mail_maintenance_active:
                await self._mail_maintenance_condition.wait()
            self._active_mail_accepts += 1

    async def _leave_mail_accept(self) -> None:
        async with self._mail_maintenance_condition:
            if self._active_mail_accepts <= 0:
                raise RuntimeError("mail accept counter underflow")
            self._active_mail_accepts -= 1
            if self._active_mail_accepts == 0:
                self._mail_maintenance_condition.notify_all()

    async def cleanup_expired_messages(
        self,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, int]:
        operation: Any = self._cleanup_expired_messages_operation
        if authorization_principal is not None:
            operation = lambda: self._cleanup_expired_messages_operation(
                authorization_principal=authorization_principal,
            )
        return await self._run_maintenance(
            "retention_cleanup",
            operation,
            authorization_principal=authorization_principal,
            required_scope="system.write",
            require_global=True,
        )

    async def _cleanup_expired_messages_operation(
        self,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, int]:
        now = utc_now()
        batch_size = int(self.settings.cleanup_batch_size)
        async with self._mail_store_lock:
            # Select only after entering the recovery/retention critical
            # section.  Otherwise recovery could recreate an already-expired
            # message between this read and the delete transaction, enqueue a
            # parser task that the stale candidate set would fail to drain, and
            # race file GC.
            delivery_batch, candidate_message_ids = await asyncio.to_thread(
                self._expiration_batch_snapshot,
                now,
                batch_size,
            )
            dropped_parse_tasks = 0
            if candidate_message_ids:
                expired_message_id_set = set(candidate_message_ids)
                dropped_parse_tasks = self.parse_queue.remove_pending(
                    lambda task: task.message_id in expired_message_id_set
                )
                await self.parse_queue.wait_until_not_active(
                    lambda message_id: message_id in expired_message_id_set
                )

            result = await self.writer.execute(
                lambda connection: self._expire_delivery_batch(
                    connection,
                    now,
                    batch_size,
                    delivery_batch,
                    authorization_principal=authorization_principal,
                )
            )
            # Recovery uses the same lock.  Keep tombstone creation and physical
            # deletion in one linearized region so it cannot hash or replay a
            # raw/manifest pair while file GC is removing it.
            gc_result = await self._drain_file_gc(int(self.settings.file_gc_batch_size))
            artifact_result = await asyncio.to_thread(self._sweep_retained_artifacts, now)
        result.update(gc_result)
        result.update(artifact_result)
        result["files"] = (
            gc_result["file_gc_deleted"]
            + artifact_result["quarantine_files_deleted"]
            + artifact_result["orphan_artifacts_deleted"]
        )
        result["dropped_parse_tasks"] = dropped_parse_tasks
        return {key: int(value) for key, value in result.items()}

    async def _run_maintenance(
        self,
        kind: str,
        operation: Any,
        *,
        authorization_principal: PermissionContext | None = None,
        required_scope: str | None = None,
        require_global: bool = False,
    ) -> Any:
        run_id = f"mnt_{uuid.uuid4().hex}"
        started_at = utc_now()
        started_monotonic = monotonic()

        def start_run(connection: sqlite3.Connection) -> None:
            if required_scope is not None:
                connection.execute("BEGIN IMMEDIATE")
                self.api_keys.transaction_authorization_principal(
                    connection,
                    authorization_principal,
                    required_scope=required_scope,
                    require_global=require_global,
                )
            connection.execute(
                """
                INSERT INTO maintenance_runs (id, kind, status, started_at)
                VALUES (?, ?, 'running', ?)
                """,
                (run_id, kind, started_at),
            )

        await self.writer.execute(start_run)
        try:
            result = await operation()
        except Exception as exc:
            await self._finish_maintenance_run(
                run_id,
                status="failed",
                error=f"{exc.__class__.__name__}: {exc}"[:2000],
            )
            _maintenance_logger.exception(
                "maintenance operation failed",
                extra={
                    "event": "maintenance_failed",
                    "task": kind,
                    "outcome": "failure",
                    "error_type": exc.__class__.__name__,
                    "duration_ms": round((monotonic() - started_monotonic) * 1000, 3),
                },
            )
            raise

        details = result if isinstance(result, dict) else {"result": result}
        await self._finish_maintenance_run(run_id, status="succeeded", details=details)
        _maintenance_logger.info(
            "maintenance operation completed",
            extra={
                "event": "maintenance_completed",
                "task": kind,
                "outcome": "success",
                "duration_ms": round((monotonic() - started_monotonic) * 1000, 3),
            },
        )
        return result

    async def _finish_maintenance_run(
        self,
        run_id: str,
        *,
        status: str,
        details: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        finished_at = utc_now()
        details_json = (
            json.dumps(details, ensure_ascii=False, sort_keys=True, default=str)
            if details is not None
            else None
        )

        def finish_run(connection: sqlite3.Connection) -> None:
            cursor = connection.execute(
                """
                UPDATE maintenance_runs
                SET status = ?, finished_at = ?, details_json = ?, error = ?
                WHERE id = ? AND status = 'running'
                """,
                (status, finished_at, details_json, error, run_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("maintenance run is missing or already finished")

        await self.writer.execute(finish_run)

    async def _message_retention_loop(self) -> None:
        interval = int(self.settings.retention_cleanup_interval_seconds)
        if interval == 30:
            interval = MESSAGE_RETENTION_CLEANUP_INTERVAL_SECONDS
        await self.observability.run_periodic(
            "message_retention",
            interval,
            self.cleanup_expired_messages,
        )

    async def _stop_message_retention_loop(self) -> None:
        task = self._retention_cleanup_task
        if task is None:
            return
        self._retention_cleanup_task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _pending_parse_scan_loop(self) -> None:
        await self.observability.run_periodic(
            "pending_parse_scan",
            PENDING_PARSE_SCAN_INTERVAL_SECONDS,
            self.requeue_pending_messages_for_parse,
        )

    async def _stop_pending_parse_scan_loop(self) -> None:
        task = self._pending_parse_scan_task
        if task is None:
            return
        self._pending_parse_scan_task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def _cutoff_timestamp(self, *, seconds: int = 0, days: int = 0) -> str:
        current = datetime.fromisoformat(utc_now().replace("Z", "+00:00"))
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        cutoff = current.astimezone(timezone.utc).replace(microsecond=0) - timedelta(
            seconds=seconds,
            days=days,
        )
        return cutoff.isoformat().replace("+00:00", "Z")

    def _expiration_batch_snapshot(
        self,
        now: str,
        limit: int,
    ) -> tuple[list[tuple[str, str, int, str]], list[str]]:
        """Freeze the exact delivery batch and messages it can orphan.

        Parser coordination must use the same delivery IDs as the following
        delete transaction.  Selecting messages and deliveries independently
        with different sort orders can otherwise miss a message that becomes
        orphaned by this batch.  The writer revalidates every snapshotted row,
        so expiry changes queued between this read and the transaction remain
        fail-closed.
        """

        with connect_database(self.settings.database_path) as connection:
            rows = connection.execute(
                """
                WITH due_batch AS (
                    SELECT id, message_id, mailbox_id, status, expires_at
                    FROM message_deliveries
                    WHERE expires_at IS NOT NULL AND expires_at <= ?
                    ORDER BY expires_at ASC, id ASC
                    LIMIT ?
                ),
                fully_expiring AS (
                    SELECT batch.message_id
                    FROM due_batch AS batch
                    GROUP BY batch.message_id
                    HAVING COUNT(*) = (
                        SELECT COUNT(*)
                        FROM message_deliveries AS all_deliveries
                        WHERE all_deliveries.message_id = batch.message_id
                    )
                )
                SELECT
                    batch.id,
                    batch.message_id,
                    batch.mailbox_id,
                    batch.status,
                    CASE WHEN fully.message_id IS NULL THEN 0 ELSE 1 END AS fully_expiring
                FROM due_batch AS batch
                LEFT JOIN fully_expiring AS fully ON fully.message_id = batch.message_id
                ORDER BY batch.expires_at ASC, batch.id ASC
                """,
                (now, limit),
            ).fetchall()
        delivery_batch = [
            (
                str(row["id"]),
                str(row["message_id"]),
                int(row["mailbox_id"]),
                str(row["status"]),
            )
            for row in rows
        ]
        candidate_message_ids = sorted(
            {
                str(row["message_id"])
                for row in rows
                if int(row["fully_expiring"]) == 1
            }
        )
        return delivery_batch, candidate_message_ids

    def _mark_interrupted_maintenance_runs_failed(self, connection: sqlite3.Connection) -> int:
        now = utc_now()
        cursor = connection.execute(
            """
            UPDATE maintenance_runs
            SET status = 'failed',
                finished_at = ?,
                error = COALESCE(error, 'runtime restarted before maintenance completed')
            WHERE status = 'running'
            """,
            (now,),
        )
        return int(cursor.rowcount or 0)

    def _expire_delivery_batch(
        self,
        connection: sqlite3.Connection,
        now: str,
        limit: int,
        delivery_batch: list[tuple[str, str, int, str]],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, int]:
        connection.execute("BEGIN IMMEDIATE")
        self.api_keys.transaction_authorization_principal(
            connection,
            authorization_principal,
            required_scope="system.write",
            require_global=True,
        )
        result = self._empty_retention_result()
        self._prepare_retention_temp_tables(connection)
        if delivery_batch:
            connection.executemany(
                """
                INSERT INTO _retention_delivery_batch (
                    id, message_id, mailbox_id, status
                ) VALUES (?, ?, ?, ?)
                """,
                delivery_batch,
            )

        # A session or API mutation may have changed a snapshotted delivery
        # before this writer request acquired the transaction.  Only rows that
        # still match both status and expiry are eligible for deletion.
        connection.execute(
            """
            DELETE FROM _retention_delivery_batch
            WHERE NOT EXISTS (
                SELECT 1
                FROM message_deliveries AS delivery
                WHERE delivery.id = _retention_delivery_batch.id
                  AND delivery.status = _retention_delivery_batch.status
                  AND delivery.expires_at IS NOT NULL
                  AND delivery.expires_at <= ?
            )
            """,
            (now,),
        )
        delivery_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM _retention_delivery_batch"
            ).fetchone()["count"]
        )
        if delivery_count:
            connection.execute(
                """
                DELETE FROM message_deliveries
                WHERE id IN (SELECT id FROM _retention_delivery_batch)
                """
            )
            result["deliveries"] = delivery_count

        connection.execute(
            """
            INSERT OR IGNORE INTO _retention_orphan_message_batch (message_id)
            SELECT batch.message_id
            FROM _retention_delivery_batch AS batch
            WHERE NOT EXISTS (
                SELECT 1
                FROM message_deliveries AS delivery
                WHERE delivery.message_id = batch.message_id
            )
            GROUP BY batch.message_id
            """
        )
        orphan_count = int(
            connection.execute(
                "SELECT COUNT(*) AS count FROM _retention_orphan_message_batch"
            ).fetchone()["count"]
        )
        if orphan_count:
            self._queue_orphan_message_files(connection, now, result)
            deleted = connection.execute(
                """
                DELETE FROM messages
                WHERE id IN (
                    SELECT message_id FROM _retention_orphan_message_batch
                )
                """
            )
            result["messages"] = int(deleted.rowcount or 0)

        if delivery_count:
            # The parser wait can be long.  Empty-mailbox aging starts when
            # the delivery is actually removed, not at the earlier snapshot.
            self._refresh_retention_mailbox_summaries(connection, utc_now())

        result["mailboxes"] += self._delete_stale_empty_mailbox_batch(connection, limit)
        result["smtp_sessions"] = self._delete_stale_smtp_session_batch(connection, limit)
        result["metric_buckets"] = self._delete_old_metric_batch(connection, limit)
        result["audit_logs"] = self._delete_old_audit_batch(connection, limit)
        result["maintenance_runs"] = self._delete_old_maintenance_run_batch(connection, limit)
        result["file_gc_pending"] = int(
            connection.execute("SELECT COUNT(*) AS count FROM file_gc_tasks").fetchone()["count"]
        )
        self._clear_retention_temp_tables(connection)
        return result

    @staticmethod
    def _prepare_retention_temp_tables(connection: sqlite3.Connection) -> None:
        """Create and reset writer-local batch tables.

        The writer owns one persistent SQLite connection.  Resetting at both
        transaction boundaries makes a successful previous batch invisible to
        the next request, while rollback restores the already-empty prior
        state after a failed request.
        """

        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _retention_delivery_batch (
                id TEXT PRIMARY KEY,
                message_id TEXT NOT NULL,
                mailbox_id INTEGER NOT NULL,
                status TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _retention_orphan_message_batch (
                message_id TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )
        connection.execute("DELETE FROM _retention_delivery_batch")
        connection.execute("DELETE FROM _retention_orphan_message_batch")

    @staticmethod
    def _clear_retention_temp_tables(connection: sqlite3.Connection) -> None:
        connection.execute("DELETE FROM _retention_delivery_batch")
        connection.execute("DELETE FROM _retention_orphan_message_batch")

    @staticmethod
    def _refresh_retention_mailbox_summaries(
        connection: sqlite3.Connection,
        now: str,
    ) -> None:
        # One correlated aggregate per affected mailbox, executed by SQLite in
        # a single statement.  This replaces two Python/SQLite round trips per
        # mailbox and keeps the exact active-delivery summary semantics.
        connection.execute(
            """
            UPDATE mailboxes
            SET (
                first_seen_at,
                last_seen_at,
                latest_message_at,
                message_count
            ) = (
                SELECT
                    CASE
                        WHEN COUNT(delivery.id) = 0 THEN mailboxes.first_seen_at
                        ELSE MIN(delivery.delivered_at)
                    END,
                    CASE
                        WHEN COUNT(delivery.id) = 0 THEN ?
                        ELSE MAX(delivery.delivered_at)
                    END,
                    MAX(delivery.delivered_at),
                    COUNT(delivery.id)
                FROM message_deliveries AS delivery
                WHERE delivery.mailbox_id = mailboxes.id
                  AND delivery.status = 'active'
            )
            WHERE id IN (
                SELECT mailbox_id FROM _retention_delivery_batch
            )
            """,
            (now,),
        )

    @staticmethod
    def _refresh_mailbox_summary_after_message_delete(
        connection: sqlite3.Connection,
        mailbox_id: int,
    ) -> bool:
        """Refresh one mailbox for non-retention service mutations."""

        summary = connection.execute(
            """
            SELECT
                COUNT(*) AS message_count,
                MIN(delivered_at) AS first_seen_at,
                MAX(delivered_at) AS latest_message_at
            FROM message_deliveries
            WHERE mailbox_id = ? AND status = 'active'
            """,
            (mailbox_id,),
        ).fetchone()
        message_count = int(summary["message_count"])
        if message_count == 0:
            connection.execute(
                """
                UPDATE mailboxes
                SET latest_message_at = NULL,
                    message_count = 0,
                    last_seen_at = ?
                WHERE id = ?
                """,
                (utc_now(), mailbox_id),
            )
            return False

        connection.execute(
            """
            UPDATE mailboxes
            SET first_seen_at = ?,
                last_seen_at = ?,
                latest_message_at = ?,
                message_count = ?
            WHERE id = ?
            """,
            (
                summary["first_seen_at"],
                summary["latest_message_at"],
                summary["latest_message_at"],
                message_count,
                mailbox_id,
            ),
        )
        return False

    def _queue_orphan_message_files(
        self,
        connection: sqlite3.Connection,
        now: str,
        result: dict[str, int],
    ) -> None:
        message_rows = connection.execute(
            """
            SELECT
                message.id,
                message.raw_path,
                message.raw_size_bytes,
                message.received_at,
                message.text_body_path,
                message.html_body_path
            FROM messages AS message
            JOIN _retention_orphan_message_batch AS batch
              ON batch.message_id = message.id
            """
        ).fetchall()
        attachment_rows = connection.execute(
            """
            SELECT attachment.storage_path
            FROM attachments AS attachment
            JOIN _retention_orphan_message_batch AS batch
              ON batch.message_id = attachment.message_id
            """
        ).fetchall()
        paths: set[str] = {str(row["storage_path"]) for row in attachment_rows}
        for row in message_rows:
            result["raw_size_bytes"] += int(row["raw_size_bytes"])
            for value in (
                row["raw_path"],
                row["text_body_path"],
                row["html_body_path"],
                self.storage.manifest_path(str(row["id"]), str(row["received_at"])),
            ):
                if value:
                    paths.add(str(value))
        result["attachments"] = len(attachment_rows)
        if paths:
            connection.executemany(
                """
                INSERT INTO file_gc_tasks (
                    storage_path, reason, attempts, created_at, updated_at
                ) VALUES (?, 'retention', 0, ?, ?)
                ON CONFLICT(storage_path) DO UPDATE SET updated_at = excluded.updated_at
                """,
                ((storage_path, now, now) for storage_path in sorted(paths)),
            )

    def _delete_stale_empty_mailbox_batch(self, connection: sqlite3.Connection, limit: int) -> int:
        cutoff = self._cutoff_timestamp(seconds=int(self.settings.empty_mailbox_retention_seconds))
        cursor = connection.execute(
            """
            DELETE FROM mailboxes
            WHERE id IN (
                SELECT mailbox.id
                FROM mailboxes AS mailbox
                WHERE mailbox.message_count = 0
                  AND mailbox.latest_message_at IS NULL
                  AND mailbox.last_seen_at <= ?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM message_deliveries AS delivery
                      WHERE delivery.mailbox_id = mailbox.id
                  )
                ORDER BY mailbox.last_seen_at ASC, mailbox.id ASC
                LIMIT ?
            )
            """,
            (cutoff, limit),
        )
        return int(cursor.rowcount or 0)

    def _delete_stale_smtp_session_batch(self, connection: sqlite3.Connection, limit: int) -> int:
        cutoff = self._cutoff_timestamp(seconds=int(self.settings.smtp_session_retention_seconds))
        with self._smtp_connection_lock:
            active_session_ids = tuple(self._active_smtp_connections)
        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _retention_active_smtp_session (
                id TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )
        connection.execute("DELETE FROM _retention_active_smtp_session")
        if active_session_ids:
            connection.executemany(
                "INSERT OR IGNORE INTO _retention_active_smtp_session (id) VALUES (?)",
                ((session_id,) for session_id in active_session_ids),
            )
        cursor = connection.execute(
            """
            DELETE FROM smtp_sessions
            WHERE id IN (
                SELECT session.id
                FROM smtp_sessions AS session
                WHERE COALESCE(
                    session.disconnect_at,
                    session.last_command_at,
                    session.connect_at
                ) <= ?
                  AND (
                      session.status != 'open'
                      OR NOT EXISTS (
                          SELECT 1
                          FROM _retention_active_smtp_session AS active
                          WHERE active.id = session.id
                      )
                  )
                ORDER BY COALESCE(
                    session.disconnect_at,
                    session.last_command_at,
                    session.connect_at
                ) ASC, session.id ASC
                LIMIT ?
            )
            """,
            (cutoff, limit),
        )
        connection.execute("DELETE FROM _retention_active_smtp_session")
        return int(cursor.rowcount or 0)

    def _delete_old_metric_batch(self, connection: sqlite3.Connection, limit: int) -> int:
        cutoff = self._cutoff_timestamp(seconds=int(self.settings.metric_retention_seconds))
        cursor = connection.execute(
            """
            DELETE FROM mail_metric_buckets
            WHERE bucket_ts IN (
                SELECT bucket_ts
                FROM mail_metric_buckets
                WHERE bucket_ts < ?
                ORDER BY bucket_ts ASC
                LIMIT ?
            )
            """,
            (cutoff, limit),
        )
        return int(cursor.rowcount or 0)

    def _delete_old_audit_batch(self, connection: sqlite3.Connection, limit: int) -> int:
        cutoff = self._cutoff_timestamp(days=int(self.settings.audit_retention_days))
        cursor = connection.execute(
            """
            DELETE FROM audit_logs
            WHERE id IN (
                SELECT id
                FROM audit_logs
                WHERE created_at < ?
                ORDER BY created_at ASC, id ASC
                LIMIT ?
            )
            """,
            (cutoff, limit),
        )
        return int(cursor.rowcount or 0)

    def _delete_old_maintenance_run_batch(self, connection: sqlite3.Connection, limit: int) -> int:
        cutoff = self._cutoff_timestamp(days=int(self.settings.maintenance_run_retention_days))
        cursor = connection.execute(
            """
            DELETE FROM maintenance_runs
            WHERE id IN (
                SELECT id
                FROM maintenance_runs
                WHERE status IN ('succeeded', 'failed')
                  AND finished_at IS NOT NULL
                  AND finished_at < ?
                ORDER BY finished_at ASC, id ASC
                LIMIT ?
            )
            """,
            (cutoff, limit),
        )
        return int(cursor.rowcount or 0)

    def _sweep_retained_artifacts(self, now: str) -> dict[str, int]:
        now_epoch = self._timestamp_epoch(now)
        batch_size = int(self.settings.artifact_sweep_batch_size)
        quarantine_cutoff = now_epoch - int(self.settings.quarantine_retention_days) * 86_400
        orphan_cutoff = now_epoch - int(self.settings.orphan_artifact_grace_seconds)

        quarantine_candidates = self._select_artifact_sweep_candidates(
            ("quarantine",),
            cutoff_epoch=quarantine_cutoff,
            limit=batch_size,
            cursor_name="quarantine",
        )
        quarantine_deleted = 0
        quarantine_failed = 0
        for candidate in quarantine_candidates:
            try:
                quarantine_deleted += int(self._safe_unlink_sweep_candidate(candidate))
            except OSError:
                quarantine_failed += 1

        orphan_candidates = self._select_artifact_sweep_candidates(
            ("attachments", "html", "raw", "text"),
            cutoff_epoch=orphan_cutoff,
            limit=batch_size,
            cursor_name="orphan",
        )
        protected = self._protected_artifact_paths(orphan_candidates)
        orphan_deleted = 0
        orphan_failed = 0
        for candidate in orphan_candidates:
            if str(candidate["storage_path"]) in protected:
                continue
            try:
                orphan_deleted += int(self._safe_unlink_sweep_candidate(candidate))
            except OSError:
                orphan_failed += 1

        failed = quarantine_failed + orphan_failed
        if failed:
            _maintenance_logger.warning(
                "artifact sweep completed with file errors",
                extra={
                    "event": "artifact_sweep_partial_failure",
                    "task": "retention_cleanup",
                    "outcome": "partial_failure",
                    "count": failed,
                },
            )
        return {
            "quarantine_files_deleted": quarantine_deleted,
            "quarantine_files_failed": quarantine_failed,
            "quarantine_files_examined": len(quarantine_candidates),
            "orphan_artifacts_deleted": orphan_deleted,
            "orphan_artifacts_failed": orphan_failed,
            "orphan_artifacts_examined": len(orphan_candidates),
            "orphan_artifacts_protected": len(protected),
        }

    def _timestamp_epoch(self, value: str) -> float:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()

    def _select_artifact_sweep_candidates(
        self,
        categories: tuple[str, ...],
        *,
        cutoff_epoch: float,
        limit: int,
        cursor_name: str,
    ) -> list[dict[str, Any]]:
        state = self._artifact_sweep_iterators.get(cursor_name)
        if state is not None and state[0] != categories:
            self._close_artifact_sweep_iterator(cursor_name)
            state = None
        if state is None:
            iterator = iter(self._iter_old_regular_artifacts(categories, cutoff_epoch))
            state = (categories, cutoff_epoch, iterator)
            self._artifact_sweep_iterators[cursor_name] = state
        iterator = state[2]

        selected: list[dict[str, Any]] = []
        for _ in range(limit):
            try:
                selected.append(next(iterator))
            except StopIteration:
                # Never wrap into a new pass in the same cleanup call.  Apart
                # from avoiding duplicate work, this fixes the pass cutoff:
                # files that age into eligibility are picked up next pass.
                self._close_artifact_sweep_iterator(cursor_name)
                break
            except BaseException:
                self._close_artifact_sweep_iterator(cursor_name)
                raise
        return selected

    def _close_artifact_sweep_iterator(self, cursor_name: str) -> None:
        state = self._artifact_sweep_iterators.pop(cursor_name, None)
        if state is None:
            return
        close = getattr(state[2], "close", None)
        if close is not None:
            close()

    def _reset_artifact_sweep_iterators(self) -> None:
        for cursor_name in tuple(self._artifact_sweep_iterators):
            self._close_artifact_sweep_iterator(cursor_name)

    def _iter_old_regular_artifacts(
        self,
        categories: tuple[str, ...],
        cutoff_epoch: float,
    ) -> Any:
        root = self.settings.storage_root.resolve(strict=False)
        for category in sorted(categories):
            category_root = root / category
            try:
                category_stat = os.stat(category_root, follow_symlinks=False)
            except OSError:
                continue
            if not stat.S_ISDIR(category_stat.st_mode):
                continue

            # CPython's os.walk closes each per-directory scandir before it
            # yields, so a pass may remain suspended between cleanup batches
            # without pinning directory descriptors.
            for directory, directory_names, file_names in os.walk(category_root, followlinks=False):
                safe_directories: list[str] = []
                for directory_name in directory_names:
                    child = Path(directory) / directory_name
                    try:
                        child_stat = os.stat(child, follow_symlinks=False)
                    except OSError:
                        continue
                    if stat.S_ISDIR(child_stat.st_mode):
                        safe_directories.append(directory_name)
                directory_names[:] = sorted(safe_directories)

                for file_name in sorted(file_names):
                    path = Path(directory) / file_name
                    try:
                        file_stat = os.stat(path, follow_symlinks=False)
                        relative_path = path.relative_to(root).as_posix()
                    except (OSError, ValueError):
                        continue
                    if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_mtime >= cutoff_epoch:
                        continue
                    yield {
                        "storage_path": relative_path,
                        "stat_signature": self._stat_signature(file_stat),
                    }

    def _stat_signature(self, file_stat: os.stat_result) -> tuple[int, int, int, int, int, int]:
        return (
            int(file_stat.st_dev),
            int(file_stat.st_ino),
            int(file_stat.st_mode),
            int(file_stat.st_size),
            int(file_stat.st_mtime_ns),
            int(file_stat.st_ctime_ns),
        )

    def _protected_artifact_paths(self, candidates: list[dict[str, Any]]) -> set[str]:
        candidate_paths = {str(candidate["storage_path"]) for candidate in candidates}
        if not candidate_paths:
            return set()
        protected = self._database_artifact_references(candidate_paths)

        active_message_ids = self.active_mail_accept_message_ids()
        for storage_path in candidate_paths:
            message_id = self._artifact_message_id(storage_path)
            if message_id is not None and message_id in active_message_ids:
                protected.add(storage_path)

        quarantine_candidates = candidate_paths.difference(protected)
        if quarantine_candidates:
            protected.update(self._quarantine_artifact_references(quarantine_candidates))

        manifest_candidates = candidate_paths.difference(protected)
        if manifest_candidates:
            protected.update(self._manifest_artifact_references(manifest_candidates))
        return protected

    def _database_artifact_references(self, candidate_paths: set[str]) -> set[str]:
        protected: set[str] = set()
        ordered_paths = sorted(candidate_paths)
        with connect_database(self.settings.database_path) as connection:
            for index in range(0, len(ordered_paths), 400):
                chunk = ordered_paths[index : index + 400]
                placeholders = ", ".join("?" for _ in chunk)
                for table_name, column_name in (
                    ("messages", "raw_path"),
                    ("messages", "text_body_path"),
                    ("messages", "html_body_path"),
                    ("attachments", "storage_path"),
                    ("file_gc_tasks", "storage_path"),
                ):
                    rows = connection.execute(
                        f"SELECT {column_name} AS storage_path FROM {table_name} "
                        f"WHERE {column_name} IN ({placeholders})",
                        tuple(chunk),
                    ).fetchall()
                    protected.update(str(row["storage_path"]) for row in rows)
        return protected

    def _manifest_artifact_references(self, candidate_paths: set[str]) -> set[str]:
        by_message_id: dict[str, set[str]] = {}
        manifest_candidates: dict[str, set[str]] = {}
        attachment_message_ids: set[str] = set()
        for storage_path in candidate_paths:
            message_id = self._artifact_message_id(storage_path)
            if message_id is None:
                continue
            by_message_id.setdefault(message_id, set()).add(storage_path)
            parts = Path(storage_path).parts
            if parts[0] == "attachments":
                attachment_message_ids.add(message_id)
                continue
            if len(parts) >= 5:
                manifest_path = (
                    Path("manifests")
                    / parts[1]
                    / parts[2]
                    / parts[3]
                    / f"{message_id}.json"
                ).as_posix()
                manifest_candidates.setdefault(manifest_path, set()).add(storage_path)

        for manifest_path, message_id in self._matching_manifest_paths(attachment_message_ids):
            manifest_candidates.setdefault(manifest_path, set()).update(
                by_message_id.get(message_id, set())
            )

        protected: set[str] = set()
        for manifest_path, associated_paths in manifest_candidates.items():
            try:
                references = self._read_manifest_artifact_references(manifest_path)
            except FileNotFoundError:
                continue
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                # A visible manifest is recovery-owned. If it is incomplete,
                # oversized, or a symlink, leave its artifacts alone until the
                # recovery scanner quarantines it.
                protected.update(associated_paths)
                continue
            protected.update(candidate_paths.intersection(references))
        return protected

    def _quarantine_artifact_references(self, candidate_paths: set[str]) -> set[str]:
        by_message_id: dict[str, set[str]] = {}
        for storage_path in candidate_paths:
            message_id = self._artifact_message_id(storage_path)
            if message_id is not None:
                by_message_id.setdefault(message_id, set()).add(storage_path)
        if not by_message_id:
            return set()

        protected: set[str] = set()
        for quarantine_path, associated_message_ids in self._matching_quarantine_paths(
            set(by_message_id)
        ):
            associated_paths: set[str] = set()
            for message_id in associated_message_ids:
                associated_paths.update(by_message_id[message_id])
            try:
                references = self._read_manifest_artifact_references(quarantine_path)
            except (OSError, UnicodeError, ValueError, json.JSONDecodeError):
                # Python recovery moves the original invalid manifest. If it is
                # not parseable, its message-id filename still ties it to the
                # corresponding evidence artifacts until quarantine retention
                # removes the record itself.
                protected.update(associated_paths)
                continue
            protected.update(candidate_paths.intersection(references))
        return protected

    def _matching_quarantine_paths(self, message_ids: set[str]) -> list[tuple[str, set[str]]]:
        root = self.settings.storage_root.resolve(strict=False)
        quarantine_root = root / "quarantine"
        try:
            quarantine_stat = os.stat(quarantine_root, follow_symlinks=False)
        except OSError:
            return []
        if not stat.S_ISDIR(quarantine_stat.st_mode):
            return []

        matches: list[tuple[str, set[str]]] = []
        for directory, directory_names, file_names in os.walk(quarantine_root, followlinks=False):
            safe_directories: list[str] = []
            for directory_name in directory_names:
                child = Path(directory) / directory_name
                try:
                    child_stat = os.stat(child, follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISDIR(child_stat.st_mode):
                    safe_directories.append(directory_name)
            directory_names[:] = sorted(safe_directories)
            for file_name in sorted(file_names):
                if not file_name.endswith(".json"):
                    continue
                stem = file_name.removesuffix(".json").removesuffix(".error")
                associated = {
                    message_id
                    for message_id in message_ids
                    if stem == message_id or stem.startswith(f"{message_id}-")
                }
                if not associated:
                    continue
                path = Path(directory) / file_name
                try:
                    relative_path = path.relative_to(root).as_posix()
                except ValueError:
                    continue
                matches.append((relative_path, associated))
        return matches

    def _artifact_message_id(self, storage_path: str) -> str | None:
        parts = Path(storage_path).parts
        if len(parts) >= 3 and parts[0] == "attachments":
            return parts[1]
        if len(parts) >= 2 and parts[0] in {"raw", "text", "html"}:
            return Path(parts[-1]).stem
        return None

    def _matching_manifest_paths(self, message_ids: set[str]) -> list[tuple[str, str]]:
        if not message_ids:
            return []
        root = self.settings.storage_root.resolve(strict=False)
        manifests_root = root / "manifests"
        try:
            manifests_stat = os.stat(manifests_root, follow_symlinks=False)
        except OSError:
            return []
        if not stat.S_ISDIR(manifests_stat.st_mode):
            return []

        expected_names = {f"{message_id}.json": message_id for message_id in message_ids}
        found: list[tuple[str, str]] = []
        for directory, directory_names, file_names in os.walk(manifests_root, followlinks=False):
            safe_directories: list[str] = []
            for directory_name in directory_names:
                child = Path(directory) / directory_name
                try:
                    child_stat = os.stat(child, follow_symlinks=False)
                except OSError:
                    continue
                if stat.S_ISDIR(child_stat.st_mode):
                    safe_directories.append(directory_name)
            directory_names[:] = sorted(safe_directories)
            for file_name in sorted(file_names):
                message_id = expected_names.get(file_name)
                if message_id is None:
                    continue
                path = Path(directory) / file_name
                try:
                    relative_path = path.relative_to(root).as_posix()
                except ValueError:
                    continue
                found.append((relative_path, message_id))
                expected_names.pop(file_name, None)
            if not expected_names:
                break
        return found

    def _read_manifest_artifact_references(self, manifest_path: str) -> set[str]:
        payload_bytes = self._read_storage_file_no_follow(
            manifest_path,
            max_bytes=MAX_MANIFEST_SWEEP_BYTES,
        )
        payload = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("manifest is not an object")

        values: list[Any] = [payload.get("raw_path")]
        parsed = payload.get("parsed")
        if isinstance(parsed, dict):
            values.extend((parsed.get("text_body_path"), parsed.get("html_body_path")))
            attachments = parsed.get("attachments")
            if isinstance(attachments, list):
                values.extend(
                    item.get("storage_path")
                    for item in attachments
                    if isinstance(item, dict)
                )

        references: set[str] = set()
        for value in values:
            if not isinstance(value, str) or not value:
                continue
            path = Path(value)
            if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
                continue
            references.add(path.as_posix())
        return references

    def _read_storage_file_no_follow(self, storage_path: str, *, max_bytes: int) -> bytes:
        parent_fd, file_name, descriptors = self._open_storage_parent_no_follow(storage_path)
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            file_fd = os.open(file_name, flags, dir_fd=parent_fd)
            descriptors.append(file_fd)
            file_stat = os.fstat(file_fd)
            if not stat.S_ISREG(file_stat.st_mode) or file_stat.st_size > max_bytes:
                raise ValueError("storage file is not a bounded regular file")
            chunks: list[bytes] = []
            remaining = max_bytes + 1
            while remaining > 0:
                chunk = os.read(file_fd, min(remaining, 1024 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if len(payload) > max_bytes:
                raise ValueError("storage file exceeds read limit")
            return payload
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _safe_unlink_sweep_candidate(self, candidate: dict[str, Any]) -> bool:
        parent_fd, file_name, descriptors = self._open_storage_parent_no_follow(
            str(candidate["storage_path"])
        )
        try:
            try:
                current_stat = os.stat(file_name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return False
            if not stat.S_ISREG(current_stat.st_mode):
                return False
            if self._stat_signature(current_stat) != tuple(candidate["stat_signature"]):
                return False
            os.unlink(file_name, dir_fd=parent_fd)
            return True
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _open_storage_parent_no_follow(self, storage_path: str) -> tuple[int, str, list[int]]:
        relative_path = Path(storage_path)
        if relative_path.is_absolute():
            raise ValueError("storage path must be relative")
        parts = relative_path.parts
        if len(parts) < 2 or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("invalid storage path")

        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptors: list[int] = []
        try:
            current_fd = os.open(self.settings.storage_root.resolve(strict=False), flags)
            descriptors.append(current_fd)
            for part in parts[:-1]:
                current_fd = os.open(part, flags, dir_fd=current_fd)
                descriptors.append(current_fd)
            return current_fd, parts[-1], descriptors
        except BaseException:
            for descriptor in reversed(descriptors):
                os.close(descriptor)
            raise

    async def _drain_file_gc(self, limit: int) -> dict[str, int]:
        now = utc_now()
        rows = await asyncio.to_thread(self._load_due_file_gc_tasks, now, limit)
        if not rows:
            pending = await asyncio.to_thread(self._count_file_gc_tasks)
            return {"file_gc_deleted": 0, "file_gc_failed": 0, "file_gc_pending": pending}

        outcomes = await asyncio.to_thread(self._delete_gc_paths, rows)
        retry_base = datetime.fromisoformat(now.replace("Z", "+00:00"))
        if retry_base.tzinfo is None:
            retry_base = retry_base.replace(tzinfo=timezone.utc)
        retry_base = retry_base.astimezone(timezone.utc).replace(microsecond=0)

        def apply_outcomes(connection: sqlite3.Connection) -> None:
            for outcome in outcomes:
                if outcome["error"] is None:
                    connection.execute("DELETE FROM file_gc_tasks WHERE id = ?", (outcome["id"],))
                    continue
                attempts = int(outcome["attempts"]) + 1
                retry_seconds = min(3600, 30 * (2 ** min(attempts - 1, 7)))
                next_attempt = (retry_base + timedelta(seconds=retry_seconds)).isoformat().replace(
                    "+00:00",
                    "Z",
                )
                connection.execute(
                    """
                    UPDATE file_gc_tasks
                    SET attempts = ?, last_error = ?, next_attempt_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (attempts, outcome["error"], next_attempt, now, outcome["id"]),
                )

        await self.writer.execute(apply_outcomes)
        pending = await asyncio.to_thread(self._count_file_gc_tasks)
        deleted = sum(1 for item in outcomes if item["error"] is None)
        return {
            "file_gc_deleted": deleted,
            "file_gc_failed": len(outcomes) - deleted,
            "file_gc_pending": pending,
        }

    def _load_due_file_gc_tasks(self, now: str, limit: int) -> list[dict[str, Any]]:
        with connect_database(self.settings.database_path) as connection:
            pending_rows = connection.execute(
                """
                SELECT id, storage_path, attempts
                FROM file_gc_tasks
                WHERE next_attempt_at IS NULL
                ORDER BY id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            retry_rows = connection.execute(
                """
                SELECT id, storage_path, attempts
                FROM file_gc_tasks
                WHERE next_attempt_at IS NOT NULL AND next_attempt_at <= ?
                ORDER BY next_attempt_at ASC, id ASC
                LIMIT ?
                """,
                (now, limit),
            ).fetchall()

        # Keep both newly-created tombstones and due retries making progress.
        # A single OR predicate with ORDER BY id made SQLite scan the whole
        # outbox; draining all NULL rows first could instead starve retries
        # under sustained deletion traffic. Alternate the two indexed streams
        # and keep the returned batch strictly bounded.
        rows: list[dict[str, Any]] = []
        pending_index = 0
        retry_index = 0
        while len(rows) < limit and (
            pending_index < len(pending_rows) or retry_index < len(retry_rows)
        ):
            if pending_index < len(pending_rows):
                rows.append(dict(pending_rows[pending_index]))
                pending_index += 1
            if len(rows) >= limit:
                break
            if retry_index < len(retry_rows):
                rows.append(dict(retry_rows[retry_index]))
                retry_index += 1
        return rows

    def _count_file_gc_tasks(self) -> int:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM file_gc_tasks").fetchone()
        return 0 if row is None else int(row["count"])

    def _delete_gc_paths(self, tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        outcomes: list[dict[str, Any]] = []
        for task in tasks:
            error: str | None = None
            try:
                self._safe_unlink_gc_path(str(task["storage_path"]))
            except Exception as exc:  # noqa: BLE001 - persisted for bounded retry
                error = f"{exc.__class__.__name__}: {exc}"[:1000]
            outcomes.append({**task, "error": error})
        return outcomes

    def _safe_unlink_gc_path(self, storage_path: str) -> None:
        parent_fd, file_name, descriptors = self._open_storage_parent_no_follow(storage_path)
        parts = Path(storage_path).parts
        try:
            try:
                current_stat = os.stat(file_name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if not stat.S_ISREG(current_stat.st_mode):
                raise OSError("GC target is not a regular file")
            os.unlink(file_name, dir_fd=parent_fd)

            # Best-effort directory pruning through already verified directory
            # descriptors. Never resolve or follow a replacement symlink.
            for descriptor_index in range(len(descriptors) - 1, 0, -1):
                try:
                    os.rmdir(parts[descriptor_index - 1], dir_fd=descriptors[descriptor_index - 1])
                except OSError:
                    break
        finally:
            for descriptor in reversed(descriptors):
                os.close(descriptor)

    def _empty_retention_result(self) -> dict[str, int]:
        return {
            "messages": 0,
            "deliveries": 0,
            "mailboxes": 0,
            "attachments": 0,
            "smtp_sessions": 0,
            "metric_buckets": 0,
            "audit_logs": 0,
            "maintenance_runs": 0,
            "raw_size_bytes": 0,
            "files": 0,
            "file_gc_deleted": 0,
            "file_gc_failed": 0,
            "file_gc_pending": 0,
            "quarantine_files_deleted": 0,
            "quarantine_files_failed": 0,
            "quarantine_files_examined": 0,
            "orphan_artifacts_deleted": 0,
            "orphan_artifacts_failed": 0,
            "orphan_artifacts_examined": 0,
            "orphan_artifacts_protected": 0,
            "dropped_parse_tasks": 0,
        }

    def _assert_runtime_secrets_are_safe(self) -> None:
        bootstrap_admin_pending = self._bootstrap_admin_pending()
        insecure_defaults = self.settings.insecure_runtime_defaults(
            bootstrap_admin_pending=bootstrap_admin_pending,
        )
        if not insecure_defaults or not self.settings.externally_bound():
            return
        raise RuntimeError(
            "Refusing to start with insecure security configuration on a non-loopback bind: "
            + ", ".join(insecure_defaults)
        )

    def external_request_security_findings(self) -> list[str]:
        """Recheck security for servers bound outside the supported runner.

        A caller can import ``app.main:app`` and override Uvicorn's bind address
        without changing ``HOST``. Startup cannot observe that CLI override, so
        the HTTP middleware uses the actual peer address and calls this guard.
        """

        findings = self.settings.insecure_runtime_defaults(bootstrap_admin_pending=False)
        if self.settings.bootstrap_admin_password == DEFAULT_BOOTSTRAP_ADMIN_PASSWORD:
            with connect_database(self.settings.database_path) as connection:
                row = connection.execute(
                    """
                    SELECT 1
                    FROM admins
                    WHERE username = ? AND must_change_password = 1
                    LIMIT 1
                    """,
                    (self.settings.bootstrap_admin_username,),
                ).fetchone()
            if row is not None:
                findings.append("BOOTSTRAP_ADMIN_PASSWORD")
        return sorted(set(findings))

    def _bootstrap_admin_pending(self) -> bool:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM admins").fetchone()
        return row is None or int(row["count"]) == 0

    def _metric_bucket_ts(self, timestamp: str) -> str:
        return timestamp[:16] + ":00Z"

    def _increment_mail_metric(
        self,
        connection: sqlite3.Connection,
        timestamp: str,
        *,
        received: int = 0,
        deliveries: int = 0,
        parse_failures: int = 0,
        rejected: int = 0,
    ) -> None:
        if received == 0 and deliveries == 0 and parse_failures == 0 and rejected == 0:
            return
        connection.execute(
            """
            INSERT INTO mail_metric_buckets (
                bucket_ts, received, deliveries, parse_failures, rejected
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(bucket_ts) DO UPDATE SET
                received = received + excluded.received,
                deliveries = deliveries + excluded.deliveries,
                parse_failures = parse_failures + excluded.parse_failures,
                rejected = rejected + excluded.rejected
            """,
            (
                self._metric_bucket_ts(timestamp),
                int(received),
                int(deliveries),
                int(parse_failures),
                int(rejected),
            ),
        )

    def list_audit_logs(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        return self.audit.list_logs(limit=limit, offset=offset)

    async def register_smtp_connection(self, session_id: str, remote_ip: str) -> tuple[bool, str | None]:
        with self._smtp_connection_lock:
            if session_id in self._active_smtp_connections:
                return True, None

            active_limit = int(self.settings.smtp_max_concurrent_connections)
            if active_limit > 0 and len(self._active_smtp_connections) >= active_limit:
                return False, "concurrent connection limit exceeded"

            rate_limit = int(self.settings.smtp_connection_rate_limit_count)
            window_seconds = int(self.settings.smtp_connection_rate_limit_window_seconds)
            if rate_limit > 0 and window_seconds > 0:
                now = monotonic()
                cutoff = now - window_seconds
                self._evict_stale_smtp_ip_windows(cutoff)
                window = self._smtp_ip_windows.get(remote_ip)
                if window is None:
                    capacity = self._smtp_ip_window_capacity()
                    while len(self._smtp_ip_windows) >= capacity:
                        evicted_ip, _evicted_window = self._smtp_ip_windows.popitem(last=False)
                        self._smtp_ip_expiry_order.pop(evicted_ip, None)
                    window = deque()
                    self._smtp_ip_windows[remote_ip] = window
                else:
                    # Rejected hot sources remain protected from churn-based
                    # LRU eviction, without changing their accepted-time expiry.
                    self._smtp_ip_windows.move_to_end(remote_ip)
                while window and window[0] <= cutoff:
                    window.popleft()
                if len(window) >= rate_limit:
                    return False, "per-ip connection rate limit exceeded"
                window.append(now)
                self._smtp_ip_expiry_order.pop(remote_ip, None)
                self._smtp_ip_expiry_order[remote_ip] = None
            elif self._smtp_ip_windows:
                # A live setting can disable the limiter. Drop its now-unused
                # state immediately instead of retaining it until shutdown.
                self._smtp_ip_windows.clear()
                self._smtp_ip_expiry_order.clear()

            self._active_smtp_connections[session_id] = remote_ip
            return True, None

    def _evict_stale_smtp_ip_windows(self, cutoff: float) -> None:
        """Amortized O(1) expiry in accepted-time order."""

        while self._smtp_ip_expiry_order:
            ip = next(iter(self._smtp_ip_expiry_order))
            window = self._smtp_ip_windows.get(ip)
            if window is None:
                self._smtp_ip_expiry_order.popitem(last=False)
                continue
            if window and window[-1] > cutoff:
                break
            self._smtp_ip_expiry_order.popitem(last=False)
            self._smtp_ip_windows.pop(ip, None)

    def _smtp_ip_window_capacity(self) -> int:
        max_connections = max(int(self.settings.smtp_max_concurrent_connections), 0)
        return min(
            SMTP_IP_RATE_STATE_MAX_ENTRIES,
            max(SMTP_IP_RATE_STATE_MIN_ENTRIES, max_connections * 4),
        )

    async def release_smtp_connection(self, session_id: str) -> bool:
        with self._smtp_connection_lock:
            return self._active_smtp_connections.pop(session_id, None) is not None

    def active_smtp_connection_count(self) -> int:
        with self._smtp_connection_lock:
            return len(self._active_smtp_connections)

    def _mark_orphaned_smtp_sessions_closed(self, connection: sqlite3.Connection) -> int:
        now = utc_now()
        cursor = connection.execute(
            """
            UPDATE smtp_sessions
            SET status = 'error',
                disconnect_at = COALESCE(disconnect_at, ?),
                last_command_at = COALESCE(last_command_at, ?),
                close_reason = COALESCE(close_reason, 'runtime restarted before disconnect')
            WHERE status = 'open'
            """,
            (now, now),
        )
        return int(cursor.rowcount or 0)

    def _clear_mail_tables(
        self,
        connection: sqlite3.Connection,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, int]:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("BEGIN IMMEDIATE")
        self.api_keys.transaction_authorization_principal(
            connection,
            authorization_principal,
            required_scope="system.write",
            require_global=True,
        )
        messages_count = int(connection.execute("SELECT COUNT(*) AS count FROM messages").fetchone()["count"])
        deliveries_count = int(connection.execute("SELECT COUNT(*) AS count FROM message_deliveries").fetchone()["count"])
        mailboxes_count = int(connection.execute("SELECT COUNT(*) AS count FROM mailboxes").fetchone()["count"])
        attachments_count = int(connection.execute("SELECT COUNT(*) AS count FROM attachments").fetchone()["count"])
        smtp_sessions_count = int(connection.execute("SELECT COUNT(*) AS count FROM smtp_sessions").fetchone()["count"])
        smtp_events_count = int(connection.execute("SELECT COUNT(*) AS count FROM smtp_events").fetchone()["count"])
        metric_buckets_count = int(connection.execute("SELECT COUNT(*) AS count FROM mail_metric_buckets").fetchone()["count"])
        total_bytes = int(
            connection.execute("SELECT COALESCE(SUM(raw_size_bytes), 0) AS total FROM messages").fetchone()["total"]
        )

        # Keep the singleton absent while bulk deletes fire row triggers.  The
        # trigger UPDATEs then become no-ops instead of rewriting the same WAL
        # page once per historical message/mailbox.  Rebuild it exactly before
        # commit; rollback restores the original row automatically.
        connection.execute("DELETE FROM dashboard_counters WHERE singleton_id = 1")
        for table_name in (
            "attachments",
            "mailbox_bulk_delete_jobs",
            "message_deliveries",
            "messages",
            "mailboxes",
            "smtp_events",
            "smtp_sessions",
            "mail_metric_buckets",
            "file_gc_tasks",
        ):
            connection.execute(f"DELETE FROM {table_name}")
        connection.execute(
            """
            INSERT INTO dashboard_counters (
                singleton_id,
                domains,
                mailboxes,
                messages,
                api_keys,
                audit_logs,
                pending_messages,
                failed_messages
            )
            SELECT
                1,
                (SELECT COUNT(*) FROM domains),
                (SELECT COUNT(*) FROM mailboxes),
                (SELECT COUNT(*) FROM messages),
                (SELECT COUNT(*) FROM api_keys),
                (SELECT COUNT(*) FROM audit_logs),
                (SELECT COUNT(*) FROM messages WHERE parse_status = 'pending'),
                (SELECT COUNT(*) FROM messages WHERE parse_status = 'failed')
            """
        )
        connection.execute("DELETE FROM sqlite_sequence WHERE name IN ('mailboxes', 'smtp_events')")
        if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise RuntimeError("mail store foreign key check failed after clear")
        return {
            "messages": messages_count,
            "deliveries": deliveries_count,
            "mailboxes": mailboxes_count,
            "attachments": attachments_count,
            "smtp_sessions": smtp_sessions_count,
            "smtp_events": smtp_events_count,
            "metric_buckets": metric_buckets_count,
            "raw_size_bytes": total_bytes,
        }

    def _compact_mail_database(self, connection: sqlite3.Connection) -> dict[str, int]:
        page_size = int(connection.execute("PRAGMA page_size").fetchone()[0])
        freelist_before = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        size_before = self._database_file_size_bytes()

        checkpoint_before = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        vacuumed = 0
        if freelist_before > 0:
            connection.execute("VACUUM")
            vacuumed = 1
        connection.execute("PRAGMA optimize")
        checkpoint_after = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()

        freelist_after = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        size_after = self._database_file_size_bytes()
        return {
            "database_size_before_bytes": size_before,
            "database_size_after_bytes": size_after,
            "database_free_bytes_before": freelist_before * page_size,
            "database_free_bytes_after": freelist_after * page_size,
            "database_vacuumed": vacuumed,
            "database_checkpoint_busy_before": int(checkpoint_before[0]) if checkpoint_before is not None else 0,
            "database_checkpoint_busy_after": int(checkpoint_after[0]) if checkpoint_after is not None else 0,
        }

    def _database_file_size_bytes(self) -> int:
        database_path = self.settings.database_path
        return sum(
            path.stat().st_size
            for path in (
                database_path,
                Path(f"{database_path}-wal"),
                Path(f"{database_path}-shm"),
            )
            if path.exists()
        )

    async def ensure_smtp_session(self, session_id: str, session: Any, *, last_rcpt_to: str | None = None) -> None:
        now = utc_now()
        peer = getattr(session, "peer", None) or ("unknown", None)
        remote_ip = peer[0] or "unknown"
        remote_port = peer[1]
        helo_name = getattr(session, "host_name", None)
        tls_used = int(bool(getattr(session, "ssl", None)))

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO smtp_sessions (
                    id,
                    remote_ip,
                    remote_port,
                    helo_name,
                    status,
                    tls_used,
                    connect_at,
                    first_command_at,
                    last_command_at,
                    last_rcpt_to_sample
                ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    helo_name = excluded.helo_name,
                    tls_used = excluded.tls_used,
                    last_command_at = excluded.last_command_at,
                    last_rcpt_to_sample = COALESCE(excluded.last_rcpt_to_sample, smtp_sessions.last_rcpt_to_sample)
                """,
                (
                    session_id,
                    remote_ip,
                    remote_port,
                    helo_name,
                    tls_used,
                    now,
                    now,
                    now,
                    last_rcpt_to,
                ),
            )

        await self.writer.execute(operation)

    async def record_smtp_rcpt(self, session_id: str, *, rcpt_to: str, accepted: bool) -> None:
        now = utc_now()

        def operation(connection: sqlite3.Connection) -> None:
            self._update_smtp_session_summary(
                connection,
                session_id,
                now,
                accepted_delta=1 if accepted else 0,
                rejected_delta=0 if accepted else 1,
                last_rcpt_to_sample=rcpt_to,
            )
            if not accepted:
                self._increment_mail_metric(connection, now, rejected=1)

        await self.writer.execute(operation)

    async def record_smtp_event(self, session_id: str, event_type: str, payload: dict[str, Any]) -> None:
        now = str(payload.get("ts") or utc_now())
        payload_json = json.dumps(payload, ensure_ascii=False)

        def operation(connection: sqlite3.Connection) -> None:
            row = connection.execute(
                """
                SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq
                FROM smtp_events
                WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            seq = int(row["next_seq"]) if row is not None else 1
            connection.execute(
                """
                INSERT OR IGNORE INTO smtp_events (session_id, seq, event_type, ts, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, seq, event_type, now, payload_json),
            )

        await self.writer.execute(operation)

    async def close_smtp_session(
        self,
        session_id: str,
        *,
        status: str,
        close_reason: str | None = None,
        result_code: int | None = None,
        result_message: str | None = None,
    ) -> None:
        now = utc_now()

        def operation(connection: sqlite3.Connection) -> None:
            self._update_smtp_session_summary(
                connection,
                session_id,
                now,
                status=status,
                disconnect_at=now,
                close_reason=close_reason,
                result_code=result_code,
                result_message=result_message,
            )

        await self.writer.execute(operation)
        await self.release_smtp_connection(session_id)

    async def close_lost_smtp_session(
        self,
        session_id: str,
        *,
        status: str = "closed",
        remote_ip: str = "unknown",
        remote_port: int | None = None,
        helo_name: str | None = None,
        tls_used: bool = False,
        close_reason: str | None = None,
        result_code: int | None = None,
        result_message: str | None = None,
    ) -> bool:
        was_active = await self.release_smtp_connection(session_id)

        now = utc_now()

        def operation(connection: sqlite3.Connection) -> int:
            if not was_active:
                cursor = connection.execute(
                    """
                    UPDATE smtp_sessions
                    SET status = ?,
                        last_command_at = ?,
                        disconnect_at = COALESCE(disconnect_at, ?),
                        close_reason = COALESCE(close_reason, ?),
                        result_code = COALESCE(result_code, ?),
                        result_message = COALESCE(result_message, ?)
                    WHERE id = ? AND status = 'open'
                    """,
                    (
                        status,
                        now,
                        now,
                        close_reason,
                        result_code,
                        result_message,
                        session_id,
                    ),
                )
                return int(cursor.rowcount or 0)

            cursor = connection.execute(
                """
                INSERT INTO smtp_sessions (
                    id,
                    remote_ip,
                    remote_port,
                    helo_name,
                    status,
                    tls_used,
                    connect_at,
                    first_command_at,
                    last_command_at,
                    disconnect_at,
                    close_reason,
                    result_code,
                    result_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    first_command_at = COALESCE(smtp_sessions.first_command_at, excluded.first_command_at),
                    last_command_at = excluded.last_command_at,
                    status = excluded.status,
                    disconnect_at = COALESCE(smtp_sessions.disconnect_at, excluded.disconnect_at),
                    close_reason = COALESCE(smtp_sessions.close_reason, excluded.close_reason),
                    result_code = COALESCE(smtp_sessions.result_code, excluded.result_code),
                    result_message = COALESCE(smtp_sessions.result_message, excluded.result_message)
                """,
                (
                    session_id,
                    remote_ip,
                    remote_port,
                    helo_name,
                    status,
                    int(tls_used),
                    now,
                    now,
                    now,
                    now,
                    close_reason,
                    result_code,
                    result_message,
                ),
            )
            return int(cursor.rowcount or 0)

        return bool(await self.writer.execute(operation))

    async def accept_message(
        self,
        *,
        rcpt_tos: list[str],
        envelope_from: str | None,
        content: bytes,
        smtp_session_id: str | None = None,
    ) -> str:
        message_id = f"msg_{uuid.uuid4().hex}"
        await self._enter_mail_accept()
        with self._active_mail_accept_message_lock:
            self._active_mail_accept_message_ids.add(message_id)
        try:
            return await self._accept_message_operation(
                message_id=message_id,
                rcpt_tos=rcpt_tos,
                envelope_from=envelope_from,
                content=content,
                smtp_session_id=smtp_session_id,
            )
        finally:
            with self._active_mail_accept_message_lock:
                self._active_mail_accept_message_ids.discard(message_id)
            await self._leave_mail_accept()

    def active_mail_accept_message_ids(self) -> set[str]:
        with self._active_mail_accept_message_lock:
            return set(self._active_mail_accept_message_ids)

    async def _accept_message_operation(
        self,
        *,
        message_id: str,
        rcpt_tos: list[str],
        envelope_from: str | None,
        content: bytes,
        smtp_session_id: str | None = None,
    ) -> str:
        received_at = utc_now()

        matches = []
        for rcpt_to in rcpt_tos:
            match = self.domains.match_address(rcpt_to)
            if match is None:
                raise ValueError(f"recipient domain not allowed: {rcpt_to}")
            matches.append((rcpt_to, match))
        if not matches:
            raise ValueError("message has no unique recipients")

        raw_path = self.storage.raw_message_path(message_id, received_at)
        manifest_path = self.storage.manifest_path(message_id, received_at)
        raw_sha256 = hashlib.sha256(content).hexdigest()
        raw_size_bytes = len(content)
        try:
            domain_policies = self._load_recovery_domain_policies(matches)
        except LookupError:
            return "451 recipient policy changed; retry later"
        recovery_order_ns = time_ns()
        recipient_recovery_payloads = [
            self._recovery_recipient_payload(rcpt_to, match, domain_policies[match.domain_id])
            for rcpt_to, match in matches
        ]
        manifest_payload = {
            "message_id": message_id,
            "smtp_session_id": smtp_session_id,
            "envelope_from": envelope_from,
            "rcpt_tos": list(rcpt_tos),
            "recipients": recipient_recovery_payloads,
            "received_at": received_at,
            "recovery_order_ns": recovery_order_ns,
            "raw_path": raw_path,
            "raw_sha256": raw_sha256,
            "raw_size_bytes": raw_size_bytes,
        }
        # Publish the raw message before its manifest. Recovery treats a visible
        # manifest as a committed ingest record, so exposing it first would let a
        # concurrent scanner quarantine an otherwise healthy in-flight delivery.
        # File IO and fsync stay off the event loop so SMTP sessions can progress.
        await asyncio.to_thread(self._write_accept_artifacts, message_id, received_at, manifest_payload, content)

        def operation(connection: sqlite3.Connection) -> list[dict[str, Any]]:
            # Own the SQLite write slot before resolving routing. This makes the
            # rematch and every delivery insert one policy snapshot even when a
            # second process edits domains concurrently.
            connection.execute("BEGIN IMMEDIATE")
            transaction_matches: list[tuple[str, DomainMatch, Any]] = []
            for rcpt_to, accepted_match in matches:
                accepted_identity = connection.execute(
                    """
                    SELECT root_domain_ascii, is_active
                    FROM domains
                    WHERE id = ?
                    """,
                    (accepted_match.domain_id,),
                ).fetchone()
                if (
                    accepted_identity is None
                    or str(accepted_identity["root_domain_ascii"])
                    != str(accepted_match.root_domain_ascii)
                    or not bool(accepted_identity["is_active"])
                ):
                    raise _RecipientPolicyChangedError(
                        f"recipient policy changed before commit: {rcpt_to}"
                    )
                current_match = match_active_domain(connection, rcpt_to)
                if current_match is None or not self._recipient_match_can_advance(
                    accepted_match,
                    current_match,
                ):
                    raise _RecipientPolicyChangedError(
                        f"recipient policy changed before commit: {rcpt_to}"
                    )
                policy_row = connection.execute(
                    """
                    SELECT retention_days, max_message_size_bytes
                    FROM domains
                    WHERE id = ? AND root_domain_ascii = ? AND is_active = 1
                    """,
                    (current_match.domain_id, current_match.root_domain_ascii),
                ).fetchone()
                if policy_row is None or raw_size_bytes > int(
                    policy_row["max_message_size_bytes"]
                ):
                    raise _RecipientPolicyChangedError(
                        f"recipient policy changed before commit: {rcpt_to}"
                    )
                transaction_matches.append(
                    (rcpt_to, current_match, policy_row["retention_days"])
                )

            if smtp_session_id is not None:
                session_exists = connection.execute(
                    "SELECT 1 FROM smtp_sessions WHERE id = ?",
                    (smtp_session_id,),
                ).fetchone()
                if session_exists is None:
                    connection.execute(
                        """
                        INSERT INTO smtp_sessions (
                            id,
                            remote_ip,
                            remote_port,
                            helo_name,
                            status,
                            tls_used,
                            connect_at,
                            first_command_at,
                            last_command_at,
                            last_rcpt_to_sample
                        ) VALUES (?, ?, ?, ?, 'open', ?, ?, ?, ?, ?)
                        """,
                        (
                            smtp_session_id,
                            "unknown",
                            None,
                            None,
                            0,
                            received_at,
                            received_at,
                            received_at,
                            None,
                        ),
                    )

                self._update_smtp_session_summary(
                    connection,
                    smtp_session_id,
                    received_at,
                    message_delta=1,
                    bytes_received_delta=raw_size_bytes,
                    last_mail_from=envelope_from,
                )

            connection.execute(
                """
                INSERT INTO messages (
                    id,
                    smtp_session_id,
                    raw_path,
                    raw_sha256,
                    raw_size_bytes,
                    envelope_from,
                    from_addr,
                    received_at,
                    parse_status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending')
                """,
                (
                    message_id,
                    smtp_session_id,
                    raw_path,
                    raw_sha256,
                    raw_size_bytes,
                    envelope_from,
                    envelope_from,
                    received_at,
                ),
            )

            # The RCPT matcher is an immutable hot-path cache. Re-evaluate every
            # accepted recipient against the domain rows visible to this write
            # transaction: a newly-created more-specific managed suffix is just
            # as important as a catch-all -> managed transition for ownership
            # and API-key isolation.
            delivery_events: list[dict[str, Any]] = []
            transaction_mailboxes: set[tuple[int, str]] = set()
            for rcpt_to, match, retention_days in transaction_matches:
                mailbox_key = (int(match.domain_id), str(match.address_canonical))
                if mailbox_key in transaction_mailboxes:
                    continue
                transaction_mailboxes.add(mailbox_key)
                mailbox_id = self._upsert_mailbox(connection, match, received_at)
                delivery_id = f"dlv_{uuid.uuid4().hex}"
                expires_at = self._delivery_expires_at(
                    received_at,
                    retention_days,
                )
                connection.execute(
                    """
                    INSERT INTO message_deliveries (
                        id,
                        message_id,
                        mailbox_id,
                        rcpt_to,
                        delivered_at,
                        expires_at,
                        mailbox_generation
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        (SELECT bulk_delete_generation FROM mailboxes WHERE id = ?)
                    )
                    """,
                    (
                        delivery_id,
                        message_id,
                        mailbox_id,
                        rcpt_to,
                        received_at,
                        expires_at,
                        mailbox_id,
                    ),
                )
                delivery_events.append(
                    {
                        "delivery_id": delivery_id,
                        "message_id": message_id,
                        "mailbox": match.address_canonical,
                        "rcpt_to": rcpt_to,
                        "parse_status": "pending",
                        "ts": received_at,
                    }
                )

            self._increment_mail_metric(
                connection,
                received_at,
                received=1,
                deliveries=len(delivery_events),
            )
            return delivery_events

        try:
            delivery_events = await self.writer.execute(operation)
        except _RecipientPolicyChangedError:
            await asyncio.to_thread(
                self._discard_uncommitted_accept_artifacts,
                raw_path,
                manifest_path,
            )
            return "451 recipient policy changed; retry later"
        for event in delivery_events:
            await self.live_state.publish({**event, "type": "mailbox_delivery"})
        # The raw file and database row are already durable.  Parsing is
        # intentionally best-effort here: bounded queue pressure must not turn
        # an accepted message into an SMTP failure.  The periodic pending scan
        # will fairly refill capacity later.
        await self.enqueue_message_for_parse(message_id, raw_size_bytes=raw_size_bytes)
        return f"250 queued as {message_id}"

    def _delivery_expires_at(self, delivered_at: str, retention_days: Any) -> str | None:
        if retention_days is None:
            return None
        days = int(retention_days)
        if days <= 0:
            return None
        timestamp = datetime.fromisoformat(delivered_at.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return (timestamp.astimezone(timezone.utc) + timedelta(days=days)).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z")

    def _write_accept_artifacts(
        self,
        message_id: str,
        received_at: str,
        manifest_payload: dict[str, Any],
        content: bytes,
    ) -> None:
        self.storage.write_raw_message(message_id, received_at, content)
        self.storage.write_manifest(message_id, received_at, manifest_payload)

    def _discard_uncommitted_accept_artifacts(
        self,
        raw_path: str,
        manifest_path: str,
    ) -> None:
        # Remove the recovery receipt first. A crash after that point can leave
        # only an unreferenced raw blob, never a stale policy snapshot that may
        # be replayed into the wrong tenant.
        failures: list[tuple[str, OSError]] = []
        for storage_path in (manifest_path, raw_path):
            try:
                self.storage.resolve(storage_path).unlink(missing_ok=True)
            except OSError as exc:
                failures.append((storage_path, exc))
        if failures:
            logging.getLogger("rapid_inbox.ingest").error(
                "failed to discard artifacts for rejected DATA transaction",
                extra={
                    "event": "smtp.data_rejected_artifact_cleanup_failed",
                    "paths": [path for path, _error in failures],
                    "errors": [str(error) for _path, error in failures],
                },
            )

    @staticmethod
    def _recipient_match_can_advance(
        accepted_match: DomainMatch,
        current_match: DomainMatch,
    ) -> bool:
        if int(accepted_match.domain_id) == int(current_match.domain_id):
            return True
        accepted_root = str(accepted_match.root_domain_ascii)
        current_root = str(current_match.root_domain_ascii)
        if accepted_root == "*":
            return current_root != "*"
        return current_root != "*" and current_root.endswith(f".{accepted_root}")

    async def drain_parser_queue(self) -> None:
        await self.parse_queue.drain()

    async def enqueue_message_for_parse(
        self,
        message_id: str,
        *,
        raw_size_bytes: int | None = None,
    ) -> bool:
        if raw_size_bytes is None:
            raw_size_bytes = await asyncio.to_thread(
                self._raw_size_bytes_for_parse,
                message_id,
            )
            if raw_size_bytes is None:
                return False
        return self.parse_queue.try_enqueue(
            ParseTask(message_id=message_id, raw_size_bytes=raw_size_bytes)
        )

    def _raw_size_bytes_for_parse(self, message_id: str) -> int | None:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                "SELECT raw_size_bytes FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        return None if row is None else int(row["raw_size_bytes"])

    async def requeue_pending_messages_for_parse(self) -> int:
        now = monotonic()
        if now - self._last_manifest_recovery_at >= MANIFEST_RECOVERY_SCAN_INTERVAL_SECONDS:
            self._last_manifest_recovery_at = now
            await self.recovery.recover_missing_manifests(incremental=True)

        # Retention removes queued work, waits for active parsers, commits file
        # tombstones, and performs file GC under this same lock.  Keep both the
        # durable pending-page read and its enqueue inside the critical section
        # so a page read before deletion cannot enqueue a stale task after GC.
        async with self._mail_store_lock:
            queued = 0
            for task in await self._next_pending_parse_task_page():
                if self.parse_queue.try_enqueue(task):
                    queued += 1
            return queued

    async def _next_pending_parse_task_page(self) -> list[ParseTask]:
        # A keyset cursor walks the whole durable pending set even when the
        # first page is already reserved in memory.  OFFSET would skip rows as
        # workers change their status, while repeatedly querying the oldest or
        # newest page can starve the opposite end under sustained ingress.
        tasks, cursor = await asyncio.to_thread(
            self._find_pending_parse_task_page,
            self._pending_parse_scan_cursor,
            PENDING_PARSE_SCAN_BATCH_SIZE,
        )
        self._pending_parse_scan_cursor = cursor
        return tasks

    def _find_pending_parse_task_page(
        self,
        cursor: tuple[str, str] | None,
        limit: int,
    ) -> tuple[list[ParseTask], tuple[str, str] | None]:
        def select_page(
            connection: sqlite3.Connection,
            after: tuple[str, str] | None,
        ) -> list[sqlite3.Row]:
            cursor_sql = ""
            params: list[Any] = []
            if after is not None:
                cursor_sql = "AND (received_at > ? OR (received_at = ? AND id > ?))"
                params.extend((after[0], after[0], after[1]))
            params.append(limit)
            return connection.execute(
                f"""
                SELECT id, raw_size_bytes, received_at
                FROM messages
                WHERE parse_status = 'pending'
                {cursor_sql}
                ORDER BY received_at ASC, id ASC
                LIMIT ?
                """,
                tuple(params),
            ).fetchall()

        with connect_database(self.settings.database_path) as connection:
            rows = select_page(connection, cursor)
            if not rows and cursor is not None:
                rows = select_page(connection, None)
        if not rows:
            return [], None
        tasks = [
            ParseTask(
                message_id=str(row["id"]),
                raw_size_bytes=int(row["raw_size_bytes"]),
            )
            for row in rows
        ]
        last_row = rows[-1]
        return tasks, (str(last_row["received_at"]), str(last_row["id"]))

    async def recover_from_manifest(self, manifest: dict[str, Any]) -> bool:
        return bool(await self.writer.execute(lambda connection: self._apply_recovery_manifest(connection, manifest)))

    async def recover_domain_snapshot(self, snapshot: dict[str, Any]) -> None:
        await self.writer.execute(lambda connection: self._ensure_recovery_domain_record(connection, snapshot, str(snapshot["received_at"])))

    async def recovery_reparse_rowid_cutoff(self) -> int:
        """Return a scalar frontier for failures that predate manifest replay."""

        return await asyncio.to_thread(self._recovery_reparse_rowid_cutoff_sync)

    def _recovery_reparse_rowid_cutoff_sync(self) -> int:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                "SELECT COALESCE(MAX(rowid), 0) AS max_rowid FROM messages"
            ).fetchone()
        return int(row["max_rowid"])

    async def find_failed_reparse_page(
        self,
        *,
        after_rowid: int,
        max_rowid: int,
        limit: int,
    ) -> tuple[list[ParseTask], int | None]:
        return await asyncio.to_thread(
            self._find_failed_reparse_page_sync,
            after_rowid,
            max_rowid,
            limit,
        )

    def _find_failed_reparse_page_sync(
        self,
        after_rowid: int,
        max_rowid: int,
        limit: int,
    ) -> tuple[list[ParseTask], int | None]:
        if limit < 1:
            raise ValueError("recovery reparse page limit must be positive")
        with connect_database(self.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT rowid AS message_rowid, id, raw_size_bytes
                FROM messages NOT INDEXED
                WHERE rowid > ?
                  AND rowid <= ?
                  AND parse_status = 'failed'
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (after_rowid, max_rowid, limit),
            ).fetchall()
        if not rows:
            return [], None
        tasks = [
            ParseTask(
                message_id=str(row["id"]),
                raw_size_bytes=int(row["raw_size_bytes"]),
            )
            for row in rows
        ]
        return tasks, int(rows[-1]["message_rowid"])

    async def enqueue_recovery_parse_task(self, task: ParseTask) -> bool:
        """Backpressure startup reparses instead of dropping a full page."""

        try:
            return await self.parse_queue.enqueue(task)
        except ValueError:
            # A historical message can exceed a newly lowered byte budget. It
            # remains failed for operator inspection instead of aborting startup.
            return False

    def validate_recovery_manifest(self, manifest: Any) -> None:
        if not isinstance(manifest, dict):
            raise ValueError("invalid recovery manifest")

        for key in ("message_id", "received_at", "raw_path", "raw_sha256", "raw_size_bytes"):
            if key not in manifest:
                raise ValueError("invalid recovery manifest")

        if not all(isinstance(manifest[key], str) for key in ("message_id", "received_at", "raw_path", "raw_sha256")):
            raise ValueError("invalid recovery manifest")
        self.storage.resolve(str(manifest["raw_path"]))
        if "recovery_order_ns" in manifest:
            recovery_order_ns = manifest["recovery_order_ns"]
            if not isinstance(recovery_order_ns, int) or isinstance(recovery_order_ns, bool) or recovery_order_ns < 0:
                raise ValueError("invalid recovery manifest")
        raw_size_bytes = manifest["raw_size_bytes"]
        if not isinstance(raw_size_bytes, int) or isinstance(raw_size_bytes, bool):
            raise ValueError("invalid recovery manifest")

        recipients = manifest.get("recipients")
        if recipients is not None:
            if not isinstance(recipients, list) or not recipients:
                raise ValueError("invalid recovery manifest")
            for recipient in recipients:
                if not isinstance(recipient, dict):
                    raise ValueError("invalid recovery manifest")
                if not isinstance(recipient.get("rcpt_to"), str):
                    raise ValueError("invalid recovery manifest")
                if not isinstance(recipient.get("domain_id"), int) or isinstance(recipient.get("domain_id"), bool):
                    raise ValueError("invalid recovery manifest")
                for key in ("domain_ascii", "root_domain_ascii", "local_part_canonical", "address_canonical"):
                    if not isinstance(recipient.get(key), str):
                        raise ValueError("invalid recovery manifest")
                # Recipient manifests are durable authorization snapshots, not
                # merely routing hints.  Reconstructing a deleted domain
                # without the policy that was in force at SMTP acceptance can
                # silently make a private message public.  Legacy ``rcpt_tos``
                # receipts remain recoverable only through an existing domain
                # row below; the structured format must always carry the full
                # policy snapshot.
                if "domain_policy" not in recipient:
                    raise ValueError("invalid recovery manifest")
                self._validate_recovery_domain_policy(recipient["domain_policy"])
        else:
            rcpt_tos = manifest.get("rcpt_tos")
            if not isinstance(rcpt_tos, list) or not rcpt_tos:
                raise ValueError("invalid recovery manifest")
            for rcpt_to in rcpt_tos:
                if not isinstance(rcpt_to, str):
                    raise ValueError("invalid recovery manifest")

        parsed = manifest.get("parsed")
        if parsed is not None:
            self._validate_recovery_parsed_manifest(parsed)

    def _validate_optional_manifest_string(self, parsed: dict[str, Any], key: str) -> None:
        value = parsed.get(key)
        if value is not None and not isinstance(value, str):
            raise ValueError("invalid recovery manifest")

    def _validate_recovery_parsed_manifest(self, parsed: Any) -> None:
        if not isinstance(parsed, dict):
            raise ValueError("invalid recovery manifest")
        status = parsed.get("status")
        if status not in {"parsed", "failed"}:
            raise ValueError("invalid recovery manifest")
        if status == "failed":
            if not isinstance(parsed.get("parse_error"), str) or not parsed["parse_error"]:
                raise ValueError("invalid recovery manifest")
            return

        for key in ("has_text", "has_html", "has_attachments"):
            if not isinstance(parsed.get(key), bool):
                raise ValueError("invalid recovery manifest")
        if not isinstance(parsed.get("attachment_count"), int) or isinstance(parsed.get("attachment_count"), bool):
            raise ValueError("invalid recovery manifest")
        if not isinstance(parsed.get("headers_json"), list):
            raise ValueError("invalid recovery manifest")
        for key in (
            "message_id_header",
            "subject",
            "from_name",
            "from_addr",
            "reply_to",
            "date_header",
            "text_preview",
            "text_body_path",
            "html_body_path",
            "verification_code",
        ):
            self._validate_optional_manifest_string(parsed, key)
        for key in ("text_body_path", "html_body_path"):
            value = parsed.get(key)
            if value is not None:
                self.storage.resolve(str(value))
        attachments = parsed.get("attachments")
        if not isinstance(attachments, list):
            raise ValueError("invalid recovery manifest")
        for attachment in attachments:
            if not isinstance(attachment, dict):
                raise ValueError("invalid recovery manifest")
            for key in ("id", "storage_path", "safe_filename", "content_type"):
                if not isinstance(attachment.get(key), str):
                    raise ValueError("invalid recovery manifest")
            if not isinstance(attachment.get("part_index"), int) or isinstance(attachment.get("part_index"), bool):
                raise ValueError("invalid recovery manifest")
            if not isinstance(attachment.get("size_bytes"), int) or isinstance(attachment.get("size_bytes"), bool):
                raise ValueError("invalid recovery manifest")
            if not isinstance(attachment.get("is_inline"), bool):
                raise ValueError("invalid recovery manifest")
            for key in ("filename", "content_disposition", "content_id", "sha256"):
                value = attachment.get(key)
                if value is not None and not isinstance(value, str):
                    raise ValueError("invalid recovery manifest")
            self.storage.resolve(str(attachment["storage_path"]))

    def _update_smtp_session_summary(
        self,
        connection: sqlite3.Connection,
        session_id: str,
        now: str,
        *,
        accepted_delta: int = 0,
        rejected_delta: int = 0,
        message_delta: int = 0,
        bytes_received_delta: int = 0,
        last_rcpt_to_sample: str | None = None,
        last_mail_from: str | None = None,
        status: str | None = None,
        disconnect_at: str | None = None,
        close_reason: str | None = None,
        result_code: int | None = None,
        result_message: str | None = None,
    ) -> None:
        connection.execute(
            """
            UPDATE smtp_sessions
            SET first_command_at = COALESCE(first_command_at, ?),
                last_command_at = ?,
                message_count = message_count + ?,
                rcpt_accepted_count = rcpt_accepted_count + ?,
                rcpt_rejected_count = rcpt_rejected_count + ?,
                bytes_received = bytes_received + ?,
                last_mail_from = COALESCE(?, last_mail_from),
                last_rcpt_to_sample = COALESCE(?, last_rcpt_to_sample),
                status = COALESCE(?, status),
                disconnect_at = COALESCE(?, disconnect_at),
                close_reason = COALESCE(?, close_reason),
                result_code = COALESCE(?, result_code),
                result_message = COALESCE(?, result_message)
            WHERE id = ?
            """,
            (
                now,
                now,
                message_delta,
                accepted_delta,
                rejected_delta,
                bytes_received_delta,
                last_mail_from,
                last_rcpt_to_sample,
                status,
                disconnect_at,
                close_reason,
                result_code,
                result_message,
                session_id,
            ),
        )

    def _validate_recovery_domain_policy(self, domain_policy: Any) -> None:
        if not isinstance(domain_policy, dict):
            raise ValueError("invalid recovery manifest")

        for key in (
            "root_domain_unicode",
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "is_active",
            "is_hidden",
            "plus_addressing_mode",
            "local_part_case_sensitive",
            "max_message_size_bytes",
            "retention_days",
            "dns_status",
        ):
            if key not in domain_policy:
                raise ValueError("invalid recovery manifest")

        if not isinstance(domain_policy["root_domain_unicode"], str):
            raise ValueError("invalid recovery manifest")
        for key in (
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "is_active",
            "is_hidden",
            "local_part_case_sensitive",
        ):
            if not isinstance(domain_policy[key], bool):
                raise ValueError("invalid recovery manifest")
        if not isinstance(domain_policy["plus_addressing_mode"], str):
            raise ValueError("invalid recovery manifest")
        if domain_policy["plus_addressing_mode"] not in {"keep", "strip"}:
            raise ValueError("invalid recovery manifest")
        if not isinstance(domain_policy["max_message_size_bytes"], int) or isinstance(domain_policy["max_message_size_bytes"], bool):
            raise ValueError("invalid recovery manifest")
        retention_days = domain_policy["retention_days"]
        if retention_days is not None and (
            not isinstance(retention_days, int) or isinstance(retention_days, bool)
        ):
            raise ValueError("invalid recovery manifest")
        if not isinstance(domain_policy["dns_status"], str):
            raise ValueError("invalid recovery manifest")
        if domain_policy["dns_status"] not in {"unknown", "ok", "warning", "error"}:
            raise ValueError("invalid recovery manifest")

    async def find_messages_for_reparse(
        self,
        *,
        statuses: tuple[str, ...] = ("pending", "failed"),
        newest_first: bool = False,
        limit: int | None = None,
    ) -> list[str]:
        if not statuses:
            return []
        return await asyncio.to_thread(
            self._find_messages_for_reparse_sync,
            statuses,
            newest_first,
            limit,
        )

    def _find_messages_for_reparse_sync(
        self,
        statuses: tuple[str, ...],
        newest_first: bool,
        limit: int | None,
    ) -> list[str]:
        placeholders = ", ".join("?" for _ in statuses)
        limit_sql = "" if limit is None else "LIMIT ?"
        order_sql = "ORDER BY received_at DESC, id DESC" if newest_first else "ORDER BY received_at ASC, id ASC"
        params: list[Any] = list(statuses)
        if limit is not None:
            params.append(int(limit))
        with connect_database(self.settings.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT id
                FROM messages
                WHERE parse_status IN ({placeholders})
                {order_sql}
                {limit_sql}
                """,
                tuple(params),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    async def get_mailbox_view(
        self,
        mailbox_address: str,
        *,
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[str, str] | None = None,
        request_ip: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("mailbox domain not managed")

        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_mailbox_view_sync,
            match,
            limit,
            offset,
            cursor,
            surface,
        )

    def _get_mailbox_view_sync(
        self,
        match: DomainMatch,
        limit: int,
        offset: int,
        cursor: tuple[str, str] | None,
        surface: str | None,
    ) -> dict[str, Any]:
        with connect_database(self.settings.database_path) as connection:
            mailbox = self._load_public_mailbox_sync(connection, match, surface=surface)
            cursor_filter = ""
            params: list[Any] = [mailbox["id"]]
            if cursor is not None:
                delivered_at, delivery_id = cursor
                cursor_filter = "AND (d.delivered_at < ? OR (d.delivered_at = ? AND d.id < ?))"
                params.extend([delivered_at, delivered_at, delivery_id])
            page_limit = limit + 1
            params.extend([page_limit, 0 if cursor is not None else offset])
            rows = connection.execute(
                f"""
                SELECT
                    d.id AS delivery_id,
                    d.delivered_at,
                    m.id AS message_id,
                    m.subject,
                    m.from_addr,
                    m.verification_code,
                    m.text_preview,
                    m.text_body_path,
                    m.html_body_path,
                    m.has_attachments,
                    m.parse_status
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.mailbox_id = ? AND d.status = 'active'
                    {cursor_filter}
                ORDER BY d.delivered_at DESC, d.id DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params),
            ).fetchall()

        items = [dict(row) for row in rows[:limit]]
        message_count = int(mailbox["message_count"])
        has_previous = offset > 0
        has_next = len(rows) > limit if cursor is not None else offset + len(items) < message_count
        next_cursor = None
        if has_next and items:
            last_item = items[-1]
            next_cursor = {
                "delivered_at": last_item["delivered_at"],
                "delivery_id": last_item["delivery_id"],
            }

        return {
            "mailbox": match.address_canonical,
            "items": items,
            "message_count": message_count,
            "limit": limit,
            "offset": offset,
            "pagination_mode": "cursor" if cursor is not None else "offset",
            "next_cursor": next_cursor,
            "has_previous": has_previous,
            "has_next": has_next,
            "previous_offset": max(offset - limit, 0) if has_previous else None,
            "next_offset": offset + limit if has_next else None,
        }

    async def get_mailbox_delivery_item(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("mailbox domain not managed")

        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_mailbox_delivery_item_sync,
            match,
            delivery_id,
            surface,
        )

    def _get_mailbox_delivery_item_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        surface: str | None,
    ) -> dict[str, Any]:
        with connect_database(self.settings.database_path) as connection:
            mailbox = self._load_public_mailbox_sync(connection, match, surface=surface)
            row = connection.execute(
                """
                SELECT
                    d.id AS delivery_id,
                    d.delivered_at,
                    m.id AS message_id,
                    m.subject,
                    m.from_addr,
                    m.verification_code,
                    m.text_preview,
                    m.text_body_path,
                    m.html_body_path,
                    m.has_attachments,
                    m.parse_status
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.id = ? AND d.mailbox_id = ? AND d.status = 'active'
                """,
                (delivery_id, mailbox["id"]),
            ).fetchone()
        if row is None:
            raise LookupError("delivery not found")
        return dict(row)

    async def get_delivery_detail(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("mailbox domain not managed")

        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_delivery_detail_sync,
            match,
            delivery_id,
            surface,
        )

    def _get_delivery_detail_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        surface: str | None,
    ) -> dict[str, Any]:
        with connect_database(self.settings.database_path) as connection:
            mailbox = self._load_public_mailbox_sync(connection, match, surface=surface)
            row = connection.execute(
                """
                SELECT
                    d.id AS delivery_id,
                    d.delivered_at,
                    m.id AS message_id,
                    m.subject,
                    m.from_addr,
                    m.verification_code,
                    m.text_body_path,
                    m.html_body_path,
                    m.parse_status,
                    m.raw_path,
                    CASE
                        WHEN COALESCE(LENGTH(CAST(m.headers_json AS BLOB)), 0) <= ?
                        THEN m.headers_json
                        ELSE NULL
                    END AS headers_json,
                    COALESCE(LENGTH(CAST(m.headers_json AS BLOB)), 0) AS headers_source_bytes
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.id = ? AND d.mailbox_id = ? AND d.status = 'active'
                """,
                (self.settings.message_preview_headers_bytes, delivery_id, mailbox["id"]),
            ).fetchone()
            if row is None:
                raise LookupError("delivery not found")
            attachments = connection.execute(
                """
                SELECT
                    id,
                    filename,
                    safe_filename,
                    content_type,
                    storage_path,
                    size_bytes,
                    is_inline
                FROM attachments
                WHERE message_id = ?
                ORDER BY part_index ASC
                """,
                (row["message_id"],),
            ).fetchall()

        text_body, text_truncated, text_source_bytes, text_preview_bytes = self.storage.read_text_preview(
            row["text_body_path"],
            self.settings.message_preview_body_bytes,
        )
        html_body, html_truncated, html_source_bytes, html_preview_bytes = self.storage.read_text_preview(
            row["html_body_path"],
            self.settings.message_preview_body_bytes,
        )
        headers_source_bytes = int(row["headers_source_bytes"] or 0)
        return {
            "delivery_id": row["delivery_id"],
            "message_id": row["message_id"],
            "mailbox": mailbox["address_canonical"],
            "received_at": row["delivered_at"],
            "subject": row["subject"],
            "from_addr": row["from_addr"],
            "verification_code": row["verification_code"],
            "parse_status": row["parse_status"],
            "text_body": text_body,
            "text_body_source_bytes": text_source_bytes,
            "text_body_preview_bytes": text_preview_bytes,
            "text_body_truncated": text_truncated,
            "html_body": html_body,
            "html_body_source_bytes": html_source_bytes,
            "html_body_preview_bytes": html_preview_bytes,
            "html_body_truncated": html_truncated,
            "raw_path": row["raw_path"],
            "headers": json.loads(row["headers_json"] or "[]"),
            "headers_source_bytes": headers_source_bytes,
            "headers_truncated": headers_source_bytes > self.settings.message_preview_headers_bytes,
            "attachments": [dict(attachment) for attachment in attachments],
        }

    async def list_mailbox_verification_codes(
        self,
        mailbox_address: str,
        *,
        limit: int = 50,
        offset: int = 0,
        request_ip: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        mailbox = await self.get_mailbox_view(
            mailbox_address,
            limit=limit,
            offset=offset,
            request_ip=request_ip,
            surface=surface,
        )
        items = [
            {
                "delivery_id": item["delivery_id"],
                "message_id": item["message_id"],
                "received_at": item["delivered_at"],
                "subject": item.get("subject"),
                "from_addr": item.get("from_addr"),
                "parse_status": item.get("parse_status"),
                "verification_code": item.get("verification_code"),
            }
            for item in mailbox["items"]
        ]
        return {**mailbox, "items": items}

    async def get_delivery_verification_code(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
        surface: str | None = None,
    ) -> dict[str, Any]:
        item = await self.get_mailbox_delivery_item(
            mailbox_address,
            delivery_id,
            request_ip=request_ip,
            surface=surface,
        )
        return {
            "delivery_id": item["delivery_id"],
            "message_id": item["message_id"],
            "received_at": item["delivered_at"],
            "parse_status": item["parse_status"],
            "verification_code": item.get("verification_code"),
        }

    async def _authorize_public_mailbox_access(
        self,
        canonical_mailbox_address: str,
        domain_id: int,
        *,
        request_ip: str | None = None,
    ) -> None:
        context = get_active_permission_context()
        if context is None:
            return
        ensure_mailbox_access(context, canonical_mailbox_address, domain_id, "public.read")
        await self.api_keys.record_usage(context, ip=request_ip)

    @staticmethod
    def _validate_public_surface(surface: str | None) -> None:
        if surface is not None and surface not in {"web", "api"}:
            raise ValueError("invalid public surface")

    def _load_public_mailbox_sync(
        self,
        connection: sqlite3.Connection,
        match: DomainMatch,
        *,
        surface: str | None,
    ) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                dom.public_web_enabled,
                dom.public_api_enabled,
                dom.is_hidden AS domain_is_hidden,
                dom.is_active AS domain_is_active,
                mb.id,
                mb.address_canonical,
                mb.message_count,
                mb.public_enabled,
                mb.is_hidden
            FROM domains AS dom
            LEFT JOIN mailboxes AS mb
                ON mb.domain_id = dom.id AND mb.address_canonical = ?
            WHERE dom.id = ?
            """,
            (match.address_canonical, match.domain_id),
        ).fetchone()
        if row is None:
            raise LookupError("mailbox domain not managed")
        if surface is not None:
            if not bool(row["domain_is_active"]) or bool(row["domain_is_hidden"]):
                raise LookupError("mailbox domain not public")
            if surface == "web" and not bool(row["public_web_enabled"]):
                raise LookupError("public web disabled")
            if surface == "api" and not bool(row["public_api_enabled"]):
                raise LookupError("public api disabled")

        if row["id"] is None:
            # Public reads must stay side-effect free.  SMTP delivery (or an
            # explicit management operation) is the only way to materialize a
            # mailbox row; otherwise random anonymous GETs can amplify into
            # unbounded SQLite writes.
            return {
                "id": -1,
                "address_canonical": match.address_canonical,
                "message_count": 0,
                "public_enabled": True,
                "is_hidden": False,
            }

        if not bool(row["public_enabled"]) or bool(row["is_hidden"]):
            raise LookupError("mailbox not public")
        return {
            "id": int(row["id"]),
            "address_canonical": str(row["address_canonical"]),
            "message_count": int(row["message_count"]),
            "public_enabled": bool(row["public_enabled"]),
            "is_hidden": bool(row["is_hidden"]),
        }

    async def get_public_raw_file(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("raw message not found")
        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_public_raw_file_sync,
            match,
            delivery_id,
            surface,
        )

    def _get_public_raw_file_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        surface: str,
    ) -> dict[str, Any]:
        row = self._load_public_raw_resource_sync(match, delivery_id, surface)
        try:
            path = self.storage.resolve(str(row["raw_path"]))
            if not path.is_file():
                raise LookupError("raw message not found")
            size_bytes = path.stat().st_size
        except (OSError, RuntimeError, ValueError) as exc:
            raise LookupError("raw message not found") from exc
        return {
            "path": path,
            "size_bytes": size_bytes,
            "sha256": row["raw_sha256"],
        }

    async def get_public_raw_message(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> bytes:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("raw message not found")
        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_public_raw_message_sync,
            match,
            delivery_id,
            surface,
        )

    def _get_public_raw_message_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        surface: str,
    ) -> bytes:
        row = self._load_public_raw_resource_sync(match, delivery_id, surface)
        try:
            return self.storage.read_bytes(str(row["raw_path"]))
        except (OSError, RuntimeError, ValueError) as exc:
            raise LookupError("raw message not found") from exc

    def _load_public_raw_resource_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        surface: str,
    ) -> sqlite3.Row:
        public_column = self._public_surface_column(surface)
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                f"""
                SELECT m.raw_path, m.raw_sha256
                FROM domains AS dom
                JOIN mailboxes AS mb
                    ON mb.domain_id = dom.id AND mb.address_canonical = ?
                JOIN message_deliveries AS delivery
                    ON delivery.mailbox_id = mb.id
                JOIN messages AS m ON m.id = delivery.message_id
                WHERE dom.id = ?
                  AND dom.is_active = 1
                  AND dom.is_hidden = 0
                  AND dom.{public_column} = 1
                  AND mb.public_enabled = 1
                  AND mb.is_hidden = 0
                  AND delivery.id = ?
                  AND delivery.status = 'active'
                """,
                (match.address_canonical, match.domain_id, delivery_id),
            ).fetchone()
        if row is None:
            raise LookupError("raw message not found")
        return row

    async def get_public_attachment_file(
        self,
        mailbox_address: str,
        delivery_id: str,
        attachment_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("attachment not found")
        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_public_attachment_file_sync,
            match,
            delivery_id,
            attachment_id,
            surface,
        )

    def _get_public_attachment_file_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        attachment_id: str,
        surface: str,
    ) -> dict[str, Any]:
        payload = self._load_public_attachment_resource_sync(
            match,
            delivery_id,
            attachment_id,
            surface,
        )
        try:
            path = self.storage.resolve(str(payload["storage_path"]))
            if not path.is_file():
                raise LookupError("attachment file not found")
        except (OSError, RuntimeError, ValueError) as exc:
            raise LookupError("attachment file not found") from exc
        payload["path"] = path
        return payload

    async def get_public_attachment(
        self,
        mailbox_address: str,
        delivery_id: str,
        attachment_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        match = self.domains.match_address(mailbox_address)
        if match is None:
            raise LookupError("attachment not found")
        self._validate_public_surface(surface)
        await self._authorize_public_mailbox_access(
            match.address_canonical,
            match.domain_id,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._get_public_attachment_sync,
            match,
            delivery_id,
            attachment_id,
            surface,
        )

    def _get_public_attachment_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        attachment_id: str,
        surface: str,
    ) -> dict[str, Any]:
        payload = self._load_public_attachment_resource_sync(
            match,
            delivery_id,
            attachment_id,
            surface,
        )
        try:
            payload["content"] = self.storage.read_bytes(str(payload["storage_path"]))
        except (OSError, RuntimeError, ValueError) as exc:
            raise LookupError("attachment file not found") from exc
        return payload

    def _load_public_attachment_resource_sync(
        self,
        match: DomainMatch,
        delivery_id: str,
        attachment_id: str,
        surface: str,
    ) -> dict[str, Any]:
        public_column = self._public_surface_column(surface)
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                f"""
                SELECT
                    attachment.id,
                    attachment.filename,
                    attachment.safe_filename,
                    attachment.content_type,
                    attachment.content_disposition,
                    attachment.content_id,
                    attachment.storage_path,
                    attachment.size_bytes,
                    attachment.is_inline
                FROM domains AS dom
                JOIN mailboxes AS mb
                    ON mb.domain_id = dom.id AND mb.address_canonical = ?
                JOIN message_deliveries AS delivery
                    ON delivery.mailbox_id = mb.id
                JOIN attachments AS attachment
                    ON attachment.message_id = delivery.message_id
                WHERE dom.id = ?
                  AND dom.is_active = 1
                  AND dom.is_hidden = 0
                  AND dom.{public_column} = 1
                  AND mb.public_enabled = 1
                  AND mb.is_hidden = 0
                  AND delivery.id = ?
                  AND delivery.status = 'active'
                  AND attachment.id = ?
                """,
                (
                    match.address_canonical,
                    match.domain_id,
                    delivery_id,
                    attachment_id,
                ),
            ).fetchone()
        if row is None:
            raise LookupError("attachment not found")
        return dict(row)

    @staticmethod
    def _public_surface_column(surface: str) -> str:
        if surface == "web":
            return "public_web_enabled"
        if surface == "api":
            return "public_api_enabled"
        raise ValueError("invalid public surface")

    async def get_raw_message(self, delivery_id: str) -> bytes:
        return await asyncio.to_thread(self._get_raw_message_sync, delivery_id)

    def _get_raw_message_sync(self, delivery_id: str) -> bytes:
        return self.storage.read_bytes(self.get_raw_message_path(delivery_id))

    def get_raw_message_path(self, delivery_id: str) -> str:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT m.raw_path
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.id = ?
                """,
                (delivery_id,),
            ).fetchone()
        if row is None:
            raise LookupError("delivery not found")
        return str(row["raw_path"])

    async def _parse_message(self, task: ParseTask) -> None:
        message_source = await asyncio.to_thread(
            self._load_parse_message_source,
            task.message_id,
        )
        if message_source is None:
            return
        raw_path, received_at = message_source

        try:
            raw_bytes = await asyncio.to_thread(self.storage.read_bytes, raw_path)
        except Exception as exc:
            attachment_paths = await self.writer.execute(
                lambda connection: self._mark_message_parse_failed(connection, task.message_id, str(exc))
            )
            await asyncio.to_thread(self._delete_attachment_files, attachment_paths)
            await self._publish_mailbox_delivery_updates(task.message_id)
            return

        try:
            parsed = await asyncio.to_thread(
                self.parser.parse_message,
                task.message_id,
                raw_bytes,
                received_at,
            )
        except Exception as exc:
            attachment_paths = await self.writer.execute(
                lambda connection: self._mark_message_parse_failed(connection, task.message_id, str(exc))
            )
            await asyncio.to_thread(self._delete_attachment_files, attachment_paths)
            await self._publish_mailbox_delivery_updates(task.message_id)
            return

        attachment_paths = await self.writer.execute(
            lambda connection: self._apply_parsed_message(connection, task.message_id, parsed)
        )
        await asyncio.to_thread(self._delete_attachment_files, attachment_paths)
        await self._publish_mailbox_delivery_updates(task.message_id)

    def _load_parse_message_source(self, message_id: str) -> tuple[str, str] | None:
        with connect_database(self.settings.database_path) as connection:
            message_row = connection.execute(
                "SELECT raw_path, received_at FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        if message_row is None:
            return None
        return (
            str(message_row["raw_path"]),
            str(message_row["received_at"]),
        )

    def _apply_parsed_message(self, connection: sqlite3.Connection, message_id: str, parsed: ParsedMessage) -> list[str]:
        attachment_paths = self._collect_attachment_storage_paths(connection, message_id)
        connection.execute(
            """
            UPDATE messages
            SET message_id_header = ?,
                subject = ?,
                from_name = ?,
                from_addr = ?,
                reply_to = ?,
                date_header = ?,
                indexed_at = ?,
                parse_status = 'parsed',
                parse_error = NULL,
                has_text = ?,
                has_html = ?,
                has_attachments = ?,
                attachment_count = ?,
                text_preview = ?,
                text_body_path = ?,
                html_body_path = ?,
                headers_json = ?,
                verification_code = ?
            WHERE id = ?
            """,
            (
                parsed.message_id_header,
                parsed.subject,
                parsed.from_name,
                parsed.from_addr,
                parsed.reply_to,
                parsed.date_header,
                utc_now(),
                int(parsed.has_text),
                int(parsed.has_html),
                int(parsed.has_attachments),
                parsed.attachment_count,
                parsed.text_preview,
                parsed.text_body_path,
                parsed.html_body_path,
                parsed.headers_json,
                parsed.verification_code,
                message_id,
            ),
        )

        connection.execute("DELETE FROM attachments WHERE message_id = ?", (message_id,))
        for attachment in parsed.attachments:
            connection.execute(
                """
                INSERT INTO attachments (
                    id,
                    message_id,
                    part_index,
                    filename,
                    safe_filename,
                    content_type,
                    content_disposition,
                    content_id,
                    storage_path,
                    sha256,
                    size_bytes,
                    is_inline,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attachment.attachment_id,
                    message_id,
                    attachment.part_index,
                    attachment.filename,
                    attachment.safe_filename,
                    attachment.content_type,
                    attachment.content_disposition,
                    attachment.content_id,
                    attachment.storage_path,
                    attachment.sha256,
                    attachment.size_bytes,
                    int(attachment.is_inline),
                    utc_now(),
                ),
            )
        return attachment_paths

    def _mark_message_parse_failed(self, connection: sqlite3.Connection, message_id: str, parse_error: str) -> list[str]:
        attachment_paths = self._collect_attachment_storage_paths(connection, message_id)
        row = connection.execute(
            "SELECT received_at, parse_status FROM messages WHERE id = ?",
            (message_id,),
        ).fetchone()
        connection.execute("DELETE FROM attachments WHERE message_id = ?", (message_id,))
        connection.execute(
            """
            UPDATE messages
            SET message_id_header = NULL,
                subject = NULL,
                from_name = NULL,
                from_addr = NULL,
                reply_to = NULL,
                date_header = NULL,
                indexed_at = ?,
                parse_status = 'failed',
                parse_error = ?,
                has_text = 0,
                has_html = 0,
                has_attachments = 0,
                attachment_count = 0,
                text_preview = NULL,
                text_body_path = NULL,
                html_body_path = NULL,
                headers_json = NULL,
                verification_code = NULL
            WHERE id = ?
            """,
            (utc_now(), parse_error, message_id),
        )
        if row is not None and str(row["parse_status"]) != "failed":
            self._increment_mail_metric(connection, str(row["received_at"]), parse_failures=1)
        return attachment_paths

    def _collect_attachment_storage_paths(self, connection: sqlite3.Connection, message_id: str) -> list[str]:
        rows = connection.execute(
            """
            SELECT storage_path
            FROM attachments
            WHERE message_id = ?
            """,
            (message_id,),
        ).fetchall()
        return [str(row["storage_path"]) for row in rows]

    def _delete_attachment_files(self, storage_paths: list[str]) -> None:
        for storage_path in storage_paths:
            try:
                self.storage.resolve(storage_path).unlink(missing_ok=True)
            except Exception:
                continue

    async def _publish_mailbox_delivery_updates(self, message_id: str) -> None:
        events = await asyncio.to_thread(
            self._load_mailbox_delivery_update_events,
            message_id,
        )
        for event in events:
            await self.live_state.publish({**event, "type": "mailbox_delivery_updated"})

    def _load_mailbox_delivery_update_events(self, message_id: str) -> list[dict[str, Any]]:
        with connect_database(self.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    d.id AS delivery_id,
                    d.rcpt_to,
                    d.delivered_at,
                    m.id AS message_id,
                    m.parse_status,
                    mb.address_canonical AS mailbox
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                JOIN mailboxes AS mb ON mb.id = d.mailbox_id
                WHERE d.message_id = ? AND d.status = 'active'
                ORDER BY d.delivered_at ASC, d.id ASC
                """,
                (message_id,),
            ).fetchall()
        return [
            {
                "delivery_id": row["delivery_id"],
                "message_id": row["message_id"],
                "mailbox": row["mailbox"],
                "rcpt_to": row["rcpt_to"],
                "parse_status": row["parse_status"],
                "ts": row["delivered_at"],
            }
            for row in rows
        ]

    def _apply_recovery_manifest(self, connection: sqlite3.Connection, manifest: dict[str, Any]) -> bool:
        message_id = str(manifest["message_id"])
        smtp_session_id = manifest.get("smtp_session_id")
        if smtp_session_id is not None:
            session_exists = connection.execute(
                "SELECT 1 FROM smtp_sessions WHERE id = ?",
                (smtp_session_id,),
            ).fetchone()
            if session_exists is None:
                smtp_session_id = None

        received_at = str(manifest["received_at"])
        raw_path = str(manifest["raw_path"])
        manifest_path = self.storage.manifest_path(message_id, received_at)
        retired = connection.execute(
            """
            SELECT 1
            FROM file_gc_tasks
            WHERE storage_path IN (?, ?)
            LIMIT 1
            """,
            (raw_path, manifest_path),
        ).fetchone()
        if retired is not None:
            return False
        raw_sha256 = str(manifest["raw_sha256"])
        raw_size_bytes = int(manifest["raw_size_bytes"])
        envelope_from = manifest.get("envelope_from")
        recipients = self._recovery_recipients_from_manifest(connection, manifest)
        parsed = manifest.get("parsed")
        parsed_status = parsed.get("status") if isinstance(parsed, dict) else None

        message_columns: dict[str, Any] = {
            "parse_status": "pending",
            "parse_error": None,
            "message_id_header": None,
            "subject": None,
            "from_name": None,
            "from_addr": envelope_from,
            "reply_to": None,
            "date_header": None,
            "indexed_at": None,
            "has_text": 0,
            "has_html": 0,
            "has_attachments": 0,
            "attachment_count": 0,
            "text_preview": None,
            "text_body_path": None,
            "html_body_path": None,
            "headers_json": None,
            "verification_code": None,
        }
        parsed_attachments: list[dict[str, Any]] = []
        if parsed_status == "parsed" and isinstance(parsed, dict):
            message_columns.update(
                {
                    "parse_status": "parsed",
                    "parse_error": None,
                    "message_id_header": parsed.get("message_id_header"),
                    "subject": parsed.get("subject"),
                    "from_name": parsed.get("from_name"),
                    "from_addr": parsed.get("from_addr"),
                    "reply_to": parsed.get("reply_to"),
                    "date_header": parsed.get("date_header"),
                    "indexed_at": received_at,
                    "has_text": int(bool(parsed.get("has_text"))),
                    "has_html": int(bool(parsed.get("has_html"))),
                    "has_attachments": int(bool(parsed.get("has_attachments"))),
                    "attachment_count": int(parsed.get("attachment_count") or 0),
                    "text_preview": parsed.get("text_preview"),
                    "text_body_path": parsed.get("text_body_path"),
                    "html_body_path": parsed.get("html_body_path"),
                    "headers_json": json.dumps(parsed.get("headers_json") or [], ensure_ascii=False),
                    "verification_code": parsed.get("verification_code"),
                }
            )
            parsed_attachments = list(parsed.get("attachments") or [])
        elif parsed_status == "failed" and isinstance(parsed, dict):
            message_columns.update(
                {
                    "parse_status": "failed",
                    "parse_error": parsed.get("parse_error"),
                    "indexed_at": received_at,
                    "from_addr": None,
                }
            )

        inserted_message = connection.execute(
            """
            INSERT OR IGNORE INTO messages (
                id,
                smtp_session_id,
                raw_path,
                raw_sha256,
                raw_size_bytes,
                envelope_from,
                from_addr,
                received_at,
                indexed_at,
                parse_status,
                parse_error,
                message_id_header,
                subject,
                from_name,
                reply_to,
                date_header,
                has_text,
                has_html,
                has_attachments,
                attachment_count,
                text_preview,
                text_body_path,
                html_body_path,
                headers_json,
                verification_code
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                smtp_session_id,
                raw_path,
                raw_sha256,
                raw_size_bytes,
                envelope_from,
                message_columns["from_addr"],
                received_at,
                message_columns["indexed_at"],
                message_columns["parse_status"],
                message_columns["parse_error"],
                message_columns["message_id_header"],
                message_columns["subject"],
                message_columns["from_name"],
                message_columns["reply_to"],
                message_columns["date_header"],
                message_columns["has_text"],
                message_columns["has_html"],
                message_columns["has_attachments"],
                message_columns["attachment_count"],
                message_columns["text_preview"],
                message_columns["text_body_path"],
                message_columns["html_body_path"],
                message_columns["headers_json"],
                message_columns["verification_code"],
            ),
        ).rowcount > 0

        mailbox_ids: set[int] = set()
        inserted_deliveries = 0
        for recipient in recipients:
            mailbox_id = self._ensure_recovery_mailbox_record(connection, recipient, received_at)
            mailbox_ids.add(mailbox_id)
            domain_policy = recipient.get("domain_policy") or {}
            retention_days = domain_policy.get("retention_days")
            domain_row = connection.execute(
                """
                SELECT d.root_domain_ascii, d.retention_days
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.id = ?
                """,
                (mailbox_id,),
            ).fetchone()
            if not domain_policy or (
                domain_row is not None
                and str(domain_row["root_domain_ascii"]) != str(recipient["root_domain_ascii"])
            ):
                retention_days = None if domain_row is None else domain_row["retention_days"]
            expires_at = self._delivery_expires_at(received_at, retention_days)
            delivery_cursor = connection.execute(
                """
                INSERT OR IGNORE INTO message_deliveries (
                    id,
                    message_id,
                    mailbox_id,
                    rcpt_to,
                    delivered_at,
                    expires_at,
                    mailbox_generation
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    (SELECT bulk_delete_generation FROM mailboxes WHERE id = ?)
                )
                """,
                (
                    f"dlv_{uuid.uuid4().hex}",
                    message_id,
                    mailbox_id,
                    recipient["rcpt_to"],
                    received_at,
                    expires_at,
                    mailbox_id,
                ),
            )
            inserted_deliveries += max(int(delivery_cursor.rowcount or 0), 0)

        for mailbox_id in mailbox_ids:
            self._refresh_mailbox_summary(connection, mailbox_id)

        for attachment in parsed_attachments:
            connection.execute(
                """
                INSERT OR IGNORE INTO attachments (
                    id,
                    message_id,
                    part_index,
                    filename,
                    safe_filename,
                    content_type,
                    content_disposition,
                    content_id,
                    storage_path,
                    sha256,
                    size_bytes,
                    is_inline,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attachment["id"],
                    message_id,
                    attachment["part_index"],
                    attachment.get("filename"),
                    attachment["safe_filename"],
                    attachment["content_type"],
                    attachment.get("content_disposition"),
                    attachment.get("content_id"),
                    attachment["storage_path"],
                    attachment.get("sha256"),
                    attachment["size_bytes"],
                    int(bool(attachment["is_inline"])),
                    received_at,
                ),
            )
        if inserted_message or inserted_deliveries:
            self._increment_mail_metric(
                connection,
                received_at,
                received=1 if inserted_message else 0,
                deliveries=inserted_deliveries,
                parse_failures=(
                    1
                    if inserted_message and message_columns["parse_status"] == "failed"
                    else 0
                ),
            )
        return True

    def _recovery_recipient_payload(self, rcpt_to: str, match, domain_policy: dict[str, Any]) -> dict[str, Any]:
        return {
            "rcpt_to": rcpt_to,
            "domain_id": match.domain_id,
            "domain_ascii": match.domain_ascii,
            "root_domain_ascii": match.root_domain_ascii,
            "local_part_canonical": match.local_part_canonical,
            "address_canonical": match.address_canonical,
            "domain_policy": domain_policy,
        }

    def _load_recovery_domain_policies(self, matches: list[tuple[str, Any]]) -> dict[int, dict[str, Any]]:
        domain_policies: dict[int, dict[str, Any]] = {}
        with connect_database(self.settings.database_path) as connection:
            for _, match in matches:
                if match.domain_id not in domain_policies:
                    domain_policies[match.domain_id] = self._load_recovery_domain_policy(connection, match.domain_id)
        return domain_policies

    def _load_recovery_domain_policy(self, connection: sqlite3.Connection, domain_id: int) -> dict[str, Any]:
        row = connection.execute(
            """
            SELECT
                root_domain_ascii,
                root_domain_unicode,
                accept_exact,
                accept_subdomains,
                public_web_enabled,
                public_api_enabled,
                is_active,
                is_hidden,
                plus_addressing_mode,
                local_part_case_sensitive,
                max_message_size_bytes,
                retention_days,
                dns_status
            FROM domains
            WHERE id = ?
            """,
            (domain_id,),
        ).fetchone()
        if row is None:
            raise LookupError(f"domain not found: {domain_id}")
        return {
            "root_domain_unicode": row["root_domain_unicode"] or row["root_domain_ascii"],
            "accept_exact": bool(row["accept_exact"]),
            "accept_subdomains": bool(row["accept_subdomains"]),
            "public_web_enabled": bool(row["public_web_enabled"]),
            "public_api_enabled": bool(row["public_api_enabled"]),
            "is_active": bool(row["is_active"]),
            "is_hidden": bool(row["is_hidden"]),
            "plus_addressing_mode": row["plus_addressing_mode"],
            "local_part_case_sensitive": bool(row["local_part_case_sensitive"]),
            "max_message_size_bytes": int(row["max_message_size_bytes"]),
            "retention_days": row["retention_days"],
            "dns_status": row["dns_status"],
        }

    def _recovery_recipients_from_manifest(
        self,
        connection: sqlite3.Connection,
        manifest: dict[str, Any],
    ) -> list[dict[str, Any]]:
        recipients = manifest.get("recipients")
        if recipients is not None:
            if not isinstance(recipients, list) or not recipients:
                raise ValueError("invalid recovery manifest recipients")
            return [self._coerce_recovery_recipient(recipient) for recipient in recipients]

        rcpt_tos = manifest.get("rcpt_tos")
        if not isinstance(rcpt_tos, list) or not rcpt_tos:
            raise ValueError("invalid recovery manifest rcpt_tos")
        return [self._resolve_legacy_recovery_recipient(connection, str(rcpt_to)) for rcpt_to in rcpt_tos]

    def _coerce_recovery_recipient(self, recipient: Any) -> dict[str, Any]:
        if not isinstance(recipient, dict):
            raise ValueError("invalid recovery manifest recipient")
        try:
            domain_policy = recipient.get("domain_policy")
            return {
                "rcpt_to": str(recipient["rcpt_to"]),
                "domain_id": int(recipient["domain_id"]),
                "domain_ascii": str(recipient["domain_ascii"]),
                "root_domain_ascii": str(recipient["root_domain_ascii"]),
                "local_part_canonical": str(recipient["local_part_canonical"]),
                "address_canonical": str(recipient["address_canonical"]),
                "domain_policy": self._coerce_recovery_domain_policy(domain_policy) if domain_policy is not None else None,
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid recovery manifest recipient") from exc

    def _coerce_recovery_domain_policy(self, domain_policy: Any) -> dict[str, Any]:
        self._validate_recovery_domain_policy(domain_policy)
        return {
            "root_domain_unicode": str(domain_policy["root_domain_unicode"]),
            "accept_exact": bool(domain_policy["accept_exact"]),
            "accept_subdomains": bool(domain_policy["accept_subdomains"]),
            "public_web_enabled": bool(domain_policy["public_web_enabled"]),
            "public_api_enabled": bool(domain_policy["public_api_enabled"]),
            "is_active": bool(domain_policy["is_active"]),
            "is_hidden": bool(domain_policy["is_hidden"]),
            "plus_addressing_mode": str(domain_policy["plus_addressing_mode"]),
            "local_part_case_sensitive": bool(domain_policy["local_part_case_sensitive"]),
            "max_message_size_bytes": int(domain_policy["max_message_size_bytes"]),
            "retention_days": domain_policy["retention_days"],
            "dns_status": str(domain_policy["dns_status"]),
        }

    def _resolve_legacy_recovery_recipient(
        self,
        connection: sqlite3.Connection,
        rcpt_to: str,
    ) -> dict[str, Any]:
        rows = connection.execute(
            """
            SELECT
                id,
                root_domain_ascii,
                accept_exact,
                accept_subdomains,
                plus_addressing_mode,
                local_part_case_sensitive
            FROM domains
            ORDER BY LENGTH(root_domain_ascii) DESC, id ASC
            """
        ).fetchall()
        match = DomainMatcher(
            [
                DomainRule(
                    domain_id=row["id"],
                    root_domain_ascii=row["root_domain_ascii"],
                    accept_exact=bool(row["accept_exact"]),
                    accept_subdomains=bool(row["accept_subdomains"]),
                    plus_addressing_mode=row["plus_addressing_mode"],
                    local_part_case_sensitive=bool(row["local_part_case_sensitive"]),
                )
                for row in rows
            ]
        ).match_address(rcpt_to)
        if match is None:
            raise ValueError(f"unable to recover recipient: {rcpt_to}")
        try:
            domain_policy = self._load_recovery_domain_policy(connection, match.domain_id)
        except LookupError as exc:
            raise ValueError(f"unable to recover recipient: {rcpt_to}") from exc
        return self._recovery_recipient_payload(rcpt_to, match, domain_policy)

    async def _ensure_mailbox_exists(self, match) -> None:
        def operation(connection: sqlite3.Connection) -> None:
            transaction_match = match
            if match.root_domain_ascii == "*":
                transaction_match = match_active_domain(
                    connection,
                    f"{match.local_part}@{match.domain_ascii}",
                ) or match
            if self._find_or_rehome_mailbox(connection, transaction_match, utc_now()) is not None:
                return
            try:
                self._insert_mailbox(
                    connection,
                    transaction_match,
                    utc_now(),
                    message_count=0,
                    latest_message_at=None,
                )
            except sqlite3.IntegrityError:
                existing = connection.execute(
                    "SELECT id FROM mailboxes WHERE address_canonical = ?",
                    (transaction_match.address_canonical,),
                ).fetchone()
                if existing is None:
                    raise

        await self.writer.execute(operation)

    def _upsert_mailbox(self, connection: sqlite3.Connection, match, received_at: str) -> int:
        mailbox_id = self._find_or_rehome_mailbox(connection, match, received_at)
        if mailbox_id is not None:
            connection.execute(
                """
                UPDATE mailboxes
                SET last_seen_at = ?,
                    latest_message_at = ?,
                    message_count = message_count + 1
                WHERE id = ?
                """,
                (received_at, received_at, mailbox_id),
            )
            return mailbox_id

        return self._insert_mailbox(connection, match, received_at, message_count=1, latest_message_at=received_at)

    def _ensure_mailbox_record(self, connection: sqlite3.Connection, match, received_at: str) -> int:
        mailbox_id = self._find_or_rehome_mailbox(connection, match, received_at)
        if mailbox_id is not None:
            return mailbox_id

        return self._insert_mailbox(connection, match, received_at, message_count=0, latest_message_at=None)

    def _find_or_rehome_mailbox(
        self,
        connection: sqlite3.Connection,
        match: DomainMatch,
        changed_at: str,
    ) -> int | None:
        existing = connection.execute(
            """
            SELECT m.id, d.root_domain_ascii
            FROM mailboxes AS m
            JOIN domains AS d ON d.id = m.domain_id
            WHERE m.address_canonical = ?
            """,
            (match.address_canonical,),
        ).fetchone()
        if existing is None:
            return None

        mailbox_id = int(existing["id"])
        result = promote_mailbox_ownership(connection, mailbox_id, match)
        if result["rehomed"]:
            self._record_mailbox_rehome_audit(
                connection,
                source_mailbox_id=mailbox_id,
                result=result,
                match=match,
                changed_at=changed_at,
                reason="smtp.write",
            )
        return int(result["mailbox_id"])

    def _record_mailbox_rehome_audit(
        self,
        connection: sqlite3.Connection,
        *,
        source_mailbox_id: int,
        result: dict[str, int],
        match: DomainMatch,
        changed_at: str,
        reason: str,
    ) -> None:
        connection.execute(
            """
            INSERT INTO audit_logs (
                actor_type, actor_ref, action, resource_type, resource_ref,
                status, details_json, created_at
            ) VALUES ('system', ?, 'mailboxes.rehome', 'mailbox', ?, 'success', ?, ?)
            """,
            (
                "recovery" if reason == "manifest.recovery" else "smtp-ingest",
                str(result["mailbox_id"]),
                json.dumps(
                    {
                        "source_mailbox_id": source_mailbox_id,
                        "destination_mailbox_id": result["mailbox_id"],
                        "destination_domain_id": match.domain_id,
                        "deliveries_moved": result["moved"],
                        "deliveries_deduplicated": result["deduplicated"],
                        "reason": reason,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                changed_at,
            ),
        )

    def _refresh_mailbox_summary(self, connection: sqlite3.Connection, mailbox_id: int) -> None:
        summary = connection.execute(
            """
            SELECT
                COUNT(*) AS message_count,
                MIN(delivered_at) AS first_seen_at,
                MAX(delivered_at) AS latest_message_at
            FROM message_deliveries
            WHERE mailbox_id = ? AND status = 'active'
            """,
            (mailbox_id,),
        ).fetchone()
        connection.execute(
            """
            UPDATE mailboxes
            SET first_seen_at = ?,
                last_seen_at = ?,
                latest_message_at = ?,
                message_count = ?
            WHERE id = ?
            """,
            (
                summary["first_seen_at"],
                summary["latest_message_at"],
                summary["latest_message_at"],
                int(summary["message_count"]),
                mailbox_id,
            ),
        )

    def _insert_mailbox(
        self,
        connection: sqlite3.Connection,
        match,
        received_at: str,
        *,
        message_count: int,
        latest_message_at: str | None,
    ) -> int:
        cursor = self._insert_mailbox_from_values(
            connection,
            domain_id=match.domain_id,
            local_part_canonical=match.local_part_canonical,
            rcpt_domain_ascii=match.domain_ascii,
            address_canonical=match.address_canonical,
            address_display=match.address_canonical,
            received_at=received_at,
            message_count=message_count,
            latest_message_at=latest_message_at,
        )
        return int(cursor.lastrowid)

    def _ensure_recovery_mailbox_record(
        self,
        connection: sqlite3.Connection,
        recipient: dict[str, Any],
        received_at: str,
    ) -> int:
        domain_id = self._ensure_recovery_domain_record(connection, recipient, received_at)
        current_match = match_active_domain(connection, str(recipient["rcpt_to"]))
        if current_match is not None and (
            current_match.root_domain_ascii != "*"
            or str(recipient["root_domain_ascii"]) == "*"
        ):
            ownership_match = current_match
        else:
            rcpt_to = str(recipient["rcpt_to"])
            ownership_match = DomainMatch(
                domain_id=domain_id,
                domain_ascii=str(recipient["domain_ascii"]),
                root_domain_ascii=str(recipient["root_domain_ascii"]),
                local_part=rcpt_to.rsplit("@", 1)[0],
                local_part_canonical=str(recipient["local_part_canonical"]),
                address_canonical=str(recipient["address_canonical"]),
            )

        candidate_addresses = tuple(
            dict.fromkeys(
                (
                    ownership_match.address_canonical,
                    str(recipient["address_canonical"]),
                )
            )
        )
        existing_target_id: int | None = None
        for candidate_address in candidate_addresses:
            existing = connection.execute(
                """
                SELECT m.id, d.root_domain_ascii
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.address_canonical = ?
                """,
                (candidate_address,),
            ).fetchone()
            if existing is None:
                continue
            mailbox_id = int(existing["id"])
            if ownership_match.root_domain_ascii != "*":
                result = promote_mailbox_ownership(connection, mailbox_id, ownership_match)
                if result["rehomed"]:
                    self._record_mailbox_rehome_audit(
                        connection,
                        source_mailbox_id=mailbox_id,
                        result=result,
                        match=ownership_match,
                        changed_at=received_at,
                        reason="manifest.recovery",
                    )
                    return int(result["mailbox_id"])
            if candidate_address == ownership_match.address_canonical:
                # Managed ownership is monotonic even if the current routing
                # rule temporarily resolves to catch-all.  Keep looking for a
                # differently-canonicalized catch-all source that must be
                # merged into this target before returning it.
                existing_target_id = mailbox_id

        if existing_target_id is not None:
            return existing_target_id

        return self._insert_mailbox(
            connection,
            ownership_match,
            received_at,
            message_count=0,
            latest_message_at=None,
        )

    def _ensure_recovery_domain_record(
        self,
        connection: sqlite3.Connection,
        recipient: dict[str, Any],
        received_at: str,
    ) -> int:
        root_domain_ascii = str(recipient["root_domain_ascii"])
        existing = connection.execute(
            "SELECT id FROM domains WHERE root_domain_ascii = ?",
            (root_domain_ascii,),
        ).fetchone()
        domain_id = int(recipient["domain_id"])
        if existing is not None and int(existing["id"]) == domain_id:
            return int(existing["id"])

        tombstone = connection.execute(
            "SELECT 1 FROM system_settings WHERE key = ?",
            (domain_routing_tombstone_key(domain_id, root_domain_ascii),),
        ).fetchone()
        if tombstone is not None:
            raise RecoveryPolicyConflictError(
                "durable recipient policy was renamed or deleted after SMTP acceptance"
            )
        if existing is not None:
            return int(existing["id"])

        domain_policy = recipient.get("domain_policy")
        if domain_policy is None:
            # Defense in depth for direct callers that bypass manifest
            # validation.  There is no safe policy to infer here: a permissive
            # default would turn a missing authorization snapshot into public
            # access, while a guessed private policy could still corrupt the
            # accepted routing contract.
            raise RecoveryPolicyConflictError(
                "durable recipient is missing its domain policy snapshot"
            )
        id_owner = connection.execute(
            "SELECT id, root_domain_ascii FROM domains WHERE id = ?",
            (domain_id,),
        ).fetchone()
        if id_owner is not None and str(id_owner["root_domain_ascii"]) == root_domain_ascii:
            return int(id_owner["id"])

        # Numeric IDs embedded in durable receipts are historical hints, not
        # identities.  If that number now belongs to another root, let SQLite
        # allocate a fresh AUTOINCREMENT ID instead of attaching this mailbox
        # and delivery to an unrelated domain.
        preferred_id = domain_id if id_owner is None else None
        return self._insert_recovery_domain_record(
            connection,
            preferred_id=preferred_id,
            root_domain_ascii=root_domain_ascii,
            domain_policy=domain_policy,
            received_at=received_at,
        )

    def _insert_recovery_domain_record(
        self,
        connection: sqlite3.Connection,
        *,
        preferred_id: int | None,
        root_domain_ascii: str,
        domain_policy: dict[str, Any],
        received_at: str,
    ) -> int:
        columns = [
            "root_domain_ascii",
            "root_domain_unicode",
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "is_active",
            "is_hidden",
            "local_part_case_sensitive",
            "plus_addressing_mode",
            "max_message_size_bytes",
            "retention_days",
            "dns_status",
            "dns_last_checked_at",
            "dns_details_json",
            "notes",
            "created_by_admin_id",
            "updated_by_admin_id",
            "created_at",
            "updated_at",
        ]
        values: list[Any] = [
            root_domain_ascii,
            domain_policy["root_domain_unicode"] or root_domain_ascii,
            int(domain_policy["accept_exact"]),
            int(domain_policy["accept_subdomains"]),
            int(domain_policy["public_web_enabled"]),
            int(domain_policy["public_api_enabled"]),
            int(domain_policy["is_active"]),
            int(domain_policy["is_hidden"]),
            int(domain_policy["local_part_case_sensitive"]),
            domain_policy["plus_addressing_mode"],
            int(domain_policy["max_message_size_bytes"]),
            domain_policy["retention_days"],
            domain_policy["dns_status"],
            None,
            None,
            None,
            None,
            None,
            received_at,
            received_at,
        ]
        if preferred_id is not None:
            columns.insert(0, "id")
            values.insert(0, preferred_id)
        placeholders = ", ".join("?" for _ in values)
        cursor = connection.execute(
            f"INSERT INTO domains ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(values),
        )
        return int(cursor.lastrowid)

    def _insert_mailbox_from_values(
        self,
        connection: sqlite3.Connection,
        *,
        domain_id: int,
        local_part_canonical: str,
        rcpt_domain_ascii: str,
        address_canonical: str,
        address_display: str,
        received_at: str,
        message_count: int,
        latest_message_at: str | None,
    ) -> sqlite3.Cursor:
        return connection.execute(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                domain_id,
                local_part_canonical,
                rcpt_domain_ascii,
                address_canonical,
                address_display,
                received_at,
                received_at,
                latest_message_at,
                message_count,
            ),
        )
