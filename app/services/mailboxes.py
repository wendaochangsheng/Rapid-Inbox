from __future__ import annotations

import asyncio
import logging
import sqlite3
import uuid
from typing import Any

from app.auth.api_keys import ApiKeyAuthorizationError
from app.auth.permissions import (
    PermissionContext,
    PermissionDenied,
    ensure_mailbox_access,
)
from app.db.connection import connect_database
from app.ingest.storage import utc_now


MAILBOX_BULK_DELETE_BATCH_SIZE = 1000

_logger = logging.getLogger("rapid_inbox.mailboxes")


class _MailboxBulkDeletePaused(RuntimeError):
    """Internal cooperative stop after the current persisted batch."""


class MailboxService:
    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime
        self._bulk_delete_tasks: dict[str, asyncio.Task[dict[str, int]]] = {}
        self._closing = False

    def list_mailboxes(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str | None = None,
        domain_id: int | None = None,
        public_enabled: bool | None = None,
        is_hidden: bool | None = None,
        allowed_domain_ids: tuple[int, ...] | None = None,
        allowed_mailbox_patterns: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        where_sql, params = self._mailbox_filter_sql(
            query=query,
            domain_id=domain_id,
            public_enabled=public_enabled,
            is_hidden=is_hidden,
            allowed_domain_ids=allowed_domain_ids,
            allowed_mailbox_patterns=allowed_mailbox_patterns,
        )
        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    m.id,
                    m.domain_id,
                    d.root_domain_ascii,
                    m.local_part_canonical,
                    m.rcpt_domain_ascii,
                    m.address_canonical,
                    m.address_display,
                    m.first_seen_at,
                    m.last_seen_at,
                    m.latest_message_at,
                    m.message_count,
                    m.public_enabled,
                    m.is_hidden,
                    m.notes
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                {where_sql}
                ORDER BY m.latest_message_at DESC, m.id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        return {"items": [self._normalize_mailbox_row(row) for row in rows]}

    def count_mailboxes(
        self,
        *,
        query: str | None = None,
        domain_id: int | None = None,
        public_enabled: bool | None = None,
        is_hidden: bool | None = None,
        allowed_domain_ids: tuple[int, ...] | None = None,
        allowed_mailbox_patterns: tuple[str, ...] = (),
    ) -> int:
        where_sql, params = self._mailbox_filter_sql(
            query=query,
            domain_id=domain_id,
            public_enabled=public_enabled,
            is_hidden=is_hidden,
            allowed_domain_ids=allowed_domain_ids,
            allowed_mailbox_patterns=allowed_mailbox_patterns,
        )
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                {where_sql}
                """,
                tuple(params),
            ).fetchone()
        return 0 if row is None else int(row["count"])

    def get_mailbox(self, mailbox_id: int) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    m.id,
                    m.domain_id,
                    d.root_domain_ascii,
                    m.local_part_canonical,
                    m.rcpt_domain_ascii,
                    m.address_canonical,
                    m.address_display,
                    m.first_seen_at,
                    m.last_seen_at,
                    m.latest_message_at,
                    m.message_count,
                    m.public_enabled,
                    m.is_hidden,
                    m.notes
                FROM mailboxes AS m
                JOIN domains AS d ON d.id = m.domain_id
                WHERE m.id = ?
                """,
                (mailbox_id,),
            ).fetchone()
        if row is None:
            raise LookupError("mailbox not found")
        return self._normalize_mailbox_row(row)

    def list_mailbox_deliveries(self, mailbox_id: int, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        self.get_mailbox(mailbox_id)
        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    d.id AS delivery_id,
                    d.rcpt_to,
                    d.delivered_at,
                    d.status,
                    d.deleted_at,
                    m.id AS message_id,
                    m.subject,
                    m.from_addr,
                    m.parse_status,
                    m.has_attachments,
                    m.attachment_count
                FROM message_deliveries AS d
                JOIN messages AS m ON m.id = d.message_id
                WHERE d.mailbox_id = ?
                ORDER BY d.delivered_at DESC, d.id DESC
                LIMIT ? OFFSET ?
                """,
                (mailbox_id, limit, offset),
            ).fetchall()
            total = connection.execute(
                "SELECT COUNT(*) AS count FROM message_deliveries WHERE mailbox_id = ?",
                (mailbox_id,),
            ).fetchone()
        return {
            "items": [dict(row) for row in rows],
            "total_count": 0 if total is None else int(total["count"]),
        }

    def _transaction_authorized_mailbox(
        self,
        connection: sqlite3.Connection,
        mailbox_id: int,
        authorization_principal: PermissionContext | None,
    ) -> sqlite3.Row:
        """Reload the actor and mailbox policy in the caller's write transaction."""

        current_principal = self._runtime.api_keys.transaction_authorization_principal(
            connection,
            authorization_principal,
            required_scope="mailboxes.write",
        )
        mailbox = connection.execute(
            """
            SELECT id, domain_id, address_canonical, bulk_delete_generation
            FROM mailboxes
            WHERE id = ?
            """,
            (mailbox_id,),
        ).fetchone()
        if mailbox is None:
            raise LookupError("mailbox not found")
        if current_principal is not None:
            try:
                ensure_mailbox_access(
                    current_principal,
                    str(mailbox["address_canonical"]),
                    int(mailbox["domain_id"]),
                    "mailboxes.write",
                )
            except PermissionDenied as exc:
                raise ApiKeyAuthorizationError(str(exc.detail)) from exc
        return mailbox

    async def update_mailbox(
        self,
        mailbox_id: int,
        payload: dict[str, Any],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("invalid mailbox payload")

        assignments: list[str] = []
        values: list[Any] = []
        if "public_enabled" in payload:
            assignments.append("public_enabled = ?")
            values.append(int(bool(payload["public_enabled"])))
        if "is_hidden" in payload:
            assignments.append("is_hidden = ?")
            values.append(int(bool(payload["is_hidden"])))
        if "notes" in payload:
            assignments.append("notes = ?")
            values.append(None if payload["notes"] is None else str(payload["notes"]))

        if not assignments:
            return await asyncio.to_thread(self.get_mailbox, mailbox_id)

        def operation(connection: sqlite3.Connection) -> None:
            # Reserve the write transaction before reloading the request-time
            # snapshot. A cross-process key revocation or grant narrowing can
            # therefore never land between the final check and this update.
            connection.execute("BEGIN IMMEDIATE")
            self._transaction_authorized_mailbox(
                connection,
                mailbox_id,
                authorization_principal,
            )
            connection.execute(
                f"UPDATE mailboxes SET {', '.join(assignments)} WHERE id = ?",
                (*values, mailbox_id),
            )

        await self._runtime.writer.execute(operation)
        return await asyncio.to_thread(self.get_mailbox, mailbox_id)

    def _create_or_resume_bulk_delete_job(
        self,
        connection: sqlite3.Connection,
        mailbox_id: int,
        *,
        deleted_at: str,
        authorization_principal: PermissionContext | None = None,
    ) -> str:
        # Authorization is part of the short durable job-creation transaction.
        # Persisted worker pages and crash recovery intentionally need no actor
        # credential after this atomic boundary.
        connection.execute("BEGIN IMMEDIATE")
        mailbox = self._transaction_authorized_mailbox(
            connection,
            mailbox_id,
            authorization_principal,
        )

        incomplete = connection.execute(
            """
            SELECT id
            FROM mailbox_bulk_delete_jobs
            WHERE mailbox_id = ?
              AND status IN ('pending', 'running', 'failed')
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            """,
            (mailbox_id,),
        ).fetchone()
        if incomplete is not None:
            return str(incomplete["id"])

        target_generation = int(mailbox["bulk_delete_generation"])
        if target_generation >= 9_223_372_036_854_775_806:
            raise OverflowError("mailbox bulk delete generation exhausted")
        advanced = connection.execute(
            """
            UPDATE mailboxes
            SET bulk_delete_generation = ?
            WHERE id = ? AND bulk_delete_generation = ?
            """,
            (target_generation + 1, mailbox_id, target_generation),
        )
        if int(advanced.rowcount or 0) != 1:
            raise RuntimeError("mailbox bulk delete generation changed unexpectedly")

        # One retained successful row per mailbox is enough for diagnostics;
        # the next explicit delete replaces it so metadata cannot grow with
        # repeated clears of the same mailbox.
        connection.execute(
            """
            DELETE FROM mailbox_bulk_delete_jobs
            WHERE mailbox_id = ? AND status = 'succeeded'
            """,
            (mailbox_id,),
        )
        frontier = connection.execute(
            """
            SELECT COALESCE(MAX(rowid), 0) AS max_rowid
            FROM message_deliveries
            WHERE mailbox_id = ?
              AND status = 'active'
              AND mailbox_generation = ?
            """,
            (mailbox_id, target_generation),
        ).fetchone()
        max_delivery_rowid = 0 if frontier is None else int(frontier["max_rowid"])
        job_id = f"mbdj_{uuid.uuid4().hex}"
        connection.execute(
            """
            INSERT INTO mailbox_bulk_delete_jobs (
                id,
                mailbox_id,
                status,
                cursor_delivery_rowid,
                max_delivery_rowid,
                target_generation,
                deleted_count,
                deleted_at,
                created_at,
                updated_at
            ) VALUES (?, ?, 'pending', 0, ?, ?, 0, ?, ?, ?)
            """,
            (
                job_id,
                mailbox_id,
                max_delivery_rowid,
                target_generation,
                deleted_at,
                deleted_at,
                deleted_at,
            ),
        )
        return job_id

    def _process_bulk_delete_job_batch(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        *,
        batch_size: int = MAILBOX_BULK_DELETE_BATCH_SIZE,
    ) -> dict[str, Any]:
        """Advance one mailbox clear in one bounded write transaction."""

        if batch_size < 1 or batch_size > MAILBOX_BULK_DELETE_BATCH_SIZE:
            raise ValueError("invalid mailbox bulk delete batch size")
        connection.execute("BEGIN IMMEDIATE")
        job = connection.execute(
            """
            SELECT
                id,
                mailbox_id,
                status,
                cursor_delivery_rowid,
                max_delivery_rowid,
                target_generation,
                deleted_count,
                deleted_at
            FROM mailbox_bulk_delete_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if job is None:
            raise LookupError("mailbox bulk delete job not found")
        if str(job["status"]) == "succeeded":
            return {
                "complete": True,
                "batch_deleted": 0,
                "deleted": int(job["deleted_count"]),
            }

        mailbox_id = int(job["mailbox_id"])
        cursor_rowid = int(job["cursor_delivery_rowid"])
        max_rowid = int(job["max_delivery_rowid"])
        target_generation = int(job["target_generation"])
        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _mailbox_bulk_delete_batch (
                delivery_rowid INTEGER PRIMARY KEY
            ) WITHOUT ROWID
            """
        )
        connection.execute("DELETE FROM _mailbox_bulk_delete_batch")
        connection.execute(
            """
            INSERT INTO _mailbox_bulk_delete_batch (delivery_rowid)
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
            (mailbox_id, target_generation, cursor_rowid, max_rowid, batch_size),
        )
        batch = connection.execute(
            """
            SELECT COUNT(*) AS count, MAX(delivery_rowid) AS max_rowid
            FROM _mailbox_bulk_delete_batch
            """
        ).fetchone()
        batch_count = int(batch["count"])
        next_cursor = max_rowid if batch_count == 0 else int(batch["max_rowid"])
        changed = 0
        if batch_count:
            cursor = connection.execute(
                """
                UPDATE message_deliveries
                SET status = 'deleted',
                    deleted_at = COALESCE(deleted_at, ?),
                    expires_at = ?
                WHERE mailbox_id = ?
                  AND status = 'active'
                  AND mailbox_generation = ?
                  AND rowid IN (
                      SELECT delivery_rowid FROM _mailbox_bulk_delete_batch
                  )
                """,
                (
                    str(job["deleted_at"]),
                    str(job["deleted_at"]),
                    mailbox_id,
                    target_generation,
                ),
            )
            changed = int(cursor.rowcount or 0)

        deleted = int(job["deleted_count"]) + changed
        complete = batch_count < batch_size or next_cursor >= max_rowid
        updated_at = utc_now()
        if complete:
            # All page transactions have committed independently; refresh the
            # derived summary exactly once at the durable completion boundary.
            self._runtime._refresh_mailbox_summary_after_message_delete(
                connection,
                mailbox_id,
            )
        connection.execute(
            """
            UPDATE mailbox_bulk_delete_jobs
            SET status = ?,
                cursor_delivery_rowid = ?,
                deleted_count = ?,
                updated_at = ?,
                finished_at = ?,
                error = NULL
            WHERE id = ?
            """,
            (
                "succeeded" if complete else "running",
                next_cursor,
                deleted,
                updated_at,
                updated_at if complete else None,
                job_id,
            ),
        )
        connection.execute("DELETE FROM _mailbox_bulk_delete_batch")
        return {
            "complete": complete,
            "batch_deleted": changed,
            "deleted": deleted,
        }

    async def _yield_between_bulk_delete_batches(self) -> None:
        # The writer commits the previous page before this yield, allowing
        # already-admitted SMTP/API mutations to run before the next page.
        await asyncio.sleep(0)

    async def _mark_bulk_delete_job_failed(
        self,
        job_id: str,
        error: BaseException,
    ) -> None:
        failed_at = utc_now()
        message = f"{error.__class__.__name__}: {error}"[:2000]

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                UPDATE mailbox_bulk_delete_jobs
                SET status = 'failed',
                    updated_at = ?,
                    finished_at = ?,
                    error = ?
                WHERE id = ? AND status != 'succeeded'
                """,
                (failed_at, failed_at, message, job_id),
            )

        try:
            await self._runtime.writer.execute(operation)
        except Exception:
            _logger.exception(
                "failed to persist mailbox bulk delete failure",
                extra={"job_id": job_id},
            )

    async def _run_bulk_delete_job(self, job_id: str) -> dict[str, int]:
        try:
            while True:
                if self._closing:
                    raise _MailboxBulkDeletePaused("mailbox service is closing")
                result = await self._runtime.writer.execute(
                    lambda connection: self._process_bulk_delete_job_batch(
                        connection,
                        job_id,
                    )
                )
                if bool(result["complete"]):
                    return {"deleted": int(result["deleted"])}
                if self._closing:
                    raise _MailboxBulkDeletePaused("mailbox service is closing")
                await self._yield_between_bulk_delete_batches()
        except BaseException as exc:
            if not isinstance(exc, (asyncio.CancelledError, _MailboxBulkDeletePaused)):
                await self._mark_bulk_delete_job_failed(job_id, exc)
            raise

    def _bulk_delete_task_finished(
        self,
        job_id: str,
        task: asyncio.Task[dict[str, int]],
    ) -> None:
        if self._bulk_delete_tasks.get(job_id) is task:
            self._bulk_delete_tasks.pop(job_id, None)
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None and not isinstance(error, _MailboxBulkDeletePaused):
            _logger.error(
                "mailbox bulk delete job failed",
                exc_info=(type(error), error, error.__traceback__),
                extra={"job_id": job_id},
            )

    async def _await_bulk_delete_job(self, job_id: str) -> dict[str, int]:
        if self._closing:
            raise RuntimeError("mailbox service is closing")
        task = self._bulk_delete_tasks.get(job_id)
        if task is None or task.done():
            task = asyncio.create_task(self._run_bulk_delete_job(job_id))
            self._bulk_delete_tasks[job_id] = task
            task.add_done_callback(
                lambda finished, tracked_job_id=job_id: self._bulk_delete_task_finished(
                    tracked_job_id,
                    finished,
                )
            )
        return await asyncio.shield(task)

    def _incomplete_bulk_delete_job_ids(self) -> list[str]:
        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM mailbox_bulk_delete_jobs
                WHERE status IN ('pending', 'running', 'failed')
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
        return [str(row["id"]) for row in rows]

    async def resume_incomplete_bulk_delete_jobs(self) -> None:
        job_ids = await asyncio.to_thread(self._incomplete_bulk_delete_job_ids)
        for job_id in job_ids:
            await self._await_bulk_delete_job(job_id)

    async def close(self) -> None:
        """Stop after each currently-owned writer page reaches a commit."""

        self._closing = True
        tasks = tuple(self._bulk_delete_tasks.values())
        if not tasks:
            return

        async def wait_for_tasks() -> None:
            await asyncio.gather(*tasks, return_exceptions=True)

        waiter = asyncio.create_task(wait_for_tasks())
        while True:
            try:
                await asyncio.shield(waiter)
                return
            except asyncio.CancelledError:
                if waiter.done():
                    await waiter
                    return
                continue

    async def soft_delete_mailbox_deliveries(
        self,
        mailbox_id: int,
        *,
        delivery_ids: list[str] | None = None,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        deleted_at = utc_now()

        if delivery_ids is None:
            job_id = await self._runtime.writer.execute(
                lambda connection: self._create_or_resume_bulk_delete_job(
                    connection,
                    mailbox_id,
                    deleted_at=deleted_at,
                    authorization_principal=authorization_principal,
                )
            )
            result = await self._await_bulk_delete_job(job_id)
            deleted = int(result["deleted"])
            return {
                "deleted": deleted,
                "delivery_ids": [],
                "delivery_ids_truncated": deleted > 0,
            }

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            self._transaction_authorized_mailbox(
                connection,
                mailbox_id,
                authorization_principal,
            )

            if not delivery_ids:
                return {"deleted": 0, "delivery_ids": []}
            connection.execute(
                """
                CREATE TEMP TABLE IF NOT EXISTS _mailbox_delivery_delete_request (
                    id TEXT PRIMARY KEY
                ) WITHOUT ROWID
                """
            )
            connection.execute("DELETE FROM _mailbox_delivery_delete_request")
            try:
                connection.executemany(
                    "INSERT OR IGNORE INTO _mailbox_delivery_delete_request (id) VALUES (?)",
                    ((str(delivery_id),) for delivery_id in delivery_ids),
                )
                rows = connection.execute(
                    """
                    SELECT delivery.id
                    FROM message_deliveries AS delivery
                    JOIN _mailbox_delivery_delete_request AS requested
                      ON requested.id = delivery.id
                    WHERE delivery.mailbox_id = ? AND delivery.status = 'active'
                    """,
                    (mailbox_id,),
                ).fetchall()
                connection.execute(
                    """
                    UPDATE message_deliveries
                    SET status = 'deleted',
                        deleted_at = COALESCE(deleted_at, ?),
                        expires_at = ?
                    WHERE mailbox_id = ? AND status = 'active'
                      AND EXISTS (
                          SELECT 1
                          FROM _mailbox_delivery_delete_request AS requested
                          WHERE requested.id = message_deliveries.id
                      )
                    """,
                    (deleted_at, deleted_at, mailbox_id),
                )
                self._runtime._refresh_mailbox_summary_after_message_delete(connection, mailbox_id)
                return {
                    "deleted": len(rows),
                    "delivery_ids": [str(row["id"]) for row in rows],
                }
            finally:
                connection.execute("DELETE FROM _mailbox_delivery_delete_request")

        return await self._runtime.writer.execute(operation)

    def _mailbox_filter_sql(
        self,
        *,
        query: str | None,
        domain_id: int | None,
        public_enabled: bool | None,
        is_hidden: bool | None,
        allowed_domain_ids: tuple[int, ...] | None = None,
        allowed_mailbox_patterns: tuple[str, ...] = (),
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            clauses.append("(m.address_canonical LIKE ? OR m.address_display LIKE ? OR m.notes LIKE ?)")
            pattern = f"%{query.strip()}%"
            params.extend([pattern, pattern, pattern])
        if domain_id is not None:
            clauses.append("m.domain_id = ?")
            params.append(int(domain_id))
        if public_enabled is not None:
            clauses.append("m.public_enabled = ?")
            params.append(int(public_enabled))
        if is_hidden is not None:
            clauses.append("m.is_hidden = ?")
            params.append(int(is_hidden))
        if allowed_domain_ids is not None:
            if not allowed_domain_ids:
                clauses.append("0 = 1")
            else:
                placeholders = ", ".join("?" for _ in allowed_domain_ids)
                clauses.append(f"m.domain_id IN ({placeholders})")
                params.extend(int(item) for item in allowed_domain_ids)
        if allowed_mailbox_patterns:
            pattern_sql = " OR ".join("m.address_canonical GLOB ?" for _ in allowed_mailbox_patterns)
            clauses.append(f"({pattern_sql})")
            params.extend(str(pattern) for pattern in allowed_mailbox_patterns)
        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _normalize_mailbox_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        for key in ("public_enabled", "is_hidden"):
            if key in payload:
                payload[key] = bool(payload[key])
        return payload


__all__ = ["MAILBOX_BULK_DELETE_BATCH_SIZE", "MailboxService"]
