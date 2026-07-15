from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Any

from app.auth.api_keys import ApiKeyService
from app.auth.permissions import PermissionContext
from app.config import MAX_MESSAGE_SIZE_LIMIT_BYTES, MAX_RETENTION_DAYS
from app.db.connection import connect_database
from app.db.writer import DatabaseWriter
from app.ingest.storage import utc_now
from app.smtp.matcher import (
    DomainMatch,
    DomainMatcher,
    DomainRule,
    normalize_domain,
    parse_mailbox_address,
)


MAILBOX_OWNERSHIP_REHOME_SETTING = "_mailbox_ownership_rehome_v2"
DOMAIN_ROUTING_TOMBSTONE_PREFIX = "_domain_routing_tombstone_v1:"
DOMAIN_REHOME_BATCH_SIZE = 1000

_logger = logging.getLogger("rapid_inbox.domains")


def domain_routing_tombstone_key(domain_id: int, root_domain_ascii: str) -> str:
    return f"{DOMAIN_ROUTING_TOMBSTONE_PREFIX}{domain_id}:{root_domain_ascii}"


def load_active_domain_matcher(connection: sqlite3.Connection) -> DomainMatcher:
    """Build the routing view owned by the current SQLite transaction.

    SMTP routing caches deliberately avoid a database read per RCPT.  The final
    mailbox ownership decision is different: it must observe the domain rows in
    the same transaction that writes the delivery, otherwise a just-created
    managed domain can lose a race to a stale catch-all match.
    """

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
        WHERE is_active = 1
        ORDER BY id ASC
        """
    ).fetchall()
    return DomainMatcher(
        [
            DomainRule(
                domain_id=int(row["id"]),
                root_domain_ascii=str(row["root_domain_ascii"]),
                accept_exact=bool(row["accept_exact"]),
                accept_subdomains=bool(row["accept_subdomains"]),
                plus_addressing_mode=str(row["plus_addressing_mode"]),
                local_part_case_sensitive=bool(row["local_part_case_sensitive"]),
            )
            for row in rows
        ]
    )


def match_active_domain(connection: sqlite3.Connection, address: str) -> DomainMatch | None:
    """Resolve one address from indexed candidate roots in the write transaction."""

    parsed = parse_mailbox_address(address)
    if parsed is None:
        return None
    _local_part, domain_ascii = parsed
    candidate_roots = [domain_ascii]
    candidate_roots.extend(
        domain_ascii[index + 1 :]
        for index, character in enumerate(domain_ascii)
        if character == "."
    )
    candidate_roots.append("*")
    placeholders = ", ".join("?" for _ in candidate_roots)
    rows = connection.execute(
        f"""
        SELECT
            id,
            root_domain_ascii,
            accept_exact,
            accept_subdomains,
            plus_addressing_mode,
            local_part_case_sensitive
        FROM domains
        WHERE is_active = 1 AND root_domain_ascii IN ({placeholders})
        ORDER BY id ASC
        """,
        tuple(candidate_roots),
    ).fetchall()
    return DomainMatcher(
        [
            DomainRule(
                domain_id=int(row["id"]),
                root_domain_ascii=str(row["root_domain_ascii"]),
                accept_exact=bool(row["accept_exact"]),
                accept_subdomains=bool(row["accept_subdomains"]),
                plus_addressing_mode=str(row["plus_addressing_mode"]),
                local_part_case_sensitive=bool(row["local_part_case_sensitive"]),
            )
            for row in rows
        ]
    ).match_address(address)


def _merged_delivery_status(first: str, second: str) -> str:
    # Keep an active historical delivery visible.  If neither copy is active,
    # hidden is less destructive than deleted and therefore wins.
    for status in ("active", "hidden", "deleted"):
        if status in {first, second}:
            return status
    return first


def _merged_expiration(first: Any, second: Any) -> str | None:
    if first is None or second is None:
        return None
    return max(str(first), str(second))


def _refresh_rehomed_mailbox_summary(
    connection: sqlite3.Connection,
    mailbox_id: int,
    *,
    first_seen_at: str,
    last_seen_at: str,
) -> None:
    summary = connection.execute(
        """
        SELECT COUNT(*) AS message_count, MAX(delivered_at) AS latest_message_at
        FROM message_deliveries
        WHERE mailbox_id = ? AND status = 'active'
        """,
        (mailbox_id,),
    ).fetchone()
    connection.execute(
        """
        UPDATE mailboxes
        SET first_seen_at = MIN(first_seen_at, ?),
            last_seen_at = MAX(last_seen_at, ?),
            latest_message_at = ?,
            message_count = ?
        WHERE id = ?
        """,
        (
            first_seen_at,
            last_seen_at,
            None if summary is None else summary["latest_message_at"],
            0 if summary is None else int(summary["message_count"]),
            mailbox_id,
        ),
    )


def _mailbox_needs_ownership_promotion(source: sqlite3.Row, match: DomainMatch) -> bool:
    """Return whether ``match`` is a monotonic improvement for this mailbox."""

    source_root = str(source["root_domain_ascii"])
    if match.root_domain_ascii == "*":
        return False
    if int(source["domain_id"]) == int(match.domain_id):
        return any(
            (
                str(source["local_part_canonical"]) != match.local_part_canonical,
                str(source["rcpt_domain_ascii"]) != match.domain_ascii,
                str(source["address_canonical"]) != match.address_canonical,
            )
        )
    if source_root == "*":
        return True
    return match.root_domain_ascii.endswith(f".{source_root}")


def promote_mailbox_ownership(
    connection: sqlite3.Connection,
    source_mailbox_id: int,
    match: DomainMatch,
) -> dict[str, int]:
    """Promote one mailbox to its current routing winner without losing history.

    The caller owns the surrounding transaction.  Target rows are selected by
    canonical address; processing and conflict resolution are deterministic so
    retries produce the same surviving mailbox and delivery IDs. Ownership can
    move from catch-all to managed, from a parent managed suffix to a more
    specific managed suffix, or be re-canonicalized within the same domain. It
    is never demoted to catch-all or to a shorter suffix.
    """

    source = connection.execute(
        """
        SELECT
            m.id,
            m.domain_id,
            m.local_part_canonical,
            m.rcpt_domain_ascii,
            m.address_canonical,
            m.first_seen_at,
            m.last_seen_at,
            m.public_enabled,
            m.is_hidden,
            m.notes,
            d.root_domain_ascii
        FROM mailboxes AS m
        JOIN domains AS d ON d.id = m.domain_id
        WHERE m.id = ?
        """,
        (source_mailbox_id,),
    ).fetchone()
    if source is None or not _mailbox_needs_ownership_promotion(source, match):
        return {"mailbox_id": 0 if source is None else int(source["id"]), "rehomed": 0, "moved": 0, "deduplicated": 0}

    target = connection.execute(
        """
        SELECT
            m.id,
            m.domain_id,
            m.local_part_canonical,
            m.rcpt_domain_ascii,
            m.address_canonical,
            m.first_seen_at,
            m.last_seen_at,
            m.public_enabled,
            m.is_hidden,
            m.notes,
            d.root_domain_ascii
        FROM mailboxes AS m
        JOIN domains AS d ON d.id = m.domain_id
        WHERE m.address_canonical = ?
        """,
        (match.address_canonical,),
    ).fetchone()

    if target is None or int(target["id"]) == int(source["id"]):
        connection.execute(
            """
            UPDATE mailboxes
            SET domain_id = ?,
                local_part_canonical = ?,
                rcpt_domain_ascii = ?,
                address_canonical = ?,
                address_display = ?
            WHERE id = ?
            """,
            (
                match.domain_id,
                match.local_part_canonical,
                match.domain_ascii,
                match.address_canonical,
                match.address_canonical,
                source["id"],
            ),
        )
        _refresh_rehomed_mailbox_summary(
            connection,
            int(source["id"]),
            first_seen_at=str(source["first_seen_at"]),
            last_seen_at=str(source["last_seen_at"]),
        )
        return {"mailbox_id": int(source["id"]), "rehomed": 1, "moved": 0, "deduplicated": 0}

    target_id = int(target["id"])
    # The target may itself have been owned by catch-all or by a shorter
    # managed suffix. Promote it when appropriate, but never demote a mailbox
    # already owned by a more-specific (possibly now inactive) domain.
    if _mailbox_needs_ownership_promotion(target, match):
        connection.execute(
            """
            UPDATE mailboxes
            SET domain_id = ?,
                local_part_canonical = ?,
                rcpt_domain_ascii = ?,
                address_display = ?,
                public_enabled = MIN(public_enabled, ?),
                is_hidden = MAX(is_hidden, ?),
                notes = COALESCE(notes, ?)
            WHERE id = ?
            """,
            (
                match.domain_id,
                match.local_part_canonical,
                match.domain_ascii,
                match.address_canonical,
                int(source["public_enabled"]),
                int(source["is_hidden"]),
                source["notes"],
                target_id,
            ),
        )
    else:
        connection.execute(
            """
            UPDATE mailboxes
            SET public_enabled = MIN(public_enabled, ?),
                is_hidden = MAX(is_hidden, ?),
                notes = COALESCE(notes, ?)
            WHERE id = ?
            """,
            (
                int(source["public_enabled"]),
                int(source["is_hidden"]),
                source["notes"],
                target_id,
            ),
        )

    deduplicated = 0
    duplicates = connection.execute(
        """
        SELECT
            source.id AS source_id,
            source.delivered_at AS source_delivered_at,
            source.status AS source_status,
            source.deleted_at AS source_deleted_at,
            source.expires_at AS source_expires_at,
            source.notes AS source_notes,
            target.id AS target_id,
            target.delivered_at AS target_delivered_at,
            target.status AS target_status,
            target.deleted_at AS target_deleted_at,
            target.expires_at AS target_expires_at,
            target.notes AS target_notes
        FROM message_deliveries AS source
        JOIN message_deliveries AS target
          ON target.message_id = source.message_id
         AND target.mailbox_id = ?
        WHERE source.mailbox_id = ?
        ORDER BY source.message_id ASC, source.id ASC
        """,
        (target_id, source["id"]),
    ).fetchall()
    for duplicate in duplicates:
        status = _merged_delivery_status(
            str(duplicate["target_status"]),
            str(duplicate["source_status"]),
        )
        deleted_at = None
        if status == "deleted":
            deleted_values = [
                str(value)
                for value in (duplicate["target_deleted_at"], duplicate["source_deleted_at"])
                if value is not None
            ]
            deleted_at = min(deleted_values) if deleted_values else None
        connection.execute(
            """
            UPDATE message_deliveries
            SET delivered_at = MIN(delivered_at, ?),
                status = ?,
                deleted_at = ?,
                expires_at = ?,
                notes = COALESCE(notes, ?),
                mailbox_generation = (
                    SELECT bulk_delete_generation
                    FROM mailboxes
                    WHERE id = ?
                )
            WHERE id = ?
            """,
            (
                duplicate["source_delivered_at"],
                status,
                deleted_at,
                _merged_expiration(
                    duplicate["target_expires_at"],
                    duplicate["source_expires_at"],
                ),
                duplicate["source_notes"],
                target_id,
                duplicate["target_id"],
            ),
        )
        connection.execute(
            "DELETE FROM message_deliveries WHERE id = ?",
            (duplicate["source_id"],),
        )
        deduplicated += 1

    moved = int(
        connection.execute(
            """
            UPDATE message_deliveries
            SET mailbox_id = ?,
                mailbox_generation = (
                    SELECT bulk_delete_generation
                    FROM mailboxes
                    WHERE id = ?
                )
            WHERE mailbox_id = ?
            """,
            (target_id, target_id, source["id"]),
        ).rowcount
    )

    _refresh_rehomed_mailbox_summary(
        connection,
        target_id,
        first_seen_at=min(str(source["first_seen_at"]), str(target["first_seen_at"])),
        last_seen_at=max(str(source["last_seen_at"]), str(target["last_seen_at"])),
    )
    connection.execute("DELETE FROM mailboxes WHERE id = ?", (source["id"],))
    return {"mailbox_id": target_id, "rehomed": 1, "moved": moved, "deduplicated": deduplicated}


def _candidate_source_domain_ids(
    connection: sqlite3.Connection,
    candidate_root_domain: str | None,
) -> set[int] | None:
    """Return ownership roots that can monotonically promote to a candidate.

    The mailbox page itself is intentionally selected by integer primary key
    before this filter is applied.  Filtering in SQL by ``domain_id`` can make
    SQLite inspect an unbounded number of unrelated catch-all rows just to fill
    one LIMIT page, defeating the bounded-write-transaction guarantee.
    """

    if candidate_root_domain is None:
        return None
    rows = connection.execute(
        "SELECT id, root_domain_ascii FROM domains ORDER BY id ASC"
    ).fetchall()
    return {
        int(row["id"])
        for row in rows
        if str(row["root_domain_ascii"]) == "*"
        or str(row["root_domain_ascii"]) == candidate_root_domain
        or candidate_root_domain.endswith(f".{row['root_domain_ascii']}")
    }


def _decode_destination_domain_ids(value: Any) -> set[int]:
    try:
        payload = json.loads(str(value or "[]"))
    except json.JSONDecodeError:
        return set()
    if not isinstance(payload, list):
        return set()
    result: set[int] = set()
    for item in payload:
        try:
            result.add(int(item))
        except (TypeError, ValueError):
            continue
    return result


class DomainService:
    def __init__(
        self,
        database_path: Path,
        writer: DatabaseWriter,
        api_keys: ApiKeyService,
    ) -> None:
        self._database_path = database_path
        self._writer = writer
        self._api_keys = api_keys
        self._reload_lock = threading.Lock()
        self._routing_snapshot: tuple[DomainMatcher, dict[int, int]] = (DomainMatcher([]), {})
        self._rehome_tasks: set[asyncio.Task[dict[str, int]]] = set()

    def reload(self) -> None:
        # Domain mutations may complete concurrently with HTTP readers and the
        # SMTP thread. Serialize reload passes, then publish matcher and size
        # limits as one immutable snapshot so a reader never observes a mixed
        # generation.
        with self._reload_lock:
            with connect_database(self._database_path) as connection:
                rows = connection.execute(
                    """
                    SELECT
                        id,
                        root_domain_ascii,
                        accept_exact,
                        accept_subdomains,
                        plus_addressing_mode,
                        local_part_case_sensitive,
                        max_message_size_bytes
                    FROM domains
                    WHERE is_active = 1
                    ORDER BY id ASC
                    """
                ).fetchall()
            matcher = DomainMatcher(
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
            )
            size_limits = {
                int(row["id"]): int(row["max_message_size_bytes"])
                for row in rows
            }
            self._routing_snapshot = (matcher, size_limits)

    @staticmethod
    def _create_rehome_job_in_connection(
        connection: sqlite3.Connection,
        *,
        reason: str,
        candidate_root_domain: str | None,
        created_at: str,
        marks_ownership_upgrade: bool = False,
    ) -> str:
        job_id = f"drj_{uuid.uuid4().hex}"
        frontier = connection.execute(
            "SELECT COALESCE(MAX(id), 0) AS max_id FROM mailboxes"
        ).fetchone()
        max_mailbox_id = 0 if frontier is None else int(frontier["max_id"])
        connection.execute(
            """
            INSERT INTO domain_rehome_jobs (
                id,
                reason,
                candidate_root_domain,
                status,
                cursor_mailbox_id,
                max_mailbox_id,
                marks_ownership_upgrade,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, 'pending', 0, ?, ?, ?, ?)
            """,
            (
                job_id,
                reason,
                candidate_root_domain,
                max_mailbox_id,
                int(marks_ownership_upgrade),
                created_at,
                created_at,
            ),
        )
        return job_id

    def _process_rehome_job_batch(
        self,
        connection: sqlite3.Connection,
        job_id: str,
        *,
        batch_size: int = DOMAIN_REHOME_BATCH_SIZE,
    ) -> dict[str, Any]:
        """Advance one persisted ownership job in one bounded write transaction."""

        if batch_size < 1 or batch_size > DOMAIN_REHOME_BATCH_SIZE:
            raise ValueError("invalid domain rehome batch size")
        connection.execute("BEGIN IMMEDIATE")
        job = connection.execute(
            """
            SELECT
                id,
                reason,
                candidate_root_domain,
                status,
                cursor_mailbox_id,
                max_mailbox_id,
                mailboxes_scanned,
                mailboxes_rehomed,
                deliveries_moved,
                deliveries_deduplicated,
                destination_domain_ids_json,
                marks_ownership_upgrade,
                created_at
            FROM domain_rehome_jobs
            WHERE id = ?
            """,
            (job_id,),
        ).fetchone()
        if job is None:
            raise LookupError("domain rehome job not found")
        if str(job["status"]) == "succeeded":
            return {
                "complete": True,
                "scanned": 0,
                "mailboxes_scanned": int(job["mailboxes_scanned"]),
                "rehomed": int(job["mailboxes_rehomed"]),
                "moved": int(job["deliveries_moved"]),
                "deduplicated": int(job["deliveries_deduplicated"]),
            }

        cursor_mailbox_id = int(job["cursor_mailbox_id"])
        max_mailbox_id = int(job["max_mailbox_id"])
        # The LIMIT applies directly to the integer-primary-key walk.  Do not
        # add a domain predicate here: for a sparse candidate SQLite may inspect
        # the entire catch-all index to find one page of matching rows.
        rows = connection.execute(
            """
            SELECT id, domain_id, address_canonical
            FROM mailboxes
            WHERE id > ? AND id <= ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (cursor_mailbox_id, max_mailbox_id, batch_size),
        ).fetchall()

        matcher = load_active_domain_matcher(connection)
        candidate_root_domain = (
            None
            if job["candidate_root_domain"] is None
            else str(job["candidate_root_domain"])
        )
        source_domain_ids = _candidate_source_domain_ids(
            connection,
            candidate_root_domain,
        )
        batch_totals = {"rehomed": 0, "moved": 0, "deduplicated": 0}
        destination_domain_ids = _decode_destination_domain_ids(
            job["destination_domain_ids_json"]
        )
        for row in rows:
            if source_domain_ids is not None and int(row["domain_id"]) not in source_domain_ids:
                continue
            match = matcher.match_address(str(row["address_canonical"]))
            if match is None or match.root_domain_ascii == "*":
                continue
            result = promote_mailbox_ownership(connection, int(row["id"]), match)
            for key in batch_totals:
                batch_totals[key] += int(result[key])
            if result["rehomed"]:
                destination_domain_ids.add(int(match.domain_id))

        scanned = len(rows)
        next_cursor = int(rows[-1]["id"]) if rows else max_mailbox_id
        complete = not rows or next_cursor >= max_mailbox_id
        mailboxes_scanned = int(job["mailboxes_scanned"]) + scanned
        rehomed = int(job["mailboxes_rehomed"]) + batch_totals["rehomed"]
        moved = int(job["deliveries_moved"]) + batch_totals["moved"]
        deduplicated = int(job["deliveries_deduplicated"]) + batch_totals["deduplicated"]
        updated_at = utc_now()

        if complete and rehomed:
            connection.execute(
                """
                INSERT INTO audit_logs (
                    actor_type, actor_ref, action, resource_type, resource_ref,
                    status, details_json, created_at
                ) VALUES (
                    'system', 'domain-routing', 'mailboxes.rehome',
                    'domain', NULL, 'success', ?, ?
                )
                """,
                (
                    json.dumps(
                        {
                            "reason": str(job["reason"]),
                            "mailboxes_rehomed": rehomed,
                            "deliveries_moved": moved,
                            "deliveries_deduplicated": deduplicated,
                            "destination_domain_ids": sorted(destination_domain_ids),
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    str(job["created_at"]),
                ),
            )
        if complete and bool(job["marks_ownership_upgrade"]):
            connection.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES (?, '1', ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = '1',
                    updated_at = excluded.updated_at
                """,
                (MAILBOX_OWNERSHIP_REHOME_SETTING, updated_at),
            )

        connection.execute(
            """
            UPDATE domain_rehome_jobs
            SET status = ?,
                cursor_mailbox_id = ?,
                mailboxes_scanned = ?,
                mailboxes_rehomed = ?,
                deliveries_moved = ?,
                deliveries_deduplicated = ?,
                destination_domain_ids_json = ?,
                updated_at = ?,
                finished_at = ?,
                error = NULL
            WHERE id = ?
            """,
            (
                "succeeded" if complete else "running",
                next_cursor,
                mailboxes_scanned,
                rehomed,
                moved,
                deduplicated,
                json.dumps(sorted(destination_domain_ids), separators=(",", ":")),
                updated_at,
                updated_at if complete else None,
                job_id,
            ),
        )
        return {
            "complete": complete,
            "scanned": scanned,
            "mailboxes_scanned": mailboxes_scanned,
            "rehomed": rehomed,
            "moved": moved,
            "deduplicated": deduplicated,
        }

    async def _yield_between_rehome_batches(self) -> None:
        # Give already-admitted SMTP/API writes a chance to run before this job
        # submits its next bounded transaction to the single writer actor.
        await asyncio.sleep(0)

    async def _mark_rehome_job_failed(self, job_id: str, error: BaseException) -> None:
        failed_at = utc_now()
        message = f"{error.__class__.__name__}: {error}"[:2000]

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                UPDATE domain_rehome_jobs
                SET status = 'failed',
                    updated_at = ?,
                    finished_at = ?,
                    error = ?
                WHERE id = ? AND status != 'succeeded'
                """,
                (failed_at, failed_at, message, job_id),
            )

        try:
            await self._writer.execute(operation)
        except Exception:
            _logger.exception(
                "failed to persist domain rehome job failure",
                extra={"job_id": job_id},
            )

    async def _run_rehome_job(self, job_id: str) -> dict[str, int]:
        try:
            while True:
                result = await self._writer.execute(
                    lambda connection: self._process_rehome_job_batch(
                        connection,
                        job_id,
                    )
                )
                if bool(result["complete"]):
                    return {
                        "mailboxes_scanned": int(result["mailboxes_scanned"]),
                        "rehomed": int(result["rehomed"]),
                        "moved": int(result["moved"]),
                        "deduplicated": int(result["deduplicated"]),
                    }
                await self._yield_between_rehome_batches()
        except BaseException as exc:
            if not isinstance(exc, asyncio.CancelledError):
                await self._mark_rehome_job_failed(job_id, exc)
            raise

    def _rehome_task_finished(self, task: asyncio.Task[dict[str, int]]) -> None:
        self._rehome_tasks.discard(task)
        if task.cancelled():
            return
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return
        if error is not None:
            _logger.error(
                "domain rehome job failed",
                exc_info=(type(error), error, error.__traceback__),
            )

    async def _await_rehome_job(self, job_id: str) -> dict[str, int]:
        # Once the short policy transaction commits, cancellation cannot roll it
        # back.  Keep the persisted migration running and tracked even if the
        # HTTP client disconnects; a crash leaves a resumable cursor for startup.
        task = asyncio.create_task(self._run_rehome_job(job_id))
        self._rehome_tasks.add(task)
        task.add_done_callback(self._rehome_task_finished)
        return await asyncio.shield(task)

    def _incomplete_rehome_job_ids(self) -> list[str]:
        with connect_database(self._database_path) as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM domain_rehome_jobs
                WHERE status IN ('pending', 'running', 'failed')
                ORDER BY created_at ASC, id ASC
                """
            ).fetchall()
        return [str(row["id"]) for row in rows]

    async def _resume_incomplete_rehome_jobs(self) -> None:
        job_ids = await asyncio.to_thread(self._incomplete_rehome_job_ids)
        for job_id in job_ids:
            await self._await_rehome_job(job_id)

    async def record_dns_check(
        self,
        domain_id: int,
        *,
        expected_root_domain_ascii: str,
        checked_at: str,
        details: dict[str, Any],
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        """Persist a DNS result only against the policy that was checked."""

        expected_root = str(expected_root_domain_ascii).strip().lower()
        if not expected_root:
            raise ValueError("invalid DNS check domain")
        dns_status = str(details.get("status") or "").strip().lower()
        if dns_status not in {"ok", "warning", "error"}:
            raise ValueError("invalid DNS check status")
        details_json = json.dumps(details, ensure_ascii=False)

        def operation(connection: sqlite3.Connection) -> None:
            # DNS resolution happens outside SQLite and may take seconds. Take
            # the write reservation before the final actor/policy reload so a
            # revoked key or concurrent rename cannot accept a stale result.
            connection.execute("BEGIN IMMEDIATE")
            self._api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="domains.write",
                domain_id=domain_id,
            )
            row = connection.execute(
                "SELECT root_domain_ascii FROM domains WHERE id = ?",
                (domain_id,),
            ).fetchone()
            if row is None:
                raise LookupError("domain not found")
            current_root = str(row["root_domain_ascii"])
            if current_root == "*":
                raise ValueError("DNS checks are not supported for the catch-all domain")
            if current_root != expected_root:
                raise ValueError("domain changed while the DNS check was running")
            cursor = connection.execute(
                """
                UPDATE domains
                SET dns_status = ?,
                    dns_last_checked_at = ?,
                    dns_details_json = ?,
                    updated_at = ?
                WHERE id = ? AND root_domain_ascii = ?
                """,
                (
                    dns_status,
                    checked_at,
                    details_json,
                    checked_at,
                    domain_id,
                    expected_root,
                ),
            )
            if cursor.rowcount != 1:
                raise LookupError("domain not found")

        await self._writer.execute(operation)
        return await asyncio.to_thread(self.get_domain, domain_id)

    async def create_domain(
        self,
        root_domain: str,
        *,
        accept_exact: bool = True,
        accept_subdomains: bool = True,
        public_web_enabled: bool = False,
        public_api_enabled: bool = False,
        plus_addressing_mode: str = "keep",
        local_part_case_sensitive: bool = False,
        is_active: bool = True,
        max_message_size_bytes: int = 52_428_800,
        retention_days: int | None = None,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        root_domain_ascii = self._coerce_root_domain(root_domain)
        accept_exact = self._coerce_bool("accept_exact", accept_exact)
        accept_subdomains = self._coerce_bool("accept_subdomains", accept_subdomains)
        public_web_enabled = self._coerce_bool("public_web_enabled", public_web_enabled)
        public_api_enabled = self._coerce_bool("public_api_enabled", public_api_enabled)
        local_part_case_sensitive = self._coerce_bool("local_part_case_sensitive", local_part_case_sensitive)
        is_active = self._coerce_bool("is_active", is_active)
        max_message_size_bytes = self._coerce_positive_int("max_message_size_bytes", max_message_size_bytes)
        retention_days = self._coerce_nullable_positive_int("retention_days", retention_days)
        plus_addressing_mode = self._coerce_plus_addressing_mode(plus_addressing_mode)

        def operation(connection: sqlite3.Connection) -> tuple[int, str | None]:
            # The authorization snapshot, domain row, and persisted rehome job
            # are one cross-process-safe transaction. A key revoked while this
            # mutation is queued cannot create either artifact.
            connection.execute("BEGIN IMMEDIATE")
            self._api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="domains.write",
                require_global=True,
            )
            cursor = connection.execute(
                """
                INSERT INTO domains (
                    root_domain_ascii,
                    root_domain_unicode,
                    accept_exact,
                    accept_subdomains,
                    public_web_enabled,
                    public_api_enabled,
                    is_active,
                    plus_addressing_mode,
                    local_part_case_sensitive,
                    max_message_size_bytes,
                    retention_days,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    root_domain_ascii,
                    root_domain,
                    int(accept_exact),
                    int(accept_subdomains),
                    int(public_web_enabled),
                    int(public_api_enabled),
                    int(is_active),
                    plus_addressing_mode,
                    int(local_part_case_sensitive),
                    max_message_size_bytes,
                    retention_days,
                    now,
                    now,
                ),
            )
            domain_id = int(cursor.lastrowid)
            job_id = (
                self._create_rehome_job_in_connection(
                    connection,
                    reason="domain.create",
                    candidate_root_domain=root_domain_ascii,
                    created_at=now,
                )
                if is_active
                else None
            )
            return domain_id, job_id

        domain_id, job_id = await self._writer.execute(operation)
        # Publish the final routing policy before walking historical rows.  SMTP
        # RCPT decisions then use the new hot snapshot immediately, while final
        # persistence still rechecks the same domain rows transactionally.
        await asyncio.to_thread(self.reload)
        if job_id is not None:
            await self._await_rehome_job(job_id)
        return await asyncio.to_thread(self.get_domain, domain_id)

    async def sync_catch_all_policy(
        self,
        *,
        enabled: bool,
        public_web_enabled: bool = False,
        public_api_enabled: bool = False,
        retention_days: int | None = None,
        max_message_size_bytes: int = 52_428_800,
    ) -> dict[str, Any] | None:
        """Create or atomically update the private fallback routing policy."""

        now = utc_now()
        normalized_retention = self._coerce_nullable_positive_int("retention_days", retention_days)
        normalized_size = self._coerce_positive_int("max_message_size_bytes", max_message_size_bytes)

        def operation(connection: sqlite3.Connection) -> tuple[int, str | None]:
            def schedule_ownership_upgrade_once() -> str | None:
                migration = connection.execute(
                    "SELECT value FROM system_settings WHERE key = ?",
                    (MAILBOX_OWNERSHIP_REHOME_SETTING,),
                ).fetchone()
                if migration is not None and str(migration["value"]) == "1":
                    return None
                pending = connection.execute(
                    """
                    SELECT id
                    FROM domain_rehome_jobs
                    WHERE marks_ownership_upgrade = 1
                      AND status != 'succeeded'
                    ORDER BY created_at ASC, id ASC
                    LIMIT 1
                    """
                ).fetchone()
                if pending is not None:
                    return str(pending["id"])
                return self._create_rehome_job_in_connection(
                    connection,
                    created_at=now,
                    reason="startup.upgrade",
                    candidate_root_domain=None,
                    marks_ownership_upgrade=True,
                )

            existing = connection.execute(
                "SELECT id FROM domains WHERE root_domain_ascii = '*'"
            ).fetchone()
            if not enabled and existing is None:
                return 0, schedule_ownership_upgrade_once()
            connection.execute(
                """
                INSERT INTO domains (
                    root_domain_ascii, root_domain_unicode, accept_exact, accept_subdomains,
                    public_web_enabled, public_api_enabled, is_active, is_hidden,
                    plus_addressing_mode, local_part_case_sensitive,
                    max_message_size_bytes, retention_days, notes, created_at, updated_at
                ) VALUES ('*', '任意域名', 1, 1, ?, ?, ?, 0, 'keep', 0, ?, ?, ?, ?, ?)
                ON CONFLICT(root_domain_ascii) DO UPDATE SET
                    public_web_enabled = excluded.public_web_enabled,
                    public_api_enabled = excluded.public_api_enabled,
                    is_active = excluded.is_active,
                    max_message_size_bytes = excluded.max_message_size_bytes,
                    retention_days = excluded.retention_days,
                    notes = excluded.notes,
                    updated_at = excluded.updated_at
                """,
                (
                    int(bool(public_web_enabled)),
                    int(bool(public_api_enabled)),
                    int(bool(enabled)),
                    normalized_size,
                    normalized_retention,
                    "系统任意域名收件策略；默认私有，仅管理员可查阅。",
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT id FROM domains WHERE root_domain_ascii = '*'"
            ).fetchone()
            if row is None:
                raise RuntimeError("catch-all policy was not created")
            return int(row["id"]), schedule_ownership_upgrade_once()

        domain_id, _upgrade_job_id = await self._writer.execute(operation)
        await asyncio.to_thread(self.reload)
        # This also resumes a policy migration that was interrupted after its
        # short commit but before its historical mailbox cursor reached EOF.
        await self._resume_incomplete_rehome_jobs()
        if domain_id == 0:
            return None
        return await asyncio.to_thread(self.get_domain, domain_id)

    async def update_domain(
        self,
        domain_id: int,
        payload: dict[str, Any],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("invalid domain payload")

        assignments: list[str] = []
        values: list[Any] = []
        if "root_domain" in payload:
            root_domain = str(payload["root_domain"]).strip()
            root_domain_ascii = self._coerce_root_domain(root_domain)
            assignments.extend(["root_domain_ascii = ?", "root_domain_unicode = ?"])
            values.extend([root_domain_ascii, root_domain])
        for field_name in (
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "local_part_case_sensitive",
            "is_active",
            "is_hidden",
        ):
            if field_name in payload:
                assignments.append(f"{field_name} = ?")
                values.append(int(self._coerce_bool(field_name, payload[field_name])))
        if "plus_addressing_mode" in payload:
            assignments.append("plus_addressing_mode = ?")
            values.append(self._coerce_plus_addressing_mode(payload["plus_addressing_mode"]))
        if "max_message_size_bytes" in payload:
            assignments.append("max_message_size_bytes = ?")
            values.append(self._coerce_positive_int("max_message_size_bytes", payload["max_message_size_bytes"]))
        if "retention_days" in payload:
            assignments.append("retention_days = ?")
            values.append(self._coerce_nullable_positive_int("retention_days", payload["retention_days"]))
        if "notes" in payload:
            assignments.append("notes = ?")
            values.append(self._nullable_text(payload["notes"]))

        if not assignments:
            return await asyncio.to_thread(self.get_domain, domain_id)

        updated_at = utc_now()
        assignments.append("updated_at = ?")
        values.append(updated_at)

        def operation(connection: sqlite3.Connection) -> str | None:
            # Acquire SQLite's write reservation before reloading the actor.
            # Otherwise a second process could narrow the key after the SELECT
            # authorization check but before this operation's first UPDATE.
            connection.execute("BEGIN IMMEDIATE")
            self._api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="domains.write",
                require_global="root_domain" in payload,
                domain_id=domain_id,
            )
            row = connection.execute(
                """
                SELECT
                    id,
                    root_domain_ascii,
                    accept_exact,
                    accept_subdomains,
                    plus_addressing_mode,
                    local_part_case_sensitive,
                    is_active
                FROM domains
                WHERE id = ?
                """,
                (domain_id,),
            ).fetchone()
            if row is None:
                raise LookupError("domain not found")
            if str(row["root_domain_ascii"]) == "*":
                raise ValueError("catch-all domain is managed through ingress settings")
            previous_root_domain = str(row["root_domain_ascii"])
            connection.execute(
                f"UPDATE domains SET {', '.join(assignments)} WHERE id = ?",
                (*values, domain_id),
            )
            updated = connection.execute(
                """
                SELECT
                    root_domain_ascii,
                    accept_exact,
                    accept_subdomains,
                    plus_addressing_mode,
                    local_part_case_sensitive,
                    is_active
                FROM domains
                WHERE id = ?
                """,
                (domain_id,),
            ).fetchone()
            if updated is None:
                raise LookupError("domain not found")
            updated_root_domain = str(updated["root_domain_ascii"])
            if updated_root_domain != previous_root_domain:
                connection.execute(
                    """
                    INSERT INTO system_settings (key, value, updated_at)
                    VALUES (?, 'renamed', ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (
                        domain_routing_tombstone_key(domain_id, previous_root_domain),
                        updated_at,
                    ),
                )
            previous_routing = (
                str(row["root_domain_ascii"]),
                int(row["accept_exact"]),
                int(row["accept_subdomains"]),
                str(row["plus_addressing_mode"]),
                int(row["local_part_case_sensitive"]),
                int(row["is_active"]),
            )
            updated_routing = (
                updated_root_domain,
                int(updated["accept_exact"]),
                int(updated["accept_subdomains"]),
                str(updated["plus_addressing_mode"]),
                int(updated["local_part_case_sensitive"]),
                int(updated["is_active"]),
            )
            if updated_routing == previous_routing:
                return None
            return self._create_rehome_job_in_connection(
                connection,
                created_at=updated_at,
                reason="domain.update",
                candidate_root_domain=updated_root_domain,
            )

        job_id = await self._writer.execute(operation)
        await asyncio.to_thread(self.reload)
        if job_id is not None:
            await self._await_rehome_job(job_id)
        return await asyncio.to_thread(self.get_domain, domain_id)

    async def delete_domain(
        self,
        domain_id: int,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            # Keep authorization, the routing tombstone, and physical deletion
            # atomic. In particular, a failed authorization or FK delete must
            # never leave a tombstone behind.
            connection.execute("BEGIN IMMEDIATE")
            self._api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="domains.write",
                domain_id=domain_id,
            )
            row = connection.execute(
                """
                SELECT
                    id,
                    root_domain_ascii,
                    root_domain_unicode,
                    accept_exact,
                    accept_subdomains,
                    public_web_enabled,
                    public_api_enabled,
                    is_active,
                    is_hidden,
                    local_part_case_sensitive,
                    plus_addressing_mode,
                    max_message_size_bytes,
                    retention_days,
                    dns_status,
                    dns_last_checked_at,
                    dns_details_json,
                    notes,
                    created_at,
                    updated_at
                FROM domains
                WHERE id = ?
                """,
                (domain_id,),
            ).fetchone()
            if row is None:
                raise LookupError("domain not found")
            if str(row["root_domain_ascii"]) == "*":
                raise ValueError("catch-all domain is managed through ingress settings")
            deleted = self._normalize_domain_row(row)
            deleted_at = utc_now()
            connection.execute(
                """
                INSERT INTO system_settings (key, value, updated_at)
                VALUES (?, 'deleted', ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (
                    domain_routing_tombstone_key(
                        domain_id,
                        str(row["root_domain_ascii"]),
                    ),
                    deleted_at,
                ),
            )
            cursor = connection.execute("DELETE FROM domains WHERE id = ?", (domain_id,))
            if cursor.rowcount != 1:
                raise LookupError("domain not found")
            return deleted

        deleted = await self._writer.execute(operation)
        await asyncio.to_thread(self.reload)
        deleted["dns_recommendations"] = self.dns_recommendations(
            str(deleted["root_domain_ascii"])
        )
        return deleted

    def list_domains(self) -> list[dict[str, Any]]:
        with connect_database(self._database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    root_domain_ascii,
                    accept_exact,
                    accept_subdomains,
                    public_web_enabled,
                    public_api_enabled,
                    is_active,
                    created_at,
                    updated_at
                FROM domains
                ORDER BY root_domain_ascii ASC
                """
            ).fetchall()
        return [self._normalize_domain_row(row) for row in rows]

    def get_domain(self, domain_id: int) -> dict[str, Any]:
        with connect_database(self._database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    root_domain_ascii,
                    root_domain_unicode,
                    accept_exact,
                    accept_subdomains,
                    public_web_enabled,
                    public_api_enabled,
                    is_active,
                    is_hidden,
                    local_part_case_sensitive,
                    plus_addressing_mode,
                    max_message_size_bytes,
                    retention_days,
                    dns_status,
                    dns_last_checked_at,
                    dns_details_json,
                    notes,
                    created_at,
                    updated_at
                FROM domains
                WHERE id = ?
                """,
                (domain_id,),
            ).fetchone()
        if row is None:
            raise LookupError("domain not found")
        payload = self._normalize_domain_row(row)
        payload["dns_recommendations"] = self.dns_recommendations(payload["root_domain_ascii"])
        return payload

    def dns_recommendations(self, root_domain: str) -> list[dict[str, str]]:
        if root_domain == "*":
            return []
        return [
            {
                "name": root_domain,
                "type": "MX",
                "value": f"10 {root_domain}",
                "purpose": "根域邮箱收件路由",
            },
            {
                "name": f"*.{root_domain}",
                "type": "MX",
                "value": f"10 {root_domain}",
                "purpose": "子域邮箱收件路由",
            },
        ]

    def match_address(self, address: str) -> DomainMatch | None:
        matcher, _size_limits = self._routing_snapshot
        return matcher.match_address(address)

    def message_size_limit(self, domain_id: int) -> int | None:
        _matcher, size_limits = self._routing_snapshot
        return size_limits.get(int(domain_id))

    def match_address_with_size_limit(self, address: str) -> tuple[DomainMatch | None, int | None]:
        matcher, size_limits = self._routing_snapshot
        match = matcher.match_address(address)
        if match is None:
            return None, None
        return match, size_limits.get(int(match.domain_id))

    def _coerce_root_domain(self, value: Any) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("invalid root_domain")
        try:
            normalized = normalize_domain(value)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("invalid root_domain") from exc
        if normalized == "*":
            raise ValueError("catch-all domain is managed through ingress settings")
        return normalized

    def _coerce_bool(self, field_name: str, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
            return bool(value)
        raise ValueError(f"invalid {field_name}")

    def _coerce_plus_addressing_mode(self, value: Any) -> str:
        if not isinstance(value, str):
            raise ValueError("invalid plus_addressing_mode")
        if value not in {"keep", "strip"}:
            raise ValueError("invalid plus_addressing_mode")
        return value

    def _coerce_positive_int(self, field_name: str, value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError(f"invalid {field_name}")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"invalid {field_name}")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid {field_name}") from exc
        if normalized < 1:
            raise ValueError(f"invalid {field_name}")
        maximum = {
            "max_message_size_bytes": MAX_MESSAGE_SIZE_LIMIT_BYTES,
            "retention_days": MAX_RETENTION_DAYS,
        }.get(field_name)
        if maximum is not None and normalized > maximum:
            raise ValueError(f"invalid {field_name}")
        if isinstance(value, float) and normalized != value:
            raise ValueError(f"invalid {field_name}")
        return normalized

    def _coerce_nullable_positive_int(self, field_name: str, value: Any) -> int | None:
        if value is None or value == "":
            return None
        return self._coerce_positive_int(field_name, value)

    def _nullable_text(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _normalize_domain_row(self, row: sqlite3.Row) -> dict[str, Any]:
        payload = dict(row)
        payload["is_catch_all"] = payload.get("root_domain_ascii") == "*"
        for key in (
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "is_active",
            "is_hidden",
            "local_part_case_sensitive",
        ):
            if key in payload:
                payload[key] = bool(payload[key])
        return payload
