from __future__ import annotations

import asyncio
import base64
import json
import logging
import re
import sqlite3
from typing import Any

from app.auth.permissions import (
    PermissionContext,
    PermissionDenied,
    ensure_mailbox_access,
)
from app.db.connection import connect_database
from app.ingest.storage import utc_now


SAFE_INLINE_CONTENT_TYPES = {
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
}

_CID_REFERENCE_RE = re.compile(r'cid:([^"\'<>\s]+)', re.IGNORECASE)
_logger = logging.getLogger("rapid_inbox.messages")


def _decode_legacy_text_preview(value: bytes | None, *, message_id: str) -> str | None:
    if value is None:
        return None
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        _logger.warning(
            "invalid UTF-8 message preview recovered",
            extra={
                "event": "invalid_text_preview_recovered",
                "message_id": message_id,
            },
        )
        return value.decode("utf-8", errors="replace")


class MessageService:
    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime

    async def get_mailbox_view(
        self,
        mailbox_address: str,
        *,
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[str, str] | None = None,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_mailbox_view(
            mailbox_address,
            limit=limit,
            offset=offset,
            cursor=cursor,
            request_ip=request_ip,
        )

    async def get_delivery_detail(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_delivery_detail(mailbox_address, delivery_id, request_ip=request_ip)

    async def get_raw_message(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
    ) -> bytes:
        await self.get_delivery_detail(mailbox_address, delivery_id, request_ip=request_ip)
        return await self._runtime.get_raw_message(delivery_id)

    async def reparse_message(
        self,
        message_id: str,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> None:
        def operation(connection: sqlite3.Connection) -> int:
            connection.execute("BEGIN IMMEDIATE")
            current_principal = self._runtime.api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="messages.write",
            )
            message = connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            if message is None:
                return 0
            resource_rows = connection.execute(
                """
                SELECT DISTINCT mailbox.domain_id, mailbox.address_canonical
                FROM message_deliveries AS delivery
                JOIN mailboxes AS mailbox ON mailbox.id = delivery.mailbox_id
                WHERE delivery.message_id = ?
                ORDER BY mailbox.domain_id ASC, mailbox.address_canonical ASC
                """,
                (message_id,),
            ).fetchall()
            self._authorize_message_resources(current_principal, resource_rows)
            cursor = connection.execute(
                """
                UPDATE messages
                SET parse_status = 'pending',
                    parse_error = NULL
                WHERE id = ?
                """,
                (message_id,),
            )
            return int(cursor.rowcount)

        # Retention removes queued work, waits for active parsing, deletes the
        # database row, and drains file GC while holding this same lock. Keep
        # the status transition and enqueue indivisible with that sequence so
        # a reparse request cannot reintroduce work between parser drain and
        # artifact deletion.
        async with self._runtime._mail_store_lock:
            updated_rows = await self._runtime.writer.execute(operation)
            if updated_rows == 0:
                raise LookupError("message not found")
            await self._runtime.enqueue_message_for_parse(message_id)

    def list_messages(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        query: str | None = None,
        parse_status: str | None = None,
        mailbox_id: int | None = None,
        allowed_domain_ids: tuple[int, ...] | None = None,
        allowed_mailbox_patterns: tuple[str, ...] = (),
    ) -> dict[str, Any]:
        where_sql, params = self._message_filter_sql(
            query=query,
            parse_status=parse_status,
            mailbox_id=mailbox_id,
            allowed_domain_ids=allowed_domain_ids,
            allowed_mailbox_patterns=allowed_mailbox_patterns,
        )
        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                f"""
                SELECT
                    m.id,
                    m.subject,
                    m.from_addr,
                    COALESCE(
                        (
                            SELECT GROUP_CONCAT(rcpt_to, ', ')
                            FROM (
                                SELECT DISTINCT rcpt_to
                                FROM message_deliveries
                                WHERE message_id = m.id
                                ORDER BY rcpt_to ASC
                            )
                        ),
                        ''
                    ) AS recipients,
                    m.received_at,
                    m.parse_status,
                    m.parse_error,
                    m.has_attachments,
                    m.attachment_count,
                    COUNT(d.id) AS delivery_count
                FROM messages AS m
                LEFT JOIN message_deliveries AS d ON d.message_id = m.id
                {where_sql}
                GROUP BY m.id
                ORDER BY m.received_at DESC, m.id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
            total = connection.execute(
                f"""
                SELECT COUNT(DISTINCT m.id) AS count
                FROM messages AS m
                LEFT JOIN message_deliveries AS d ON d.message_id = m.id
                {where_sql}
                """,
                tuple(params),
            ).fetchone()
        return {
            "items": [dict(row) for row in rows],
            "total_count": 0 if total is None else int(total["count"]),
        }

    def get_admin_message_detail(
        self,
        message_id: str,
        *,
        include_html_preview: bool = False,
    ) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    smtp_session_id,
                    raw_path,
                    raw_sha256,
                    raw_size_bytes,
                    envelope_from,
                    message_id_header,
                    subject,
                    from_name,
                    from_addr,
                    reply_to,
                    date_header,
                    received_at,
                    indexed_at,
                    parse_status,
                    parse_error,
                    has_text,
                    has_html,
                    has_attachments,
                    attachment_count,
                    CAST(text_preview AS BLOB) AS text_preview_raw,
                    text_body_path,
                    html_body_path,
                    CASE
                        WHEN COALESCE(LENGTH(CAST(headers_json AS BLOB)), 0) <= ?
                        THEN headers_json
                        ELSE NULL
                    END AS headers_json,
                    COALESCE(LENGTH(CAST(headers_json AS BLOB)), 0) AS headers_source_bytes
                FROM messages
                WHERE id = ?
                """,
                (self._runtime.settings.message_preview_headers_bytes, message_id),
            ).fetchone()
            if row is None:
                raise LookupError("message not found")
            deliveries = connection.execute(
                """
                SELECT
                    d.id AS delivery_id,
                    d.mailbox_id,
                    mb.address_canonical AS mailbox,
                    d.rcpt_to,
                    d.delivered_at,
                    d.status,
                    d.deleted_at
                FROM message_deliveries AS d
                JOIN mailboxes AS mb ON mb.id = d.mailbox_id
                WHERE d.message_id = ?
                ORDER BY d.delivered_at DESC, d.id DESC
                """,
                (message_id,),
            ).fetchall()
            attachments = connection.execute(
                """
                SELECT
                    id,
                    filename,
                    safe_filename,
                    content_type,
                    content_disposition,
                    content_id,
                    storage_path,
                    size_bytes,
                    is_inline
                FROM attachments
                WHERE message_id = ?
                ORDER BY part_index ASC
                """,
                (message_id,),
            ).fetchall()

        payload = dict(row)
        payload["text_preview"] = _decode_legacy_text_preview(
            payload.pop("text_preview_raw"),
            message_id=message_id,
        )
        for key in ("has_text", "has_html", "has_attachments"):
            payload[key] = bool(payload[key])
        (
            payload["text_body"],
            payload["text_body_truncated"],
            payload["text_body_source_bytes"],
            payload["text_body_preview_bytes"],
        ) = self._runtime.storage.read_text_preview(
            payload.get("text_body_path"),
            self._runtime.settings.message_preview_body_bytes,
        )
        (
            payload["html_body"],
            payload["html_body_truncated"],
            payload["html_body_source_bytes"],
            payload["html_body_preview_bytes"],
        ) = self._runtime.storage.read_text_preview(
            payload.get("html_body_path"),
            self._runtime.settings.message_preview_body_bytes,
        )
        payload["headers_source_bytes"] = int(payload.get("headers_source_bytes") or 0)
        payload["headers_truncated"] = (
            payload["headers_source_bytes"] > self._runtime.settings.message_preview_headers_bytes
        )
        payload["headers"] = json.loads(payload.get("headers_json") or "[]")
        payload.pop("headers_json", None)
        payload["deliveries"] = [dict(delivery) for delivery in deliveries]
        payload["attachments"] = [dict(attachment) for attachment in attachments]
        payload["html_preview_srcdoc"] = ""
        payload.update(self._empty_inline_preview_metadata())
        if include_html_preview and payload["html_body"] and not payload["text_body"]:
            html_body, inline_metadata = self.rewrite_cid_references_bounded(
                payload["html_body"],
                payload["attachments"],
            )
            payload.update(inline_metadata)
            payload["html_preview_srcdoc"] = self.build_public_html_preview_document(html_body)
        return payload

    def get_admin_delivery_detail(self, delivery_id: str) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT message_id
                FROM message_deliveries
                WHERE id = ?
                """,
                (delivery_id,),
            ).fetchone()
        if row is None:
            raise LookupError("delivery not found")
        detail = self.get_admin_message_detail(str(row["message_id"]))
        detail["selected_delivery_id"] = delivery_id
        return detail

    def get_admin_raw_message(self, message_id: str) -> bytes:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                "SELECT raw_path FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        if row is None:
            raise LookupError("message not found")
        return self._runtime.storage.read_bytes(str(row["raw_path"]))

    def get_admin_raw_file(self, message_id: str) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                "SELECT raw_path, raw_sha256 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
        if row is None:
            raise LookupError("message not found")
        path = self._runtime.storage.resolve(str(row["raw_path"]))
        if not path.is_file():
            raise LookupError("raw message not found")
        return {
            "path": path,
            "size_bytes": path.stat().st_size,
            "sha256": row["raw_sha256"],
        }

    def get_admin_attachment(self, message_id: str, attachment_id: str) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT id, filename, safe_filename, content_type, content_disposition,
                       content_id, storage_path, size_bytes, is_inline
                FROM attachments
                WHERE message_id = ? AND id = ?
                """,
                (message_id, attachment_id),
            ).fetchone()
        if row is None:
            raise LookupError("attachment not found")
        payload = dict(row)
        payload["content"] = self._runtime.storage.read_bytes(str(payload["storage_path"]))
        return payload

    def get_admin_attachment_file(self, message_id: str, attachment_id: str) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT id, filename, safe_filename, content_type, content_disposition,
                       content_id, storage_path, size_bytes, is_inline
                FROM attachments
                WHERE message_id = ? AND id = ?
                """,
                (message_id, attachment_id),
            ).fetchone()
        if row is None:
            raise LookupError("attachment not found")
        payload = dict(row)
        path = self._runtime.storage.resolve(str(payload["storage_path"]))
        if not path.is_file():
            raise LookupError("attachment file not found")
        payload["path"] = path
        return payload

    def get_message_delivery_ids(self, message_id: str, *, active_only: bool = False) -> list[str]:
        status_filter = "AND status = 'active'" if active_only else ""
        with connect_database(self._runtime.settings.database_path) as connection:
            message = connection.execute("SELECT 1 FROM messages WHERE id = ?", (message_id,)).fetchone()
            if message is None:
                raise LookupError("message not found")
            rows = connection.execute(
                f"""
                SELECT id
                FROM message_deliveries
                WHERE message_id = ? {status_filter}
                ORDER BY delivered_at DESC, id DESC
                """,
                (message_id,),
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def message_has_delivery(self, message_id: str, delivery_id: str) -> bool:
        with connect_database(self._runtime.settings.database_path) as connection:
            row = connection.execute(
                "SELECT 1 FROM message_deliveries WHERE message_id = ? AND id = ?",
                (message_id, delivery_id),
            ).fetchone()
        return row is not None

    async def soft_delete_delivery(
        self,
        delivery_id: str,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        result = await self.soft_delete_deliveries(
            [delivery_id],
            authorization_principal=authorization_principal,
        )
        if result["deleted"] == 0:
            raise LookupError("delivery not found")
        return result

    async def soft_delete_message(
        self,
        message_id: str,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        deleted_at = utc_now()

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            current_principal = self._runtime.api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="messages.write",
            )
            message = connection.execute(
                "SELECT 1 FROM messages WHERE id = ?",
                (message_id,),
            ).fetchone()
            if message is None:
                raise LookupError("message not found")
            resource_rows = connection.execute(
                """
                SELECT
                    delivery.id,
                    delivery.status,
                    mailbox.domain_id,
                    mailbox.address_canonical
                FROM message_deliveries AS delivery
                JOIN mailboxes AS mailbox ON mailbox.id = delivery.mailbox_id
                WHERE delivery.message_id = ?
                ORDER BY delivery.id ASC
                """,
                (message_id,),
            ).fetchall()
            self._authorize_message_resources(current_principal, resource_rows)
            self._replace_delivery_delete_request(
                connection,
                [str(row["id"]) for row in resource_rows],
            )
            try:
                return self._delete_requested_deliveries(connection, deleted_at)
            finally:
                connection.execute("DELETE FROM _message_delivery_delete_request")

        return await self._runtime.writer.execute(operation)

    async def soft_delete_deliveries(
        self,
        delivery_ids: list[str],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        deleted_at = utc_now()
        unique_ids = []
        seen: set[str] = set()
        for delivery_id in delivery_ids:
            if delivery_id in seen:
                continue
            seen.add(delivery_id)
            unique_ids.append(delivery_id)
        if not unique_ids:
            return {"deleted": 0, "delivery_ids": []}

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            current_principal = self._runtime.api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="messages.write",
            )
            self._replace_delivery_delete_request(connection, unique_ids)
            try:
                rows = connection.execute(
                    """
                    SELECT
                        delivery.id,
                        delivery.mailbox_id,
                        mailbox.domain_id,
                        mailbox.address_canonical
                    FROM message_deliveries AS delivery
                    JOIN _message_delivery_delete_request AS requested
                      ON requested.id = delivery.id
                    JOIN mailboxes AS mailbox ON mailbox.id = delivery.mailbox_id
                    WHERE delivery.status = 'active'
                    ORDER BY delivery.id ASC
                    """
                ).fetchall()
                if current_principal is not None and not current_principal.legacy_credential:
                    for row in rows:
                        ensure_mailbox_access(
                            current_principal,
                            str(row["address_canonical"]),
                            int(row["domain_id"]),
                            "messages.write",
                        )
                if not rows:
                    return {"deleted": 0, "delivery_ids": []}
                return self._delete_requested_deliveries(connection, deleted_at)
            finally:
                connection.execute("DELETE FROM _message_delivery_delete_request")

        return await self._runtime.writer.execute(operation)

    @staticmethod
    def _authorize_message_resources(
        principal: PermissionContext | None,
        rows: list[sqlite3.Row],
    ) -> None:
        if principal is None:
            return
        if principal.legacy_credential:
            return
        if not rows:
            raise PermissionDenied("message has no authorized mailbox resources")
        for row in rows:
            ensure_mailbox_access(
                principal,
                str(row["address_canonical"]),
                int(row["domain_id"]),
                "messages.write",
            )

    @staticmethod
    def _replace_delivery_delete_request(
        connection: sqlite3.Connection,
        delivery_ids: list[str],
    ) -> None:
        connection.execute(
            """
            CREATE TEMP TABLE IF NOT EXISTS _message_delivery_delete_request (
                id TEXT PRIMARY KEY
            ) WITHOUT ROWID
            """
        )
        connection.execute("DELETE FROM _message_delivery_delete_request")
        connection.executemany(
            "INSERT OR IGNORE INTO _message_delivery_delete_request (id) VALUES (?)",
            ((delivery_id,) for delivery_id in delivery_ids),
        )

    def _delete_requested_deliveries(
        self,
        connection: sqlite3.Connection,
        deleted_at: str,
    ) -> dict[str, Any]:
        rows = connection.execute(
            """
            SELECT delivery.id
            FROM message_deliveries AS delivery
            JOIN _message_delivery_delete_request AS requested
              ON requested.id = delivery.id
            WHERE delivery.status = 'active'
            ORDER BY delivery.id ASC
            """
        ).fetchall()
        if not rows:
            return {"deleted": 0, "delivery_ids": []}
        connection.execute(
            """
            UPDATE message_deliveries
            SET status = 'deleted',
                deleted_at = COALESCE(deleted_at, ?),
                expires_at = ?
            WHERE status = 'active'
              AND EXISTS (
                  SELECT 1
                  FROM _message_delivery_delete_request AS requested
                  WHERE requested.id = message_deliveries.id
              )
            """,
            (deleted_at, deleted_at),
        )
        self._refresh_deleted_mailbox_summaries(connection, deleted_at)
        return {
            "deleted": len(rows),
            "delivery_ids": [str(row["id"]) for row in rows],
        }

    @staticmethod
    def _refresh_deleted_mailbox_summaries(connection: sqlite3.Connection, deleted_at: str) -> None:
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
                SELECT DISTINCT delivery.mailbox_id
                FROM message_deliveries AS delivery
                JOIN _message_delivery_delete_request AS requested
                  ON requested.id = delivery.id
            )
            """,
            (deleted_at,),
        )

    async def get_public_mailbox_view(
        self,
        mailbox_address: str,
        *,
        surface: str,
        limit: int = 50,
        offset: int = 0,
        cursor: tuple[str, str] | None = None,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        mailbox = await self._runtime.get_mailbox_view(
            mailbox_address,
            limit=limit,
            offset=offset,
            cursor=cursor,
            request_ip=request_ip,
            surface=surface,
        )
        items = [self._prepare_public_mailbox_item(item, surface=surface) for item in mailbox["items"]]
        return {**mailbox, "items": items}

    async def get_public_mailbox_verification_codes(
        self,
        mailbox_address: str,
        *,
        limit: int = 50,
        offset: int = 0,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.list_mailbox_verification_codes(
            mailbox_address,
            limit=limit,
            offset=offset,
            request_ip=request_ip,
            surface="api",
        )

    async def get_public_delivery_verification_code(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_delivery_verification_code(
            mailbox_address,
            delivery_id,
            request_ip=request_ip,
            surface="api",
        )

    async def get_public_delivery_detail(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        detail = await self._runtime.get_delivery_detail(
            mailbox_address,
            delivery_id,
            request_ip=request_ip,
            surface=surface,
        )
        # Storage layout is an implementation detail and can expose host paths,
        # retention partitions and message identifiers.  Public consumers only
        # receive opaque resource IDs and download routes.
        detail.pop("raw_path", None)
        detail["attachments"] = [
            {key: value for key, value in attachment.items() if key != "storage_path"}
            for attachment in detail.get("attachments", [])
        ]
        return detail

    async def get_public_mailbox_item(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        item = await self._runtime.get_mailbox_delivery_item(
            mailbox_address,
            delivery_id,
            request_ip=request_ip,
            surface=surface,
        )
        return self._prepare_public_mailbox_item(item, surface=surface)

    async def get_public_raw_message(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> bytes:
        return await self._runtime.get_public_raw_message(
            mailbox_address,
            delivery_id,
            surface=surface,
            request_ip=request_ip,
        )

    async def get_public_raw_file(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_public_raw_file(
            mailbox_address,
            delivery_id,
            surface=surface,
            request_ip=request_ip,
        )

    async def get_public_html_preview_srcdoc(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> str:
        preview = await self.get_public_html_preview(
            mailbox_address,
            delivery_id,
            surface=surface,
            request_ip=request_ip,
        )
        return str(preview["srcdoc"])

    async def get_public_html_preview(
        self,
        mailbox_address: str,
        delivery_id: str,
        *,
        surface: str,
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        detail = await self.get_public_delivery_detail(
            mailbox_address,
            delivery_id,
            surface=surface,
            request_ip=request_ip,
        )
        return await asyncio.to_thread(
            self._build_public_html_preview_sync,
            detail,
        )

    def _build_public_html_preview_sync(self, detail: dict[str, Any]) -> dict[str, Any]:
        attachments = self._load_attachments_with_content_ids(
            str(detail["message_id"]),
            list(detail["attachments"]),
        )
        html_body, inline_metadata = self.rewrite_cid_references_bounded(
            detail["html_body"] or "",
            attachments,
        )
        return {
            "srcdoc": self.build_public_html_preview_document(html_body),
            "html_body_truncated": bool(detail.get("html_body_truncated")),
            **inline_metadata,
        }

    def build_public_html_preview_document(self, html_body: str) -> str:
        return (
            "<!doctype html>"
            '<html lang="zh-CN">'
            "<head>"
            '<meta charset="utf-8" />'
            '<meta http-equiv="Content-Security-Policy" content="default-src \'none\'; img-src data:; style-src \'unsafe-inline\'; form-action \'none\'; connect-src \'none\'; object-src \'none\'; frame-src \'none\'; script-src \'none\'" />'
            '<meta name="referrer" content="no-referrer" />'
            '<base href="about:srcdoc" />'
            "</head>"
            f"<body>{html_body}</body>"
            "</html>"
        )

    def rewrite_cid_references(
        self,
        html_body: str,
        attachments: list[dict[str, Any]],
    ) -> str:
        rewritten, _metadata = self.rewrite_cid_references_bounded(html_body, attachments)
        return rewritten

    def rewrite_cid_references_bounded(
        self,
        html_body: str,
        attachments: list[dict[str, Any]],
    ) -> tuple[str, dict[str, int]]:
        referenced_content_ids = {
            self._normalize_cid_reference(match.group(1))
            for match in _CID_REFERENCE_RE.finditer(html_body)
        }
        referenced_content_ids.discard("")
        attachment_routes: dict[str, str] = {}
        embedded_source_bytes = 0
        embedded_encoded_bytes = 0
        skipped_count = 0
        item_limit = int(self._runtime.settings.message_preview_inline_item_bytes)
        total_limit = int(self._runtime.settings.message_preview_inline_total_bytes)
        for attachment in attachments:
            content_id = self._normalize_cid_reference(attachment.get("content_id"))
            if not content_id or content_id not in referenced_content_ids or content_id in attachment_routes:
                continue
            remaining = total_limit - embedded_source_bytes
            data_url = self._build_inline_data_url_bounded(
                attachment,
                max_source_bytes=min(item_limit, remaining) if remaining > 0 else 0,
            )
            if data_url is None:
                skipped_count += 1
                continue
            route, source_bytes = data_url
            attachment_routes[content_id] = route
            embedded_source_bytes += source_bytes
            embedded_encoded_bytes += len(route.encode("ascii"))

        def replace_reference(match: re.Match[str]) -> str:
            reference = self._normalize_cid_reference(match.group(1))
            return attachment_routes.get(reference, match.group(0))

        metadata = {
            "inline_preview_embedded_count": len(attachment_routes),
            "inline_preview_skipped_count": skipped_count,
            "inline_preview_embedded_source_bytes": embedded_source_bytes,
            "inline_preview_embedded_encoded_bytes": embedded_encoded_bytes,
            "inline_preview_item_limit_bytes": item_limit,
            "inline_preview_total_limit_bytes": total_limit,
        }
        return _CID_REFERENCE_RE.sub(replace_reference, html_body), metadata

    def _empty_inline_preview_metadata(self) -> dict[str, int]:
        return {
            "inline_preview_embedded_count": 0,
            "inline_preview_skipped_count": 0,
            "inline_preview_embedded_source_bytes": 0,
            "inline_preview_embedded_encoded_bytes": 0,
            "inline_preview_item_limit_bytes": int(
                self._runtime.settings.message_preview_inline_item_bytes
            ),
            "inline_preview_total_limit_bytes": int(
                self._runtime.settings.message_preview_inline_total_bytes
            ),
        }

    def _normalize_cid_reference(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().strip("<>").lower()

    def _build_inline_data_url_bounded(
        self,
        attachment: dict[str, Any],
        *,
        max_source_bytes: int,
    ) -> tuple[str, int] | None:
        content_type = self._normalize_content_type(attachment.get("content_type"))
        if content_type not in SAFE_INLINE_CONTENT_TYPES or max_source_bytes < 1:
            return None
        declared_size = int(attachment.get("size_bytes") or 0)
        if declared_size < 0 or declared_size > max_source_bytes:
            return None
        try:
            content, truncated, source_bytes = self._runtime.storage.read_bytes_limited(
                str(attachment["storage_path"]),
                max_source_bytes,
            )
        except (OSError, RuntimeError, ValueError):
            return None
        if truncated or source_bytes > max_source_bytes:
            return None
        encoded = base64.b64encode(content).decode("ascii")
        return f"data:{content_type};base64,{encoded}", len(content)

    def _load_attachments_with_content_ids(
        self,
        message_id: str,
        attachments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not attachments:
            return attachments

        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT id, content_id, storage_path
                FROM attachments
                WHERE message_id = ?
                ORDER BY part_index ASC
                """,
                (message_id,),
            ).fetchall()

        stored = {
            str(row["id"]): {
                "content_id": row["content_id"],
                "storage_path": row["storage_path"],
            }
            for row in rows
        }
        enriched_attachments: list[dict[str, Any]] = []
        for attachment in attachments:
            payload = dict(attachment)
            metadata = stored.get(str(payload["id"]))
            if metadata is not None:
                payload.update(metadata)
            enriched_attachments.append(payload)
        return enriched_attachments

    def _normalize_content_type(self, value: Any) -> str:
        return str(value or "").split(";", 1)[0].strip().lower()

    def _message_filter_sql(
        self,
        *,
        query: str | None,
        parse_status: str | None,
        mailbox_id: int | None,
        allowed_domain_ids: tuple[int, ...] | None = None,
        allowed_mailbox_patterns: tuple[str, ...] = (),
    ) -> tuple[str, list[Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if query:
            pattern = f"%{query.strip()}%"
            clauses.append(
                "(m.subject LIKE ? OR m.from_addr LIKE ? OR m.envelope_from LIKE ? OR d.rcpt_to LIKE ?)"
            )
            params.extend([pattern, pattern, pattern, pattern])
        if parse_status:
            if parse_status not in {"pending", "parsed", "failed"}:
                raise ValueError("invalid parse_status")
            clauses.append("m.parse_status = ?")
            params.append(parse_status)
        if mailbox_id is not None:
            clauses.append("d.mailbox_id = ?")
            params.append(int(mailbox_id))
        if allowed_domain_ids is not None:
            if not allowed_domain_ids:
                clauses.append("0 = 1")
            else:
                placeholders = ", ".join("?" for _ in allowed_domain_ids)
                # A shared message is visible only when every one of its
                # deliveries is within the caller's resource grant.
                clauses.append(
                    f"EXISTS (SELECT 1 FROM message_deliveries ad JOIN mailboxes amb ON amb.id = ad.mailbox_id "
                    f"WHERE ad.message_id = m.id AND amb.domain_id IN ({placeholders}))"
                )
                params.extend(int(item) for item in allowed_domain_ids)
                clauses.append(
                    f"NOT EXISTS (SELECT 1 FROM message_deliveries ud JOIN mailboxes umb ON umb.id = ud.mailbox_id "
                    f"WHERE ud.message_id = m.id AND umb.domain_id NOT IN ({placeholders}))"
                )
                params.extend(int(item) for item in allowed_domain_ids)
        if allowed_mailbox_patterns:
            allowed_pattern_sql = " OR ".join(
                "pmb.address_canonical GLOB ?" for _ in allowed_mailbox_patterns
            )
            clauses.append(
                "EXISTS (SELECT 1 FROM message_deliveries pd "
                "JOIN mailboxes pmb ON pmb.id = pd.mailbox_id "
                f"WHERE pd.message_id = m.id AND ({allowed_pattern_sql}))"
            )
            params.extend(str(pattern) for pattern in allowed_mailbox_patterns)
            denied_pattern_sql = " AND ".join(
                "pmb.address_canonical NOT GLOB ?" for _ in allowed_mailbox_patterns
            )
            clauses.append(
                "NOT EXISTS (SELECT 1 FROM message_deliveries pd "
                "JOIN mailboxes pmb ON pmb.id = pd.mailbox_id "
                f"WHERE pd.message_id = m.id AND ({denied_pattern_sql}))"
            )
            params.extend(str(pattern) for pattern in allowed_mailbox_patterns)
        if not clauses:
            return "", params
        return "WHERE " + " AND ".join(clauses), params

    def _prepare_public_mailbox_item(self, item: dict[str, Any], *, surface: str) -> dict[str, Any]:
        payload = dict(item)
        if surface == "web":
            payload["verification_code"] = payload.get("verification_code")
        payload.pop("text_preview", None)
        payload.pop("text_body_path", None)
        payload.pop("html_body_path", None)
        return payload
