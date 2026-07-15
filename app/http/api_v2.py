from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import sqlite3
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from fnmatch import fnmatchcase
from http import HTTPStatus
from pathlib import Path
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Query, Request, Security, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.routing import APIRoute
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from starlette.exceptions import HTTPException as StarletteHTTPException

from app.auth.api_keys import ApiKeyAuthorizationError
from app.auth.permissions import (
    PermissionContext,
    PermissionDenied,
    api_key_is_within_principal,
)
from app.db.connection import connect_database
from app.db.read_pool import (
    SQLiteReadPoolClosedError,
    SQLiteReadPoolOverloadedError,
    SQLiteReadPoolPausedError,
    SQLiteReadPoolTimeoutError,
)
from app.ingest.storage import utc_now
from app.observability import current_request_id
from app.services.attachments import AttachmentService
from app.services.dashboard import get_dashboard_service
from app.services.dns_check import DnsCheckService

from .api_models import (
    AdminCreate,
    AdminOut,
    AdminPasswordReset,
    AdminUpdate,
    ApiKeyActionOut,
    ApiKeyCreate,
    ApiKeyOut,
    ApiKeySecretOut,
    ApiKeyUpdate,
    AttachmentOut,
    AuditEventOut,
    DeleteOut,
    DashboardStatusOut,
    DeliveryOut,
    DomainCreate,
    DomainOut,
    DomainUpdate,
    Envelope,
    MailboxOut,
    MailboxUpdate,
    MaintenanceResultOut,
    MessageDetailOut,
    MessageSummaryOut,
    PageInfo,
    PrincipalOut,
    ProblemDetails,
    PublicMessageDetailOut,
    PublicMessageSummaryOut,
    ReparseOut,
    ResourceDeleteOut,
    SessionRevokeOut,
    SettingsOut,
    SettingsUpdate,
    SmtpEventOut,
    SmtpSessionDetailOut,
    SmtpSessionOut,
    VerificationCodeOut,
)


logger = logging.getLogger("rapid_inbox.api_v2")


API_KEY_SCAN_MIN_ROWS = 1000
API_KEY_SCAN_ROWS_PER_VISIBLE_ITEM = 20
API_KEY_SCAN_MAX_ROWS = 5000
API_KEY_SCAN_BATCH_MAX_ROWS = 900


class ApiProblem(Exception):
    def __init__(
        self,
        status_code: int,
        code: str,
        detail: str,
        *,
        title: str | None = None,
        headers: dict[str, str] | None = None,
        errors: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.code = code
        self.detail = detail
        self.title = title or _status_title(status_code)
        self.headers = headers or {}
        self.errors = errors


def _status_title(status_code: int) -> str:
    try:
        return HTTPStatus(status_code).phrase
    except ValueError:
        return "Request failed"


def _request_id(request: Request) -> str:
    request_id = str(getattr(request.state, "request_id", "") or current_request_id())
    return request_id or uuid.uuid4().hex


def _problem_response(request: Request, problem: ApiProblem) -> JSONResponse:
    payload = ProblemDetails(
        type=f"urn:rapid-inbox:problem:{problem.code}",
        title=problem.title,
        status=problem.status_code,
        detail=problem.detail,
        instance=request.url.path,
        code=problem.code,
        request_id=_request_id(request),
        errors=problem.errors,
    )
    return JSONResponse(
        payload.model_dump(mode="json"),
        status_code=problem.status_code,
        media_type="application/problem+json",
        headers=problem.headers,
    )


class ProblemRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        original_handler = super().get_route_handler()

        async def problem_handler(request: Request) -> Response:
            try:
                return await original_handler(request)
            except ApiProblem as exc:
                return _problem_response(request, exc)
            except RequestValidationError as exc:
                errors = [
                    {
                        "location": [str(item) if not isinstance(item, int) else item for item in error["loc"]],
                        "message": str(error["msg"]),
                        "code": str(error["type"]),
                    }
                    for error in exc.errors()
                ]
                return _problem_response(
                    request,
                    ApiProblem(
                        status.HTTP_422_UNPROCESSABLE_CONTENT,
                        "validation_error",
                        "The request did not satisfy the API schema.",
                        errors=errors,
                    ),
                )
            except StarletteHTTPException as exc:
                return _problem_response(
                    request,
                    ApiProblem(
                        exc.status_code,
                        "http_error",
                        str(exc.detail),
                        headers=dict(exc.headers or {}),
                    ),
                )
            except Exception:
                logger.exception("Unhandled API v2 request failure", extra={"path": request.url.path})
                return _problem_response(
                    request,
                    ApiProblem(500, "internal_error", "An unexpected server error occurred."),
                )

        return problem_handler


PROBLEM_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ProblemDetails, "description": "Invalid request"},
    401: {"model": ProblemDetails, "description": "Authentication required"},
    403: {"model": ProblemDetails, "description": "Permission denied"},
    404: {"model": ProblemDetails, "description": "Resource not found"},
    409: {"model": ProblemDetails, "description": "Resource conflict"},
    422: {"model": ProblemDetails, "description": "Validation failed"},
    429: {"model": ProblemDetails, "description": "Rate limit exceeded"},
    503: {"model": ProblemDetails, "description": "Service temporarily unavailable"},
}

router = APIRouter(
    prefix="/api/v2",
    tags=["API v2"],
    route_class=ProblemRoute,
    responses=PROBLEM_RESPONSES,
)

bearer_scheme = HTTPBearer(
    auto_error=False,
    scheme_name="BearerAuth",
    bearerFormat="Rapid Inbox API key",
    description="Use `Authorization: Bearer ri_<kind>_<prefix>_<secret>`.",
)


async def require_principal(
    request: Request,
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(bearer_scheme)],
) -> PermissionContext:
    if "api_key" in request.query_params:
        raise ApiProblem(
            400,
            "query_credentials_not_allowed",
            "API credentials are accepted only in the Authorization header.",
        )
    if credentials is None or credentials.scheme.lower() != "bearer" or not credentials.credentials:
        raise ApiProblem(
            401,
            "authentication_required",
            "A valid Bearer API key is required.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    request_ip = request.client.host if request.client is not None else None
    try:
        api_keys = request.app.state.runtime.api_keys
        principal = api_keys.authenticate_plain_text_cached(
            credentials.credentials,
            request_ip=request_ip,
        )
        if principal is None:
            principal = await asyncio.to_thread(
                api_keys.authenticate_plain_text,
                credentials.credentials,
                request_ip=request_ip,
            )
    except LookupError as exc:
        raise ApiProblem(
            401,
            "invalid_credential",
            "The Bearer API key is invalid or inactive.",
            headers={"WWW-Authenticate": 'Bearer error="invalid_token"'},
        ) from exc
    if principal.kind not in {"admin", "service", "public"}:
        raise ApiProblem(403, "principal_kind_not_allowed", "This credential kind cannot access API v2.")

    try:
        await request.app.state.runtime.api_keys.record_usage(principal, ip=request_ip)
    except StarletteHTTPException as exc:
        if exc.status_code == 429:
            raise ApiProblem(
                429,
                "rate_limit_exceeded",
                "The API key rate limit has been exceeded.",
                headers={"Retry-After": "60"},
            ) from exc
        if exc.status_code == 403:
            raise ApiProblem(403, "credential_ip_not_allowed", "The API key is not allowed from this IP.") from exc
        raise ApiProblem(401, "invalid_credential", "The Bearer API key is no longer valid.") from exc
    return principal


Principal = Annotated[PermissionContext, Security(require_principal)]


def _require_scope(principal: PermissionContext, required_scope: str) -> None:
    if required_scope in principal.scopes:
        return
    if required_scope.endswith(".read") and f"{required_scope[:-5]}.write" in principal.scopes:
        return
    raise ApiProblem(403, "insufficient_scope", "The credential lacks the required scope.")


def _allowed_domain_ids(principal: PermissionContext) -> tuple[int, ...] | None:
    if principal.domain_grant_mode == "all":
        return None
    if principal.domain_grant_mode == "selected":
        return tuple(sorted({int(domain_id) for domain_id in principal.domain_ids}))
    return ()


def _require_global_grant(principal: PermissionContext) -> None:
    if principal.domain_grant_mode != "all":
        raise ApiProblem(403, "global_grant_required", "This operation requires an all-domain grant.")


def _domain_allowed(principal: PermissionContext, domain_id: int) -> bool:
    allowed = _allowed_domain_ids(principal)
    return allowed is None or domain_id in allowed


def _mailbox_allowed(principal: PermissionContext, domain_id: int, address: str) -> bool:
    if not _domain_allowed(principal, domain_id):
        return False
    return not principal.mailbox_patterns or any(
        fnmatchcase(address, pattern)
        for pattern in principal.mailbox_patterns
    )


def _grant_cursor_value(principal: PermissionContext) -> dict[str, Any]:
    return {
        "mode": principal.domain_grant_mode,
        "domain_ids": list(_allowed_domain_ids(principal) or ()),
        "mailbox_patterns": list(principal.mailbox_patterns),
    }


def _filter_fingerprint(filters: dict[str, Any]) -> str:
    encoded = json.dumps(filters, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:24]


def _cursor_signing_key(request: Request) -> bytes:
    settings = request.app.state.settings
    configured_secret = str(getattr(settings, "api_cursor_secret", "") or "").strip()
    if configured_secret:
        secret = configured_secret.encode("utf-8")
    else:
        if settings.externally_bound():
            raise ApiProblem(
                500,
                "cursor_signing_unavailable",
                "API cursor signing is not configured.",
            )
        # The deterministic fallback is intentionally limited to loopback-only
        # development. External deployments fail during startup without an
        # explicit API_CURSOR_SECRET.
        database_path = Path(settings.database_path).resolve()
        secret = f"rapid-inbox-loopback-cursor:{database_path}".encode("utf-8")
    return hmac.digest(secret, b"rapid-inbox-api-v2-cursor-v1", "sha256")


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _b64url_decode(value: str) -> bytes:
    if not value or "=" in value:
        raise ValueError("non-canonical base64url")
    padded = value + "=" * (-len(value) % 4)
    decoded = base64.b64decode(padded.encode("ascii"), altchars=b"-_", validate=True)
    if _b64url_encode(decoded) != value:
        raise ValueError("non-canonical base64url")
    return decoded


def _encode_cursor(
    request: Request,
    resource: str,
    position: dict[str, Any],
    filters: dict[str, Any],
) -> str:
    payload = {
        "v": 2,
        "resource": resource,
        "filter": _filter_fingerprint(filters),
        "position": position,
    }
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.digest(_cursor_signing_key(request), encoded, "sha256")
    return f"{_b64url_encode(encoded)}.{_b64url_encode(signature)}"


def _decode_cursor(
    request: Request,
    cursor: str | None,
    resource: str,
    filters: dict[str, Any],
) -> dict[str, Any] | None:
    if cursor is None:
        return None
    if not cursor or len(cursor) > 2048:
        raise ApiProblem(400, "invalid_cursor", "The pagination cursor is invalid.")
    try:
        encoded_payload, encoded_signature = cursor.split(".", 1)
        if not encoded_payload or not encoded_signature or "." in encoded_signature:
            raise ValueError("invalid cursor framing")
        payload_bytes = _b64url_decode(encoded_payload)
        provided_signature = _b64url_decode(encoded_signature)
        expected_signature = hmac.digest(_cursor_signing_key(request), payload_bytes, "sha256")
        if not hmac.compare_digest(provided_signature, expected_signature):
            raise ValueError("invalid cursor signature")
        payload = json.loads(payload_bytes.decode("utf-8"))
    except (binascii.Error, ValueError, UnicodeError, json.JSONDecodeError) as exc:
        raise ApiProblem(400, "invalid_cursor", "The pagination cursor is invalid.") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("v") != 2
        or payload.get("resource") != resource
        or payload.get("filter") != _filter_fingerprint(filters)
        or not isinstance(payload.get("position"), dict)
    ):
        raise ApiProblem(400, "invalid_cursor", "The pagination cursor is invalid for this query.")
    return dict(payload["position"])


async def _fetch_all(request: Request, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    try:
        return await request.app.state.runtime.read_pool.fetch_all(query, params)
    except SQLiteReadPoolOverloadedError as exc:
        raise ApiProblem(
            503,
            "database_read_overloaded",
            "Database read capacity is temporarily exhausted.",
            headers={"Retry-After": "1"},
        ) from exc
    except SQLiteReadPoolTimeoutError as exc:
        raise ApiProblem(
            503,
            "database_read_timeout",
            "The database read exceeded its execution deadline.",
            headers={"Retry-After": "1"},
        ) from exc
    except (SQLiteReadPoolPausedError, SQLiteReadPoolClosedError) as exc:
        raise ApiProblem(
            503,
            "database_read_unavailable",
            "Database reads are temporarily unavailable.",
            headers={"Retry-After": "1"},
        ) from exc


async def _fetch_one(request: Request, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
    try:
        return await request.app.state.runtime.read_pool.fetch_one(query, params)
    except SQLiteReadPoolOverloadedError as exc:
        raise ApiProblem(
            503,
            "database_read_overloaded",
            "Database read capacity is temporarily exhausted.",
            headers={"Retry-After": "1"},
        ) from exc
    except SQLiteReadPoolTimeoutError as exc:
        raise ApiProblem(
            503,
            "database_read_timeout",
            "The database read exceeded its execution deadline.",
            headers={"Retry-After": "1"},
        ) from exc
    except (SQLiteReadPoolPausedError, SQLiteReadPoolClosedError) as exc:
        raise ApiProblem(
            503,
            "database_read_unavailable",
            "Database reads are temporarily unavailable.",
            headers={"Retry-After": "1"},
        ) from exc


def _page(limit: int, rows: list[dict[str, Any]], cursor_factory: Callable[[dict[str, Any]], str]) -> PageInfo:
    has_more = len(rows) > limit
    visible_rows = rows[:limit]
    next_cursor = cursor_factory(visible_rows[-1]) if has_more and visible_rows else None
    return PageInfo(limit=limit, has_more=has_more, next_cursor=next_cursor)


def _envelope(request: Request, data: Any, page: PageInfo | None = None) -> dict[str, Any]:
    return {"data": data, "page": page, "request_id": _request_id(request)}


def _decode_json(value: Any) -> Any | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


def _hydrate_api_key_rows(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> list[dict[str, Any]]:
    if not rows:
        return []
    ids = [int(row["id"]) for row in rows]
    placeholders = ", ".join("?" for _ in ids)
    scopes: dict[int, list[str]] = {api_key_id: [] for api_key_id in ids}
    domains: dict[int, list[int]] = {api_key_id: [] for api_key_id in ids}
    mailboxes: dict[int, list[str]] = {api_key_id: [] for api_key_id in ids}
    for row in connection.execute(
        f"SELECT api_key_id, scope FROM api_key_scopes WHERE api_key_id IN ({placeholders}) ORDER BY api_key_id, scope",
        tuple(ids),
    ):
        scopes[int(row["api_key_id"])].append(str(row["scope"]))
    for row in connection.execute(
        f"SELECT api_key_id, domain_id FROM api_key_domain_grants WHERE api_key_id IN ({placeholders}) ORDER BY api_key_id, domain_id",
        tuple(ids),
    ):
        domains[int(row["api_key_id"])].append(int(row["domain_id"]))
    for row in connection.execute(
        f"SELECT api_key_id, mailbox_pattern FROM api_key_mailbox_grants WHERE api_key_id IN ({placeholders}) ORDER BY api_key_id, mailbox_pattern",
        tuple(ids),
    ):
        mailboxes[int(row["api_key_id"])].append(str(row["mailbox_pattern"]))

    hydrated: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        api_key_id = int(payload["id"])
        decoded_cidrs = _decode_json(payload.pop("allowed_ip_cidrs", None))
        if isinstance(decoded_cidrs, str):
            allowed_ip_cidrs = [decoded_cidrs]
        elif isinstance(decoded_cidrs, list):
            allowed_ip_cidrs = [str(item) for item in decoded_cidrs if str(item).strip()]
        else:
            allowed_ip_cidrs = []
        payload.update(
            {
                "id": api_key_id,
                "allow_header": bool(payload["allow_header"]),
                "allow_query": bool(payload["allow_query"]),
                "rate_limit_per_min": int(payload["rate_limit_per_min"]),
                "allowed_ip_cidrs": allowed_ip_cidrs,
                "scopes": scopes[api_key_id],
                "domain_ids": domains[api_key_id],
                "mailbox_patterns": mailboxes[api_key_id],
            }
        )
        hydrated.append(payload)
    return hydrated


@dataclass(frozen=True, slots=True)
class _ApiKeyScanPage:
    items: list[dict[str, Any]]
    continuation_position: tuple[str, int] | None
    has_more: bool
    scanned_rows: int


def _list_api_keys_keyset_sync(
    database_path: Path,
    principal: PermissionContext,
    position: tuple[str, int] | None,
    wanted: int,
) -> _ApiKeyScanPage:
    if wanted < 1:
        raise ValueError("wanted must be positive")
    visible: list[dict[str, Any]] = []
    scan_position = position
    scanned_rows = 0
    scan_budget = min(
        API_KEY_SCAN_MAX_ROWS,
        max(API_KEY_SCAN_MIN_ROWS, wanted * API_KEY_SCAN_ROWS_PER_VISIBLE_ITEM),
    )
    batch_size = max(128, min(API_KEY_SCAN_BATCH_MAX_ROWS, wanted * 4))
    with connect_database(database_path) as connection:
        while len(visible) < wanted and scanned_rows < scan_budget:
            clauses: list[str] = []
            params: list[Any] = []
            if scan_position is not None:
                clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
                params.extend([scan_position[0], scan_position[0], scan_position[1]])
            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            rows_to_process = min(batch_size, scan_budget - scanned_rows)
            rows = connection.execute(
                f"""
                SELECT
                    id, public_id, name, description, kind, key_prefix, status,
                    domain_grant_mode, allow_header, allow_query, rate_limit_per_min,
                    allowed_ip_cidrs, expires_at, last_used_at, last_used_ip,
                    revoked_at, created_at
                FROM api_keys
                {where_sql}
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (*params, rows_to_process + 1),
            ).fetchall()
            if not rows:
                return _ApiKeyScanPage(visible, None, False, scanned_rows)

            process_rows = list(rows[:rows_to_process])
            hydrated_rows = _hydrate_api_key_rows(connection, process_rows)
            for row_index, (row, payload) in enumerate(
                zip(process_rows, hydrated_rows, strict=True)
            ):
                scan_position = (str(row["created_at"]), int(row["id"]))
                scanned_rows += 1
                if _api_key_within_principal(principal, payload):
                    visible.append(payload)
                    if len(visible) >= wanted:
                        has_more = row_index + 1 < len(rows)
                        return _ApiKeyScanPage(
                            visible,
                            scan_position if has_more else None,
                            has_more,
                            scanned_rows,
                        )

            if len(rows) <= rows_to_process:
                return _ApiKeyScanPage(visible, None, False, scanned_rows)

    return _ApiKeyScanPage(
        visible,
        scan_position,
        scan_position is not None,
        scanned_rows,
    )


def _domain_out(payload: dict[str, Any], runtime: Any) -> DomainOut:
    root_domain = str(payload["root_domain_ascii"])
    recommendations = payload.get("dns_recommendations")
    if recommendations is None:
        recommendations = runtime.domains.dns_recommendations(root_domain)
    return DomainOut(
        id=int(payload["id"]),
        root_domain_ascii=root_domain,
        root_domain_unicode=payload.get("root_domain_unicode"),
        accept_exact=bool(payload["accept_exact"]),
        accept_subdomains=bool(payload["accept_subdomains"]),
        public_web_enabled=bool(payload["public_web_enabled"]),
        public_api_enabled=bool(payload["public_api_enabled"]),
        is_active=bool(payload["is_active"]),
        is_hidden=bool(payload.get("is_hidden", False)),
        local_part_case_sensitive=bool(payload.get("local_part_case_sensitive", False)),
        plus_addressing_mode=str(payload.get("plus_addressing_mode") or "keep"),
        max_message_size_bytes=int(payload.get("max_message_size_bytes") or 52_428_800),
        retention_days=payload.get("retention_days"),
        dns_status=str(payload.get("dns_status") or "unknown"),
        dns_last_checked_at=payload.get("dns_last_checked_at"),
        dns_details=_decode_json(payload.get("dns_details_json")),
        notes=payload.get("notes"),
        created_at=str(payload["created_at"]),
        updated_at=str(payload["updated_at"]),
        dns_recommendations=recommendations,
    )


def _mailbox_out(payload: dict[str, Any]) -> MailboxOut:
    return MailboxOut(
        id=int(payload["id"]),
        domain_id=int(payload["domain_id"]),
        root_domain_ascii=str(payload["root_domain_ascii"]),
        local_part_canonical=str(payload["local_part_canonical"]),
        rcpt_domain_ascii=str(payload["rcpt_domain_ascii"]),
        address_canonical=str(payload["address_canonical"]),
        address_display=str(payload["address_display"]),
        first_seen_at=str(payload["first_seen_at"]),
        last_seen_at=str(payload["last_seen_at"]),
        latest_message_at=payload.get("latest_message_at"),
        message_count=int(payload["message_count"]),
        public_enabled=bool(payload["public_enabled"]),
        is_hidden=bool(payload["is_hidden"]),
        notes=payload.get("notes"),
    )


def _message_summary_out(payload: dict[str, Any]) -> MessageSummaryOut:
    return MessageSummaryOut(
        id=str(payload["id"]),
        subject=payload.get("subject"),
        from_addr=payload.get("from_addr"),
        recipients=str(payload.get("recipients") or ""),
        received_at=str(payload["received_at"]),
        parse_status=str(payload["parse_status"]),
        parse_error=payload.get("parse_error"),
        has_attachments=bool(payload["has_attachments"]),
        attachment_count=int(payload["attachment_count"]),
        delivery_count=int(payload["delivery_count"]),
    )


def _message_detail_out(payload: dict[str, Any]) -> MessageDetailOut:
    deliveries = [
        DeliveryOut(
            delivery_id=str(item["delivery_id"]),
            mailbox_id=int(item["mailbox_id"]),
            mailbox=str(item["mailbox"]),
            rcpt_to=str(item["rcpt_to"]),
            delivered_at=str(item["delivered_at"]),
            status=str(item["status"]),
            deleted_at=item.get("deleted_at"),
            expires_at=item.get("expires_at"),
        )
        for item in payload.get("deliveries", [])
    ]
    attachments = [
        AttachmentOut(
            id=str(item["id"]),
            filename=item.get("filename"),
            safe_filename=item.get("safe_filename"),
            content_type=item.get("content_type"),
            content_disposition=item.get("content_disposition"),
            content_id=item.get("content_id"),
            size_bytes=int(item["size_bytes"]),
            is_inline=bool(item["is_inline"]),
        )
        for item in payload.get("attachments", [])
    ]
    return MessageDetailOut(
        id=str(payload["id"]),
        smtp_session_id=payload.get("smtp_session_id"),
        raw_sha256=str(payload["raw_sha256"]),
        raw_size_bytes=int(payload["raw_size_bytes"]),
        envelope_from=payload.get("envelope_from"),
        message_id_header=payload.get("message_id_header"),
        subject=payload.get("subject"),
        from_name=payload.get("from_name"),
        from_addr=payload.get("from_addr"),
        reply_to=payload.get("reply_to"),
        date_header=payload.get("date_header"),
        received_at=str(payload["received_at"]),
        indexed_at=payload.get("indexed_at"),
        parse_status=str(payload["parse_status"]),
        parse_error=payload.get("parse_error"),
        has_text=bool(payload["has_text"]),
        has_html=bool(payload["has_html"]),
        has_attachments=bool(payload["has_attachments"]),
        attachment_count=int(payload["attachment_count"]),
        text_preview=payload.get("text_preview"),
        text_body=str(payload.get("text_body") or ""),
        text_body_source_bytes=int(payload.get("text_body_source_bytes") or 0),
        text_body_preview_bytes=int(payload.get("text_body_preview_bytes") or 0),
        text_body_truncated=bool(payload.get("text_body_truncated")),
        html_body=str(payload.get("html_body") or ""),
        html_body_source_bytes=int(payload.get("html_body_source_bytes") or 0),
        html_body_preview_bytes=int(payload.get("html_body_preview_bytes") or 0),
        html_body_truncated=bool(payload.get("html_body_truncated")),
        headers=list(payload.get("headers") or []),
        headers_source_bytes=int(payload.get("headers_source_bytes") or 0),
        headers_truncated=bool(payload.get("headers_truncated")),
        inline_preview_embedded_count=int(payload.get("inline_preview_embedded_count") or 0),
        inline_preview_skipped_count=int(payload.get("inline_preview_skipped_count") or 0),
        inline_preview_embedded_source_bytes=int(
            payload.get("inline_preview_embedded_source_bytes") or 0
        ),
        inline_preview_embedded_encoded_bytes=int(
            payload.get("inline_preview_embedded_encoded_bytes") or 0
        ),
        inline_preview_item_limit_bytes=int(payload.get("inline_preview_item_limit_bytes") or 0),
        inline_preview_total_limit_bytes=int(payload.get("inline_preview_total_limit_bytes") or 0),
        deliveries=deliveries,
        attachments=attachments,
    )


def _public_message_summary_out(payload: dict[str, Any]) -> PublicMessageSummaryOut:
    return PublicMessageSummaryOut(
        delivery_id=str(payload["delivery_id"]),
        delivered_at=str(payload["delivered_at"]),
        message_id=str(payload["message_id"]),
        subject=payload.get("subject"),
        from_addr=payload.get("from_addr"),
        verification_code=payload.get("verification_code"),
        has_attachments=bool(payload.get("has_attachments")),
        parse_status=str(payload["parse_status"]),
    )


def _public_message_detail_out(payload: dict[str, Any]) -> PublicMessageDetailOut:
    return PublicMessageDetailOut.model_validate(payload)


def _verification_code_out(payload: dict[str, Any]) -> VerificationCodeOut:
    return VerificationCodeOut.model_validate(payload)


def _smtp_session_out(payload: dict[str, Any]) -> SmtpSessionOut:
    return SmtpSessionOut.model_validate({**payload, "tls_used": bool(payload["tls_used"])})


def _smtp_event_out(payload: dict[str, Any]) -> SmtpEventOut:
    return SmtpEventOut(
        id=int(payload["id"]),
        seq=int(payload["seq"]),
        event_type=str(payload["event_type"]),
        ts=str(payload["ts"]),
        payload=_decode_json(payload.get("payload_json")),
    )


def _admin_out(payload: dict[str, Any]) -> AdminOut:
    return AdminOut.model_validate(payload)


def _api_key_out(payload: dict[str, Any]) -> ApiKeyOut:
    projected = {
        field_name: payload[field_name]
        for field_name in ApiKeyOut.model_fields
        if field_name in payload
    }
    return ApiKeyOut.model_validate(projected)


def _effective_scopes(principal: PermissionContext) -> set[str]:
    scopes = set(principal.scopes)
    scopes.update(
        f"{scope[:-6]}.read"
        for scope in principal.scopes
        if scope.endswith(".write")
    )
    return scopes


def _api_key_within_principal(principal: PermissionContext, target: dict[str, Any]) -> bool:
    return api_key_is_within_principal(principal, target)


def _require_api_key_within_principal(principal: PermissionContext, target: dict[str, Any]) -> None:
    if not _api_key_within_principal(principal, target):
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.")


async def _authorized_mailbox(
    request: Request,
    principal: PermissionContext,
    mailbox_id: int,
) -> dict[str, Any]:
    try:
        mailbox = await asyncio.to_thread(request.app.state.runtime.mailboxes.get_mailbox, mailbox_id)
    except LookupError as exc:
        raise ApiProblem(404, "mailbox_not_found", "The mailbox was not found.") from exc
    if not _mailbox_allowed(principal, int(mailbox["domain_id"]), str(mailbox["address_canonical"])):
        raise ApiProblem(404, "mailbox_not_found", "The mailbox was not found.")
    return mailbox


def _authorized_public_mailbox(
    request: Request,
    principal: PermissionContext,
    mailbox_address: str,
) -> tuple[str, int]:
    _require_scope(principal, "public.read")
    match = request.app.state.runtime.domains.match_address(mailbox_address)
    if match is None or not _mailbox_allowed(
        principal,
        int(match.domain_id),
        str(match.address_canonical),
    ):
        # Do not disclose whether a mailbox/domain exists outside the key's
        # grant. Domain public_api and mailbox visibility are checked again by
        # MessageService before any data or file path is returned.
        raise ApiProblem(404, "public_mailbox_not_found", "The public mailbox was not found.")
    return str(match.address_canonical), int(match.domain_id)


async def _authorized_api_key(
    request: Request,
    principal: PermissionContext,
    api_key_id: int,
) -> dict[str, Any]:
    try:
        target = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
    except LookupError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    _require_api_key_within_principal(principal, target)
    return target


def _dashboard_status_out(snapshot: dict[str, Any]) -> DashboardStatusOut:
    return DashboardStatusOut.model_validate(
        {
            "generated_at": snapshot["generated_at"],
            "cache": snapshot["cache"],
            "health": snapshot["health"],
            "http": snapshot["http"],
            "mail": snapshot["mail"],
            "smtp": snapshot["smtp"],
            "ingestd": snapshot["ingestd"],
            "parse_queue": snapshot["parse_queue"],
            "database": snapshot["database"],
            "disk": snapshot["disk"],
            "background_tasks": snapshot["background_tasks"],
            "cleanup": snapshot["cleanup"],
            "recent_messages": snapshot["recent_messages"],
            "recent_domains": snapshot["recent_domains"],
            "delivery_chart": snapshot["delivery_chart"],
            "totals": snapshot["totals_raw"],
        }
    )


async def _message_is_allowed(request: Request, principal: PermissionContext, message_id: str) -> bool:
    resources = await _fetch_all(
        request,
        """
        SELECT mb.domain_id, mb.address_canonical
        FROM message_deliveries AS delivery
        JOIN mailboxes AS mb ON mb.id = delivery.mailbox_id
        WHERE delivery.message_id = ?
        """,
        (message_id,),
    )
    return bool(resources) and all(
        _mailbox_allowed(principal, int(resource["domain_id"]), str(resource["address_canonical"]))
        for resource in resources
    )


def _message_access_sql(
    principal: PermissionContext,
    alias: str = "m",
    *,
    require_delivery: bool = True,
) -> tuple[list[str], list[Any]]:
    clauses = (
        [
            f"EXISTS (SELECT 1 FROM message_deliveries AS access_delivery "
            f"WHERE access_delivery.message_id = {alias}.id)"
        ]
        if require_delivery
        else []
    )
    params: list[Any] = []
    allowed = _allowed_domain_ids(principal)
    if allowed is not None:
        if not allowed:
            clauses.append("0 = 1")
        else:
            placeholders = ", ".join("?" for _ in allowed)
            clauses.append(
                f"""
                NOT EXISTS (
                    SELECT 1
                    FROM message_deliveries AS denied_delivery
                    JOIN mailboxes AS denied_mailbox ON denied_mailbox.id = denied_delivery.mailbox_id
                    WHERE denied_delivery.message_id = {alias}.id
                      AND denied_mailbox.domain_id NOT IN ({placeholders})
                )
                """
            )
            params.extend(allowed)
    if principal.mailbox_patterns:
        pattern_sql = " OR ".join("pattern_mailbox.address_canonical GLOB ?" for _ in principal.mailbox_patterns)
        clauses.append(
            f"""
            NOT EXISTS (
                SELECT 1
                FROM message_deliveries AS pattern_delivery
                JOIN mailboxes AS pattern_mailbox ON pattern_mailbox.id = pattern_delivery.mailbox_id
                WHERE pattern_delivery.message_id = {alias}.id
                  AND NOT ({pattern_sql})
            )
            """
        )
        params.extend(principal.mailbox_patterns)
    return clauses, params


def _message_list_query(
    principal: PermissionContext,
    *,
    normalized_query: str | None,
    parse_status: str | None,
    mailbox_id: int | None,
    position: tuple[str, str] | None,
    limit: int,
) -> tuple[str, tuple[Any, ...]]:
    """Build a stable message keyset query without scanning denied history.

    An unrestricted global list deliberately remains driven by the
    ``messages(received_at, id)`` index. A mailbox, selected-domain grant, or
    mailbox pattern instead builds the comparatively sparse authorized
    delivery candidates first, then performs primary-key lookups in messages.
    The outer anti-probes preserve the fail-closed rule that *every* delivery
    of a visible message must remain within the caller's grant.
    """

    allowed = _allowed_domain_ids(principal)
    use_authorized_candidates = bool(
        mailbox_id is not None
        or allowed is not None
        or principal.mailbox_patterns
    )
    query_prefix = ""
    outer_params: list[Any]
    if use_authorized_candidates:
        candidate_clauses: list[str] = []
        candidate_params: list[Any] = []
        needs_mailbox = allowed is not None or bool(principal.mailbox_patterns)
        candidate_mailbox_join = (
            "JOIN mailboxes AS candidate_mailbox "
            "ON candidate_mailbox.id = candidate_delivery.mailbox_id"
            if needs_mailbox
            else ""
        )
        if mailbox_id is not None:
            candidate_clauses.append("candidate_delivery.mailbox_id = ?")
            candidate_params.append(mailbox_id)
        if allowed is not None:
            if not allowed:
                candidate_clauses.append("0 = 1")
            else:
                placeholders = ", ".join("?" for _ in allowed)
                candidate_clauses.append(f"candidate_mailbox.domain_id IN ({placeholders})")
                candidate_params.extend(allowed)
        if principal.mailbox_patterns:
            pattern_sql = " OR ".join(
                "candidate_mailbox.address_canonical GLOB ?"
                for _ in principal.mailbox_patterns
            )
            candidate_clauses.append(f"({pattern_sql})")
            candidate_params.extend(principal.mailbox_patterns)
        candidate_where_sql = " AND ".join(f"({clause})" for clause in candidate_clauses)
        query_prefix = f"""
        WITH authorized_message_candidates(message_id) AS (
            SELECT DISTINCT candidate_delivery.message_id
            FROM message_deliveries AS candidate_delivery
            {candidate_mailbox_join}
            WHERE {candidate_where_sql}
        )
        """
        source_sql = """
        authorized_message_candidates AS authorized_candidate
        CROSS JOIN messages AS m ON m.id = authorized_candidate.message_id
        """
        clauses, access_params = _message_access_sql(
            principal,
            require_delivery=False,
        )
        outer_params = [*candidate_params, *access_params]
    else:
        source_sql = "messages AS m"
        clauses, outer_params = _message_access_sql(principal)

    if normalized_query:
        search = f"%{normalized_query}%"
        clauses.append(
            """
            (
                m.subject LIKE ? OR m.from_addr LIKE ? OR m.envelope_from LIKE ?
                OR EXISTS (
                    SELECT 1 FROM message_deliveries AS query_delivery
                    WHERE query_delivery.message_id = m.id AND query_delivery.rcpt_to LIKE ?
                )
            )
            """
        )
        outer_params.extend([search, search, search, search])
    if parse_status is not None:
        clauses.append("m.parse_status = ?")
        outer_params.append(parse_status)
    if position is not None:
        received_at, message_id = position
        clauses.append("(m.received_at < ? OR (m.received_at = ? AND m.id < ?))")
        outer_params.extend([received_at, received_at, message_id])
    where_sql = (
        "WHERE " + " AND ".join(f"({clause})" for clause in clauses)
        if clauses
        else ""
    )
    outer_params.append(limit + 1)
    return (
        f"""
        {query_prefix}
        SELECT
            m.id, m.subject, m.from_addr,
            COALESCE((
                SELECT GROUP_CONCAT(recipient.rcpt_to, ', ')
                FROM (
                    SELECT DISTINCT rcpt_to
                    FROM message_deliveries
                    WHERE message_id = m.id
                    ORDER BY rcpt_to ASC
                ) AS recipient
            ), '') AS recipients,
            m.received_at, m.parse_status, m.parse_error,
            m.has_attachments, m.attachment_count,
            (SELECT COUNT(*) FROM message_deliveries AS count_delivery
             WHERE count_delivery.message_id = m.id) AS delivery_count
        FROM {source_sql}
        {where_sql}
        ORDER BY m.received_at DESC, m.id DESC
        LIMIT ?
        """,
        tuple(outer_params),
    )


async def _audit_mutation(
    request: Request,
    principal: PermissionContext,
    action: str,
    resource_type: str,
    resource_ref: str | None,
    details: Any | None = None,
) -> None:
    try:
        await request.app.state.runtime.audit.log(
            "api_key",
            str(principal.api_key_id or principal.public_id),
            action,
            resource_type,
            resource_ref,
            "success",
            ip=request.client.host if request.client is not None else None,
            user_agent=request.headers.get("user-agent"),
            details=details,
        )
    except Exception:
        logger.warning("API v2 audit write failed", exc_info=True, extra={"action": action})


@router.get("/me", response_model=Envelope[PrincipalOut], operation_id="getV2Principal")
async def get_me(request: Request, principal: Principal) -> dict[str, Any]:
    data = PrincipalOut(
        id=principal.public_id or str(principal.api_key_id or ""),
        name=principal.name,
        kind=principal.kind,
        scopes=sorted(principal.scopes),
        domain_grant_mode=principal.domain_grant_mode,
        domain_ids=sorted(principal.domain_ids),
        mailbox_patterns=list(principal.mailbox_patterns),
    )
    return _envelope(request, data)


def _public_cursor_position(position: dict[str, Any] | None) -> tuple[str, str] | None:
    if position is None:
        return None
    try:
        delivered_at = str(position["delivered_at"])
        delivery_id = str(position["delivery_id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ApiProblem(400, "invalid_cursor", "The public mailbox cursor is invalid.") from exc
    if not delivered_at or not delivery_id:
        raise ApiProblem(400, "invalid_cursor", "The public mailbox cursor is invalid.")
    return delivered_at, delivery_id


def _public_page(
    request: Request,
    *,
    resource: str,
    filters: dict[str, Any],
    limit: int,
    next_position: dict[str, Any] | None,
) -> PageInfo:
    next_cursor = None
    if next_position is not None:
        try:
            position = {
                "delivered_at": str(next_position["delivered_at"]),
                "delivery_id": str(next_position["delivery_id"]),
            }
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(500, "invalid_pagination_state", "The mailbox pagination state is invalid.") from exc
        next_cursor = _encode_cursor(request, resource, position, filters)
    return PageInfo(limit=limit, has_more=next_cursor is not None, next_cursor=next_cursor)


@router.get(
    "/public/mailboxes/{mailbox_address}/messages",
    response_model=Envelope[list[PublicMessageSummaryOut]],
    operation_id="listV2PublicMailboxMessages",
)
async def list_public_mailbox_messages(
    mailbox_address: str,
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    filters = {
        "mailbox": canonical_mailbox,
        "grant": _grant_cursor_value(principal),
    }
    position = _public_cursor_position(
        _decode_cursor(request, cursor, "public-mailbox-messages", filters)
    )
    try:
        result = await request.app.state.runtime.messages.get_public_mailbox_view(
            canonical_mailbox,
            surface="api",
            limit=limit,
            offset=0,
            cursor=position,
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_mailbox_not_found", "The public mailbox was not found.") from exc
    page = _public_page(
        request,
        resource="public-mailbox-messages",
        filters=filters,
        limit=limit,
        next_position=result.get("next_cursor"),
    )
    return _envelope(
        request,
        [_public_message_summary_out(item) for item in result.get("items") or ()],
        page,
    )


@router.get(
    "/public/mailboxes/{mailbox_address}/verification-codes",
    response_model=Envelope[list[VerificationCodeOut]],
    operation_id="listV2PublicMailboxVerificationCodes",
)
async def list_public_mailbox_verification_codes(
    mailbox_address: str,
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    filters = {
        "mailbox": canonical_mailbox,
        "grant": _grant_cursor_value(principal),
    }
    position = _public_cursor_position(
        _decode_cursor(request, cursor, "public-mailbox-verification-codes", filters)
    )
    try:
        result = await request.app.state.runtime.messages.get_public_mailbox_view(
            canonical_mailbox,
            surface="api",
            limit=limit,
            offset=0,
            cursor=position,
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_mailbox_not_found", "The public mailbox was not found.") from exc
    items = [
        _verification_code_out(
            {
                "delivery_id": item["delivery_id"],
                "message_id": item["message_id"],
                "received_at": item["delivered_at"],
                "subject": item.get("subject"),
                "from_addr": item.get("from_addr"),
                "parse_status": item["parse_status"],
                "verification_code": item.get("verification_code"),
            }
        )
        for item in result.get("items") or ()
    ]
    page = _public_page(
        request,
        resource="public-mailbox-verification-codes",
        filters=filters,
        limit=limit,
        next_position=result.get("next_cursor"),
    )
    return _envelope(request, items, page)


@router.get(
    "/public/mailboxes/{mailbox_address}/messages/{delivery_id}",
    response_model=Envelope[PublicMessageDetailOut],
    operation_id="getV2PublicMailboxMessage",
)
async def get_public_mailbox_message(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    try:
        detail = await request.app.state.runtime.messages.get_public_delivery_detail(
            canonical_mailbox,
            delivery_id,
            surface="api",
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_message_not_found", "The public message was not found.") from exc
    return _envelope(request, _public_message_detail_out(detail))


@router.get(
    "/public/mailboxes/{mailbox_address}/messages/{delivery_id}/verification-code",
    response_model=Envelope[VerificationCodeOut],
    operation_id="getV2PublicMailboxVerificationCode",
)
async def get_public_mailbox_verification_code(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    try:
        result = await request.app.state.runtime.messages.get_public_delivery_verification_code(
            canonical_mailbox,
            delivery_id,
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_message_not_found", "The public message was not found.") from exc
    return _envelope(request, _verification_code_out(result))


@router.get(
    "/public/mailboxes/{mailbox_address}/messages/{delivery_id}/raw",
    response_class=FileResponse,
    responses={200: {"content": {"message/rfc822": {}}, "description": "Raw RFC 822 message"}},
    operation_id="downloadV2PublicMailboxMessageRaw",
)
async def download_public_mailbox_message_raw(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    principal: Principal,
) -> Response:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    try:
        raw = await request.app.state.runtime.messages.get_public_raw_file(
            canonical_mailbox,
            delivery_id,
            surface="api",
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_message_raw_not_found", "The raw public message was not found.") from exc
    return FileResponse(
        raw["path"],
        media_type="message/rfc822",
        filename=f"{delivery_id}.eml",
        headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"},
    )


@router.get(
    "/public/mailboxes/{mailbox_address}/messages/{delivery_id}/attachments/{attachment_id}",
    response_class=FileResponse,
    responses={200: {"content": {"application/octet-stream": {}}, "description": "Public message attachment"}},
    operation_id="downloadV2PublicMailboxMessageAttachment",
)
async def download_public_mailbox_message_attachment(
    mailbox_address: str,
    delivery_id: str,
    attachment_id: str,
    request: Request,
    principal: Principal,
) -> Response:
    canonical_mailbox, _domain_id = _authorized_public_mailbox(request, principal, mailbox_address)
    service = AttachmentService(request.app.state.runtime, request.app.state.runtime.messages)
    try:
        attachment = await service.get_delivery_attachment_file(
            canonical_mailbox,
            delivery_id,
            attachment_id,
            surface="api",
            request_ip=request.client.host if request.client is not None else None,
        )
    except LookupError as exc:
        raise ApiProblem(404, "public_attachment_not_found", "The public attachment was not found.") from exc
    headers = service.build_attachment_response_headers(attachment)
    headers["Cache-Control"] = "private, no-store"
    return FileResponse(
        attachment["path"],
        media_type=str(attachment.get("content_type") or "application/octet-stream"),
        filename=str(attachment.get("safe_filename") or "attachment.bin"),
        headers=headers,
    )


@router.get("/domains", response_model=Envelope[list[DomainOut]], operation_id="listV2Domains")
async def list_domains(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    _require_scope(principal, "domains.read")
    filters = {"grant": _grant_cursor_value(principal)}
    position = _decode_cursor(request, cursor, "domains", filters)
    clauses: list[str] = []
    params: list[Any] = []
    allowed = _allowed_domain_ids(principal)
    if allowed is not None:
        if not allowed:
            clauses.append("0 = 1")
        else:
            placeholders = ", ".join("?" for _ in allowed)
            clauses.append(f"id IN ({placeholders})")
            params.extend(allowed)
    if position is not None:
        try:
            last_id = int(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The domain cursor is invalid.") from exc
        clauses.append("id > ?")
        params.append(last_id)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await _fetch_all(
        request,
        f"""
        SELECT
            id, root_domain_ascii, root_domain_unicode, accept_exact, accept_subdomains,
            public_web_enabled, public_api_enabled, is_active, is_hidden,
            local_part_case_sensitive, plus_addressing_mode, max_message_size_bytes,
            retention_days, dns_status, dns_last_checked_at, dns_details_json,
            notes, created_at, updated_at
        FROM domains
        {where_sql}
        ORDER BY id ASC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(request, "domains", {"id": int(row["id"])}, filters),
    )
    data = [_domain_out(row, request.app.state.runtime) for row in rows[:limit]]
    return _envelope(request, data, page)


@router.get("/domains/{domain_id}", response_model=Envelope[DomainOut], operation_id="getV2Domain")
async def get_domain(domain_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "domains.read")
    if not _domain_allowed(principal, domain_id):
        raise ApiProblem(404, "domain_not_found", "The domain was not found.")
    try:
        domain = await asyncio.to_thread(request.app.state.runtime.domains.get_domain, domain_id)
    except LookupError as exc:
        raise ApiProblem(404, "domain_not_found", "The domain was not found.") from exc
    return _envelope(request, _domain_out(domain, request.app.state.runtime))


@router.post(
    "/domains/{domain_id}/dns-check",
    response_model=Envelope[DomainOut],
    operation_id="runV2DomainDnsCheck",
)
async def run_domain_dns_check(domain_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "domains.write")
    if not _domain_allowed(principal, domain_id):
        raise ApiProblem(404, "domain_not_found", "The domain was not found.")
    try:
        domain = await asyncio.to_thread(request.app.state.runtime.domains.get_domain, domain_id)
    except LookupError as exc:
        raise ApiProblem(404, "domain_not_found", "The domain was not found.") from exc
    if str(domain["root_domain_ascii"]) == "*":
        raise ApiProblem(
            422,
            "dns_check_not_supported",
            "DNS checks are not supported for the system catch-all domain.",
        )

    result = await DnsCheckService().run_dns_check(str(domain["root_domain_ascii"]))
    checked_at = utc_now()
    stored_result = {
        "domain_id": domain_id,
        "root_domain_ascii": str(domain["root_domain_ascii"]),
        "checked_at": checked_at,
        **result,
    }

    try:
        updated = await request.app.state.runtime.domains.record_dns_check(
            domain_id,
            expected_root_domain_ascii=str(domain["root_domain_ascii"]),
            checked_at=checked_at,
            details=stored_result,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "Domain authorization changed before the DNS result was stored.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "domain_not_found", "The domain was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(409, "domain_changed", str(exc)) from exc
    await _audit_mutation(request, principal, "domains.dns_check", "domain", str(domain_id), stored_result)
    return _envelope(request, _domain_out(updated, request.app.state.runtime))


@router.post(
    "/domains",
    response_model=Envelope[DomainOut],
    status_code=status.HTTP_201_CREATED,
    operation_id="createV2Domain",
)
async def create_domain(payload: DomainCreate, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "domains.write")
    _require_global_grant(principal)
    try:
        domain = await request.app.state.runtime.create_domain(
            **payload.model_dump(),
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    except sqlite3.IntegrityError as exc:
        raise ApiProblem(409, "domain_conflict", "A domain with this name already exists.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_domain", str(exc)) from exc
    await _audit_mutation(request, principal, "domains.create", "domain", str(domain["id"]))
    return _envelope(request, _domain_out(domain, request.app.state.runtime))


@router.patch("/domains/{domain_id}", response_model=Envelope[DomainOut], operation_id="updateV2Domain")
async def update_domain(
    domain_id: int,
    payload: DomainUpdate,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "domains.write")
    if not _domain_allowed(principal, domain_id):
        raise ApiProblem(404, "domain_not_found", "The domain was not found.")
    updates = payload.model_dump(exclude_unset=True)
    if "root_domain" in updates:
        _require_global_grant(principal)
    try:
        domain = await request.app.state.runtime.domains.update_domain(
            domain_id,
            updates,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "domain_not_found", "The domain was not found.") from exc
    except sqlite3.IntegrityError as exc:
        raise ApiProblem(409, "domain_conflict", "The domain update conflicts with an existing domain.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_domain", str(exc)) from exc
    await _audit_mutation(request, principal, "domains.update", "domain", str(domain_id))
    return _envelope(request, _domain_out(domain, request.app.state.runtime))


@router.delete("/domains/{domain_id}", response_model=Envelope[DeleteOut], operation_id="deleteV2Domain")
async def delete_domain(domain_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "domains.write")
    if not _domain_allowed(principal, domain_id):
        raise ApiProblem(404, "domain_not_found", "The domain was not found.")
    try:
        await request.app.state.runtime.domains.delete_domain(
            domain_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "domain_not_found", "The domain was not found.") from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise ApiProblem(409, "domain_in_use", "The domain still has dependent resources.") from exc
    await _audit_mutation(request, principal, "domains.delete", "domain", str(domain_id))
    return _envelope(request, DeleteOut(id=domain_id, deleted=True))


@router.get("/mailboxes", response_model=Envelope[list[MailboxOut]], operation_id="listV2Mailboxes")
async def list_mailboxes(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
    domain_id: int | None = Query(default=None, ge=1),
    q: str | None = Query(default=None, max_length=256),
    public_enabled: bool | None = None,
    is_hidden: bool | None = None,
) -> dict[str, Any]:
    _require_scope(principal, "mailboxes.read")
    normalized_query = q.strip() if q and q.strip() else None
    filters = {
        "grant": _grant_cursor_value(principal),
        "domain_id": domain_id,
        "q": normalized_query,
        "public_enabled": public_enabled,
        "is_hidden": is_hidden,
    }
    position = _decode_cursor(request, cursor, "mailboxes", filters)
    clauses: list[str] = []
    params: list[Any] = []
    allowed = _allowed_domain_ids(principal)
    if allowed is not None:
        if not allowed:
            clauses.append("0 = 1")
        else:
            placeholders = ", ".join("?" for _ in allowed)
            clauses.append(f"m.domain_id IN ({placeholders})")
            params.extend(allowed)
    if principal.mailbox_patterns:
        pattern_sql = " OR ".join("m.address_canonical GLOB ?" for _ in principal.mailbox_patterns)
        clauses.append(f"({pattern_sql})")
        params.extend(principal.mailbox_patterns)
    if domain_id is not None:
        clauses.append("m.domain_id = ?")
        params.append(domain_id)
    if normalized_query:
        clauses.append("(m.address_canonical LIKE ? OR m.address_display LIKE ? OR m.notes LIKE ?)")
        search = f"%{normalized_query}%"
        params.extend([search, search, search])
    if public_enabled is not None:
        clauses.append("m.public_enabled = ?")
        params.append(int(public_enabled))
    if is_hidden is not None:
        clauses.append("m.is_hidden = ?")
        params.append(int(is_hidden))
    if position is not None:
        try:
            latest = str(position["latest"])
            last_id = int(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The mailbox cursor is invalid.") from exc
        clauses.append("(COALESCE(m.latest_message_at, '') < ? OR (COALESCE(m.latest_message_at, '') = ? AND m.id < ?))")
        params.extend([latest, latest, last_id])
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await _fetch_all(
        request,
        f"""
        SELECT
            m.id, m.domain_id, d.root_domain_ascii, m.local_part_canonical,
            m.rcpt_domain_ascii, m.address_canonical, m.address_display,
            m.first_seen_at, m.last_seen_at, m.latest_message_at, m.message_count,
            m.public_enabled, m.is_hidden, m.notes
        FROM mailboxes AS m
        JOIN domains AS d ON d.id = m.domain_id
        {where_sql}
        ORDER BY COALESCE(m.latest_message_at, '') DESC, m.id DESC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(
            request,
            "mailboxes",
            {"latest": str(row.get("latest_message_at") or ""), "id": int(row["id"])},
            filters,
        ),
    )
    return _envelope(request, [_mailbox_out(row) for row in rows[:limit]], page)


@router.get("/mailboxes/{mailbox_id}", response_model=Envelope[MailboxOut], operation_id="getV2Mailbox")
async def get_mailbox(mailbox_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "mailboxes.read")
    mailbox = await _authorized_mailbox(request, principal, mailbox_id)
    return _envelope(request, _mailbox_out(mailbox))


@router.patch("/mailboxes/{mailbox_id}", response_model=Envelope[MailboxOut], operation_id="updateV2Mailbox")
async def update_mailbox(
    mailbox_id: int,
    payload: MailboxUpdate,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "mailboxes.write")
    await _authorized_mailbox(request, principal, mailbox_id)
    try:
        mailbox = await request.app.state.runtime.mailboxes.update_mailbox(
            mailbox_id,
            payload.model_dump(exclude_unset=True),
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "Mailbox authorization changed before the mutation committed.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "mailbox_not_found", "The mailbox was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_mailbox", str(exc)) from exc
    await _audit_mutation(request, principal, "mailboxes.update", "mailbox", str(mailbox_id))
    return _envelope(request, _mailbox_out(mailbox))


@router.delete(
    "/mailboxes/{mailbox_id}",
    response_model=Envelope[ResourceDeleteOut],
    operation_id="deleteV2MailboxDeliveries",
)
async def delete_mailbox(mailbox_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "mailboxes.write")
    await _authorized_mailbox(request, principal, mailbox_id)
    try:
        result = await request.app.state.runtime.mailboxes.soft_delete_mailbox_deliveries(
            mailbox_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "Mailbox authorization changed before the mutation committed.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "mailbox_not_found", "The mailbox was not found.") from exc
    affected = int(result.get("deleted") or 0)
    await _audit_mutation(
        request,
        principal,
        "deliveries.bulk_delete",
        "mailbox",
        str(mailbox_id),
        {"affected": affected},
    )
    return _envelope(
        request,
        ResourceDeleteOut(id=str(mailbox_id), deleted=True, affected=affected),
    )


@router.get("/messages", response_model=Envelope[list[MessageSummaryOut]], operation_id="listV2Messages")
async def list_messages(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
    q: str | None = Query(default=None, max_length=256),
    parse_status: Literal["pending", "parsed", "failed"] | None = None,
    mailbox_id: int | None = Query(default=None, ge=1),
) -> dict[str, Any]:
    _require_scope(principal, "messages.read")
    normalized_query = q.strip() if q and q.strip() else None
    filters = {
        "grant": _grant_cursor_value(principal),
        "q": normalized_query,
        "parse_status": parse_status,
        "mailbox_id": mailbox_id,
    }
    position = _decode_cursor(request, cursor, "messages", filters)
    position_values: tuple[str, str] | None = None
    if position is not None:
        try:
            received_at = str(position["received_at"])
            message_id = str(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The message cursor is invalid.") from exc
        position_values = (received_at, message_id)
    query, params = _message_list_query(
        principal,
        normalized_query=normalized_query,
        parse_status=parse_status,
        mailbox_id=mailbox_id,
        position=position_values,
        limit=limit,
    )
    rows = await _fetch_all(request, query, params)
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(
            request,
            "messages",
            {"received_at": str(row["received_at"]), "id": str(row["id"])},
            filters,
        ),
    )
    return _envelope(request, [_message_summary_out(row) for row in rows[:limit]], page)


@router.get("/messages/{message_id}", response_model=Envelope[MessageDetailOut], operation_id="getV2Message")
async def get_message(message_id: str, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "messages.read")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        message = await asyncio.to_thread(request.app.state.runtime.messages.get_admin_message_detail, message_id)
    except LookupError as exc:
        raise ApiProblem(404, "message_not_found", "The message was not found.") from exc
    return _envelope(request, _message_detail_out(message))


@router.get(
    "/messages/{message_id}/raw",
    response_class=FileResponse,
    responses={200: {"content": {"message/rfc822": {}}, "description": "Raw RFC 822 message"}},
    operation_id="downloadV2MessageRaw",
)
async def download_message_raw(message_id: str, request: Request, principal: Principal) -> Response:
    _require_scope(principal, "messages.read")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        raw = await asyncio.to_thread(request.app.state.runtime.messages.get_admin_raw_file, message_id)
    except LookupError as exc:
        raise ApiProblem(404, "message_raw_not_found", "The raw message was not found.") from exc
    return FileResponse(
        raw["path"],
        media_type="message/rfc822",
        filename=f"{message_id}.eml",
        headers={
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get(
    "/messages/{message_id}/attachments/{attachment_id}",
    response_class=FileResponse,
    responses={
        200: {
            "content": {"application/octet-stream": {}},
            "description": "Message attachment",
        }
    },
    operation_id="downloadV2MessageAttachment",
)
async def download_message_attachment(
    message_id: str,
    attachment_id: str,
    request: Request,
    principal: Principal,
) -> Response:
    _require_scope(principal, "messages.read")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        attachment = await asyncio.to_thread(
            request.app.state.runtime.messages.get_admin_attachment_file,
            message_id,
            attachment_id,
        )
    except LookupError as exc:
        raise ApiProblem(404, "attachment_not_found", "The attachment was not found.") from exc
    return FileResponse(
        attachment["path"],
        media_type=str(attachment.get("content_type") or "application/octet-stream"),
        filename=str(attachment.get("safe_filename") or "attachment.bin"),
        headers={
            "Cache-Control": "private, no-store",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.post(
    "/messages/{message_id}/reparse",
    response_model=Envelope[ReparseOut],
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="reparseV2Message",
)
async def reparse_message(message_id: str, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "messages.write")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        await request.app.state.runtime.messages.reparse_message(
            message_id,
            authorization_principal=principal,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise ApiProblem(
            403,
            "message_authorization_changed",
            "Message authorization changed before the mutation committed.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "message_not_found", "The message was not found.") from exc
    await _audit_mutation(request, principal, "messages.reparse", "message", message_id)
    return _envelope(request, ReparseOut(message_id=message_id, queued=True))


@router.delete(
    "/messages/{message_id}",
    response_model=Envelope[ResourceDeleteOut],
    operation_id="deleteV2MessageDeliveries",
)
async def delete_message(message_id: str, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "messages.write")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        result = await request.app.state.runtime.messages.soft_delete_message(
            message_id,
            authorization_principal=principal,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise ApiProblem(
            403,
            "message_authorization_changed",
            "Message authorization changed before the mutation committed.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "message_not_found", "The message was not found.") from exc
    affected = int(result.get("deleted") or 0)
    await _audit_mutation(
        request,
        principal,
        "deliveries.bulk_delete",
        "message",
        message_id,
        {"affected": affected},
    )
    return _envelope(
        request,
        ResourceDeleteOut(id=message_id, deleted=True, affected=affected),
    )


@router.delete(
    "/messages/{message_id}/deliveries/{delivery_id}",
    response_model=Envelope[ResourceDeleteOut],
    operation_id="deleteV2MessageDelivery",
)
async def delete_message_delivery(
    message_id: str,
    delivery_id: str,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "messages.write")
    if not await _message_is_allowed(request, principal, message_id):
        raise ApiProblem(404, "message_not_found", "The message was not found.")
    try:
        delivery_exists = await asyncio.to_thread(
            request.app.state.runtime.messages.message_has_delivery,
            message_id,
            delivery_id,
        )
    except LookupError as exc:
        raise ApiProblem(404, "message_not_found", "The message was not found.") from exc
    if not delivery_exists:
        raise ApiProblem(404, "delivery_not_found", "The delivery was not found.")
    try:
        result = await request.app.state.runtime.messages.soft_delete_delivery(
            delivery_id,
            authorization_principal=principal,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise ApiProblem(
            403,
            "message_authorization_changed",
            "Message authorization changed before the mutation committed.",
        ) from exc
    except LookupError as exc:
        raise ApiProblem(404, "delivery_not_found", "The delivery was not found.") from exc
    affected = int(result.get("deleted") or 0)
    await _audit_mutation(
        request,
        principal,
        "deliveries.delete",
        "delivery",
        delivery_id,
        {"message_id": message_id},
    )
    return _envelope(
        request,
        ResourceDeleteOut(id=delivery_id, deleted=True, affected=affected),
    )


_SMTP_SESSION_COLUMNS = """
    id, remote_ip, remote_port, local_ip, local_port, helo_name, status,
    tls_used, tls_cipher, tls_protocol, connect_at, disconnect_at,
    first_command_at, last_command_at, message_count, rcpt_accepted_count,
    rcpt_rejected_count, bytes_received, last_mail_from, last_rcpt_to_sample,
    result_code, result_message, close_reason
"""


async def _smtp_event_page(
    request: Request,
    session_id: str,
    *,
    limit: int,
    cursor: str | None,
) -> tuple[list[SmtpEventOut], PageInfo]:
    filters = {"session_id": session_id}
    position = _decode_cursor(request, cursor, "smtp-events", filters)
    clauses = ["session_id = ?"]
    params: list[Any] = [session_id]
    if position is not None:
        try:
            seq = int(position["seq"])
            event_id = int(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The SMTP event cursor is invalid.") from exc
        clauses.append("(seq > ? OR (seq = ? AND id > ?))")
        params.extend([seq, seq, event_id])
    rows = await _fetch_all(
        request,
        f"""
        SELECT id, seq, event_type, ts, payload_json
        FROM smtp_events
        WHERE {' AND '.join(clauses)}
        ORDER BY seq ASC, id ASC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(
            request,
            "smtp-events",
            {"seq": int(row["seq"]), "id": int(row["id"])},
            filters,
        ),
    )
    return [_smtp_event_out(row) for row in rows[:limit]], page


@router.get(
    "/smtp-sessions",
    response_model=Envelope[list[SmtpSessionOut]],
    operation_id="listV2SmtpSessions",
)
async def list_smtp_sessions(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
    session_status: Literal["open", "closed", "error"] | None = Query(default=None, alias="status"),
    remote_ip: str | None = Query(default=None, min_length=1, max_length=128),
) -> dict[str, Any]:
    _require_scope(principal, "smtp.read")
    _require_global_grant(principal)
    filters = {"status": session_status, "remote_ip": remote_ip}
    position = _decode_cursor(request, cursor, "smtp-sessions", filters)
    clauses: list[str] = []
    params: list[Any] = []
    if session_status is not None:
        clauses.append("status = ?")
        params.append(session_status)
    if remote_ip is not None:
        clauses.append("remote_ip = ?")
        params.append(remote_ip)
    if position is not None:
        try:
            connect_at = str(position["connect_at"])
            session_id = str(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The SMTP session cursor is invalid.") from exc
        if not connect_at or not session_id:
            raise ApiProblem(400, "invalid_cursor", "The SMTP session cursor is invalid.")
        clauses.append("(connect_at < ? OR (connect_at = ? AND id < ?))")
        params.extend([connect_at, connect_at, session_id])
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await _fetch_all(
        request,
        f"""
        SELECT {_SMTP_SESSION_COLUMNS}
        FROM smtp_sessions
        {where_sql}
        ORDER BY connect_at DESC, id DESC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(
            request,
            "smtp-sessions",
            {"connect_at": str(row["connect_at"]), "id": str(row["id"])},
            filters,
        ),
    )
    return _envelope(request, [_smtp_session_out(row) for row in rows[:limit]], page)


@router.get(
    "/smtp-sessions/{session_id}",
    response_model=Envelope[SmtpSessionDetailOut],
    operation_id="getV2SmtpSession",
)
async def get_smtp_session(
    session_id: str,
    request: Request,
    principal: Principal,
    event_limit: Annotated[int, Query(ge=1, le=500)] = 100,
    event_cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    _require_scope(principal, "smtp.read")
    _require_global_grant(principal)
    session = await _fetch_one(
        request,
        f"SELECT {_SMTP_SESSION_COLUMNS} FROM smtp_sessions WHERE id = ?",
        (session_id,),
    )
    if session is None:
        raise ApiProblem(404, "smtp_session_not_found", "The SMTP session was not found.")
    events, events_page = await _smtp_event_page(
        request,
        session_id,
        limit=event_limit,
        cursor=event_cursor,
    )
    data = SmtpSessionDetailOut(
        **_smtp_session_out(session).model_dump(),
        events=events,
        events_page=events_page,
    )
    return _envelope(request, data)


@router.get(
    "/smtp-sessions/{session_id}/events",
    response_model=Envelope[list[SmtpEventOut]],
    operation_id="listV2SmtpSessionEvents",
)
async def list_smtp_session_events(
    session_id: str,
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    _require_scope(principal, "smtp.read")
    _require_global_grant(principal)
    exists = await _fetch_one(request, "SELECT id FROM smtp_sessions WHERE id = ?", (session_id,))
    if exists is None:
        raise ApiProblem(404, "smtp_session_not_found", "The SMTP session was not found.")
    events, page = await _smtp_event_page(request, session_id, limit=limit, cursor=cursor)
    return _envelope(request, events, page)


@router.get("/audit-events", response_model=Envelope[list[AuditEventOut]], operation_id="listV2AuditEvents")
async def list_audit_events(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
    actor: str | None = Query(default=None, max_length=256),
    action: str | None = Query(default=None, max_length=256),
    resource: str | None = Query(default=None, max_length=256),
) -> dict[str, Any]:
    _require_scope(principal, "audit.read")
    _require_global_grant(principal)
    filters = {"actor": actor, "action": action, "resource": resource}
    position = _decode_cursor(request, cursor, "audit-events", filters)
    clauses: list[str] = []
    params: list[Any] = []
    if actor:
        clauses.append("(actor_ref = ? OR actor_type = ?)")
        params.extend([actor, actor])
    if action:
        clauses.append("action = ?")
        params.append(action)
    if resource:
        clauses.append("(resource_type = ? OR resource_ref = ?)")
        params.extend([resource, resource])
    if position is not None:
        try:
            created_at = str(position["created_at"])
            event_id = int(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The audit cursor is invalid.") from exc
        clauses.append("(created_at < ? OR (created_at = ? AND id < ?))")
        params.extend([created_at, created_at, event_id])
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await _fetch_all(
        request,
        f"""
        SELECT id, actor_type, actor_ref, action, resource_type, resource_ref,
               status, ip, user_agent, details_json, created_at
        FROM audit_logs
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(
            request,
            "audit-events",
            {"created_at": str(row["created_at"]), "id": int(row["id"])},
            filters,
        ),
    )
    data = [
        AuditEventOut(
            id=int(row["id"]),
            actor_type=str(row["actor_type"]),
            actor_ref=row.get("actor_ref"),
            action=str(row["action"]),
            resource_type=str(row["resource_type"]),
            resource_ref=row.get("resource_ref"),
            status=str(row["status"]),
            ip=row.get("ip"),
            user_agent=row.get("user_agent"),
            details=_decode_json(row.get("details_json")),
            created_at=str(row["created_at"]),
        )
        for row in rows[:limit]
    ]
    return _envelope(request, data, page)


@router.get(
    "/dashboard/status",
    response_model=Envelope[DashboardStatusOut],
    operation_id="getV2DashboardStatus",
)
async def get_dashboard_status(request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "system.read")
    _require_global_grant(principal)
    snapshot = await get_dashboard_service(request.app).snapshot()
    return _envelope(request, _dashboard_status_out(snapshot))


@router.post(
    "/maintenance/cleanup",
    response_model=Envelope[MaintenanceResultOut],
    operation_id="runV2MaintenanceCleanup",
)
async def run_maintenance_cleanup(request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "system.write")
    _require_global_grant(principal)
    try:
        result = await request.app.state.runtime.cleanup_expired_messages(
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    await _audit_mutation(
        request,
        principal,
        "maintenance.cleanup",
        "system",
        None,
        result,
    )
    return _envelope(
        request,
        MaintenanceResultOut(operation="cleanup_expired_messages", result=result),
    )


@router.post(
    "/maintenance/clear-all",
    response_model=Envelope[MaintenanceResultOut],
    operation_id="runV2MaintenanceClearAll",
)
async def run_maintenance_clear_all(request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "system.write")
    _require_global_grant(principal)
    try:
        result = await request.app.state.runtime.clear_all_mail(
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    await _audit_mutation(
        request,
        principal,
        "mail.clear_all",
        "system",
        None,
        result,
    )
    return _envelope(
        request,
        MaintenanceResultOut(operation="clear_all_mail", result=result),
    )


@router.get("/system/settings", response_model=Envelope[SettingsOut], operation_id="getV2Settings")
async def get_settings(request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "system.read")
    _require_global_grant(principal)
    settings_payload = await asyncio.to_thread(request.app.state.runtime.system_settings.get_settings)
    return _envelope(request, SettingsOut.model_validate(settings_payload))


@router.patch("/system/settings", response_model=Envelope[SettingsOut], operation_id="updateV2Settings")
async def update_settings(
    payload: SettingsUpdate,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "system.write")
    _require_global_grant(principal)
    try:
        settings_payload = await request.app.state.runtime.system_settings.update_settings(
            payload.model_dump(exclude_unset=True),
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "authorization_changed",
            "The acting credential changed during authorization.",
        ) from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_settings", str(exc)) from exc
    await _audit_mutation(request, principal, "settings.update", "system_settings", None)
    return _envelope(request, SettingsOut.model_validate(settings_payload))


@router.get("/api-keys", response_model=Envelope[list[ApiKeyOut]], operation_id="listV2ApiKeys")
async def list_api_keys(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    _require_scope(principal, "api_keys.read")
    _require_global_grant(principal)
    filters = {
        "grant": _grant_cursor_value(principal),
        "scopes": sorted(_effective_scopes(principal)),
        "kind": principal.kind,
        "rate_limit_per_min": principal.rate_limit_per_min,
        "allowed_ip_cidrs": list(principal.allowed_ip_cidrs),
        "expires_at": principal.expires_at,
        "allow_header": principal.allow_header,
        "allow_query": principal.allow_query,
    }
    raw_position = _decode_cursor(request, cursor, "api-keys", filters)
    position: tuple[str, int] | None = None
    if raw_position is not None:
        try:
            position = (str(raw_position["created_at"]), int(raw_position["id"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The API key cursor is invalid.") from exc
    scan_page = await asyncio.to_thread(
        _list_api_keys_keyset_sync,
        request.app.state.settings.database_path,
        principal,
        position,
        limit,
    )
    next_cursor = None
    if scan_page.has_more and scan_page.continuation_position is not None:
        next_cursor = _encode_cursor(
            request,
            "api-keys",
            {
                "created_at": scan_page.continuation_position[0],
                "id": scan_page.continuation_position[1],
            },
            filters,
        )
    page = PageInfo(
        limit=limit,
        has_more=scan_page.has_more,
        next_cursor=next_cursor,
    )
    return _envelope(request, [_api_key_out(row) for row in scan_page.items], page)


@router.post(
    "/api-keys",
    response_model=Envelope[ApiKeySecretOut],
    status_code=status.HTTP_201_CREATED,
    responses={
        201: {
            "description": "API key created; the secret is returned only once",
            "headers": {"Cache-Control": {"schema": {"type": "string"}}},
        }
    },
    operation_id="createV2ApiKey",
)
async def create_api_key(
    payload: ApiKeyCreate,
    request: Request,
    response: Response,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "api_keys.write")
    _require_global_grant(principal)
    requested = payload.model_dump()
    if not _api_key_within_principal(principal, requested):
        raise ApiProblem(
            403,
            "api_key_delegation_denied",
            "The requested API key would exceed the caller's permissions.",
        )
    try:
        created = await request.app.state.runtime.api_keys.create_key(
            **requested,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(
            403,
            "api_key_delegation_denied",
            "The acting credential or requested policy changed during authorization.",
        ) from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise ApiProblem(422, "invalid_api_key", str(exc)) from exc
    await _audit_mutation(request, principal, "api_keys.create", "api_key", str(created["id"]))
    response.headers["Cache-Control"] = "no-store"
    return _envelope(
        request,
        ApiKeySecretOut(
            api_key=_api_key_out(created),
            secret=str(created["plain_text"]),
        ),
    )


@router.get(
    "/api-keys/{api_key_id}",
    response_model=Envelope[ApiKeyOut],
    operation_id="getV2ApiKey",
)
async def get_api_key(api_key_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "api_keys.read")
    _require_global_grant(principal)
    target = await _authorized_api_key(request, principal, api_key_id)
    return _envelope(request, _api_key_out(target))


@router.patch(
    "/api-keys/{api_key_id}",
    response_model=Envelope[ApiKeyOut],
    operation_id="updateV2ApiKey",
)
async def update_api_key(
    api_key_id: int,
    payload: ApiKeyUpdate,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "api_keys.write")
    _require_global_grant(principal)
    target = await _authorized_api_key(request, principal, api_key_id)
    updates = payload.model_dump(exclude_unset=True)
    requested = {**target, **updates}
    if not _api_key_within_principal(principal, requested):
        raise ApiProblem(
            403,
            "api_key_delegation_denied",
            "The API key update would exceed the caller's permissions.",
        )
    try:
        updated = await request.app.state.runtime.api_keys.update_key(
            api_key_id,
            **updates,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except LookupError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_api_key", str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise ApiProblem(409, "api_key_conflict", "The API key update conflicts with existing state.") from exc
    await _audit_mutation(request, principal, "api_keys.update", "api_key", str(api_key_id))
    return _envelope(request, _api_key_out(updated))


@router.post(
    "/api-keys/{api_key_id}/rotate",
    response_model=Envelope[ApiKeySecretOut],
    responses={
        200: {
            "description": "API key rotated; the replacement secret is returned only once",
            "headers": {"Cache-Control": {"schema": {"type": "string"}}},
        }
    },
    operation_id="rotateV2ApiKey",
)
async def rotate_api_key(
    api_key_id: int,
    request: Request,
    response: Response,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "api_keys.write")
    _require_global_grant(principal)
    await _authorized_api_key(request, principal, api_key_id)
    try:
        rotated = await request.app.state.runtime.api_keys.rotate_key(
            api_key_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except LookupError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(409, "api_key_not_rotatable", str(exc)) from exc
    await _audit_mutation(request, principal, "api_keys.rotate", "api_key", str(api_key_id))
    response.headers["Cache-Control"] = "no-store"
    return _envelope(
        request,
        ApiKeySecretOut(
            api_key=_api_key_out(rotated),
            secret=str(rotated["plain_text"]),
        ),
    )


@router.post(
    "/api-keys/{api_key_id}/revoke",
    response_model=Envelope[ApiKeyActionOut],
    operation_id="revokeV2ApiKey",
)
async def revoke_api_key(api_key_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "api_keys.write")
    _require_global_grant(principal)
    await _authorized_api_key(request, principal, api_key_id)
    try:
        revoked = await request.app.state.runtime.api_keys.revoke_key(
            api_key_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except LookupError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    await _audit_mutation(request, principal, "api_keys.revoke", "api_key", str(api_key_id))
    return _envelope(request, ApiKeyActionOut.model_validate(revoked))


@router.delete(
    "/api-keys/{api_key_id}",
    response_model=Envelope[DeleteOut],
    operation_id="deleteV2ApiKey",
)
async def delete_api_key(api_key_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "api_keys.write")
    _require_global_grant(principal)
    await _authorized_api_key(request, principal, api_key_id)
    try:
        deleted = await request.app.state.runtime.api_keys.delete_key(
            api_key_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except LookupError as exc:
        raise ApiProblem(404, "api_key_not_found", "The API key was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(409, "api_key_not_revoked", str(exc)) from exc
    await _audit_mutation(request, principal, "api_keys.delete", "api_key", str(api_key_id))
    return _envelope(request, DeleteOut.model_validate(deleted))


@router.get("/admins", response_model=Envelope[list[AdminOut]], operation_id="listV2Admins")
async def list_admins(
    request: Request,
    principal: Principal,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    cursor: Annotated[str | None, Query(max_length=2048)] = None,
) -> dict[str, Any]:
    _require_scope(principal, "admins.read")
    _require_global_grant(principal)
    filters: dict[str, Any] = {}
    position = _decode_cursor(request, cursor, "admins", filters)
    clauses: list[str] = []
    params: list[Any] = []
    if position is not None:
        try:
            admin_id = int(position["id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ApiProblem(400, "invalid_cursor", "The admin cursor is invalid.") from exc
        clauses.append("id > ?")
        params.append(admin_id)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = await _fetch_all(
        request,
        f"""
        SELECT id, username, display_name, role, is_active, must_change_password,
               created_at, updated_at, last_login_at, last_login_ip
        FROM admins
        {where_sql}
        ORDER BY id ASC
        LIMIT ?
        """,
        (*params, limit + 1),
    )
    page = _page(
        limit,
        rows,
        lambda row: _encode_cursor(request, "admins", {"id": int(row["id"])}, filters),
    )
    return _envelope(request, [_admin_out(row) for row in rows[:limit]], page)


@router.post(
    "/admins",
    response_model=Envelope[AdminOut],
    status_code=status.HTTP_201_CREATED,
    operation_id="createV2Admin",
)
async def create_admin(payload: AdminCreate, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "admins.write")
    _require_scope(principal, "admins.credentials.write")
    _require_global_grant(principal)
    try:
        admin = await request.app.state.runtime.auth.create_admin(
            **payload.model_dump(),
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(403, "admin_delegation_forbidden", str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise ApiProblem(409, "admin_conflict", "An admin with this username already exists.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_admin", str(exc)) from exc
    await _audit_mutation(request, principal, "admins.create", "admin", str(admin["id"]))
    return _envelope(request, _admin_out(admin))


@router.get("/admins/{admin_id}", response_model=Envelope[AdminOut], operation_id="getV2Admin")
async def get_admin(admin_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "admins.read")
    _require_global_grant(principal)
    try:
        admin = await asyncio.to_thread(request.app.state.runtime.auth.get_admin, admin_id)
    except LookupError as exc:
        raise ApiProblem(404, "admin_not_found", "The admin was not found.") from exc
    return _envelope(request, _admin_out(admin))


@router.patch("/admins/{admin_id}", response_model=Envelope[AdminOut], operation_id="updateV2Admin")
async def update_admin(
    admin_id: int,
    payload: AdminUpdate,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "admins.write")
    _require_global_grant(principal)
    try:
        admin = await request.app.state.runtime.auth.update_admin(
            admin_id,
            **payload.model_dump(exclude_unset=True),
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(403, "admin_delegation_forbidden", str(exc)) from exc
    except LookupError as exc:
        raise ApiProblem(404, "admin_not_found", "The admin was not found.") from exc
    except sqlite3.IntegrityError as exc:
        raise ApiProblem(409, "admin_conflict", "The admin update conflicts with existing state.") from exc
    except ValueError as exc:
        raise ApiProblem(409, "admin_invariant_violation", str(exc)) from exc
    await _audit_mutation(request, principal, "admins.update", "admin", str(admin_id))
    return _envelope(request, _admin_out(admin))


@router.delete("/admins/{admin_id}", response_model=Envelope[DeleteOut], operation_id="deleteV2Admin")
async def delete_admin(admin_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "admins.write")
    _require_global_grant(principal)
    try:
        await request.app.state.runtime.auth.delete_admin(
            admin_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(403, "admin_delegation_forbidden", str(exc)) from exc
    except LookupError as exc:
        raise ApiProblem(404, "admin_not_found", "The admin was not found.") from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise ApiProblem(409, "admin_invariant_violation", str(exc)) from exc
    await _audit_mutation(request, principal, "admins.delete", "admin", str(admin_id))
    return _envelope(request, DeleteOut(id=admin_id, deleted=True))


@router.post(
    "/admins/{admin_id}/password",
    response_model=Envelope[AdminOut],
    operation_id="resetV2AdminPassword",
)
async def reset_admin_password(
    admin_id: int,
    payload: AdminPasswordReset,
    request: Request,
    principal: Principal,
) -> dict[str, Any]:
    _require_scope(principal, "admins.credentials.write")
    _require_global_grant(principal)
    try:
        admin = await request.app.state.runtime.auth.reset_admin_password(
            admin_id,
            payload.password,
            must_change_password=payload.must_change_password,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(403, "admin_delegation_forbidden", str(exc)) from exc
    except LookupError as exc:
        raise ApiProblem(404, "admin_not_found", "The admin was not found.") from exc
    except ValueError as exc:
        raise ApiProblem(422, "invalid_password", str(exc)) from exc
    await _audit_mutation(request, principal, "admins.password_reset", "admin", str(admin_id))
    return _envelope(request, _admin_out(admin))


@router.post(
    "/admins/{admin_id}/sessions/revoke",
    response_model=Envelope[SessionRevokeOut],
    operation_id="revokeV2AdminSessions",
)
async def revoke_admin_sessions(admin_id: int, request: Request, principal: Principal) -> dict[str, Any]:
    _require_scope(principal, "admins.sessions.write")
    _require_global_grant(principal)
    try:
        revoked = await request.app.state.runtime.auth.revoke_admin_sessions(
            admin_id,
            authorization_principal=principal,
        )
    except ApiKeyAuthorizationError as exc:
        raise ApiProblem(403, "admin_delegation_forbidden", str(exc)) from exc
    except LookupError as exc:
        raise ApiProblem(404, "admin_not_found", "The admin was not found.") from exc
    await _audit_mutation(request, principal, "admins.sessions_revoke", "admin", str(admin_id))
    return _envelope(request, SessionRevokeOut(admin_id=admin_id, revoked_sessions=revoked))


__all__ = ["router"]
