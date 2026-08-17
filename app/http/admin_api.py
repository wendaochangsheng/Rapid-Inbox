from __future__ import annotations

import asyncio
import hmac
import ipaddress
import json
import sqlite3
from typing import Any
from urllib.parse import urlsplit

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response, StreamingResponse

from app.auth.api_keys import ApiKeyAuthorizationError
from app.auth.permissions import (
    PermissionContext,
    PermissionDenied,
    delegated_api_key_policy_is_within_principal,
    mailbox_pattern_matches,
    role_permission_context,
)
from app.db.connection import connect_database
from app.http.pagination import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, build_pagination_context
from app.http.live import (
    iter_smtp_live_events,
    smtp_live_snapshot,
    smtp_sessions_page,
)
from app.http.sse import (
    stream_smtp_live_events,
)
from app.ingest.storage import utc_now
from app.services.dashboard import get_dashboard_service
from app.services.dns_check import DnsCheckService


router = APIRouter()


LEGACY_API_MAX_PAGE_SIZE = 200
LEGACY_API_MAX_EVENT_PAGE_SIZE = 1000
LEGACY_API_MAX_BULK_DELIVERY_IDS = 1000


def _client_ip(request: Request) -> str | None:
    return request.client.host if request.client is not None else None


async def _current_admin_session(request: Request | WebSocket) -> dict[str, Any] | None:
    cookie_name = request.app.state.settings.session_cookie_name
    token = request.cookies.get(cookie_name)
    if not token:
        return None

    try:
        return await request.app.state.runtime.auth.get_session_admin(token, ip=_client_ip(request))
    except LookupError:
        return None


def _session_permission_context(admin: dict[str, Any]) -> PermissionContext:
    return role_permission_context(admin)


def _render_template(request: Request, template_name: str, context: dict[str, Any], *, status_code: int = 200) -> Response:
    response = request.app.state.templates.TemplateResponse(request, template_name, context)
    response.status_code = status_code
    return response


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _coerce_text_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw_values = value.replace("\n", ",").split(",")
    elif isinstance(value, (list, tuple)):
        raw_values = [str(item) for item in value]
    else:
        raw_values = [str(value)]
    return [item.strip() for item in raw_values if item and item.strip()]


def _coerce_int_list(value: Any) -> list[int]:
    return [int(item) for item in _coerce_text_list(value)]


def _coerce_non_negative_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"invalid {field_name}")
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"invalid {field_name}")
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid {field_name}") from exc
    if normalized < 0:
        raise ValueError(f"invalid {field_name}")
    return normalized


def _nullable_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


async def require_admin_key(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> PermissionContext:
    if x_api_key is None:
        raise HTTPException(status_code=401, detail="invalid admin api key")

    settings = request.app.state.settings
    if settings.legacy_admin_token_enabled and hmac.compare_digest(x_api_key, settings.admin_token):
        return PermissionContext(
            scopes=(),
            domain_ids=(),
            mailbox_patterns=(),
            api_key_id=None,
            public_id="legacy-admin-token",
            name="legacy-admin-token",
            kind="admin",
            legacy_credential=True,
        )

    try:
        context = await asyncio.to_thread(
            request.app.state.runtime.api_keys.authenticate_plain_text,
            x_api_key,
        )
    except LookupError as exc:
        raise HTTPException(status_code=401, detail="invalid admin api key") from exc

    if context.kind not in {"admin", "service"}:
        raise HTTPException(status_code=403, detail="invalid admin api key")

    return context


require_admin_api_key = require_admin_key


def require_admin_scope(admin: PermissionContext, required_scope: str) -> None:
    if admin.legacy_credential:
        return
    if required_scope not in admin.scopes:
        if required_scope.endswith(".read"):
            write_scope = f"{required_scope[:-5]}.write"
            if write_scope in admin.scopes:
                return
        raise HTTPException(status_code=403, detail=required_scope)


def _allowed_domain_ids(admin: PermissionContext) -> tuple[int, ...] | None:
    if admin.legacy_credential or admin.domain_grant_mode == "all":
        return None
    if admin.domain_grant_mode == "selected":
        return tuple(int(item) for item in admin.domain_ids)
    return ()


def _require_global_grant(admin: PermissionContext) -> None:
    if _allowed_domain_ids(admin) is not None:
        raise HTTPException(status_code=403, detail="operation requires an all-domain grant")


def _require_domain_grant(admin: PermissionContext, domain_id: int) -> None:
    allowed = _allowed_domain_ids(admin)
    if allowed is not None and int(domain_id) not in allowed:
        raise HTTPException(status_code=403, detail="resource outside domain grant")


async def _require_mailbox_grant(
    request: Request,
    admin: PermissionContext,
    mailbox_id: int,
) -> dict[str, Any]:
    try:
        mailbox = await asyncio.to_thread(
            request.app.state.runtime.mailboxes.get_mailbox,
            mailbox_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _require_domain_grant(admin, int(mailbox["domain_id"]))
    if admin.mailbox_patterns and not any(
        mailbox_pattern_matches(str(mailbox["address_canonical"]), pattern)
        for pattern in admin.mailbox_patterns
    ):
        raise HTTPException(status_code=403, detail="resource outside mailbox grant")
    return mailbox


def _message_grant_rows(database_path, message_id: str) -> list[dict[str, Any]]:
    with connect_database(database_path) as connection:
        rows = connection.execute(
            """
            SELECT DISTINCT mb.domain_id, mb.address_canonical
            FROM message_deliveries AS d
            JOIN mailboxes AS mb ON mb.id = d.mailbox_id
            WHERE d.message_id = ?
            """,
            (message_id,),
        ).fetchall()
    return [dict(row) for row in rows]


async def _require_message_grant(
    request: Request,
    admin: PermissionContext,
    message_id: str,
) -> None:
    allowed = _allowed_domain_ids(admin)
    if allowed is None and not admin.mailbox_patterns:
        return
    rows = await asyncio.to_thread(
        _message_grant_rows,
        request.app.state.settings.database_path,
        message_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail="message not found")
    resource_domains = {int(row["domain_id"]) for row in rows}
    if allowed is not None and not resource_domains.issubset(set(allowed)):
        raise HTTPException(status_code=403, detail="resource outside domain grant")
    if admin.mailbox_patterns and any(
        not any(
            mailbox_pattern_matches(str(row["address_canonical"]), pattern)
            for pattern in admin.mailbox_patterns
        )
        for row in rows
    ):
        raise HTTPException(status_code=403, detail="resource outside mailbox grant")


def _delivery_message_id(database_path, delivery_id: str) -> str | None:
    with connect_database(database_path) as connection:
        row = connection.execute(
            "SELECT message_id FROM message_deliveries WHERE id = ?",
            (delivery_id,),
        ).fetchone()
    return None if row is None else str(row["message_id"])


async def _require_delivery_grant(
    request: Request,
    admin: PermissionContext,
    delivery_id: str,
) -> str:
    message_id = await asyncio.to_thread(
        _delivery_message_id,
        request.app.state.settings.database_path,
        delivery_id,
    )
    if message_id is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    await _require_message_grant(request, admin, message_id)
    return message_id


def _require_key_management_grant(admin: PermissionContext, target: dict[str, Any]) -> None:
    if admin.legacy_credential:
        return
    if not delegated_api_key_policy_is_within_principal(admin, target):
        raise HTTPException(status_code=403, detail="target key exceeds caller operational policy")
    effective_scopes = set(admin.scopes)
    effective_scopes.update(
        f"{scope[:-6]}.read" for scope in admin.scopes if scope.endswith(".write")
    )
    if not set(target.get("scopes") or ()).issubset(effective_scopes):
        raise HTTPException(status_code=403, detail="target key exceeds caller scopes")
    target_mode = str(target.get("domain_grant_mode") or "none")
    if target_mode == "all" and admin.domain_grant_mode != "all":
        raise HTTPException(status_code=403, detail="target key exceeds caller domain grant")
    if target_mode == "selected":
        allowed = _allowed_domain_ids(admin)
        if allowed is not None and not set(target.get("domain_ids") or ()).issubset(set(allowed)):
            raise HTTPException(status_code=403, detail="target key exceeds caller domain grant")

    parent_patterns = set(admin.mailbox_patterns)
    target_patterns = set(str(item) for item in target.get("mailbox_patterns") or ())
    if parent_patterns:
        if not target_patterns:
            raise HTTPException(status_code=403, detail="target key exceeds caller mailbox grant")
        for target_pattern in target_patterns:
            if target_pattern in parent_patterns:
                continue
            # Glob containment is not generally decidable by comparing two
            # patterns. As in API v2, only an exact child pattern or a literal
            # address that demonstrably matches a parent glob is delegated.
            if "*" in target_pattern or "?" in target_pattern:
                raise HTTPException(status_code=403, detail="target key exceeds caller mailbox grant")
            if not any(
                mailbox_pattern_matches(target_pattern, parent_pattern)
                for parent_pattern in parent_patterns
            ):
                raise HTTPException(status_code=403, detail="target key exceeds caller mailbox grant")


def _key_is_within_grant(admin: PermissionContext, target: dict[str, Any]) -> bool:
    try:
        _require_key_management_grant(admin, target)
    except HTTPException:
        return False
    return True


def _key_mutation_principal(admin: PermissionContext) -> PermissionContext | None:
    return None if admin.legacy_credential else admin


def _list_mailboxes_page(service, **filters: Any) -> dict[str, Any]:
    result = service.list_mailboxes(**filters)
    count_filters = dict(filters)
    count_filters.pop("limit", None)
    count_filters.pop("offset", None)
    result["total_count"] = service.count_mailboxes(**count_filters)
    return result


def _mailbox_detail(
    service,
    admin: PermissionContext,
    mailbox_id: int,
    *,
    limit: int,
    offset: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    mailbox = service.get_mailbox(mailbox_id)
    _require_domain_grant(admin, int(mailbox["domain_id"]))
    if admin.mailbox_patterns and not any(
        mailbox_pattern_matches(str(mailbox["address_canonical"]), pattern)
        for pattern in admin.mailbox_patterns
    ):
        raise HTTPException(status_code=403, detail="resource outside mailbox grant")
    deliveries = service.list_mailbox_deliveries(mailbox_id, limit=limit, offset=offset)
    return mailbox, deliveries


def _domains_page(
    database_path,
    *,
    allowed_domain_ids: tuple[int, ...] | None,
    limit: int,
    offset: int,
) -> dict[str, Any]:
    clauses: list[str] = []
    params: list[Any] = []
    if allowed_domain_ids is not None:
        if not allowed_domain_ids:
            clauses.append("0 = 1")
        else:
            placeholders = ", ".join("?" for _ in allowed_domain_ids)
            clauses.append(f"id IN ({placeholders})")
            params.extend(allowed_domain_ids)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect_database(database_path) as connection:
        rows = connection.execute(
            f"""
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
            {where_sql}
            ORDER BY root_domain_ascii ASC, id ASC
            LIMIT ? OFFSET ?
            """,
            (*params, limit, offset),
        ).fetchall()
        total = connection.execute(
            f"SELECT COUNT(*) AS count FROM domains {where_sql}",
            tuple(params),
        ).fetchone()

    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["is_catch_all"] = item.get("root_domain_ascii") == "*"
        for field in (
            "accept_exact",
            "accept_subdomains",
            "public_web_enabled",
            "public_api_enabled",
            "is_active",
        ):
            item[field] = bool(item[field])
        items.append(item)
    return {
        "items": items,
        "total_count": 0 if total is None else int(total["count"]),
        "limit": limit,
        "offset": offset,
    }


def _smtp_session_detail(
    database_path,
    session_id: str,
    *,
    event_limit: int,
    event_offset: int,
) -> dict[str, Any] | None:
    with connect_database(database_path) as connection:
        row = connection.execute(
            "SELECT * FROM smtp_sessions WHERE id = ?",
            (session_id,),
        ).fetchone()
        if row is None:
            return None
        events = connection.execute(
            """
            SELECT id, seq, event_type, ts, payload_json
            FROM smtp_events
            WHERE session_id = ?
            ORDER BY seq ASC
            LIMIT ? OFFSET ?
            """,
            (session_id, event_limit, event_offset),
        ).fetchall()
        event_total = connection.execute(
            "SELECT COUNT(*) AS count FROM smtp_events WHERE session_id = ?",
            (session_id,),
        ).fetchone()
    result = dict(row)
    result["tls_used"] = bool(result["tls_used"])
    result["events"] = [dict(event) for event in events]
    result["events_total_count"] = 0 if event_total is None else int(event_total["count"])
    result["events_limit"] = event_limit
    result["events_offset"] = event_offset
    return result


async def _record_admin_key_usage(request: Request | WebSocket, admin: PermissionContext) -> None:
    if admin.api_key_id is None:
        return
    request_ip = request.client.host if request.client is not None else None
    await request.app.state.runtime.api_keys.record_usage(admin, ip=request_ip)


def _audit_actor_ref(admin: PermissionContext) -> str:
    if admin.api_key_id is not None:
        return str(admin.api_key_id)
    return admin.public_id or "legacy-admin-token"


async def _write_audit_best_effort(
    request: Request,
    admin: PermissionContext,
    action: str,
    resource_type: str,
    resource_ref: str | None,
    status_value: str,
) -> None:
    try:
        await request.app.state.runtime.audit.log(
            "api_key",
            _audit_actor_ref(admin),
            action,
            resource_type,
            resource_ref,
            status_value,
        )
    except Exception:
        # Mutation already completed; audit writes are best-effort here.
        return


async def require_admin_live_access(
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> PermissionContext:
    if x_api_key is not None:
        admin = await require_admin_key(request, x_api_key)
        require_admin_scope(admin, "live.read")
        _require_global_grant(admin)
        await _record_admin_key_usage(request, admin)
        return admin

    admin_session = await _current_admin_session(request)
    if admin_session is None:
        raise HTTPException(status_code=404, detail="live page not found")
    if admin_session.get("must_change_password"):
        raise HTTPException(status_code=403, detail="password change required")
    return _session_permission_context(admin_session)


def _normalized_admin_websocket_origin(value: str) -> tuple[str, str, int] | None:
    try:
        parsed = urlsplit(value)
        scheme = parsed.scheme.lower()
        hostname = parsed.hostname
        port = parsed.port
    except (TypeError, ValueError):
        return None
    if scheme not in {"http", "https"} or not hostname:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        return None
    if port is None:
        port = 443 if scheme == "https" else 80
    return scheme, hostname.lower(), port


def _admin_websocket_origin_is_allowed(websocket: WebSocket) -> bool:
    origins = websocket.headers.getlist("origin")
    if len(origins) != 1:
        return False
    scheme = "https" if websocket.url.scheme.lower() == "wss" else "http"
    hostname = websocket.url.hostname
    if not hostname:
        return False
    port = websocket.url.port
    if port is None:
        port = 443 if scheme == "https" else 80
    return _normalized_admin_websocket_origin(origins[0]) == (
        scheme,
        hostname.lower(),
        port,
    )


def _admin_websocket_is_remote(websocket: WebSocket) -> bool:
    if websocket.client is None:
        return True
    try:
        return not ipaddress.ip_address(websocket.client.host).is_loopback
    except ValueError:
        return True


async def _authorize_admin_websocket(websocket: WebSocket) -> PermissionContext | None:
    if "api_key" in websocket.query_params:
        return None
    settings = websocket.app.state.settings
    if not settings.externally_bound() and _admin_websocket_is_remote(websocket):
        findings = await asyncio.to_thread(
            websocket.app.state.runtime.external_request_security_findings
        )
        if findings:
            return None

    x_api_keys = websocket.headers.getlist("x-api-key")
    if len(x_api_keys) > 1:
        return None
    if x_api_keys:
        x_api_key = x_api_keys[0]
        try:
            admin = await require_admin_key(websocket, x_api_key)  # type: ignore[arg-type]
            require_admin_scope(admin, "live.read")
            _require_global_grant(admin)
            await _record_admin_key_usage(websocket, admin)
        except (HTTPException, PermissionDenied):
            return None
        return admin

    if not _admin_websocket_origin_is_allowed(websocket):
        return None
    admin_session = await _current_admin_session(websocket)
    if admin_session is None or admin_session.get("must_change_password"):
        return None
    try:
        admin = _session_permission_context(admin_session)
        require_admin_scope(admin, "live.read")
        _require_global_grant(admin)
    except (HTTPException, PermissionDenied):
        return None
    return admin


@router.get("/admin/live", response_class=HTMLResponse)
async def live_page(
    request: Request,
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> Response:
    admin = await _current_admin_session(request)
    if admin is None:
        raise HTTPException(status_code=404, detail="live page not found")
    if admin.get("must_change_password"):
        return RedirectResponse("/admin/settings?force_password_change=1", status_code=status.HTTP_303_SEE_OTHER)

    runtime = request.app.state.runtime
    live_events, live_cursor = runtime.live_state.snapshot_state()
    initial_events = await asyncio.to_thread(
        smtp_live_snapshot,
        runtime,
        history_limit=DEFAULT_PAGE_SIZE,
        events=live_events,
    )
    sessions, total_count = await asyncio.to_thread(
        smtp_sessions_page,
        runtime,
        limit=limit,
        offset=offset,
    )
    return await asyncio.to_thread(
        _render_template,
        request,
        "admin/live.html",
        {
            "page_title": "实时活动",
            "admin": admin,
            "events": initial_events,
            "sessions": sessions,
            "websocket_url": f"/api/v1/admin/live/smtp/ws?after_cursor={live_cursor}",
            "stream_item_limit": DEFAULT_PAGE_SIZE,
            "sessions_pagination": build_pagination_context(
                path="/admin/live",
                limit=limit,
                offset=offset,
                total_count=total_count,
                item_count=len(sessions),
            ),
        },
    )


@router.get("/api/v1/admin/dashboard/metrics")
async def dashboard_metrics(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "system.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    return await get_dashboard_service(request.app).snapshot()


@router.get("/api/v1/admin/live/smtp/stream")
async def smtp_stream(
    request: Request,
    _admin: PermissionContext = Depends(require_admin_live_access),
    after_cursor: str | None = Query(default=None),
    last_event_id: str | None = Header(default=None, alias="Last-Event-ID"),
) -> StreamingResponse:
    return StreamingResponse(
        stream_smtp_live_events(
            request.app.state.runtime,
            after_cursor=after_cursor,
            last_event_id=last_event_id,
        ),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Deprecation": "true",
            "Link": '</api/v1/admin/live/smtp/ws>; rel="alternate"',
        },
    )


@router.websocket("/api/v1/admin/live/smtp/ws")
async def smtp_websocket(websocket: WebSocket) -> None:
    _admin = await _authorize_admin_websocket(websocket)
    if _admin is None:
        await websocket.accept()
        await websocket.close(code=1008, reason="admin WebSocket access denied")
        return

    await websocket.accept()
    event_stream = iter_smtp_live_events(
        websocket.app.state.runtime,
        after_cursor=websocket.query_params.get("after_cursor"),
    )
    receive_task: asyncio.Task[dict[str, Any]] | None = asyncio.create_task(
        websocket.receive()
    )
    event_task: asyncio.Task[dict[str, Any]] | None = asyncio.create_task(
        anext(event_stream)
    )
    try:
        while True:
            done, _ = await asyncio.wait(
                {receive_task, event_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if receive_task in done:
                message = receive_task.result()
                if message.get("type") == "websocket.disconnect":
                    return
                await websocket.close(
                    code=1008,
                    reason="admin live WebSocket is server-only",
                )
                return

            try:
                payload = event_task.result()
            except StopAsyncIteration:
                return
            await websocket.send_json(payload)
            event_task = asyncio.create_task(anext(event_stream))
    except (asyncio.CancelledError, WebSocketDisconnect, RuntimeError):
        return
    finally:
        tasks = tuple(
            task for task in (receive_task, event_task) if task is not None
        )
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await event_stream.aclose()


@router.post("/api/v1/admin/domains/{domain_id}/dns-check")
async def run_domain_dns_check(
    domain_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "domains.write")
    _require_domain_grant(admin, domain_id)
    await _record_admin_key_usage(request, admin)
    runtime = request.app.state.runtime

    try:
        domain = await asyncio.to_thread(runtime.domains.get_domain, domain_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    dns_check = DnsCheckService()
    check_result = await dns_check.run_dns_check(domain["root_domain_ascii"])
    checked_at = utc_now()
    stored_result = {
        "domain_id": domain_id,
        "root_domain_ascii": domain["root_domain_ascii"],
        "checked_at": checked_at,
        **check_result,
    }

    try:
        updated_domain = await runtime.domains.record_dns_check(
            domain_id,
            expected_root_domain_ascii=str(domain["root_domain_ascii"]),
            checked_at=checked_at,
            details=stored_result,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="domain authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    updated_domain["dns_check"] = stored_result
    await _write_audit_best_effort(request, admin, "domains.dns_check", "domain", str(domain_id), "success")
    return updated_domain


@router.get("/api/v1/admin/domains")
async def list_domains(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=LEGACY_API_MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict:
    require_admin_scope(admin, "domains.read")
    await _record_admin_key_usage(request, admin)
    allowed = _allowed_domain_ids(admin)
    return await asyncio.to_thread(
        _domains_page,
        request.app.state.settings.database_path,
        allowed_domain_ids=allowed,
        limit=limit,
        offset=offset,
    )


@router.post("/api/v1/admin/domains", status_code=status.HTTP_201_CREATED)
async def create_domain(
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict:
    require_admin_scope(admin, "domains.write")
    if _allowed_domain_ids(admin) is not None:
        raise HTTPException(status_code=403, detail="creating domains requires an all-domain grant")
    await _record_admin_key_usage(request, admin)
    runtime = request.app.state.runtime
    root_domain = payload.get("root_domain")
    if not isinstance(root_domain, str) or not root_domain.strip():
        raise HTTPException(status_code=422, detail="root_domain is required")
    settings = runtime.get_settings()

    try:
        created = await runtime.create_domain(
            root_domain,
            accept_exact=payload.get("accept_exact", True),
            accept_subdomains=payload.get("accept_subdomains", True),
            public_web_enabled=payload.get("public_web_enabled", False),
            public_api_enabled=payload.get("public_api_enabled", False),
            plus_addressing_mode=payload.get("plus_addressing_mode", "keep"),
            local_part_case_sensitive=payload.get("local_part_case_sensitive", False),
            is_active=payload.get("is_active", True),
            max_message_size_bytes=payload.get("max_message_size_bytes", settings["max_message_size_bytes"]),
            retention_days=payload.get("retention_days"),
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="domain authorization changed") from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "domains.create", "domain", str(created["id"]), "success")
    return created


@router.get("/api/v1/admin/domains/{domain_id}")
async def get_domain(
    domain_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "domains.read")
    _require_domain_grant(admin, domain_id)
    await _record_admin_key_usage(request, admin)
    try:
        return await asyncio.to_thread(request.app.state.runtime.domains.get_domain, domain_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/api/v1/admin/domains/{domain_id}")
async def update_domain(
    domain_id: int,
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "domains.write")
    _require_domain_grant(admin, domain_id)
    if "root_domain" in payload:
        _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        updated = await request.app.state.runtime.domains.update_domain(
            domain_id,
            payload,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="domain authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "domains.update", "domain", str(domain_id), "success")
    return updated


@router.delete("/api/v1/admin/domains/{domain_id}")
async def delete_domain(
    domain_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "domains.write")
    _require_domain_grant(admin, domain_id)
    await _record_admin_key_usage(request, admin)
    try:
        deleted = await request.app.state.runtime.domains.delete_domain(
            domain_id,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="domain authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="domain has dependent mailboxes or grants") from exc
    await _write_audit_best_effort(request, admin, "domains.delete", "domain", str(domain_id), "success")
    return {"deleted": True, "domain": deleted}


@router.get("/api/v1/admin/mailboxes")
async def list_mailboxes(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    q: str | None = Query(default=None),
    domain_id: int | None = Query(default=None),
    public_enabled: bool | None = Query(default=None),
    is_hidden: bool | None = Query(default=None),
) -> dict[str, Any]:
    require_admin_scope(admin, "mailboxes.read")
    await _record_admin_key_usage(request, admin)
    allowed_domains = _allowed_domain_ids(admin)
    if domain_id is not None:
        _require_domain_grant(admin, domain_id)
    service = request.app.state.runtime.mailboxes
    result = await asyncio.to_thread(
        _list_mailboxes_page,
        service,
        limit=limit,
        offset=offset,
        query=q,
        domain_id=domain_id,
        public_enabled=public_enabled,
        is_hidden=is_hidden,
        allowed_domain_ids=allowed_domains,
        allowed_mailbox_patterns=tuple(admin.mailbox_patterns),
    )
    return result


@router.get("/api/v1/admin/mailboxes/{mailbox_id}")
async def get_mailbox(
    mailbox_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict[str, Any]:
    require_admin_scope(admin, "mailboxes.read")
    await _record_admin_key_usage(request, admin)
    try:
        mailbox, deliveries = await asyncio.to_thread(
            _mailbox_detail,
            request.app.state.runtime.mailboxes,
            admin,
            mailbox_id,
            limit=limit,
            offset=offset,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return {**mailbox, "deliveries": deliveries["items"], "delivery_count": deliveries["total_count"]}


@router.patch("/api/v1/admin/mailboxes/{mailbox_id}")
async def update_mailbox(
    mailbox_id: int,
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "mailboxes.write")
    await _record_admin_key_usage(request, admin)
    await _require_mailbox_grant(request, admin, mailbox_id)

    updates: dict[str, Any] = {}
    if "public_enabled" in payload:
        updates["public_enabled"] = _coerce_bool(payload["public_enabled"])
    if "is_hidden" in payload:
        updates["is_hidden"] = _coerce_bool(payload["is_hidden"])
    if not updates:
        raise HTTPException(status_code=422, detail="public_enabled or is_hidden is required")

    try:
        updated = await request.app.state.runtime.mailboxes.update_mailbox(
            mailbox_id,
            updates,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="mailbox authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    await _write_audit_best_effort(request, admin, "mailboxes.update", "mailbox", str(mailbox_id), "success")
    return updated


@router.delete("/api/v1/admin/mailboxes/{mailbox_id}")
async def delete_mailbox_deliveries(
    mailbox_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "mailboxes.write")
    await _record_admin_key_usage(request, admin)
    await _require_mailbox_grant(request, admin, mailbox_id)
    try:
        result = await request.app.state.runtime.mailboxes.soft_delete_mailbox_deliveries(
            mailbox_id,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="mailbox authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _write_audit_best_effort(
        request,
        admin,
        "deliveries.bulk_delete",
        "mailbox",
        str(mailbox_id),
        "success",
    )
    return result


@router.get("/api/v1/admin/messages")
async def list_messages(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    q: str | None = Query(default=None),
    parse_status: str | None = Query(default=None),
    mailbox_id: int | None = Query(default=None),
) -> dict[str, Any]:
    require_admin_scope(admin, "messages.read")
    await _record_admin_key_usage(request, admin)
    allowed_domains = _allowed_domain_ids(admin)
    if mailbox_id is not None:
        await _require_mailbox_grant(request, admin, mailbox_id)
    try:
        return await asyncio.to_thread(
            request.app.state.runtime.messages.list_messages,
            limit=limit,
            offset=offset,
            query=q,
            parse_status=parse_status,
            mailbox_id=mailbox_id,
            allowed_domain_ids=allowed_domains,
            allowed_mailbox_patterns=tuple(admin.mailbox_patterns),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/api/v1/admin/messages/bulk-delete")
async def bulk_delete_deliveries(
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "messages.write")
    delivery_ids = _coerce_text_list(payload.get("delivery_ids"))
    if len(delivery_ids) > LEGACY_API_MAX_BULK_DELIVERY_IDS:
        raise HTTPException(
            status_code=422,
            detail=(
                "delivery_ids exceeds the maximum of "
                f"{LEGACY_API_MAX_BULK_DELIVERY_IDS} items"
            ),
        )
    await _record_admin_key_usage(request, admin)
    for delivery_id in delivery_ids:
        await _require_delivery_grant(request, admin, delivery_id)
    try:
        result = await request.app.state.runtime.messages.soft_delete_deliveries(
            delivery_ids,
            authorization_principal=admin,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise HTTPException(status_code=403, detail="message authorization changed") from exc
    await _write_audit_best_effort(
        request,
        admin,
        "deliveries.bulk_delete",
        "delivery",
        None,
        "success",
    )
    return result


@router.post("/api/v1/admin/messages/{message_id}/reparse", status_code=status.HTTP_202_ACCEPTED)
async def reparse_message(
    message_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict:
    require_admin_scope(admin, "messages.write")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    try:
        await request.app.state.runtime.messages.reparse_message(
            message_id,
            authorization_principal=admin,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise HTTPException(status_code=403, detail="message authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "messages.reparse", "message", message_id, "success")
    return {"queued": True, "message_id": message_id}


@router.get("/api/v1/admin/messages/{message_id}")
async def get_message(
    message_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "messages.read")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    try:
        return await asyncio.to_thread(
            request.app.state.runtime.messages.get_admin_message_detail,
            message_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/api/v1/admin/messages/{message_id}/raw")
async def download_message_raw(
    message_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> Response:
    require_admin_scope(admin, "messages.read")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    try:
        raw = await asyncio.to_thread(
            request.app.state.runtime.messages.get_admin_raw_file,
            message_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(raw["path"], media_type="message/rfc822", filename=f"{message_id}.eml")


@router.get("/api/v1/admin/messages/{message_id}/attachments/{attachment_id}")
async def download_message_attachment(
    message_id: str,
    attachment_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> Response:
    require_admin_scope(admin, "messages.read")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    try:
        attachment = await asyncio.to_thread(
            request.app.state.runtime.messages.get_admin_attachment_file,
            message_id,
            attachment_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    safe_filename = attachment.get("safe_filename") or "attachment.bin"
    return FileResponse(
        attachment["path"],
        media_type=attachment.get("content_type") or "application/octet-stream",
        filename=safe_filename,
        headers={
            "Content-Disposition": f'attachment; filename="{safe_filename}"',
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.delete("/api/v1/admin/messages/{message_id}")
async def delete_message_deliveries(
    message_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "messages.write")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    try:
        result = await request.app.state.runtime.messages.soft_delete_message(
            message_id,
            authorization_principal=admin,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise HTTPException(status_code=403, detail="message authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "deliveries.bulk_delete", "message", message_id, "success")
    return result


@router.delete("/api/v1/admin/messages/{message_id}/deliveries/{delivery_id}")
async def delete_message_delivery(
    message_id: str,
    delivery_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "messages.write")
    await _record_admin_key_usage(request, admin)
    await _require_message_grant(request, admin, message_id)
    await _require_delivery_grant(request, admin, delivery_id)
    try:
        delivery_exists = await asyncio.to_thread(
            request.app.state.runtime.messages.message_has_delivery,
            message_id,
            delivery_id,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    if not delivery_exists:
        raise HTTPException(status_code=404, detail="delivery not found")
    try:
        result = await request.app.state.runtime.messages.soft_delete_delivery(
            delivery_id,
            authorization_principal=admin,
        )
    except (ApiKeyAuthorizationError, PermissionDenied) as exc:
        raise HTTPException(status_code=403, detail="message authorization changed") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "deliveries.delete", "delivery", delivery_id, "success")
    return result


@router.get("/api/v1/admin/smtp-sessions")
async def list_smtp_sessions(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict[str, Any]:
    require_admin_scope(admin, "smtp.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    runtime = request.app.state.runtime
    items, total_count = await asyncio.to_thread(
        smtp_sessions_page,
        runtime,
        limit=limit,
        offset=offset,
    )
    return {"items": items, "total_count": total_count}


@router.get("/api/v1/admin/smtp-sessions/{session_id}")
async def get_smtp_session(
    session_id: str,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    event_limit: int = Query(default=100, ge=1, le=LEGACY_API_MAX_EVENT_PAGE_SIZE),
    event_offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict[str, Any]:
    require_admin_scope(admin, "smtp.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    runtime = request.app.state.runtime
    result = await asyncio.to_thread(
        _smtp_session_detail,
        runtime.settings.database_path,
        session_id,
        event_limit=event_limit,
        event_offset=event_offset,
    )
    if result is None:
        raise HTTPException(status_code=404, detail="smtp session not found")
    return result


@router.get("/api/v1/admin/audit-logs")
async def list_audit_logs(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=0, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    actor: str | None = Query(default=None),
    action: str | None = Query(default=None),
    resource: str | None = Query(default=None),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
) -> dict:
    require_admin_scope(admin, "audit.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    return await asyncio.to_thread(
        request.app.state.runtime.audit.list_logs,
        limit=limit,
        offset=offset,
        actor=actor,
        action=action,
        resource=resource,
        start_time=start_time,
        end_time=end_time,
    )


@router.get("/api/v1/admin/settings")
async def get_settings(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict:
    require_admin_scope(admin, "system.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    return request.app.state.runtime.system_settings.get_settings()


@router.patch("/api/v1/admin/settings")
async def update_settings(
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict:
    require_admin_scope(admin, "system.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        updated = await request.app.state.runtime.system_settings.update_settings(
            payload,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="settings authorization changed") from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "settings.update", "system_settings", None, "success")
    return updated


@router.get("/api/v1/admin/api-keys")
async def list_api_keys(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.read")
    await _record_admin_key_usage(request, admin)
    result = await asyncio.to_thread(
        request.app.state.runtime.api_keys.list_keys,
        limit=limit,
        offset=offset,
    )
    if not admin.legacy_credential:
        result["items"] = [
            item for item in result["items"]
            if _key_is_within_grant(admin, item)
        ]
        result["total_count"] = len(result["items"])
    return result


@router.get("/api/v1/admin/admins")
async def list_admins(
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    return await asyncio.to_thread(
        request.app.state.runtime.auth.list_admins,
        limit=limit,
        offset=offset,
    )


@router.post("/api/v1/admin/admins", status_code=status.HTTP_201_CREATED)
async def create_admin_account(
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.write")
    require_admin_scope(admin, "admins.credentials.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        created = await request.app.state.runtime.auth.create_admin(
            username=str(payload.get("username") or ""),
            password=str(payload.get("password") or ""),
            role=str(payload.get("role") or "viewer"),
            display_name=_nullable_text(payload.get("display_name")),
            is_active=_coerce_bool(payload.get("is_active", True)),
            must_change_password=_coerce_bool(payload.get("must_change_password", True)),
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "admins.create", "admin", str(created["id"]), "success")
    return created


@router.get("/api/v1/admin/admins/{admin_id}")
async def get_admin_account(
    admin_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.read")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        return await asyncio.to_thread(request.app.state.runtime.auth.get_admin, admin_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/api/v1/admin/admins/{admin_id}")
async def update_admin_account(
    admin_id: int,
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    updates = {
        key: payload[key]
        for key in ("username", "display_name", "role", "is_active", "must_change_password")
        if key in payload
    }
    try:
        updated = await request.app.state.runtime.auth.update_admin(
            admin_id,
            **updates,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "admins.update", "admin", str(admin_id), "success")
    return updated


@router.post("/api/v1/admin/admins/{admin_id}/reset-password")
async def reset_admin_account_password(
    admin_id: int,
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.credentials.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        updated = await request.app.state.runtime.auth.reset_admin_password(
            admin_id,
            str(payload.get("password") or ""),
            must_change_password=_coerce_bool(payload.get("must_change_password", True)),
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "admins.password_reset", "admin", str(admin_id), "success")
    return updated


@router.post("/api/v1/admin/admins/{admin_id}/revoke-sessions")
async def revoke_admin_account_sessions(
    admin_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.sessions.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        revoked = await request.app.state.runtime.auth.revoke_admin_sessions(
            admin_id,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "admins.sessions_revoke", "admin", str(admin_id), "success")
    return {"admin_id": admin_id, "revoked_sessions": revoked}


@router.delete("/api/v1/admin/admins/{admin_id}")
async def delete_admin_account(
    admin_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "admins.write")
    _require_global_grant(admin)
    await _record_admin_key_usage(request, admin)
    try:
        deleted = await request.app.state.runtime.auth.delete_admin(
            admin_id,
            authorization_principal=admin,
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    await _write_audit_best_effort(request, admin, "admins.delete", "admin", str(admin_id), "success")
    return {"deleted": True, "admin": deleted}


@router.post("/api/v1/admin/api-keys", status_code=status.HTTP_201_CREATED)
async def create_api_key(
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.write")
    await _record_admin_key_usage(request, admin)

    name = str(payload.get("name", "")).strip()
    kind = str(payload.get("kind", "")).strip()
    scopes = _coerce_text_list(payload.get("scopes"))
    grant_all_domains = _coerce_bool(payload.get("grant_all_domains")) if "grant_all_domains" in payload else False
    domain_ids = [] if grant_all_domains else _coerce_int_list(payload.get("domain_ids"))
    domain_grant_mode = str(payload.get("domain_grant_mode") or ("all" if grant_all_domains else ("selected" if domain_ids else "none")))
    mailbox_patterns = _coerce_text_list(payload.get("mailbox_patterns"))
    try:
        rate_limit_per_min = _coerce_non_negative_int(
            payload.get("rate_limit_per_min", 3600),
            "rate_limit_per_min",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    allowed_ip_cidrs = _coerce_text_list(payload.get("allowed_ip_cidrs"))
    expires_at = _nullable_text(payload.get("expires_at"))
    allow_header = _coerce_bool(payload.get("allow_header", True))
    allow_query = _coerce_bool(payload.get("allow_query", False))

    if not name:
        raise HTTPException(status_code=422, detail="name is required")
    if not kind:
        raise HTTPException(status_code=422, detail="kind is required")
    if not scopes:
        raise HTTPException(status_code=422, detail="scopes are required")
    _require_key_management_grant(
        admin,
        {
            "scopes": scopes,
            "domain_grant_mode": domain_grant_mode,
            "domain_ids": domain_ids,
            "mailbox_patterns": mailbox_patterns,
            "rate_limit_per_min": rate_limit_per_min,
            "allowed_ip_cidrs": allowed_ip_cidrs,
            "expires_at": expires_at,
            "allow_header": allow_header,
            "allow_query": allow_query,
        },
    )

    try:
        created = await request.app.state.runtime.api_keys.create_key(
            name=name,
            kind=kind,
            scopes=scopes,
            domain_ids=domain_ids,
            domain_grant_mode=domain_grant_mode,
            mailbox_patterns=mailbox_patterns,
            rate_limit_per_min=rate_limit_per_min,
            allowed_ip_cidrs=allowed_ip_cidrs,
            expires_at=expires_at,
            allow_header=allow_header,
            allow_query=allow_query,
            authorization_principal=_key_mutation_principal(admin),
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=403, detail="api key authorization changed") from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    await _write_audit_best_effort(request, admin, "api_keys.create", "api_key", str(created["id"]), "success")
    return created


@router.get("/api/v1/admin/api-keys/{api_key_id}")
async def get_api_key(
    api_key_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.read")
    await _record_admin_key_usage(request, admin)
    try:
        target = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
        _require_key_management_grant(admin, target)
        return target
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.patch("/api/v1/admin/api-keys/{api_key_id}")
async def update_api_key(
    api_key_id: int,
    payload: dict[str, Any],
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.write")
    await _record_admin_key_usage(request, admin)
    try:
        existing_key = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    _require_key_management_grant(admin, existing_key)

    updates: dict[str, Any] = {}
    if "name" in payload:
        updates["name"] = str(payload.get("name", "")).strip()
    if "description" in payload:
        updates["description"] = _nullable_text(payload.get("description"))
    if "kind" in payload:
        updates["kind"] = str(payload.get("kind", "")).strip()
    if "status" in payload:
        updates["status"] = str(payload.get("status", "")).strip()
    if "allow_header" in payload:
        updates["allow_header"] = _coerce_bool(payload.get("allow_header"))
    if "allow_query" in payload:
        updates["allow_query"] = _coerce_bool(payload.get("allow_query"))
    if "rate_limit_per_min" in payload:
        try:
            updates["rate_limit_per_min"] = _coerce_non_negative_int(
                payload.get("rate_limit_per_min"),
                "rate_limit_per_min",
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    if "allowed_ip_cidrs" in payload:
        updates["allowed_ip_cidrs"] = _coerce_text_list(payload.get("allowed_ip_cidrs"))
    if "expires_at" in payload:
        updates["expires_at"] = _nullable_text(payload.get("expires_at"))
    if "scopes" in payload:
        updates["scopes"] = _coerce_text_list(payload.get("scopes"))
    grant_all_domains = _coerce_bool(payload.get("grant_all_domains")) if "grant_all_domains" in payload else False
    if grant_all_domains:
        updates["domain_ids"] = []
        updates["domain_grant_mode"] = "all"
    elif "domain_ids" in payload:
        try:
            updates["domain_ids"] = _coerce_int_list(payload.get("domain_ids"))
            updates["domain_grant_mode"] = str(payload.get("domain_grant_mode") or ("selected" if updates["domain_ids"] else "none"))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail="invalid domain_ids") from exc
    if "mailbox_patterns" in payload:
        updates["mailbox_patterns"] = _coerce_text_list(payload.get("mailbox_patterns"))

    proposed_key = dict(existing_key)
    proposed_key.update(updates)
    _require_key_management_grant(admin, proposed_key)

    try:
        updated = await request.app.state.runtime.api_keys.update_key(
            api_key_id,
            **updates,
            authorization_principal=_key_mutation_principal(admin),
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=404, detail="api key not found") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, sqlite3.IntegrityError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    await _write_audit_best_effort(request, admin, "api_keys.update", "api_key", str(api_key_id), "success")
    return updated


@router.post("/api/v1/admin/api-keys/{api_key_id}/rotate")
async def rotate_api_key(
    api_key_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.write")
    await _record_admin_key_usage(request, admin)
    try:
        target = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
        _require_key_management_grant(admin, target)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        rotated = await request.app.state.runtime.api_keys.rotate_key(
            api_key_id,
            authorization_principal=_key_mutation_principal(admin),
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=404, detail="api key not found") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except sqlite3.IntegrityError as exc:
        raise HTTPException(status_code=409, detail="api key rotation conflict") from exc

    await _write_audit_best_effort(request, admin, "api_keys.rotate", "api_key", str(api_key_id), "success")
    return rotated


@router.post("/api/v1/admin/api-keys/{api_key_id}/revoke")
async def revoke_api_key(
    api_key_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.write")
    await _record_admin_key_usage(request, admin)
    try:
        target = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
        _require_key_management_grant(admin, target)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        revoked = await request.app.state.runtime.api_keys.revoke_key(
            api_key_id,
            authorization_principal=_key_mutation_principal(admin),
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=404, detail="api key not found") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    await _write_audit_best_effort(request, admin, "api_keys.revoke", "api_key", str(api_key_id), "success")
    return revoked


@router.delete("/api/v1/admin/api-keys/{api_key_id}")
async def delete_api_key(
    api_key_id: int,
    request: Request,
    admin: PermissionContext = Depends(require_admin_key),
) -> dict[str, Any]:
    require_admin_scope(admin, "api_keys.write")
    await _record_admin_key_usage(request, admin)
    try:
        target = await asyncio.to_thread(request.app.state.runtime.api_keys.get_key, api_key_id)
        _require_key_management_grant(admin, target)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        deleted = await request.app.state.runtime.api_keys.delete_key(
            api_key_id,
            authorization_principal=_key_mutation_principal(admin),
        )
    except ApiKeyAuthorizationError as exc:
        raise HTTPException(status_code=404, detail="api key not found") from exc
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    await _write_audit_best_effort(request, admin, "api_keys.delete", "api_key", str(api_key_id), "success")
    return {"deleted": True, "api_key_id": api_key_id}
