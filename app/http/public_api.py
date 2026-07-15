from __future__ import annotations

import asyncio
from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response

from app.auth.api_keys import set_active_permission_context
from app.auth.permissions import PermissionContext
from app.services.attachments import AttachmentService
from app.services.messages import MessageService

from .api_v2 import (
    ApiProblem,
    _decode_cursor as _decode_signed_cursor,
    _encode_cursor as _encode_signed_cursor,
)


router = APIRouter()


async def require_public_api_key(
    request: Request,
    api_key: str | None,
    query_api_key: str | None = None,
) -> PermissionContext:
    set_active_permission_context(None)
    credential = api_key or query_api_key
    if not credential:
        raise HTTPException(status_code=401, detail="invalid api key")

    transport = "header" if api_key else "query"
    try:
        context = await asyncio.to_thread(
            request.app.state.runtime.api_keys.authenticate_public_credential,
            credential,
            transport=transport,
        )
    except LookupError as exc:
        raise HTTPException(status_code=401, detail="invalid api key") from exc
    if "public.read" not in context.scopes:
        raise HTTPException(status_code=403, detail="public.read")
    set_active_permission_context(context)
    return context


def _message_service(request: Request) -> MessageService:
    return MessageService(request.app.state.runtime)


def _attachment_service(request: Request) -> AttachmentService:
    runtime = request.app.state.runtime
    return AttachmentService(runtime, _message_service(request))


def _cursor_filters(mailbox_address: str, principal: PermissionContext) -> dict[str, str]:
    principal_id = principal.public_id or str(principal.api_key_id or "legacy")
    return {
        "mailbox": mailbox_address,
        "principal": principal_id,
    }


def _decode_cursor(
    request: Request,
    cursor: str | None,
    *,
    mailbox_address: str,
    principal: PermissionContext,
) -> tuple[str, str] | None:
    try:
        position = _decode_signed_cursor(
            request,
            cursor,
            "v1-public-mailbox-messages",
            _cursor_filters(mailbox_address, principal),
        )
    except ApiProblem as exc:
        raise HTTPException(status_code=422, detail="invalid cursor") from exc
    if position is None:
        return None
    delivered_at = position.get("delivered_at")
    delivery_id = position.get("delivery_id")
    if (
        not isinstance(delivered_at, str)
        or not delivered_at
        or not isinstance(delivery_id, str)
        or not delivery_id
    ):
        raise HTTPException(status_code=422, detail="invalid cursor")
    return delivered_at, delivery_id


def _encode_cursor(
    request: Request,
    cursor: dict[str, str] | None,
    *,
    mailbox_address: str,
    principal: PermissionContext,
) -> str | None:
    if cursor is None:
        return None
    try:
        return _encode_signed_cursor(
            request,
            "v1-public-mailbox-messages",
            {
                "delivered_at": str(cursor["delivered_at"]),
                "delivery_id": str(cursor["delivery_id"]),
            },
            _cursor_filters(mailbox_address, principal),
        )
    except (ApiProblem, KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=500, detail="invalid pagination state") from exc


@router.get("/api/v1/public/mailboxes/{mailbox_address}/messages")
async def list_mailbox_messages(
    mailbox_address: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
    cursor: str | None = Query(default=None, max_length=2048),
) -> dict:
    principal = await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    try:
        result = await _message_service(request).get_public_mailbox_view(
            mailbox_address,
            surface="api",
            limit=limit,
            offset=offset,
            cursor=_decode_cursor(
                request,
                cursor,
                mailbox_address=mailbox_address,
                principal=principal,
            ),
            request_ip=request_ip,
        )
        result["next_cursor"] = _encode_cursor(
            request,
            result.get("next_cursor"),
            mailbox_address=mailbox_address,
            principal=principal,
        )
        result["pagination"] = {
            "mode": result["pagination_mode"],
            "next_cursor": result["next_cursor"],
            "limit": result["limit"],
            "offset": result["offset"],
        }
        return result
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)


@router.get("/api/v1/public/mailboxes/{mailbox_address}/verification-codes")
async def list_mailbox_verification_codes(
    mailbox_address: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=1000),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> dict:
    await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    try:
        return await _message_service(request).get_public_mailbox_verification_codes(
            mailbox_address,
            limit=limit,
            offset=offset,
            request_ip=request_ip,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)


@router.get("/api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}")
async def get_mailbox_message(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
) -> dict:
    await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    try:
        return await _message_service(request).get_public_delivery_detail(
            mailbox_address,
            delivery_id,
            surface="api",
            request_ip=request_ip,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)


@router.get("/api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/verification-code")
async def get_mailbox_message_verification_code(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
) -> dict:
    await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    try:
        return await _message_service(request).get_public_delivery_verification_code(
            mailbox_address,
            delivery_id,
            request_ip=request_ip,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)


@router.get("/api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/raw")
async def get_mailbox_message_raw(
    mailbox_address: str,
    delivery_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
) -> Response:
    await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    try:
        raw_file = await _message_service(request).get_public_raw_file(
            mailbox_address,
            delivery_id,
            surface="api",
            request_ip=request_ip,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)
    return FileResponse(raw_file["path"], media_type="message/rfc822", filename=f"{delivery_id}.eml")


@router.get("/api/v1/public/mailboxes/{mailbox_address}/messages/{delivery_id}/attachments/{attachment_id}")
async def get_mailbox_message_attachment(
    mailbox_address: str,
    delivery_id: str,
    attachment_id: str,
    request: Request,
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    api_key: str | None = Query(default=None),
) -> Response:
    await require_public_api_key(request, x_api_key, api_key)
    request_ip = request.client.host if request.client is not None else None
    service = _attachment_service(request)
    try:
        attachment = await service.get_delivery_attachment_file(
            mailbox_address,
            delivery_id,
            attachment_id,
            surface="api",
            request_ip=request_ip,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    finally:
        set_active_permission_context(None)
    return FileResponse(
        attachment["path"],
        media_type=attachment.get("content_type") or "application/octet-stream",
        filename=attachment.get("safe_filename") or "attachment.bin",
        headers=service.build_attachment_response_headers(attachment),
    )
