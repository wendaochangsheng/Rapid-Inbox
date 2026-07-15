from __future__ import annotations

import asyncio

from fastapi import APIRouter, HTTPException, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse, Response

from app.http.pagination import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE, build_pagination_context
from app.services.attachments import AttachmentService
from app.services.messages import MessageService


router = APIRouter()


def _message_service(request: Request) -> MessageService:
    return MessageService(request.app.state.runtime)


def _attachment_service(request: Request) -> AttachmentService:
    runtime = request.app.state.runtime
    return AttachmentService(runtime, _message_service(request))


def _render_template(
    request: Request,
    template_name: str,
    context: dict,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return request.app.state.templates.TemplateResponse(
        request,
        template_name,
        context,
        headers=headers,
    )


async def _render_async(
    request: Request,
    template_name: str,
    context: dict,
    *,
    headers: dict[str, str] | None = None,
) -> Response:
    return await asyncio.to_thread(
        _render_template,
        request,
        template_name,
        context,
        headers=headers,
    )


def _parse_live_cursor(cursor: str | None) -> tuple[str, int] | None:
    if cursor is None:
        return None
    try:
        generation, seq_text = cursor.rsplit(":", 1)
        if not generation:
            return None
        seq = int(seq_text)
    except (AttributeError, ValueError):
        return None
    return generation, seq


@router.get("/", response_class=HTMLResponse)
async def home_page(request: Request) -> Response:
    return await _render_async(
        request,
        "public/home.html",
        {
            "page_title": "首页",
            "mailbox_example": "收件@示例域名.cn",
            "mail_route_prefix": "/mail/PLACEHOLDER",
        },
    )


@router.get("/mail/{mailbox_address}", response_class=HTMLResponse)
async def mailbox_page(
    mailbox_address: str,
    request: Request,
    limit: int = Query(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(default=0, ge=0, le=1_000_000),
) -> Response:
    service = _message_service(request)
    try:
        mailbox = await service.get_public_mailbox_view(
            mailbox_address,
            surface="web",
            limit=limit,
            offset=offset,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    pagination = build_pagination_context(
        path=f"/mail/{mailbox['mailbox']}",
        limit=mailbox["limit"],
        offset=mailbox["offset"],
        total_count=mailbox["message_count"],
        item_count=len(mailbox["items"]),
    )
    _, live_cursor = request.app.state.runtime.live_state.snapshot_state()
    mailbox_live_enabled = mailbox["offset"] == 0
    return await _render_async(
        request,
        "public/mailbox.html",
        {
            "page_title": mailbox["mailbox"],
            "mailbox_address": mailbox["mailbox"],
            "items": mailbox["items"],
            "message_count": mailbox["message_count"],
            "mailbox_live_enabled": mailbox_live_enabled,
            "mailbox_live_cursor": live_cursor if mailbox_live_enabled else "",
            "mailbox_live_url": f"/mail/{mailbox['mailbox']}/ws?after_cursor={live_cursor}" if mailbox_live_enabled else "",
            **pagination,
        },
    )


@router.websocket("/mail/{mailbox_address}/ws")
async def mailbox_websocket(mailbox_address: str, websocket: WebSocket) -> None:
    service = _message_service(websocket)
    after_cursor = websocket.query_params.get("after_cursor")
    await websocket.accept()
    try:
        mailbox = await service.get_public_mailbox_view(
            mailbox_address,
            surface="web",
            limit=1,
            offset=0,
        )
    except LookupError:
        await websocket.close(code=1008)
        return

    runtime = websocket.app.state.runtime
    parsed_cursor = _parse_live_cursor(after_cursor)
    if parsed_cursor is not None and parsed_cursor[0] == runtime.live_state.generation:
        last_seq = max(parsed_cursor[1], 0)
    else:
        _, live_cursor = runtime.live_state.snapshot_state()
        parsed_live_cursor = _parse_live_cursor(live_cursor)
        last_seq = 0 if parsed_live_cursor is None else parsed_live_cursor[1]

    canonical_mailbox = str(mailbox["mailbox"])

    try:
        while True:
            new_events = runtime.live_state.snapshot_since(last_seq)
            if new_events:
                last_seq = int(new_events[-1].get("seq", last_seq))
                for event in new_events:
                    event_type = str(event.get("type") or "")
                    if event_type not in {"mailbox_delivery", "mailbox_delivery_updated"}:
                        continue
                    if str(event.get("mailbox") or "") != canonical_mailbox:
                        continue
                    delivery_id = str(event.get("delivery_id") or "")
                    if not delivery_id:
                        continue
                    try:
                        item = await service.get_public_mailbox_item(
                            canonical_mailbox,
                            delivery_id,
                            surface="web",
                        )
                    except LookupError:
                        continue
                    if event_type == "mailbox_delivery" and str(event.get("parse_status") or "") == "pending":
                        item["parse_status"] = "pending"
                        item["subject"] = None
                        item["verification_code"] = None
                    await websocket.send_json({"type": event_type, "item": item})
                continue
            await asyncio.sleep(0.25)
    except asyncio.CancelledError:
        return
    except (WebSocketDisconnect, RuntimeError):
        return


@router.get("/mail/{mailbox_address}/{delivery_id}", response_class=HTMLResponse)
async def message_page(mailbox_address: str, delivery_id: str, request: Request) -> Response:
    service = _message_service(request)
    try:
        detail = await service.get_public_delivery_detail(mailbox_address, delivery_id, surface="web")
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    detail["page_title"] = detail.get("subject") or "邮件详情"
    return await _render_async(
        request,
        "public/message.html",
        detail,
    )


@router.get("/mail/{mailbox_address}/{delivery_id}/raw")
async def message_raw(mailbox_address: str, delivery_id: str, request: Request) -> Response:
    service = _message_service(request)
    try:
        raw_file = await service.get_public_raw_file(mailbox_address, delivery_id, surface="web")
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(raw_file["path"], media_type="message/rfc822", filename=f"{delivery_id}.eml")


@router.get("/mail/{mailbox_address}/{delivery_id}/html", response_class=HTMLResponse)
async def message_html_frame(mailbox_address: str, delivery_id: str, request: Request) -> Response:
    service = _message_service(request)
    try:
        preview = await service.get_public_html_preview(mailbox_address, delivery_id, surface="web")
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return await _render_async(
        request,
        "public/html_frame.html",
        {"page_title": "网页预览", "srcdoc": preview["srcdoc"]},
        headers={
            "Content-Security-Policy": "default-src 'none'; img-src 'self' data:; style-src 'unsafe-inline'",
            "X-Preview-Body-Truncated": "1" if preview["html_body_truncated"] else "0",
            "X-Preview-Inline-Embedded": str(preview["inline_preview_embedded_count"]),
            "X-Preview-Inline-Skipped": str(preview["inline_preview_skipped_count"]),
        },
    )


@router.get("/mail/{mailbox_address}/{delivery_id}/attachments/{attachment_id}")
async def message_attachment(
    mailbox_address: str,
    delivery_id: str,
    attachment_id: str,
    request: Request,
) -> Response:
    service = _attachment_service(request)
    try:
        attachment = await service.get_delivery_attachment_file(
            mailbox_address,
            delivery_id,
            attachment_id,
            surface="web",
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(
        attachment["path"],
        media_type=attachment.get("content_type") or "application/octet-stream",
        filename=attachment.get("safe_filename") or "attachment.bin",
        headers=service.build_attachment_response_headers(attachment),
    )
