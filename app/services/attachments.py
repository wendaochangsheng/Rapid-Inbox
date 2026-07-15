from __future__ import annotations

from typing import Any

from app.services.messages import MessageService, SAFE_INLINE_CONTENT_TYPES


class AttachmentService:
    def __init__(self, runtime: Any, messages: MessageService | None = None) -> None:
        self._runtime = runtime
        self._messages = messages or MessageService(runtime)

    async def get_delivery_attachment(
        self,
        mailbox_address: str,
        delivery_id: str,
        attachment_id: str,
        *,
        surface: str = "web",
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_public_attachment(
            mailbox_address,
            delivery_id,
            attachment_id,
            surface=surface,
            request_ip=request_ip,
        )

    async def get_delivery_attachment_file(
        self,
        mailbox_address: str,
        delivery_id: str,
        attachment_id: str,
        *,
        surface: str = "web",
        request_ip: str | None = None,
    ) -> dict[str, Any]:
        return await self._runtime.get_public_attachment_file(
            mailbox_address,
            delivery_id,
            attachment_id,
            surface=surface,
            request_ip=request_ip,
        )

    def build_attachment_response_headers(self, attachment: dict[str, Any]) -> dict[str, str]:
        disposition = "inline" if self._should_inline_attachment(attachment) else "attachment"
        safe_filename = attachment.get("safe_filename") or "attachment.bin"
        return {
            "Content-Disposition": f'{disposition}; filename="{safe_filename}"',
            "X-Content-Type-Options": "nosniff",
        }

    def _should_inline_attachment(self, attachment: dict[str, Any]) -> bool:
        if not bool(attachment.get("is_inline")):
            return False
        content_type = str(attachment.get("content_type") or "").split(";", 1)[0].strip().lower()
        return content_type in SAFE_INLINE_CONTENT_TYPES


__all__ = ["AttachmentService"]
