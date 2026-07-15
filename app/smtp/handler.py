from __future__ import annotations

import uuid

from app.ingest.storage import utc_now


class RapidInboxHandler:
    def __init__(self, runtime) -> None:
        self.runtime = runtime

    async def handle_CONNECT(self, server, session, envelope, hostname, port):
        session_id = self._ensure_session_id(session)
        allowed, reason = await self._ensure_session_allowed(session, session_id)
        if not allowed:
            return f"421 {reason}"
        await self.runtime.ensure_smtp_session(session_id, session)
        await self._publish_session_event(
            session_id,
            {
                **self._session_event(session, session_id, {"state": "connect"}),
                "type": "connect",
            },
        )
        return None

    async def handle_RCPT(self, server, session, envelope, address: str, rcpt_options):
        session_id = self._ensure_session_id(session)
        allowed, reason = await self._ensure_session_allowed(session, session_id)
        if not allowed:
            return f"421 {reason}"
        await self.runtime.ensure_smtp_session(session_id, session, last_rcpt_to=address)
        session_event = self._session_event(session, session_id, {"rcpt_to": address, "state": "rcpt"})
        try:
            if address in envelope.rcpt_tos:
                await self.runtime.record_smtp_rcpt(session_id, rcpt_to=address, accepted=True)
                await self._publish_session_event(
                    session_id,
                    {
                        **session_event,
                        "type": "rcpt_accepted",
                        "rcpt_to": address,
                        "state": "rcpt",
                    },
                )
                return "250 OK"

            if self.runtime.domains.match_address(address) is None:
                await self.runtime.record_smtp_rcpt(session_id, rcpt_to=address, accepted=False)
                await self._publish_session_event(
                    session_id,
                    {
                        **session_event,
                        "type": "rcpt_rejected",
                        "rcpt_to": address,
                        "state": "rcpt",
                    },
                )
                return "550 domain not allowed"

            if len(envelope.rcpt_tos) >= self._max_recipients_per_message():
                await self.runtime.record_smtp_rcpt(session_id, rcpt_to=address, accepted=False)
                await self._publish_session_event(
                    session_id,
                    {
                        **session_event,
                        "type": "rcpt_rejected",
                        "rcpt_to": address,
                        "state": "rcpt",
                        "reason": "recipient limit exceeded",
                    },
                )
                return "552 too many recipients"

            envelope.rcpt_tos.append(address)
            await self.runtime.record_smtp_rcpt(session_id, rcpt_to=address, accepted=True)
            await self._publish_session_event(
                session_id,
                {
                    **session_event,
                    "type": "rcpt_accepted",
                    "rcpt_to": address,
                    "state": "rcpt",
                },
            )
            return "250 OK"
        except Exception:
            try:
                await self.runtime.close_smtp_session(
                    session_id,
                    status="error",
                    close_reason="RCPT handling error",
                )
            except Exception:
                pass
            raise

    async def handle_DATA(self, server, session, envelope):
        session_id = self._ensure_session_id(session)
        allowed, reason = await self._ensure_session_allowed(session, session_id)
        if not allowed:
            return f"421 {reason}"
        await self.runtime.ensure_smtp_session(session_id, session)
        if not envelope.rcpt_tos:
            return "554 no valid recipients"
        if len(envelope.rcpt_tos) > self._max_recipients_per_message():
            return "552 too many recipients"
        if len(envelope.content) > self._effective_message_size_limit(envelope.rcpt_tos):
            return "552 message too large"

        try:
            result = await self.runtime.accept_message(
                rcpt_tos=list(envelope.rcpt_tos),
                envelope_from=getattr(envelope, "mail_from", None),
                content=envelope.content,
                smtp_session_id=session_id,
            )
        except Exception:
            try:
                await self.runtime.close_smtp_session(
                    session_id,
                    status="error",
                    close_reason="DATA handling error",
                )
            except Exception:
                pass
            raise

        message_id = None
        if result.startswith("250 queued as "):
            message_id = result.removeprefix("250 queued as ").strip() or None
        else:
            return result
        await self._publish_session_event(
            session_id,
            {
                **self._session_event(session, session_id, {"state": "queued"}),
                "type": "queued",
                "mail_from": getattr(envelope, "mail_from", None),
                "rcpt_count": len(envelope.rcpt_tos),
                "message_id": message_id,
                "state": "queued",
            },
        )
        if message_id is not None and self.runtime.settings.smtp_close_after_data:
            self._schedule_transport_close(server)
        return result

    async def handle_QUIT(self, server, session, envelope):
        session_id = self._ensure_session_id(session)
        try:
            await self.runtime.close_smtp_session(
                session_id,
                status="closed",
                close_reason="client quit",
                result_code=221,
                result_message="2.0.0 Bye",
            )
            await self._publish_session_event(
                session_id,
                {
                    **self._session_event(session, session_id, {"state": "disconnect"}),
                    "type": "disconnect",
                    "result_code": 221,
                    "result_message": "2.0.0 Bye",
                },
            )
        except Exception:
            pass
        return "221 2.0.0 Bye"

    async def handle_DISCONNECT(self, server, session, envelope, error=None):
        session_id = getattr(session, "rapid_inbox_session_id", None)
        if session_id is None:
            return
        status = "error" if error is not None else "closed"
        close_reason = "connection lost"
        if error is not None:
            close_reason = f"connection lost: {error.__class__.__name__}"
        peer = getattr(session, "peer", None) or ("unknown", None)
        closed = await self.runtime.close_lost_smtp_session(
            session_id,
            status=status,
            remote_ip=peer[0] or "unknown",
            remote_port=peer[1],
            helo_name=getattr(session, "host_name", None),
            tls_used=bool(getattr(session, "ssl", None)),
            close_reason=close_reason,
        )
        if not closed:
            return
        await self._publish_session_event(
            session_id,
            {
                **self._session_event(session, session_id, {"state": "disconnect"}),
                "type": "disconnect",
                "close_reason": close_reason,
            },
        )

    async def _ensure_session_allowed(self, session, session_id: str) -> tuple[bool, str | None]:
        peer = getattr(session, "peer", None) or ("unknown", None)
        remote_ip = peer[0] or "unknown"
        return await self.runtime.register_smtp_connection(session_id, str(remote_ip))

    async def _publish_session_event(self, session_id: str, event: dict[str, object]) -> None:
        await self.runtime.live_state.publish(event)
        try:
            await self.runtime.record_smtp_event(session_id, str(event.get("type") or "event"), dict(event))
        except Exception:
            return

    def _max_recipients_per_message(self) -> int:
        return int(self.runtime.get_settings()["max_recipients_per_message"])

    def _effective_message_size_limit(self, rcpt_tos: list[str]) -> int:
        limit = int(self.runtime.get_settings()["max_message_size_bytes"])
        seen_domain_ids: set[int] = set()
        for rcpt_to in rcpt_tos:
            match, domain_limit = self.runtime.domains.match_address_with_size_limit(rcpt_to)
            if match is None or match.domain_id in seen_domain_ids:
                continue
            seen_domain_ids.add(match.domain_id)
            if domain_limit is not None:
                limit = min(limit, domain_limit)
        return limit

    def _schedule_transport_close(self, server) -> None:
        loop = getattr(server, "loop", None)
        transport = getattr(server, "transport", None)
        if loop is None or transport is None:
            return

        def close_transport() -> None:
            if not transport.is_closing():
                transport.close()

        try:
            loop.call_later(0.05, close_transport)
        except RuntimeError:
            return

    def _ensure_session_id(self, session) -> str:
        session_id = getattr(session, "rapid_inbox_session_id", None)
        if session_id is None:
            session_id = f"smtp_{uuid.uuid4().hex}"
            setattr(session, "rapid_inbox_session_id", session_id)
        return session_id

    def _session_event(self, session, session_id: str, extra: dict[str, object] | None = None) -> dict[str, object]:
        peer = getattr(session, "peer", None) or ("unknown", None)
        event: dict[str, object] = {
            "session_id": session_id,
            "remote_ip": peer[0] or "unknown",
            "remote_port": peer[1],
            "helo": getattr(session, "host_name", None),
            "tls_used": bool(getattr(session, "ssl", None)),
            "ts": utc_now(),
        }
        if extra:
            event.update(extra)
        return event
