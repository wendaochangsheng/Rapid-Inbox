from __future__ import annotations

import asyncio
import threading
from functools import partial
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from httpx import Headers
from starlette.websockets import WebSocketDisconnect

from app.config import Settings
from app.http import admin_api
from app.http.live import iter_smtp_live_events, smtp_live_snapshot
from app.main import create_app
from app.smtp.live_state import LiveState


ORIGIN = "http://testserver"


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )


def _login_and_change_password(client: TestClient, settings: Settings) -> None:
    login = client.post(
        "/admin/login",
        data={
            "username": settings.bootstrap_admin_username,
            "password": settings.bootstrap_admin_password,
        },
    )
    assert login.status_code == 200
    changed = client.post(
        "/admin/settings/password",
        headers={"Origin": ORIGIN},
        data={
            "current_password": settings.bootstrap_admin_password,
            "new_password": "admin-live-websocket-password",
            "confirm_password": "admin-live-websocket-password",
        },
    )
    assert changed.status_code == 200


def _assert_websocket_denied(
    client: TestClient,
    *,
    headers: dict[str, str] | Headers | None = None,
) -> None:
    with pytest.raises(WebSocketDisconnect) as exc_info:
        with client.websocket_connect(
            "/api/v1/admin/live/smtp/ws",
            headers=headers or {},
        ) as websocket:
            websocket.receive_json()
    assert exc_info.value.code == 1008


def test_admin_live_websocket_uses_session_origin_and_resumable_cursor(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _login_and_change_password(client, settings)
        runtime = app.state.runtime
        _, cursor = runtime.live_state.snapshot_state()

        with client.websocket_connect(
            f"/api/v1/admin/live/smtp/ws?after_cursor={cursor}",
            headers={"Origin": ORIGIN},
        ) as websocket:
            asyncio.run(
                runtime.live_state.publish(
                    {
                        "type": "queued",
                        "session_id": "smtp-ws-session",
                        "message_id": "msg-ws-session",
                        "ts": "2026-08-17T00:00:00Z",
                    }
                )
            )
            first_payload = websocket.receive_json()

        asyncio.run(
            runtime.live_state.publish(
                {
                    "type": "queued",
                    "session_id": "smtp-ws-resumed",
                    "message_id": "msg-ws-resumed",
                    "ts": "2026-08-17T00:00:01Z",
                }
            )
        )
        with client.websocket_connect(
            "/api/v1/admin/live/smtp/ws"
            f"?after_cursor={first_payload['cursor']}",
            headers={"Origin": ORIGIN},
        ) as websocket:
            resumed_payload = websocket.receive_json()

        assert first_payload["type"] == "queued"
        assert first_payload["session_id"] == "smtp-ws-session"
        assert first_payload["message_id"] == "msg-ws-session"
        assert first_payload["cursor"].startswith(f"{runtime.live_state.generation}:")
        assert resumed_payload["type"] == "queued"
        assert resumed_payload["session_id"] == "smtp-ws-resumed"
        assert resumed_payload["message_id"] == "msg-ws-resumed"
        assert int(resumed_payload["cursor"].rsplit(":", 1)[1]) > int(
            first_payload["cursor"].rsplit(":", 1)[1]
        )


def test_admin_live_websocket_rejects_missing_cross_origin_and_unready_sessions(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _assert_websocket_denied(client, headers={"Origin": ORIGIN})

        login = client.post(
            "/admin/login",
            data={
                "username": settings.bootstrap_admin_username,
                "password": settings.bootstrap_admin_password,
            },
        )
        assert login.status_code == 200
        _assert_websocket_denied(client, headers={"Origin": ORIGIN})

        changed = client.post(
            "/admin/settings/password",
            headers={"Origin": ORIGIN},
            data={
                "current_password": settings.bootstrap_admin_password,
                "new_password": "admin-live-websocket-password",
                "confirm_password": "admin-live-websocket-password",
            },
        )
        assert changed.status_code == 200
        _assert_websocket_denied(client)
        _assert_websocket_denied(client, headers={"Origin": "https://attacker.invalid"})
        for malformed_origin in (
            "null",
            "ws://testserver",
            "http://testserver/path",
            "http://testserver?query=1",
        ):
            _assert_websocket_denied(
                client,
                headers={"Origin": malformed_origin},
            )
        _assert_websocket_denied(
            client,
            headers=Headers([("Origin", ORIGIN), ("Origin", ORIGIN)]),
        )

        cookie_name = settings.session_cookie_name
        revoked_token = client.cookies.get(cookie_name)
        assert revoked_token is not None
        logout = client.post(
            "/admin/logout",
            headers={"Origin": ORIGIN},
            follow_redirects=False,
        )
        assert logout.status_code == 303
        client.cookies.set(cookie_name, revoked_token)
        _assert_websocket_denied(client, headers={"Origin": ORIGIN})
        client.cookies.set(cookie_name, "forged-admin-session")
        _assert_websocket_denied(client, headers={"Origin": ORIGIN})


@pytest.mark.parametrize("frame_type", ["text", "bytes"])
def test_admin_live_websocket_rejects_client_application_frames(
    tmp_path: Path,
    frame_type: str,
) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _login_and_change_password(client, settings)
        _, cursor = app.state.runtime.live_state.snapshot_state()
        with client.websocket_connect(
            f"/api/v1/admin/live/smtp/ws?after_cursor={cursor}",
            headers={"Origin": ORIGIN},
        ) as websocket:
            if frame_type == "text":
                websocket.send_text("unexpected-client-frame")
            else:
                websocket.send_bytes(b"unexpected-client-frame")
            with pytest.raises(WebSocketDisconnect) as exc_info:
                websocket.receive_json()
        assert exc_info.value.code == 1008


def test_admin_live_websocket_reports_gap_then_resumes(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _login_and_change_password(client, settings)
        runtime = app.state.runtime
        runtime.live_state = LiveState(max_events=2)
        generation = runtime.live_state.generation
        for index in range(3):
            asyncio.run(
                runtime.live_state.publish(
                    {
                        "type": "queued",
                        "session_id": f"smtp-gap-{index}",
                        "ts": "2026-08-17T00:00:00Z",
                    }
                )
            )

        with client.websocket_connect(
            f"/api/v1/admin/live/smtp/ws?after_cursor={generation}:0",
            headers={"Origin": ORIGIN},
        ) as websocket:
            gap = websocket.receive_json()
            first_resumed = websocket.receive_json()
            latest_resumed = websocket.receive_json()

        assert gap["type"] == "gap"
        assert gap["reason"] == "ring_overrun"
        assert first_resumed["type"] == "queued"
        assert first_resumed["session_id"] == "smtp-gap-1"
        assert latest_resumed["type"] == "queued"
        assert latest_resumed["session_id"] == "smtp-gap-2"
        assert latest_resumed["cursor"].endswith(":3")


def test_admin_live_websocket_supports_header_api_key_but_rejects_query_credentials(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        runtime = app.state.runtime
        assert client.portal is not None
        key = client.portal.call(
            partial(
                runtime.api_keys.create_key,
                name="admin-live-websocket-key",
                kind="admin",
                scopes=["live.read"],
                domain_ids=[],
                domain_grant_mode="all",
                mailbox_patterns=[],
                allowed_ip_cidrs=["127.0.0.1/32"],
            )
        )
        _, cursor = runtime.live_state.snapshot_state()
        with client.websocket_connect(
            f"/api/v1/admin/live/smtp/ws?after_cursor={cursor}",
            headers={"X-API-Key": key["plain_text"]},
        ) as websocket:
            asyncio.run(
                runtime.live_state.publish(
                    {
                        "type": "connect",
                        "session_id": "smtp-api-key",
                        "ts": "2026-08-17T00:00:00Z",
                    }
                )
            )
            assert websocket.receive_json()["session_id"] == "smtp-api-key"

        domain = client.portal.call(
            partial(runtime.create_domain, "selected-live.example")
        )
        selected_key = client.portal.call(
            partial(
                runtime.api_keys.create_key,
                name="selected-admin-live-websocket-key",
                kind="admin",
                scopes=["live.read"],
                domain_ids=[domain["id"]],
                domain_grant_mode="selected",
                mailbox_patterns=[],
            )
        )
        missing_scope_key = client.portal.call(
            partial(
                runtime.api_keys.create_key,
                name="missing-scope-admin-live-websocket-key",
                kind="admin",
                scopes=["system.read"],
                domain_ids=[],
                domain_grant_mode="all",
                mailbox_patterns=[],
            )
        )
        denied_ip_key = client.portal.call(
            partial(
                runtime.api_keys.create_key,
                name="denied-ip-admin-live-websocket-key",
                kind="admin",
                scopes=["live.read"],
                domain_ids=[],
                domain_grant_mode="all",
                mailbox_patterns=[],
                allowed_ip_cidrs=["203.0.113.0/24"],
            )
        )
        _assert_websocket_denied(
            client,
            headers={"X-API-Key": selected_key["plain_text"]},
        )
        _assert_websocket_denied(
            client,
            headers={"X-API-Key": missing_scope_key["plain_text"]},
        )
        _assert_websocket_denied(
            client,
            headers={"X-API-Key": denied_ip_key["plain_text"]},
        )
        _assert_websocket_denied(
            client,
            headers=Headers([
                ("X-API-Key", key["plain_text"]),
                ("X-API-Key", key["plain_text"]),
            ]),
        )
        with pytest.raises(WebSocketDisconnect) as exc_info:
            with client.websocket_connect(
                f"/api/v1/admin/live/smtp/ws?api_key={key['plain_text']}",
                headers={"Origin": ORIGIN},
            ) as websocket:
                websocket.receive_json()
        assert exc_info.value.code == 1008


def test_admin_live_websocket_receives_committed_delivery_from_sqlite_outbox(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _login_and_change_password(client, settings)
        runtime = app.state.runtime
        assert client.portal is not None
        client.portal.call(partial(runtime.create_domain, "outbox-live.example"))
        _, cursor = runtime.live_state.snapshot_state()

        with client.websocket_connect(
            f"/api/v1/admin/live/smtp/ws?after_cursor={cursor}",
            headers={"Origin": ORIGIN},
        ) as websocket:
            queued = client.portal.call(
                partial(
                    runtime.accept_message,
                    rcpt_tos=["box@outbox-live.example"],
                    envelope_from="sender@example.test",
                    content=(
                        b"From: sender@example.test\r\n"
                        b"To: box@outbox-live.example\r\n"
                        b"Subject: outbox live\r\n"
                        b"\r\n"
                        b"committed delivery\r\n"
                    ),
                    smtp_session_id="smtp-outbox-live",
                )
            )
            payload = websocket.receive_json()

        assert queued.startswith("250 queued as ")
        assert payload["type"] == "delivery_committed"
        assert payload["session_id"] == "smtp-outbox-live"
        assert payload["message_id"] == queued.removeprefix("250 queued as ")
        assert payload["rcpt_to"] == "box@outbox-live.example"
        assert payload["mail_from"] == "sender@example.test"
        assert payload["source"] == "committed_outbox"
        assert payload["cursor"].startswith(f"{runtime.live_state.generation}:")


def test_admin_live_websocket_closes_event_iterator_after_client_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings(tmp_path)
    app = create_app(settings=settings)
    iterator_closed = threading.Event()

    async def stalled_event_stream(*_args, **_kwargs):
        try:
            while True:
                await asyncio.sleep(60)
                yield {"type": "cursor", "cursor": "unused:0"}
        finally:
            iterator_closed.set()

    monkeypatch.setattr(admin_api, "iter_smtp_live_events", stalled_event_stream)
    with TestClient(
        app,
        base_url=ORIGIN,
        client=("127.0.0.1", 50000),
    ) as client:
        _login_and_change_password(client, settings)
        with client.websocket_connect(
            "/api/v1/admin/live/smtp/ws?after_cursor=unused:0",
            headers={"Origin": ORIGIN},
        ) as websocket:
            websocket.send_text("close-the-server-only-stream")
            with pytest.raises(WebSocketDisconnect) as exc_info:
                websocket.receive_json()
        assert exc_info.value.code == 1008

        assert iterator_closed.wait(timeout=1)


@pytest.mark.asyncio
async def test_admin_live_iterator_maps_committed_delivery_and_advances_ignored_cursor(
    runtime,
) -> None:
    _, cursor = runtime.live_state.snapshot_state()
    stream = iter_smtp_live_events(runtime, after_cursor=cursor, poll_interval=0.01)
    try:
        await runtime.live_state.publish(
            {
                "type": "mailbox_delivery",
                "session_id": "smtp-cpp",
                "delivery_id": "dlv-cpp",
                "message_id": "msg-cpp",
                "rcpt_to": "box@example.test",
                "mail_from": "sender@example.test",
                "parse_status": "parsed",
                "ts": "2026-08-17T00:00:00Z",
            }
        )
        committed = await asyncio.wait_for(anext(stream), timeout=0.2)
        await runtime.live_state.publish(
            {
                "type": "mailbox_delivery_updated",
                "delivery_id": "dlv-cpp",
                "message_id": "msg-cpp",
                "ts": "2026-08-17T00:00:01Z",
            }
        )
        cursor_only = await asyncio.wait_for(anext(stream), timeout=0.2)
    finally:
        await stream.aclose()

    assert committed == {
        "type": "delivery_committed",
        "session_id": "smtp-cpp",
        "delivery_id": "dlv-cpp",
        "message_id": "msg-cpp",
        "rcpt_to": "box@example.test",
        "mail_from": "sender@example.test",
        "parse_status": "parsed",
        "ts": "2026-08-17T00:00:00Z",
        "source": "committed_outbox",
        "cursor": committed["cursor"],
    }
    assert cursor_only["type"] == "cursor"
    assert int(cursor_only["cursor"].rsplit(":", 1)[1]) > int(
        committed["cursor"].rsplit(":", 1)[1]
    )


@pytest.mark.asyncio
async def test_admin_live_snapshot_filters_public_mailbox_internal_events(runtime) -> None:
    await runtime.live_state.publish(
        {
            "type": "mailbox_delivery_updated",
            "delivery_id": "dlv-internal",
            "message_id": "msg-internal",
        }
    )

    assert smtp_live_snapshot(runtime) == []


@pytest.mark.asyncio
async def test_admin_live_iterator_stops_quietly_when_cancelled() -> None:
    state = LiveState()
    runtime = type("Runtime", (), {"live_state": state})()
    _, cursor = state.snapshot_state()
    async def consume() -> None:
        async for _event in iter_smtp_live_events(
            runtime,
            poll_interval=60,
            after_cursor=cursor,
        ):
            pass

    pending = asyncio.create_task(consume())
    await asyncio.sleep(0)
    pending.cancel()
    await asyncio.wait_for(pending, timeout=1)
