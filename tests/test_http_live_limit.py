from __future__ import annotations

import asyncio

import pytest

from app.config import Settings
from app.main import (
    HttpConcurrencyLimitMiddleware,
    LiveConnectionLimitMiddleware,
    RequestBodyLimitMiddleware,
    RequestSecurityMiddleware,
)


def _http_scope(path: str, *, root_path: str = "") -> dict:
    return {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": root_path,
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("127.0.0.1", 8000),
    }


def _websocket_scope(path: str, *, root_path: str = "") -> dict:
    return {
        "type": "websocket",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "scheme": "ws",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": root_path,
        "headers": [],
        "client": ("127.0.0.1", 12346),
        "server": ("127.0.0.1", 8000),
        "subprotocols": [],
    }


@pytest.mark.asyncio
async def test_request_security_middleware_forwards_concurrent_streaming_responses(
    tmp_path,
) -> None:
    async def streaming_app(scope, _receive, send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"cache-control", b"public, max-age=60")],
            }
        )
        await send(
            {
                "type": "http.response.body",
                "body": scope["path"].encode("ascii"),
                "more_body": True,
            }
        )
        await asyncio.sleep(0)
        await send(
            {
                "type": "http.response.body",
                "body": b"-done",
                "more_body": False,
            }
        )

    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    middleware = RequestSecurityMiddleware(
        streaming_app,
        settings=settings,
        runtime=object(),
    )

    async def one_request(index: int) -> list[dict]:
        sent: list[dict] = []

        async def receive() -> dict:
            return {"type": "http.request", "body": b"", "more_body": False}

        async def send(message: dict) -> None:
            sent.append(message)

        await middleware(_http_scope(f"/api/v2/items/{index}"), receive, send)
        return sent

    responses = await asyncio.gather(*(one_request(index) for index in range(64)))

    for index, sent in enumerate(responses):
        assert [message["type"] for message in sent] == [
            "http.response.start",
            "http.response.body",
            "http.response.body",
        ]
        headers = dict(sent[0]["headers"])
        assert headers[b"cache-control"] == b"private, no-store"
        assert headers[b"x-content-type-options"] == b"nosniff"
        assert sent[1] == {
            "type": "http.response.body",
            "body": f"/api/v2/items/{index}".encode("ascii"),
            "more_body": True,
        }
        assert sent[2]["body"] == b"-done"


@pytest.mark.asyncio
async def test_request_security_headers_honor_root_path(tmp_path) -> None:
    async def public_cache_app(_scope, _receive, send) -> None:
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"cache-control", b"public, max-age=60")],
            }
        )
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    middleware = RequestSecurityMiddleware(
        public_cache_app,
        settings=settings,
        runtime=object(),
    )
    sent: list[dict] = []

    async def receive() -> dict:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict) -> None:
        sent.append(message)

    await middleware(
        _http_scope("/prefix/admin", root_path="/prefix"),
        receive,
        send,
    )

    headers = dict(sent[0]["headers"])
    assert headers[b"cache-control"] == b"private, no-store"


def test_live_connection_limit_classifies_routes_under_root_path() -> None:
    assert LiveConnectionLimitMiddleware._is_live_connection(
        _http_scope(
            "/prefix/api/v1/admin/live/smtp/stream",
            root_path="/prefix",
        )
    )
    assert LiveConnectionLimitMiddleware._is_live_connection(
        _websocket_scope(
            "/prefix/mail/box@example.test/ws",
            root_path="/prefix",
        )
    )
    assert not LiveConnectionLimitMiddleware._is_live_connection(
        _http_scope("/prefix/health/live", root_path="/prefix")
    )


@pytest.mark.asyncio
async def test_live_connection_limit_is_shared_by_sse_and_websocket() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_app(scope, _receive, send) -> None:
        entered.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok", "more_body": False})

    middleware = LiveConnectionLimitMiddleware(blocking_app, max_connections=1)

    async def receive() -> dict:
        return {"type": "http.disconnect"}

    first_messages: list[dict] = []

    async def send_first(message: dict) -> None:
        first_messages.append(message)

    first = asyncio.create_task(
        middleware(
            _http_scope("/api/v1/admin/live/smtp/stream"),
            receive,
            send_first,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    rejected_http: list[dict] = []

    async def send_rejected_http(message: dict) -> None:
        rejected_http.append(message)

    await middleware(
        _http_scope("/api/v1/admin/live/smtp/stream"),
        receive,
        send_rejected_http,
    )
    rejected_websocket: list[dict] = []

    async def send_rejected_websocket(message: dict) -> None:
        rejected_websocket.append(message)

    await middleware(
        _websocket_scope("/mail/box@example.test/ws"),
        receive,
        send_rejected_websocket,
    )

    assert rejected_http[0]["type"] == "http.response.start"
    assert rejected_http[0]["status"] == 503
    assert rejected_websocket == [
        {
            "type": "websocket.close",
            "code": 1013,
            "reason": "live connection capacity exceeded",
        }
    ]

    release.set()
    await asyncio.wait_for(first, timeout=1)
    assert first_messages[0]["status"] == 200
    assert middleware._active_connections == 0


def test_live_connection_limit_configuration_is_bounded(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    assert settings.http_live_connection_limit == 256

    with pytest.raises(ValueError, match="HTTP_LIVE_CONNECTION_LIMIT"):
        Settings(
            storage_root=tmp_path / "zero",
            database_path=tmp_path / "zero" / "app.db",
            http_live_connection_limit=0,
        )
    with pytest.raises(ValueError, match="HTTP_LIVE_CONNECTION_LIMIT"):
        Settings(
            storage_root=tmp_path / "too-many",
            database_path=tmp_path / "too-many" / "app.db",
            http_live_connection_limit=100_001,
        )


@pytest.mark.asyncio
async def test_http_concurrency_limit_rejects_excess_work() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_app(_scope, _receive, send) -> None:
        entered.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    middleware = HttpConcurrencyLimitMiddleware(blocking_app, max_connections=1)

    async def receive() -> dict:
        return {"type": "http.disconnect"}

    first_messages: list[dict] = []

    async def first_send(message: dict) -> None:
        first_messages.append(message)

    first = asyncio.create_task(middleware(_http_scope("/health/live"), receive, first_send))
    await asyncio.wait_for(entered.wait(), timeout=1)
    rejected: list[dict] = []

    async def rejected_send(message: dict) -> None:
        rejected.append(message)

    await middleware(_http_scope("/health/live"), receive, rejected_send)
    assert rejected[0]["status"] == 503

    release.set()
    await first
    assert first_messages[0]["status"] == 204


@pytest.mark.asyncio
async def test_request_body_middleware_bounds_chunk_count_and_replays_one_body() -> None:
    received_body: list[bytes] = []

    async def inner_app(_scope, receive, send) -> None:
        message = await receive()
        received_body.append(message["body"])
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    middleware = RequestBodyLimitMiddleware(
        inner_app,
        max_body_bytes=64,
        body_timeout_seconds=1,
        body_memory_budget_bytes=64,
    )
    middleware.MAX_BODY_CHUNKS = 2
    chunks = iter(
        [
            {"type": "http.request", "body": b"ab", "more_body": True},
            {"type": "http.request", "body": b"cd", "more_body": False},
        ]
    )

    async def receive() -> dict:
        return next(chunks)

    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    await middleware(_http_scope("/admin/login") | {"method": "POST"}, receive, send)
    assert received_body == [b"abcd"]
    assert sent[0]["status"] == 204

    too_many = iter(
        [
            {"type": "http.request", "body": b"", "more_body": True},
            {"type": "http.request", "body": b"", "more_body": True},
            {"type": "http.request", "body": b"", "more_body": False},
        ]
    )

    async def receive_too_many() -> dict:
        return next(too_many)

    rejected: list[dict] = []

    async def reject_send(message: dict) -> None:
        rejected.append(message)

    await middleware(
        _http_scope("/admin/login") | {"method": "POST"},
        receive_too_many,
        reject_send,
    )
    assert rejected[0]["status"] == 413


@pytest.mark.asyncio
async def test_request_body_memory_budget_is_shared_and_released() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_app(_scope, receive, send) -> None:
        await receive()
        entered.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 204, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    middleware = RequestBodyLimitMiddleware(
        blocking_app,
        max_body_bytes=4,
        body_timeout_seconds=1,
        body_memory_budget_bytes=4,
    )

    async def full_body() -> dict:
        return {"type": "http.request", "body": b"abcd", "more_body": False}

    async def send_first(_message: dict) -> None:
        return None

    first = asyncio.create_task(
        middleware(
            _http_scope("/admin/login") | {"method": "POST"},
            full_body,
            send_first,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)

    second_messages: list[dict] = []

    async def one_byte() -> dict:
        return {"type": "http.request", "body": b"x", "more_body": False}

    async def send_second(message: dict) -> None:
        second_messages.append(message)

    await middleware(
        _http_scope("/admin/login") | {"method": "POST"},
        one_byte,
        send_second,
    )
    assert second_messages[0]["status"] == 503

    release.set()
    await first
    assert middleware._reserved_body_bytes == 0
