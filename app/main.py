from __future__ import annotations

import asyncio
import ipaddress
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.datastructures import MutableHeaders

from app.config import Settings, default_settings
from app.auth.sessions import AuthenticationOverloadedError
from app.db.writer import DatabaseWriterOverloadedError
from app.db.read_pool import SQLiteReadPoolOverloadedError, SQLiteReadPoolPausedError
from app.http.api_v2 import router as api_v2_router
from app.http.admin_views import router as admin_views_router
from app.http.admin_api import router as admin_api_router
from app.http.public_api import router as public_api_router
from app.http.system import router as system_router
from app.http.template_helpers import register_template_helpers
from app.http.public_views import router as public_views_router
from app.observability import HTTPObservabilityMiddleware, configure_logging, shutdown_logging
from app.runtime import RapidInboxRuntime
from app.smtp.server import SMTPServer


class HttpConcurrencyLimitMiddleware:
    """Apply a process-wide bound even when Uvicorn is launched manually."""

    def __init__(self, app, *, max_connections: int) -> None:
        self.app = app
        self.max_connections = int(max_connections)
        self._lock = threading.Lock()
        self._active_connections = 0

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        with self._lock:
            admitted = self._active_connections < self.max_connections
            if admitted:
                self._active_connections += 1
        if not admitted:
            if scope.get("type") == "websocket":
                await send(
                    {
                        "type": "websocket.close",
                        "code": 1013,
                        "reason": "HTTP connection capacity exceeded",
                    }
                )
            else:
                response = JSONResponse(
                    {"detail": "HTTP connection capacity exceeded"},
                    status_code=503,
                    headers={"Retry-After": "1", "Connection": "close"},
                )
                await response(scope, receive, send)
            return
        try:
            await self.app(scope, receive, send)
        finally:
            with self._lock:
                self._active_connections -= 1


class LiveConnectionLimitMiddleware:
    """Bound administrator/public WebSockets and the deprecated SSE stream."""

    def __init__(self, app, *, max_connections: int) -> None:
        self.app = app
        self.max_connections = int(max_connections)
        self._lock = threading.Lock()
        self._active_connections = 0

    @staticmethod
    def _is_live_connection(scope) -> bool:
        scope_type = scope.get("type")
        path = _scope_route_path(scope)
        if scope_type == "http":
            return (
                str(scope.get("method") or "").upper() == "GET"
                and path == "/api/v1/admin/live/smtp/stream"
            )
        if scope_type != "websocket":
            return False
        return path == "/api/v1/admin/live/smtp/ws" or (
            path.startswith("/mail/") and path.endswith("/ws")
        )

    async def __call__(self, scope, receive, send) -> None:
        if not self._is_live_connection(scope):
            await self.app(scope, receive, send)
            return

        with self._lock:
            admitted = self._active_connections < self.max_connections
            if admitted:
                self._active_connections += 1
        if not admitted:
            if scope.get("type") == "websocket":
                await send(
                    {
                        "type": "websocket.close",
                        "code": 1013,
                        "reason": "live connection capacity exceeded",
                    }
                )
            else:
                response = JSONResponse(
                    {"detail": "live connection capacity exceeded"},
                    status_code=503,
                    headers={
                        "Retry-After": "1",
                        "Cache-Control": "private, no-store",
                    },
                )
                await response(scope, receive, send)
            return

        try:
            await self.app(scope, receive, send)
        finally:
            with self._lock:
                self._active_connections -= 1


class RequestBodyLimitMiddleware:
    """Bound HTTP control-plane bodies, including chunked requests.

    Rapid Inbox does not upload messages over HTTP, so buffering the small
    JSON/form control body before routing gives deterministic memory usage and
    closes the Content-Length/chunked bypass gap. SMTP DATA has its own,
    independently configured streaming limit.
    """

    MAX_BODY_CHUNKS = 1024

    def __init__(
        self,
        app,
        *,
        max_body_bytes: int,
        body_timeout_seconds: int,
        body_memory_budget_bytes: int,
    ) -> None:
        self.app = app
        self.max_body_bytes = int(max_body_bytes)
        self.body_timeout_seconds = int(body_timeout_seconds)
        self.body_memory_budget_bytes = int(body_memory_budget_bytes)
        self._body_budget_lock = threading.Lock()
        self._reserved_body_bytes = 0

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        content_lengths = [
            value.strip()
            for name, value in scope.get("headers", ())
            if name.lower() == b"content-length"
        ]
        invalid_content_length = len(content_lengths) > 1 or (
            bool(content_lengths) and not content_lengths[0].isdigit()
        )
        declared_too_large = (
            len(content_lengths) == 1
            and content_lengths[0].isdigit()
            and (
                len(content_lengths[0]) > 20
                or int(content_lengths[0]) > self.max_body_bytes
            )
        )
        if invalid_content_length or declared_too_large:
            status_code = 413 if declared_too_large else 400
            detail = "request body too large" if status_code == 413 else "invalid content length"
            response = JSONResponse(
                {"detail": detail},
                status_code=status_code,
                headers={"Connection": "close"},
            )
            await response(scope, receive, send)
            return

        body = bytearray()
        body_chunks = 0
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.body_timeout_seconds
        reserved_bytes = 0
        try:
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    response = JSONResponse(
                        {"detail": "request body timeout"},
                        status_code=408,
                        headers={"Connection": "close"},
                    )
                    await response(scope, receive, send)
                    return
                try:
                    message = await asyncio.wait_for(receive(), timeout=remaining)
                except asyncio.TimeoutError:
                    response = JSONResponse(
                        {"detail": "request body timeout"},
                        status_code=408,
                        headers={"Connection": "close"},
                    )
                    await response(scope, receive, send)
                    return
                if message.get("type") == "http.disconnect":
                    return
                if message.get("type") != "http.request":
                    continue
                body_chunks += 1
                if body_chunks > self.MAX_BODY_CHUNKS:
                    response = JSONResponse(
                        {"detail": "too many request body chunks"},
                        status_code=413,
                        headers={"Connection": "close"},
                    )
                    await response(scope, receive, send)
                    return
                chunk = message.get("body", b"")
                if len(body) + len(chunk) > self.max_body_bytes:
                    response = JSONResponse(
                        {"detail": "request body too large"},
                        status_code=413,
                        headers={"Connection": "close"},
                    )
                    await response(scope, receive, send)
                    return
                with self._body_budget_lock:
                    admitted = (
                        self._reserved_body_bytes + len(chunk)
                        <= self.body_memory_budget_bytes
                    )
                    if admitted:
                        self._reserved_body_bytes += len(chunk)
                        reserved_bytes += len(chunk)
                if not admitted:
                    response = JSONResponse(
                        {"detail": "request body capacity exceeded"},
                        status_code=503,
                        headers={"Connection": "close", "Retry-After": "1"},
                    )
                    await response(scope, receive, send)
                    return
                body.extend(chunk)
                if not message.get("more_body", False):
                    break

            replay_body = bytes(body)
            body.clear()
            replayed = False

            async def replay_receive():
                nonlocal replayed
                if not replayed:
                    replayed = True
                    return {"type": "http.request", "body": replay_body, "more_body": False}
                return await receive()

            await self.app(scope, replay_receive, send)
        finally:
            if reserved_bytes:
                with self._body_budget_lock:
                    self._reserved_body_bytes -= reserved_bytes


class RequestSecurityMiddleware:
    """Apply HTTP guards and response headers on the direct ASGI path.

    Starlette's function-style ``@app.middleware("http")`` adapter proxies
    every response through an AnyIO task group and in-memory object stream.
    That overhead is especially expensive for the small JSON responses that
    dominate the control plane. This middleware preserves the same guards and
    header policy while forwarding ASGI messages directly, including streamed
    file responses.
    """

    def __init__(self, app, *, settings: Settings, runtime: RapidInboxRuntime) -> None:
        self.app = app
        self.settings = settings
        self.runtime = runtime

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive=receive)

        async def send_with_security_headers(message) -> None:
            if message.get("type") == "http.response.start":
                _apply_security_headers(request, MutableHeaders(scope=message))
            await send(message)

        if not _admin_form_origin_is_allowed(request):
            response = JSONResponse(
                {"detail": "cross-origin admin form submission rejected"},
                status_code=403,
                headers={"Connection": "close"},
            )
            await response(scope, receive, send_with_security_headers)
            return

        if not self.settings.externally_bound() and _is_remote_client(request):
            findings = await asyncio.to_thread(self.runtime.external_request_security_findings)
            if findings:
                response = JSONResponse(
                    {"detail": "external access is disabled until security settings are configured"},
                    status_code=503,
                )
                await response(scope, receive, send_with_security_headers)
                return
        await self.app(scope, receive, send_with_security_headers)


def _request_scheme(request: Request) -> str:
    # Trust only the ASGI scope. Uvicorn updates it after validating the
    # connecting proxy against forwarded_allow_ips; a raw header is spoofable.
    return request.url.scheme.lower()


def _normalized_http_origin(value: str) -> tuple[str, str, int] | None:
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
    if port is None:
        port = 443 if scheme == "https" else 80
    return scheme, hostname.lower(), port


def _scope_route_path(scope) -> str:
    path = str(scope.get("path") or "")
    root_path = str(scope.get("root_path") or "")
    if not root_path or not path.startswith(root_path):
        return path
    if path == root_path:
        return ""
    if len(path) > len(root_path) and path[len(root_path)] == "/":
        return path[len(root_path) :]
    return path


def _admin_form_origin_is_allowed(request: Request) -> bool:
    path = _scope_route_path(request.scope)
    if request.method.upper() not in {"POST", "PUT", "PATCH", "DELETE"}:
        return True
    if path != "/admin" and not path.startswith("/admin/"):
        return True

    origins = request.headers.getlist("origin")
    referers = request.headers.getlist("referer")
    if len(origins) > 1 or len(referers) > 1:
        return False
    candidates = origins + referers
    if not candidates:
        # Preserve non-browser bootstrap/login clients. Authenticated admin
        # writes require browser origin evidence in addition to SameSite=Lax.
        return path == "/admin/login"

    expected = _normalized_http_origin(str(request.base_url))
    return expected is not None and all(
        _normalized_http_origin(candidate) == expected for candidate in candidates
    )


def _apply_security_headers(request: Request, headers: MutableHeaders) -> None:
    headers.setdefault("X-Content-Type-Options", "nosniff")
    headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    headers.setdefault("Referrer-Policy", "same-origin")
    headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
    # Mail bodies, API responses and the administration UI can all contain
    # credentials or private message data.  Applying this centrally also
    # covers error responses, redirects and legacy v1 routes that do not use
    # the standard Authorization header (and therefore are not treated
    # specially by shared caches).
    path = _scope_route_path(request.scope)
    if path.startswith(("/admin", "/api/", "/mail/")):
        headers["Cache-Control"] = "private, no-store"
    if _request_scheme(request) == "https":
        headers.setdefault("Strict-Transport-Security", "max-age=31536000; includeSubDomains")


def _is_remote_client(request: Request) -> bool:
    if request.client is None:
        return True
    try:
        return not ipaddress.ip_address(request.client.host).is_loopback
    except ValueError:
        return True


def create_app(*, settings: Settings | None = None, embed_smtp: bool = False) -> FastAPI:
    resolved_settings = settings or default_settings(Path.cwd())
    runtime = RapidInboxRuntime(resolved_settings)
    app_dir = Path(__file__).resolve().parent
    templates = Jinja2Templates(directory=str(app_dir / "templates"))
    register_template_helpers(templates)

    async def _shutdown(runtime: RapidInboxRuntime, smtp_server: SMTPServer | None) -> None:
        if smtp_server is not None:
            await asyncio.to_thread(smtp_server.stop)
        await runtime.stop()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(resolved_settings, metrics=runtime.observability.metrics)
        app.state.settings = resolved_settings
        app.state.runtime = runtime
        app.state.observability = runtime.observability
        app.state.templates = templates
        smtp_server: SMTPServer | None = None
        try:
            await runtime.start()
            if embed_smtp:
                smtp_server = SMTPServer(runtime)
                smtp_server.start()
            yield
        finally:
            try:
                shutdown_task = asyncio.create_task(_shutdown(runtime, smtp_server))
                while True:
                    try:
                        await asyncio.shield(shutdown_task)
                        break
                    except asyncio.CancelledError:
                        if shutdown_task.done():
                            await shutdown_task
                            break
                        continue
            finally:
                shutdown_logging()

    app = FastAPI(title="Rapid Inbox", lifespan=lifespan)
    app.state.settings = resolved_settings
    app.state.runtime = runtime
    app.state.observability = runtime.observability
    app.state.templates = templates

    @app.exception_handler(DatabaseWriterOverloadedError)
    async def database_writer_overloaded(_request: Request, _exc: DatabaseWriterOverloadedError):
        return JSONResponse(
            {"detail": "database write capacity exceeded"},
            status_code=503,
            headers={"Retry-After": "1", "Cache-Control": "private, no-store"},
        )

    @app.exception_handler(SQLiteReadPoolOverloadedError)
    async def database_reader_overloaded(_request: Request, _exc: SQLiteReadPoolOverloadedError):
        return JSONResponse(
            {"detail": "database read capacity exceeded"},
            status_code=503,
            headers={"Retry-After": "1", "Cache-Control": "private, no-store"},
        )

    @app.exception_handler(SQLiteReadPoolPausedError)
    async def database_reader_paused(_request: Request, _exc: SQLiteReadPoolPausedError):
        return JSONResponse(
            {"detail": "database reads temporarily unavailable"},
            status_code=503,
            headers={"Retry-After": "1", "Cache-Control": "private, no-store"},
        )

    @app.exception_handler(AuthenticationOverloadedError)
    async def authentication_overloaded(_request: Request, _exc: AuthenticationOverloadedError):
        return JSONResponse(
            {"detail": "authentication capacity exceeded"},
            status_code=503,
            headers={"Retry-After": "1", "Cache-Control": "private, no-store"},
        )
    app.add_middleware(
        RequestBodyLimitMiddleware,
        max_body_bytes=resolved_settings.http_max_request_body_bytes,
        body_timeout_seconds=resolved_settings.http_request_body_timeout_seconds,
        body_memory_budget_bytes=resolved_settings.http_body_memory_budget_bytes,
    )
    app.add_middleware(
        LiveConnectionLimitMiddleware,
        max_connections=resolved_settings.http_live_connection_limit,
    )
    app.add_middleware(
        HttpConcurrencyLimitMiddleware,
        max_connections=resolved_settings.http_concurrency_limit,
    )

    app.add_middleware(
        RequestSecurityMiddleware,
        settings=resolved_settings,
        runtime=runtime,
    )

    app.add_middleware(HTTPObservabilityMiddleware, observability=runtime.observability)

    app.mount("/static", StaticFiles(directory=str(app_dir / "static")), name="static")
    app.include_router(system_router)
    app.include_router(api_v2_router)
    app.include_router(public_views_router)
    app.include_router(public_api_router)
    app.include_router(admin_views_router)
    app.include_router(admin_api_router)
    return app


app = create_app()
