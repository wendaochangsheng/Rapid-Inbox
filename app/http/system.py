from __future__ import annotations

import hmac
import logging

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse

from app.observability import application_version
from app.services.dashboard import get_dashboard_service


router = APIRouter(include_in_schema=False)
NO_STORE_HEADERS = {"Cache-Control": "no-store"}
_logger = logging.getLogger("rapid_inbox.metrics")


@router.get("/health/live")
async def health_live() -> JSONResponse:
    return JSONResponse({"status": "alive"}, headers=NO_STORE_HEADERS)


@router.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    runtime = request.app.state.runtime
    result = await request.app.state.observability.readiness.check(runtime)
    status_code = status.HTTP_200_OK if result["status"] == "ready" else status.HTTP_503_SERVICE_UNAVAILABLE
    return JSONResponse(result, status_code=status_code, headers=NO_STORE_HEADERS)


@router.get("/version")
async def version() -> JSONResponse:
    return JSONResponse(
        {
            "name": "rapid-inbox",
            "version": application_version(),
            "api_version": "v2",
            "supported_api_versions": ["v1", "v2"],
        },
        headers=NO_STORE_HEADERS,
    )


def _metrics_credential(authorization: str | None, x_metrics_token: str | None) -> str | None:
    if x_metrics_token:
        return x_metrics_token
    if not authorization:
        return None
    scheme, separator, credential = authorization.partition(" ")
    if not separator or scheme.lower() != "bearer" or not credential:
        return None
    return credential


@router.get("/metrics")
async def metrics(
    request: Request,
    authorization: str | None = Header(default=None),
    x_metrics_token: str | None = Header(default=None, alias="X-Metrics-Token"),
) -> PlainTextResponse:
    settings = request.app.state.settings
    if not settings.metrics_enabled:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="not found",
            headers=NO_STORE_HEADERS,
        )

    expected_token = settings.metrics_token
    if expected_token:
        candidate = _metrics_credential(authorization, x_metrics_token)
        if candidate is None or not hmac.compare_digest(candidate, expected_token):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="invalid metrics token",
                headers={**NO_STORE_HEADERS, "WWW-Authenticate": "Bearer"},
            )

    observability = request.app.state.observability
    try:
        operational_snapshot = await get_dashboard_service(request.app).snapshot()
    except Exception:
        operational_snapshot = None
        _logger.exception(
            "operational metrics snapshot unavailable",
            extra={"event": "metrics_snapshot_failure"},
        )
    content = observability.metrics.render(
        started_monotonic=observability.started_monotonic,
        operational_snapshot=operational_snapshot,
    )
    return PlainTextResponse(
        content,
        media_type="text/plain; version=0.0.4; charset=utf-8",
        headers=NO_STORE_HEADERS,
    )


__all__ = ["router"]
