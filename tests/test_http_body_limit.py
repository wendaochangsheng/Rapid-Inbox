from __future__ import annotations

import httpx
import pytest

from app.config import Settings, default_settings
from app.main import create_app


def test_http_body_limit_loads_from_environment(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("HTTP_MAX_REQUEST_BODY_BYTES", "12345")

    settings = default_settings(tmp_path)

    assert settings.http_max_request_body_bytes == 12345


@pytest.mark.parametrize("value", [0, 67_108_865, True])
def test_http_body_limit_rejects_invalid_configuration(tmp_path, value: object) -> None:
    with pytest.raises(ValueError, match="HTTP_MAX_REQUEST_BODY_BYTES"):
        Settings(
            storage_root=tmp_path / "storage",
            database_path=tmp_path / "storage" / "app.db",
            http_max_request_body_bytes=value,  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_http_body_limit_rejects_content_length_before_route(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        http_max_request_body_bytes=64,
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            headers={"Origin": "http://testserver"},
        ) as client:
            response = await client.post("/admin/login", content=b"x" * 65)

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["connection"] == "close"
    assert response.headers["x-request-id"]


@pytest.mark.asyncio
async def test_http_body_limit_rejects_chunked_body_without_content_length(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        http_max_request_body_bytes=64,
    )
    app = create_app(settings=settings)

    async def body_chunks():
        yield b"x" * 40
        yield b"y" * 25

    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            headers={"Origin": "http://testserver"},
        ) as client:
            response = await client.post("/admin/login", content=body_chunks())

    assert response.status_code == 413
    assert response.json() == {"detail": "request body too large"}
