from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncIterator
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from app.config import Settings
from app.db.connection import connect_database
from app.main import create_app
from app.runtime import RapidInboxRuntime


@dataclass(slots=True)
class SeededMessage:
    message_id: str
    delivery_id: str
    public_api_key: str


@pytest.fixture
def sample_email_bytes() -> bytes:
    return (
        b"From: Sender <sender@example.com>\r\n"
        b"To: Foo <foo@adb.com>\r\n"
        b"Subject: Hello Rapid Inbox\r\n"
        b"Message-ID: <hello@example.com>\r\n"
        b"Date: Sat, 18 Apr 2026 20:00:00 +0000\r\n"
        b"MIME-Version: 1.0\r\n"
        b"Content-Type: multipart/alternative; boundary=boundary42\r\n"
        b"\r\n"
        b"--boundary42\r\n"
        b"Content-Type: text/plain; charset=utf-8\r\n"
        b"\r\n"
        b"Hello from tests.\r\n"
        b"\r\n"
        b"--boundary42\r\n"
        b"Content-Type: text/html; charset=utf-8\r\n"
        b"\r\n"
        b"<html><body><p>Hello from tests.</p></body></html>\r\n"
        b"\r\n"
        b"--boundary42--\r\n"
    )


@pytest_asyncio.fixture
async def app_fixture(tmp_path) -> AsyncIterator[tuple[FastAPI, RapidInboxRuntime]]:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        yield app, app.state.runtime


@pytest_asyncio.fixture
async def runtime(app_fixture) -> RapidInboxRuntime:
    _, runtime = app_fixture
    return runtime


@pytest_asyncio.fixture
async def app_client(app_fixture) -> AsyncIterator[httpx.AsyncClient]:
    app, _ = app_fixture
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        headers={"Origin": "http://testserver"},
    ) as client:
        yield client


@pytest_asyncio.fixture
async def admin_client(app_client: httpx.AsyncClient, runtime: RapidInboxRuntime) -> httpx.AsyncClient:
    key = await runtime.api_keys.create_key(
        name="fixture-admin",
        kind="admin",
        scopes=[
            "public.read",
            "domains.write",
            "messages.write",
            "messages.read",
            "audit.read",
            "system.write",
            "system.read",
            "live.read",
            "mailboxes.write",
            "mailboxes.read",
            "api_keys.write",
            "api_keys.read",
            "smtp.read",
        ],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=[],
        # The shared test administrator represents an unrestricted root
        # credential. Bounded API-key managers may not delegate an unlimited
        # child, so make that root policy explicit instead of relying on the
        # ordinary 3600/min default.
        rate_limit_per_min=0,
        allow_query=True,
    )
    app_client.headers["X-API-Key"] = key["plain_text"]
    return app_client


@pytest_asyncio.fixture
async def seeded_message(runtime: RapidInboxRuntime, sample_email_bytes: bytes) -> SeededMessage:
    await runtime.create_domain("adb.com", public_web_enabled=True, public_api_enabled=True)
    await runtime.ensure_smtp_session(
        "smtp_fixture_1",
        SimpleNamespace(peer=("127.0.0.1", 2525), host_name="pytest", ssl=None),
    )
    key = await runtime.api_keys.create_key(
        name="fixture-public",
        kind="public",
        scopes=["public.read"],
        domain_ids=[],
        domain_grant_mode="all",
        mailbox_patterns=["foo@adb.com"],
    )
    await runtime.accept_message(
        rcpt_tos=["foo@adb.com"],
        envelope_from="sender@example.com",
        content=sample_email_bytes,
        smtp_session_id="smtp_fixture_1",
    )
    await runtime.drain_parser_queue()
    mailbox = await runtime.get_mailbox_view("foo@adb.com")
    return SeededMessage(
        message_id=mailbox["items"][0]["message_id"],
        delivery_id=mailbox["items"][0]["delivery_id"],
        public_api_key=key["plain_text"],
    )


__all__ = [
    "admin_client",
    "SeededMessage",
    "app_client",
    "app_fixture",
    "connect_database",
    "seeded_message",
    "runtime",
    "sample_email_bytes",
]
