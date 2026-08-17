from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.config import default_settings
from app.db.connection import connect_database
from app import http_runner
from app import main as app_main
from app.runtime import RapidInboxRuntime
from app import smtp_runner
from app.smtp.handler import RapidInboxHandler
from app.smtp.server import RapidInboxController, SMTPServer


@pytest.mark.asyncio
async def test_smtp_handler_accepts_allowed_domain_and_rejects_unknown(tmp_path, sample_email_bytes: bytes) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")

        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)

        allowed = await handler.handle_RCPT(None, session, envelope, "foo@adb.com", [])
        rejected = await handler.handle_RCPT(None, session, envelope, "foo@example.com", [])
        queued = await handler.handle_DATA(None, session, envelope)
        quit_response = await handler.handle_QUIT(None, session, envelope)
        await runtime.drain_parser_queue()
        mailbox = await runtime.get_mailbox_view("foo@adb.com")
        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    status,
                    message_count,
                    rcpt_accepted_count,
                    rcpt_rejected_count,
                    bytes_received,
                    last_mail_from,
                    last_rcpt_to_sample,
                    disconnect_at
                FROM smtp_sessions
                WHERE id = ?
                """,
                (session.rapid_inbox_session_id,),
            ).fetchone()

        assert allowed == "250 OK"
        assert rejected.startswith("550")
        assert queued.startswith("250 queued as ")
        assert quit_response.startswith("221")
        assert mailbox["items"][0]["parse_status"] == "parsed"
        assert row["status"] == "closed"
        assert row["message_count"] == 1
        assert row["rcpt_accepted_count"] == 1
        assert row["rcpt_rejected_count"] == 1
        assert row["bytes_received"] == len(sample_email_bytes)
        assert row["last_mail_from"] == "sender@example.com"
        assert row["last_rcpt_to_sample"] == "foo@example.com"
        assert row["disconnect_at"] is not None
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_releases_session_when_client_disconnects_without_quit(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = default_settings(tmp_path)
    settings.smtp_close_after_data = False
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")

        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)

        accepted = await handler.handle_RCPT(None, session, envelope, "foo@adb.com", [])
        queued = await handler.handle_DATA(None, session, envelope)
        assert accepted == "250 OK"
        assert queued.startswith("250 queued as ")
        assert runtime.active_smtp_connection_count() == 1

        await handler.handle_DISCONNECT(None, session, envelope, None)

        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT status, disconnect_at, close_reason
                FROM smtp_sessions
                WHERE id = ?
                """,
                (session.rapid_inbox_session_id,),
            ).fetchone()
            events = [
                str(event["event_type"])
                for event in connection.execute(
                    """
                    SELECT event_type
                    FROM smtp_events
                    WHERE session_id = ?
                    ORDER BY seq ASC
                    """,
                    (session.rapid_inbox_session_id,),
                ).fetchall()
            ]

        assert runtime.active_smtp_connection_count() == 0
        assert row["status"] == "closed"
        assert row["disconnect_at"] is not None
        assert row["close_reason"] == "connection lost"
        assert events[-1] == "disconnect"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_schedules_transport_close_after_successful_data(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    class FakeLoop:
        def __init__(self) -> None:
            self.delay = None
            self.callback = None

        def call_later(self, delay, callback):
            self.delay = delay
            self.callback = callback

    class FakeTransport:
        def __init__(self) -> None:
            self.closed = False

        def is_closing(self) -> bool:
            return self.closed

        def close(self) -> None:
            self.closed = True

    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)
    fake_loop = FakeLoop()
    fake_transport = FakeTransport()
    server = SimpleNamespace(loop=fake_loop, transport=fake_transport)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")

        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)

        await handler.handle_RCPT(server, session, envelope, "foo@adb.com", [])
        queued = await handler.handle_DATA(server, session, envelope)

        assert queued.startswith("250 queued as ")
        assert fake_loop.delay == 0.05
        assert fake_loop.callback is not None

        fake_loop.callback()

        assert fake_transport.closed is True
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_disconnect_close_wins_if_session_row_is_created_late(tmp_path) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        session = SimpleNamespace(
            rapid_inbox_session_id="smtp_disconnect_race",
            peer=("127.0.0.1", 2525),
            host_name="mx1.test",
            ssl=None,
        )
        envelope = SimpleNamespace(rcpt_tos=[], mail_from=None, content=b"")

        allowed, reason = await runtime.register_smtp_connection("smtp_disconnect_race", "127.0.0.1")
        assert allowed is True
        assert reason is None

        await handler.handle_DISCONNECT(None, session, envelope, None)
        await runtime.ensure_smtp_session(
            "smtp_disconnect_race",
            session,
            last_rcpt_to="late@example.com",
        )

        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT status, disconnect_at, close_reason, last_rcpt_to_sample
                FROM smtp_sessions
                WHERE id = 'smtp_disconnect_race'
                """
            ).fetchone()

        assert row["status"] == "closed"
        assert row["disconnect_at"] is not None
        assert row["close_reason"] == "connection lost"
        assert row["last_rcpt_to_sample"] == "late@example.com"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_disconnect_closes_existing_open_row_even_if_connection_was_not_active(tmp_path) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        with connect_database(settings.database_path) as connection:
            connection.execute(
                """
                INSERT INTO smtp_sessions (
                    id,
                    remote_ip,
                    status,
                    connect_at,
                    last_command_at
                ) VALUES ('smtp_open_without_active_connection', '127.0.0.1', 'open', ?, ?)
                """,
                ("2026-04-18T20:00:00Z", "2026-04-18T20:00:01Z"),
            )

        session = SimpleNamespace(
            rapid_inbox_session_id="smtp_open_without_active_connection",
            peer=("127.0.0.1", 2525),
            host_name="mx1.test",
            ssl=None,
        )
        envelope = SimpleNamespace(rcpt_tos=[], mail_from=None, content=b"")

        await handler.handle_DISCONNECT(None, session, envelope, None)

        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT status, disconnect_at, close_reason
                FROM smtp_sessions
                WHERE id = 'smtp_open_without_active_connection'
                """
            ).fetchone()

        assert row["status"] == "closed"
        assert row["disconnect_at"] is not None
        assert row["close_reason"] == "connection lost"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_persists_connect_and_disconnect_events(tmp_path) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=b"")

        connected = await handler.handle_CONNECT(None, session, envelope, "127.0.0.1", 2525)
        quit_response = await handler.handle_QUIT(None, session, envelope)

        with connect_database(settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT event_type
                FROM smtp_events
                WHERE session_id = ?
                ORDER BY seq ASC
                """,
                (session.rapid_inbox_session_id,),
            ).fetchall()

        assert connected is None
        assert quit_response.startswith("221")
        assert [row["event_type"] for row in rows] == ["connect", "disconnect"]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_quit_without_prior_activity_does_not_create_open_session(tmp_path) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from=None, content=b"")

        quit_response = await handler.handle_QUIT(None, session, envelope)

        with connect_database(settings.database_path) as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM smtp_sessions WHERE id = ?",
                (session.rapid_inbox_session_id,),
            ).fetchone()

        assert quit_response.startswith("221")
        assert row["count"] == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_rejects_extra_recipients_after_live_limit_update(
    tmp_path,
    sample_email_bytes: bytes,
) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        await runtime.create_domain("adb.com")
        await runtime.update_settings({"max_recipients_per_message": 1})

        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(rcpt_tos=[], mail_from="sender@example.com", content=sample_email_bytes)

        first = await handler.handle_RCPT(None, session, envelope, "foo@adb.com", [])
        second = await handler.handle_RCPT(None, session, envelope, "bar@adb.com", [])
        queued = await handler.handle_DATA(None, session, envelope)
        await runtime.drain_parser_queue()
        mailbox = await runtime.get_mailbox_view("foo@adb.com")

        assert first == "250 OK"
        assert second == "552 too many recipients"
        assert queued.startswith("250 queued as ")
        assert mailbox["items"][0]["parse_status"] == "parsed"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_smtp_handler_applies_smallest_domain_message_size_limit(
    tmp_path,
) -> None:
    settings = default_settings(tmp_path)
    runtime = RapidInboxRuntime(settings)
    handler = RapidInboxHandler(runtime)

    await runtime.start()
    try:
        await runtime.create_domain("large.adb.com", max_message_size_bytes=100)
        await runtime.create_domain("small.adb.com", max_message_size_bytes=10)
        await runtime.update_settings({"max_message_size_bytes": 1000})

        session = SimpleNamespace(peer=("127.0.0.1", 2525), host_name="mx1.test", ssl=None)
        envelope = SimpleNamespace(
            rcpt_tos=[],
            mail_from="sender@example.com",
            content=b"0123456789abcdefghij",
        )

        first = await handler.handle_RCPT(None, session, envelope, "first@large.adb.com", [])
        second = await handler.handle_RCPT(None, session, envelope, "second@small.adb.com", [])
        result = await handler.handle_DATA(None, session, envelope)

        assert first == "250 OK"
        assert second == "250 OK"
        assert result == "552 message too large"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_create_app_with_embedded_smtp_starts_and_stops_controller(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {"start": 0, "stop": 0}

    class FakeSMTPServer:
        def __init__(self, runtime) -> None:
            calls["runtime"] = runtime

        def start(self) -> None:
            calls["start"] = int(calls["start"]) + 1

        def stop(self) -> None:
            calls["stop"] = int(calls["stop"]) + 1

    monkeypatch.setattr(app_main, "SMTPServer", FakeSMTPServer)

    settings = default_settings(tmp_path)
    app = app_main.create_app(settings=settings, embed_smtp=True)

    async with app.router.lifespan_context(app):
        assert calls["start"] == 1
        assert calls["runtime"] is app.state.runtime
    assert calls["stop"] == 1


def test_http_runner_builds_embedded_smtp_app(monkeypatch, tmp_path) -> None:
    calls: dict[str, object] = {}

    class DummyApp:
        pass

    def fake_default_settings(base_dir):
        calls["base_dir"] = base_dir
        return SimpleNamespace(host="127.0.0.1", port=8000)

    def fake_create_app(*, settings=None, embed_smtp=False):
        calls["settings"] = settings
        calls["embed_smtp"] = embed_smtp
        return DummyApp()

    def fake_run(
        app,
        host,
        port,
        reload,
        limit_concurrency,
        ws_max_size,
        ws_max_queue,
    ):
        calls["app"] = app
        calls["host"] = host
        calls["port"] = port
        calls["reload"] = reload
        calls["limit_concurrency"] = limit_concurrency
        calls["ws_max_size"] = ws_max_size
        calls["ws_max_queue"] = ws_max_queue

    monkeypatch.setattr(http_runner, "default_settings", fake_default_settings)
    monkeypatch.setattr(http_runner, "create_app", fake_create_app)
    monkeypatch.setattr(http_runner.uvicorn, "run", fake_run)
    monkeypatch.chdir(tmp_path)

    http_runner.main()

    assert calls["base_dir"] == tmp_path
    assert calls["embed_smtp"] is True
    assert isinstance(calls["app"], DummyApp)
    assert calls["host"] == "127.0.0.1"
    assert calls["port"] == 8000
    assert calls["reload"] is False
    assert calls["limit_concurrency"] == 1000
    assert calls["ws_max_size"] == 16 * 1024
    assert calls["ws_max_queue"] == 1


def test_smtp_server_uses_disconnect_aware_protocol(tmp_path) -> None:
    settings = default_settings(tmp_path)
    runtime = SimpleNamespace(settings=settings)
    server = SMTPServer(runtime)

    assert isinstance(server._controller, RapidInboxController)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure_stage", "expected_server_stop_calls"),
    [
        ("init", 0),
        ("start", 1),
    ],
)
async def test_smtp_runner_stops_runtime_on_startup_failure(
    monkeypatch,
    failure_stage: str,
    expected_server_stop_calls: int,
) -> None:
    runtime_holder: dict[str, object] = {}
    server_holder: dict[str, object] = {}

    class FakeRuntime:
        def __init__(self, settings) -> None:
            self.settings = settings
            self.start_calls = 0
            self.stop_calls = 0
            runtime_holder["runtime"] = self

        async def start(self) -> None:
            self.start_calls += 1

        async def stop(self) -> None:
            self.stop_calls += 1

    class FakeServer:
        def __init__(self, runtime) -> None:
            self.runtime = runtime
            self.stop_calls = 0
            server_holder["server"] = self
            if failure_stage == "init":
                raise RuntimeError("smtp bootstrap failed")

        def start(self) -> None:
            if failure_stage == "start":
                raise RuntimeError("smtp bootstrap failed")

        def stop(self) -> None:
            self.stop_calls += 1

    monkeypatch.setattr(smtp_runner, "default_settings", lambda base_dir: object())
    monkeypatch.setattr(smtp_runner, "RapidInboxRuntime", FakeRuntime)
    monkeypatch.setattr(smtp_runner, "SMTPServer", FakeServer)

    with pytest.raises(RuntimeError, match="smtp bootstrap failed"):
        await smtp_runner.main_async()

    runtime = runtime_holder["runtime"]
    assert isinstance(runtime, FakeRuntime)
    assert runtime.start_calls == 1
    assert runtime.stop_calls == 1

    server = server_holder["server"]
    assert isinstance(server, FakeServer)
    assert server.stop_calls == expected_server_stop_calls
