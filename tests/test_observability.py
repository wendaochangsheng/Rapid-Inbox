from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
import subprocess
import sys
import threading
import time

import httpx
import pytest
from fastapi import FastAPI

from app import runtime as runtime_module
from app.config import Settings, default_settings
from app.main import create_app
from app.observability import (
    AsyncLogDispatcher,
    HTTPObservabilityMiddleware,
    JsonLogFormatter,
    MetricsRegistry,
    Observability,
    TextLogFormatter,
    configure_logging,
    shutdown_logging,
    _process_resident_memory_bytes,
)
from app.runtime import RapidInboxRuntime
from app.services.dashboard import get_dashboard_service


class _BlockingLogSink(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.entered.set()
        self.release.wait()
        self.records.append(record)


def test_application_preload_does_not_start_log_listener_thread() -> None:
    script = (
        "import threading\n"
        "import app.main\n"
        "print(sum(t.name == 'rapid-inbox-log-listener' for t in threading.enumerate()))\n"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        check=True,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert completed.stdout.strip() == "0"


def test_process_resident_memory_uses_current_proc_pages(tmp_path, monkeypatch) -> None:
    statm = tmp_path / "statm"
    statm.write_text("999 123 4 5 6 7 8\n", encoding="ascii")
    monkeypatch.setattr("app.observability.os.sysconf", lambda _name: 4096)

    assert _process_resident_memory_bytes(statm) == 123 * 4096


@pytest.mark.asyncio
async def test_system_endpoints_expose_safe_health_version_and_http_metrics(app_client, caplog) -> None:
    caplog.set_level(logging.INFO, logger="rapid_inbox.http")
    secret = "must-not-appear-in-observability"

    live = await app_client.get(
        f"/health/live?api_key={secret}",
        headers={
            "Authorization": f"Bearer {secret}",
            "Cookie": f"session={secret}",
            "X-Request-ID": "request-test-123",
        },
    )
    ready = await app_client.get("/health/ready")
    version = await app_client.get("/version")
    metrics = await app_client.get("/metrics")

    assert live.status_code == 200
    assert live.json() == {"status": "alive"}
    assert live.headers["x-request-id"] == "request-test-123"
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert all(check["ok"] for check in ready.json()["checks"].values())
    assert version.status_code == 200
    assert version.json()["name"] == "rapid-inbox"
    assert version.json()["version"]
    assert version.json()["api_version"] == "v2"
    assert version.json()["supported_api_versions"] == ["v1", "v2"]
    assert metrics.status_code == 200
    assert "rapid_inbox_http_requests_total" in metrics.text
    assert 'route="/health/live"' in metrics.text
    assert 'route="/health/ready"' in metrics.text
    assert "rapid_inbox_dashboard_snapshot_available 1" in metrics.text
    assert "rapid_inbox_ingestd_online 0" in metrics.text
    assert "rapid_inbox_database_snapshot_ok 1" in metrics.text
    assert "rapid_inbox_disk_snapshot_ok 1" in metrics.text
    assert 'rapid_inbox_parse_queue_messages{state="failed"} 0' in metrics.text
    assert metrics.headers["cache-control"] == "no-store"
    assert secret not in metrics.text

    request_records = [record for record in caplog.records if getattr(record, "event", None) == "http_request"]
    assert request_records
    live_record = next(record for record in request_records if getattr(record, "route", None) == "/health/live")
    assert live_record.request_id == "request-test-123"
    assert live_record.remote_ip == "127.0.0.1"
    assert not hasattr(live_record, "query_string")
    assert secret not in json.dumps(live_record.__dict__, default=str)


@pytest.mark.asyncio
async def test_request_id_middleware_replaces_untrusted_values(app_client) -> None:
    response = await app_client.get("/health/live", headers={"X-Request-ID": "not valid / unsafe"})

    request_id = response.headers["x-request-id"]
    assert request_id != "not valid / unsafe"
    assert len(request_id) == 32
    assert request_id.isalnum()


@pytest.mark.asyncio
async def test_blocked_log_sink_does_not_freeze_concurrent_http_requests(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    observability = Observability(settings)
    sink = _BlockingLogSink()
    shutdown_logging(timeout_seconds=0.1)
    dispatcher = configure_logging(settings, metrics=observability.metrics)
    dispatcher.sink = sink
    dispatcher.configure(
        level=logging.INFO,
        formatter=JsonLogFormatter(),
        metrics=observability.metrics,
    )
    http_logger = logging.getLogger("rapid_inbox.http")
    previous_handlers = list(http_logger.handlers)
    previous_level = http_logger.level
    previous_propagate = http_logger.propagate
    http_logger.handlers = []
    http_logger.setLevel(logging.INFO)
    http_logger.propagate = True

    app = FastAPI()

    @app.get("/probe")
    async def probe() -> dict[str, bool]:
        await asyncio.sleep(0)
        return {"ok": True}

    app.add_middleware(HTTPObservabilityMiddleware, observability=observability)
    failsafe_release = threading.Timer(2.0, sink.release.set)
    failsafe_release.daemon = True
    failsafe_release.start()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            first = asyncio.create_task(client.get("/probe"))
            assert await asyncio.to_thread(sink.entered.wait, 1.0)
            started = time.monotonic()
            second = asyncio.create_task(client.get("/probe"))
            first_response, second_response = await asyncio.wait_for(
                asyncio.gather(first, second),
                timeout=1.0,
            )
            elapsed = time.monotonic() - started

        assert first_response.status_code == 200
        assert second_response.status_code == 200
        assert elapsed < 0.5
        assert not sink.release.is_set()
    finally:
        http_logger.handlers = previous_handlers
        http_logger.setLevel(previous_level)
        http_logger.propagate = previous_propagate
        sink.release.set()
        failsafe_release.cancel()
        assert shutdown_logging(timeout_seconds=1.0)


def test_log_queue_is_bounded_and_counts_drops_with_finite_shutdown() -> None:
    metrics = MetricsRegistry()
    sink = _BlockingLogSink()
    dispatcher = AsyncLogDispatcher(sink, queue_capacity=2, metrics=metrics)
    dispatcher.configure(
        level=logging.INFO,
        formatter=JsonLogFormatter(),
        metrics=metrics,
    )
    logger = logging.getLogger("rapid_inbox.test.bounded_log_queue")
    previous_handlers = list(logger.handlers)
    previous_level = logger.level
    previous_propagate = logger.propagate
    logger.handlers = [dispatcher.handler]
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        logger.info("listener blocker")
        assert sink.entered.wait(timeout=1.0)

        offered_while_blocked = 9
        for index in range(offered_while_blocked):
            logger.info("queued record %s", index)

        assert dispatcher.pending_count == dispatcher.queue_capacity == 2
        rendered = metrics.render(started_monotonic=time.monotonic())
        assert (
            'rapid_inbox_log_records_dropped_total{reason="queue_full"} 7'
            in rendered
        )

        started = time.monotonic()
        assert dispatcher.shutdown(timeout_seconds=0.02) is False
        assert time.monotonic() - started < 0.25
        assert dispatcher.pending_count == 0
        rendered = metrics.render(started_monotonic=time.monotonic())
        assert (
            'rapid_inbox_log_records_dropped_total{reason="shutdown_timeout"} 2'
            in rendered
        )
    finally:
        logger.handlers = previous_handlers
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate
        sink.release.set()
        assert dispatcher.shutdown(timeout_seconds=1.0)


@pytest.mark.asyncio
async def test_remote_peer_cannot_bypass_loopback_startup_security_with_cli_bind(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        host="127.0.0.1",
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, client=("203.0.113.10", 43123))
        async with httpx.AsyncClient(transport=transport, base_url="http://public.example") as client:
            response = await client.get("/health/live")

    assert response.status_code == 503
    assert response.json()["detail"] == (
        "external access is disabled until security settings are configured"
    )


@pytest.mark.asyncio
async def test_metrics_endpoint_supports_bearer_or_dedicated_token(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        metrics_token="metrics-secret",
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            missing = await client.get("/metrics")
            wrong = await client.get("/metrics", headers={"Authorization": "Bearer wrong"})
            bearer = await client.get("/metrics", headers={"Authorization": "Bearer metrics-secret"})
            dedicated = await client.get("/metrics", headers={"X-Metrics-Token": "metrics-secret"})

    assert missing.status_code == 401
    assert missing.headers["www-authenticate"] == "Bearer"
    assert missing.headers["cache-control"] == "no-store"
    assert wrong.status_code == 401
    assert wrong.headers["cache-control"] == "no-store"
    assert bearer.status_code == 200
    assert dedicated.status_code == 200
    assert bearer.headers["cache-control"] == "no-store"
    assert dedicated.headers["cache-control"] == "no-store"


def test_operational_metrics_render_bounded_snapshot_values() -> None:
    registry = MetricsRegistry()
    snapshot = {
        "ingestd": {
            "state": "online",
            "online": True,
            "queue_messages": 7,
            "queue_bytes": 8192,
            "active_connections": 12,
            "max_connections": 1024,
        },
        "database": {
            "ok": True,
            "database_bytes": 4096,
            "wal_bytes": 2048,
            "shm_bytes": 1024,
        },
        "smtp": {
            "python_active_connections": 3,
            "ingestd_active_connections": 12,
            "open_sessions": 5,
        },
        "parse_queue": {
            "running": True,
            "queued": 4,
            "active_workers": 2,
            "pending_messages": 9,
            "failed_messages": 1,
        },
        "disk": {
            "ok": True,
            "total_bytes": 100_000,
            "used_bytes": 60_000,
            "free_bytes": 40_000,
            "used_percent": 60.0,
        },
        "mail": {
            "received_last_minute": 10,
            "received_last_day": 100,
            "deliveries_last_minute": 11,
            "deliveries_last_day": 110,
            "rejected_last_day": 4,
            "parse_failures_last_day": 2,
        },
        "cleanup": {"status": "failure", "consecutive_failures": 3},
    }

    rendered = registry.render(started_monotonic=0.0, operational_snapshot=snapshot)

    assert "rapid_inbox_ingestd_online 1" in rendered
    assert "rapid_inbox_ingestd_queue_messages 7" in rendered
    assert "rapid_inbox_ingestd_queue_bytes 8192" in rendered
    assert "rapid_inbox_ingestd_active_connections 12" in rendered
    assert "rapid_inbox_ingestd_max_connections 1024" in rendered
    assert 'rapid_inbox_smtp_active_connections{implementation="python"} 3' in rendered
    assert 'rapid_inbox_smtp_active_connections{implementation="ingestd"} 12' in rendered
    assert "rapid_inbox_smtp_open_sessions 5" in rendered
    assert 'rapid_inbox_parse_queue_messages{state="queued"} 4' in rendered
    assert 'rapid_inbox_parse_queue_messages{state="active"} 2' in rendered
    assert 'rapid_inbox_parse_queue_messages{state="pending"} 9' in rendered
    assert 'rapid_inbox_parse_queue_messages{state="failed"} 1' in rendered
    assert "rapid_inbox_database_bytes 4096" in rendered
    assert "rapid_inbox_database_wal_bytes 2048" in rendered
    assert "rapid_inbox_database_shm_bytes 1024" in rendered
    assert "rapid_inbox_disk_total_bytes 100000" in rendered
    assert "rapid_inbox_disk_used_bytes 60000" in rendered
    assert "rapid_inbox_disk_free_bytes 40000" in rendered
    assert "rapid_inbox_disk_used_percent 60" in rendered
    assert 'rapid_inbox_mail_received{window="1m"} 10' in rendered
    assert 'rapid_inbox_mail_received{window="24h"} 100' in rendered
    assert 'rapid_inbox_mail_deliveries{window="1m"} 11' in rendered
    assert 'rapid_inbox_mail_deliveries{window="24h"} 110' in rendered
    assert 'rapid_inbox_mail_rejections{window="24h"} 4' in rendered
    assert 'rapid_inbox_mail_parse_failures{window="24h"} 2' in rendered
    assert 'rapid_inbox_cleanup_status{status="failure"} 1' in rendered
    assert "rapid_inbox_cleanup_consecutive_failures 3" in rendered


def test_operational_metrics_omit_stale_or_nonfinite_values() -> None:
    registry = MetricsRegistry()
    snapshot = {
        "ingestd": {
            "state": "stale",
            "online": False,
            "queue_messages": 99,
            "queue_bytes": 1000,
            "active_connections": 8,
            "max_connections": 10,
        },
        "database": {
            "ok": False,
            "database_bytes": 4096,
            "wal_bytes": 2048,
            "shm_bytes": 1024,
        },
        "smtp": {"python_active_connections": 1, "open_sessions": 77},
        "parse_queue": {
            "running": True,
            "queued": 2,
            "active_workers": float("nan"),
            "pending_messages": 88,
            "failed_messages": 6,
        },
        "disk": {
            "ok": False,
            "total_bytes": 123,
            "used_bytes": 100,
            "free_bytes": 23,
            "used_percent": float("nan"),
        },
        "mail": {"deliveries_last_day": 999},
        "cleanup": {"status": "unexpected", "consecutive_failures": float("inf")},
    }

    rendered = registry.render(started_monotonic=0.0, operational_snapshot=snapshot)

    assert "rapid_inbox_ingestd_online 0" in rendered
    assert 'rapid_inbox_ingestd_status{state="stale"} 1' in rendered
    assert "rapid_inbox_ingestd_queue_messages" not in rendered
    assert 'rapid_inbox_smtp_active_connections{implementation="ingestd"}' not in rendered
    assert 'rapid_inbox_smtp_active_connections{implementation="python"} 1' in rendered
    assert "rapid_inbox_smtp_open_sessions" not in rendered
    assert 'rapid_inbox_parse_queue_messages{state="queued"} 2' in rendered
    assert 'rapid_inbox_parse_queue_messages{state="active"}' not in rendered
    assert 'rapid_inbox_parse_queue_messages{state="pending"}' not in rendered
    assert 'rapid_inbox_parse_queue_messages{state="failed"}' not in rendered
    assert "rapid_inbox_database_bytes" not in rendered
    assert "rapid_inbox_database_wal_bytes" not in rendered
    assert "rapid_inbox_database_shm_bytes" not in rendered
    assert "rapid_inbox_disk_total_bytes" not in rendered
    assert "rapid_inbox_mail_deliveries" not in rendered
    assert 'rapid_inbox_cleanup_status{status="unknown"} 1' in rendered
    assert "rapid_inbox_cleanup_consecutive_failures" not in rendered
    sample_values = [
        line.rsplit(" ", 1)[-1].lower()
        for line in rendered.splitlines()
        if line and not line.startswith("#")
    ]
    assert not {"nan", "+nan", "-nan", "inf", "+inf", "-inf"}.intersection(sample_values)


@pytest.mark.asyncio
async def test_metrics_endpoint_reuses_dashboard_snapshot_cache(app_client, app_fixture, monkeypatch) -> None:
    app, _ = app_fixture
    service = get_dashboard_service(app)
    original_build = service._build_snapshot
    build_count = 0

    def counted_build():
        nonlocal build_count
        build_count += 1
        return original_build()

    monkeypatch.setattr(service, "_build_snapshot", counted_build)

    first = await app_client.get("/metrics")
    second = await app_client.get("/metrics")

    assert first.status_code == 200
    assert second.status_code == 200
    assert build_count == 1
    assert "rapid_inbox_dashboard_snapshot_available 1" in second.text


@pytest.mark.asyncio
async def test_metrics_endpoint_keeps_base_metrics_when_dashboard_snapshot_fails(
    app_client,
    app_fixture,
    monkeypatch,
) -> None:
    app, _ = app_fixture
    service = get_dashboard_service(app)

    async def fail_snapshot():
        raise RuntimeError("dashboard unavailable")

    monkeypatch.setattr(service, "snapshot", fail_snapshot)

    response = await app_client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert "rapid_inbox_http_requests_in_flight" in response.text
    assert "rapid_inbox_dashboard_snapshot_available 0" in response.text


@pytest.mark.asyncio
async def test_metrics_endpoint_can_be_disabled(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        metrics_enabled=False,
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/metrics")

    assert response.status_code == 404
    assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_readiness_fails_when_free_disk_requirement_is_not_met(tmp_path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        readiness_min_free_disk_bytes=10**30,
    )
    app = create_app(settings=settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            response = await client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["checks"]["disk"]["ok"] is False


@pytest.mark.asyncio
async def test_periodic_task_records_failure_recovery_metrics_and_log(tmp_path, caplog) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
    )
    observability = Observability(settings)
    attempts = 0
    recovered = asyncio.Event()

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary maintenance failure")
        recovered.set()

    caplog.set_level(logging.ERROR, logger="rapid_inbox.background")
    task = asyncio.create_task(observability.run_periodic("fixture_task", 0.001, operation))
    try:
        await asyncio.wait_for(recovered.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    snapshot = observability.background.snapshot()["fixture_task"]
    rendered = observability.metrics.render(started_monotonic=observability.started_monotonic)
    assert snapshot["running"] is False
    assert snapshot["total_failures"] == 1
    assert snapshot["total_successes"] >= 1
    assert snapshot["consecutive_failures"] == 0
    assert 'task="fixture_task",outcome="failure"' in rendered
    assert 'task="fixture_task",outcome="success"' in rendered
    assert any(getattr(record, "event", None) == "background_task_failure" for record in caplog.records)


def test_json_log_formatter_has_stable_structured_fields_without_arbitrary_extras() -> None:
    record = logging.LogRecord(
        name="rapid_inbox.test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="request complete",
        args=(),
        exc_info=None,
    )
    record.event = "http_request"
    record.method = "GET"
    record.route = "/mail/{mailbox_address}"
    record.status_code = 200
    record.authorization = "secret-that-must-be-ignored"

    payload = json.loads(JsonLogFormatter().format(record))

    assert payload["event"] == "http_request"
    assert payload["route"] == "/mail/{mailbox_address}"
    assert "authorization" not in payload
    assert "secret-that-must-be-ignored" not in json.dumps(payload)


def test_text_log_formatter_keeps_the_same_safe_structured_context() -> None:
    record = logging.LogRecord(
        name="rapid_inbox.test",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="maintenance delayed",
        args=(),
        exc_info=None,
    )
    record.event = "background_task_failure"
    record.task = "message_retention"
    record.cookie = "must-not-be-rendered"

    rendered = TextLogFormatter().format(record)

    assert "level=WARNING" in rendered
    assert 'event="background_task_failure"' in rendered
    assert 'task="message_retention"' in rendered
    assert "must-not-be-rendered" not in rendered


@pytest.mark.parametrize(
    ("loop_name", "operation_name", "interval_name", "task_name"),
    [
        (
            "_message_retention_loop",
            "cleanup_expired_messages",
            "MESSAGE_RETENTION_CLEANUP_INTERVAL_SECONDS",
            "message_retention",
        ),
        (
            "_pending_parse_scan_loop",
            "requeue_pending_messages_for_parse",
            "PENDING_PARSE_SCAN_INTERVAL_SECONDS",
            "pending_parse_scan",
        ),
        (
            "_mailbox_live_event_loop",
            "_publish_pending_mailbox_live_events",
            "MAILBOX_LIVE_EVENT_POLL_INTERVAL_SECONDS",
            "mailbox_live_events",
        ),
    ],
)
@pytest.mark.asyncio
async def test_runtime_periodic_loops_report_errors_and_recovery(
    tmp_path,
    monkeypatch,
    loop_name: str,
    operation_name: str,
    interval_name: str,
    task_name: str,
) -> None:
    runtime = RapidInboxRuntime(
        Settings(
            storage_root=tmp_path / "storage",
            database_path=tmp_path / "storage" / "app.db",
        )
    )
    attempts = 0
    recovered = asyncio.Event()

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary loop failure")
        recovered.set()

    monkeypatch.setattr(runtime_module, interval_name, 0.001)
    monkeypatch.setattr(runtime, operation_name, operation)
    task = asyncio.create_task(getattr(runtime, loop_name)())
    try:
        await asyncio.wait_for(recovered.wait(), timeout=1)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    state = runtime.observability.background.snapshot()[task_name]
    assert state["total_failures"] == 1
    assert state["total_successes"] >= 1
    assert state["consecutive_failures"] == 0


def test_observability_settings_load_from_dotenv(tmp_path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "LOG_LEVEL=warning",
                "LOG_FORMAT=text",
                "REQUEST_LOG_ENABLED=false",
                "METRICS_ENABLED=false",
                "METRICS_TOKEN=metrics-secret",
                "READINESS_MIN_FREE_DISK_BYTES=12345",
            ]
        ),
        encoding="utf-8",
    )

    settings = default_settings(tmp_path)

    assert settings.log_level == "WARNING"
    assert settings.log_format == "text"
    assert settings.request_log_enabled is False
    assert settings.metrics_enabled is False
    assert settings.metrics_token == "metrics-secret"
    assert settings.readiness_min_free_disk_bytes == 12345


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("log_level", "verbose"),
        ("log_format", "xml"),
        ("readiness_min_free_disk_bytes", -1),
    ],
)
def test_observability_settings_reject_invalid_values(tmp_path, field_name: str, value: object) -> None:
    kwargs = {
        "storage_root": tmp_path / "storage",
        "database_path": tmp_path / "storage" / "app.db",
        field_name: value,
    }

    with pytest.raises(ValueError):
        Settings(**kwargs)
