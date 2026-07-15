from __future__ import annotations

import asyncio
import json
import threading
from types import SimpleNamespace

import pytest

from app.services.dashboard import DashboardService
from app.ingest.storage import INGEST_STATUS_FILENAME, INGEST_STATUS_FRESH_SECONDS


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def _status_service(tmp_path, payload: dict[str, object] | str, *, age_seconds: float = 0.5):
    storage_root = tmp_path / "storage"
    storage_root.mkdir()
    status_path = storage_root / INGEST_STATUS_FILENAME
    status_path.write_text(
        payload if isinstance(payload, str) else json.dumps(payload),
        encoding="utf-8",
    )
    modified_at = status_path.stat().st_mtime
    return DashboardService(
        SimpleNamespace(settings=SimpleNamespace(storage_root=storage_root)),
        wall_clock=lambda: modified_at + age_seconds,
    )


def _valid_ingestd_status(**updates: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "instance_id": "ingest_test_instance",
        "pid": 4321,
        "updated_at": "2026-07-15T02:03:04Z",
        "token": "maintenance-token-must-not-leak",
        "queue_messages": 7,
        "queue_bytes": 8192,
    }
    payload.update(updates)
    return payload


@pytest.mark.asyncio
async def test_dashboard_snapshot_cache_prevents_stampede(monkeypatch) -> None:
    clock = FakeClock()
    service = DashboardService(SimpleNamespace(), ttl_seconds=1.5, clock=clock)
    build_count = 0

    def build() -> dict[str, object]:
        nonlocal build_count
        build_count += 1
        return {"generated_at": f"snapshot-{build_count}"}

    monkeypatch.setattr(service, "_build_snapshot", build)
    snapshots = await asyncio.gather(*(service.snapshot() for _ in range(20)))

    assert build_count == 1
    assert {item["generated_at"] for item in snapshots} == {"snapshot-1"}
    assert all(item["cache"]["age_seconds"] == 0 for item in snapshots)


@pytest.mark.asyncio
async def test_dashboard_snapshot_refreshes_after_ttl(monkeypatch) -> None:
    clock = FakeClock()
    service = DashboardService(SimpleNamespace(), ttl_seconds=1.5, clock=clock)
    build_count = 0

    def build() -> dict[str, object]:
        nonlocal build_count
        build_count += 1
        return {"version": build_count}

    monkeypatch.setattr(service, "_build_snapshot", build)
    first = await service.snapshot()
    clock.value += 1.0
    cached = await service.snapshot()
    clock.value += 0.6
    refreshed = await service.snapshot()

    assert first["version"] == 1
    assert cached["version"] == 1
    assert cached["cache"]["age_seconds"] == 1.0
    assert refreshed["version"] == 2
    assert build_count == 2


@pytest.mark.asyncio
async def test_dashboard_build_runs_off_event_loop_thread(monkeypatch) -> None:
    service = DashboardService(SimpleNamespace())
    event_loop_thread = threading.get_ident()
    build_thread = 0

    def build() -> dict[str, object]:
        nonlocal build_thread
        build_thread = threading.get_ident()
        return {"ok": True}

    monkeypatch.setattr(service, "_build_snapshot", build)
    await service.snapshot()

    assert build_thread
    assert build_thread != event_loop_thread


def test_ingestd_snapshot_reports_missing_status(tmp_path) -> None:
    storage_root = tmp_path / "storage"
    storage_root.mkdir()
    service = DashboardService(SimpleNamespace(settings=SimpleNamespace(storage_root=storage_root)))

    status = service._ingestd_snapshot()

    assert status == {
        "state": "missing",
        "present": False,
        "online": False,
        "stale": False,
        "instance_id": None,
        "pid": None,
        "updated_at": None,
        "age_seconds": None,
        "stale_after_seconds": float(INGEST_STATUS_FRESH_SECONDS),
        "queue_messages": None,
        "queue_bytes": None,
        "active_connections": None,
        "max_connections": None,
        "error_type": None,
    }


def test_ingestd_snapshot_validates_and_exposes_fresh_instance(tmp_path) -> None:
    service = _status_service(
        tmp_path,
        _valid_ingestd_status(active_connections=12, max_connections=1024),
    )

    status = service._ingestd_snapshot()

    assert status["state"] == "online"
    assert status["online"] is True
    assert status["stale"] is False
    assert status["instance_id"] == "ingest_test_instance"
    assert status["pid"] == 4321
    assert status["age_seconds"] == 0.5
    assert status["queue_messages"] == 7
    assert status["queue_bytes"] == 8192
    assert status["active_connections"] == 12
    assert status["max_connections"] == 1024
    assert "token" not in status


def test_ingestd_snapshot_preserves_instance_data_when_stale(tmp_path) -> None:
    service = _status_service(
        tmp_path,
        _valid_ingestd_status(),
        age_seconds=INGEST_STATUS_FRESH_SECONDS + 0.25,
    )

    status = service._ingestd_snapshot()

    assert status["state"] == "stale"
    assert status["online"] is False
    assert status["stale"] is True
    assert status["instance_id"] == "ingest_test_instance"
    assert status["pid"] == 4321
    assert status["queue_messages"] == 7


@pytest.mark.parametrize(
    "payload",
    [
        "{not-json",
        _valid_ingestd_status(pid=True),
        _valid_ingestd_status(queue_messages=-1),
        _valid_ingestd_status(updated_at="not-a-time"),
    ],
)
def test_ingestd_snapshot_fails_closed_for_corrupt_status(tmp_path, payload) -> None:
    service = _status_service(tmp_path, payload)

    status = service._ingestd_snapshot()

    assert status["state"] == "invalid"
    assert status["present"] is True
    assert status["online"] is False
    assert status["error_type"] in {"JSONDecodeError", "ValueError"}


@pytest.mark.asyncio
async def test_ingestd_status_io_runs_off_event_loop_thread(runtime, monkeypatch) -> None:
    service = DashboardService(runtime)
    event_loop_thread = threading.get_ident()
    read_thread = 0
    original = service._ingestd_snapshot

    def read_status() -> dict[str, object]:
        nonlocal read_thread
        read_thread = threading.get_ident()
        return original()

    monkeypatch.setattr(service, "_ingestd_snapshot", read_status)
    await service.snapshot()

    assert read_thread
    assert read_thread != event_loop_thread


def test_stale_ingestd_status_raises_health_alert(tmp_path) -> None:
    service = _status_service(
        tmp_path,
        _valid_ingestd_status(),
        age_seconds=INGEST_STATUS_FRESH_SECONDS + 10,
    )
    service.runtime.settings.readiness_min_free_disk_bytes = 0
    ingestd = service._ingestd_snapshot()

    alerts = service._alerts(
        operational={"ok": True},
        database={"ok": True, "parse_failures_last_day": 0, "wal_bytes": 0, "database_bytes": 0},
        disk={"ok": True, "used_percent": 0, "warning_threshold_percent": 85, "free_bytes": 1},
        parse_queue={"pending_messages": 0, "queued": 0},
        background_tasks={},
        cleanup={"status": "success"},
        ingestd=ingestd,
    )

    assert any(alert["code"] == "ingestd_status_stale" and alert["severity"] == "danger" for alert in alerts)


def test_dashboard_http_rate_and_p95(runtime) -> None:
    clock = FakeClock(100.0)
    runtime.observability.started_monotonic = 90.0
    service = DashboardService(runtime, clock=clock)
    metrics = runtime.observability.metrics
    for _ in range(18):
        metrics.http_finished("GET", "/fast", 200, 0.01)
    for _ in range(2):
        metrics.http_finished("GET", "/slow", 200, 0.4)

    first = service._http_snapshot()
    for _ in range(10):
        metrics.http_finished("GET", "/fast", 200, 0.01)
    clock.value = 110.0
    second = service._http_snapshot()

    assert first["requests_total"] == 20
    assert first["requests_per_second"] == 2.0
    assert first["p95_ms"] == 500.0
    assert second["requests_per_second"] == 1.0
    assert second["rate_window_seconds"] == 10.0


@pytest.mark.parametrize(
    ("state", "expected"),
    [
        (None, "unknown"),
        ({"last_success_at": None, "last_error_at": None, "consecutive_failures": 0}, "unknown"),
        (
            {
                "last_success_at": "2026-07-15T10:00:00Z",
                "last_error_at": None,
                "consecutive_failures": 0,
            },
            "success",
        ),
        (
            {
                "last_success_at": "2026-07-15T10:00:00Z",
                "last_error_at": "2026-07-15T10:01:00Z",
                "last_error_type": "OSError",
                "consecutive_failures": 1,
            },
            "failure",
        ),
    ],
)
def test_dashboard_cleanup_status_is_evidence_based(state, expected) -> None:
    service = DashboardService(SimpleNamespace())

    assert service._cleanup_snapshot(state)["status"] == expected


@pytest.mark.asyncio
async def test_dashboard_metrics_api_requires_system_read(app_client, runtime) -> None:
    denied_key = await runtime.api_keys.create_key(
        name="dashboard-denied",
        kind="admin",
        scopes=["domains.read"],
        domain_ids=[],
        mailbox_patterns=[],
    )
    allowed_key = await runtime.api_keys.create_key(
        name="dashboard-reader",
        kind="admin",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )

    denied = await app_client.get(
        "/api/v1/admin/dashboard/metrics",
        headers={"X-API-Key": denied_key["plain_text"]},
    )
    allowed = await app_client.get(
        "/api/v1/admin/dashboard/metrics",
        headers={"X-API-Key": allowed_key["plain_text"]},
    )

    assert denied.status_code == 403
    assert denied.json()["detail"] == "system.read"
    assert allowed.status_code == 200
    assert allowed.json()["health"]["status"] in {"ok", "warning", "danger"}
    assert "p95_ms" in allowed.json()["http"]
    assert "deliveries_last_day" in allowed.json()["mail"]


@pytest.mark.asyncio
async def test_v2_dashboard_exposes_fresh_ingestd_status_without_maintenance_token(
    app_client,
    runtime,
) -> None:
    allowed_key = await runtime.api_keys.create_key(
        name="dashboard-v2-ingestd-reader",
        kind="admin",
        scopes=["system.read"],
        domain_ids=[],
        mailbox_patterns=[],
        domain_grant_mode="all",
    )
    status_path = runtime.settings.storage_root / INGEST_STATUS_FILENAME
    status_path.write_text(
        json.dumps(_valid_ingestd_status(active_connections=12, max_connections=1024)),
        encoding="utf-8",
    )

    response = await app_client.get(
        "/api/v2/dashboard/status",
        headers={"Authorization": f"Bearer {allowed_key['plain_text']}"},
    )

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["ingestd"]["state"] == "online"
    assert data["ingestd"]["instance_id"] == "ingest_test_instance"
    assert data["ingestd"]["pid"] == 4321
    assert data["ingestd"]["queue_messages"] == 7
    assert data["smtp"]["ingestd_active_connections"] == 12
    assert data["smtp"]["active_connections"] == 12
    assert "token" not in data["ingestd"]


@pytest.mark.asyncio
async def test_admin_dashboard_renders_operational_sections(app_client, runtime) -> None:
    login = await app_client.post(
        "/admin/login",
        data={"username": "admin", "password": runtime.settings.bootstrap_admin_password},
    )
    assert login.status_code == 303
    changed = await app_client.post(
        "/admin/settings/password",
        data={
            "current_password": runtime.settings.bootstrap_admin_password,
            "new_password": "dashboard-test-password",
            "confirm_password": "dashboard-test-password",
        },
    )
    assert changed.status_code == 303
    (runtime.settings.storage_root / INGEST_STATUS_FILENAME).write_text(
        json.dumps(_valid_ingestd_status(active_connections=12, max_connections=1024)),
        encoding="utf-8",
    )

    response = await app_client.get("/admin")

    assert response.status_code == 200
    assert "系统健康" in response.text
    assert "数据面状态" in response.text
    assert "C++ ingestd" in response.text
    assert "ingest_test_instance" in response.text
    assert "maintenance-token-must-not-leak" not in response.text
    assert "HTTP / API" in response.text
    assert "后台任务与清理" in response.text
    assert "尚无运行数据" in response.text
