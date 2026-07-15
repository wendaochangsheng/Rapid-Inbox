from __future__ import annotations

import asyncio
import json
import math
import os
import shutil
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import monotonic, time
from typing import Any, Callable

from app.db.connection import connect_database
from app.ingest.storage import INGEST_STATUS_FILENAME, INGEST_STATUS_FRESH_SECONDS
from app.observability import HTTP_DURATION_BUCKETS


DEFAULT_DASHBOARD_CACHE_SECONDS = 1.5
_MIB = 1024 * 1024
_INGEST_STATUS_MAX_BYTES = 64 * 1024


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _utc_cutoff(now: datetime, seconds: int) -> str:
    # Metrics are intentionally stored in minute buckets.  Flooring the
    # rolling cutoff keeps every query index-aligned and caps a 24-hour scan
    # at roughly 1,441 tiny rows, independent of mail volume.
    cutoff = (now - timedelta(seconds=seconds)).replace(second=0, microsecond=0)
    return cutoff.isoformat(timespec="seconds").replace("+00:00", "Z")


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _human_bytes(value: int) -> str:
    size = max(int(value), 0)
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(size)
    for unit in units:
        if amount < 1024 or unit == units[-1]:
            return f"{amount:.0f} {unit}" if unit == "B" else f"{amount:.1f} {unit}"
        amount /= 1024
    return f"{size} B"


def _delivery_chart(connection: sqlite3.Connection, *, hours: int = 24) -> dict[str, Any]:
    """Build the dashboard's fixed-width hourly delivery curve."""

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    buckets: list[dict[str, Any]] = []
    bucket_index: dict[str, int] = {}
    for offset in range(hours - 1, -1, -1):
        timestamp = now - timedelta(hours=offset)
        key = timestamp.strftime("%Y-%m-%dT%H:00:00Z")
        bucket_index[key] = len(buckets)
        buckets.append({"ts": key, "hour": timestamp.hour, "value": 0})

    cutoff = (now - timedelta(hours=hours - 1)).strftime("%Y-%m-%dT%H:00:00Z")
    rows = connection.execute(
        """
        SELECT
            substr(bucket_ts, 1, 13) || ':00:00Z' AS bucket,
            COALESCE(SUM(deliveries), 0) AS count
        FROM mail_metric_buckets
        WHERE bucket_ts >= ?
        GROUP BY bucket
        """,
        (cutoff,),
    ).fetchall()
    for row in rows:
        index = bucket_index.get(str(row["bucket"]))
        if index is not None:
            buckets[index]["value"] = int(row["count"])

    values = [int(item["value"]) for item in buckets]
    peak = max(values, default=0)
    width = 1000.0
    height = 240.0
    pad_x = 24.0
    pad_top = 18.0
    pad_bottom = 30.0
    inner_width = width - pad_x * 2
    inner_height = height - pad_top - pad_bottom
    scale_max = max(peak, 1)

    coordinates: list[tuple[float, float]] = []
    for index, item in enumerate(buckets):
        x = pad_x + (index / max(len(buckets) - 1, 1)) * inner_width
        y = pad_top + inner_height - (int(item["value"]) / scale_max) * inner_height
        coordinates.append((x, y))
        item["x"] = round(x, 2)
        item["y"] = round(y, 2)

    def segment(
        p0: tuple[float, float],
        p1: tuple[float, float],
        p2: tuple[float, float],
        p3: tuple[float, float],
    ) -> str:
        c1x = p1[0] + (p2[0] - p0[0]) / 6
        c1y = p1[1] + (p2[1] - p0[1]) / 6
        c2x = p2[0] - (p3[0] - p1[0]) / 6
        c2y = p2[1] - (p3[1] - p1[1]) / 6
        return f"C {c1x:.2f} {c1y:.2f}, {c2x:.2f} {c2y:.2f}, {p2[0]:.2f} {p2[1]:.2f}"

    if coordinates:
        path_parts = [f"M {coordinates[0][0]:.2f} {coordinates[0][1]:.2f}"]
        for index in range(len(coordinates) - 1):
            p0 = coordinates[index - 1] if index > 0 else coordinates[index]
            p1 = coordinates[index]
            p2 = coordinates[index + 1]
            p3 = coordinates[index + 2] if index + 2 < len(coordinates) else p2
            path_parts.append(segment(p0, p1, p2, p3))
        line_path = " ".join(path_parts)
        baseline = pad_top + inner_height
        area_path = (
            f"{line_path} L {coordinates[-1][0]:.2f} {baseline:.2f}"
            f" L {coordinates[0][0]:.2f} {baseline:.2f} Z"
        )
    else:
        line_path = ""
        area_path = ""

    bucket_count = len(buckets)
    tick_indices = (
        sorted({0, bucket_count // 4, bucket_count // 2, (3 * bucket_count) // 4, bucket_count - 1})
        if bucket_count
        else []
    )
    return {
        "buckets": buckets,
        "peak": peak,
        "total": sum(values),
        "line_path": line_path,
        "area_path": area_path,
        "ticks": [
            {"x": buckets[index]["x"], "label": f"{int(buckets[index]['hour']):02d}:00"}
            for index in tick_indices
        ],
        "view_width": int(width),
        "view_height": int(height),
        "baseline_y": round(pad_top + inner_height, 2),
        "pad_top": pad_top,
    }


class DashboardService:
    """Produce a bounded-cost operational snapshot shared by HTML and JSON clients."""

    def __init__(
        self,
        runtime: Any,
        *,
        ttl_seconds: float = DEFAULT_DASHBOARD_CACHE_SECONDS,
        clock: Callable[[], float] = monotonic,
        wall_clock: Callable[[], float] = time,
    ) -> None:
        self.runtime = runtime
        self.ttl_seconds = max(float(ttl_seconds), 0.0)
        self._clock = clock
        self._wall_clock = wall_clock
        self._cache_lock = asyncio.Lock()
        self._cached_snapshot: dict[str, Any] | None = None
        self._cached_at = 0.0
        self._rate_lock = threading.Lock()
        self._last_request_total: int | None = None
        self._last_request_sample_at: float | None = None

    async def snapshot(self) -> dict[str, Any]:
        now = self._clock()
        if self._cache_is_fresh(now):
            return self._with_cache_age(self._cached_snapshot, now)

        async with self._cache_lock:
            now = self._clock()
            if not self._cache_is_fresh(now):
                snapshot = await asyncio.to_thread(self._build_snapshot)
                self._cached_snapshot = snapshot
                self._cached_at = self._clock()
                now = self._cached_at
            return self._with_cache_age(self._cached_snapshot, now)

    def invalidate(self) -> None:
        self._cached_snapshot = None
        self._cached_at = 0.0

    def _cache_is_fresh(self, now: float) -> bool:
        return self._cached_snapshot is not None and max(now - self._cached_at, 0.0) < self.ttl_seconds

    def _with_cache_age(self, snapshot: dict[str, Any] | None, now: float) -> dict[str, Any]:
        if snapshot is None:
            return {}
        result = dict(snapshot)
        result["cache"] = {
            "ttl_seconds": self.ttl_seconds,
            "age_seconds": round(max(now - self._cached_at, 0.0), 3),
        }
        return result

    def _build_snapshot(self) -> dict[str, Any]:
        database = self._database_snapshot()
        disk = self._disk_snapshot()
        http = self._http_snapshot()
        background_tasks = self._background_snapshot()
        cleanup = self._cleanup_snapshot(background_tasks.get("message_retention"))
        ingestd = self._ingestd_snapshot()
        python_active_connections = self._active_smtp_connections()
        ingestd_active_connections = (
            ingestd.get("active_connections") if ingestd.get("online") else None
        )
        smtp = {
            "active_connections": python_active_connections + int(ingestd_active_connections or 0),
            "python_active_connections": python_active_connections,
            "ingestd_active_connections": ingestd_active_connections,
            "open_sessions": int(database.get("open_sessions", 0)),
        }
        parse_queue = self._parse_queue_snapshot(
            int(database.get("pending_messages", 0)),
            int(database.get("failed_messages", 0)),
        )
        operational = self._operational_snapshot()
        alerts = self._alerts(
            operational=operational,
            database=database,
            disk=disk,
            parse_queue=parse_queue,
            background_tasks=background_tasks,
            cleanup=cleanup,
            ingestd=ingestd,
        )
        health_status = "danger" if any(item["severity"] == "danger" for item in alerts) else (
            "warning" if any(item["severity"] == "warning" for item in alerts) else "ok"
        )

        mail = {
            "received_last_minute": int(database.get("received_last_minute", 0)),
            "received_last_five_minutes": int(database.get("received_last_five_minutes", 0)),
            "received_last_day": int(database.get("received_last_day", 0)),
            "deliveries_last_minute": int(database.get("deliveries_last_minute", 0)),
            "deliveries_last_day": int(database.get("deliveries_last_day", 0)),
            "rejected_last_day": int(database.get("rejected_last_day", 0)),
            "parse_failures_last_day": int(database.get("parse_failures_last_day", 0)),
        }
        totals = {
            "domains": int(database.get("domains", 0)),
            "mailboxes": int(database.get("mailboxes", 0)),
            "messages": int(database.get("messages", 0)),
            "api_keys": int(database.get("api_keys", 0)),
            "audit_logs": int(database.get("audit_logs", 0)),
        }
        result = {
            "generated_at": _utc_now(),
            "health": {"status": health_status, "alerts": alerts, "operational": operational},
            "http": http,
            "mail": mail,
            "smtp": smtp,
            "ingestd": ingestd,
            "parse_queue": parse_queue,
            "database": {
                key: value
                for key, value in database.items()
                if key
                in {
                    "ok",
                    "error_type",
                    "database_bytes",
                    "wal_bytes",
                    "shm_bytes",
                    "free_bytes",
                }
            },
            "disk": disk,
            "background_tasks": background_tasks,
            "cleanup": cleanup,
            "recent_messages": database.get("recent_messages", []),
            "recent_domains": database.get("recent_domains", []),
            "delivery_chart": database.get("delivery_chart", self._empty_delivery_chart()),
            "totals_raw": totals,
        }
        result.update(self._legacy_template_context(result, database, totals))
        return result

    def _database_snapshot(self) -> dict[str, Any]:
        database_path = Path(self.runtime.settings.database_path)
        sizes = {
            "database_bytes": _file_size(database_path),
            "wal_bytes": _file_size(Path(f"{database_path}-wal")),
            "shm_bytes": _file_size(Path(f"{database_path}-shm")),
        }
        now = datetime.now(timezone.utc).replace(microsecond=0)
        params = {
            "minute": _utc_cutoff(now, 60),
            "five_minutes": _utc_cutoff(now, 5 * 60),
            "day": _utc_cutoff(now, 24 * 60 * 60),
        }
        try:
            with connect_database(database_path) as connection:
                row = connection.execute(
                    """
                    WITH recent_metrics AS (
                        SELECT
                            COALESCE(SUM(
                                CASE WHEN bucket_ts >= :minute THEN received ELSE 0 END
                            ), 0) AS received_last_minute,
                            COALESCE(SUM(
                                CASE WHEN bucket_ts >= :five_minutes THEN received ELSE 0 END
                            ), 0) AS received_last_five_minutes,
                            COALESCE(SUM(received), 0) AS received_last_day,
                            COALESCE(SUM(
                                CASE WHEN bucket_ts >= :minute THEN deliveries ELSE 0 END
                            ), 0) AS deliveries_last_minute,
                            COALESCE(SUM(deliveries), 0) AS deliveries_last_day,
                            COALESCE(SUM(rejected), 0) AS rejected_last_day,
                            COALESCE(SUM(parse_failures), 0) AS parse_failures_last_day
                        FROM mail_metric_buckets
                        WHERE bucket_ts >= :day
                    )
                    SELECT
                        (SELECT COUNT(*) FROM smtp_sessions WHERE status = 'open') AS open_sessions,
                        recent.received_last_minute,
                        recent.received_last_five_minutes,
                        recent.received_last_day,
                        recent.deliveries_last_minute,
                        recent.deliveries_last_day,
                        recent.rejected_last_day,
                        recent.parse_failures_last_day,
                        counters.domains,
                        counters.mailboxes,
                        counters.messages,
                        counters.pending_messages,
                        counters.failed_messages,
                        counters.api_keys,
                        counters.audit_logs
                    FROM dashboard_counters AS counters
                    CROSS JOIN recent_metrics AS recent
                    WHERE counters.singleton_id = 1
                    """,
                    params,
                ).fetchone()
                values = {} if row is None else {key: int(row[key] or 0) for key in row.keys()}
                recent_messages = [
                    dict(item)
                    for item in connection.execute(
                        """
                        SELECT id, subject, from_addr, received_at, parse_status, attachment_count
                        FROM messages
                        ORDER BY received_at DESC, id DESC
                        LIMIT 5
                        """
                    ).fetchall()
                ]
                recent_domains = [
                    dict(item)
                    for item in connection.execute(
                        """
                        SELECT id, root_domain_ascii, is_active, created_at
                        FROM domains
                        ORDER BY created_at DESC, id DESC
                        LIMIT 5
                        """
                    ).fetchall()
                ]
                chart = _delivery_chart(connection, hours=24)
        except Exception as exc:
            return {
                "ok": False,
                "error_type": type(exc).__name__,
                "free_bytes": 0,
                "recent_messages": [],
                "recent_domains": [],
                "delivery_chart": self._empty_delivery_chart(),
                **sizes,
            }

        free_bytes = 0
        try:
            free_bytes = int(shutil.disk_usage(database_path.parent).free)
        except OSError:
            pass
        return {
            "ok": True,
            "error_type": None,
            "free_bytes": free_bytes,
            "recent_messages": recent_messages,
            "recent_domains": recent_domains,
            "delivery_chart": chart,
            **sizes,
            **values,
        }

    def _disk_snapshot(self) -> dict[str, Any]:
        root = Path(self.runtime.settings.storage_root)
        threshold = float(self.runtime.settings.disk_warning_threshold_percent)
        try:
            usage = shutil.disk_usage(root)
        except OSError as exc:
            return {
                "ok": False,
                "error_type": type(exc).__name__,
                "total_bytes": 0,
                "used_bytes": 0,
                "free_bytes": 0,
                "used_percent": 0.0,
                "warning_threshold_percent": threshold,
            }
        used_percent = 0.0 if usage.total == 0 else (usage.used / usage.total) * 100
        return {
            "ok": True,
            "error_type": None,
            "total_bytes": int(usage.total),
            "used_bytes": int(usage.used),
            "free_bytes": int(usage.free),
            "used_percent": round(used_percent, 2),
            "warning_threshold_percent": threshold,
        }

    def _http_snapshot(self) -> dict[str, Any]:
        registry = self.runtime.observability.metrics
        with registry._lock:  # The registry intentionally owns consistency for these cumulative counters.
            requests_total = sum(int(value) for value in registry._http_requests.values())
            in_flight = int(registry._http_in_flight)
            duration_count = sum(int(value) for value in registry._http_duration_count.values())
            duration_sum = sum(float(value) for value in registry._http_duration_sum.values())
            aggregate_buckets = [0 for _ in HTTP_DURATION_BUCKETS]
            for counts in registry._http_duration_buckets.values():
                for index, count in enumerate(counts):
                    aggregate_buckets[index] += int(count)

        p95_ms: float | None = None
        if duration_count > 0:
            target = math.ceil(duration_count * 0.95)
            for upper_bound, count in zip(HTTP_DURATION_BUCKETS, aggregate_buckets, strict=True):
                if count >= target:
                    p95_ms = round(upper_bound * 1000, 3)
                    break
            if p95_ms is None:
                p95_ms = round(HTTP_DURATION_BUCKETS[-1] * 1000, 3)

        sampled_at = self._clock()
        with self._rate_lock:
            previous_total = self._last_request_total
            previous_at = self._last_request_sample_at
            self._last_request_total = requests_total
            self._last_request_sample_at = sampled_at

        requests_per_second: float | None
        rate_window_seconds: float
        rate_kind: str
        if previous_total is not None and previous_at is not None and requests_total >= previous_total:
            rate_window_seconds = max(sampled_at - previous_at, 0.0)
            requests_per_second = (
                (requests_total - previous_total) / rate_window_seconds if rate_window_seconds > 0 else None
            )
            rate_kind = "interval"
        else:
            started_at = float(getattr(self.runtime.observability, "started_monotonic", sampled_at))
            rate_window_seconds = max(sampled_at - started_at, 0.0)
            requests_per_second = requests_total / rate_window_seconds if rate_window_seconds > 0 else None
            rate_kind = "process_average"

        return {
            "enabled": bool(registry.enabled),
            "requests_total": requests_total,
            "requests_in_flight": in_flight,
            "requests_per_second": None if requests_per_second is None else round(requests_per_second, 3),
            "rate_window_seconds": round(rate_window_seconds, 3),
            "rate_kind": rate_kind,
            "p95_ms": p95_ms,
            "average_ms": None if duration_count == 0 else round((duration_sum / duration_count) * 1000, 3),
        }

    def _background_snapshot(self) -> dict[str, dict[str, Any]]:
        try:
            snapshot = self.runtime.observability.background.snapshot()
        except Exception:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for name, state in snapshot.items():
            failures = int(state.get("consecutive_failures") or 0)
            running = bool(state.get("running"))
            status = "danger" if failures >= 3 or not running else ("warning" if failures else "ok")
            result[str(name)] = {
                "name": str(name),
                "running": running,
                "in_progress": bool(state.get("in_progress")),
                "last_started_at": state.get("last_started_at"),
                "last_success_at": state.get("last_success_at"),
                "last_error_at": state.get("last_error_at"),
                "last_error_type": state.get("last_error_type"),
                "consecutive_failures": failures,
                "total_successes": int(state.get("total_successes") or 0),
                "total_failures": int(state.get("total_failures") or 0),
                "status": status,
            }
        return result

    def _cleanup_snapshot(self, state: dict[str, Any] | None) -> dict[str, Any]:
        if not state:
            return {
                "status": "unknown",
                "last_success_at": None,
                "last_error_at": None,
                "last_error_type": None,
                "consecutive_failures": 0,
            }
        last_success_at = state.get("last_success_at")
        last_error_at = state.get("last_error_at")
        failures = int(state.get("consecutive_failures") or 0)
        if failures > 0 or (last_error_at and (not last_success_at or str(last_error_at) > str(last_success_at))):
            status = "failure"
        elif last_success_at:
            status = "success"
        else:
            status = "unknown"
        return {
            "status": status,
            "last_success_at": last_success_at,
            "last_error_at": last_error_at,
            "last_error_type": state.get("last_error_type"),
            "consecutive_failures": failures,
        }

    def _parse_queue_snapshot(
        self,
        pending_messages: int,
        failed_messages: int,
    ) -> dict[str, Any]:
        queue = self.runtime.parse_queue
        try:
            queued = int(queue.queued_messages)
        except Exception:
            queued = 0
        try:
            active_workers = int(queue.active_messages)
        except Exception:
            active_workers = 0
        return {
            "running": bool(getattr(queue, "is_running", False)),
            "queued": queued,
            "active_workers": active_workers,
            "reserved_messages": max(int(getattr(queue, "reserved_messages", 0)), 0),
            "reserved_bytes": max(int(getattr(queue, "reserved_bytes", 0)), 0),
            "max_messages": max(int(getattr(queue, "max_messages", 0)), 0),
            "max_bytes": max(int(getattr(queue, "max_bytes", 0)), 0),
            "pending_messages": max(int(pending_messages), 0),
            "failed_messages": max(int(failed_messages), 0),
        }

    def _active_smtp_connections(self) -> int:
        try:
            return max(int(self.runtime.active_smtp_connection_count()), 0)
        except Exception:
            return 0

    def _ingestd_snapshot(self) -> dict[str, Any]:
        """Read and validate the C++ ingest daemon's atomic heartbeat file."""

        stale_after_seconds = float(INGEST_STATUS_FRESH_SECONDS)
        empty: dict[str, Any] = {
            "state": "missing",
            "present": False,
            "online": False,
            "stale": False,
            "instance_id": None,
            "pid": None,
            "updated_at": None,
            "age_seconds": None,
            "stale_after_seconds": stale_after_seconds,
            "queue_messages": None,
            "queue_bytes": None,
            "active_connections": None,
            "max_connections": None,
            "error_type": None,
        }
        try:
            status_path = Path(self.runtime.settings.storage_root) / INGEST_STATUS_FILENAME
        except Exception as exc:
            return {**empty, "state": "invalid", "error_type": type(exc).__name__}

        try:
            with status_path.open("rb") as heartbeat:
                stat_result = os.fstat(heartbeat.fileno())
                if stat_result.st_size > _INGEST_STATUS_MAX_BYTES:
                    raise ValueError("ingest status exceeds size limit")
                raw_status = heartbeat.read(_INGEST_STATUS_MAX_BYTES + 1)
        except FileNotFoundError:
            return empty
        except OSError as exc:
            return {
                **empty,
                "state": "invalid",
                "present": True,
                "error_type": type(exc).__name__,
            }
        except ValueError as exc:
            return {
                **empty,
                "state": "invalid",
                "present": True,
                "error_type": type(exc).__name__,
            }

        age_seconds = round(max(float(self._wall_clock()) - float(stat_result.st_mtime), 0.0), 3)
        invalid = {
            **empty,
            "state": "invalid",
            "present": True,
            "age_seconds": age_seconds,
        }
        try:
            payload = json.loads(raw_status)
            if not isinstance(payload, dict):
                raise ValueError("ingest status root must be an object")

            instance_id = payload.get("instance_id")
            if (
                not isinstance(instance_id, str)
                or not instance_id.strip()
                or len(instance_id) > 128
                or any(character.isspace() for character in instance_id)
            ):
                raise ValueError("invalid ingest instance id")
            instance_id = instance_id.strip()

            pid = self._nonnegative_status_integer(payload.get("pid"), "pid", minimum=1)
            queue_messages = self._nonnegative_status_integer(
                payload.get("queue_messages"),
                "queue_messages",
            )
            queue_bytes = self._nonnegative_status_integer(payload.get("queue_bytes"), "queue_bytes")

            updated_at = payload.get("updated_at")
            if not isinstance(updated_at, str) or not updated_at:
                raise ValueError("invalid ingest updated_at")
            parsed_updated_at = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            if parsed_updated_at.tzinfo is None:
                raise ValueError("ingest updated_at must include a timezone")

            raw_active_connections = payload.get("active_connections")
            active_connections = (
                None
                if raw_active_connections is None
                else self._nonnegative_status_integer(raw_active_connections, "active_connections")
            )
            raw_max_connections = payload.get("max_connections")
            max_connections = (
                None
                if raw_max_connections is None
                else self._nonnegative_status_integer(raw_max_connections, "max_connections")
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, OverflowError) as exc:
            return {**invalid, "error_type": type(exc).__name__}

        stale = age_seconds > stale_after_seconds
        return {
            **empty,
            "state": "stale" if stale else "online",
            "present": True,
            "online": not stale,
            "stale": stale,
            "instance_id": instance_id,
            "pid": pid,
            "updated_at": updated_at,
            "age_seconds": age_seconds,
            "queue_messages": queue_messages,
            "queue_bytes": queue_bytes,
            "active_connections": active_connections,
            "max_connections": max_connections,
        }

    @staticmethod
    def _nonnegative_status_integer(value: Any, field: str, *, minimum: int = 0) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"invalid ingest {field}")
        return value

    def _operational_snapshot(self) -> dict[str, Any]:
        try:
            snapshot = dict(self.runtime.operational_state())
        except Exception as exc:
            return {"ok": False, "error_type": type(exc).__name__}
        snapshot["ok"] = bool(snapshot.get("ok"))
        return snapshot

    def _alerts(
        self,
        *,
        operational: dict[str, Any],
        database: dict[str, Any],
        disk: dict[str, Any],
        parse_queue: dict[str, Any],
        background_tasks: dict[str, dict[str, Any]],
        cleanup: dict[str, Any],
        ingestd: dict[str, Any],
    ) -> list[dict[str, str]]:
        alerts: list[dict[str, str]] = []

        def add(severity: str, code: str, title: str, detail: str) -> None:
            alerts.append({"severity": severity, "code": code, "title": title, "detail": detail})

        if not operational.get("ok"):
            add("danger", "runtime_not_ready", "运行时未就绪", "解析队列或后台维护任务未完整运行。")
        if not database.get("ok"):
            add("danger", "database_unavailable", "数据库不可用", "无法读取仪表盘数据库快照。")

        ingestd_state = str(ingestd.get("state") or "invalid")
        if ingestd_state == "missing":
            add(
                "info",
                "ingestd_status_missing",
                "未发现 C++ 收件进程",
                "未读取到 ingestd 心跳；使用 Python SMTP 兼容模式时这是正常状态。",
            )
        elif ingestd_state == "invalid":
            add(
                "warning",
                "ingestd_status_invalid",
                "C++ 收件状态无效",
                "ingestd 心跳文件无法解析或字段不完整，请检查进程与存储目录。",
            )
        elif ingestd_state == "stale":
            age_seconds = float(ingestd.get("age_seconds") or 0.0)
            add(
                "danger",
                "ingestd_status_stale",
                "C++ 收件心跳已中断",
                f"实例 {ingestd.get('instance_id') or 'unknown'} 已 {age_seconds:.1f} 秒未刷新状态。",
            )
        elif ingestd_state == "online":
            queue_messages = int(ingestd.get("queue_messages") or 0)
            queue_bytes = int(ingestd.get("queue_bytes") or 0)
            active_connections = int(ingestd.get("active_connections") or 0)
            max_connections = int(ingestd.get("max_connections") or 0)
            if max_connections > 0 and active_connections >= max_connections:
                add(
                    "danger",
                    "ingestd_connections_exhausted",
                    "C++ SMTP 连接已满",
                    f"当前 {active_connections}/{max_connections} 个连接，新的 SMTP 连接将被拒绝。",
                )
            elif max_connections > 0 and active_connections >= math.ceil(max_connections * 0.9):
                add(
                    "warning",
                    "ingestd_connections_high",
                    "C++ SMTP 连接接近上限",
                    f"当前 {active_connections}/{max_connections} 个连接。",
                )
            if queue_messages >= 1000 or queue_bytes >= 256 * _MIB:
                add(
                    "danger",
                    "ingestd_queue_high",
                    "C++ 收件队列严重积压",
                    f"当前有 {queue_messages} 封、{_human_bytes(queue_bytes)} 邮件等待写入。",
                )
            elif queue_messages > 0 or queue_bytes > 0:
                add(
                    "warning",
                    "ingestd_queue_pending",
                    "C++ 收件队列尚未排空",
                    f"当前有 {queue_messages} 封、{_human_bytes(queue_bytes)} 邮件等待写入。",
                )
        if not disk.get("ok"):
            add("warning", "disk_unknown", "磁盘状态未知", "无法读取存储卷容量。")
        else:
            used_percent = float(disk.get("used_percent") or 0.0)
            threshold = float(disk.get("warning_threshold_percent") or 85.0)
            if used_percent >= threshold:
                add("danger", "disk_threshold", "磁盘逼近上限", f"存储卷已使用 {used_percent:.1f}%。")
            elif used_percent >= max(threshold - 10.0, 0.0):
                add("warning", "disk_pressure", "磁盘空间偏紧", f"存储卷已使用 {used_percent:.1f}%。")
            minimum_free = int(getattr(self.runtime.settings, "readiness_min_free_disk_bytes", 0))
            if int(disk.get("free_bytes") or 0) < minimum_free:
                add("danger", "disk_free_low", "可用磁盘不足", "可用空间低于运行就绪阈值。")

        pending = int(parse_queue.get("pending_messages") or 0)
        queued = int(parse_queue.get("queued") or 0)
        backlog = max(pending, queued)
        if backlog >= 1000:
            add("danger", "parse_backlog_high", "解析积压严重", f"当前至少有 {backlog} 封邮件等待解析。")
        elif backlog > 0:
            add("warning", "parse_backlog", "存在解析积压", f"当前至少有 {backlog} 封邮件等待解析。")

        failures = int(database.get("parse_failures_last_day") or 0)
        if failures >= 10:
            add("danger", "parse_failures_high", "解析失败过多", f"最近 24 小时发生 {failures} 次解析失败。")
        elif failures > 0:
            add("warning", "parse_failures", "存在解析失败", f"最近 24 小时发生 {failures} 次解析失败。")

        for name, state in background_tasks.items():
            consecutive = int(state.get("consecutive_failures") or 0)
            if consecutive >= 3:
                add("danger", f"background_{name}", "后台任务持续失败", f"{name} 已连续失败 {consecutive} 次。")
            elif consecutive > 0:
                add("warning", f"background_{name}", "后台任务失败", f"{name} 已连续失败 {consecutive} 次。")

        if cleanup["status"] == "failure":
            add("warning", "cleanup_failed", "最近清理失败", "过期邮件清理任务最近一次运行失败。")
        elif cleanup["status"] == "unknown":
            add("info", "cleanup_unknown", "尚无清理运行数据", "任务已注册，但还没有成功或失败记录。")

        wal_bytes = int(database.get("wal_bytes") or 0)
        database_bytes = int(database.get("database_bytes") or 0)
        if wal_bytes > max(256 * _MIB, database_bytes * 2):
            add("warning", "wal_large", "WAL 文件偏大", "数据库预写日志需要关注检查点状态。")
        return alerts

    def _legacy_template_context(
        self,
        snapshot: dict[str, Any],
        database: dict[str, Any],
        totals: dict[str, int],
    ) -> dict[str, Any]:
        mail = snapshot["mail"]
        disk = snapshot["disk"]
        pending = int(snapshot["parse_queue"]["pending_messages"])
        failed_messages = int(database.get("failed_messages", 0))
        disk_percent = float(disk.get("used_percent") or 0.0)
        threshold = float(disk.get("warning_threshold_percent") or 85.0)
        disk_state = "danger" if not disk.get("ok") or disk_percent >= threshold else (
            "warning" if disk_percent >= max(threshold - 10.0, 0.0) else "ok"
        )
        return {
            "live_stats": [
                {
                    "label": "当前 SMTP 会话",
                    "value": snapshot["smtp"]["active_connections"],
                    "hint": "当前仍与监听器保持连接的 SMTP 会话。",
                    "state": "ok" if snapshot["smtp"]["active_connections"] else "idle",
                },
                {
                    "label": "1 分钟收到邮件",
                    "value": mail["received_last_minute"],
                    "hint": "最近 60 秒进入持久化层的独立邮件数。",
                    "state": "ok",
                },
                {
                    "label": "1 分钟投递",
                    "value": mail["deliveries_last_minute"],
                    "hint": "最近 60 秒产生的收件人投递数。",
                    "state": "ok",
                },
                {
                    "label": "HTTP 吞吐",
                    "value": "—" if snapshot["http"]["requests_per_second"] is None else f"{snapshot['http']['requests_per_second']:.2f}/s",
                    "hint": "管理与公共 API 的最近采样请求速率。",
                    "state": "ok",
                },
            ],
            "today_stats": [
                {
                    "label": "待解析邮件",
                    "value": pending,
                    "hint": "已持久化、仍等待 MIME 解析。",
                    "state": "warning" if pending else "ok",
                },
                {
                    "label": "解析失败总数",
                    "value": failed_messages,
                    "hint": "当前数据库中解析状态为失败的邮件。",
                    "state": "danger" if failed_messages else "ok",
                },
                {
                    "label": "24 小时 SMTP 拒绝",
                    "value": mail["rejected_last_day"],
                    "hint": "最近 24 小时被 SMTP 策略拒绝的收件人命令。",
                    "state": "warning" if mail["rejected_last_day"] else "ok",
                },
                {
                    "label": "磁盘使用",
                    "value": f"{disk_percent:.1f}%" if disk.get("ok") else "未知",
                    "hint": f"已用 {_human_bytes(int(disk.get('used_bytes') or 0))} / {_human_bytes(int(disk.get('total_bytes') or 0))}，阈值 {threshold:.0f}%。",
                    "state": disk_state,
                },
            ],
            "totals": [
                {"label": "已接入域名", "value": totals["domains"], "icon": "globe"},
                {"label": "已收录邮箱", "value": totals["mailboxes"], "icon": "inbox"},
                {"label": "邮件总数", "value": totals["messages"], "icon": "mail"},
                {"label": "API 密钥", "value": totals["api_keys"], "icon": "key-round"},
                {"label": "审计记录", "value": totals["audit_logs"], "icon": "scroll-text"},
            ],
        }

    @staticmethod
    def _empty_delivery_chart() -> dict[str, Any]:
        return {
            "buckets": [],
            "peak": 0,
            "total": 0,
            "line_path": "",
            "area_path": "",
            "ticks": [],
            "view_width": 1000,
            "view_height": 240,
            "baseline_y": 210.0,
            "pad_top": 18.0,
        }


def get_dashboard_service(app: Any) -> DashboardService:
    runtime = app.state.runtime
    service = getattr(app.state, "dashboard_service", None)
    if not isinstance(service, DashboardService) or service.runtime is not runtime:
        service = DashboardService(runtime)
        app.state.dashboard_service = service
    return service


__all__ = ["DashboardService", "get_dashboard_service"]
