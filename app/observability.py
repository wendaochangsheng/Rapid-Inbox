from __future__ import annotations

import asyncio
import copy
import json
import logging
import logging.handlers
import math
import os
import queue
import re
import resource
import shutil
import sqlite3
import sys
import threading
from collections import defaultdict
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
from importlib import metadata
from pathlib import Path
from time import monotonic, process_time
from typing import Any
from uuid import uuid4

from app.config import Settings


SERVICE_NAME = "rapid-inbox"
HTTP_DURATION_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
KNOWN_HTTP_METHODS = {"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}
REQUEST_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
BACKGROUND_FAILURES_BEFORE_NOT_READY = 3
MAX_EXPORTED_METRIC_VALUE = (1 << 63) - 1
INGESTD_STATES = ("unknown", "missing", "invalid", "stale", "online")
CLEANUP_STATES = ("unknown", "success", "failure")
LOG_QUEUE_CAPACITY = 4096
LOG_SHUTDOWN_FLUSH_TIMEOUT_SECONDS = 1.0
LOG_DROP_REASONS = ("queue_full", "enqueue_error", "sink_error", "shutdown_timeout")

_request_id: ContextVar[str] = ContextVar("rapid_inbox_request_id", default="")
_http_logger = logging.getLogger("rapid_inbox.http")
_background_logger = logging.getLogger("rapid_inbox.background")
_readiness_logger = logging.getLogger("rapid_inbox.readiness")


def _process_resident_memory_bytes(
    statm_path: Path = Path("/proc/self/statm"),
) -> int:
    """Return current RSS where the operating system exposes it cheaply.

    ``ru_maxrss`` is a lifetime high-water mark and can even survive ``exec``
    on Linux.  Exporting it as current resident memory made a freshly started
    HTTP worker appear to retain memory used by its launcher.  Linux is the
    primary deployment target, so read the process page counters directly and
    retain ``getrusage`` only as a portable fallback.
    """

    try:
        fields = statm_path.read_text(encoding="ascii").split()
        resident_pages = int(fields[1])
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        if resident_pages >= 0 and page_size > 0:
            return resident_pages * page_size
    except (IndexError, OSError, TypeError, ValueError):
        pass

    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(rss if sys.platform == "darwin" else rss * 1024)


_STANDARD_LOG_RECORD_FIELDS = frozenset(
    logging.LogRecord("", 0, "", 0, "", (), None).__dict__
)
_SAFE_STRUCTURED_FIELDS = frozenset(
    {
        "consecutive_failures",
        "count",
        "delivery_id",
        "duration_ms",
        "error_type",
        "event",
        "failed_checks",
        "mailbox",
        "message_id",
        "method",
        "node_id",
        "outcome",
        "remote_ip",
        "route",
        "service",
        "smtp_session_id",
        "status_code",
        "task",
    }
)


def _utc_log_timestamp(created: float) -> str:
    return (
        datetime.fromtimestamp(created, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": _utc_log_timestamp(record.created),
            "level": record.levelname,
            "service": getattr(record, "service", SERVICE_NAME),
            "logger": record.name,
            "message": record.getMessage(),
        }
        request_id = getattr(record, "request_id", None) or current_request_id()
        if request_id:
            payload["request_id"] = request_id
        for field_name in _SAFE_STRUCTURED_FIELDS:
            if field_name in _STANDARD_LOG_RECORD_FIELDS or field_name == "service":
                continue
            value = getattr(record, field_name, None)
            if value is not None:
                payload[field_name] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str)


class TextLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        fields = [
            _utc_log_timestamp(record.created),
            f"level={record.levelname}",
            f"service={getattr(record, 'service', SERVICE_NAME)}",
            f"logger={record.name}",
        ]
        request_id = getattr(record, "request_id", None) or current_request_id()
        if request_id:
            fields.append(f"request_id={request_id}")
        for field_name in sorted(_SAFE_STRUCTURED_FIELDS):
            if field_name == "service":
                continue
            value = getattr(record, field_name, None)
            if value is not None:
                fields.append(f"{field_name}={json.dumps(value, ensure_ascii=False, default=str)}")
        fields.append(record.getMessage())
        if record.exc_info:
            fields.append(self.formatException(record.exc_info))
        return " ".join(fields)


class BoundedQueueLogHandler(logging.handlers.QueueHandler):
    """Non-blocking QueueHandler that never falls back to synchronous stderr I/O."""

    def __init__(
        self,
        log_queue: queue.Queue[logging.LogRecord],
        *,
        on_drop: Callable[[str, int], None],
    ) -> None:
        super().__init__(log_queue)
        self._on_drop = on_drop

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        # QueueHandler.prepare() formats on the caller thread. Keep formatting,
        # exception rendering and stream I/O entirely on the listener instead.
        prepared = copy.copy(record)
        if not getattr(prepared, "request_id", None):
            request_id = current_request_id()
            if request_id:
                prepared.request_id = request_id
        return prepared

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(self.prepare(record))
        except queue.Full:
            self._on_drop("queue_full", 1)
        except Exception:
            # logging.Handler.handleError() can synchronously write a traceback
            # to stderr. A failed enqueue is deliberately counted and dropped.
            self._on_drop("enqueue_error", 1)


class AsyncLogDispatcher:
    """Own a bounded log queue and one daemon I/O thread."""

    def __init__(
        self,
        sink: logging.Handler,
        *,
        queue_capacity: int = LOG_QUEUE_CAPACITY,
        metrics: MetricsRegistry | None = None,
    ) -> None:
        if queue_capacity < 1:
            raise ValueError("log queue capacity must be positive")
        self.sink = sink
        self.queue_capacity = int(queue_capacity)
        self._queue: queue.Queue[logging.LogRecord] = queue.Queue(maxsize=self.queue_capacity)
        self._metrics = metrics
        self._closing = threading.Event()
        self._abandon = threading.Event()
        self.handler = BoundedQueueLogHandler(self._queue, on_drop=self._record_drop)
        setattr(self.handler, "_rapid_inbox_handler", True)
        self._thread = threading.Thread(
            target=self._run,
            name="rapid-inbox-log-listener",
            daemon=True,
        )
        self._thread.start()

    @property
    def pending_count(self) -> int:
        return self._queue.qsize()

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def configure(
        self,
        *,
        level: int,
        formatter: logging.Formatter,
        metrics: MetricsRegistry | None,
    ) -> None:
        self._metrics = metrics
        self.handler.setLevel(level)
        self.sink.setLevel(level)
        self.sink.setFormatter(formatter)

    def shutdown(self, *, timeout_seconds: float = LOG_SHUTDOWN_FLUSH_TIMEOUT_SECONDS) -> bool:
        """Try to flush queued records, abandoning the queue after the deadline."""

        self._closing.set()
        self._thread.join(timeout=max(float(timeout_seconds), 0.0))
        if not self._thread.is_alive():
            self.handler.close()
            return True

        self._abandon.set()
        dropped = self._discard_pending()
        if dropped:
            self._record_drop("shutdown_timeout", dropped)
        # A stream handler may be blocked inside an OS write. The listener is a
        # daemon and will exit after that write returns; shutdown itself remains
        # bounded and never waits indefinitely for an external log consumer.
        self.handler.close()
        return False

    def _run(self) -> None:
        while True:
            if self._abandon.is_set():
                return
            if self._closing.is_set() and self._queue.empty():
                return
            try:
                record = self._queue.get(timeout=0.05)
            except queue.Empty:
                continue
            try:
                if self._abandon.is_set():
                    self._record_drop("shutdown_timeout", 1)
                    continue
                self.sink.handle(record)
            except Exception:
                # Never log listener failures through the same queue.
                self._record_drop("sink_error", 1)
            finally:
                self._queue.task_done()

    def _discard_pending(self) -> int:
        discarded = 0
        while True:
            try:
                self._queue.get_nowait()
            except queue.Empty:
                return discarded
            else:
                discarded += 1
                self._queue.task_done()

    def _record_drop(self, reason: str, count: int) -> None:
        metrics = self._metrics
        if metrics is not None:
            metrics.log_dropped(reason, count=count)


_logging_dispatcher_lock = threading.Lock()
_logging_dispatcher: AsyncLogDispatcher | None = None


def configure_logging(
    settings: Settings,
    *,
    metrics: MetricsRegistry | None = None,
) -> AsyncLogDispatcher:
    """Install an idempotent bounded application logger without disturbing tests."""

    formatter: logging.Formatter
    formatter = JsonLogFormatter() if settings.log_format == "json" else TextLogFormatter()
    root_logger = logging.getLogger()
    level = getattr(logging, settings.log_level)
    root_logger.setLevel(level)

    global _logging_dispatcher
    with _logging_dispatcher_lock:
        dispatcher = _logging_dispatcher
        if dispatcher is None or not dispatcher.is_alive:
            for existing_handler in tuple(root_logger.handlers):
                if getattr(existing_handler, "_rapid_inbox_handler", False):
                    root_logger.removeHandler(existing_handler)
            dispatcher = AsyncLogDispatcher(logging.StreamHandler(), metrics=metrics)
            root_logger.addHandler(dispatcher.handler)
            _logging_dispatcher = dispatcher
        elif dispatcher.handler not in root_logger.handlers:
            root_logger.addHandler(dispatcher.handler)
        dispatcher.configure(level=level, formatter=formatter, metrics=metrics)

    # Uvicorn's default access line contains the raw query string. Disable it and
    # use the bounded, redacted route-template logger below instead.
    logging.getLogger("uvicorn.access").disabled = True
    # These client libraries log complete URLs at INFO. Application access logs
    # are emitted above from route templates, so their INFO records add no value
    # and can disclose query credentials during tests or internal callbacks.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    for logger_name in ("uvicorn.error", "uvicorn"):
        for handler in logging.getLogger(logger_name).handlers:
            handler.setFormatter(formatter)
    return dispatcher


def shutdown_logging(
    *,
    timeout_seconds: float = LOG_SHUTDOWN_FLUSH_TIMEOUT_SECONDS,
) -> bool:
    """Detach the process logger and bound the final queue flush."""

    global _logging_dispatcher
    with _logging_dispatcher_lock:
        dispatcher = _logging_dispatcher
        _logging_dispatcher = None
        if dispatcher is None:
            return True
        root_logger = logging.getLogger()
        if dispatcher.handler in root_logger.handlers:
            root_logger.removeHandler(dispatcher.handler)
    return dispatcher.shutdown(timeout_seconds=timeout_seconds)


def current_request_id() -> str:
    return _request_id.get()


@lru_cache(maxsize=1)
def application_version() -> str:
    try:
        return metadata.version("rapid-inbox")
    except metadata.PackageNotFoundError:
        return "0+unknown"


def _prometheus_escape(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(values: tuple[tuple[str, str], ...]) -> str:
    if not values:
        return ""
    return "{" + ",".join(f'{key}="{_prometheus_escape(value)}"' for key, value in values) + "}"


def _number(value: float | int) -> str:
    if isinstance(value, int):
        return str(value)
    if not math.isfinite(value):
        return "0"
    return format(value, ".12g")


def _metric_value(
    value: object,
    *,
    maximum: int | float = MAX_EXPORTED_METRIC_VALUE,
) -> int | float | None:
    """Accept only finite, non-negative snapshot values with bounded output size."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value < 0 or value > maximum:
        return None
    return value


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _append_metric_family(
    lines: list[str],
    *,
    name: str,
    help_text: str,
    metric_type: str,
    samples: list[tuple[tuple[tuple[str, str], ...], int | float]],
) -> None:
    if not samples:
        return
    lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} {metric_type}"))
    lines.extend(f"{name}{_labels(labels)} {_number(value)}" for labels, value in samples)


def _failed_parse_messages(snapshot: Mapping[str, Any], parse_queue: Mapping[str, Any]) -> int | float | None:
    failed = _metric_value(parse_queue.get("failed_messages"))
    if failed is not None:
        return failed

    # Older or externally supplied snapshots may only expose this count in the
    # legacy presentation section. Keep that compatibility fallback while the
    # live service uses the machine-readable parse_queue field above.
    today_stats = snapshot.get("today_stats")
    if not isinstance(today_stats, list):
        return None
    for item in today_stats:
        stat = _mapping(item)
        if stat.get("label") == "解析失败总数":
            return _metric_value(stat.get("value"))
    return None


def _operational_metric_lines(snapshot_value: object) -> list[str]:
    snapshot_available = isinstance(snapshot_value, Mapping)
    snapshot = _mapping(snapshot_value)
    lines: list[str] = []
    _append_metric_family(
        lines,
        name="rapid_inbox_dashboard_snapshot_available",
        help_text="Whether the shared operational dashboard snapshot is available.",
        metric_type="gauge",
        samples=[((), int(snapshot_available))],
    )
    if not snapshot_available:
        return lines

    ingestd = _mapping(snapshot.get("ingestd"))
    raw_ingestd_state = ingestd.get("state")
    ingestd_state = raw_ingestd_state if raw_ingestd_state in INGESTD_STATES else "unknown"
    ingestd_online = ingestd_state == "online" and ingestd.get("online") is True
    _append_metric_family(
        lines,
        name="rapid_inbox_ingestd_status",
        help_text="C++ ingest daemon heartbeat state as a one-hot gauge.",
        metric_type="gauge",
        samples=[((("state", state),), int(state == ingestd_state)) for state in INGESTD_STATES],
    )
    _append_metric_family(
        lines,
        name="rapid_inbox_ingestd_online",
        help_text="Whether the C++ ingest daemon has a fresh valid heartbeat.",
        metric_type="gauge",
        samples=[((), int(ingestd_online))],
    )
    if ingestd_online:
        for metric_name, field, help_text in (
            (
                "rapid_inbox_ingestd_queue_messages",
                "queue_messages",
                "Messages currently reserved in the C++ ingest queue.",
            ),
            (
                "rapid_inbox_ingestd_queue_bytes",
                "queue_bytes",
                "Bytes currently reserved in the C++ ingest queue.",
            ),
            (
                "rapid_inbox_ingestd_active_connections",
                "active_connections",
                "Active SMTP connections reported by the C++ ingest daemon.",
            ),
            (
                "rapid_inbox_ingestd_max_connections",
                "max_connections",
                "Configured SMTP connection capacity of the C++ ingest daemon.",
            ),
        ):
            value = _metric_value(ingestd.get(field))
            _append_metric_family(
                lines,
                name=metric_name,
                help_text=help_text,
                metric_type="gauge",
                samples=[] if value is None else [((), value)],
            )

    database = _mapping(snapshot.get("database"))
    database_ok = database.get("ok") is True
    _append_metric_family(
        lines,
        name="rapid_inbox_database_snapshot_ok",
        help_text="Whether the dashboard database query succeeded.",
        metric_type="gauge",
        samples=[((), int(database_ok))],
    )
    if database_ok:
        for metric_name, field, help_text in (
            (
                "rapid_inbox_database_bytes",
                "database_bytes",
                "SQLite main database file size in bytes.",
            ),
            (
                "rapid_inbox_database_wal_bytes",
                "wal_bytes",
                "SQLite write-ahead log file size in bytes.",
            ),
            (
                "rapid_inbox_database_shm_bytes",
                "shm_bytes",
                "SQLite shared-memory file size in bytes.",
            ),
        ):
            value = _metric_value(database.get(field))
            _append_metric_family(
                lines,
                name=metric_name,
                help_text=help_text,
                metric_type="gauge",
                samples=[] if value is None else [((), value)],
            )

    smtp = _mapping(snapshot.get("smtp"))
    smtp_active_samples: list[tuple[tuple[tuple[str, str], ...], int | float]] = []
    python_active = _metric_value(smtp.get("python_active_connections"))
    if python_active is not None:
        smtp_active_samples.append(((("implementation", "python"),), python_active))
    ingestd_active = _metric_value(ingestd.get("active_connections")) if ingestd_online else None
    if ingestd_active is not None:
        smtp_active_samples.append(((("implementation", "ingestd"),), ingestd_active))
    _append_metric_family(
        lines,
        name="rapid_inbox_smtp_active_connections",
        help_text="Active SMTP connections by bounded implementation name.",
        metric_type="gauge",
        samples=smtp_active_samples,
    )
    open_sessions = _metric_value(smtp.get("open_sessions")) if database_ok else None
    _append_metric_family(
        lines,
        name="rapid_inbox_smtp_open_sessions",
        help_text="Persisted SMTP sessions whose status is open.",
        metric_type="gauge",
        samples=[] if open_sessions is None else [((), open_sessions)],
    )

    parse_queue = _mapping(snapshot.get("parse_queue"))
    parse_running = parse_queue.get("running")
    if isinstance(parse_running, bool):
        _append_metric_family(
            lines,
            name="rapid_inbox_parse_queue_running",
            help_text="Whether the MIME parse worker queue is running.",
            metric_type="gauge",
            samples=[((), int(parse_running))],
        )
    parse_samples: list[tuple[tuple[tuple[str, str], ...], int | float]] = []
    for state, field in (("queued", "queued"), ("active", "active_workers")):
        value = _metric_value(parse_queue.get(field))
        if value is not None:
            parse_samples.append(((("state", state),), value))
    if database_ok:
        pending = _metric_value(parse_queue.get("pending_messages"))
        failed = _failed_parse_messages(snapshot, parse_queue)
        if pending is not None:
            parse_samples.append(((("state", "pending"),), pending))
        if failed is not None:
            parse_samples.append(((("state", "failed"),), failed))
    _append_metric_family(
        lines,
        name="rapid_inbox_parse_queue_messages",
        help_text="MIME parse work by fixed queue state.",
        metric_type="gauge",
        samples=parse_samples,
    )

    disk = _mapping(snapshot.get("disk"))
    disk_ok = disk.get("ok") is True
    _append_metric_family(
        lines,
        name="rapid_inbox_disk_snapshot_ok",
        help_text="Whether storage volume usage was read successfully.",
        metric_type="gauge",
        samples=[((), int(disk_ok))],
    )
    if disk_ok:
        for metric_name, field, help_text, maximum in (
            (
                "rapid_inbox_disk_total_bytes",
                "total_bytes",
                "Total bytes on the storage volume.",
                MAX_EXPORTED_METRIC_VALUE,
            ),
            (
                "rapid_inbox_disk_used_bytes",
                "used_bytes",
                "Used bytes on the storage volume.",
                MAX_EXPORTED_METRIC_VALUE,
            ),
            (
                "rapid_inbox_disk_free_bytes",
                "free_bytes",
                "Free bytes on the storage volume.",
                MAX_EXPORTED_METRIC_VALUE,
            ),
            (
                "rapid_inbox_disk_used_percent",
                "used_percent",
                "Percentage of the storage volume in use.",
                100.0,
            ),
        ):
            value = _metric_value(disk.get(field), maximum=maximum)
            _append_metric_family(
                lines,
                name=metric_name,
                help_text=help_text,
                metric_type="gauge",
                samples=[] if value is None else [((), value)],
            )

    if database_ok:
        mail = _mapping(snapshot.get("mail"))
        for metric_name, help_text, fields in (
            (
                "rapid_inbox_mail_received",
                "Persisted messages received within the rolling window.",
                (("1m", "received_last_minute"), ("24h", "received_last_day")),
            ),
            (
                "rapid_inbox_mail_deliveries",
                "Recipient deliveries created within the rolling window.",
                (("1m", "deliveries_last_minute"), ("24h", "deliveries_last_day")),
            ),
            (
                "rapid_inbox_mail_rejections",
                "SMTP recipient rejections within the rolling window.",
                (("24h", "rejected_last_day"),),
            ),
            (
                "rapid_inbox_mail_parse_failures",
                "MIME parse failures within the rolling window.",
                (("24h", "parse_failures_last_day"),),
            ),
        ):
            samples = []
            for window, field in fields:
                value = _metric_value(mail.get(field))
                if value is not None:
                    samples.append(((("window", window),), value))
            _append_metric_family(
                lines,
                name=metric_name,
                help_text=help_text,
                metric_type="gauge",
                samples=samples,
            )

    cleanup = _mapping(snapshot.get("cleanup"))
    raw_cleanup_status = cleanup.get("status")
    cleanup_status = raw_cleanup_status if raw_cleanup_status in CLEANUP_STATES else "unknown"
    _append_metric_family(
        lines,
        name="rapid_inbox_cleanup_status",
        help_text="Latest message cleanup outcome as a one-hot gauge.",
        metric_type="gauge",
        samples=[((("status", state),), int(state == cleanup_status)) for state in CLEANUP_STATES],
    )
    cleanup_failures = _metric_value(cleanup.get("consecutive_failures"))
    _append_metric_family(
        lines,
        name="rapid_inbox_cleanup_consecutive_failures",
        help_text="Current consecutive failures of the message cleanup task.",
        metric_type="gauge",
        samples=[] if cleanup_failures is None else [((), cleanup_failures)],
    )
    return lines


class MetricsRegistry:
    """Small bounded-cardinality registry for the HTTP and maintenance hot paths."""

    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled
        self._lock = threading.Lock()
        self._http_in_flight = 0
        self._http_requests: dict[tuple[str, str, str], int] = defaultdict(int)
        self._http_duration_count: dict[tuple[str, str], int] = defaultdict(int)
        self._http_duration_sum: dict[tuple[str, str], float] = defaultdict(float)
        self._http_duration_buckets: dict[tuple[str, str], list[int]] = {}
        self._background_runs: dict[tuple[str, str], int] = defaultdict(int)
        self._background_consecutive_failures: dict[str, int] = defaultdict(int)
        self._log_records_dropped: dict[str, int] = {
            reason: 0 for reason in LOG_DROP_REASONS
        }
        self._ready = 0

    def http_started(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._http_in_flight += 1

    def http_finished(self, method: str, route: str, status_code: int, duration_seconds: float) -> None:
        if not self.enabled:
            return
        normalized_method = method if method in KNOWN_HTTP_METHODS else "OTHER"
        normalized_route = route or "unmatched"
        status_text = str(min(max(int(status_code), 100), 599))
        duration = max(float(duration_seconds), 0.0)
        histogram_key = (normalized_method, normalized_route)
        with self._lock:
            self._http_in_flight = max(self._http_in_flight - 1, 0)
            self._http_requests[(normalized_method, normalized_route, status_text)] += 1
            self._http_duration_count[histogram_key] += 1
            self._http_duration_sum[histogram_key] += duration
            bucket_counts = self._http_duration_buckets.setdefault(
                histogram_key,
                [0 for _ in HTTP_DURATION_BUCKETS],
            )
            for index, upper_bound in enumerate(HTTP_DURATION_BUCKETS):
                if duration <= upper_bound:
                    bucket_counts[index] += 1

    def background_run(self, task_name: str, outcome: str, *, consecutive_failures: int) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._background_runs[(task_name, outcome)] += 1
            self._background_consecutive_failures[task_name] = max(int(consecutive_failures), 0)

    def log_dropped(self, reason: str, *, count: int = 1) -> None:
        if not self.enabled:
            return
        normalized_reason = reason if reason in LOG_DROP_REASONS else "enqueue_error"
        increment = max(int(count), 0)
        if increment == 0:
            return
        with self._lock:
            self._log_records_dropped[normalized_reason] += increment

    def set_ready(self, ready: bool) -> None:
        if not self.enabled:
            return
        with self._lock:
            self._ready = int(ready)

    def render(
        self,
        *,
        started_monotonic: float,
        operational_snapshot: Mapping[str, Any] | None = None,
    ) -> str:
        with self._lock:
            http_in_flight = self._http_in_flight
            http_requests = dict(self._http_requests)
            duration_count = dict(self._http_duration_count)
            duration_sum = dict(self._http_duration_sum)
            duration_buckets = {key: list(value) for key, value in self._http_duration_buckets.items()}
            background_runs = dict(self._background_runs)
            background_failures = dict(self._background_consecutive_failures)
            log_records_dropped = dict(self._log_records_dropped)
            ready = self._ready

        lines = [
            "# HELP rapid_inbox_http_requests_total Completed HTTP requests.",
            "# TYPE rapid_inbox_http_requests_total counter",
        ]
        for (method, route, status_code), value in sorted(http_requests.items()):
            labels = (("method", method), ("route", route), ("status", status_code))
            lines.append(f"rapid_inbox_http_requests_total{_labels(labels)} {value}")

        lines.extend(
            [
                "# HELP rapid_inbox_http_requests_in_flight HTTP requests currently being served.",
                "# TYPE rapid_inbox_http_requests_in_flight gauge",
                f"rapid_inbox_http_requests_in_flight {http_in_flight}",
                "# HELP rapid_inbox_http_request_duration_seconds End-to-end HTTP request duration.",
                "# TYPE rapid_inbox_http_request_duration_seconds histogram",
            ]
        )
        for key in sorted(duration_count):
            method, route = key
            base_labels = (("method", method), ("route", route))
            for upper_bound, count in zip(HTTP_DURATION_BUCKETS, duration_buckets[key], strict=True):
                labels = (*base_labels, ("le", _number(upper_bound)))
                lines.append(f"rapid_inbox_http_request_duration_seconds_bucket{_labels(labels)} {count}")
            labels = (*base_labels, ("le", "+Inf"))
            lines.append(
                f"rapid_inbox_http_request_duration_seconds_bucket{_labels(labels)} {duration_count[key]}"
            )
            lines.append(
                f"rapid_inbox_http_request_duration_seconds_sum{_labels(base_labels)} "
                f"{_number(duration_sum[key])}"
            )
            lines.append(
                f"rapid_inbox_http_request_duration_seconds_count{_labels(base_labels)} {duration_count[key]}"
            )

        lines.extend(
            [
                "# HELP rapid_inbox_background_task_runs_total Background task attempts by outcome.",
                "# TYPE rapid_inbox_background_task_runs_total counter",
            ]
        )
        for (task_name, outcome), value in sorted(background_runs.items()):
            labels = (("task", task_name), ("outcome", outcome))
            lines.append(f"rapid_inbox_background_task_runs_total{_labels(labels)} {value}")
        lines.extend(
            [
                "# HELP rapid_inbox_background_task_consecutive_failures Current consecutive failures.",
                "# TYPE rapid_inbox_background_task_consecutive_failures gauge",
            ]
        )
        for task_name, value in sorted(background_failures.items()):
            lines.append(
                "rapid_inbox_background_task_consecutive_failures"
                f'{_labels((("task", task_name),))} {value}'
            )

        lines.extend(
            [
                "# HELP rapid_inbox_log_records_dropped_total Log records dropped by the bounded asynchronous logger.",
                "# TYPE rapid_inbox_log_records_dropped_total counter",
            ]
        )
        for reason in LOG_DROP_REASONS:
            lines.append(
                "rapid_inbox_log_records_dropped_total"
                f'{_labels((("reason", reason),))} {log_records_dropped[reason]}'
            )

        rss_bytes = _process_resident_memory_bytes()
        lines.extend(
            [
                "# HELP rapid_inbox_ready Whether the HTTP runtime passed its latest readiness probe.",
                "# TYPE rapid_inbox_ready gauge",
                f"rapid_inbox_ready {ready}",
                "# HELP rapid_inbox_process_uptime_seconds Process uptime observed by this runtime.",
                "# TYPE rapid_inbox_process_uptime_seconds gauge",
                f"rapid_inbox_process_uptime_seconds {_number(max(monotonic() - started_monotonic, 0.0))}",
                "# HELP rapid_inbox_process_cpu_seconds_total Process CPU time.",
                "# TYPE rapid_inbox_process_cpu_seconds_total counter",
                f"rapid_inbox_process_cpu_seconds_total {_number(process_time())}",
                "# HELP rapid_inbox_process_resident_memory_bytes Current resident memory reported by the OS.",
                "# TYPE rapid_inbox_process_resident_memory_bytes gauge",
                f"rapid_inbox_process_resident_memory_bytes {rss_bytes}",
            ]
        )
        lines.extend(_operational_metric_lines(operational_snapshot))
        return "\n".join(lines) + "\n"


@dataclass(slots=True)
class BackgroundTaskState:
    name: str
    running: bool = False
    in_progress: bool = False
    last_started_at: str | None = None
    last_success_at: str | None = None
    last_error_at: str | None = None
    last_error_type: str | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0


class BackgroundTaskMonitor:
    def __init__(self, metrics: MetricsRegistry) -> None:
        self._metrics = metrics
        self._lock = threading.Lock()
        self._states: dict[str, BackgroundTaskState] = {}

    def register(self, task_name: str) -> None:
        with self._lock:
            state = self._states.setdefault(task_name, BackgroundTaskState(name=task_name))
            state.running = True

    def begin(self, task_name: str) -> None:
        now = _utc_now()
        with self._lock:
            state = self._states.setdefault(task_name, BackgroundTaskState(name=task_name))
            state.running = True
            state.in_progress = True
            state.last_started_at = now

    def success(self, task_name: str) -> None:
        now = _utc_now()
        with self._lock:
            state = self._states.setdefault(task_name, BackgroundTaskState(name=task_name))
            state.in_progress = False
            state.last_success_at = now
            state.last_error_type = None
            state.last_error = None
            state.consecutive_failures = 0
            state.total_successes += 1
        self._metrics.background_run(task_name, "success", consecutive_failures=0)

    def failure(self, task_name: str, error: BaseException) -> None:
        now = _utc_now()
        with self._lock:
            state = self._states.setdefault(task_name, BackgroundTaskState(name=task_name))
            state.in_progress = False
            state.last_error_at = now
            state.last_error_type = type(error).__name__
            state.last_error = str(error)[:500]
            state.consecutive_failures += 1
            state.total_failures += 1
            consecutive_failures = state.consecutive_failures
        self._metrics.background_run(
            task_name,
            "failure",
            consecutive_failures=consecutive_failures,
        )
        _background_logger.error(
            "background task attempt failed",
            extra={
                "event": "background_task_failure",
                "task": task_name,
                "error_type": type(error).__name__,
                "consecutive_failures": consecutive_failures,
            },
            exc_info=(type(error), error, error.__traceback__),
        )

    def stop(self, task_name: str) -> None:
        with self._lock:
            state = self._states.setdefault(task_name, BackgroundTaskState(name=task_name))
            state.running = False
            state.in_progress = False

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {name: asdict(state) for name, state in self._states.items()}

    def readiness_snapshot(self) -> tuple[bool, dict[str, dict[str, Any]]]:
        snapshot = self.snapshot()
        sanitized: dict[str, dict[str, Any]] = {}
        ready = True
        for name, state in snapshot.items():
            task_ready = (
                bool(state["running"])
                and int(state["consecutive_failures"]) < BACKGROUND_FAILURES_BEFORE_NOT_READY
            )
            ready = ready and task_ready
            sanitized[name] = {
                "ok": task_ready,
                "running": bool(state["running"]),
                "in_progress": bool(state["in_progress"]),
                "consecutive_failures": int(state["consecutive_failures"]),
                "last_success_at": state["last_success_at"],
                "last_error_at": state["last_error_at"],
                "last_error_type": state["last_error_type"],
            }
        return ready, sanitized


class ReadinessProbe:
    def __init__(self, settings: Settings, metrics: MetricsRegistry, *, cache_seconds: float = 0.5) -> None:
        self._settings = settings
        self._metrics = metrics
        self._cache_seconds = max(float(cache_seconds), 0.0)
        self._lock = asyncio.Lock()
        self._cached_at = 0.0
        self._cached_result: dict[str, Any] | None = None
        self._last_logged_ready: bool | None = None

    async def check(self, runtime: Any, *, force: bool = False) -> dict[str, Any]:
        now = monotonic()
        if not force and self._cached_result is not None and now - self._cached_at < self._cache_seconds:
            return self._cached_result
        async with self._lock:
            now = monotonic()
            if not force and self._cached_result is not None and now - self._cached_at < self._cache_seconds:
                return self._cached_result
            runtime_check = runtime.operational_state()
            io_checks = await asyncio.to_thread(self._probe_io)
            background_ready, background = runtime.observability.background.readiness_snapshot()
            checks = {
                "runtime": runtime_check,
                "background_tasks": {"ok": background_ready, "tasks": background},
                **io_checks,
            }
            ready = all(bool(check.get("ok")) for check in checks.values())
            result = {"status": "ready" if ready else "not_ready", "checks": checks}
            self._cached_at = monotonic()
            self._cached_result = result
            self._metrics.set_ready(ready)
            self._log_transition(ready, checks)
            return result

    def _probe_io(self) -> dict[str, dict[str, Any]]:
        return {
            "database": self._probe_database(),
            "storage": self._probe_storage(),
            "disk": self._probe_disk(),
        }

    def _probe_database(self) -> dict[str, Any]:
        try:
            connection = sqlite3.connect(self._settings.database_path, timeout=0.25)
            try:
                connection.execute("SELECT 1 FROM sqlite_schema LIMIT 1").fetchone()
            finally:
                connection.close()
        except Exception as exc:
            return {"ok": False, "error_type": type(exc).__name__}
        return {"ok": True}

    def _probe_storage(self) -> dict[str, Any]:
        probe_path: Path | None = None
        descriptor: int | None = None
        try:
            if not self._settings.storage_root.is_dir() or not self._settings.tmp_dir.is_dir():
                return {"ok": False, "error_type": "StorageDirectoryMissing"}
            probe_path = self._settings.tmp_dir / f".readiness-{os.getpid()}-{uuid4().hex}.tmp"
            descriptor = os.open(probe_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(descriptor)
            descriptor = None
            probe_path.unlink()
        except Exception as exc:
            if descriptor is not None:
                with suppress(OSError):
                    os.close(descriptor)
            if probe_path is not None:
                with suppress(OSError):
                    probe_path.unlink(missing_ok=True)
            return {"ok": False, "error_type": type(exc).__name__}
        return {"ok": True}

    def _probe_disk(self) -> dict[str, Any]:
        try:
            usage = shutil.disk_usage(self._settings.storage_root)
        except Exception as exc:
            return {"ok": False, "error_type": type(exc).__name__}
        required = int(self._settings.readiness_min_free_disk_bytes)
        return {
            "ok": usage.free >= required,
            "free_bytes": int(usage.free),
            "required_free_bytes": required,
        }

    def _log_transition(self, ready: bool, checks: dict[str, dict[str, Any]]) -> None:
        if ready == self._last_logged_ready:
            return
        self._last_logged_ready = ready
        failed_checks = [name for name, check in checks.items() if not bool(check.get("ok"))]
        log = _readiness_logger.info if ready else _readiness_logger.warning
        log(
            "runtime readiness changed",
            extra={
                "event": "readiness_changed",
                "outcome": "ready" if ready else "not_ready",
                "failed_checks": failed_checks,
            },
        )


class Observability:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.started_monotonic = monotonic()
        self.metrics = MetricsRegistry(enabled=settings.metrics_enabled)
        self.background = BackgroundTaskMonitor(self.metrics)
        self.readiness = ReadinessProbe(settings, self.metrics)

    async def run_periodic(
        self,
        task_name: str,
        interval_seconds: float,
        operation: Callable[[], Awaitable[Any]],
    ) -> None:
        self.background.register(task_name)
        try:
            while True:
                await asyncio.sleep(interval_seconds)
                self.background.begin(task_name)
                try:
                    await operation()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self.background.failure(task_name, exc)
                else:
                    self.background.success(task_name)
        finally:
            self.background.stop(task_name)


class HTTPObservabilityMiddleware:
    def __init__(self, app: Any, *, observability: Observability) -> None:
        self.app = app
        self.observability = observability

    async def __call__(self, scope: dict[str, Any], receive: Callable[..., Any], send: Callable[..., Any]) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        request_id = self._request_id(scope)
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        token = _request_id.set(request_id)
        started_at = monotonic()
        status_code: int | None = None
        raised_error: BaseException | None = None
        self.observability.metrics.http_started()

        async def send_with_request_id(message: dict[str, Any]) -> None:
            nonlocal status_code
            if message.get("type") == "http.response.start":
                status_code = int(message.get("status", 200))
                headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != b"x-request-id"
                ]
                headers.append((b"x-request-id", request_id.encode("ascii")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        except BaseException as exc:
            raised_error = exc
            raise
        finally:
            duration = max(monotonic() - started_at, 0.0)
            route = self._route_template(scope)
            final_status = status_code if status_code is not None else 500
            method = self._method(scope)
            self.observability.metrics.http_finished(method, route, final_status, duration)
            if self.observability.settings.request_log_enabled:
                extra = {
                    "event": "http_request",
                    "request_id": request_id,
                    "method": method,
                    "route": route,
                    "status_code": final_status,
                    "duration_ms": round(duration * 1000, 3),
                }
                client = scope.get("client")
                if isinstance(client, (tuple, list)) and client:
                    extra["remote_ip"] = str(client[0])[:128]
                if raised_error is None:
                    _http_logger.info("HTTP request completed", extra=extra)
                else:
                    extra["error_type"] = type(raised_error).__name__
                    _http_logger.error(
                        "HTTP request failed",
                        extra=extra,
                        exc_info=(type(raised_error), raised_error, raised_error.__traceback__),
                    )
            _request_id.reset(token)

    def _request_id(self, scope: dict[str, Any]) -> str:
        for name, value in scope.get("headers", []):
            if name.lower() != b"x-request-id":
                continue
            try:
                candidate = value.decode("ascii")
            except UnicodeDecodeError:
                break
            if REQUEST_ID_PATTERN.fullmatch(candidate):
                return candidate
            break
        return uuid4().hex

    def _method(self, scope: dict[str, Any]) -> str:
        method = str(scope.get("method") or "OTHER").upper()
        return method if method in KNOWN_HTTP_METHODS else "OTHER"

    def _route_template(self, scope: dict[str, Any]) -> str:
        route = scope.get("route")
        route_path = getattr(route, "path", None)
        if not isinstance(route_path, str) or not route_path.startswith("/"):
            return "unmatched"
        return route_path[:256]


__all__ = [
    "BackgroundTaskMonitor",
    "HTTPObservabilityMiddleware",
    "JsonLogFormatter",
    "MetricsRegistry",
    "Observability",
    "TextLogFormatter",
    "application_version",
    "configure_logging",
    "current_request_id",
]
