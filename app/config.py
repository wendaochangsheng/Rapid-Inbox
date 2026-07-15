from __future__ import annotations

import ast
import ipaddress
import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_BOOTSTRAP_ADMIN_PASSWORD = "change-me-now"
DEFAULT_ADMIN_TOKEN = "dev-admin-token"
DEFAULT_PUBLIC_API_KEY = "public-demo-key"

MAX_MESSAGE_SIZE_LIMIT_BYTES = 1_073_741_824
MAX_RECIPIENTS_LIMIT = 10_000
MAX_SMTP_CONNECTIONS_LIMIT = 100_000
MAX_RATE_LIMIT_COUNT = 10_000_000
DEFAULT_SMTP_MAX_CONCURRENT_CONNECTIONS = 1024
# The compatibility SMTP listener closes after DATA by default, so keep the
# anti-churn guard high enough for legitimate relay bursts while still making
# the amount of per-IP sliding-window state finite.
DEFAULT_SMTP_CONNECTION_RATE_LIMIT_COUNT = 60_000
MAX_RETENTION_DAYS = 36_500
# Cleanup runs inside the single SQLite writer transaction. Very large values
# create oversized IN lists/WAL bursts and can exceed SQLite's build-specific
# host-parameter ceiling, so keep each maintenance slice intentionally small.
MAX_CLEANUP_BATCH_SIZE = 10_000
MAX_HTTP_REQUEST_BODY_LIMIT_BYTES = 67_108_864
MAX_HTTP_LIVE_CONNECTIONS_LIMIT = 100_000
MAX_HTTP_CONCURRENCY_LIMIT = 100_000
MAX_HTTP_BODY_MEMORY_BUDGET_BYTES = 8_589_934_592
MAX_PARSE_QUEUE_MESSAGES_LIMIT = 1_000_000
MAX_PARSE_QUEUE_BYTES_LIMIT = 1_099_511_627_776
MAX_MESSAGE_PREVIEW_BODY_BYTES = 16_777_216
MAX_MESSAGE_PREVIEW_HEADERS_BYTES = 1_048_576
MAX_MESSAGE_PREVIEW_INLINE_ITEM_BYTES = 8_388_608
MAX_MESSAGE_PREVIEW_INLINE_TOTAL_BYTES = 33_554_432


@dataclass(slots=True)
class Settings:
    storage_root: Path
    database_path: Path
    bootstrap_admin_username: str = "admin"
    bootstrap_admin_password: str = DEFAULT_BOOTSTRAP_ADMIN_PASSWORD
    session_cookie_name: str = "rapid_inbox_session"
    host: str = "127.0.0.1"
    port: int = 8000
    http_max_request_body_bytes: int = 1_048_576
    http_request_body_timeout_seconds: int = 15
    http_body_memory_budget_bytes: int = 268_435_456
    http_concurrency_limit: int = 1000
    http_live_connection_limit: int = 256
    database_write_queue_capacity: int = 256
    database_write_max_waiters: int = 1024
    database_read_pool_size: int = 1
    database_read_queue_capacity: int = 256
    database_read_max_waiters: int = 1024
    database_read_timeout_seconds: int = 5
    smtp_host: str = "127.0.0.1"
    smtp_port: int = 25
    max_message_size_bytes: int = 52_428_800
    max_recipients_per_message: int = 20
    smtp_idle_timeout_seconds: int = 30
    smtp_max_concurrent_connections: int = DEFAULT_SMTP_MAX_CONCURRENT_CONNECTIONS
    smtp_connection_rate_limit_count: int = DEFAULT_SMTP_CONNECTION_RATE_LIMIT_COUNT
    smtp_connection_rate_limit_window_seconds: int = 60
    smtp_close_after_data: bool = True
    parse_worker_count: int = 4
    parse_queue_max_messages: int = 10_000
    parse_queue_max_bytes: int = 536_870_912
    message_preview_body_bytes: int = 131_072
    message_preview_headers_bytes: int = 65_536
    message_preview_inline_item_bytes: int = 65_536
    message_preview_inline_total_bytes: int = 262_144
    fsync_storage_writes: bool = False
    disk_warning_threshold_percent: int = 85
    ingress_mode: str = "managed_only"
    catch_all_public_web_enabled: bool = False
    catch_all_public_api_enabled: bool = False
    catch_all_retention_days: int = 0
    retention_cleanup_interval_seconds: int = 30
    smtp_session_retention_seconds: int = 24 * 60 * 60
    empty_mailbox_retention_seconds: int = 24 * 60 * 60
    metric_retention_seconds: int = 7 * 24 * 60 * 60
    audit_retention_days: int = 90
    cleanup_batch_size: int = 1000
    file_gc_batch_size: int = 500
    maintenance_run_retention_days: int = 30
    quarantine_retention_days: int = 30
    orphan_artifact_grace_seconds: int = 24 * 60 * 60
    artifact_sweep_batch_size: int = 500
    log_level: str = "INFO"
    log_format: str = "json"
    request_log_enabled: bool = True
    metrics_enabled: bool = True
    metrics_token: str = ""
    api_cursor_secret: str = ""
    readiness_min_free_disk_bytes: int = 64 * 1024 * 1024
    admin_token: str = DEFAULT_ADMIN_TOKEN
    public_api_key: str = DEFAULT_PUBLIC_API_KEY
    legacy_admin_token_enabled: bool = True
    legacy_public_api_key_enabled: bool = True

    def __post_init__(self) -> None:
        self._validate_integer_range("PORT", self.port, 1, 65_535)
        self._validate_integer_range(
            "HTTP_MAX_REQUEST_BODY_BYTES",
            self.http_max_request_body_bytes,
            1,
            MAX_HTTP_REQUEST_BODY_LIMIT_BYTES,
        )
        self._validate_integer_range(
            "HTTP_LIVE_CONNECTION_LIMIT",
            self.http_live_connection_limit,
            1,
            MAX_HTTP_LIVE_CONNECTIONS_LIMIT,
        )
        self._validate_integer_range(
            "HTTP_REQUEST_BODY_TIMEOUT_SECONDS",
            self.http_request_body_timeout_seconds,
            1,
            300,
        )
        self._validate_integer_range(
            "HTTP_CONCURRENCY_LIMIT",
            self.http_concurrency_limit,
            1,
            MAX_HTTP_CONCURRENCY_LIMIT,
        )
        self._validate_integer_range(
            "HTTP_BODY_MEMORY_BUDGET_BYTES",
            self.http_body_memory_budget_bytes,
            1,
            MAX_HTTP_BODY_MEMORY_BUDGET_BYTES,
        )
        if self.http_body_memory_budget_bytes < self.http_max_request_body_bytes:
            raise ValueError(
                "HTTP_BODY_MEMORY_BUDGET_BYTES must be at least HTTP_MAX_REQUEST_BODY_BYTES"
            )
        self._validate_integer_range(
            "DATABASE_WRITE_QUEUE_CAPACITY",
            self.database_write_queue_capacity,
            1,
            100_000,
        )
        self._validate_integer_range(
            "DATABASE_WRITE_MAX_WAITERS",
            self.database_write_max_waiters,
            1,
            100_000,
        )
        self._validate_integer_range(
            "DATABASE_READ_POOL_SIZE",
            self.database_read_pool_size,
            1,
            256,
        )
        self._validate_integer_range(
            "DATABASE_READ_QUEUE_CAPACITY",
            self.database_read_queue_capacity,
            1,
            100_000,
        )
        self._validate_integer_range(
            "DATABASE_READ_MAX_WAITERS",
            self.database_read_max_waiters,
            1,
            100_000,
        )
        if self.database_read_queue_capacity < self.database_read_pool_size:
            raise ValueError(
                "DATABASE_READ_QUEUE_CAPACITY must be at least DATABASE_READ_POOL_SIZE"
            )
        self._validate_integer_range(
            "DATABASE_READ_TIMEOUT_SECONDS",
            self.database_read_timeout_seconds,
            1,
            300,
        )
        self._validate_integer_range("SMTP_PORT", self.smtp_port, 1, 65_535)
        self._validate_integer_range(
            "MAX_MESSAGE_SIZE_BYTES",
            self.max_message_size_bytes,
            1,
            MAX_MESSAGE_SIZE_LIMIT_BYTES,
        )
        self._validate_integer_range(
            "MAX_RECIPIENTS_PER_MESSAGE",
            self.max_recipients_per_message,
            1,
            MAX_RECIPIENTS_LIMIT,
        )
        self._validate_integer_range(
            "SMTP_IDLE_TIMEOUT_SECONDS",
            self.smtp_idle_timeout_seconds,
            1,
            86_400,
        )
        self._validate_integer_range(
            "SMTP_MAX_CONCURRENT_CONNECTIONS",
            self.smtp_max_concurrent_connections,
            0,
            MAX_SMTP_CONNECTIONS_LIMIT,
        )
        self._validate_integer_range(
            "SMTP_CONNECTION_RATE_LIMIT_COUNT",
            self.smtp_connection_rate_limit_count,
            0,
            MAX_RATE_LIMIT_COUNT,
        )
        self._validate_integer_range(
            "SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS",
            self.smtp_connection_rate_limit_window_seconds,
            1,
            86_400,
        )
        if (
            _host_exposes_network(self.smtp_host)
            and self.smtp_max_concurrent_connections == 0
        ):
            raise ValueError(
                "SMTP_MAX_CONCURRENT_CONNECTIONS must be non-zero for a non-loopback SMTP_HOST"
            )
        self._validate_integer_range("PARSE_WORKER_COUNT", self.parse_worker_count, 1, 128)
        self._validate_integer_range(
            "PARSE_QUEUE_MAX_MESSAGES",
            self.parse_queue_max_messages,
            1,
            MAX_PARSE_QUEUE_MESSAGES_LIMIT,
        )
        self._validate_integer_range(
            "PARSE_QUEUE_MAX_BYTES",
            self.parse_queue_max_bytes,
            1,
            MAX_PARSE_QUEUE_BYTES_LIMIT,
        )
        if self.parse_queue_max_bytes < self.max_message_size_bytes:
            raise ValueError("PARSE_QUEUE_MAX_BYTES must be at least MAX_MESSAGE_SIZE_BYTES")
        self._validate_integer_range(
            "MESSAGE_PREVIEW_BODY_BYTES",
            self.message_preview_body_bytes,
            1,
            MAX_MESSAGE_PREVIEW_BODY_BYTES,
        )
        self._validate_integer_range(
            "MESSAGE_PREVIEW_HEADERS_BYTES",
            self.message_preview_headers_bytes,
            1,
            MAX_MESSAGE_PREVIEW_HEADERS_BYTES,
        )
        self._validate_integer_range(
            "MESSAGE_PREVIEW_INLINE_ITEM_BYTES",
            self.message_preview_inline_item_bytes,
            1,
            MAX_MESSAGE_PREVIEW_INLINE_ITEM_BYTES,
        )
        self._validate_integer_range(
            "MESSAGE_PREVIEW_INLINE_TOTAL_BYTES",
            self.message_preview_inline_total_bytes,
            1,
            MAX_MESSAGE_PREVIEW_INLINE_TOTAL_BYTES,
        )
        if self.message_preview_inline_item_bytes > self.message_preview_inline_total_bytes:
            raise ValueError(
                "MESSAGE_PREVIEW_INLINE_ITEM_BYTES must not exceed "
                "MESSAGE_PREVIEW_INLINE_TOTAL_BYTES"
            )
        self._validate_integer_range(
            "DISK_WARNING_THRESHOLD_PERCENT",
            self.disk_warning_threshold_percent,
            1,
            100,
        )
        if not _legacy_secret_enabled(self.admin_token, DEFAULT_ADMIN_TOKEN):
            self.legacy_admin_token_enabled = False
        if not _legacy_secret_enabled(self.public_api_key, DEFAULT_PUBLIC_API_KEY):
            self.legacy_public_api_key_enabled = False
        self.log_level = self.log_level.strip().upper() or "INFO"
        if self.log_level not in {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}:
            raise ValueError("invalid LOG_LEVEL")
        self.log_format = self.log_format.strip().lower() or "json"
        if self.log_format not in {"json", "text"}:
            raise ValueError("invalid LOG_FORMAT")
        self.metrics_token = self.metrics_token.strip()
        self.api_cursor_secret = self.api_cursor_secret.strip()
        if self.api_cursor_secret and len(self.api_cursor_secret) < 32:
            raise ValueError("API_CURSOR_SECRET must contain at least 32 characters")
        if self.readiness_min_free_disk_bytes < 0:
            raise ValueError("invalid READINESS_MIN_FREE_DISK_BYTES")
        self.ingress_mode = self.ingress_mode.strip().lower() or "managed_only"
        if self.ingress_mode not in {"managed_only", "managed_plus_catchall"}:
            raise ValueError("invalid INGRESS_MODE")
        self._validate_integer_range(
            "CATCH_ALL_RETENTION_DAYS",
            self.catch_all_retention_days,
            0,
            MAX_RETENTION_DAYS,
        )
        self._validate_integer_range(
            "RETENTION_CLEANUP_INTERVAL_SECONDS",
            self.retention_cleanup_interval_seconds,
            1,
            86_400,
        )
        for key in (
            "smtp_session_retention_seconds",
            "empty_mailbox_retention_seconds",
            "metric_retention_seconds",
        ):
            self._validate_integer_range(key.upper(), getattr(self, key), 1, 315_360_000)
        self._validate_integer_range(
            "AUDIT_RETENTION_DAYS",
            self.audit_retention_days,
            1,
            MAX_RETENTION_DAYS,
        )
        self._validate_integer_range(
            "CLEANUP_BATCH_SIZE",
            self.cleanup_batch_size,
            1,
            MAX_CLEANUP_BATCH_SIZE,
        )
        self._validate_integer_range(
            "FILE_GC_BATCH_SIZE",
            self.file_gc_batch_size,
            1,
            MAX_CLEANUP_BATCH_SIZE,
        )
        for key in (
            "maintenance_run_retention_days",
            "quarantine_retention_days",
        ):
            self._validate_integer_range(key.upper(), getattr(self, key), 1, MAX_RETENTION_DAYS)
        self._validate_integer_range(
            "ORPHAN_ARTIFACT_GRACE_SECONDS",
            self.orphan_artifact_grace_seconds,
            1,
            315_360_000,
        )
        self._validate_integer_range(
            "ARTIFACT_SWEEP_BATCH_SIZE",
            self.artifact_sweep_batch_size,
            1,
            MAX_CLEANUP_BATCH_SIZE,
        )

    @staticmethod
    def _validate_integer_range(key: str, value: object, minimum: int, maximum: int) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum or value > maximum:
            raise ValueError(f"invalid {key}")

    @property
    def raw_dir(self) -> Path:
        return self.storage_root / "raw"

    @property
    def text_dir(self) -> Path:
        return self.storage_root / "text"

    @property
    def html_dir(self) -> Path:
        return self.storage_root / "html"

    @property
    def attachments_dir(self) -> Path:
        return self.storage_root / "attachments"

    @property
    def manifests_dir(self) -> Path:
        return self.storage_root / "manifests"

    @property
    def tmp_dir(self) -> Path:
        return self.storage_root / "tmp"

    def ensure_directories(self) -> None:
        for path in (
            self.storage_root,
            self.raw_dir,
            self.text_dir,
            self.html_dir,
            self.attachments_dir,
            self.manifests_dir,
            self.tmp_dir,
        ):
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            _chmod_private(path, directory=True)

    def externally_bound(self) -> bool:
        return _host_exposes_network(self.host)

    def insecure_runtime_defaults(self, *, bootstrap_admin_pending: bool) -> list[str]:
        findings: list[str] = []
        if bootstrap_admin_pending and self.bootstrap_admin_password == DEFAULT_BOOTSTRAP_ADMIN_PASSWORD:
            findings.append("BOOTSTRAP_ADMIN_PASSWORD")
        if self.legacy_admin_token_enabled and self.admin_token == DEFAULT_ADMIN_TOKEN:
            findings.append("ADMIN_TOKEN")
        if self.legacy_public_api_key_enabled and self.public_api_key == DEFAULT_PUBLIC_API_KEY:
            findings.append("PUBLIC_API_KEY")
        if not self.api_cursor_secret:
            findings.append("API_CURSOR_SECRET")
        if self.metrics_enabled and not self.metrics_token.strip():
            findings.append("METRICS_TOKEN")
        return findings


def _chmod_private(path: Path, *, directory: bool = False) -> None:
    try:
        path.chmod(0o700 if directory else 0o600)
    except OSError:
        return


def _host_exposes_network(host: str) -> bool:
    normalized = (host or "").strip().strip("[]").lower()
    if normalized in {"", "localhost"}:
        return False
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        return True
    return not address.is_loopback


def _legacy_secret_enabled(value: str, default: str) -> bool:
    text = value.strip()
    return bool(text) and text != default


def _load_dotenv(dotenv_path: Path) -> dict[str, str]:
    if not dotenv_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].strip()
        key, separator, value = line.partition("=")
        if not separator:
            continue
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if value[:1] in {"'", '"'} and value[-1:] == value[:1]:
            value = ast.literal_eval(value)
        values[key] = value
    return values


def _resolve_path(value: str | None, *, default: Path, base_dir: Path) -> Path:
    if value is None or not value.strip():
        return default
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base_dir / path
    return path


def _coerce_str(raw: dict[str, str], key: str, default: str) -> str:
    value = raw.get(key)
    return default if value is None else value


def _coerce_int(raw: dict[str, str], key: str, default: int) -> int:
    value = raw.get(key)
    if value is None or not value.strip():
        return default
    return int(value)


def _coerce_bool(raw: dict[str, str], key: str, default: bool) -> bool:
    value = raw.get(key)
    if value is None or not value.strip():
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid {key}")


def default_settings(base_dir: Path) -> Settings:
    dotenv_values = _load_dotenv(base_dir / ".env")
    merged = {**dotenv_values, **os.environ}
    admin_token = _coerce_str(merged, "ADMIN_TOKEN", DEFAULT_ADMIN_TOKEN)
    public_api_key = _coerce_str(merged, "PUBLIC_API_KEY", DEFAULT_PUBLIC_API_KEY)

    storage_root = _resolve_path(
        merged.get("STORAGE_ROOT"),
        default=base_dir / "storage",
        base_dir=base_dir,
    )
    database_path = _resolve_path(
        merged.get("DATABASE_PATH"),
        default=storage_root / "app.db",
        base_dir=base_dir,
    )

    return Settings(
        storage_root=storage_root,
        database_path=database_path,
        bootstrap_admin_username=_coerce_str(merged, "BOOTSTRAP_ADMIN_USERNAME", "admin"),
        bootstrap_admin_password=_coerce_str(merged, "BOOTSTRAP_ADMIN_PASSWORD", DEFAULT_BOOTSTRAP_ADMIN_PASSWORD),
        session_cookie_name=_coerce_str(merged, "SESSION_COOKIE_NAME", "rapid_inbox_session"),
        host=_coerce_str(merged, "HOST", "127.0.0.1"),
        port=_coerce_int(merged, "PORT", 8000),
        http_max_request_body_bytes=_coerce_int(
            merged,
            "HTTP_MAX_REQUEST_BODY_BYTES",
            1_048_576,
        ),
        http_live_connection_limit=_coerce_int(
            merged,
            "HTTP_LIVE_CONNECTION_LIMIT",
            256,
        ),
        http_request_body_timeout_seconds=_coerce_int(
            merged,
            "HTTP_REQUEST_BODY_TIMEOUT_SECONDS",
            15,
        ),
        http_body_memory_budget_bytes=_coerce_int(
            merged,
            "HTTP_BODY_MEMORY_BUDGET_BYTES",
            268_435_456,
        ),
        http_concurrency_limit=_coerce_int(
            merged,
            "HTTP_CONCURRENCY_LIMIT",
            1000,
        ),
        database_write_queue_capacity=_coerce_int(
            merged,
            "DATABASE_WRITE_QUEUE_CAPACITY",
            256,
        ),
        database_write_max_waiters=_coerce_int(
            merged,
            "DATABASE_WRITE_MAX_WAITERS",
            1024,
        ),
        database_read_pool_size=_coerce_int(
            merged,
            "DATABASE_READ_POOL_SIZE",
            1,
        ),
        database_read_queue_capacity=_coerce_int(
            merged,
            "DATABASE_READ_QUEUE_CAPACITY",
            256,
        ),
        database_read_max_waiters=_coerce_int(
            merged,
            "DATABASE_READ_MAX_WAITERS",
            1024,
        ),
        database_read_timeout_seconds=_coerce_int(
            merged,
            "DATABASE_READ_TIMEOUT_SECONDS",
            5,
        ),
        smtp_host=_coerce_str(merged, "SMTP_HOST", "127.0.0.1"),
        smtp_port=_coerce_int(merged, "SMTP_PORT", 25),
        max_message_size_bytes=_coerce_int(merged, "MAX_MESSAGE_SIZE_BYTES", 52_428_800),
        max_recipients_per_message=_coerce_int(merged, "MAX_RECIPIENTS_PER_MESSAGE", 20),
        smtp_idle_timeout_seconds=_coerce_int(merged, "SMTP_IDLE_TIMEOUT_SECONDS", 30),
        smtp_max_concurrent_connections=_coerce_int(
            merged,
            "SMTP_MAX_CONCURRENT_CONNECTIONS",
            DEFAULT_SMTP_MAX_CONCURRENT_CONNECTIONS,
        ),
        smtp_connection_rate_limit_count=_coerce_int(
            merged,
            "SMTP_CONNECTION_RATE_LIMIT_COUNT",
            DEFAULT_SMTP_CONNECTION_RATE_LIMIT_COUNT,
        ),
        smtp_connection_rate_limit_window_seconds=_coerce_int(
            merged,
            "SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS",
            60,
        ),
        smtp_close_after_data=_coerce_bool(merged, "SMTP_CLOSE_AFTER_DATA", True),
        parse_worker_count=_coerce_int(merged, "PARSE_WORKER_COUNT", 4),
        parse_queue_max_messages=_coerce_int(merged, "PARSE_QUEUE_MAX_MESSAGES", 10_000),
        parse_queue_max_bytes=_coerce_int(merged, "PARSE_QUEUE_MAX_BYTES", 536_870_912),
        message_preview_body_bytes=_coerce_int(
            merged,
            "MESSAGE_PREVIEW_BODY_BYTES",
            131_072,
        ),
        message_preview_headers_bytes=_coerce_int(
            merged,
            "MESSAGE_PREVIEW_HEADERS_BYTES",
            65_536,
        ),
        message_preview_inline_item_bytes=_coerce_int(
            merged,
            "MESSAGE_PREVIEW_INLINE_ITEM_BYTES",
            65_536,
        ),
        message_preview_inline_total_bytes=_coerce_int(
            merged,
            "MESSAGE_PREVIEW_INLINE_TOTAL_BYTES",
            262_144,
        ),
        fsync_storage_writes=_coerce_bool(merged, "FSYNC_STORAGE_WRITES", False),
        disk_warning_threshold_percent=_coerce_int(merged, "DISK_WARNING_THRESHOLD_PERCENT", 85),
        ingress_mode=_coerce_str(merged, "INGRESS_MODE", "managed_only"),
        catch_all_public_web_enabled=_coerce_bool(merged, "CATCH_ALL_PUBLIC_WEB_ENABLED", False),
        catch_all_public_api_enabled=_coerce_bool(merged, "CATCH_ALL_PUBLIC_API_ENABLED", False),
        catch_all_retention_days=_coerce_int(merged, "CATCH_ALL_RETENTION_DAYS", 0),
        retention_cleanup_interval_seconds=_coerce_int(merged, "RETENTION_CLEANUP_INTERVAL_SECONDS", 30),
        smtp_session_retention_seconds=_coerce_int(merged, "SMTP_SESSION_RETENTION_SECONDS", 86_400),
        empty_mailbox_retention_seconds=_coerce_int(merged, "EMPTY_MAILBOX_RETENTION_SECONDS", 86_400),
        metric_retention_seconds=_coerce_int(merged, "METRIC_RETENTION_SECONDS", 604_800),
        audit_retention_days=_coerce_int(merged, "AUDIT_RETENTION_DAYS", 90),
        cleanup_batch_size=_coerce_int(merged, "CLEANUP_BATCH_SIZE", 1000),
        file_gc_batch_size=_coerce_int(merged, "FILE_GC_BATCH_SIZE", 500),
        maintenance_run_retention_days=_coerce_int(merged, "MAINTENANCE_RUN_RETENTION_DAYS", 30),
        quarantine_retention_days=_coerce_int(merged, "QUARANTINE_RETENTION_DAYS", 30),
        orphan_artifact_grace_seconds=_coerce_int(merged, "ORPHAN_ARTIFACT_GRACE_SECONDS", 86_400),
        artifact_sweep_batch_size=_coerce_int(merged, "ARTIFACT_SWEEP_BATCH_SIZE", 500),
        log_level=_coerce_str(merged, "LOG_LEVEL", "INFO"),
        log_format=_coerce_str(merged, "LOG_FORMAT", "json"),
        request_log_enabled=_coerce_bool(merged, "REQUEST_LOG_ENABLED", True),
        metrics_enabled=_coerce_bool(merged, "METRICS_ENABLED", True),
        metrics_token=_coerce_str(merged, "METRICS_TOKEN", ""),
        api_cursor_secret=_coerce_str(merged, "API_CURSOR_SECRET", ""),
        readiness_min_free_disk_bytes=_coerce_int(
            merged,
            "READINESS_MIN_FREE_DISK_BYTES",
            64 * 1024 * 1024,
        ),
        admin_token=admin_token,
        public_api_key=public_api_key,
        legacy_admin_token_enabled=_legacy_secret_enabled(admin_token, DEFAULT_ADMIN_TOKEN),
        legacy_public_api_key_enabled=_legacy_secret_enabled(public_api_key, DEFAULT_PUBLIC_API_KEY),
    )
