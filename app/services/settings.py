from __future__ import annotations

import sqlite3
import threading
from typing import Any

from app.auth.permissions import PermissionContext
from app.config import (
    MAX_CLEANUP_BATCH_SIZE,
    MAX_MESSAGE_SIZE_LIMIT_BYTES,
    MAX_RATE_LIMIT_COUNT,
    MAX_RECIPIENTS_LIMIT,
    MAX_RETENTION_DAYS,
    MAX_SMTP_CONNECTIONS_LIMIT,
)
from app.db.connection import connect_database
from app.ingest.storage import utc_now


class SettingsService:
    SUPPORTED_SETTINGS = {
        "max_message_size_bytes",
        "max_recipients_per_message",
        "smtp_idle_timeout_seconds",
        "smtp_max_concurrent_connections",
        "smtp_connection_rate_limit_count",
        "smtp_connection_rate_limit_window_seconds",
        "disk_warning_threshold_percent",
        "ingress_mode",
        "catch_all_public_web_enabled",
        "catch_all_public_api_enabled",
        "catch_all_retention_days",
        "retention_cleanup_interval_seconds",
        "smtp_session_retention_seconds",
        "empty_mailbox_retention_seconds",
        "metric_retention_seconds",
        "audit_retention_days",
        "cleanup_batch_size",
        "file_gc_batch_size",
    }
    INTEGER_SETTINGS = {
        "max_message_size_bytes",
        "max_recipients_per_message",
        "smtp_idle_timeout_seconds",
        "smtp_max_concurrent_connections",
        "smtp_connection_rate_limit_count",
        "smtp_connection_rate_limit_window_seconds",
        "disk_warning_threshold_percent",
        "catch_all_retention_days",
        "retention_cleanup_interval_seconds",
        "smtp_session_retention_seconds",
        "empty_mailbox_retention_seconds",
        "metric_retention_seconds",
        "audit_retention_days",
        "cleanup_batch_size",
        "file_gc_batch_size",
    }
    NON_NEGATIVE_INTEGER_SETTINGS = {
        "smtp_max_concurrent_connections",
        "smtp_connection_rate_limit_count",
        "catch_all_retention_days",
    }
    BOOLEAN_SETTINGS = {"catch_all_public_web_enabled", "catch_all_public_api_enabled"}
    MAXIMUM_INTEGER_SETTINGS = {
        "max_message_size_bytes": MAX_MESSAGE_SIZE_LIMIT_BYTES,
        "max_recipients_per_message": MAX_RECIPIENTS_LIMIT,
        "smtp_idle_timeout_seconds": 86_400,
        "smtp_max_concurrent_connections": MAX_SMTP_CONNECTIONS_LIMIT,
        "smtp_connection_rate_limit_count": MAX_RATE_LIMIT_COUNT,
        "smtp_connection_rate_limit_window_seconds": 86_400,
        "disk_warning_threshold_percent": 100,
        "catch_all_retention_days": MAX_RETENTION_DAYS,
        "retention_cleanup_interval_seconds": 86_400,
        "smtp_session_retention_seconds": 315_360_000,
        "empty_mailbox_retention_seconds": 315_360_000,
        "metric_retention_seconds": 315_360_000,
        "audit_retention_days": MAX_RETENTION_DAYS,
        "cleanup_batch_size": MAX_CLEANUP_BATCH_SIZE,
        "file_gc_batch_size": MAX_CLEANUP_BATCH_SIZE,
    }

    def __init__(self, runtime: Any) -> None:
        self._runtime = runtime
        self._snapshot_lock = threading.Lock()
        self._settings_snapshot = self._base_settings()

    def get_settings(self) -> dict[str, Any]:
        # Startup and every successful mutation atomically refresh this live
        # snapshot. SMTP may run on a separate event-loop thread, so return a
        # copy rather than reading mutable Settings fields one by one.
        with self._snapshot_lock:
            return dict(self._settings_snapshot)

    async def load_persisted_settings(self) -> dict[str, Any]:
        settings = self._load_persisted_settings()
        self._validate_parse_queue_budget(settings)
        self._runtime.apply_live_settings(settings)
        self._refresh_snapshot()
        await self._sync_ingress_policy()
        return settings

    async def update_settings(
        self,
        payload: dict[str, Any],
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise ValueError("invalid settings payload")

        self._validate_supported_keys(payload)
        normalized = self._normalize_payload(payload)
        if not normalized:
            return self.get_settings()
        self._validate_parse_queue_budget(
            {key: self._deserialize_value(key, value) for key, value in normalized.items()}
        )

        now = utc_now()

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute("BEGIN IMMEDIATE")
            self._runtime.api_keys.transaction_authorization_principal(
                connection,
                authorization_principal,
                required_scope="system.write",
                require_global=True,
            )
            for key, value in normalized.items():
                connection.execute(
                    """
                    INSERT INTO system_settings (key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, value, now),
                )

        await self._runtime.writer.execute(operation)
        self._runtime.apply_live_settings(
            {key: self._deserialize_value(key, value) for key, value in normalized.items()}
        )
        self._refresh_snapshot()
        await self._sync_ingress_policy()
        return self.get_settings()

    def _refresh_snapshot(self) -> None:
        snapshot = self._base_settings()
        with self._snapshot_lock:
            self._settings_snapshot = snapshot

    def _validate_parse_queue_budget(self, updates: dict[str, Any]) -> None:
        max_message_size = int(
            updates.get(
                "max_message_size_bytes",
                self._runtime.settings.max_message_size_bytes,
            )
        )
        if max_message_size > int(self._runtime.settings.parse_queue_max_bytes):
            raise ValueError("invalid max_message_size_bytes: exceeds parse queue byte budget")

    def _base_settings(self) -> dict[str, Any]:
        return {
            "max_message_size_bytes": int(self._runtime.settings.max_message_size_bytes),
            "max_recipients_per_message": int(self._runtime.settings.max_recipients_per_message),
            "smtp_idle_timeout_seconds": int(self._runtime.settings.smtp_idle_timeout_seconds),
            "smtp_max_concurrent_connections": int(self._runtime.settings.smtp_max_concurrent_connections),
            "smtp_connection_rate_limit_count": int(self._runtime.settings.smtp_connection_rate_limit_count),
            "smtp_connection_rate_limit_window_seconds": int(
                self._runtime.settings.smtp_connection_rate_limit_window_seconds
            ),
            "disk_warning_threshold_percent": int(self._runtime.settings.disk_warning_threshold_percent),
            "ingress_mode": str(self._runtime.settings.ingress_mode),
            "catch_all_public_web_enabled": bool(self._runtime.settings.catch_all_public_web_enabled),
            "catch_all_public_api_enabled": bool(self._runtime.settings.catch_all_public_api_enabled),
            "catch_all_retention_days": int(self._runtime.settings.catch_all_retention_days),
            "retention_cleanup_interval_seconds": int(self._runtime.settings.retention_cleanup_interval_seconds),
            "smtp_session_retention_seconds": int(self._runtime.settings.smtp_session_retention_seconds),
            "empty_mailbox_retention_seconds": int(self._runtime.settings.empty_mailbox_retention_seconds),
            "metric_retention_seconds": int(self._runtime.settings.metric_retention_seconds),
            "audit_retention_days": int(self._runtime.settings.audit_retention_days),
            "cleanup_batch_size": int(self._runtime.settings.cleanup_batch_size),
            "file_gc_batch_size": int(self._runtime.settings.file_gc_batch_size),
        }

    def _load_persisted_settings(self) -> dict[str, Any]:
        with connect_database(self._runtime.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT key, value
                FROM system_settings
                ORDER BY key ASC
                """
            ).fetchall()

        settings: dict[str, Any] = {}
        for row in rows:
            key = str(row["key"])
            if key not in self.SUPPORTED_SETTINGS:
                continue
            settings[key] = self._deserialize_value(key, row["value"])
        return settings

    def _normalize_payload(self, payload: dict[str, Any]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for key, value in payload.items():
            key_text = str(key)
            if key_text in self.BOOLEAN_SETTINGS:
                normalized[key_text] = "1" if self._coerce_bool(key_text, value) else "0"
            elif key_text == "ingress_mode":
                mode = str(value).strip().lower()
                if mode not in {"managed_only", "managed_plus_catchall"}:
                    raise ValueError("invalid ingress_mode")
                normalized[key_text] = mode
            elif key_text in self.NON_NEGATIVE_INTEGER_SETTINGS:
                normalized[key_text] = str(self._coerce_non_negative_int(key_text, value))
            elif key_text in self.INTEGER_SETTINGS:
                normalized[key_text] = str(self._coerce_positive_int(key_text, value))
            else:
                normalized[key_text] = self._coerce_text_value(value)
        return normalized

    def _validate_supported_keys(self, payload: dict[str, Any]) -> None:
        unsupported = sorted({str(key) for key in payload if str(key) not in self.SUPPORTED_SETTINGS})
        if unsupported:
            raise ValueError(f"unsupported settings: {', '.join(unsupported)}")

    def _coerce_positive_int(self, key: str, value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError(f"invalid {key}")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"invalid {key}")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid {key}") from exc
        if normalized < 1:
            raise ValueError(f"invalid {key}")
        maximum = self.MAXIMUM_INTEGER_SETTINGS.get(key)
        if maximum is not None and normalized > maximum:
            raise ValueError(f"invalid {key}")
        if isinstance(value, float) and normalized != value:
            raise ValueError(f"invalid {key}")
        return normalized

    def _coerce_non_negative_int(self, key: str, value: Any) -> int:
        if isinstance(value, bool):
            raise ValueError(f"invalid {key}")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"invalid {key}")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid {key}") from exc
        if normalized < 0:
            raise ValueError(f"invalid {key}")
        maximum = self.MAXIMUM_INTEGER_SETTINGS.get(key)
        if maximum is not None and normalized > maximum:
            raise ValueError(f"invalid {key}")
        return normalized

    def _coerce_text_value(self, value: Any) -> str:
        if value is None:
            raise ValueError("invalid settings value")
        if isinstance(value, bool):
            return "1" if value else "0"
        return str(value)

    def _coerce_bool(self, key: str, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and value in (0, 1):
            return bool(value)
        if isinstance(value, str) and value.strip().lower() in {"1", "true", "yes", "on"}:
            return True
        if isinstance(value, str) and value.strip().lower() in {"0", "false", "no", "off"}:
            return False
        raise ValueError(f"invalid {key}")

    def _deserialize_value(self, key: str, value: Any) -> Any:
        if key in self.BOOLEAN_SETTINGS:
            try:
                return self._coerce_bool(key, value)
            except ValueError:
                return bool(self._base_settings()[key])
        if key == "ingress_mode":
            mode = str(value).strip().lower()
            return mode if mode in {"managed_only", "managed_plus_catchall"} else self._base_settings()[key]
        if key in self.NON_NEGATIVE_INTEGER_SETTINGS:
            try:
                return self._coerce_non_negative_int(key, value)
            except ValueError:
                return int(self._base_settings()[key])
        if key in self.INTEGER_SETTINGS:
            try:
                return self._coerce_positive_int(key, value)
            except ValueError:
                return int(self._base_settings()[key])
        return value

    async def _sync_ingress_policy(self) -> None:
        settings = self._base_settings()
        settings.update(self._load_persisted_settings())
        await self._runtime.domains.sync_catch_all_policy(
            enabled=settings["ingress_mode"] == "managed_plus_catchall",
            public_web_enabled=bool(settings["catch_all_public_web_enabled"]),
            public_api_enabled=bool(settings["catch_all_public_api_enabled"]),
            retention_days=(int(settings["catch_all_retention_days"]) or None),
            max_message_size_bytes=int(settings["max_message_size_bytes"]),
        )


__all__ = ["SettingsService"]
