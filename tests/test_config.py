import os
from pathlib import Path

import pytest

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10 fallback
    import tomli as tomllib

from app.config import Settings, default_settings


def test_settings_derive_storage_paths(tmp_path: Path) -> None:
    settings = Settings(storage_root=tmp_path, database_path=tmp_path / "app.db")

    assert settings.raw_dir == tmp_path / "raw"
    assert settings.text_dir == tmp_path / "text"
    assert settings.html_dir == tmp_path / "html"
    assert settings.attachments_dir == tmp_path / "attachments"


def test_settings_include_bootstrap_and_operational_defaults(tmp_path: Path) -> None:
    settings = Settings(storage_root=tmp_path / "storage", database_path=tmp_path / "storage" / "app.db")

    assert settings.bootstrap_admin_username == "admin"
    assert settings.bootstrap_admin_password == "change-me-now"
    assert settings.smtp_port == 25
    assert settings.smtp_idle_timeout_seconds == 30
    assert settings.smtp_max_concurrent_connections == 1024
    assert settings.smtp_connection_rate_limit_count == 60_000
    assert settings.max_recipients_per_message == 20
    assert settings.smtp_close_after_data is True
    assert settings.parse_queue_max_messages == 10_000
    assert settings.parse_queue_max_bytes == 536_870_912
    assert settings.http_live_connection_limit == 256
    assert settings.database_read_pool_size == 1
    assert settings.database_read_queue_capacity == 256
    assert settings.database_read_max_waiters == 1024
    assert settings.database_read_timeout_seconds == 5
    assert settings.session_cookie_name == "rapid_inbox_session"


def test_default_settings_loads_values_from_dotenv(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "STORAGE_ROOT=./custom-storage",
                "DATABASE_PATH=./custom-storage/custom.db",
                "BOOTSTRAP_ADMIN_USERNAME=rooter",
                "BOOTSTRAP_ADMIN_PASSWORD=super-secret",
                "SESSION_COOKIE_NAME=ri_cookie",
                "HOST=0.0.0.0",
                "PORT=18000",
                "HTTP_LIVE_CONNECTION_LIMIT=73",
                "DATABASE_READ_POOL_SIZE=2",
                "DATABASE_READ_QUEUE_CAPACITY=19",
                "DATABASE_READ_MAX_WAITERS=37",
                "DATABASE_READ_TIMEOUT_SECONDS=11",
                "SMTP_HOST=0.0.0.0",
                "SMTP_PORT=2525",
                "MAX_MESSAGE_SIZE_BYTES=1024",
                "MAX_RECIPIENTS_PER_MESSAGE=9",
                "SMTP_CLOSE_AFTER_DATA=false",
                "PARSE_WORKER_COUNT=7",
                "PARSE_QUEUE_MAX_MESSAGES=321",
                "PARSE_QUEUE_MAX_BYTES=2048",
                "FSYNC_STORAGE_WRITES=true",
                "ADMIN_TOKEN=admin-token-1",
                "PUBLIC_API_KEY=public-token-1",
            ]
        ),
        encoding="utf-8",
    )

    settings = default_settings(tmp_path)

    assert settings.storage_root == tmp_path / "custom-storage"
    assert settings.database_path == tmp_path / "custom-storage" / "custom.db"
    assert settings.bootstrap_admin_username == "rooter"
    assert settings.bootstrap_admin_password == "super-secret"
    assert settings.session_cookie_name == "ri_cookie"
    assert settings.host == "0.0.0.0"
    assert settings.port == 18000
    assert settings.http_live_connection_limit == 73
    assert settings.database_read_pool_size == 2
    assert settings.database_read_queue_capacity == 19
    assert settings.database_read_max_waiters == 37
    assert settings.database_read_timeout_seconds == 11
    assert settings.smtp_host == "0.0.0.0"
    assert settings.smtp_port == 2525
    assert settings.max_message_size_bytes == 1024
    assert settings.max_recipients_per_message == 9
    assert settings.smtp_close_after_data is False
    assert settings.parse_worker_count == 7
    assert settings.parse_queue_max_messages == 321
    assert settings.parse_queue_max_bytes == 2048
    assert settings.fsync_storage_writes is True
    assert settings.admin_token == "admin-token-1"
    assert settings.public_api_key == "public-token-1"
    assert settings.legacy_admin_token_enabled is True
    assert settings.legacy_public_api_key_enabled is True


def test_default_legacy_tokens_are_not_enabled_for_runtime_config(tmp_path: Path) -> None:
    settings = default_settings(tmp_path)

    assert settings.admin_token == "dev-admin-token"
    assert settings.public_api_key == "public-demo-key"
    assert settings.legacy_admin_token_enabled is False
    assert settings.legacy_public_api_key_enabled is False


def test_settings_constructor_disables_builtin_demo_tokens(tmp_path: Path) -> None:
    settings = Settings(storage_root=tmp_path / "storage", database_path=tmp_path / "storage" / "app.db")

    assert settings.legacy_admin_token_enabled is False
    assert settings.legacy_public_api_key_enabled is False


def test_external_security_findings_require_metrics_auth_when_enabled(tmp_path: Path) -> None:
    settings = Settings(
        storage_root=tmp_path / "storage",
        database_path=tmp_path / "storage" / "app.db",
        host="0.0.0.0",
        bootstrap_admin_password="strong-bootstrap-password",
        api_cursor_secret="x" * 48,
    )

    assert settings.insecure_runtime_defaults(bootstrap_admin_pending=True) == ["METRICS_TOKEN"]

    settings.metrics_enabled = False
    assert settings.insecure_runtime_defaults(bootstrap_admin_pending=True) == []


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("port", 0),
        ("smtp_port", 65_536),
        ("max_message_size_bytes", 1_073_741_825),
        ("max_recipients_per_message", 10_001),
        ("smtp_idle_timeout_seconds", 0),
        ("smtp_max_concurrent_connections", -1),
        ("parse_worker_count", 0),
        ("database_read_pool_size", 0),
        ("database_read_queue_capacity", 0),
        ("database_read_max_waiters", 0),
        ("database_read_timeout_seconds", 0),
        ("parse_queue_max_messages", 0),
        ("parse_queue_max_messages", 1_000_001),
        ("parse_queue_max_bytes", 1_099_511_627_777),
        ("disk_warning_threshold_percent", 101),
        ("catch_all_retention_days", 36_501),
        ("cleanup_batch_size", 1_000_001),
    ],
)
def test_settings_reject_resource_limits_outside_safe_ranges(
    tmp_path: Path,
    field_name: str,
    value: int,
) -> None:
    kwargs = {
        "storage_root": tmp_path / "storage",
        "database_path": tmp_path / "storage" / "app.db",
        field_name: value,
    }

    with pytest.raises(ValueError, match="invalid"):
        Settings(**kwargs)


def test_settings_reject_read_queue_smaller_than_actor_pool(tmp_path: Path) -> None:
    with pytest.raises(
        ValueError,
        match="DATABASE_READ_QUEUE_CAPACITY must be at least DATABASE_READ_POOL_SIZE",
    ):
        Settings(
            storage_root=tmp_path / "storage",
            database_path=tmp_path / "storage" / "app.db",
            database_read_pool_size=4,
            database_read_queue_capacity=3,
        )


def test_blank_legacy_tokens_are_not_enabled(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(["ADMIN_TOKEN=", "PUBLIC_API_KEY="]),
        encoding="utf-8",
    )

    settings = default_settings(tmp_path)

    assert settings.legacy_admin_token_enabled is False
    assert settings.legacy_public_api_key_enabled is False


def test_settings_require_parse_byte_budget_to_cover_largest_message(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="PARSE_QUEUE_MAX_BYTES"):
        Settings(
            storage_root=tmp_path / "storage",
            database_path=tmp_path / "storage" / "app.db",
            max_message_size_bytes=1025,
            parse_queue_max_bytes=1024,
        )


def test_settings_reject_unbounded_public_python_smtp_listener(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="SMTP_MAX_CONCURRENT_CONNECTIONS"):
        Settings(
            storage_root=tmp_path / "storage",
            database_path=tmp_path / "storage" / "app.db",
            smtp_host="0.0.0.0",
            smtp_max_concurrent_connections=0,
        )

    # Explicit unlimited mode remains available for a loopback-only developer
    # listener where untrusted network peers cannot consume the process.
    settings = Settings(
        storage_root=tmp_path / "loopback-storage",
        database_path=tmp_path / "loopback-storage" / "app.db",
        smtp_host="127.0.0.1",
        smtp_max_concurrent_connections=0,
    )
    assert settings.smtp_max_concurrent_connections == 0


def test_default_settings_rejects_misspelled_boolean_values(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("FSYNC_STORAGE_WRITES=treu\n", encoding="utf-8")

    with pytest.raises(ValueError, match="FSYNC_STORAGE_WRITES"):
        default_settings(tmp_path)


def test_environment_variables_override_dotenv(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "HOST=127.0.0.1",
                "SMTP_HOST=127.0.0.1",
                "BOOTSTRAP_ADMIN_PASSWORD=from-dotenv",
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("HOST", "0.0.0.0")
    monkeypatch.setenv("SMTP_HOST", "0.0.0.0")
    monkeypatch.setenv("BOOTSTRAP_ADMIN_PASSWORD", "from-env")

    settings = default_settings(tmp_path)

    assert settings.host == "0.0.0.0"
    assert settings.smtp_host == "0.0.0.0"
    assert settings.bootstrap_admin_password == "from-env"


def test_default_settings_resolves_relative_paths_from_base_dir(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "STORAGE_ROOT=./runtime/storage",
                "DATABASE_PATH=./runtime/data/app.db",
            ]
        ),
        encoding="utf-8",
    )

    settings = default_settings(tmp_path)

    assert settings.storage_root == tmp_path / "runtime" / "storage"
    assert settings.database_path == tmp_path / "runtime" / "data" / "app.db"


@pytest.mark.asyncio
async def test_live_settings_reject_values_above_runtime_safety_limits(runtime) -> None:
    with pytest.raises(ValueError, match="invalid max_message_size_bytes"):
        await runtime.update_settings({"max_message_size_bytes": 1_073_741_825})
    with pytest.raises(ValueError, match="invalid cleanup_batch_size"):
        await runtime.update_settings({"cleanup_batch_size": 1_000_001})

    current = runtime.get_settings()
    assert current["max_message_size_bytes"] == 52_428_800
    assert current["cleanup_batch_size"] == 1000


@pytest.mark.asyncio
async def test_live_message_size_cannot_exceed_static_parse_byte_budget(runtime) -> None:
    runtime.settings.parse_queue_max_bytes = runtime.settings.max_message_size_bytes

    with pytest.raises(ValueError, match="parse queue byte budget"):
        await runtime.update_settings(
            {"max_message_size_bytes": runtime.settings.parse_queue_max_bytes + 1}
        )

    assert runtime.get_settings()["max_message_size_bytes"] == runtime.settings.parse_queue_max_bytes


@pytest.mark.asyncio
async def test_successful_live_update_immediately_refreshes_settings_snapshot(runtime) -> None:
    updated = await runtime.update_settings({"max_recipients_per_message": 33})

    assert updated["max_recipients_per_message"] == 33
    assert runtime.get_settings()["max_recipients_per_message"] == 33
    assert runtime.settings.max_recipients_per_message == 33

    updated["max_recipients_per_message"] = 99
    assert runtime.get_settings()["max_recipients_per_message"] == 33


def test_project_declares_websocket_runtime_dependency() -> None:
    project_root = Path(__file__).resolve().parents[1]
    pyproject = tomllib.loads((project_root / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = pyproject["project"]["dependencies"]
    constraints = (project_root / "constraints-dev.txt").read_text(encoding="utf-8")

    assert any(dependency.startswith("websockets==") for dependency in dependencies)
    assert "websockets==" in constraints
