from __future__ import annotations

import importlib.util
import os
import shutil
import socketserver
import sqlite3
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
SYSTEM_DIR = REPO_ROOT / "deploy/system"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


verify = load_module("system_verify_deployment", SYSTEM_DIR / "verify_deployment.py")
database_admin = load_module("system_database_admin", SYSTEM_DIR / "database_admin.py")


class ReadyHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/health/ready":
            self.send_error(404)
            return
        payload = b'{"status":"ready"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, _format: str, *_args) -> None:
        return


class SMTPHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        self.wfile.write(b"220 test-smtp\r\n")
        self.wfile.flush()
        assert self.rfile.readline().startswith(b"EHLO ")
        self.wfile.write(b"250-test-smtp\r\n250 PIPELINING\r\n")
        self.wfile.flush()
        assert self.rfile.readline() == b"NOOP\r\n"
        self.wfile.write(b"250 OK\r\n")
        self.wfile.flush()
        assert self.rfile.readline() == b"QUIT\r\n"
        self.wfile.write(b"221 Bye\r\n")
        self.wfile.flush()


def serve(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return thread


def test_http_and_smtp_protocol_checks() -> None:
    http_server = ThreadingHTTPServer(("127.0.0.1", 0), ReadyHandler)
    smtp_server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), SMTPHandler)
    smtp_server.daemon_threads = True
    threads = [serve(http_server), serve(smtp_server)]
    try:
        verify.check_http("127.0.0.1", http_server.server_port, 2.0)
        verify.check_smtp("127.0.0.1", smtp_server.server_address[1], 2.0)
    finally:
        http_server.shutdown()
        smtp_server.shutdown()
        http_server.server_close()
        smtp_server.server_close()
        for thread in threads:
            thread.join(timeout=2)


def test_sqlite_backup_and_atomic_restore(tmp_path: Path) -> None:
    database_path = tmp_path / "storage" / "app.db"
    database_path.parent.mkdir()
    writer = sqlite3.connect(database_path)
    writer.execute("PRAGMA journal_mode = WAL")
    writer.execute("CREATE TABLE state (value TEXT NOT NULL)")
    writer.execute("INSERT INTO state VALUES ('before')")
    writer.commit()
    assert Path(f"{database_path}-wal").exists()

    backup_path = tmp_path / "backups" / "pre-migration.db"
    database_admin.backup_database(database_path, backup_path)
    writer.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("UPDATE state SET value = 'after'")

    database_admin.restore_database(database_path, backup_path)
    assert not Path(f"{database_path}-wal").exists()
    assert not Path(f"{database_path}-shm").exists()
    with sqlite3.connect(database_path) as connection:
        value = connection.execute("SELECT value FROM state").fetchone()[0]
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    assert value == "before"
    assert integrity == "ok"


def test_init_db_helper_uses_explicit_base_directory(tmp_path: Path) -> None:
    staged_root = tmp_path / "release"
    shutil.copytree(REPO_ROOT / "app", staged_root / "app")
    shutil.copytree(SYSTEM_DIR, staged_root / "deploy/system")
    shutil.copy2(REPO_ROOT / "sqlite_schema.sql", staged_root / "sqlite_schema.sql")
    database_path = tmp_path / "state" / "app.db"
    environment = {
        **os.environ,
        "PYTHONDONTWRITEBYTECODE": "1",
        "STORAGE_ROOT": str(tmp_path / "state"),
        "DATABASE_PATH": str(database_path),
        "HOST": "127.0.0.1",
    }
    subprocess.run(
        [
            sys.executable,
            str(staged_root / "deploy/system/run_http.py"),
            "--base-dir",
            str(staged_root),
            "--check",
        ],
        check=True,
        env=environment,
    )
    subprocess.run(
        [
            sys.executable,
            str(staged_root / "deploy/system/init_db.py"),
            "--base-dir",
            str(staged_root),
        ],
        check=True,
        env=environment,
    )
    with sqlite3.connect(database_path) as connection:
        table = connection.execute(
            "SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'messages'"
        ).fetchone()
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
    assert table == ("messages",)
    assert integrity == "ok"


def test_installer_shell_syntax_and_help() -> None:
    subprocess.run(["bash", "-n", str(SYSTEM_DIR / "install.sh")], check=True)
    result = subprocess.run(
        ["bash", str(SYSTEM_DIR / "install.sh"), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "install" in result.stdout
    assert "update" in result.stdout
    assert "status" in result.stdout
    assert "uninstall" in result.stdout


def test_systemd_templates_render_and_verify(tmp_path: Path) -> None:
    analyzer = shutil.which("systemd-analyze")
    if analyzer is None:
        pytest.skip("systemd-analyze is not installed")

    current = tmp_path / "current"
    config_dir = tmp_path / "etc"
    data_root = tmp_path / "data"
    unit_dir = tmp_path / "units"
    for path in (
        current / ".venv/bin",
        current / "bin",
        current / "deploy/system",
        config_dir,
        data_root,
        unit_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)

    for executable in (
        current / ".venv/bin/python",
        current / "bin/rapid-inbox-ingestd",
    ):
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
        executable.chmod(0o755)
    for helper in (
        current / "deploy/system/init_db.py",
        current / "deploy/system/run_http.py",
        current / "deploy/system/verify_deployment.py",
    ):
        helper.write_text("", encoding="ascii")

    config_file = config_dir / "rapid-inbox.env"
    config_file.write_text(
        "HOST=127.0.0.1\nPORT=8000\nHTTP_CONCURRENCY_LIMIT=1000\n"
        "SMTP_HOST=127.0.0.1\nSMTP_PORT=2525\n",
        encoding="ascii",
    )
    replacements = {
        "@CURRENT_DIR@": str(current),
        "@CONFIG_DIR@": str(config_dir),
        "@CONFIG_FILE@": str(config_file),
        "@DATA_ROOT@": str(data_root),
        "@SERVICE_USER@": "root",
        "@SERVICE_GROUP@": "root",
    }
    rendered = []
    for template in sorted((SYSTEM_DIR / "templates").glob("*.in")):
        content = template.read_text(encoding="ascii")
        for placeholder, value in replacements.items():
            content = content.replace(placeholder, value)
        assert "@" not in content
        output = unit_dir / template.stem
        output.write_text(content, encoding="ascii")
        rendered.append(str(output))

    subprocess.run(
        [analyzer, "verify", *rendered],
        check=True,
        capture_output=True,
        text=True,
    )


def test_repo_ignores_python_cache_artifacts() -> None:
    ignore_text = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
    assert "__pycache__/" in ignore_text
    assert "*.pyc" in ignore_text
