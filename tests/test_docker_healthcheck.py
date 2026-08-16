from __future__ import annotations

import importlib.util
import json
import os
import re
import socket
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType


def _load_healthcheck() -> ModuleType:
    path = Path(__file__).resolve().parents[1] / "deploy" / "docker" / "healthcheck.py"
    spec = importlib.util.spec_from_file_location("rapid_inbox_docker_healthcheck", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_http_healthcheck_requires_application_readiness() -> None:
    healthcheck = _load_healthcheck()

    class ReadyHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            assert self.path == "/health/ready"
            payload = json.dumps({"status": "ready"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), ReadyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        healthcheck.check_http("127.0.0.1", server.server_port, 1.0)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_smtp_healthcheck_exercises_banner_ehlo_noop_and_quit() -> None:
    healthcheck = _load_healthcheck()
    listener = socket.socket()
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    commands: list[bytes] = []
    server_errors: list[BaseException] = []

    def serve() -> None:
        try:
            connection, _address = listener.accept()
            with connection, connection.makefile("rb") as stream:
                connection.sendall(b"220 rapid-inbox-ingestd\r\n")
                commands.append(stream.readline())
                connection.sendall(b"250-SIZE 52428800\r\n250 SMTPUTF8\r\n")
                commands.append(stream.readline())
                connection.sendall(b"250 OK\r\n")
                commands.append(stream.readline())
                connection.sendall(b"221 2.0.0 Bye\r\n")
        except BaseException as exc:
            server_errors.append(exc)

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    try:
        healthcheck.check_smtp("127.0.0.1", listener.getsockname()[1], 1.0)
    finally:
        listener.close()
        thread.join(timeout=2)

    assert not server_errors
    assert commands == [b"EHLO container-healthcheck\r\n", b"NOOP\r\n", b"QUIT\r\n"]


def test_compose_keeps_http_and_ingestd_in_one_container() -> None:
    root = Path(__file__).resolve().parents[1]
    compose = (root / "compose.yaml").read_text(encoding="utf-8")
    service_section = compose.split("services:\n", 1)[1].split("\nvolumes:\n", 1)[0]
    service_names = re.findall(r"^  ([a-zA-Z0-9_.-]+):$", service_section, re.MULTILINE)

    assert service_names == ["app"]
    assert "      - run\n" in service_section
    assert "    - rapid-inbox-data:/var/lib/rapid-inbox\n" in compose
    assert "        - all\n" in service_section

    entrypoint = (root / "deploy" / "docker" / "entrypoint.sh").read_text(
        encoding="utf-8"
    )
    readiness_gate = entrypoint.index("HTTP did not become ready before timeout")
    managed_ingestd_start = entrypoint.index(
        "/usr/local/bin/rapid-inbox-ingestd --base-dir /app &"
    )
    assert readiness_gate < managed_ingestd_start


def test_docker_launcher_builds_before_stop_and_preserves_the_volume() -> None:
    root = Path(__file__).resolve().parents[1]
    launcher = (root / "docker-deploy.sh").read_text(encoding="utf-8")
    rollout = launcher.split("rollout() {", 1)[1].split("show_credentials() {", 1)[0]

    assert rollout.index("compose build") < rollout.index("compose stop")
    assert rollout.index("compose stop") < rollout.index("compose up")
    assert "compose ps app" in launcher
    assert ".rapid-inbox-docker/rapid-inbox.env" in launcher
    assert "docker volume rm" not in launcher
    assert "compose down -v" not in launcher
    assert "compose down --volumes" not in launcher


def test_docker_launcher_prints_pending_credentials_after_a_retry(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[1]
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    failure_marker = tmp_path / "fail-build-once"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        """#!/usr/bin/env bash
set -eu
if [ "${1:-}" = info ]; then
  exit 0
fi
if [ "${1:-}" = inspect ]; then
  printf 'healthy\\n'
  exit 0
fi
case " $* " in
  *' compose version '*) exit 0 ;;
  *' build '*)
    if [ ! -e "$FAKE_DOCKER_FAILURE_MARKER" ]; then
      : > "$FAKE_DOCKER_FAILURE_MARKER"
      exit 1
    fi
    ;;
  *' ps -q app '*) printf 'fake-container-id\\n' ;;
esac
exit 0
""",
        encoding="utf-8",
    )
    fake_docker.chmod(0o755)

    config_file = tmp_path / "private" / "rapid-inbox.env"
    environment = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DOCKER_FAILURE_MARKER": str(failure_marker),
        "RAPID_INBOX_COMPOSE_PROJECT": "rapid-inbox-test",
        "RAPID_INBOX_CONFIG_FILE": str(config_file),
        "RAPID_INBOX_HEALTH_TIMEOUT_SECONDS": "2",
    }
    first = subprocess.run(
        [str(root / "docker-deploy.sh")],
        cwd=root,
        env=environment,
        capture_output=True,
        text=True,
    )
    assert first.returncode != 0
    assert config_file.is_file()
    pending_marker = Path(f"{config_file}.credentials-pending")
    assert pending_marker.is_file()
    assert "Generated admin password (shown once)" not in first.stdout

    second = subprocess.run(
        [str(root / "docker-deploy.sh")],
        cwd=root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    assert "Generated admin password (shown once)" in second.stdout
    assert not pending_marker.exists()
