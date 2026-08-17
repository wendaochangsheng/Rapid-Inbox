#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
RUN_DIR="${RUN_DIR:-$ROOT_DIR/.rapid-inbox-run}"
HTTP_HOST="${HTTP_HOST:-${HOST:-}}"
HTTP_PORT="${HTTP_PORT:-${PORT:-}}"
HTTP_CONCURRENCY_LIMIT="${HTTP_CONCURRENCY_LIMIT:-}"
SMTP_HOST="${SMTP_HOST:-}"
SMTP_PORT="${SMTP_PORT:-}"
INSTALL_EXTRAS="${INSTALL_EXTRAS:-1}"
USE_CPP_INGESTD="${USE_CPP_INGESTD:-1}"
BUILD_LOCAL_INGESTD="${BUILD_LOCAL_INGESTD:-0}"
INGESTD_RELEASE_REPO="${INGESTD_RELEASE_REPO:-wendaochangsheng/Rapid-Inbox}"
INGESTD_VERSION="${INGESTD_VERSION:-latest}"
INGESTD_BINARY_URL="${INGESTD_BINARY_URL:-}"
INGESTD_BIN_DIR="${INGESTD_BIN_DIR:-$RUN_DIR/bin}"
INGESTD_BIN="$INGESTD_BIN_DIR/rapid-inbox-ingestd"
GENERATED_BOOTSTRAP_PASSWORD=""

usage() {
    cat <<'EOF'
Usage: bash quickstart.sh [--python-smtp] [--build-local] [--binary-url URL] [--ingestd-version VERSION] [--http-port PORT] [--smtp-port PORT] [--no-install]

Starts Rapid Inbox directly from the source tree for local evaluation and development.
For long-running deployments, use ./docker-deploy.sh (primary) or
deploy/system/install.sh (native systemd alternative).

Default mode:
  - Python HTTP bound to 127.0.0.1:8000
  - C++ rapid-inbox-ingestd bound to 0.0.0.0:25
  - Downloads a prebuilt ingestd binary from GitHub Releases when available

Fallback mode:
  - Pass --python-smtp to use the Python SMTP runner instead of C++ ingestd
  - Pass --build-local to compile ingestd on this machine instead of downloading

Release options:
  --binary-url URL          Download ingestd from an explicit .tar.gz URL
  --ingestd-version VALUE   Release tag to download, or mutable "latest" (default)

For repeatable deployments, pin a reviewed release tag with --ingestd-version.
Using "latest" prints a warning because it can select a different binary later.

Open:
  - Admin login: http://127.0.0.1:8000/admin/login
EOF
}

die() {
    printf 'quickstart: %s\n' "$*" >&2
    exit 1
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

require_cmd() {
    have_cmd "$1" || die "missing required command: $1"
}

dotenv_value() {
    local key="$1"
    if [ ! -f "$ROOT_DIR/.env" ]; then
        return 0
    fi
    "$PYTHON_BIN" - "$ROOT_DIR/.env" "$key" <<'PY'
import sys
from pathlib import Path

dotenv = Path(sys.argv[1])
key = sys.argv[2]
for raw_line in dotenv.read_text(encoding="utf-8").splitlines():
    line = raw_line.strip()
    if not line or line.startswith("#"):
        continue
    if line.startswith("export "):
        line = line[7:].strip()
    if "=" not in line:
        continue
    found_key, value = line.split("=", 1)
    if found_key.strip() != key:
        continue
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    print(value)
    raise SystemExit(0)
raise SystemExit(1)
PY
}

resolve_dotenv_value() {
    local variable_name="$1"
    local dotenv_key="$2"
    local default_value="$3"
    local value="${!variable_name:-}"

    if [ -z "$value" ]; then
        value="$(dotenv_value "$dotenv_key" || true)"
    fi
    if [ -z "$value" ]; then
        value="$default_value"
    fi
    printf -v "$variable_name" '%s' "$value"
}

ensure_venv() {
    if [ ! -x "$VENV_DIR/bin/python" ]; then
        require_cmd python3
        python3 -m venv "$VENV_DIR"
    fi
    PYTHON_BIN="$VENV_DIR/bin/python"
    if ! "$PYTHON_BIN" -m pip --version >/dev/null 2>&1; then
        "$PYTHON_BIN" -m ensurepip --upgrade >/dev/null 2>&1 || true
    fi
}

install_python_deps() {
    if [ "$INSTALL_EXTRAS" -ne 1 ]; then
        return
    fi
    "$PYTHON_BIN" -m pip install -c "$ROOT_DIR/constraints-dev.txt" -e "${ROOT_DIR}[dev]"
}

build_cpp_ingestd() {
    require_cmd cmake
    require_cmd c++
    cmake -S "$ROOT_DIR/cpp/ingestd" -B "$ROOT_DIR/cpp/ingestd/build" -DCMAKE_BUILD_TYPE=Release
    cmake --build "$ROOT_DIR/cpp/ingestd/build"
    INGESTD_BIN="$ROOT_DIR/cpp/ingestd/build/rapid-inbox-ingestd"
}

ingestd_asset_name() {
    local os
    local arch
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os:$arch" in
        Linux:x86_64|Linux:amd64)
            printf 'rapid-inbox-ingestd-linux-x86_64.tar.gz'
            ;;
        *)
            return 1
            ;;
    esac
}

ingestd_download_url() {
    local asset_name
    if [ -n "$INGESTD_BINARY_URL" ]; then
        printf '%s\n' "$INGESTD_BINARY_URL"
        return 0
    fi
    asset_name="$(ingestd_asset_name)" || return 1
    if [ "$INGESTD_VERSION" = "latest" ]; then
        printf '%s\n' "https://github.com/${INGESTD_RELEASE_REPO}/releases/latest/download/${asset_name}"
    else
        printf '%s\n' "https://github.com/${INGESTD_RELEASE_REPO}/releases/download/${INGESTD_VERSION}/${asset_name}"
    fi
}

download_cpp_ingestd() {
    local url
    url="$(ingestd_download_url)" || return 1
    mkdir -p "$INGESTD_BIN_DIR"
    printf 'Downloading prebuilt ingestd: %s\n' "$url"
    "$PYTHON_BIN" - "$url" "$INGESTD_BIN_DIR" <<'PY'
import hashlib
import os
import stat
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

url = sys.argv[1]
target_dir = Path(sys.argv[2])
target = target_dir / "rapid-inbox-ingestd"

request = urllib.request.Request(url, headers={"User-Agent": "Rapid-Inbox quickstart"})
token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
if token:
    request.add_header("Authorization", f"Bearer {token}")

with tempfile.TemporaryDirectory(prefix="rapid-inbox-ingestd-") as tmp:
    archive_path = Path(tmp) / "ingestd.tar.gz"
    with urllib.request.urlopen(request, timeout=30) as response:
        archive_path.write_bytes(response.read())

    expected_sha256 = os.environ.get("INGESTD_SHA256", "").strip().lower()
    if not expected_sha256:
        checksum_request = urllib.request.Request(
            url + ".sha256",
            headers={"User-Agent": "Rapid-Inbox quickstart"},
        )
        if token:
            checksum_request.add_header("Authorization", f"Bearer {token}")
        with urllib.request.urlopen(checksum_request, timeout=30) as response:
            expected_sha256 = response.read().decode("ascii").strip().split()[0].lower()
    actual_sha256 = hashlib.sha256(archive_path.read_bytes()).hexdigest()
    if len(expected_sha256) != 64 or actual_sha256 != expected_sha256:
        raise SystemExit("ingestd archive SHA-256 verification failed")

    with tarfile.open(archive_path, "r:gz") as archive:
        member = next(
            (
                item
                for item in archive.getmembers()
                if item.isfile() and Path(item.name).name == "rapid-inbox-ingestd"
            ),
            None,
        )
        if member is None:
            raise SystemExit("archive does not contain rapid-inbox-ingestd")
        extracted = archive.extractfile(member)
        if extracted is None:
            raise SystemExit("failed to extract rapid-inbox-ingestd")
        tmp_bin = Path(tmp) / "rapid-inbox-ingestd"
        tmp_bin.write_bytes(extracted.read())
        tmp_bin.chmod(tmp_bin.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
        target_dir.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_bin, target)

target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
PY
}

prepare_cpp_ingestd() {
    if [ "$BUILD_LOCAL_INGESTD" -eq 1 ]; then
        build_cpp_ingestd
        return
    fi
    if download_cpp_ingestd; then
        return
    fi
    printf 'quickstart: prebuilt ingestd download failed; falling back to local build.\n' >&2
    build_cpp_ingestd
}

initialize_env() {
    if [ -f "$ROOT_DIR/.env" ] || [ ! -f "$ROOT_DIR/.env.example" ]; then
        return
    fi
    GENERATED_BOOTSTRAP_PASSWORD="$($PYTHON_BIN - "$ROOT_DIR/.env.example" "$ROOT_DIR/.env" <<'PY'
import secrets
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text(encoding="utf-8")
target = Path(sys.argv[2])
password = secrets.token_urlsafe(24)
cursor_secret = secrets.token_urlsafe(48)
metrics_secret = secrets.token_urlsafe(32)
source = source.replace(
    "BOOTSTRAP_ADMIN_PASSWORD=change-me-now",
    f"BOOTSTRAP_ADMIN_PASSWORD={password}",
)
source = source.replace(
    "API_CURSOR_SECRET=",
    f"API_CURSOR_SECRET={cursor_secret}",
)
source = source.replace(
    "# METRICS_TOKEN=replace-with-a-long-random-token",
    f"METRICS_TOKEN={metrics_secret}",
)
target.write_text(source, encoding="utf-8")
target.chmod(0o600)
print(password)
PY
)"
}

initialize_database_schema() {
    printf 'Initializing database schema and migrations...\n'
    "$PYTHON_BIN" - "$ROOT_DIR" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root))

from app.config import default_settings
from app.db.connection import initialize_database

settings = default_settings(root)
initialize_database(settings.database_path)
PY
}

warn_if_external_http_bind() {
    local normalized_host
    normalized_host="${HTTP_HOST#[}"
    normalized_host="${normalized_host%]}"
    case "${normalized_host,,}" in
        localhost|::1|127.*)
            return
            ;;
    esac

    cat >&2 <<EOF

************************************************************************
SECURITY WARNING: HTTP is bound to non-loopback address ${HTTP_HOST}:${HTTP_PORT}.
Rapid Inbox does not terminate HTTP TLS. Put this management endpoint behind
a trusted HTTPS reverse proxy before exposing it to any untrusted network.
************************************************************************

EOF
}

warn_if_mutable_ingestd_release() {
    if [ "$USE_CPP_INGESTD" -ne 1 ] \
        || [ "$BUILD_LOCAL_INGESTD" -eq 1 ] \
        || [ -n "$INGESTD_BINARY_URL" ] \
        || [ "$INGESTD_VERSION" != "latest" ]; then
        return
    fi

    cat >&2 <<'EOF'
quickstart: WARNING: INGESTD_VERSION=latest is a mutable release pointer.
The SHA-256 check verifies the downloaded asset, but does not pin its version.
For repeatable deployments, use --ingestd-version with a reviewed release tag
or use --build-local from a reviewed source checkout.
EOF
}

wait_for_tcp() {
    local host="$1"
    local port="$2"
    local label="$3"
    "$PYTHON_BIN" - "$host" "$port" "$label" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
port = int(sys.argv[2])
label = sys.argv[3]
deadline = time.monotonic() + 30
last_error = None
while time.monotonic() < deadline:
    with socket.socket() as sock:
        sock.settimeout(0.5)
        try:
            sock.connect((host, port))
        except OSError as exc:
            last_error = exc
            time.sleep(0.2)
            continue
        raise SystemExit(0)
raise SystemExit(f"{label} did not become ready: {last_error}")
PY
}

wait_for_http_ready() {
    local host="$1"
    local port="$2"
    "$PYTHON_BIN" - "$host" "$port" "HTTP" <<'PY'
import json
import sys
import time
import urllib.error
import urllib.request

url = f"http://{sys.argv[1]}:{int(sys.argv[2])}/health/ready"
deadline = time.monotonic() + 30
last_error = None
while time.monotonic() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=1) as response:
            payload = json.load(response)
            if response.status == 200 and payload.get("status") == "ready":
                raise SystemExit(0)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        last_error = exc
    time.sleep(0.2)
raise SystemExit(f"HTTP readiness check failed: {last_error}")
PY
}

tail_logs_on_failure() {
    printf '\n--- HTTP log ---\n' >&2
    tail -n 80 "$RUN_DIR/http.log" >&2 || true
    if [ "$USE_CPP_INGESTD" -eq 1 ]; then
        printf '\n--- ingestd log ---\n' >&2
        tail -n 80 "$RUN_DIR/ingestd.log" >&2 || true
    fi
}

cleanup() {
    local exit_code=$?
    if [ -n "${HTTP_PID:-}" ] && kill -0 "$HTTP_PID" >/dev/null 2>&1; then
        kill "$HTTP_PID" >/dev/null 2>&1 || true
        wait "$HTTP_PID" >/dev/null 2>&1 || true
    fi
    if [ -n "${INGESTD_PID:-}" ] && kill -0 "$INGESTD_PID" >/dev/null 2>&1; then
        kill "$INGESTD_PID" >/dev/null 2>&1 || true
        wait "$INGESTD_PID" >/dev/null 2>&1 || true
    fi
    if [ "$exit_code" -ne 0 ]; then
        tail_logs_on_failure
    fi
}

handle_signal() {
    trap - INT TERM
    exit 0
}

if [ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ]; then
    usage
    exit 0
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --python-smtp)
            USE_CPP_INGESTD=0
            ;;
        --build-local)
            BUILD_LOCAL_INGESTD=1
            ;;
        --binary-url)
            shift
            [ $# -gt 0 ] || die "--binary-url needs a value"
            INGESTD_BINARY_URL="$1"
            ;;
        --ingestd-version)
            shift
            [ $# -gt 0 ] || die "--ingestd-version needs a value"
            INGESTD_VERSION="$1"
            ;;
        --http-port)
            shift
            [ $# -gt 0 ] || die "--http-port needs a value"
            HTTP_PORT="$1"
            ;;
        --smtp-port)
            shift
            [ $# -gt 0 ] || die "--smtp-port needs a value"
            SMTP_PORT="$1"
            ;;
        --no-install)
            INSTALL_EXTRAS=0
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            die "unknown option: $1"
            ;;
    esac
    shift
done

trap cleanup EXIT
trap handle_signal INT TERM

require_cmd python3
ensure_venv

initialize_env

if [ -z "${BOOTSTRAP_ADMIN_USERNAME:-}" ]; then
    BOOTSTRAP_ADMIN_USERNAME="$(dotenv_value BOOTSTRAP_ADMIN_USERNAME || true)"
fi
if [ -z "${BOOTSTRAP_ADMIN_PASSWORD:-}" ]; then
    BOOTSTRAP_ADMIN_PASSWORD="$(dotenv_value BOOTSTRAP_ADMIN_PASSWORD || true)"
fi

resolve_dotenv_value HTTP_HOST HOST "127.0.0.1"
resolve_dotenv_value HTTP_PORT PORT "8000"
resolve_dotenv_value HTTP_CONCURRENCY_LIMIT HTTP_CONCURRENCY_LIMIT "1000"
resolve_dotenv_value SMTP_HOST SMTP_HOST "0.0.0.0"
resolve_dotenv_value SMTP_PORT SMTP_PORT "25"

export HOST="$HTTP_HOST"
export PORT="$HTTP_PORT"
export HTTP_CONCURRENCY_LIMIT
export SMTP_HOST="$SMTP_HOST"
export SMTP_PORT="$SMTP_PORT"

mkdir -p "$RUN_DIR"
chmod 700 "$RUN_DIR"
: > "$RUN_DIR/http.log"
: > "$RUN_DIR/ingestd.log"
chmod 600 "$RUN_DIR/http.log" "$RUN_DIR/ingestd.log"

install_python_deps
initialize_database_schema
warn_if_external_http_bind

if [ "$USE_CPP_INGESTD" -eq 1 ]; then
    warn_if_mutable_ingestd_release
    prepare_cpp_ingestd
fi

if [ "$USE_CPP_INGESTD" -eq 1 ]; then
    "$VENV_DIR/bin/uvicorn" app.main:app --host "$HTTP_HOST" --port "$HTTP_PORT" \
        --limit-concurrency "$HTTP_CONCURRENCY_LIMIT" \
        --ws-max-size 16384 --ws-max-queue 1 > "$RUN_DIR/http.log" 2>&1 &
    HTTP_PID=$!
    "$INGESTD_BIN" --base-dir "$ROOT_DIR" > "$RUN_DIR/ingestd.log" 2>&1 &
    INGESTD_PID=$!
else
    "$VENV_DIR/bin/rapid-inbox-http" > "$RUN_DIR/http.log" 2>&1 &
    HTTP_PID=$!
fi

WAIT_HTTP_HOST="$HTTP_HOST"
if [ "$WAIT_HTTP_HOST" = "0.0.0.0" ] || [ "$WAIT_HTTP_HOST" = "::" ]; then
    WAIT_HTTP_HOST="127.0.0.1"
fi
wait_for_http_ready "$WAIT_HTTP_HOST" "$HTTP_PORT"
if [ "$USE_CPP_INGESTD" -eq 1 ]; then
    WAIT_SMTP_HOST="$SMTP_HOST"
    if [ "$WAIT_SMTP_HOST" = "0.0.0.0" ] || [ "$WAIT_SMTP_HOST" = "::" ]; then
        WAIT_SMTP_HOST="127.0.0.1"
    fi
    wait_for_tcp "$WAIT_SMTP_HOST" "$SMTP_PORT" "SMTP ingestd"
fi

printf 'Rapid Inbox is ready.\n'
printf 'HTTP bound to: %s:%s\n' "$HTTP_HOST" "$HTTP_PORT"
printf 'Admin login: http://127.0.0.1:%s/admin/login\n' "$HTTP_PORT"
if [ -n "$GENERATED_BOOTSTRAP_PASSWORD" ]; then
    printf 'Generated admin password (shown once): %s\n' "$GENERATED_BOOTSTRAP_PASSWORD"
fi
if [ "$USE_CPP_INGESTD" -eq 1 ]; then
    printf 'SMTP ingestd bound to: %s:%s\n' "$SMTP_HOST" "$SMTP_PORT"
else
    printf 'SMTP runner: Python embedded SMTP bound to %s:%s\n' "$SMTP_HOST" "$SMTP_PORT"
fi
printf 'Press Ctrl+C to stop.\n'

while true; do
    if ! kill -0 "$HTTP_PID" >/dev/null 2>&1; then
        die "HTTP process exited unexpectedly"
    fi
    if [ "$USE_CPP_INGESTD" -eq 1 ] && ! kill -0 "$INGESTD_PID" >/dev/null 2>&1; then
        die "ingestd process exited unexpectedly"
    fi
    sleep 1
done
