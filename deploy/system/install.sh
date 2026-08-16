#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly SCRIPT_DIR
SOURCE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"
readonly SOURCE_ROOT
readonly SERVICE_USER="rapid-inbox"
readonly SERVICE_GROUP="rapid-inbox"
readonly INSTALL_ROOT="/opt/rapid-inbox"
readonly RELEASES_DIR="$INSTALL_ROOT/releases"
readonly CURRENT_LINK="$INSTALL_ROOT/current"
readonly MANAGED_MARKER="$INSTALL_ROOT/.managed-by-system-installer"
readonly CONFIG_DIR="/etc/rapid-inbox"
readonly CONFIG_FILE="$CONFIG_DIR/rapid-inbox.env"
readonly PENDING_CREDENTIALS_FILE="$CONFIG_DIR/.bootstrap-credentials.pending"
readonly DATA_ROOT="/var/lib/rapid-inbox"
readonly UNIT_DIR="/etc/systemd/system"
readonly TARGET_UNIT="rapid-inbox.target"
readonly HTTP_UNIT="rapid-inbox-http.service"
readonly INGESTD_UNIT="rapid-inbox-ingestd.service"
readonly TARGET_WANTS_LINK="$UNIT_DIR/multi-user.target.wants/$TARGET_UNIT"

NEW_RELEASE=""
PREVIOUS_RELEASE=""
UNITS_TEMP_DIR=""
CONFIG_TEMP=""
CREDENTIALS_TEMP=""
MARKER_TEMP=""
DB_BACKUP_PATH=""
TRANSACTION_ACTIVE=0
SERVICES_STOPPED=0
CURRENT_SWITCHED=0
ALLOW_DB_RESTORE=0
DEPLOY_SUCCEEDED=0
FIRST_MANAGED_INSTALL=0
UNITS_INSTALLED=0

usage() {
    cat <<'EOF'
Usage: sudo bash deploy/system/install.sh <command>

Commands:
  install     Install Rapid Inbox, or idempotently deploy a new release
  update      Deploy this checkout over an existing managed installation
  status      Show service state and run HTTP plus SMTP protocol checks
  uninstall   Remove services and application releases; preserve config/data

This installer supports systemd-based Debian/Ubuntu-family Linux systems.
HTTP defaults to 127.0.0.1:8000. Configuration and data are kept in:
  /etc/rapid-inbox/rapid-inbox.env
  /var/lib/rapid-inbox
EOF
}

log() {
    printf 'rapid-inbox-system: %s\n' "$*"
}

die() {
    printf 'rapid-inbox-system: ERROR: %s\n' "$*" >&2
    exit 1
}

remove_release_dir() {
    local path="$1"
    case "$path" in
        "$RELEASES_DIR"/release-*) rm -rf --one-file-system -- "$path" ;;
        *) log "refusing to remove unexpected release path: $path" ;;
    esac
}

switch_current_to() {
    local target="$1"
    local temporary_link="$INSTALL_ROOT/.current.$$"
    rm -f -- "$temporary_link"
    ln -s "$target" "$temporary_link"
    mv -Tf -- "$temporary_link" "$CURRENT_LINK"
}

run_as_service() {
    runuser -u "$SERVICE_USER" -g "$SERVICE_GROUP" -- \
        env -i HOME="$DATA_ROOT" LANG=C.UTF-8 PATH="/usr/local/bin:/usr/bin:/bin" "$@"
}

cleanup_transients() {
    if [ -n "$CONFIG_TEMP" ]; then
        case "$CONFIG_TEMP" in
            "$CONFIG_DIR"/.rapid-inbox.env.*) rm -f -- "$CONFIG_TEMP" ;;
        esac
    fi
    if [ -n "$CREDENTIALS_TEMP" ]; then
        case "$CREDENTIALS_TEMP" in
            "$CONFIG_DIR"/.bootstrap-credentials.*) rm -f -- "$CREDENTIALS_TEMP" ;;
        esac
    fi
    if [ -n "$UNITS_TEMP_DIR" ]; then
        case "$UNITS_TEMP_DIR" in
            /tmp/rapid-inbox-units.*) rm -rf --one-file-system -- "$UNITS_TEMP_DIR" ;;
        esac
    fi
    if [ -n "$MARKER_TEMP" ]; then
        case "$MARKER_TEMP" in
            "$INSTALL_ROOT"/.managed-marker.*) rm -f -- "$MARKER_TEMP" ;;
        esac
    fi
}

rollback_deployment() {
    [ "$TRANSACTION_ACTIVE" -eq 1 ] || return 0
    log "deployment failed; restoring the previous managed state"

    if [ "$SERVICES_STOPPED" -eq 1 ]; then
        systemctl stop "$TARGET_UNIT" "$INGESTD_UNIT" "$HTTP_UNIT" >/dev/null 2>&1 || true
    fi

    if [ "$ALLOW_DB_RESTORE" -eq 1 ] && [ -f "$DB_BACKUP_PATH" ] && [ -n "$NEW_RELEASE" ]; then
        log "restoring the pre-migration SQLite backup"
        run_as_service \
            "$NEW_RELEASE/.venv/bin/python" \
            "$NEW_RELEASE/deploy/system/database_admin.py" \
            restore --base-dir "$NEW_RELEASE" --input "$DB_BACKUP_PATH" || \
            log "automatic database restore failed; backup retained at $DB_BACKUP_PATH"
    fi

    if [ "$CURRENT_SWITCHED" -eq 1 ]; then
        if [ -n "$PREVIOUS_RELEASE" ] && [ -d "$PREVIOUS_RELEASE" ]; then
            switch_current_to "$PREVIOUS_RELEASE" || true
        else
            rm -f -- "$CURRENT_LINK"
        fi
    fi

    if [ "$UNITS_INSTALLED" -eq 1 ]; then
        if [ "$FIRST_MANAGED_INSTALL" -eq 1 ]; then
            systemctl disable "$TARGET_UNIT" >/dev/null 2>&1 || true
            rm -f -- \
                "$TARGET_WANTS_LINK" \
                "$UNIT_DIR/$TARGET_UNIT" \
                "$UNIT_DIR/$HTTP_UNIT" \
                "$UNIT_DIR/$INGESTD_UNIT"
            rm -f -- "$MANAGED_MARKER"
        elif [ -d "$UNITS_TEMP_DIR/original" ]; then
            local unit_name
            for unit_name in "$TARGET_UNIT" "$HTTP_UNIT" "$INGESTD_UNIT"; do
                if [ -f "$UNITS_TEMP_DIR/original/$unit_name" ]; then
                    install -m 0644 -o root -g root \
                        "$UNITS_TEMP_DIR/original/$unit_name" "$UNIT_DIR/$unit_name"
                fi
            done
            if [ -f "$UNITS_TEMP_DIR/original/managed-marker" ]; then
                install -m 0644 -o root -g root \
                    "$UNITS_TEMP_DIR/original/managed-marker" "$MANAGED_MARKER"
            fi
        fi
    fi

    systemctl daemon-reload >/dev/null 2>&1 || true
    if [ "$SERVICES_STOPPED" -eq 1 ] && [ -n "$PREVIOUS_RELEASE" ] && [ -d "$PREVIOUS_RELEASE" ]; then
        systemctl start "$TARGET_UNIT" >/dev/null 2>&1 || \
            log "the previous release could not be restarted automatically"
    fi

    if [ -n "$NEW_RELEASE" ] && [ "$NEW_RELEASE" != "$PREVIOUS_RELEASE" ]; then
        remove_release_dir "$NEW_RELEASE"
    fi
}

on_exit() {
    local exit_code=$?
    trap - EXIT INT TERM
    set +e
    if [ "$exit_code" -ne 0 ] && [ "$DEPLOY_SUCCEEDED" -ne 1 ]; then
        rollback_deployment
    fi
    cleanup_transients
    exit "$exit_code"
}

trap on_exit EXIT
trap 'exit 130' INT TERM

require_root() {
    [ "${EUID:-$(id -u)}" -eq 0 ] || \
        die "root privileges are required; rerun with sudo"
}

detect_supported_system() {
    [ "$(uname -s)" = "Linux" ] || die "only Linux is supported"
    [ -r /etc/os-release ] || die "/etc/os-release is required"

    # os-release is the distribution-provided, shell-compatible identity file.
    # shellcheck disable=SC1091
    . /etc/os-release
    case " ${ID:-} ${ID_LIKE:-} " in
        *" debian "*|*" ubuntu "*) ;;
        *) die "unsupported distribution: expected Debian/Ubuntu or a compatible derivative" ;;
    esac

    if [ "${ID:-}" = "debian" ] && [ "${VERSION_ID%%.*}" -lt 12 ]; then
        die "Debian 12 or newer is required (Python >= 3.10 and CMake >= 3.25)"
    fi
    if [ "${ID:-}" = "ubuntu" ] && [ "${VERSION_ID%%.*}" -lt 24 ]; then
        die "Ubuntu 24.04 or newer is required (the packaged CMake must be >= 3.25)"
    fi

    command -v apt-get >/dev/null 2>&1 || die "apt-get is required"
    command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
    [ -d /run/systemd/system ] || die "systemd is not running as the system service manager"
}

validate_fixed_paths() {
    local path
    for path in \
        "$INSTALL_ROOT" \
        "$RELEASES_DIR" \
        "$MANAGED_MARKER" \
        "$CONFIG_DIR" \
        "$CONFIG_FILE" \
        "$PENDING_CREDENTIALS_FILE" \
        "$DATA_ROOT" \
        "$DATA_ROOT/storage" \
        "$DATA_ROOT/backups"; do
        [ ! -L "$path" ] || die "refusing to follow symlink at managed path: $path"
    done

    if [ -e "$CONFIG_FILE" ] && [ ! -f "$CONFIG_FILE" ]; then
        die "configuration path is not a regular file: $CONFIG_FILE"
    fi
    if [ -e "$MANAGED_MARKER" ] && [ ! -f "$MANAGED_MARKER" ]; then
        die "managed marker is not a regular file: $MANAGED_MARKER"
    fi
}

validate_first_install_boundary() {
    local unit_name unexpected_path
    for unit_name in "$TARGET_UNIT" "$HTTP_UNIT" "$INGESTD_UNIT"; do
        if [ -e "$UNIT_DIR/$unit_name" ] || [ -L "$UNIT_DIR/$unit_name" ]; then
            die "refusing to overwrite non-managed unit: $UNIT_DIR/$unit_name"
        fi
    done
    if [ -e "$TARGET_WANTS_LINK" ] || [ -L "$TARGET_WANTS_LINK" ]; then
        die "refusing to overwrite non-managed enablement link: $TARGET_WANTS_LINK"
    fi

    if [ -e "$INSTALL_ROOT" ]; then
        [ -d "$INSTALL_ROOT" ] || die "application root is not a directory: $INSTALL_ROOT"
        unexpected_path="$(find "$INSTALL_ROOT" -mindepth 1 -maxdepth 1 \
            ! -name releases -print -quit)"
        [ -z "$unexpected_path" ] || \
            die "$INSTALL_ROOT is not marked as managed and contains $unexpected_path"
        if [ -d "$RELEASES_DIR" ]; then
            unexpected_path="$(find "$RELEASES_DIR" -mindepth 1 -print -quit)"
            [ -z "$unexpected_path" ] || \
                die "unmanaged releases directory is not empty: $RELEASES_DIR"
        fi
    fi
}

validate_existing_managed_units() {
    local unit_name
    for unit_name in "$TARGET_UNIT" "$HTTP_UNIT" "$INGESTD_UNIT"; do
        if [ ! -f "$UNIT_DIR/$unit_name" ] || [ -L "$UNIT_DIR/$unit_name" ]; then
            die "managed unit is missing or not a regular file: $UNIT_DIR/$unit_name"
        fi
    done
}

install_dependencies() {
    log "installing operating-system dependencies"
    export DEBIAN_FRONTEND=noninteractive
    apt-get update
    apt-get install -y --no-install-recommends \
        ca-certificates \
        cmake \
        g++ \
        libicu-dev \
        libsqlite3-dev \
        libssl-dev \
        libunistring-dev \
        openssl \
        python3 \
        python3-pip \
        python3-venv \
        tar \
        util-linux

    python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 10))' || \
        die "Python 3.10 or newer is required"
    local cmake_version
    cmake_version="$(cmake --version | awk 'NR == 1 {print $3}')"
    [ "$(printf '%s\n%s\n' 3.25 "$cmake_version" | sort -V | head -n 1)" = "3.25" ] || \
        die "CMake 3.25 or newer is required; found $cmake_version"
}

validate_source_checkout() {
    [ -f "$SOURCE_ROOT/pyproject.toml" ] || die "pyproject.toml not found under $SOURCE_ROOT"
    [ -f "$SOURCE_ROOT/sqlite_schema.sql" ] || die "sqlite_schema.sql not found under $SOURCE_ROOT"
    [ -f "$SOURCE_ROOT/.env.example" ] || die ".env.example not found under $SOURCE_ROOT"
    [ -f "$SOURCE_ROOT/cpp/ingestd/CMakeLists.txt" ] || die "ingestd source not found under $SOURCE_ROOT"
    [ -f "$SCRIPT_DIR/init_db.py" ] || die "init_db.py is missing"
    [ -f "$SCRIPT_DIR/database_admin.py" ] || die "database_admin.py is missing"
    [ -f "$SCRIPT_DIR/run_http.py" ] || die "run_http.py is missing"
    [ -f "$SCRIPT_DIR/verify_deployment.py" ] || die "verify_deployment.py is missing"
}

ensure_service_account_and_directories() {
    if ! getent group "$SERVICE_GROUP" >/dev/null; then
        groupadd --system "$SERVICE_GROUP"
    fi
    if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
        local nologin_shell
        nologin_shell="$(command -v nologin || printf '/usr/sbin/nologin')"
        useradd --system --gid "$SERVICE_GROUP" --home-dir "$DATA_ROOT" \
            --no-create-home --shell "$nologin_shell" "$SERVICE_USER"
    fi
    [ "$(id -u "$SERVICE_USER")" -ne 0 ] || die "$SERVICE_USER must not resolve to uid 0"
    [ "$(getent group "$SERVICE_GROUP" | awk -F: '{print $3}')" -ne 0 ] || \
        die "$SERVICE_GROUP must not resolve to gid 0"
    case "$(getent passwd "$SERVICE_USER" | awk -F: '{print $7}')" in
        */nologin|*/false) ;;
        *) die "existing $SERVICE_USER account must use a nologin shell" ;;
    esac

    install -d -m 0755 -o root -g root "$INSTALL_ROOT" "$RELEASES_DIR"
    install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
    install -d -m 0700 -o "$SERVICE_USER" -g "$SERVICE_GROUP" \
        "$DATA_ROOT" "$DATA_ROOT/storage" "$DATA_ROOT/backups"
}

initialize_config_if_needed() {
    if [ -f "$CONFIG_FILE" ]; then
        chown "root:$SERVICE_GROUP" "$CONFIG_FILE"
        chmod 0640 "$CONFIG_FILE"
        log "preserving existing configuration: $CONFIG_FILE"
        return
    fi

    local bootstrap_username bootstrap_password cursor_secret metrics_token
    bootstrap_username="admin"
    bootstrap_password="$(openssl rand -hex 24)"
    cursor_secret="$(openssl rand -hex 32)"
    metrics_token="$(openssl rand -hex 24)"
    CONFIG_TEMP="$(mktemp "$CONFIG_DIR/.rapid-inbox.env.XXXXXX")"
    sed \
        -e "s|^STORAGE_ROOT=.*|STORAGE_ROOT=$DATA_ROOT/storage|" \
        -e "s|^DATABASE_PATH=.*|DATABASE_PATH=$DATA_ROOT/storage/app.db|" \
        -e "s|^BOOTSTRAP_ADMIN_USERNAME=.*|BOOTSTRAP_ADMIN_USERNAME=$bootstrap_username|" \
        -e "s|^BOOTSTRAP_ADMIN_PASSWORD=.*|BOOTSTRAP_ADMIN_PASSWORD=$bootstrap_password|" \
        -e 's|^HOST=.*|HOST=127.0.0.1|' \
        -e "s|^API_CURSOR_SECRET=.*|API_CURSOR_SECRET=$cursor_secret|" \
        -e "s|^# METRICS_TOKEN=.*|METRICS_TOKEN=$metrics_token|" \
        "$SOURCE_ROOT/.env.example" > "$CONFIG_TEMP"
    CREDENTIALS_TEMP="$(mktemp "$CONFIG_DIR/.bootstrap-credentials.XXXXXX")"
    {
        printf 'username=%s\n' "$bootstrap_username"
        printf 'password=%s\n' "$bootstrap_password"
    } > "$CREDENTIALS_TEMP"
    install -m 0600 -o root -g root "$CREDENTIALS_TEMP" "$PENDING_CREDENTIALS_FILE"
    install -m 0640 -o root -g "$SERVICE_GROUP" "$CONFIG_TEMP" "$CONFIG_FILE"
    rm -f -- "$CREDENTIALS_TEMP"
    CREDENTIALS_TEMP=""
    rm -f -- "$CONFIG_TEMP"
    CONFIG_TEMP=""
    unset bootstrap_username bootstrap_password cursor_secret metrics_token
    log "created private configuration at $CONFIG_FILE; bootstrap credentials will be shown after acceptance"
}

print_pending_credentials_once() {
    [ -f "$PENDING_CREDENTIALS_FILE" ] || return 0
    local username password
    username="$(awk -F= '$1 == "username" {print substr($0, index($0, "=") + 1); exit}' "$PENDING_CREDENTIALS_FILE")"
    password="$(awk -F= '$1 == "password" {print substr($0, index($0, "=") + 1); exit}' "$PENDING_CREDENTIALS_FILE")"
    if [ -z "$username" ] || [ -z "$password" ]; then
        die "pending bootstrap credentials are incomplete: $PENDING_CREDENTIALS_FILE"
    fi

    # Remove first so an interrupted terminal write cannot make a later run
    # print the same bootstrap secret a second time.
    rm -f -- "$PENDING_CREDENTIALS_FILE"
    printf '\nRapid Inbox initial administrator (shown once after successful acceptance):\n'
    printf '  username: %s\n' "$username"
    printf '  password: %s\n' "$password"
    printf 'Change this password at first login. It remains in %s until you edit it.\n\n' "$CONFIG_FILE"
    unset username password
}

warn_for_external_http() {
    local configured_host
    configured_host="$(awk -F= '$1 == "HOST" {value=substr($0, index($0, "=") + 1); print value; exit}' "$CONFIG_FILE")"
    configured_host="${configured_host%\"}"
    configured_host="${configured_host#\"}"
    configured_host="${configured_host%\'}"
    configured_host="${configured_host#\'}"
    case "${configured_host,,}" in
        ""|localhost|127.*|::1|'[::1]') return ;;
    esac
    printf '%s\n' \
        "rapid-inbox-system: WARNING: HTTP is configured on $configured_host." \
        "rapid-inbox-system: Put it behind a trusted HTTPS reverse proxy before external exposure." >&2
}

resolve_previous_release() {
    PREVIOUS_RELEASE=""
    if [ -L "$CURRENT_LINK" ]; then
        PREVIOUS_RELEASE="$(readlink -f "$CURRENT_LINK")"
        case "$PREVIOUS_RELEASE" in
            "$RELEASES_DIR"/release-*) ;;
            *) die "current symlink points outside the managed releases directory" ;;
        esac
        [ -d "$PREVIOUS_RELEASE" ] || die "current release does not exist: $PREVIOUS_RELEASE"
    elif [ -e "$CURRENT_LINK" ]; then
        die "$CURRENT_LINK exists but is not a managed symlink"
    fi
}

copy_source_to_release() {
    local release_name
    local -a release_inputs=(
        .env.example
        LICENSE
        README.md
        app
        constraints-dev.txt
        cpp/ingestd
        deploy/system
        pyproject.toml
        sqlite_schema.sql
    )
    release_name="release-$(date -u +%Y%m%dT%H%M%SZ)"
    NEW_RELEASE="$(mktemp -d "$RELEASES_DIR/$release_name-XXXXXXXX")"
    chmod 0755 "$NEW_RELEASE"

    log "staging source in $NEW_RELEASE"
    tar -C "$SOURCE_ROOT" \
        --exclude='cpp/ingestd/build*' \
        --exclude='*/__pycache__' \
        --exclude='*.pyc' \
        -cf - "${release_inputs[@]}" | tar --no-same-owner -C "$NEW_RELEASE" -xf -
    ln -s "$CONFIG_FILE" "$NEW_RELEASE/.env"
}

build_release() {
    log "building the Python environment"
    python3 -m venv "$NEW_RELEASE/.venv"
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
        "$NEW_RELEASE/.venv/bin/python" -m pip install \
        --constraint "$NEW_RELEASE/constraints-dev.txt" \
        --editable "$NEW_RELEASE"

    log "building the C++ SMTP ingest service"
    cmake -S "$NEW_RELEASE/cpp/ingestd" -B "$NEW_RELEASE/.build/ingestd" \
        -DCMAKE_BUILD_TYPE=Release
    cmake --build "$NEW_RELEASE/.build/ingestd" --config Release \
        --target rapid-inbox-ingestd --parallel
    install -d -m 0755 -o root -g root "$NEW_RELEASE/bin"
    install -m 0755 -o root -g root \
        "$NEW_RELEASE/.build/ingestd/rapid-inbox-ingestd" \
        "$NEW_RELEASE/bin/rapid-inbox-ingestd"
    "$NEW_RELEASE/bin/rapid-inbox-ingestd" --help >/dev/null
    run_as_service \
        "$NEW_RELEASE/.venv/bin/python" \
        "$NEW_RELEASE/deploy/system/run_http.py" \
        --base-dir "$NEW_RELEASE" --check
}

render_and_install_units() {
    UNITS_TEMP_DIR="$(mktemp -d /tmp/rapid-inbox-units.XXXXXX)"
    install -d -m 0700 "$UNITS_TEMP_DIR/rendered"
    if [ "$FIRST_MANAGED_INSTALL" -ne 1 ]; then
        install -d -m 0700 "$UNITS_TEMP_DIR/original"
        local unit_name
        for unit_name in "$TARGET_UNIT" "$HTTP_UNIT" "$INGESTD_UNIT"; do
            [ -f "$UNIT_DIR/$unit_name" ] || \
                die "managed unit is missing and cannot be backed up: $UNIT_DIR/$unit_name"
            cp -a -- "$UNIT_DIR/$unit_name" "$UNITS_TEMP_DIR/original/$unit_name"
        done
        cp -a -- "$MANAGED_MARKER" "$UNITS_TEMP_DIR/original/managed-marker"
    fi

    local template output
    for template in "$SCRIPT_DIR/templates"/*.in; do
        output="$UNITS_TEMP_DIR/rendered/$(basename "${template%.in}")"
        sed \
            -e "s|@CURRENT_DIR@|$CURRENT_LINK|g" \
            -e "s|@CONFIG_DIR@|$CONFIG_DIR|g" \
            -e "s|@CONFIG_FILE@|$CONFIG_FILE|g" \
            -e "s|@DATA_ROOT@|$DATA_ROOT|g" \
            -e "s|@SERVICE_USER@|$SERVICE_USER|g" \
            -e "s|@SERVICE_GROUP@|$SERVICE_GROUP|g" \
            "$template" > "$output"
    done

    UNITS_INSTALLED=1
    for output in "$UNITS_TEMP_DIR/rendered"/*; do
        install -m 0644 -o root -g root "$output" "$UNIT_DIR/$(basename "$output")"
    done
    systemctl daemon-reload
}

stop_all_services() {
    SERVICES_STOPPED=1
    log "stopping HTTP and SMTP writers before database migration"
    systemctl stop "$TARGET_UNIT" >/dev/null 2>&1 || true
    systemctl stop "$INGESTD_UNIT" "$HTTP_UNIT" >/dev/null 2>&1 || true
    if systemctl is-active --quiet "$INGESTD_UNIT" || systemctl is-active --quiet "$HTTP_UNIT"; then
        die "services did not stop cleanly; migration was not started"
    fi
}

backup_and_migrate_database() {
    DB_BACKUP_PATH="$DATA_ROOT/backups/$(basename "$NEW_RELEASE")-pre-migration.db"
    run_as_service \
        "$NEW_RELEASE/.venv/bin/python" \
        "$NEW_RELEASE/deploy/system/database_admin.py" \
        backup --base-dir "$NEW_RELEASE" --output "$DB_BACKUP_PATH"
    if [ -f "$DB_BACKUP_PATH" ]; then
        ALLOW_DB_RESTORE=1
        log "created consistent SQLite backup: $DB_BACKUP_PATH"
    fi

    log "initializing the database schema and migrations"
    run_as_service \
        "$NEW_RELEASE/.venv/bin/python" \
        "$NEW_RELEASE/deploy/system/init_db.py" --base-dir "$NEW_RELEASE"
    run_as_service \
        "$NEW_RELEASE/bin/rapid-inbox-ingestd" \
        --base-dir "$NEW_RELEASE" --writer-smoke
}

start_and_verify_release() {
    CURRENT_SWITCHED=1
    switch_current_to "$NEW_RELEASE"

    systemctl enable "$TARGET_UNIT"
    # Once listeners start, never restore an older DB snapshot automatically:
    # doing so could discard a message accepted during the acceptance window.
    ALLOW_DB_RESTORE=0
    systemctl start "$TARGET_UNIT"
    systemctl is-active --quiet "$TARGET_UNIT"
    systemctl is-active --quiet "$HTTP_UNIT"
    systemctl is-active --quiet "$INGESTD_UNIT"
    run_as_service \
        "$CURRENT_LINK/.venv/bin/python" \
        "$CURRENT_LINK/deploy/system/verify_deployment.py" \
        --base-dir "$CURRENT_LINK" --timeout 30
}

write_managed_marker() {
    MARKER_TEMP="$(mktemp "$INSTALL_ROOT/.managed-marker.XXXXXX")"
    {
        printf 'installer=deploy/system/install.sh\n'
        printf 'release=%s\n' "$(basename "$NEW_RELEASE")"
        printf 'installed_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    } > "$MARKER_TEMP"
    chmod 0644 "$MARKER_TEMP"
    mv -f -- "$MARKER_TEMP" "$MANAGED_MARKER"
    MARKER_TEMP=""
}

deploy() {
    local mode="$1"
    require_root
    detect_supported_system
    validate_source_checkout
    validate_fixed_paths

    if [ "$mode" = "update" ] && [ ! -f "$MANAGED_MARKER" ]; then
        die "no managed installation found; run install first"
    fi
    if [ ! -f "$MANAGED_MARKER" ]; then
        FIRST_MANAGED_INSTALL=1
        validate_first_install_boundary
    else
        validate_existing_managed_units
    fi
    resolve_previous_release

    TRANSACTION_ACTIVE=1
    install_dependencies
    ensure_service_account_and_directories
    initialize_config_if_needed
    warn_for_external_http

    # All network/package/build work completes while the previous release runs.
    copy_source_to_release
    build_release
    render_and_install_units

    # The write boundary begins here: no old writer remains during backup/migration.
    stop_all_services
    backup_and_migrate_database
    start_and_verify_release
    write_managed_marker

    DEPLOY_SUCCEEDED=1
    TRANSACTION_ACTIVE=0
    log "deployment is active: $NEW_RELEASE"
    log "HTTP management bind is configured in $CONFIG_FILE (default: 127.0.0.1:8000)"
    log "configuration: $CONFIG_FILE"
    log "data: $DATA_ROOT"
    print_pending_credentials_once
}

show_status() {
    require_root
    command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
    [ -f "$MANAGED_MARKER" ] || die "no managed Rapid Inbox installation found"
    [ -L "$CURRENT_LINK" ] || die "managed current release is missing"

    local failed=0 state enabled
    for unit in "$TARGET_UNIT" "$HTTP_UNIT" "$INGESTD_UNIT"; do
        state="$(systemctl is-active "$unit" 2>/dev/null || true)"
        enabled="$(systemctl is-enabled "$unit" 2>/dev/null || true)"
        printf '%-34s active=%-10s enabled=%s\n' "$unit" "${state:-unknown}" "${enabled:-unknown}"
        [ "$state" = "active" ] || failed=1
    done

    run_as_service \
        "$CURRENT_LINK/.venv/bin/python" \
        "$CURRENT_LINK/deploy/system/verify_deployment.py" \
        --base-dir "$CURRENT_LINK" --timeout 5 || failed=1
    return "$failed"
}

uninstall_managed() {
    require_root
    command -v systemctl >/dev/null 2>&1 || die "systemctl is required"
    if [ ! -f "$MANAGED_MARKER" ] || [ -L "$MANAGED_MARKER" ]; then
        die "no regular managed-install marker found; nothing was removed"
    fi
    [ ! -L "$INSTALL_ROOT" ] || die "refusing to remove symlinked application root: $INSTALL_ROOT"
    log "stopping and disabling managed services"
    systemctl disable --now "$TARGET_UNIT" >/dev/null 2>&1 || true
    systemctl stop "$INGESTD_UNIT" "$HTTP_UNIT" >/dev/null 2>&1 || true
    rm -f -- \
        "$TARGET_WANTS_LINK" \
        "$UNIT_DIR/$TARGET_UNIT" \
        "$UNIT_DIR/$HTTP_UNIT" \
        "$UNIT_DIR/$INGESTD_UNIT"
    systemctl daemon-reload
    systemctl reset-failed >/dev/null 2>&1 || true

    if [ -f "$MANAGED_MARKER" ] && [ -d "$INSTALL_ROOT" ]; then
        rm -rf --one-file-system -- "$INSTALL_ROOT"
    elif [ -e "$INSTALL_ROOT" ]; then
        log "application root is not marked as managed; leaving it untouched: $INSTALL_ROOT"
    fi

    log "application services and managed releases were removed"
    log "configuration preserved at $CONFIG_DIR"
    log "data and SQLite backups preserved at $DATA_ROOT"
    log "the $SERVICE_USER account was preserved so retained data keeps a stable owner"
}

main() {
    [ "$#" -eq 1 ] || { usage >&2; exit 2; }
    case "$1" in
        install) deploy install ;;
        update) deploy update ;;
        status) show_status ;;
        uninstall) uninstall_managed ;;
        --help|-h|help) usage ;;
        *) usage >&2; die "unknown command: $1" ;;
    esac
}

main "$@"
