#!/usr/bin/env bash
set -Eeuo pipefail

role="${1:-run}"

run_http() {
    exec uvicorn app.main:app \
        --host "${HOST:-0.0.0.0}" \
        --port "${PORT:-8000}" \
        --limit-concurrency "${HTTP_CONCURRENCY_LIMIT:-1000}"
}

run_ingestd() {
    exec /usr/local/bin/rapid-inbox-ingestd --base-dir /app
}

run_all() {
    local http_pid=""
    local ingestd_pid=""
    local shutdown_requested=0
    local startup_timeout="${HTTP_STARTUP_TIMEOUT_SECONDS:-120}"
    if [[ ! "$startup_timeout" =~ ^[0-9]+$ ]] || (( startup_timeout <= 0 )); then
        printf 'container startup: invalid HTTP_STARTUP_TIMEOUT_SECONDS\n' >&2
        return 1
    fi
    local startup_deadline=$((SECONDS + startup_timeout))

    request_shutdown() {
        shutdown_requested=1
        if [[ -n "$ingestd_pid" ]] && kill -0 "$ingestd_pid" 2>/dev/null; then
            kill -TERM "$ingestd_pid" 2>/dev/null || true
        fi
        if [[ -n "$http_pid" ]] && kill -0 "$http_pid" 2>/dev/null; then
            kill -TERM "$http_pid" 2>/dev/null || true
        fi
    }

    trap request_shutdown SIGINT SIGTERM

    uvicorn app.main:app \
        --host "${HOST:-0.0.0.0}" \
        --port "${PORT:-8000}" \
        --limit-concurrency "${HTTP_CONCURRENCY_LIMIT:-1000}" &
    http_pid=$!

    # The HTTP lifespan applies the schema and migrations before Uvicorn begins
    # serving. Do not expose SMTP until that initialization and recovery is ready.
    while (( SECONDS < startup_deadline )); do
        if (( shutdown_requested )); then
            wait "$http_pid" 2>/dev/null || true
            return 0
        fi
        if ! kill -0 "$http_pid" 2>/dev/null; then
            local http_status=0
            set +e
            wait "$http_pid"
            http_status=$?
            set -e
            if (( http_status == 0 )); then
                return 1
            fi
            return "$http_status"
        fi
        if python /usr/local/lib/rapid-inbox/healthcheck.py \
            http \
            --http-host 127.0.0.1 \
            --http-port "${PORT:-8000}" \
            --timeout 2 >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done

    if (( SECONDS >= startup_deadline )); then
        printf 'container startup: HTTP did not become ready before timeout\n' >&2
        request_shutdown
        wait "$http_pid" 2>/dev/null || true
        return 1
    fi

    if (( shutdown_requested )); then
        wait "$http_pid" 2>/dev/null || true
        return 0
    fi

    /usr/local/bin/rapid-inbox-ingestd --base-dir /app &
    ingestd_pid=$!

    local child_status=0
    set +e
    wait -n "$http_pid" "$ingestd_pid"
    child_status=$?
    set -e

    local orderly_shutdown="$shutdown_requested"
    if (( ! orderly_shutdown )); then
        printf 'container runtime: a managed process exited with status %s\n' \
            "$child_status" >&2
    fi
    request_shutdown

    set +e
    wait "$ingestd_pid" 2>/dev/null
    wait "$http_pid" 2>/dev/null
    set -e

    if (( orderly_shutdown )); then
        return 0
    fi
    if (( child_status == 0 )); then
        return 1
    fi
    return "$child_status"
}

case "$role" in
    run)
        run_all
        ;;
    http)
        run_http
        ;;
    ingestd)
        run_ingestd
        ;;
    *)
        exec "$@"
        ;;
esac
