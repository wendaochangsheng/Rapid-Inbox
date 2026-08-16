#!/usr/bin/env python3
from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import time
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Rapid Inbox HTTP and SMTP readiness")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--http-only", action="store_true")
    parser.add_argument("--smtp-only", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    if args.http_only and args.smtp_only:
        parser.error("--http-only and --smtp-only are mutually exclusive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    return args


def probe_host(configured_host: str) -> str:
    normalized = configured_host.strip().strip("[]")
    if normalized in {"", "0.0.0.0"}:
        return "127.0.0.1"
    if normalized == "::":
        return "::1"
    return normalized


def check_http(host: str, port: int, timeout: float) -> None:
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    try:
        connection.request("GET", "/health/ready", headers={"Connection": "close"})
        response = connection.getresponse()
        body = response.read(1_048_577)
        if len(body) > 1_048_576:
            raise RuntimeError("HTTP readiness response exceeded 1 MiB")
        payload = json.loads(body)
        if not isinstance(payload, dict) or response.status != 200 or payload.get("status") != "ready":
            raise RuntimeError(f"HTTP readiness returned {response.status}: {payload!r}")
    finally:
        connection.close()


def read_smtp_response(stream, expected_code: int) -> list[str]:
    lines: list[str] = []
    while True:
        raw_line = stream.readline(8193)
        if not raw_line:
            raise RuntimeError("SMTP connection closed before a complete response")
        if len(raw_line) > 8192:
            raise RuntimeError("SMTP response line exceeded 8192 bytes")
        line = raw_line.decode("utf-8", errors="replace").rstrip("\r\n")
        lines.append(line)
        if len(line) < 3 or not line[:3].isdigit():
            raise RuntimeError(f"invalid SMTP response: {line!r}")
        code = int(line[:3])
        if code != expected_code:
            raise RuntimeError(f"SMTP expected {expected_code}, received {line!r}")
        if len(line) == 3 or line[3:4] == " ":
            return lines
        if line[3:4] != "-":
            raise RuntimeError(f"invalid SMTP response separator: {line!r}")


def check_smtp(host: str, port: int, timeout: float) -> None:
    with socket.create_connection((host, port), timeout=timeout) as connection:
        connection.settimeout(timeout)
        stream = connection.makefile("rb")
        try:
            read_smtp_response(stream, 220)
            connection.sendall(b"EHLO rapid-inbox-system-check.local\r\n")
            read_smtp_response(stream, 250)
            connection.sendall(b"NOOP\r\n")
            read_smtp_response(stream, 250)
            connection.sendall(b"QUIT\r\n")
            read_smtp_response(stream, 221)
        finally:
            stream.close()


def retry(label: str, deadline: float, operation) -> None:
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            operation(max(0.2, min(2.0, deadline - time.monotonic())))
            return
        except (OSError, ValueError, RuntimeError) as exc:
            last_error = exc
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
    raise RuntimeError(f"{label} check failed: {last_error}")


def main() -> int:
    args = parse_args()
    base_dir = args.base_dir.resolve(strict=True)
    sys.path.insert(0, str(base_dir))
    from app.config import default_settings

    settings = default_settings(base_dir)
    failures: list[str] = []
    if not args.smtp_only:
        http_host = probe_host(settings.host)
        try:
            retry(
                "HTTP /health/ready",
                time.monotonic() + args.timeout,
                lambda timeout: check_http(http_host, settings.port, timeout),
            )
            if not args.quiet:
                print(f"HTTP ready: http://{http_host}:{settings.port}/health/ready")
        except RuntimeError as exc:
            failures.append(str(exc))

    if not args.http_only:
        smtp_host = probe_host(settings.smtp_host)
        try:
            retry(
                "SMTP banner/EHLO/NOOP/QUIT",
                time.monotonic() + args.timeout,
                lambda timeout: check_smtp(smtp_host, settings.smtp_port, timeout),
            )
            if not args.quiet:
                print(f"SMTP ready: {smtp_host}:{settings.smtp_port} (banner/EHLO/NOOP/QUIT)")
        except RuntimeError as exc:
            failures.append(str(exc))

    for failure in failures:
        print(f"verification failed: {failure}", file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
