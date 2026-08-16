#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import socket
import sys
import urllib.request
from typing import BinaryIO


def check_http(host: str, port: int, timeout: float) -> None:
    request = urllib.request.Request(
        f"http://{host}:{port}/health/ready",
        headers={"User-Agent": "rapid-inbox-container-healthcheck"},
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.load(response)
        if response.status != 200 or payload.get("status") != "ready":
            raise RuntimeError(
                f"HTTP readiness returned status={response.status}, payload={payload!r}"
            )


def _read_smtp_reply(stream: BinaryIO) -> int:
    first = stream.readline(4097)
    if not first or len(first) > 4096:
        raise RuntimeError("SMTP server returned an empty or oversized reply")
    if len(first) < 4 or not first[:3].isdigit() or first[3:4] not in {b" ", b"-"}:
        raise RuntimeError(f"SMTP server returned a malformed reply: {first!r}")

    code = int(first[:3])
    if first[3:4] == b"-":
        terminator = first[:3] + b" "
        for _ in range(99):
            line = stream.readline(4097)
            if not line or len(line) > 4096:
                raise RuntimeError("SMTP multiline reply was incomplete or oversized")
            if line.startswith(terminator):
                break
        else:
            raise RuntimeError("SMTP multiline reply exceeded 100 lines")
    return code


def _smtp_command(sock: socket.socket, stream: BinaryIO, command: bytes, expected: int) -> None:
    sock.sendall(command + b"\r\n")
    code = _read_smtp_reply(stream)
    if code != expected:
        raise RuntimeError(
            f"SMTP command {command.decode('ascii', 'replace')} returned {code}, expected {expected}"
        )


def check_smtp(host: str, port: int, timeout: float) -> None:
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        with sock.makefile("rb") as stream:
            banner = _read_smtp_reply(stream)
            if banner != 220:
                raise RuntimeError(f"SMTP banner returned {banner}, expected 220")
            _smtp_command(sock, stream, b"EHLO container-healthcheck", 250)
            _smtp_command(sock, stream, b"NOOP", 250)
            _smtp_command(sock, stream, b"QUIT", 221)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rapid Inbox container healthcheck")
    parser.add_argument("mode", choices=("all", "http", "smtp"))
    parser.add_argument("--http-host", default="127.0.0.1")
    parser.add_argument("--http-port", type=int, default=8000)
    parser.add_argument("--smtp-host", default="127.0.0.1")
    parser.add_argument("--smtp-port", type=int, default=2525)
    parser.add_argument("--timeout", type=float, default=2.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.mode in {"all", "http"}:
            check_http(args.http_host, args.http_port, args.timeout)
        if args.mode in {"all", "smtp"}:
            check_smtp(args.smtp_host, args.smtp_port, args.timeout)
    except Exception as exc:
        print(f"{args.mode} healthcheck failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
