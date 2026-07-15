from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import quote

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "tools" / "http_stress_test.py"


def _load_script_module():
    spec = importlib.util.spec_from_file_location("http_stress_test", SCRIPT)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_http_stress_script_help_documents_safe_concurrency_options() -> None:
    environment_token = "ri_admin_help_must_stay_secret"
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
        env={**os.environ, "RAPID_INBOX_API_TOKEN": environment_token},
    )

    assert result.returncode == 0
    assert "--url" in result.stdout
    assert "--method" in result.stdout
    assert "--count" in result.stdout
    assert "--concurrency" in result.stdout
    assert "--timeout" in result.stdout
    assert "--keep-alive" in result.stdout
    assert "--json" in result.stdout
    assert "--json-output" in result.stdout
    assert "RAPID_INBOX_API_TOKEN" in result.stdout
    assert "/api/v2/domains" in result.stdout
    assert "GET" in result.stdout and "HEAD" in result.stdout
    assert "用法:" in result.stdout
    assert "选项:" in result.stdout
    assert environment_token not in result.stdout
    assert environment_token not in result.stderr


def test_http_stress_args_use_environment_token_and_allow_only_get_or_head() -> None:
    module = _load_script_module()
    args = module.parse_args(
        [
            "--url",
            "https://inbox.example/api/v2/domains?limit=1",
            "--method",
            "head",
            "--count",
            "25",
            "--concurrency",
            "5",
            "--timeout",
            "2.5",
            "--keep-alive",
            "0",
        ],
        environ={module.TOKEN_ENV_VAR: "ri_admin_environment_secret"},
    )
    module.validate_args(args)

    assert args.method == "HEAD"
    assert args.token == "ri_admin_environment_secret"
    assert args.count == 25
    assert args.concurrency == 5
    assert args.timeout == 2.5
    assert args.keep_alive == 0

    with pytest.raises(SystemExit):
        module.parse_args(["--method", "POST"], environ={})

    args.method = "DELETE"
    with pytest.raises(SystemExit, match="GET.*HEAD"):
        module.validate_args(args)


def test_http_stress_summary_reports_status_rps_percentiles_and_bytes() -> None:
    module = _load_script_module()
    summary = module.summarize_results(
        [
            module.RequestResult(True, 200, 10.0, 100),
            module.RequestResult(True, 204, 20.0, 0),
            module.RequestResult(False, None, 30.0, 0, "ConnectError: refused"),
            module.RequestResult(False, 500, 40.0, 50),
        ],
        2.0,
    )

    assert summary["attempted"] == 4
    assert summary["succeeded"] == 2
    assert summary["failed"] == 2
    assert summary["status_codes"] == {"200": 1, "204": 1, "500": 1}
    assert summary["requests_per_second"] == 2.0
    assert summary["successful_requests_per_second"] == 1.0
    assert summary["latency_ms_p50"] == 25.0
    assert summary["latency_ms_p95"] == 38.5
    assert summary["latency_ms_p99"] == 39.7
    assert summary["response_bytes"] == 150
    assert summary["response_bytes_per_second"] == 75.0
    assert summary["first_errors"] == ["ConnectError: refused"]


def test_http_stress_reports_and_errors_redact_bearer_token(monkeypatch, capsys) -> None:
    module = _load_script_module()
    token = "ri_admin_prefix_secret/value"
    encoded_token = quote(token, safe="")
    summary = module.summarize_results(
        [
            module.RequestResult(
                False,
                None,
                1.0,
                0,
                f"failed for {token} and {encoded_token}",
            )
        ],
        1.0,
        secrets=(token,),
    )
    report = {
        "generated_at": "2026-07-15T00:00:00Z",
        "target": {
            "url": module.redact_url(
                f"https://example.test/api/v2/domains?token={token}&cursor={token}",
                (token,),
            ),
            "method": "GET",
        },
        "configuration": {
            "count": 1,
            "concurrency": 1,
            "timeout_seconds": 1.0,
            "keep_alive_seconds": 5.0,
            "bearer_auth": True,
        },
        "results": summary,
    }
    serialized = module._json_text(report)
    module.print_summary(report)
    printed = capsys.readouterr().out

    assert token not in serialized
    assert encoded_token not in serialized
    assert token not in printed
    assert encoded_token not in printed
    assert "REDACTED" in serialized

    async def fail_with_secret(_args):
        raise RuntimeError(f"transport failed with {token}")

    monkeypatch.setattr(module, "run_stress", fail_with_secret)
    exit_code = module.main(["--token", token, "--count", "1"])
    captured = capsys.readouterr()
    assert exit_code == 2
    assert token not in captured.out
    assert token not in captured.err
    assert "REDACTED" in captured.err


def test_http_stress_json_shape_never_serializes_authorization_header() -> None:
    module = _load_script_module()
    token = "ri_service_prefix_confidential"
    headers = module.build_headers(token)

    assert headers["Authorization"] == f"Bearer {token}"
    public_configuration = {
        "count": 1,
        "concurrency": 1,
        "timeout_seconds": 10.0,
        "keep_alive_seconds": 5.0,
        "bearer_auth": bool(token),
    }
    serialized = json.dumps(public_configuration)
    assert token not in serialized
    assert "Authorization" not in serialized


@pytest.mark.asyncio
async def test_http_stress_runner_uses_one_reusable_connection_per_worker() -> None:
    module = _load_script_module()
    connection_count = 0
    request_count = 0

    async def handle_client(reader, writer) -> None:
        nonlocal connection_count, request_count
        connection_count += 1
        try:
            while True:
                try:
                    await reader.readuntil(b"\r\n\r\n")
                except (EOFError, asyncio.IncompleteReadError):
                    return
                request_count += 1
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Length: 2\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Connection: keep-alive\r\n\r\n"
                    b"ok"
                )
                await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    args = module.parse_args(
        [
            "--url",
            f"http://127.0.0.1:{port}/api/v2/domains",
            "--token",
            "ri_admin_test_token",
            "--count",
            "12",
            "--concurrency",
            "3",
            "--keep-alive",
            "5",
        ],
        environ={},
    )
    try:
        report = await module.run_stress(args)
    finally:
        server.close()
        await server.wait_closed()

    assert request_count == 12
    assert connection_count == 3
    assert report["results"]["attempted"] == 12
    assert report["results"]["succeeded"] == 12
    assert report["results"]["failed"] == 0
    assert report["results"]["status_codes"] == {"200": 12}
    assert report["results"]["response_bytes"] == 24
