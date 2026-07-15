#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import ssl
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, NamedTuple
from urllib.parse import parse_qsl, quote, quote_plus, urlencode, urlsplit, urlunsplit

import httpx


DEFAULT_URL = "http://127.0.0.1:8000/api/v2/domains"
TOKEN_ENV_VAR = "RAPID_INBOX_API_TOKEN"
USER_AGENT = "Rapid-Inbox-http-stress/1"
_REDACTED = "[REDACTED]"
_SENSITIVE_QUERY_NAMES = {
    "api_key",
    "apikey",
    "access_token",
    "auth",
    "authorization",
    "bearer",
    "credential",
    "key",
    "password",
    "secret",
    "token",
}


class ChineseArgumentParser(argparse.ArgumentParser):
    def format_help(self) -> str:
        return super().format_help().replace("usage:", "用法:").replace("options:", "选项:")

    def format_usage(self) -> str:
        return super().format_usage().replace("usage:", "用法:")


class RequestResult(NamedTuple):
    ok: bool
    status_code: int | None
    latency_ms: float
    response_bytes: int
    error: str | None = None


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def redact_text(value: object, secrets: Iterable[str] = ()) -> str:
    redacted = str(value)
    for raw_secret in secrets:
        secret = str(raw_secret or "")
        if not secret:
            continue
        variants = {secret, quote(secret, safe=""), quote_plus(secret, safe="")}
        for variant in sorted(variants, key=len, reverse=True):
            if variant:
                redacted = redacted.replace(variant, _REDACTED)
    return redacted


def redact_url(url: str, secrets: Iterable[str] = ()) -> str:
    parts = urlsplit(url)
    hostname = parts.hostname or ""
    if ":" in hostname and not hostname.startswith("["):
        hostname = f"[{hostname}]"
    port = f":{parts.port}" if parts.port is not None else ""
    netloc = f"{hostname}{port}"
    query = urlencode(
        [
            (name, _REDACTED if name.strip().lower() in _SENSITIVE_QUERY_NAMES else value)
            for name, value in parse_qsl(parts.query, keep_blank_values=True)
        ],
        doseq=True,
    )
    safe_url = urlunsplit((parts.scheme, netloc, parts.path, query, ""))
    return redact_text(safe_url, secrets)


def normalize_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = value.strip()
    if not token:
        return None
    if len(token) > 4096 or any(not 33 <= ord(character) <= 126 for character in token):
        raise SystemExit("Bearer token 格式无效。")
    return token


def validate_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise SystemExit("--url 必须是绝对 HTTP 或 HTTPS URL。")
    try:
        _ = parts.port
    except ValueError as exc:
        raise SystemExit("--url 包含无效端口。") from exc
    if parts.username is not None or parts.password is not None:
        raise SystemExit("--url 不允许包含用户凭据。")
    if parts.fragment:
        raise SystemExit("--url 不允许包含 URL fragment。")
    return url


def parse_args(
    argv: list[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> argparse.Namespace:
    environment = os.environ if environ is None else environ
    parser = ChineseArgumentParser(
        description="对 Rapid Inbox HTTP API 运行只读 GET/HEAD 高并发压测。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示帮助信息并退出。")
    parser.add_argument("--url", default=DEFAULT_URL, help=f"目标 URL，默认 {DEFAULT_URL}")
    parser.add_argument(
        "--method",
        type=str.upper,
        choices=("GET", "HEAD"),
        default="GET",
        help="只读 HTTP 方法：GET 或 HEAD，默认 GET。",
    )
    parser.add_argument(
        "--token",
        default=environment.get(TOKEN_ENV_VAR),
        help=f"Bearer token；建议改用环境变量 {TOKEN_ENV_VAR}，避免进入 shell 历史。",
    )
    parser.add_argument("--count", type=int, default=5000, help="请求总数，默认 5000。")
    parser.add_argument("--concurrency", type=int, default=100, help="并发 worker/连接上限，默认 100。")
    parser.add_argument("--timeout", type=float, default=10.0, help="单请求超时秒数，默认 10。")
    parser.add_argument(
        "--keep-alive",
        type=float,
        nargs="?",
        const=5.0,
        default=5.0,
        help="连接池 keep-alive 秒数；不带值时为 5，设为 0 禁用连接复用，默认 5。",
    )
    parser.add_argument("--json", action="store_true", help="仅向标准输出写 JSON 结果。")
    parser.add_argument("--json-output", type=Path, help="另将 JSON 结果写入文件。")
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    args.url = validate_url(str(args.url))
    args.token = normalize_token(args.token)
    args.method = str(args.method).upper()
    if args.method not in {"GET", "HEAD"}:
        raise SystemExit("--method 只允许 GET 或 HEAD。")
    if args.count < 1:
        raise SystemExit("--count 必须大于等于 1。")
    if args.concurrency < 1:
        raise SystemExit("--concurrency 必须大于等于 1。")
    if args.concurrency > 10_000:
        raise SystemExit("--concurrency 不得超过 10000。")
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        raise SystemExit("--timeout 必须大于 0。")
    if not math.isfinite(args.keep_alive) or args.keep_alive < 0:
        raise SystemExit("--keep-alive 必须大于等于 0。")


def build_headers(token: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    if not 0.0 <= quantile <= 1.0:
        raise ValueError("quantile must be between zero and one")
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower_index = int(position)
    upper_index = min(lower_index + 1, len(ordered) - 1)
    fraction = position - lower_index
    return ordered[lower_index] + (ordered[upper_index] - ordered[lower_index]) * fraction


def summarize_results(
    results: list[RequestResult],
    elapsed_seconds: float,
    *,
    secrets: Iterable[str] = (),
) -> dict[str, Any]:
    successes = [result for result in results if result.ok]
    latencies = [result.latency_ms for result in results]
    status_counts = Counter(result.status_code for result in results if result.status_code is not None)
    response_bytes = sum(max(int(result.response_bytes), 0) for result in results)
    elapsed = max(float(elapsed_seconds), 0.0)
    return {
        "attempted": len(results),
        "succeeded": len(successes),
        "failed": len(results) - len(successes),
        "elapsed_seconds": round(elapsed, 6),
        "requests_per_second": round(len(results) / elapsed, 3) if elapsed > 0 else 0.0,
        "successful_requests_per_second": round(len(successes) / elapsed, 3) if elapsed > 0 else 0.0,
        "latency_ms_p50": round(percentile(latencies, 0.50), 3),
        "latency_ms_p95": round(percentile(latencies, 0.95), 3),
        "latency_ms_p99": round(percentile(latencies, 0.99), 3),
        "response_bytes": response_bytes,
        "response_bytes_per_second": round(response_bytes / elapsed, 3) if elapsed > 0 else 0.0,
        "status_codes": {str(code): status_counts[code] for code in sorted(status_counts)},
        "first_errors": [
            redact_text(result.error, secrets)
            for result in results
            if result.error
        ][:5],
    }


async def _request_worker(
    *,
    worker_id: int,
    work: Iterable[int],
    method: str,
    url: str,
    headers: Mapping[str, str],
    ssl_context: ssl.SSLContext,
    timeout_seconds: float,
    keep_alive_seconds: float,
    results: list[RequestResult],
    secrets: tuple[str, ...],
) -> None:
    keep_alive_enabled = keep_alive_seconds > 0
    limits = httpx.Limits(
        max_connections=1,
        max_keepalive_connections=1 if keep_alive_enabled else 0,
        keepalive_expiry=keep_alive_seconds if keep_alive_enabled else None,
    )
    async with httpx.AsyncClient(
        headers=headers,
        verify=ssl_context,
        timeout=httpx.Timeout(timeout_seconds),
        limits=limits,
        follow_redirects=False,
        trust_env=False,
    ) as client:
        iterator = iter(work)
        while True:
            try:
                next(iterator)
            except StopIteration:
                return
            started = time.perf_counter()
            try:
                response = await client.request(method, url)
                latency_ms = (time.perf_counter() - started) * 1000
                status_code = int(response.status_code)
                results.append(
                    RequestResult(
                        ok=200 <= status_code < 300,
                        status_code=status_code,
                        latency_ms=latency_ms,
                        response_bytes=len(response.content),
                    )
                )
            except Exception as exc:
                latency_ms = (time.perf_counter() - started) * 1000
                error = redact_text(f"worker {worker_id}: {type(exc).__name__}: {exc}", secrets)
                results.append(
                    RequestResult(
                        ok=False,
                        status_code=None,
                        latency_ms=latency_ms,
                        response_bytes=0,
                        error=error,
                    )
                )


async def run_stress(args: argparse.Namespace) -> dict[str, Any]:
    validate_args(args)
    effective_concurrency = min(args.concurrency, args.count)
    secrets = (args.token,) if args.token else ()
    headers = build_headers(args.token)
    # Loading the platform CA bundle is comparatively expensive. Share the
    # immutable TLS context while keeping one independent connection pool per
    # worker; otherwise creating many clients serially dominates short runs.
    ssl_context = httpx.create_ssl_context(verify=True, trust_env=False)
    results: list[RequestResult] = []
    # Each worker receives a disjoint strided range, keeping scheduling and
    # memory bounded even for very large request counts.
    workloads = [range(worker_id, args.count, effective_concurrency) for worker_id in range(effective_concurrency)]

    started = time.perf_counter()
    await asyncio.gather(
        *(
            _request_worker(
                worker_id=worker_id + 1,
                work=workload,
                method=args.method,
                url=args.url,
                headers=headers,
                ssl_context=ssl_context,
                timeout_seconds=args.timeout,
                keep_alive_seconds=args.keep_alive,
                results=results,
                secrets=secrets,
            )
            for worker_id, workload in enumerate(workloads)
        )
    )
    elapsed = time.perf_counter() - started
    return {
        "generated_at": _utc_now(),
        "target": {
            "url": redact_url(args.url, secrets),
            "method": args.method,
        },
        "configuration": {
            "count": args.count,
            "concurrency": effective_concurrency,
            "timeout_seconds": args.timeout,
            "keep_alive_seconds": args.keep_alive,
            "bearer_auth": bool(args.token),
        },
        "results": summarize_results(results, elapsed, secrets=secrets),
    }


def format_bytes(value: float | int) -> str:
    size = max(float(value), 0.0)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TiB"


def print_summary(report: dict[str, Any]) -> None:
    target = report["target"]
    configuration = report["configuration"]
    result = report["results"]
    print("HTTP API 压测结果")
    print(f"目标: {target['method']} {target['url']}")
    print(
        "配置: "
        f"{configuration['count']} 请求，"
        f"并发 {configuration['concurrency']}，"
        f"超时 {configuration['timeout_seconds']} 秒，"
        f"keep-alive {configuration['keep_alive_seconds']} 秒，"
        f"Bearer {'已启用' if configuration['bearer_auth'] else '未启用'}"
    )
    print(
        "请求: "
        f"{result['succeeded']}/{result['attempted']} 成功，"
        f"{result['failed']} 失败，"
        f"耗时 {result['elapsed_seconds']} 秒，"
        f"RPS {result['requests_per_second']}"
    )
    print(
        "延迟: "
        f"P50 {result['latency_ms_p50']} ms，"
        f"P95 {result['latency_ms_p95']} ms，"
        f"P99 {result['latency_ms_p99']} ms"
    )
    print(
        "响应体: "
        f"{format_bytes(result['response_bytes'])}，"
        f"吞吐 {format_bytes(result['response_bytes_per_second'])}/s"
    )
    status_text = ", ".join(f"{code}={count}" for code, count in result["status_codes"].items()) or "无"
    print(f"状态码: {status_text}")
    if result["first_errors"]:
        print("前几个错误:")
        for error in result["first_errors"]:
            print(f"  - {error}")


def _json_text(report: dict[str, Any]) -> str:
    return json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    token = normalize_token(args.token)
    secrets = (token,) if token else ()
    try:
        validate_args(args)
        report = asyncio.run(run_stress(args))
        json_text = _json_text(report)
        if args.json_output is not None:
            args.json_output.write_text(json_text, encoding="utf-8")
        if args.json:
            sys.stdout.write(json_text)
        else:
            print_summary(report)
        return 0 if report["results"]["failed"] == 0 else 1
    except KeyboardInterrupt:
        print("压测已中断。", file=sys.stderr)
        return 130
    except SystemExit:
        raise
    except Exception as exc:
        print(f"压测失败: {redact_text(exc, secrets)}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
