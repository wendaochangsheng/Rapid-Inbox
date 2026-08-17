#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


WEBSOCKET_MAX_MESSAGE_BYTES = 16 * 1024
WEBSOCKET_MAX_QUEUE_MESSAGES = 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Rapid Inbox HTTP control plane")
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--check", action="store_true", help=argparse.SUPPRESS)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = args.base_dir.resolve(strict=True)
    os.chdir(base_dir)
    sys.path.insert(0, str(base_dir))

    import uvicorn

    from app.config import default_settings

    settings = default_settings(base_dir)
    if args.check:
        __import__("app.main")
        return 0
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=False,
        limit_concurrency=settings.http_concurrency_limit,
        ws_max_size=WEBSOCKET_MAX_MESSAGE_BYTES,
        ws_max_queue=WEBSOCKET_MAX_QUEUE_MESSAGES,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
