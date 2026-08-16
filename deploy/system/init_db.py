#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize the Rapid Inbox database")
    parser.add_argument("--base-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_dir = args.base_dir.resolve(strict=True)
    sys.path.insert(0, str(base_dir))

    from app.config import default_settings
    from app.db.connection import initialize_database

    settings = default_settings(base_dir)
    settings.ensure_directories()
    initialize_database(settings.database_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
