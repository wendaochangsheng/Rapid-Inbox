#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Back up or restore the Rapid Inbox SQLite database")
    parser.add_argument("action", choices=("backup", "restore"))
    parser.add_argument("--base-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--input", type=Path)
    return parser.parse_args()


def settings_database_path(base_dir: Path) -> Path:
    sys.path.insert(0, str(base_dir))
    from app.config import default_settings

    return default_settings(base_dir).database_path


def check_integrity(connection: sqlite3.Connection, label: str) -> None:
    result = connection.execute("PRAGMA integrity_check").fetchone()
    if result is None or result[0] != "ok":
        raise RuntimeError(f"{label} failed SQLite integrity_check")


def open_read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"{path.resolve(strict=True).as_uri()}?mode=ro", uri=True)


def backup_database(database_path: Path, output: Path) -> None:
    if not database_path.exists():
        return
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if output.exists():
        raise FileExistsError(f"backup already exists: {output}")

    source = open_read_only(database_path)
    completed = False
    try:
        check_integrity(source, "source database")
        target = sqlite3.connect(output)
        try:
            source.backup(target)
            check_integrity(target, "backup database")
            completed = True
        finally:
            target.close()
    finally:
        source.close()
        if not completed:
            output.unlink(missing_ok=True)
    output.chmod(0o600)


def restore_database(database_path: Path, backup_path: Path) -> None:
    if not backup_path.is_file():
        raise FileNotFoundError(f"backup does not exist: {backup_path}")
    database_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)

    source = open_read_only(backup_path)
    temporary_path: Path | None = None
    try:
        check_integrity(source, "backup database")
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{database_path.name}.restore-",
            dir=database_path.parent,
        )
        os.close(descriptor)
        temporary_path = Path(temporary_name)
        target = sqlite3.connect(temporary_path)
        try:
            source.backup(target)
            check_integrity(target, "restored database")
        finally:
            target.close()
        temporary_path.chmod(0o600)
        for suffix in ("-wal", "-shm", "-journal"):
            Path(f"{database_path}{suffix}").unlink(missing_ok=True)
        os.replace(temporary_path, database_path)
        temporary_path = None
    finally:
        source.close()
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
            for suffix in ("-wal", "-shm", "-journal"):
                Path(f"{temporary_path}{suffix}").unlink(missing_ok=True)


def main() -> int:
    args = parse_args()
    base_dir = args.base_dir.resolve(strict=True)
    database_path = settings_database_path(base_dir)

    if args.action == "backup":
        if args.output is None or args.input is not None:
            raise SystemExit("backup requires --output and does not accept --input")
        backup_database(database_path, args.output.resolve())
    else:
        if args.input is None or args.output is not None:
            raise SystemExit("restore requires --input and does not accept --output")
        restore_database(database_path, args.input.resolve(strict=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
