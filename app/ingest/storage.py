from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic, sleep, time

from app.config import Settings


INGEST_STATUS_FILENAME = ".ingestd.status.json"
MAINTENANCE_LOCK_FILENAME = ".maintenance.lock"
MAINTENANCE_DRAINED_FILENAME = ".maintenance.drained.json"
INGEST_STATUS_FRESH_SECONDS = 3.0
MAINTENANCE_DRAIN_TIMEOUT_SECONDS = 30.0
MAINTENANCE_DRAIN_POLL_SECONDS = 0.05
STALE_PART_MIN_AGE_SECONDS = 60 * 60


@dataclass(frozen=True, slots=True)
class MaintenanceLease:
    path: Path
    token: str


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def path_date_parts(timestamp: str) -> tuple[str, str, str]:
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return (f"{dt.year:04d}", f"{dt.month:02d}", f"{dt.day:02d}")


def safe_filename(filename: str | None) -> str:
    base_name = filename or "attachment.bin"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._")
    cleaned = cleaned or "attachment.bin"
    # Leave ample room for the attachment id and temporary-file prefix under
    # common NAME_MAX=255 filesystems.
    if len(cleaned.encode("utf-8")) <= 160:
        return cleaned
    stem, suffix = os.path.splitext(cleaned)
    suffix = suffix[:20]
    budget = max(1, 160 - len(suffix.encode("utf-8")))
    return stem.encode("utf-8")[:budget].decode("utf-8", errors="ignore") + suffix


class FileStorage:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    @property
    def storage_root(self) -> Path:
        return self._settings.storage_root

    def raw_message_path(self, message_id: str, received_at: str) -> str:
        return self._dated_path("raw", message_id, ".eml", received_at)

    def manifest_path(self, message_id: str, received_at: str) -> str:
        return self._dated_path("manifests", message_id, ".json", received_at)

    def write_raw_message(self, message_id: str, received_at: str, content: bytes) -> tuple[str, str, int]:
        relative_path = self.raw_message_path(message_id, received_at)
        self._write_bytes(relative_path, content)
        digest = hashlib.sha256(content).hexdigest()
        return relative_path, digest, len(content)

    def write_text_body(self, message_id: str, received_at: str, content: str) -> str:
        relative_path = self._dated_path("text", message_id, ".txt", received_at)
        self._write_bytes(relative_path, content.encode("utf-8"))
        return relative_path

    def write_html_body(self, message_id: str, received_at: str, content: str) -> str:
        relative_path = self._dated_path("html", message_id, ".html", received_at)
        self._write_bytes(relative_path, content.encode("utf-8"))
        return relative_path

    def write_attachment(self, message_id: str, attachment_id: str, filename: str | None, content: bytes) -> tuple[str, str]:
        safe_name = safe_filename(filename)
        relative_path = str(Path("attachments") / message_id / f"{attachment_id}-{safe_name}")
        self._write_bytes(relative_path, content)
        return relative_path, safe_name

    def write_manifest(self, message_id: str, received_at: str, payload: dict[str, object]) -> str:
        relative_path = self.manifest_path(message_id, received_at)
        content = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        self._write_bytes(relative_path, content)
        return relative_path

    def read_bytes(self, relative_path: str) -> bytes:
        return self.resolve(relative_path).read_bytes()

    def read_text(self, relative_path: str | None) -> str | None:
        if not relative_path:
            return None
        return self.resolve(relative_path).read_text(encoding="utf-8")

    def read_bytes_limited(
        self,
        relative_path: str,
        max_bytes: int,
    ) -> tuple[bytes, bool, int]:
        """Read at most ``max_bytes`` and report truncation and source size."""

        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes < 1:
            raise ValueError("max_bytes must be a positive integer")
        path = self.resolve(relative_path)
        with path.open("rb") as handle:
            content = handle.read(max_bytes + 1)
            source_bytes = int(os.fstat(handle.fileno()).st_size)
        truncated = source_bytes > max_bytes or len(content) > max_bytes
        return content[:max_bytes], truncated, source_bytes

    def read_text_preview(
        self,
        relative_path: str | None,
        max_bytes: int,
    ) -> tuple[str, bool, int, int]:
        if not relative_path:
            return "", False, 0, 0
        content, truncated, source_bytes = self.read_bytes_limited(relative_path, max_bytes)
        # A byte cap may split a UTF-8 code point. Dropping only that partial
        # suffix keeps the preview valid without reading beyond the budget.
        text = content.decode("utf-8", errors="ignore")
        returned_bytes = len(text.encode("utf-8"))
        return text, truncated, source_bytes, returned_bytes

    def resolve(self, relative_path: str) -> Path:
        path = Path(relative_path)
        if path.is_absolute():
            raise ValueError("storage path must be relative")
        root = self.storage_root.resolve(strict=False)
        candidate = (root / path).resolve(strict=False)
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise ValueError("storage path escapes storage root") from exc
        return candidate

    def cleanup_stale_parts(self) -> None:
        # HTTP and ingestd can restart independently. Never unlink a temp file
        # merely because it is visible during startup: the other process may be
        # writing it right now. Age-gating leaves crash debris for a later pass
        # without corrupting an active durable publish.
        for category in (self._settings.raw_dir, self._settings.text_dir, self._settings.html_dir, self._settings.manifests_dir):
            for part_file in category.rglob("*.part"):
                self._unlink_stale_part(part_file)

        # Hidden temp files are the current write-ahead artifact naming scheme.
        for part_file in self.storage_root.rglob(".*.part"):
            self._unlink_stale_part(part_file)

        # C++ uses mkstemp names such as ``.msg.eml.tmp.A1B2C3``.
        for part_file in self.storage_root.rglob(".*.tmp.*"):
            self._unlink_stale_part(part_file)

    def _unlink_stale_part(self, part_file: Path) -> None:
        try:
            age_seconds = max(time() - part_file.stat().st_mtime, 0.0)
            if age_seconds < STALE_PART_MIN_AGE_SECONDS:
                return
            part_file.unlink(missing_ok=True)
        except OSError:
            return

    def cleanup_abandoned_clear_trash(self) -> None:
        for trash_path in self.storage_root.glob(".clear-mail-trash-*"):
            self._delete_tree_in_background(trash_path)

    def begin_maintenance(self, operation: str) -> MaintenanceLease:
        lock_path = self.storage_root / MAINTENANCE_LOCK_FILENAME
        token = uuid.uuid4().hex
        payload = json.dumps(
            {
                "operation": operation,
                "pid": os.getpid(),
                "started_at": utc_now(),
                "token": token,
            },
            sort_keys=True,
        )
        try:
            fd = os.open(lock_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError as exc:
            raise RuntimeError("another storage maintenance operation is active") from exc
        try:
            # Acquire the unique lease before deleting a stale acknowledgement,
            # but delete it before publishing this token. An ingest daemon may
            # observe an empty lock as fail-closed; it cannot acknowledge the
            # new token until the JSON payload is complete.
            (self.storage_root / MAINTENANCE_DRAINED_FILENAME).unlink(missing_ok=True)
            os.write(fd, payload.encode("utf-8"))
            os.fsync(fd)
        except BaseException:
            lock_path.unlink(missing_ok=True)
            raise
        finally:
            os.close(fd)
        return MaintenanceLease(path=lock_path, token=token)

    def wait_for_ingestd_drain(
        self,
        lease: MaintenanceLease,
        *,
        timeout_seconds: float | None = None,
        heartbeat_fresh_seconds: float | None = None,
        poll_seconds: float | None = None,
    ) -> None:
        timeout = MAINTENANCE_DRAIN_TIMEOUT_SECONDS if timeout_seconds is None else max(timeout_seconds, 0.0)
        freshness = INGEST_STATUS_FRESH_SECONDS if heartbeat_fresh_seconds is None else max(
            heartbeat_fresh_seconds,
            0.0,
        )
        poll = MAINTENANCE_DRAIN_POLL_SECONDS if poll_seconds is None else max(poll_seconds, 0.001)
        deadline = monotonic() + timeout
        status_path = self.storage_root / INGEST_STATUS_FILENAME
        drained_path = self.storage_root / MAINTENANCE_DRAINED_FILENAME

        while True:
            drained = self._read_json_object(drained_path)
            if drained is not None and drained.get("token") == lease.token:
                return

            try:
                status_stat = status_path.stat()
            except OSError:
                # ingestd publishes this file synchronously before opening the
                # SMTP listener. No status therefore means no process capable
                # of accepting a durable job under this storage root.
                return

            age_seconds = max(time() - status_stat.st_mtime, 0.0)
            if age_seconds > freshness:
                status = self._read_json_object(status_path)
                process_alive = self._status_process_is_alive(status)
                if process_alive is False:
                    # A stale lease owned by a provably-dead PID is crash debris.
                    # A replacement daemon must observe the already-published
                    # maintenance lock before it can expose its SMTP listener.
                    return
                # Stale does not mean dead: a live process may be blocked in its
                # status/logging thread while reservations, queued jobs, or a DB
                # transaction still exist. Malformed/permission-denied leases
                # are also fail-closed because their absence cannot be proven.

            if monotonic() >= deadline:
                raise TimeoutError("timed out waiting for ingestd to drain for maintenance")
            sleep(min(poll, max(deadline - monotonic(), 0.0)))

    def _status_process_is_alive(self, status: dict[str, object] | None) -> bool | None:
        if status is None:
            return None
        pid = status.get("pid")
        if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
            return None
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        except OSError:
            return None
        return True

    def end_maintenance(self, lease: MaintenanceLease | Path) -> None:
        lock_path = lease.path if isinstance(lease, MaintenanceLease) else lease
        token = lease.token if isinstance(lease, MaintenanceLease) else None
        try:
            lock_path.unlink(missing_ok=True)
            drained_path = self.storage_root / MAINTENANCE_DRAINED_FILENAME
            if token is None:
                drained_path.unlink(missing_ok=True)
            else:
                drained = self._read_json_object(drained_path)
                if drained is not None and drained.get("token") == token:
                    drained_path.unlink(missing_ok=True)
        finally:
            if self._settings.fsync_storage_writes:
                self._fsync_directory(self.storage_root)

    def _read_json_object(self, path: Path) -> dict[str, object] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, UnicodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def clear_mail_data(self) -> int:
        moved_directories = 0
        trash_root = self.storage_root / f".clear-mail-trash-{uuid.uuid4().hex}"
        for directory in (
            self._settings.raw_dir,
            self._settings.text_dir,
            self._settings.html_dir,
            self._settings.attachments_dir,
            self._settings.manifests_dir,
            self._settings.tmp_dir,
        ):
            if directory.exists():
                trash_root.mkdir(parents=True, exist_ok=True, mode=0o700)
                self._chmod_private(trash_root, directory=True)
                try:
                    directory.replace(trash_root / directory.name)
                    moved_directories += 1
                except OSError:
                    shutil.rmtree(directory, ignore_errors=True)
            directory.mkdir(parents=True, exist_ok=True, mode=0o700)
            self._chmod_private(directory, directory=True)
        if moved_directories:
            self._delete_tree_in_background(trash_root)
        else:
            shutil.rmtree(trash_root, ignore_errors=True)
        return moved_directories

    def _delete_tree_in_background(self, path: Path) -> None:
        thread = threading.Thread(
            target=shutil.rmtree,
            args=(path,),
            kwargs={"ignore_errors": True},
            daemon=True,
        )
        thread.start()

    def _dated_path(self, category: str, message_id: str, suffix: str, received_at: str) -> str:
        year, month, day = path_date_parts(received_at)
        return str(Path(category) / year / month / day / f"{message_id}{suffix}")

    def _write_bytes(self, relative_path: str, content: bytes) -> None:
        final_path = self.resolve(relative_path)
        self._ensure_private_directory(final_path.parent)
        # Keep write-ahead temp files hidden so final filenames can safely end in ".part".
        part_path = final_path.with_name(f".{final_path.name}.part")
        with part_path.open("wb") as handle:
            handle.write(content)
            if self._settings.fsync_storage_writes:
                handle.flush()
                os.fsync(handle.fileno())
        self._chmod_private(part_path)
        os.replace(part_path, final_path)
        self._chmod_private(final_path)
        if self._settings.fsync_storage_writes:
            self._fsync_directory_chain(final_path.parent)

    def _ensure_private_directory(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._chmod_directory_chain(directory)

    def _chmod_directory_chain(self, directory: Path) -> None:
        root = self.storage_root.resolve(strict=False)
        current = directory.resolve(strict=False)
        try:
            current.relative_to(root)
        except ValueError:
            return

        paths: list[Path] = []
        while True:
            paths.append(current)
            if current == root:
                break
            current = current.parent
        for path in reversed(paths):
            self._chmod_private(path, directory=True)

    def _chmod_private(self, path: Path, *, directory: bool = False) -> None:
        try:
            path.chmod(0o700 if directory else 0o600)
        except OSError:
            return

    def _fsync_directory_chain(self, directory: Path) -> None:
        current = directory
        while True:
            self._fsync_directory(current)
            if current.parent == current:
                break
            current = current.parent

    def _fsync_directory(self, directory: Path) -> None:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        try:
            directory_fd = os.open(directory, flags)
        except OSError:
            return
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
