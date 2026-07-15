from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import secrets
import sqlite3
import threading
import uuid
from collections import OrderedDict, deque
from datetime import datetime, timedelta, timezone
from functools import partial
from time import monotonic
from typing import Any

from app.config import DEFAULT_BOOTSTRAP_ADMIN_PASSWORD, Settings
from app.db.connection import connect_database
from app.db.writer import DatabaseWriter
from app.ingest.storage import utc_now

from .api_keys import ApiKeyAuthorizationError, ApiKeyService
from .passwords import hash_password, verify_password
from .permissions import PermissionContext, ROLE_SCOPES


SESSION_DURATION_DAYS = 30
SESSION_TOUCH_INTERVAL_SECONDS = 60
LOGIN_FAILURE_LIMIT = 5
LOGIN_IP_FAILURE_LIMIT = 50
LOGIN_FAILURE_WINDOW_SECONDS = 15 * 60
LOGIN_FAILURE_MAX_KEYS = 10_000
PASSWORD_WORKER_COUNT = 4
PASSWORD_PENDING_LIMIT = 64
PASSWORD_WAITER_LIMIT = 256
MIN_ADMIN_PASSWORD_LENGTH = 12
MAX_ADMIN_PASSWORD_LENGTH = 1024
VALID_ADMIN_ROLES = frozenset(ROLE_SCOPES)
_UNSET = object()


class AuthenticationOverloadedError(RuntimeError):
    """Raised when bounded password-work admission is exhausted."""

# A valid, precomputed PBKDF2 hash used whenever a username is absent. This
# keeps the expensive verification path comparable without creating a new salt
# or hash on every failed login.
DUMMY_PASSWORD_HASH = (
    "rapid-inbox-dummy-auth-salt-v1$"
    "3fdae81bd2c22471ad9ec67bf9379edf46e9ee7e8c62b37bc6834ce973e8df82"
)


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _utc_now_plus_days(days: int) -> str:
    return (
        datetime.now(timezone.utc).replace(microsecond=0) + timedelta(days=days)
    ).isoformat().replace("+00:00", "Z")


def _parse_utc_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


class AuthService:
    def __init__(
        self,
        settings: Settings,
        writer: DatabaseWriter,
        api_keys: ApiKeyService,
    ) -> None:
        self.settings = settings
        self.writer = writer
        self.api_keys = api_keys
        self._login_failure_lock = threading.Lock()
        self._login_failures: OrderedDict[str, deque[float]] = OrderedDict()
        self._login_ip_failures: OrderedDict[str, deque[float]] = OrderedDict()
        self._password_executor_lock = threading.Lock()
        self._password_executor: concurrent.futures.ThreadPoolExecutor | None = (
            concurrent.futures.ThreadPoolExecutor(
                max_workers=PASSWORD_WORKER_COUNT,
                thread_name_prefix="rapid-inbox-auth",
            )
        )
        self._password_slots = asyncio.Semaphore(PASSWORD_PENDING_LIMIT)
        self._password_waiter_lock = threading.Lock()
        self._password_waiters = 0
        self._password_waiter_limit = PASSWORD_WAITER_LIMIT
        self._password_executor_closed = threading.Event()

    async def count_admins(self) -> int:
        def operation() -> int:
            with connect_database(self.settings.database_path) as connection:
                row = connection.execute("SELECT COUNT(*) AS count FROM admins").fetchone()
            return int(row["count"])

        return await asyncio.to_thread(operation)

    def list_admins(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        if limit < 1 or limit > 1000:
            raise ValueError("invalid limit")
        if offset < 0:
            raise ValueError("invalid offset")
        with connect_database(self.settings.database_path) as connection:
            rows = connection.execute(
                """
                SELECT
                    id,
                    username,
                    display_name,
                    role,
                    is_active,
                    must_change_password,
                    created_at,
                    updated_at,
                    last_login_at,
                    last_login_ip
                FROM admins
                ORDER BY created_at ASC, id ASC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            total = connection.execute("SELECT COUNT(*) AS count FROM admins").fetchone()
        return {
            "items": [self._admin_payload(row) for row in rows],
            "total_count": 0 if total is None else int(total["count"]),
        }

    def get_admin(self, admin_id: int) -> dict[str, Any]:
        with connect_database(self.settings.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    username,
                    display_name,
                    role,
                    is_active,
                    must_change_password,
                    created_at,
                    updated_at,
                    last_login_at,
                    last_login_ip
                FROM admins
                WHERE id = ?
                """,
                (admin_id,),
            ).fetchone()
        if row is None:
            raise LookupError("admin not found")
        return self._admin_payload(row)

    async def create_admin(
        self,
        *,
        username: str,
        password: str,
        role: str = "viewer",
        display_name: str | None = None,
        is_active: bool = True,
        must_change_password: bool = True,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        normalized_username = self._normalize_username(username)
        normalized_role = self._normalize_role(role)
        normalized_display_name = self._normalize_display_name(display_name)
        normalized_is_active = self._coerce_bool("is_active", is_active)
        normalized_must_change_password = self._coerce_bool("must_change_password", must_change_password)
        self._validate_new_password(password)
        password_hash = await self._run_password_work(hash_password, password)
        now = utc_now()

        def operation(connection: sqlite3.Connection) -> int:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_admin_principal(
                connection,
                authorization_principal,
                required_scopes=("admins.write", "admins.credentials.write"),
            )
            self._assert_role_within_principal(principal, normalized_role)
            cursor = connection.execute(
                """
                INSERT INTO admins (
                    username,
                    display_name,
                    password_hash,
                    role,
                    is_active,
                    must_change_password,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    normalized_username,
                    normalized_display_name,
                    password_hash,
                    normalized_role,
                    int(normalized_is_active),
                    int(normalized_must_change_password),
                    now,
                    now,
                ),
            )
            return int(cursor.lastrowid)

        admin_id = await self.writer.execute(operation)
        return await asyncio.to_thread(self.get_admin, admin_id)

    async def update_admin(
        self,
        admin_id: int,
        *,
        username: object = _UNSET,
        display_name: object = _UNSET,
        role: object = _UNSET,
        is_active: object = _UNSET,
        must_change_password: object = _UNSET,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        updates: dict[str, Any] = {}
        if username is not _UNSET:
            updates["username"] = self._normalize_username(username)
        if display_name is not _UNSET:
            updates["display_name"] = self._normalize_display_name(display_name)
        if role is not _UNSET:
            updates["role"] = self._normalize_role(role)
        if is_active is not _UNSET:
            updates["is_active"] = int(self._coerce_bool("is_active", is_active))
        if must_change_password is not _UNSET:
            updates["must_change_password"] = int(
                self._coerce_bool("must_change_password", must_change_password)
            )
        if not updates:
            return await asyncio.to_thread(self.get_admin, admin_id)

        now = utc_now()

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_admin_principal(
                connection,
                authorization_principal,
                required_scopes=("admins.write",),
            )
            row = connection.execute(
                "SELECT id, role, is_active FROM admins WHERE id = ?",
                (admin_id,),
            ).fetchone()
            if row is None:
                raise LookupError("admin not found")
            next_role = str(updates.get("role", row["role"]))
            # A bounded administrator may neither manage an account already
            # above its authority nor assign a role above its authority.
            self._assert_role_within_principal(principal, str(row["role"]))
            self._assert_role_within_principal(principal, next_role)
            next_is_active = bool(updates.get("is_active", row["is_active"]))
            self._ensure_active_superadmin_remains(
                connection,
                row,
                next_role=next_role,
                next_is_active=next_is_active,
            )

            assignments = [f"{column} = ?" for column in updates]
            connection.execute(
                f"UPDATE admins SET {', '.join(assignments)}, updated_at = ? WHERE id = ?",
                (*updates.values(), now, admin_id),
            )
            if bool(row["is_active"]) and not next_is_active:
                self._revoke_admin_sessions_in_connection(connection, admin_id, revoked_at=now)

        await self.writer.execute(operation)
        return await asyncio.to_thread(self.get_admin, admin_id)

    async def delete_admin(
        self,
        admin_id: int,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_admin_principal(
                connection,
                authorization_principal,
                required_scopes=("admins.write",),
            )
            row = connection.execute(
                """
                SELECT
                    id,
                    username,
                    display_name,
                    role,
                    is_active,
                    must_change_password,
                    created_at,
                    updated_at,
                    last_login_at,
                    last_login_ip
                FROM admins
                WHERE id = ?
                """,
                (admin_id,),
            ).fetchone()
            if row is None:
                raise LookupError("admin not found")
            self._assert_role_within_principal(principal, str(row["role"]))
            self._ensure_active_superadmin_remains(
                connection,
                row,
                next_role=str(row["role"]),
                next_is_active=bool(row["is_active"]),
                deleting=True,
            )
            connection.execute("DELETE FROM admins WHERE id = ?", (admin_id,))
            return self._admin_payload(row)

        return await self.writer.execute(operation)

    async def ensure_bootstrap_admin(self) -> None:
        password_hash = await self._run_password_work(
            hash_password,
            self.settings.bootstrap_admin_password,
        )
        now = utc_now()
        await self.writer.execute(
            lambda connection: connection.execute(
                """
                INSERT INTO admins (
                    username,
                    password_hash,
                    must_change_password,
                    created_at,
                    updated_at
                )
                SELECT ?, ?, 1, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM admins
                )
                """,
                (
                    self.settings.bootstrap_admin_username,
                    password_hash,
                    now,
                    now,
                ),
            )
        )

    async def authenticate_admin(self, username: str, password: str, *, ip: str | None = None) -> dict[str, Any]:
        self.assert_login_allowed(username, ip=ip)
        if not isinstance(password, str) or not password or len(password) > MAX_ADMIN_PASSWORD_LENGTH:
            self.record_login_failure(username, ip=ip)
            raise LookupError("invalid admin credentials")

        def load_and_verify() -> tuple[dict[str, Any] | None, bool]:
            with connect_database(self.settings.database_path) as connection:
                stored_row = connection.execute(
                    """
                    SELECT
                        id,
                        username,
                        display_name,
                        role,
                        is_active,
                        must_change_password,
                        created_at,
                        updated_at,
                        last_login_at,
                        last_login_ip,
                        password_hash
                    FROM admins
                    WHERE username = ? AND is_active = 1
                    """,
                    (username,),
                ).fetchone()
            payload = None if stored_row is None else dict(stored_row)
            stored_hash = DUMMY_PASSWORD_HASH if payload is None else str(payload["password_hash"])
            return payload, verify_password(password, stored_hash)

        row, password_matches = await self._run_password_work(load_and_verify)
        if row is None or not password_matches:
            self.record_login_failure(username, ip=ip)
            raise LookupError("invalid admin credentials")

        now = utc_now()
        expected_username = str(row["username"])
        expected_password_hash = str(row["password_hash"])

        def finalize_authentication(connection: sqlite3.Connection) -> bool:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE admins
                SET last_login_at = ?,
                    last_login_ip = COALESCE(?, last_login_ip)
                WHERE id = ?
                  AND is_active = 1
                  AND username = ?
                  AND password_hash = ?
                """,
                (
                    now,
                    ip,
                    row["id"],
                    expected_username,
                    expected_password_hash,
                ),
            )
            return cursor.rowcount == 1

        if not await self.writer.execute(finalize_authentication):
            self.record_login_failure(username, ip=ip)
            raise LookupError("invalid admin credentials")

        self.clear_login_failures(username, ip=ip)
        admin = self._admin_payload(row)
        admin["last_login_at"] = now
        admin["last_login_ip"] = ip if ip is not None else row["last_login_ip"]
        # Internal one-use proof consumed by the browser login flow. It is
        # deliberately absent from `_admin_payload` and removed before audit
        # or template data is created.
        admin["_password_hash_proof"] = expected_password_hash
        return admin

    def assert_login_allowed(self, username: str, *, ip: str | None = None) -> None:
        key = self._login_failure_key(username, ip)
        ip_key = self._login_ip_key(ip)
        now = monotonic()
        cutoff = now - LOGIN_FAILURE_WINDOW_SECONDS
        with self._login_failure_lock:
            window = self._active_login_window(self._login_failures, key, cutoff)
            ip_window = self._active_login_window(self._login_ip_failures, ip_key, cutoff)
            if (window is not None and len(window) >= LOGIN_FAILURE_LIMIT) or (
                ip_window is not None and len(ip_window) >= LOGIN_IP_FAILURE_LIMIT
            ):
                raise PermissionError("too many login attempts")

    def record_login_failure(self, username: str, *, ip: str | None = None) -> None:
        key = self._login_failure_key(username, ip)
        ip_key = self._login_ip_key(ip)
        now = monotonic()
        cutoff = now - LOGIN_FAILURE_WINDOW_SECONDS
        with self._login_failure_lock:
            self._record_login_window(self._login_failures, key, now, cutoff)
            self._record_login_window(self._login_ip_failures, ip_key, now, cutoff)

    def clear_login_failures(self, username: str, *, ip: str | None = None) -> None:
        key = self._login_failure_key(username, ip)
        with self._login_failure_lock:
            self._login_failures.pop(key, None)

    def _login_failure_key(self, username: str, ip: str | None) -> str:
        normalized_username = username.strip().casefold().encode("utf-8", errors="replace")
        username_digest = hashlib.sha256(normalized_username).hexdigest()
        return f"{self._login_ip_key(ip)}:{username_digest}"

    def _login_ip_key(self, ip: str | None) -> str:
        return str(ip or "unknown")[:128]

    def _active_login_window(
        self,
        windows: OrderedDict[str, deque[float]],
        key: str,
        cutoff: float,
    ) -> deque[float] | None:
        window = windows.get(key)
        if window is None:
            return None
        while window and window[0] <= cutoff:
            window.popleft()
        if not window:
            windows.pop(key, None)
            return None
        windows.move_to_end(key)
        return window

    def _record_login_window(
        self,
        windows: OrderedDict[str, deque[float]],
        key: str,
        now: float,
        cutoff: float,
    ) -> None:
        window = self._active_login_window(windows, key, cutoff)
        if window is None:
            while len(windows) >= LOGIN_FAILURE_MAX_KEYS:
                windows.popitem(last=False)
            window = deque()
            windows[key] = window
        window.append(now)
        windows.move_to_end(key)

    async def change_admin_password(
        self,
        admin_id: int,
        current_password: str,
        new_password: str,
        *,
        current_session_id: str | None = None,
    ) -> None:
        self._validate_new_password(new_password)
        if not isinstance(current_password, str) or not current_password or len(current_password) > MAX_ADMIN_PASSWORD_LENGTH:
            raise LookupError("invalid admin credentials")

        def verify_and_hash() -> tuple[str, str]:
            with connect_database(self.settings.database_path) as connection:
                row = connection.execute(
                    """
                    SELECT id, password_hash
                    FROM admins
                    WHERE id = ? AND is_active = 1
                    """,
                    (admin_id,),
                ).fetchone()
            if row is None or not verify_password(current_password, row["password_hash"]):
                raise LookupError("invalid admin credentials")
            if verify_password(new_password, row["password_hash"]):
                raise ValueError("password must be changed")
            return hash_password(new_password), str(row["password_hash"])

        password_hash, expected_password_hash = await self._run_password_work(
            verify_and_hash
        )

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute("BEGIN IMMEDIATE")
            # Authorize against the time at which the mutation actually owns
            # the writer transaction. A request may spend time queued after
            # proving the old password, during which its session can expire.
            updated_at = utc_now()
            if current_session_id is not None:
                session = connection.execute(
                    """
                    SELECT s.id
                    FROM admin_sessions AS s
                    JOIN admins AS a ON a.id = s.admin_id
                    WHERE s.id = ?
                      AND s.admin_id = ?
                      AND s.revoked_at IS NULL
                      AND s.expires_at > ?
                      AND a.is_active = 1
                    """,
                    (current_session_id, admin_id, updated_at),
                ).fetchone()
                if session is None:
                    raise ApiKeyAuthorizationError(
                        "acting admin session is no longer active"
                    )
            cursor = connection.execute(
                """
                UPDATE admins
                SET password_hash = ?,
                    must_change_password = 0,
                    updated_at = ?
                WHERE id = ?
                  AND is_active = 1
                  AND password_hash = ?
                """,
                (
                    password_hash,
                    updated_at,
                    admin_id,
                    expected_password_hash,
                ),
            )
            if cursor.rowcount != 1:
                # A password reset/change that committed while this request
                # was hashing invalidates its proof of the former password.
                raise LookupError("invalid admin credentials")
            self._revoke_admin_sessions_in_connection(
                connection,
                admin_id,
                revoked_at=updated_at,
                except_session_id=current_session_id,
            )

        await self.writer.execute(operation)

    async def reset_admin_password(
        self,
        admin_id: int,
        new_password: str,
        *,
        must_change_password: bool = True,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        self._validate_new_password(new_password)
        normalized_must_change = self._coerce_bool("must_change_password", must_change_password)
        password_hash = await self._run_password_work(hash_password, new_password)
        updated_at = utc_now()

        def operation(connection: sqlite3.Connection) -> None:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_admin_principal(
                connection,
                authorization_principal,
                required_scopes=("admins.credentials.write",),
            )
            row = connection.execute(
                "SELECT role FROM admins WHERE id = ?",
                (admin_id,),
            ).fetchone()
            if row is None:
                raise LookupError("admin not found")
            self._assert_role_within_principal(principal, str(row["role"]))
            cursor = connection.execute(
                """
                UPDATE admins
                SET password_hash = ?,
                    must_change_password = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (password_hash, int(normalized_must_change), updated_at, admin_id),
            )
            if cursor.rowcount != 1:
                raise LookupError("admin not found")
            self._revoke_admin_sessions_in_connection(connection, admin_id, revoked_at=updated_at)

        await self.writer.execute(operation)
        return await asyncio.to_thread(self.get_admin, admin_id)

    async def create_session(
        self,
        *,
        admin_id: int,
        ip: str | None,
        user_agent: str | None,
        expected_password_hash: str | None = None,
    ) -> dict[str, Any]:
        session_id = f"sess_{uuid.uuid4().hex}"
        token = secrets.token_urlsafe(32)
        token_hash = _hash_session_token(token)
        created_at = utc_now()
        expires_at = _utc_now_plus_days(SESSION_DURATION_DAYS)

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            password_predicate = ""
            params: list[Any] = [admin_id]
            if expected_password_hash is not None:
                password_predicate = " AND password_hash = ?"
                params.append(str(expected_password_hash))
            admin_row = connection.execute(
                f"""
                SELECT id
                FROM admins
                WHERE id = ? AND is_active = 1
                {password_predicate}
                """,
                tuple(params),
            ).fetchone()
            if admin_row is None:
                raise LookupError("admin not found")

            connection.execute(
                """
                INSERT INTO admin_sessions (
                    id,
                    admin_id,
                    session_token_hash,
                    created_at,
                    expires_at,
                    last_seen_at,
                    last_ip,
                    user_agent
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    admin_id,
                    token_hash,
                    created_at,
                    expires_at,
                    created_at,
                    ip,
                    user_agent,
                ),
            )
            return {
                "id": session_id,
                "admin_id": admin_id,
                "token": token,
                "created_at": created_at,
                "expires_at": expires_at,
                "last_seen_at": created_at,
                "last_ip": ip,
                "user_agent": user_agent,
            }

        return await self.writer.execute(operation)

    async def get_session_admin(self, token: str, *, ip: str | None = None) -> dict[str, Any]:
        token_hash = _hash_session_token(token)
        now = utc_now()

        def load_session() -> dict[str, Any] | None:
            with connect_database(self.settings.database_path) as connection:
                row = connection.execute(
                    """
                    SELECT
                        s.id AS session_id,
                        s.admin_id,
                        s.created_at AS session_created_at,
                        s.expires_at,
                        s.last_seen_at AS session_last_seen_at,
                        s.last_ip AS session_last_ip,
                        s.user_agent AS session_user_agent,
                        a.id,
                        a.username,
                        a.display_name,
                        a.role,
                        a.is_active,
                        a.must_change_password,
                        a.created_at,
                        a.updated_at,
                        a.last_login_at,
                        a.last_login_ip
                    FROM admin_sessions AS s
                    JOIN admins AS a ON a.id = s.admin_id
                    WHERE s.session_token_hash = ?
                        AND s.revoked_at IS NULL
                        AND s.expires_at > ?
                        AND a.is_active = 1
                    """,
                    (token_hash, now),
                ).fetchone()
            return None if row is None else dict(row)

        row = await asyncio.to_thread(load_session)

        if row is None:
            raise LookupError("session not found")

        last_seen_at = row["session_last_seen_at"]
        last_seen = _parse_utc_timestamp(last_seen_at)
        now_datetime = _parse_utc_timestamp(now)
        ip_changed = ip is not None and ip != row["session_last_ip"]
        should_touch = (
            last_seen is None
            or now_datetime is None
            or (now_datetime - last_seen).total_seconds() >= SESSION_TOUCH_INTERVAL_SECONDS
            or ip_changed
        )
        if should_touch:
            await self.writer.execute(
                lambda connection: connection.execute(
                    """
                    UPDATE admin_sessions
                    SET last_seen_at = ?,
                        last_ip = COALESCE(?, last_ip)
                    WHERE id = ? AND revoked_at IS NULL
                    """,
                    (now, ip, row["session_id"]),
                )
            )
            last_seen_at = now

        payload = self._admin_payload(row)
        payload.update(
            {
                "session_id": row["session_id"],
                "admin_id": int(row["admin_id"]),
                "session_created_at": row["session_created_at"],
                "session_expires_at": row["expires_at"],
                "session_last_seen_at": last_seen_at,
                "session_last_ip": ip if ip is not None else row["session_last_ip"],
                "session_user_agent": row["session_user_agent"],
            }
        )
        return payload

    async def revoke_session(self, session_id: str) -> None:
        revoked_at = utc_now()
        await self.writer.execute(
            lambda connection: connection.execute(
                """
                UPDATE admin_sessions
                SET revoked_at = COALESCE(revoked_at, ?)
                WHERE id = ?
                """,
                (revoked_at, session_id),
            )
        )

    async def revoke_admin_sessions(
        self,
        admin_id: int,
        *,
        except_session_id: str | None = None,
        authorization_principal: PermissionContext | None = None,
    ) -> int:
        revoked_at = utc_now()

        def operation(connection: sqlite3.Connection) -> int:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_admin_principal(
                connection,
                authorization_principal,
                required_scopes=("admins.sessions.write",),
            )
            existing = connection.execute(
                "SELECT id, role FROM admins WHERE id = ?",
                (admin_id,),
            ).fetchone()
            if existing is None:
                raise LookupError("admin not found")
            self._assert_role_within_principal(principal, str(existing["role"]))
            return self._revoke_admin_sessions_in_connection(
                connection,
                admin_id,
                revoked_at=revoked_at,
                except_session_id=except_session_id,
            )

        return await self.writer.execute(operation)

    def _transaction_admin_principal(
        self,
        connection: sqlite3.Connection,
        principal: PermissionContext | None,
        *,
        required_scopes: tuple[str, ...],
    ) -> PermissionContext | None:
        """Reload a privileged actor and authorize it in the write transaction."""

        current = principal
        for required_scope in required_scopes:
            current = self.api_keys.transaction_authorization_principal(
                connection,
                current,
                required_scope=required_scope,
                require_global=True,
            )
        if current is None or current.legacy_credential:
            return current

        # Browser permission checks are snapshots too.  Re-load a human
        # administrator's session and role while the same IMMEDIATE
        # transaction owns the subsequent mutation.
        if current.admin_session_id is not None:
            params: list[Any] = [current.admin_session_id, utc_now()]
            admin_match = ""
            if current.admin_id is not None:
                admin_match = " AND a.id = ?"
                params.append(int(current.admin_id))
            row = connection.execute(
                f"""
                SELECT s.id AS session_id, a.id AS admin_id, a.role
                FROM admin_sessions AS s
                JOIN admins AS a ON a.id = s.admin_id
                WHERE s.id = ?
                    AND s.revoked_at IS NULL
                    AND s.expires_at > ?
                    AND a.is_active = 1
                    {admin_match}
                """,
                tuple(params),
            ).fetchone()
            if row is None:
                raise ApiKeyAuthorizationError("acting admin session is no longer active")
            scopes = ROLE_SCOPES.get(str(row["role"]))
            if scopes is None:
                raise ApiKeyAuthorizationError("acting admin role is invalid")
            current = PermissionContext(
                scopes=tuple(sorted(scopes)),
                domain_ids=(),
                mailbox_patterns=(),
                domain_grant_mode="all",
                public_id=str(row["session_id"]),
                name=f"admin:{int(row['admin_id'])}",
                kind="admin",
                admin_id=int(row["admin_id"]),
                admin_session_id=str(row["session_id"]),
                allow_query=True,
            )

        for required_scope in required_scopes:
            if not self._scope_is_granted(current.scopes, required_scope):
                raise ApiKeyAuthorizationError(
                    f"acting principal no longer has required scope: {required_scope}"
                )
        if current.domain_grant_mode != "all":
            raise ApiKeyAuthorizationError("acting principal no longer has an all-domain grant")
        return current

    @staticmethod
    def _scope_is_granted(scopes: tuple[str, ...], required_scope: str) -> bool:
        if required_scope in scopes:
            return True
        return (
            required_scope.endswith(".read")
            and f"{required_scope[:-5]}.write" in scopes
        )

    @classmethod
    def _assert_role_within_principal(
        cls,
        principal: PermissionContext | None,
        role: str,
    ) -> None:
        if principal is None or principal.legacy_credential:
            return
        role_scopes = ROLE_SCOPES.get(role)
        if role_scopes is None:
            raise ValueError("invalid admin role")
        effective_scopes = set(principal.scopes)
        effective_scopes.update(
            f"{scope[:-6]}.read"
            for scope in principal.scopes
            if scope.endswith(".write")
        )
        if not role_scopes.issubset(effective_scopes):
            raise ApiKeyAuthorizationError(
                "admin role exceeds acting principal permissions"
            )

    async def close(self) -> None:
        with self._password_executor_lock:
            executor = self._password_executor
            if executor is not None:
                self._password_executor = None

        if executor is None:
            await asyncio.to_thread(self._password_executor_closed.wait)
            return

        def shutdown() -> None:
            try:
                executor.shutdown(wait=True, cancel_futures=False)
            finally:
                self._password_executor_closed.set()

        await asyncio.shield(asyncio.to_thread(shutdown))

    async def _run_password_work(self, operation, /, *args):
        with self._password_waiter_lock:
            if self._password_waiters >= self._password_waiter_limit:
                raise AuthenticationOverloadedError("authentication work queue is full")
            self._password_waiters += 1
        try:
            await self._password_slots.acquire()
        finally:
            with self._password_waiter_lock:
                self._password_waiters -= 1
        with self._password_executor_lock:
            executor = self._password_executor
        if executor is None:
            self._password_slots.release()
            raise RuntimeError("authentication service is closed")

        loop = asyncio.get_running_loop()
        try:
            future = loop.run_in_executor(executor, partial(operation, *args))
        except BaseException:
            self._password_slots.release()
            raise
        future.add_done_callback(lambda _future: self._password_slots.release())
        return await asyncio.shield(future)

    def _revoke_admin_sessions_in_connection(
        self,
        connection: sqlite3.Connection,
        admin_id: int,
        *,
        revoked_at: str,
        except_session_id: str | None = None,
    ) -> int:
        if except_session_id is None:
            cursor = connection.execute(
                """
                UPDATE admin_sessions
                SET revoked_at = COALESCE(revoked_at, ?)
                WHERE admin_id = ? AND revoked_at IS NULL
                """,
                (revoked_at, admin_id),
            )
        else:
            cursor = connection.execute(
                """
                UPDATE admin_sessions
                SET revoked_at = COALESCE(revoked_at, ?)
                WHERE admin_id = ? AND id <> ? AND revoked_at IS NULL
                """,
                (revoked_at, admin_id, except_session_id),
            )
        return int(cursor.rowcount)

    def _ensure_active_superadmin_remains(
        self,
        connection: sqlite3.Connection,
        current_admin: sqlite3.Row,
        *,
        next_role: str,
        next_is_active: bool,
        deleting: bool = False,
    ) -> None:
        if str(current_admin["role"]) != "superadmin" or not bool(current_admin["is_active"]):
            return
        if not deleting and next_role == "superadmin" and next_is_active:
            return
        row = connection.execute(
            """
            SELECT COUNT(*) AS count
            FROM admins
            WHERE role = 'superadmin' AND is_active = 1
            """
        ).fetchone()
        if row is None or int(row["count"]) <= 1:
            raise ValueError("cannot remove last active superadmin")

    def _normalize_username(self, value: object) -> str:
        username = str(value).strip()
        if not username or len(username) > 128:
            raise ValueError("invalid username")
        if any(ord(character) < 32 for character in username):
            raise ValueError("invalid username")
        return username

    def _normalize_display_name(self, value: object) -> str | None:
        if value is None:
            return None
        display_name = str(value).strip()
        if not display_name:
            return None
        if len(display_name) > 200 or any(ord(character) < 32 for character in display_name):
            raise ValueError("invalid display name")
        return display_name

    def _normalize_role(self, value: object) -> str:
        role = str(value).strip()
        if role not in VALID_ADMIN_ROLES:
            raise ValueError("invalid admin role")
        return role

    def _coerce_bool(self, field_name: str, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
            return bool(value)
        raise ValueError(f"invalid {field_name}")

    def _validate_new_password(self, password: object) -> str:
        if not isinstance(password, str) or len(password) < MIN_ADMIN_PASSWORD_LENGTH:
            raise ValueError(f"password must be at least {MIN_ADMIN_PASSWORD_LENGTH} characters")
        if len(password) > MAX_ADMIN_PASSWORD_LENGTH:
            raise ValueError(f"password must be at most {MAX_ADMIN_PASSWORD_LENGTH} characters")
        if password == DEFAULT_BOOTSTRAP_ADMIN_PASSWORD:
            raise ValueError("password must not use the default bootstrap value")
        return password

    def _admin_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "username": row["username"],
            "display_name": row["display_name"],
            "role": row["role"],
            "is_active": bool(row["is_active"]),
            "must_change_password": bool(row["must_change_password"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_login_at": row["last_login_at"],
            "last_login_ip": row["last_login_ip"],
        }
