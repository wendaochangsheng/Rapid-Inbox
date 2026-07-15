from __future__ import annotations

import asyncio
import ipaddress
import json
import hashlib
import hmac
import secrets
import sqlite3
import threading
import uuid
from collections import OrderedDict
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Sequence

from fastapi import HTTPException

from app.config import MAX_RATE_LIMIT_COUNT
from app.db.connection import connect_database
from app.db.writer import DatabaseWriter
from app.ingest.storage import utc_now

from .permissions import (
    PermissionContext,
    ROLE_SCOPES,
    VALID_DOMAIN_GRANT_MODES,
    api_key_is_within_principal,
    validate_scopes_for_kind,
)


VALID_API_KEY_KINDS = {"admin", "public", "service"}
VALID_API_KEY_STATUSES = {"active", "revoked", "expired", "disabled"}
_UNSET = object()
_ACTIVE_PERMISSION_CONTEXT: ContextVar[PermissionContext | None] = ContextVar(
    "active_permission_context",
    default=None,
)

AUTH_CACHE_TTL_SECONDS = 2.0
AUTH_CACHE_MAX_ENTRIES = 4096
USAGE_PERSIST_INTERVAL_SECONDS = 30.0
USAGE_STATE_MAX_ENTRIES = 65_536
MAX_MAILBOX_PATTERNS = 100
MAX_ALLOWED_IP_CIDRS = 100


class ApiKeyAuthorizationError(PermissionError):
    """The acting principal no longer contains the requested key policy."""


@dataclass(frozen=True, slots=True)
class _AuthenticationRecord:
    context: PermissionContext
    secret_hash: str
    status: str
    allow_header: bool
    allow_query: bool
    rate_limit_per_min: int
    allowed_ip_cidrs: str | None
    expires_at: str | None

def make_api_key(kind: str) -> tuple[str, str, str]:
    prefix = secrets.token_hex(4)
    secret = secrets.token_urlsafe(24)
    plain_text = f"ri_{kind}_{prefix}_{secret}"
    secret_hash = hashlib.sha256(secret.encode("utf-8")).hexdigest()
    return prefix, plain_text, secret_hash


def get_active_permission_context() -> PermissionContext | None:
    return _ACTIVE_PERMISSION_CONTEXT.get()


def set_active_permission_context(context: PermissionContext | None) -> None:
    _ACTIVE_PERMISSION_CONTEXT.set(context)


class PublicAPIKeyProxy(str):
    def __new__(cls, legacy_value: str, service: "ApiKeyService") -> "PublicAPIKeyProxy":
        proxy = str.__new__(cls, legacy_value)
        proxy._service = service
        return proxy

    def __ne__(self, other: object) -> bool:
        return self._service.compare_public_api_key(other)


class ApiKeyService:
    def __init__(self, database_path: Path, writer: DatabaseWriter) -> None:
        self.database_path = database_path
        self.writer = writer
        self._legacy_public_api_key: str | None = None
        self._legacy_public_context: PermissionContext | None = None
        self._usage_lock = threading.Lock()
        self._usage_buckets: OrderedDict[int, tuple[float, float]] = OrderedDict()
        self._last_usage_persisted: OrderedDict[int, float] = OrderedDict()
        self._auth_cache_lock = threading.Lock()
        self._auth_cache: OrderedDict[
            tuple[str, str],
            tuple[float, _AuthenticationRecord],
        ] = OrderedDict()
        # Mutations advance this epoch after commit. A concurrent cold loader
        # may still finish the request that linearized before the mutation, but
        # it must never repopulate either cache with the stale pre-commit row.
        self._auth_cache_epoch = 0
        self._usage_policy_cache: OrderedDict[
            int,
            tuple[float, int, str | None, str],
        ] = OrderedDict()

    def configure_legacy_public_api_key(self, legacy_token: str, *, enabled: bool = True) -> PublicAPIKeyProxy:
        self._legacy_public_api_key = legacy_token if enabled else None
        self._legacy_public_context = (
            PermissionContext(
                scopes=("public.read",),
                domain_ids=(),
                mailbox_patterns=(),
                domain_grant_mode="all",
                public_id="legacy-public-token",
                name="legacy-public-token",
                kind="public",
                legacy_credential=True,
            )
            if enabled
            else None
        )
        return PublicAPIKeyProxy(legacy_token, self)

    async def create_key(
        self,
        *,
        name: str,
        kind: str,
        scopes: Sequence[str],
        domain_ids: Sequence[int],
        mailbox_patterns: Sequence[str],
        domain_grant_mode: str | None = None,
        description: str | None = None,
        owner_admin_id: int | None = None,
        created_by_admin_id: int | None = None,
        status: str = "active",
        allow_header: bool = True,
        allow_query: bool = False,
        rate_limit_per_min: int = 3600,
        allowed_ip_cidrs: Sequence[str] | None = None,
        expires_at: str | None = None,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        if kind not in VALID_API_KEY_KINDS:
            raise ValueError("invalid api key kind")
        if status not in VALID_API_KEY_STATUSES:
            raise ValueError("invalid api key status")

        normalized_name = self._normalize_name(name)
        normalized_description = self._normalize_description(description)
        scope_values = validate_scopes_for_kind(kind, list(scopes))
        domain_values = self._unique_int_values(domain_ids)
        if domain_grant_mode is None:
            normalized_domain_grant_mode = "selected" if domain_values else "none"
        else:
            normalized_domain_grant_mode = self._normalize_domain_grant_mode(domain_grant_mode)
        self._validate_domain_grants(normalized_domain_grant_mode, domain_values)
        mailbox_values = self._normalize_mailbox_patterns(mailbox_patterns)
        allowed_ip_values = self._normalize_ip_cidrs(allowed_ip_cidrs or ())
        allowed_ip_cidrs_json = json.dumps(list(allowed_ip_values), ensure_ascii=False) if allowed_ip_values else None
        normalized_rate_limit = self._coerce_non_negative_int(
            "rate_limit_per_min",
            rate_limit_per_min,
        )
        normalized_allow_header = self._coerce_bool("allow_header", allow_header)
        normalized_allow_query = self._coerce_bool("allow_query", allow_query)
        normalized_expires_at = self._normalize_expiration(expires_at)
        key_prefix, plain_text, secret_hash = make_api_key(kind)
        public_id = f"ak_{uuid.uuid4().hex}"
        created_at = utc_now()
        requested_policy = {
            "kind": kind,
            "status": status,
            "allow_header": normalized_allow_header,
            "allow_query": normalized_allow_query,
            "rate_limit_per_min": normalized_rate_limit,
            "allowed_ip_cidrs": list(allowed_ip_values),
            "expires_at": normalized_expires_at,
            "scopes": list(scope_values),
            "domain_ids": list(domain_values),
            "mailbox_patterns": list(mailbox_values),
            "domain_grant_mode": normalized_domain_grant_mode,
        }

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_management_principal(
                connection,
                authorization_principal,
            )
            self._assert_key_within_principal(principal, requested_policy)
            cursor = connection.execute(
                """
                INSERT INTO api_keys (
                    public_id,
                    name,
                    description,
                    kind,
                    key_prefix,
                    secret_hash,
                    owner_admin_id,
                    status,
                    domain_grant_mode,
                    allow_header,
                    allow_query,
                    rate_limit_per_min,
                    allowed_ip_cidrs,
                    expires_at,
                    created_by_admin_id,
                    created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    public_id,
                    normalized_name,
                    normalized_description,
                    kind,
                    key_prefix,
                    secret_hash,
                    owner_admin_id,
                    status,
                    normalized_domain_grant_mode,
                    int(normalized_allow_header),
                    int(normalized_allow_query),
                    normalized_rate_limit,
                    allowed_ip_cidrs_json,
                    normalized_expires_at,
                    created_by_admin_id,
                    created_at,
                ),
            )
            api_key_id = int(cursor.lastrowid)

            for scope in scope_values:
                connection.execute(
                    "INSERT INTO api_key_scopes (api_key_id, scope) VALUES (?, ?)",
                    (api_key_id, scope),
                )
            for domain_id in domain_values:
                connection.execute(
                    "INSERT INTO api_key_domain_grants (api_key_id, domain_id) VALUES (?, ?)",
                    (api_key_id, domain_id),
                )
            for mailbox_pattern in mailbox_values:
                connection.execute(
                    "INSERT INTO api_key_mailbox_grants (api_key_id, mailbox_pattern) VALUES (?, ?)",
                    (api_key_id, mailbox_pattern),
                )

            return {
                "id": api_key_id,
                "public_id": public_id,
                "name": normalized_name,
                "description": normalized_description,
                "kind": kind,
                "status": status,
                "domain_grant_mode": normalized_domain_grant_mode,
                "key_prefix": key_prefix,
                "plain_text": plain_text,
                "scopes": list(scope_values),
                "domain_ids": list(domain_values),
                "mailbox_patterns": list(mailbox_values),
                "owner_admin_id": owner_admin_id,
                "created_by_admin_id": created_by_admin_id,
                "allow_header": normalized_allow_header,
                "allow_query": normalized_allow_query,
                "rate_limit_per_min": normalized_rate_limit,
                "allowed_ip_cidrs": list(allowed_ip_values),
                "expires_at": normalized_expires_at,
                "created_at": created_at,
            }

        return await self.writer.execute(operation)

    def get_key(self, api_key_id: int) -> dict[str, Any]:
        with connect_database(self.database_path) as connection:
            api_key = self._load_key(connection, api_key_id)
        if api_key is None:
            raise LookupError("api key not found")
        return api_key

    def transaction_authorization_principal(
        self,
        connection: sqlite3.Connection,
        principal: PermissionContext | None,
        *,
        required_scope: str,
        require_global: bool = False,
        domain_id: int | None = None,
    ) -> PermissionContext | None:
        """Reload and authorize a mutation actor in the caller's transaction.

        Request-time permission contexts are only snapshots.  Mutating services
        call this helper from their writer transaction so a revoked or narrowed
        API key cannot win a check/write race. Human administrator contexts
        carrying a session ID are reloaded through the same connection.
        """
        if principal is None:
            return None
        if principal.legacy_credential:
            # Legacy administrator credentials intentionally retain their
            # historical unrestricted compatibility semantics.
            return principal
        current = principal
        if principal.api_key_id is not None:
            actor = self._load_key(connection, int(principal.api_key_id))
            if (
                actor is None
                or str(actor["status"]) != "active"
                or self._expiration_is_due(actor.get("expires_at"))
            ):
                raise ApiKeyAuthorizationError("acting api key is no longer active")
            try:
                current = PermissionContext(
                    scopes=validate_scopes_for_kind(
                        str(actor["kind"]),
                        list(actor.get("scopes") or ()),
                    ),
                    domain_ids=tuple(int(value) for value in actor.get("domain_ids") or ()),
                    mailbox_patterns=self._normalize_mailbox_patterns(
                        list(actor.get("mailbox_patterns") or ())
                    ),
                    domain_grant_mode=str(actor.get("domain_grant_mode") or "none"),
                    api_key_id=int(actor["id"]),
                    public_id=str(actor["public_id"]),
                    name=str(actor["name"]),
                    kind=str(actor["kind"]),
                    rate_limit_per_min=int(actor["rate_limit_per_min"]),
                    allowed_ip_cidrs=self._normalize_ip_cidrs(
                        list(actor.get("allowed_ip_cidrs") or ())
                    ),
                    expires_at=(
                        None
                        if actor.get("expires_at") is None
                        else str(actor["expires_at"])
                    ),
                    allow_header=bool(actor["allow_header"]),
                    allow_query=bool(actor["allow_query"]),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise ApiKeyAuthorizationError("acting api key policy is invalid") from exc
        elif principal.admin_session_id is not None:
            params: list[Any] = [principal.admin_session_id, utc_now()]
            admin_match = ""
            if principal.admin_id is not None:
                admin_match = " AND a.id = ?"
                params.append(int(principal.admin_id))
            actor = connection.execute(
                f"""
                SELECT
                    s.id AS session_id,
                    a.id AS admin_id,
                    a.username,
                    a.display_name,
                    a.role
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
            if actor is None:
                raise ApiKeyAuthorizationError("acting admin session is no longer active")
            role_scopes = ROLE_SCOPES.get(str(actor["role"]))
            if role_scopes is None:
                raise ApiKeyAuthorizationError("acting admin role is invalid")
            current = PermissionContext(
                scopes=tuple(sorted(role_scopes)),
                domain_ids=(),
                mailbox_patterns=(),
                domain_grant_mode="all",
                public_id=str(actor["session_id"]),
                name=str(actor["display_name"] or actor["username"]),
                kind="admin",
                allow_query=True,
                admin_id=int(actor["admin_id"]),
                admin_session_id=str(actor["session_id"]),
            )

        has_scope = required_scope in current.scopes
        if required_scope.endswith(".read"):
            has_scope = has_scope or f"{required_scope[:-5]}.write" in current.scopes
        if not has_scope:
            raise ApiKeyAuthorizationError(
                f"acting principal no longer has required scope: {required_scope}"
            )

        if require_global and current.domain_grant_mode != "all":
            raise ApiKeyAuthorizationError("acting principal no longer has an all-domain grant")

        if domain_id is not None:
            normalized_domain_id = int(domain_id)
            if current.domain_grant_mode == "all":
                pass
            elif (
                current.domain_grant_mode == "selected"
                and normalized_domain_id in current.domain_ids
            ):
                pass
            else:
                raise ApiKeyAuthorizationError(
                    "acting principal no longer has the target domain grant"
                )
        return current

    def _transaction_management_principal(
        self,
        connection: sqlite3.Connection,
        principal: PermissionContext | None,
    ) -> PermissionContext | None:
        return self.transaction_authorization_principal(
            connection,
            principal,
            required_scope="api_keys.write",
            require_global=True,
        )

    def _assert_key_within_principal(
        self,
        principal: PermissionContext | None,
        target: dict[str, Any],
    ) -> None:
        if principal is not None and not api_key_is_within_principal(principal, target):
            raise ApiKeyAuthorizationError("api key policy exceeds acting principal")

    def list_keys(self, *, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        with connect_database(self.database_path) as connection:
            rows = connection.execute(
                """
                SELECT id
                FROM api_keys
                ORDER BY created_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            total = connection.execute("SELECT COUNT(*) AS count FROM api_keys").fetchone()
            items = [
                api_key
                for row in rows
                if (api_key := self._load_key(connection, int(row["id"]))) is not None
            ]
        return {"items": items, "total_count": 0 if total is None else int(total["count"])}

    async def update_key(
        self,
        api_key_id: int,
        *,
        name: object = _UNSET,
        description: object = _UNSET,
        kind: object = _UNSET,
        status: object = _UNSET,
        allow_header: object = _UNSET,
        allow_query: object = _UNSET,
        rate_limit_per_min: object = _UNSET,
        allowed_ip_cidrs: object = _UNSET,
        expires_at: object = _UNSET,
        scopes: object = _UNSET,
        domain_ids: object = _UNSET,
        mailbox_patterns: object = _UNSET,
        domain_grant_mode: object = _UNSET,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        field_updates: dict[str, Any] = {}

        if name is not _UNSET:
            field_updates["name"] = self._normalize_name(name)
        if description is not _UNSET:
            field_updates["description"] = self._normalize_description(description)
        expected_kind: str | None = None
        if kind is not _UNSET:
            normalized_kind = str(kind).strip()
            if normalized_kind not in VALID_API_KEY_KINDS:
                raise ValueError("invalid api key kind")
            expected_kind = normalized_kind
        if status is not _UNSET:
            normalized_status = str(status).strip()
            if normalized_status not in VALID_API_KEY_STATUSES:
                raise ValueError("invalid api key status")
            field_updates["status"] = normalized_status
        if allow_header is not _UNSET:
            field_updates["allow_header"] = int(self._coerce_bool("allow_header", allow_header))
        if allow_query is not _UNSET:
            field_updates["allow_query"] = int(self._coerce_bool("allow_query", allow_query))
        if rate_limit_per_min is not _UNSET:
            field_updates["rate_limit_per_min"] = self._coerce_non_negative_int(
                "rate_limit_per_min",
                rate_limit_per_min,
            )
        if allowed_ip_cidrs is not _UNSET:
            allowed_ip_values = self._normalize_ip_cidrs(allowed_ip_cidrs or ())
            field_updates["allowed_ip_cidrs"] = (
                json.dumps(list(allowed_ip_values), ensure_ascii=False) if allowed_ip_values else None
            )
        if expires_at is not _UNSET:
            field_updates["expires_at"] = self._normalize_expiration(expires_at)

        raw_scope_values = list(scopes) if scopes is not _UNSET else None
        domain_values = self._unique_int_values(domain_ids) if domain_ids is not _UNSET else None
        mailbox_values = self._normalize_mailbox_patterns(mailbox_patterns) if mailbox_patterns is not _UNSET else None
        normalized_domain_grant_mode: str | None = None
        if domain_grant_mode is not _UNSET:
            normalized_domain_grant_mode = self._normalize_domain_grant_mode(domain_grant_mode)
        elif domain_values is not None:
            # Supplying a domain set is an explicit switch to selected mode.
            # An empty set remains selected-and-denied rather than widening to all.
            normalized_domain_grant_mode = "selected"
        if normalized_domain_grant_mode is not None and domain_values is not None:
            self._validate_domain_grants(normalized_domain_grant_mode, domain_values)
        if normalized_domain_grant_mode is not None:
            field_updates["domain_grant_mode"] = normalized_domain_grant_mode
        now = utc_now()

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_management_principal(
                connection,
                authorization_principal,
            )
            current = self._load_key(connection, api_key_id)
            if current is None:
                raise LookupError("api key not found")
            self._assert_key_within_principal(principal, current)
            if expected_kind is not None and current["kind"] != expected_kind:
                raise ValueError("api key kind cannot be changed")

            scope_values = (
                validate_scopes_for_kind(str(current["kind"]), raw_scope_values)
                if raw_scope_values is not None
                else None
            )
            effective_domain_grant_mode = normalized_domain_grant_mode or str(current["domain_grant_mode"])
            if domain_values is not None:
                self._validate_domain_grants(effective_domain_grant_mode, domain_values)

            if field_updates:
                assignments = [f"{column} = ?" for column in field_updates]
                params = list(field_updates.values())
                if field_updates.get("status") == "revoked":
                    assignments.append("revoked_at = COALESCE(revoked_at, ?)")
                    params.append(now)
                elif "status" in field_updates:
                    assignments.append("revoked_at = NULL")
                params.append(api_key_id)
                connection.execute(
                    f"""
                    UPDATE api_keys
                    SET {', '.join(assignments)}
                    WHERE id = ?
                    """,
                    params,
                )

            if scope_values is not None:
                connection.execute("DELETE FROM api_key_scopes WHERE api_key_id = ?", (api_key_id,))
                for scope in scope_values:
                    connection.execute(
                        "INSERT INTO api_key_scopes (api_key_id, scope) VALUES (?, ?)",
                        (api_key_id, scope),
                    )
            if domain_values is not None or normalized_domain_grant_mode in {"none", "all"}:
                connection.execute("DELETE FROM api_key_domain_grants WHERE api_key_id = ?", (api_key_id,))
                for domain_id in domain_values or ():
                    connection.execute(
                        "INSERT INTO api_key_domain_grants (api_key_id, domain_id) VALUES (?, ?)",
                        (api_key_id, domain_id),
                    )
            if mailbox_values is not None:
                connection.execute("DELETE FROM api_key_mailbox_grants WHERE api_key_id = ?", (api_key_id,))
                for mailbox_pattern in mailbox_values:
                    connection.execute(
                        "INSERT INTO api_key_mailbox_grants (api_key_id, mailbox_pattern) VALUES (?, ?)",
                        (api_key_id, mailbox_pattern),
                    )

            api_key = self._load_key(connection, api_key_id)
            if api_key is None:
                raise LookupError("api key not found")
            self._assert_key_within_principal(principal, api_key)
            return api_key

        updated = await self.writer.execute(operation)
        self._invalidate_key_cache(api_key_id)
        return updated

    async def delete_key(
        self,
        api_key_id: int,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_management_principal(
                connection,
                authorization_principal,
            )
            target = self._load_key(connection, api_key_id)
            if target is None:
                raise LookupError("api key not found")
            self._assert_key_within_principal(principal, target)
            if str(target["status"]) != "revoked":
                raise ValueError("api key must be revoked before deletion")
            connection.execute("DELETE FROM api_keys WHERE id = ?", (api_key_id,))
            return {"id": int(target["id"]), "deleted": True}

        deleted = await self.writer.execute(operation)
        self._invalidate_key_cache(api_key_id)
        return deleted

    async def revoke_key(
        self,
        api_key_id: int,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        revoked_at = utc_now()

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_management_principal(
                connection,
                authorization_principal,
            )
            target = self._load_key(connection, api_key_id)
            if target is None:
                raise LookupError("api key not found")
            self._assert_key_within_principal(principal, target)

            connection.execute(
                """
                UPDATE api_keys
                SET status = 'revoked',
                    revoked_at = COALESCE(revoked_at, ?)
                WHERE id = ?
                """,
                (revoked_at, api_key_id),
            )
            return {
                "id": int(target["id"]),
                "status": "revoked",
                "revoked_at": str(target["revoked_at"] or revoked_at),
            }

        revoked = await self.writer.execute(operation)
        self._invalidate_key_cache(api_key_id)
        return revoked

    async def rotate_key(
        self,
        api_key_id: int,
        *,
        authorization_principal: PermissionContext | None = None,
    ) -> dict[str, Any]:
        now = utc_now()

        def operation(connection: sqlite3.Connection) -> dict[str, Any]:
            connection.execute("BEGIN IMMEDIATE")
            principal = self._transaction_management_principal(
                connection,
                authorization_principal,
            )
            target = self._load_key(connection, api_key_id)
            if target is None:
                raise LookupError("api key not found")
            self._assert_key_within_principal(principal, target)
            if str(target["status"]) != "active":
                raise ValueError("only active api keys can be rotated")
            if self._expiration_is_due(target["expires_at"]):
                raise ValueError("expired api keys cannot be rotated")
            key_prefix, plain_text, secret_hash = make_api_key(str(target["kind"]))
            connection.execute(
                """
                UPDATE api_keys
                SET key_prefix = ?,
                    secret_hash = ?
                WHERE id = ?
                """,
                (key_prefix, secret_hash, api_key_id),
            )
            api_key = self._load_key(connection, api_key_id)
            if api_key is None:
                raise LookupError("api key not found")
            api_key["plain_text"] = plain_text
            api_key["rotated_at"] = now
            return api_key

        rotated = await self.writer.execute(operation)
        self._invalidate_key_cache(api_key_id)
        return rotated

    def authenticate_plain_text(self, plain_text: str, *, request_ip: str | None = None) -> PermissionContext:
        return self._authenticate_plain_text(plain_text, transport="header", request_ip=request_ip)

    def authenticate_plain_text_cached(
        self,
        plain_text: str,
        *,
        request_ip: str | None = None,
    ) -> PermissionContext | None:
        """Authenticate from the bounded hot cache without database I/O.

        API v2 calls this on the event-loop thread. The work is limited to
        parsing, one short lock hold, SHA-256 and bounded policy checks. A miss
        returns ``None`` so the caller can move SQLite work to a worker thread.
        """

        kind, key_prefix, secret = self._parse_plain_text(plain_text)
        record = self._cached_authentication_record(kind, key_prefix)
        if record is None:
            return None
        return self._validate_authentication_record(
            record,
            secret,
            transport="header",
            request_ip=request_ip,
        )

    def authenticate_query(self, plain_text: str, *, request_ip: str | None = None) -> PermissionContext:
        return self._authenticate_plain_text(plain_text, transport="query", request_ip=request_ip)

    def authenticate_public_credential(
        self,
        plain_text: str,
        *,
        transport: str,
        request_ip: str | None = None,
    ) -> PermissionContext:
        if (
            self._legacy_public_api_key is not None
            and self._legacy_public_context is not None
            and hmac.compare_digest(plain_text, self._legacy_public_api_key)
        ):
            return self._legacy_public_context
        return self._authenticate_plain_text(plain_text, transport=transport, request_ip=request_ip)

    async def record_usage(self, context: PermissionContext, *, ip: str | None = None) -> None:
        if context.api_key_id is None:
            return

        api_key_id = int(context.api_key_id)
        policy = self._cached_usage_policy(api_key_id)
        if policy is None:
            with self._auth_cache_lock:
                load_epoch = self._auth_cache_epoch
            loaded_policy = await asyncio.to_thread(self._load_usage_policy, api_key_id)
            if loaded_policy is None or loaded_policy[2] != "active":
                raise HTTPException(status_code=401, detail="invalid api key")
            rate_limit_per_min, allowed_ip_cidrs, status_value = loaded_policy
            self._store_usage_policy(
                api_key_id,
                rate_limit_per_min,
                allowed_ip_cidrs,
                status_value,
                expected_epoch=load_epoch,
            )
        else:
            rate_limit_per_min, allowed_ip_cidrs, status_value = policy

        if status_value != "active":
            raise HTTPException(status_code=401, detail="invalid api key")
        if not self._request_ip_allowed(ip, allowed_ip_cidrs):
            raise HTTPException(status_code=403, detail="api key ip not allowed")

        now_monotonic = monotonic()
        should_persist = False
        with self._usage_lock:
            if rate_limit_per_min > 0:
                capacity = float(rate_limit_per_min)
                bucket = self._usage_buckets.get(api_key_id)
                if bucket is None:
                    tokens = capacity
                    last_refill = now_monotonic
                else:
                    previous_tokens, last_refill = bucket
                    elapsed = max(now_monotonic - last_refill, 0.0)
                    tokens = min(
                        capacity,
                        previous_tokens + elapsed * (capacity / 60.0),
                    )
                if tokens < 1.0:
                    self._usage_buckets[api_key_id] = (tokens, now_monotonic)
                    self._usage_buckets.move_to_end(api_key_id)
                    raise HTTPException(status_code=429, detail="api key rate limit exceeded")
                self._usage_buckets[api_key_id] = (tokens - 1.0, now_monotonic)
                self._usage_buckets.move_to_end(api_key_id)
                while len(self._usage_buckets) > USAGE_STATE_MAX_ENTRIES:
                    self._usage_buckets.popitem(last=False)
            else:
                self._usage_buckets.pop(api_key_id, None)

            last_persisted = self._last_usage_persisted.get(api_key_id)
            if (
                last_persisted is None
                or now_monotonic - last_persisted >= USAGE_PERSIST_INTERVAL_SECONDS
            ):
                self._last_usage_persisted[api_key_id] = now_monotonic
                self._last_usage_persisted.move_to_end(api_key_id)
                while len(self._last_usage_persisted) > USAGE_STATE_MAX_ENTRIES:
                    self._last_usage_persisted.popitem(last=False)
                should_persist = True

        if not should_persist:
            return

        now = utc_now()
        try:
            await self.writer.execute(
                lambda connection: connection.execute(
                    """
                    UPDATE api_keys
                    SET last_used_at = ?,
                        last_used_ip = COALESCE(?, last_used_ip)
                    WHERE id = ? AND status = 'active'
                    """,
                    (now, ip, api_key_id),
                )
            )
        except Exception:
            with self._usage_lock:
                if self._last_usage_persisted.get(api_key_id) == now_monotonic:
                    self._last_usage_persisted.pop(api_key_id, None)
            raise

    def _load_usage_policy(self, api_key_id: int) -> tuple[int, str | None, str] | None:
        with connect_database(self.database_path) as connection:
            row = connection.execute(
                """
                SELECT rate_limit_per_min, allowed_ip_cidrs, status
                FROM api_keys
                WHERE id = ?
                """,
                (api_key_id,),
            ).fetchone()
        if row is None:
            return None
        return (
            int(row["rate_limit_per_min"]),
            row["allowed_ip_cidrs"],
            str(row["status"]),
        )

    def _authenticate_plain_text(
        self,
        plain_text: str,
        *,
        transport: str,
        request_ip: str | None = None,
    ) -> PermissionContext:
        kind, key_prefix, secret = self._parse_plain_text(plain_text)
        record = self._get_authentication_record(kind, key_prefix)
        return self._validate_authentication_record(
            record,
            secret,
            transport=transport,
            request_ip=request_ip,
        )

    def _validate_authentication_record(
        self,
        record: _AuthenticationRecord,
        secret: str,
        *,
        transport: str,
        request_ip: str | None,
    ) -> PermissionContext:
        candidate_hash = hashlib.sha256(secret.encode("utf-8")).hexdigest()
        if not hmac.compare_digest(candidate_hash, record.secret_hash):
            raise LookupError("invalid api key")
        if record.status != "active":
            raise LookupError("inactive api key")
        if transport == "header":
            if not record.allow_header:
                raise LookupError("header access disabled")
        elif transport == "query":
            if not record.allow_query:
                raise LookupError("query access disabled")
        else:
            raise ValueError("invalid api key transport")
        if self._expiration_is_due(record.expires_at):
            raise LookupError("expired api key")
        if request_ip is not None and not self._request_ip_allowed(
            request_ip,
            record.allowed_ip_cidrs,
        ):
            raise LookupError("api key ip not allowed")
        return record.context

    def _get_authentication_record(self, kind: str, key_prefix: str) -> _AuthenticationRecord:
        cached = self._cached_authentication_record(kind, key_prefix)
        if cached is not None:
            return cached

        cache_key = (kind, key_prefix)
        now = monotonic()
        with self._auth_cache_lock:
            load_epoch = self._auth_cache_epoch
        record = self._load_authentication_record(kind, key_prefix)
        cache_expires_at = now + AUTH_CACHE_TTL_SECONDS
        with self._auth_cache_lock:
            if load_epoch != self._auth_cache_epoch:
                return record
            # Selected-domain grants can change through a domain FK cascade, which
            # happens outside ApiKeyService. Keep those credentials uncached so a
            # deleted grant is fail-closed immediately; all/none keys use the hot cache.
            if record.context.domain_grant_mode != "selected":
                self._auth_cache[cache_key] = (cache_expires_at, record)
                self._auth_cache.move_to_end(cache_key)
                while len(self._auth_cache) > AUTH_CACHE_MAX_ENTRIES:
                    self._auth_cache.popitem(last=False)
            for api_key_id, policy in list(self._usage_policy_cache.items()):
                if policy[0] <= now:
                    self._usage_policy_cache.pop(api_key_id, None)
            if record.context.api_key_id is not None:
                api_key_id = int(record.context.api_key_id)
                self._usage_policy_cache[api_key_id] = (
                    cache_expires_at,
                    record.rate_limit_per_min,
                    record.allowed_ip_cidrs,
                    record.status,
                )
                self._usage_policy_cache.move_to_end(api_key_id)
                while len(self._usage_policy_cache) > AUTH_CACHE_MAX_ENTRIES:
                    self._usage_policy_cache.popitem(last=False)
        return record

    def _cached_authentication_record(
        self,
        kind: str,
        key_prefix: str,
    ) -> _AuthenticationRecord | None:
        cache_key = (kind, key_prefix)
        now = monotonic()
        with self._auth_cache_lock:
            cached = self._auth_cache.get(cache_key)
            if cached is None:
                return None
            expires_at, record = cached
            if expires_at > now:
                self._auth_cache.move_to_end(cache_key)
                return record
            self._auth_cache.pop(cache_key, None)
        return None

    def _load_authentication_record(self, kind: str, key_prefix: str) -> _AuthenticationRecord:
        with connect_database(self.database_path) as connection:
            row = connection.execute(
                """
                SELECT
                    id,
                    public_id,
                    name,
                    kind,
                    secret_hash,
                    status,
                    domain_grant_mode,
                    allow_header,
                    allow_query,
                    rate_limit_per_min,
                    allowed_ip_cidrs,
                    expires_at
                FROM api_keys
                WHERE key_prefix = ?
                """,
                (key_prefix,),
            ).fetchone()
            if row is None or str(row["kind"]) != kind:
                raise LookupError("invalid api key")

            scope_rows = connection.execute(
                """
                SELECT scope
                FROM api_key_scopes
                WHERE api_key_id = ?
                ORDER BY scope ASC
                """,
                (row["id"],),
            ).fetchall()
            domain_rows = connection.execute(
                """
                SELECT domain_id
                FROM api_key_domain_grants
                WHERE api_key_id = ?
                ORDER BY domain_id ASC
                """,
                (row["id"],),
            ).fetchall()
            mailbox_rows = connection.execute(
                """
                SELECT mailbox_pattern
                FROM api_key_mailbox_grants
                WHERE api_key_id = ?
                ORDER BY mailbox_pattern ASC
                """,
                (row["id"],),
            ).fetchall()

        try:
            authenticated_scopes = validate_scopes_for_kind(
                str(row["kind"]),
                [str(scope_row["scope"]) for scope_row in scope_rows],
            )
        except ValueError as exc:
            raise LookupError("invalid api key scope configuration") from exc

        try:
            mailbox_patterns = self._normalize_mailbox_patterns(
                [str(mailbox_row["mailbox_pattern"]) for mailbox_row in mailbox_rows]
            )
        except ValueError as exc:
            raise LookupError("invalid api key mailbox grant configuration") from exc
        try:
            allowed_ip_cidrs = self._normalize_ip_cidrs(
                self._decode_allowed_ip_cidrs(row["allowed_ip_cidrs"])
            )
        except ValueError as exc:
            raise LookupError("invalid api key IP policy configuration") from exc

        context = PermissionContext(
            scopes=authenticated_scopes,
            domain_ids=tuple(int(domain_row["domain_id"]) for domain_row in domain_rows),
            mailbox_patterns=mailbox_patterns,
            domain_grant_mode=str(row["domain_grant_mode"]),
            api_key_id=int(row["id"]),
            public_id=str(row["public_id"]),
            name=str(row["name"]),
            kind=str(row["kind"]),
            rate_limit_per_min=int(row["rate_limit_per_min"]),
            allowed_ip_cidrs=allowed_ip_cidrs,
            expires_at=None if row["expires_at"] is None else str(row["expires_at"]),
            allow_header=bool(row["allow_header"]),
            allow_query=bool(row["allow_query"]),
        )
        return _AuthenticationRecord(
            context=context,
            secret_hash=str(row["secret_hash"]),
            status=str(row["status"]),
            allow_header=bool(row["allow_header"]),
            allow_query=bool(row["allow_query"]),
            rate_limit_per_min=int(row["rate_limit_per_min"]),
            allowed_ip_cidrs=row["allowed_ip_cidrs"],
            expires_at=None if row["expires_at"] is None else str(row["expires_at"]),
        )

    def _cached_usage_policy(self, api_key_id: int) -> tuple[int, str | None, str] | None:
        now = monotonic()
        with self._auth_cache_lock:
            cached = self._usage_policy_cache.get(api_key_id)
            if cached is None:
                return None
            expires_at, rate_limit, allowed_ip_cidrs, status_value = cached
            if expires_at <= now:
                self._usage_policy_cache.pop(api_key_id, None)
                return None
            self._usage_policy_cache.move_to_end(api_key_id)
            return rate_limit, allowed_ip_cidrs, status_value

    def _store_usage_policy(
        self,
        api_key_id: int,
        rate_limit_per_min: int,
        allowed_ip_cidrs: str | None,
        status_value: str,
        *,
        expected_epoch: int | None = None,
    ) -> None:
        with self._auth_cache_lock:
            if expected_epoch is not None and expected_epoch != self._auth_cache_epoch:
                return
            self._usage_policy_cache[api_key_id] = (
                monotonic() + AUTH_CACHE_TTL_SECONDS,
                int(rate_limit_per_min),
                allowed_ip_cidrs,
                status_value,
            )
            self._usage_policy_cache.move_to_end(api_key_id)
            while len(self._usage_policy_cache) > AUTH_CACHE_MAX_ENTRIES:
                self._usage_policy_cache.popitem(last=False)

    def _invalidate_key_cache(self, api_key_id: int) -> None:
        with self._auth_cache_lock:
            self._auth_cache_epoch += 1
            stale_keys = [
                cache_key
                for cache_key, (_expires_at, record) in self._auth_cache.items()
                if record.context.api_key_id == api_key_id
            ]
            for cache_key in stale_keys:
                self._auth_cache.pop(cache_key, None)
            self._usage_policy_cache.pop(api_key_id, None)
        with self._usage_lock:
            self._usage_buckets.pop(api_key_id, None)
            self._last_usage_persisted.pop(api_key_id, None)

    def compare_public_api_key(self, candidate: object) -> bool:
        set_active_permission_context(None)

        if not isinstance(candidate, str):
            return True

        if self._legacy_public_api_key is not None and hmac.compare_digest(candidate, self._legacy_public_api_key):
            if self._legacy_public_context is not None:
                set_active_permission_context(self._legacy_public_context)
            return False

        try:
            context = self.authenticate_plain_text(candidate)
        except LookupError:
            return True

        set_active_permission_context(context)
        return False

    def _parse_plain_text(self, plain_text: str) -> tuple[str, str, str]:
        if not isinstance(plain_text, str) or len(plain_text) > 512:
            raise LookupError("invalid api key")
        parts = plain_text.split("_", 3)
        if len(parts) != 4 or parts[0] != "ri":
            raise LookupError("invalid api key")
        _, kind, key_prefix, secret = parts
        if kind not in VALID_API_KEY_KINDS or not key_prefix or not secret:
            raise LookupError("invalid api key")
        return kind, key_prefix, secret

    def _normalize_expiration(self, value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value).strip()
            if not text:
                return None
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError("invalid expires_at") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("expires_at must include a timezone")
        return (
            parsed.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    def _expiration_is_due(self, value: object) -> bool:
        if value is None or value == "":
            return False
        try:
            normalized = self._normalize_expiration(value)
        except (TypeError, ValueError):
            # A malformed persisted expiry is a security configuration error;
            # fail closed instead of accidentally turning the key perpetual.
            return True
        return normalized is not None and normalized <= utc_now()

    def _normalize_domain_grant_mode(self, value: object) -> str:
        mode = str(value).strip().lower()
        if mode not in VALID_DOMAIN_GRANT_MODES:
            raise ValueError("invalid domain grant mode")
        return mode

    def _validate_domain_grants(self, mode: str, domain_ids: Sequence[int]) -> None:
        if mode in {"none", "all"} and domain_ids:
            raise ValueError(f"domain ids are not allowed in {mode} grant mode")

    def _normalize_mailbox_patterns(self, values: Sequence[str]) -> tuple[str, ...]:
        if len(values) > MAX_MAILBOX_PATTERNS:
            raise ValueError("too many mailbox patterns")
        seen: set[str] = set()
        normalized_values: list[str] = []
        for value in values:
            pattern = str(value).strip()
            if not pattern:
                raise ValueError("invalid empty mailbox pattern")
            if (
                len(pattern) > 320
                or "\x00" in pattern
                or any(ord(character) < 32 for character in pattern)
                or "[" in pattern
                or "]" in pattern
            ):
                raise ValueError("invalid mailbox pattern")
            if pattern.count("@") != 1:
                raise ValueError("mailbox pattern must contain exactly one @")
            if pattern in seen:
                continue
            seen.add(pattern)
            normalized_values.append(pattern)
        return tuple(normalized_values)

    def _unique_int_values(self, values: Sequence[int]) -> tuple[int, ...]:
        if len(values) > 10_000:
            raise ValueError("too many domain ids")
        seen: set[int] = set()
        unique_values: list[int] = []
        for value in values:
            if isinstance(value, bool):
                raise ValueError("invalid domain id")
            try:
                normalized = int(value)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid domain id") from exc
            if normalized < 1:
                raise ValueError("invalid domain id")
            if normalized in seen:
                continue
            seen.add(normalized)
            unique_values.append(normalized)
        return tuple(unique_values)

    def _normalize_name(self, value: object) -> str:
        name = str(value).strip()
        if not name or len(name) > 200 or any(ord(character) < 32 for character in name):
            raise ValueError("invalid api key name")
        return name

    def _normalize_description(self, value: object) -> str | None:
        description = self._nullable_text(value)
        if description is not None and len(description) > 4000:
            raise ValueError("invalid api key description")
        return description

    def _nullable_text(self, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    def _coerce_bool(self, field_name: str, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, int) and not isinstance(value, bool) and value in (0, 1):
            return bool(value)
        raise ValueError(f"invalid {field_name}")

    def _coerce_non_negative_int(self, field_name: str, value: object) -> int:
        if isinstance(value, bool):
            raise ValueError(f"invalid {field_name}")
        if isinstance(value, float) and not value.is_integer():
            raise ValueError(f"invalid {field_name}")
        try:
            normalized = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"invalid {field_name}") from exc
        if normalized < 0:
            raise ValueError(f"invalid {field_name}")
        if field_name == "rate_limit_per_min" and normalized > MAX_RATE_LIMIT_COUNT:
            raise ValueError(f"invalid {field_name}")
        return normalized

    def _normalize_ip_cidrs(self, values: Sequence[str]) -> tuple[str, ...]:
        if len(values) > MAX_ALLOWED_IP_CIDRS:
            raise ValueError("too many allowed IP networks")
        seen: set[str] = set()
        normalized_values: list[str] = []
        for value in values:
            network = ipaddress.ip_network(str(value), strict=False)
            canonical = network.with_prefixlen
            if canonical in seen:
                continue
            seen.add(canonical)
            normalized_values.append(canonical)
        return tuple(normalized_values)

    def _request_ip_allowed(self, request_ip: str | None, allowed_ip_cidrs_raw: str | None) -> bool:
        if not allowed_ip_cidrs_raw:
            return True
        if request_ip is None:
            return False

        try:
            request_address = ipaddress.ip_address(request_ip)
        except ValueError:
            return False

        try:
            allowed_cidrs = json.loads(allowed_ip_cidrs_raw)
        except json.JSONDecodeError:
            return False

        if isinstance(allowed_cidrs, str):
            allowed_cidrs = [allowed_cidrs]
        if not isinstance(allowed_cidrs, list):
            return False
        if not allowed_cidrs:
            return True

        for allowed_cidr in allowed_cidrs:
            try:
                network = ipaddress.ip_network(str(allowed_cidr), strict=False)
            except ValueError:
                return False
            if request_address in network:
                return True
        return False

    def _load_key(self, connection: sqlite3.Connection, api_key_id: int) -> dict[str, Any] | None:
        row = connection.execute(
            """
            SELECT
                id,
                public_id,
                name,
                description,
                kind,
                key_prefix,
                owner_admin_id,
                status,
                domain_grant_mode,
                allow_header,
                allow_query,
                rate_limit_per_min,
                allowed_ip_cidrs,
                expires_at,
                last_used_at,
                last_used_ip,
                revoked_at,
                created_by_admin_id,
                created_at
            FROM api_keys
            WHERE id = ?
            """,
            (api_key_id,),
        ).fetchone()
        if row is None:
            return None

        scope_rows = connection.execute(
            """
            SELECT scope
            FROM api_key_scopes
            WHERE api_key_id = ?
            ORDER BY scope ASC
            """,
            (api_key_id,),
        ).fetchall()
        domain_rows = connection.execute(
            """
            SELECT domain_id
            FROM api_key_domain_grants
            WHERE api_key_id = ?
            ORDER BY domain_id ASC
            """,
            (api_key_id,),
        ).fetchall()
        mailbox_rows = connection.execute(
            """
            SELECT mailbox_pattern
            FROM api_key_mailbox_grants
            WHERE api_key_id = ?
            ORDER BY mailbox_pattern ASC
            """,
            (api_key_id,),
        ).fetchall()

        allowed_ip_cidrs = self._decode_allowed_ip_cidrs(row["allowed_ip_cidrs"])
        domain_ids = [int(domain_row["domain_id"]) for domain_row in domain_rows]
        return {
            "id": int(row["id"]),
            "public_id": str(row["public_id"]),
            "name": str(row["name"]),
            "description": row["description"],
            "kind": str(row["kind"]),
            "key_prefix": str(row["key_prefix"]),
            "owner_admin_id": row["owner_admin_id"],
            "status": str(row["status"]),
            "domain_grant_mode": str(row["domain_grant_mode"]),
            "allow_header": bool(row["allow_header"]),
            "allow_query": bool(row["allow_query"]),
            "rate_limit_per_min": int(row["rate_limit_per_min"]),
            "allowed_ip_cidrs": allowed_ip_cidrs,
            "expires_at": row["expires_at"],
            "last_used_at": row["last_used_at"],
            "last_used_ip": row["last_used_ip"],
            "revoked_at": row["revoked_at"],
            "created_by_admin_id": row["created_by_admin_id"],
            "created_at": row["created_at"],
            "scopes": [str(scope_row["scope"]) for scope_row in scope_rows],
            "domain_ids": domain_ids,
            "mailbox_patterns": [str(mailbox_row["mailbox_pattern"]) for mailbox_row in mailbox_rows],
        }

    def _decode_allowed_ip_cidrs(self, value: str | None) -> list[str]:
        if not value:
            return []
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        if isinstance(decoded, str):
            return [decoded]
        if isinstance(decoded, list):
            return [str(item) for item in decoded if str(item).strip()]
        return []


__all__ = [
    "ApiKeyAuthorizationError",
    "ApiKeyService",
    "PublicAPIKeyProxy",
    "get_active_permission_context",
    "make_api_key",
    "set_active_permission_context",
]
