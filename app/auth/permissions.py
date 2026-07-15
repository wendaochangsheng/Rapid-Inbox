from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from datetime import datetime, timezone
from fnmatch import fnmatchcase
from types import MappingProxyType
from typing import Any, Mapping

from fastapi import HTTPException


VALID_DOMAIN_GRANT_MODES = frozenset({"none", "selected", "all"})

PUBLIC_API_KEY_SCOPES = frozenset({"public.read"})
SERVICE_API_KEY_SCOPES = frozenset(
    {
        "public.read",
        "live.read",
        "domains.read",
        "domains.write",
        "mailboxes.read",
        "mailboxes.write",
        "messages.read",
        "messages.write",
        "smtp.read",
        "audit.read",
        "system.read",
    }
)
ADMIN_API_KEY_SCOPES = frozenset(
    {
        *SERVICE_API_KEY_SCOPES,
        "system.write",
        "api_keys.read",
        "api_keys.write",
        "admins.read",
        "admins.write",
        "admins.credentials.write",
        "admins.sessions.write",
    }
)
VALID_API_KEY_SCOPES = frozenset(
    {
        *PUBLIC_API_KEY_SCOPES,
        *SERVICE_API_KEY_SCOPES,
        *ADMIN_API_KEY_SCOPES,
    }
)
API_KEY_KIND_SCOPES: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "public": PUBLIC_API_KEY_SCOPES,
        "service": SERVICE_API_KEY_SCOPES,
        "admin": ADMIN_API_KEY_SCOPES,
    }
)

_VIEWER_SCOPES = frozenset(
    {
        "live.read",
        "domains.read",
        "mailboxes.read",
        "messages.read",
        "smtp.read",
        "audit.read",
        "system.read",
        "api_keys.read",
        "admins.read",
    }
)
_OPERATOR_SCOPES = frozenset(
    {
        *_VIEWER_SCOPES,
        "domains.write",
        "mailboxes.write",
        "messages.write",
    }
)
ROLE_SCOPES: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        "viewer": _VIEWER_SCOPES,
        "operator": _OPERATOR_SCOPES,
        "superadmin": ADMIN_API_KEY_SCOPES,
    }
)


@dataclass(slots=True)
class PermissionContext:
    scopes: tuple[str, ...]
    domain_ids: tuple[int, ...]
    mailbox_patterns: tuple[str, ...]
    domain_grant_mode: str = "none"
    api_key_id: int | None = None
    public_id: str = ""
    name: str = ""
    kind: str = "public"
    legacy_credential: bool = False
    rate_limit_per_min: int = 0
    allowed_ip_cidrs: tuple[str, ...] = ()
    expires_at: str | None = None
    allow_header: bool = True
    allow_query: bool = False
    # Human-session identities are carried separately from API-key identities
    # so privileged service mutations can re-load the session and role inside
    # the same writer transaction.  Contexts without these fields remain
    # useful for trusted internal callers and tests.
    admin_id: int | None = None
    admin_session_id: str | None = None


class PermissionDenied(HTTPException):
    def __init__(self, detail: str) -> None:
        super().__init__(status_code=403, detail=detail)


def validate_scopes_for_kind(kind: str, scopes: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    allowed_scopes = API_KEY_KIND_SCOPES.get(kind)
    if allowed_scopes is None:
        raise ValueError("invalid api key kind")
    if len(scopes) > 100:
        raise ValueError("too many api key scopes")

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_scope in scopes:
        scope = str(raw_scope).strip()
        if not scope:
            raise ValueError("invalid empty api key scope")
        if scope not in VALID_API_KEY_SCOPES:
            raise ValueError(f"invalid api key scope: {scope}")
        if scope not in allowed_scopes:
            raise ValueError(f"api key kind {kind} cannot use scope: {scope}")
        if scope in seen:
            continue
        seen.add(scope)
        normalized.append(scope)
    if not normalized:
        raise ValueError("api key scopes are required")
    return tuple(normalized)


def role_permission_context(admin: Mapping[str, Any] | str) -> PermissionContext:
    if isinstance(admin, str):
        role = admin
        public_id = f"role:{role}"
        name = role
        admin_id = None
        admin_session_id = None
    else:
        role = str(admin.get("role") or "")
        public_id = str(admin.get("session_id") or admin.get("id") or admin.get("username") or "admin")
        name = str(admin.get("display_name") or admin.get("username") or public_id)
        raw_admin_id = admin.get("admin_id", admin.get("id"))
        try:
            admin_id = None if raw_admin_id is None else int(raw_admin_id)
        except (TypeError, ValueError) as exc:
            raise PermissionDenied("invalid admin identity") from exc
        raw_session_id = admin.get("session_id")
        admin_session_id = None if raw_session_id is None else str(raw_session_id)

    scopes = ROLE_SCOPES.get(role)
    if scopes is None:
        raise PermissionDenied("invalid admin role")
    return PermissionContext(
        scopes=tuple(sorted(scopes)),
        domain_ids=(),
        mailbox_patterns=(),
        domain_grant_mode="all",
        public_id=public_id,
        name=name,
        kind="admin",
        admin_id=admin_id,
        admin_session_id=admin_session_id,
        # Human administrator sessions are not constrained to one API-key
        # transport. Model them as an unrestricted delegation root; role scopes
        # still decide which administrators may manage keys at all.
        allow_query=True,
    )


def require_admin_role_scope(admin: Mapping[str, Any] | PermissionContext | str, required_scope: str) -> PermissionContext:
    context = admin if isinstance(admin, PermissionContext) else role_permission_context(admin)
    if required_scope not in VALID_API_KEY_SCOPES:
        raise ValueError(f"invalid required scope: {required_scope}")
    if required_scope not in context.scopes:
        raise PermissionDenied(required_scope)
    return context


def mailbox_pattern_matches(mailbox_address: str, mailbox_pattern: str) -> bool:
    # Only '*' and '?' wildcards are accepted when keys are created. This keeps
    # Python point checks identical to SQLite GLOB filters used by list APIs;
    # bracket expressions are rejected so the two engines cannot disagree and
    # expose a mailbox outside the caller's grant.
    return fnmatchcase(mailbox_address, mailbox_pattern)


def delegated_api_key_policy_is_within_principal(
    principal: PermissionContext,
    target: Mapping[str, Any],
) -> bool:
    """Return whether a child key preserves the caller's operational bounds."""

    try:
        target_rate = int(target.get("rate_limit_per_min", 3600))
    except (TypeError, ValueError):
        return False
    if isinstance(target.get("rate_limit_per_min", 3600), bool) or target_rate < 0:
        return False
    if principal.rate_limit_per_min > 0 and (
        target_rate == 0 or target_rate > principal.rate_limit_per_min
    ):
        return False

    raw_target_cidrs = target.get("allowed_ip_cidrs") or ()
    if isinstance(raw_target_cidrs, str):
        raw_target_cidrs = (raw_target_cidrs,)
    try:
        parent_networks = tuple(
            ipaddress.ip_network(value, strict=False)
            for value in principal.allowed_ip_cidrs
        )
        target_networks = tuple(
            ipaddress.ip_network(str(value), strict=False)
            for value in raw_target_cidrs
        )
    except (TypeError, ValueError):
        return False
    if parent_networks:
        if not target_networks:
            return False
        if any(
            not any(
                child.version == parent.version and child.subnet_of(parent)
                for parent in parent_networks
            )
            for child in target_networks
        ):
            return False

    if principal.expires_at is not None:
        target_expiration = _parse_api_key_expiration(target.get("expires_at"))
        parent_expiration = _parse_api_key_expiration(principal.expires_at)
        if (
            target_expiration is None
            or parent_expiration is None
            or target_expiration > parent_expiration
        ):
            return False

    if bool(target.get("allow_header", True)) and not principal.allow_header:
        return False
    if bool(target.get("allow_query", False)) and not principal.allow_query:
        return False
    return True


def api_key_is_within_principal(
    principal: PermissionContext,
    target: Mapping[str, Any],
) -> bool:
    """Prove that an API-key policy is contained by the acting principal."""

    if not delegated_api_key_policy_is_within_principal(principal, target):
        return False
    effective_scopes = set(principal.scopes)
    effective_scopes.update(
        f"{scope[:-6]}.read"
        for scope in principal.scopes
        if scope.endswith(".write")
    )
    if not set(str(scope) for scope in target.get("scopes") or ()).issubset(effective_scopes):
        return False

    target_mode = str(target.get("domain_grant_mode") or "none")
    if principal.domain_grant_mode == "all":
        allowed_domains: set[int] | None = None
    elif principal.domain_grant_mode == "selected":
        allowed_domains = {int(domain_id) for domain_id in principal.domain_ids}
    else:
        allowed_domains = set()
    if target_mode == "all":
        if allowed_domains is not None:
            return False
    elif target_mode == "selected":
        try:
            target_domains = {int(domain_id) for domain_id in target.get("domain_ids") or ()}
        except (TypeError, ValueError):
            return False
        if allowed_domains is not None and not target_domains.issubset(allowed_domains):
            return False
    elif target_mode != "none":
        return False

    principal_patterns = set(principal.mailbox_patterns)
    target_patterns = {str(pattern) for pattern in target.get("mailbox_patterns") or ()}
    if principal_patterns:
        if not target_patterns:
            return False
        for target_pattern in target_patterns:
            if target_pattern in principal_patterns:
                continue
            # Full glob-language containment is deliberately not guessed. A
            # literal child grant can safely be proven to fit a caller glob;
            # any different child glob is denied unless it exactly matches.
            if "*" in target_pattern or "?" in target_pattern:
                return False
            if not any(fnmatchcase(target_pattern, parent) for parent in principal_patterns):
                return False
    return True


def _parse_api_key_expiration(value: object) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def ensure_mailbox_access(grants: PermissionContext, mailbox_address: str, domain_id: int, required_scope: str) -> None:
    if required_scope not in grants.scopes:
        raise PermissionDenied(required_scope)
    # Legacy credentials remain an explicitly unrestricted compatibility path.
    # New credentials never infer unrestricted access from an empty grant set.
    if grants.legacy_credential:
        return

    if grants.domain_grant_mode == "all":
        pass
    elif grants.domain_grant_mode == "selected":
        if domain_id not in grants.domain_ids:
            raise PermissionDenied("domain grant missing")
    elif grants.domain_grant_mode == "none":
        raise PermissionDenied("domain grant missing")
    else:
        raise PermissionDenied("invalid domain grant mode")

    if grants.mailbox_patterns and not any(
        mailbox_pattern_matches(mailbox_address, pattern)
        for pattern in grants.mailbox_patterns
    ):
        raise PermissionDenied("mailbox grant missing")


__all__ = [
    "ADMIN_API_KEY_SCOPES",
    "API_KEY_KIND_SCOPES",
    "PermissionContext",
    "PermissionDenied",
    "PUBLIC_API_KEY_SCOPES",
    "ROLE_SCOPES",
    "SERVICE_API_KEY_SCOPES",
    "VALID_API_KEY_SCOPES",
    "VALID_DOMAIN_GRANT_MODES",
    "api_key_is_within_principal",
    "delegated_api_key_policy_is_within_principal",
    "ensure_mailbox_access",
    "mailbox_pattern_matches",
    "require_admin_role_scope",
    "role_permission_context",
    "validate_scopes_for_kind",
]
