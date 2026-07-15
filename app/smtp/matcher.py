from __future__ import annotations

from dataclasses import dataclass
import unicodedata


MAX_LOCAL_PART_BYTES = 64
MAX_DOMAIN_BYTES = 253
MAX_MAILBOX_BYTES = 254
_ASCII_ATEXT = frozenset("!#$%&'*+-/=?^_`{|}~")


def _validate_ascii_domain(domain: str) -> None:
    if not domain or len(domain) > MAX_DOMAIN_BYTES:
        raise ValueError("invalid domain length")
    for label in domain.split("."):
        if not label or len(label) > 63:
            raise ValueError("invalid domain label length")
        if label.startswith("-") or label.endswith("-"):
            raise ValueError("domain label cannot start or end with hyphen")
        if any(not (char.isascii() and (char.isalnum() or char == "-")) for char in label):
            raise ValueError("invalid domain label character")


def normalize_domain(domain: str) -> str:
    normalized = domain.strip().rstrip(".").lower()
    if normalized == "*":
        return normalized
    if not normalized:
        raise ValueError("empty domain")
    mapped_labels = normalized.replace("。", ".").replace("．", ".").replace("｡", ".").split(".")
    if any(not label or label.startswith("-") or label.endswith("-") for label in mapped_labels):
        raise ValueError("domain label cannot start or end with hyphen")
    ascii_domain = normalized.encode("idna").decode("ascii").lower()
    _validate_ascii_domain(ascii_domain)
    return ascii_domain


def _valid_local_part(local_part: str, *, allow_smtputf8: bool) -> bool:
    try:
        encoded = local_part.encode("utf-8")
    except UnicodeEncodeError:
        return False
    if not encoded or len(encoded) > MAX_LOCAL_PART_BYTES:
        return False
    if local_part.startswith(".") or local_part.endswith(".") or ".." in local_part:
        return False
    for char in local_part:
        if ord(char) >= 128:
            if not allow_smtputf8 or char.isspace() or unicodedata.category(char).startswith("C"):
                return False
            continue
        if char == "." or char.isalnum() or char in _ASCII_ATEXT:
            continue
        return False
    return True


def parse_mailbox_address(
    address: str,
    *,
    allow_smtputf8: bool = True,
) -> tuple[str, str] | None:
    try:
        if not address or len(address.encode("utf-8")) > MAX_MAILBOX_BYTES:
            return None
    except UnicodeEncodeError:
        return None
    if address.count("@") != 1:
        return None
    local_part, raw_domain = address.split("@", 1)
    if (
        not _valid_local_part(local_part, allow_smtputf8=allow_smtputf8)
        or not raw_domain
        or raw_domain.endswith(".")
        or raw_domain.strip() != raw_domain
        or (not allow_smtputf8 and not raw_domain.isascii())
    ):
        return None
    try:
        normalized_domain = normalize_domain(raw_domain)
    except (UnicodeError, ValueError):
        return None
    if normalized_domain == "*":
        return None
    if len(local_part.encode("utf-8")) + 1 + len(normalized_domain) > MAX_MAILBOX_BYTES:
        return None
    return local_part, normalized_domain


@dataclass(frozen=True, slots=True)
class DomainRule:
    domain_id: int
    root_domain_ascii: str
    accept_exact: bool
    accept_subdomains: bool
    plus_addressing_mode: str = "keep"
    local_part_case_sensitive: bool = False

    def normalized_root(self) -> str:
        return normalize_domain(self.root_domain_ascii)


@dataclass(frozen=True, slots=True)
class DomainMatch:
    domain_id: int
    domain_ascii: str
    root_domain_ascii: str
    local_part: str
    local_part_canonical: str
    address_canonical: str


class DomainMatcher:
    def __init__(self, rules: list[DomainRule]) -> None:
        self._rules_by_root: dict[str, DomainRule] = {}
        self._catch_all_rule: DomainRule | None = None
        for rule in rules:
            normalized_root = rule.normalized_root()
            if normalized_root == "*":
                if self._catch_all_rule is None:
                    self._catch_all_rule = rule
                continue
            # DomainService loads rows by ascending id. Preserve the former
            # stable-sort behavior if a synthetic caller supplies duplicates:
            # the first configured rule wins.
            self._rules_by_root.setdefault(normalized_root, rule)

    def match_address(self, address: str) -> DomainMatch | None:
        parsed = parse_mailbox_address(address)
        if parsed is None:
            return None
        local_part, normalized_domain = parsed

        normalized_root = normalized_domain
        rule = self._rules_by_root.get(normalized_root)
        is_exact = rule is not None
        if rule is None:
            # Walk DNS suffixes from longest to shortest. This keeps lookup
            # proportional to label count instead of configured-domain count.
            for dot_index, character in enumerate(normalized_domain):
                if character != ".":
                    continue
                candidate_root = normalized_domain[dot_index + 1 :]
                candidate = self._rules_by_root.get(candidate_root)
                if candidate is not None:
                    normalized_root = candidate_root
                    rule = candidate
                    break

        if rule is None and self._catch_all_rule is not None:
            rule = self._catch_all_rule
            normalized_root = "*"
            is_exact = True

        if rule is not None:
            if normalized_root != "*" and is_exact and not rule.accept_exact:
                return None
            if normalized_root != "*" and not is_exact and not rule.accept_subdomains:
                return None

            local_part_canonical = local_part
            if rule.plus_addressing_mode == "strip":
                local_part_canonical = local_part_canonical.split("+", 1)[0]
            if not rule.local_part_case_sensitive:
                local_part_canonical = local_part_canonical.lower()
            if not _valid_local_part(local_part_canonical, allow_smtputf8=True):
                return None
            if (
                len(local_part_canonical.encode("utf-8"))
                + 1
                + len(normalized_domain)
                > MAX_MAILBOX_BYTES
            ):
                return None

            return DomainMatch(
                domain_id=rule.domain_id,
                domain_ascii=normalized_domain,
                root_domain_ascii=normalized_root,
                local_part=local_part,
                local_part_canonical=local_part_canonical,
                address_canonical=f"{local_part_canonical}@{normalized_domain}",
            )

        return None
