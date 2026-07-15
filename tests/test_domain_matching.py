import asyncio
import time

import pytest

from app.smtp import matcher as matcher_module
from app.smtp.matcher import DomainMatcher, DomainRule, normalize_domain, parse_mailbox_address


def test_domain_matcher_prefers_longest_suffix_and_rule_specific_normalization() -> None:
    matcher = DomainMatcher(
        [
            DomainRule(
                domain_id=1,
                root_domain_ascii="adb.com",
                accept_exact=True,
                accept_subdomains=True,
                plus_addressing_mode="keep",
                local_part_case_sensitive=False,
            ),
            DomainRule(
                domain_id=2,
                root_domain_ascii="x.adb.com",
                accept_exact=True,
                accept_subdomains=False,
                plus_addressing_mode="strip",
                local_part_case_sensitive=False,
            ),
        ]
    )

    assert matcher.match_address("Foo+tag@b.x.adb.com") is None

    exact_match = matcher.match_address("Foo+tag@x.adb.com")
    parent_match = matcher.match_address("Foo+tag@z.adb.com")

    assert exact_match is not None
    assert exact_match.domain_id == 2
    assert exact_match.address_canonical == "foo@x.adb.com"

    assert parent_match is not None
    assert parent_match.domain_id == 1
    assert parent_match.address_canonical == "foo+tag@z.adb.com"


def test_domain_matcher_normalizes_unicode_domain_to_idna() -> None:
    matcher = DomainMatcher(
        [
            DomainRule(
                domain_id=3,
                root_domain_ascii="xn--fsqu00a.xn--0zwm56d",
                accept_exact=True,
                accept_subdomains=True,
                plus_addressing_mode="keep",
                local_part_case_sensitive=False,
            )
        ]
    )

    match = matcher.match_address("Inbox@例子.测试")

    assert match is not None
    assert match.domain_ascii == "xn--fsqu00a.xn--0zwm56d"
    assert match.address_canonical == "inbox@xn--fsqu00a.xn--0zwm56d"


def test_domain_matcher_lookup_does_not_renormalize_every_configured_rule(monkeypatch) -> None:
    calls = 0
    real_normalize = matcher_module.normalize_domain

    def counting_normalize(value: str) -> str:
        nonlocal calls
        calls += 1
        return real_normalize(value)

    monkeypatch.setattr(matcher_module, "normalize_domain", counting_normalize)
    matcher = DomainMatcher(
        [
            DomainRule(
                domain_id=index + 1,
                root_domain_ascii=f"domain-{index}.example",
                accept_exact=True,
                accept_subdomains=True,
            )
            for index in range(2_000)
        ]
    )
    calls = 0

    match = matcher.match_address("user@child.domain-1999.example")

    assert match is not None
    assert match.domain_id == 2_000
    # One normalization belongs to the recipient domain itself; configured
    # rules are already indexed by normalized root.
    assert calls == 1


def test_domain_and_mailbox_syntax_limits_match_cpp_ingestd() -> None:
    assert parse_mailbox_address(f"{'a' * 64}@example.com") is not None
    assert parse_mailbox_address(f"{'a' * 65}@example.com") is None
    for invalid in (
        ".user@example.com",
        "user.@example.com",
        "user..tag@example.com",
        "user name@example.com",
        "user@@example.com",
        "user@example.com.",
        "user@ example.com",
        "user@bad_domain.example",
    ):
        assert parse_mailbox_address(invalid) is None

    assert parse_mailbox_address("Üser@example.com", allow_smtputf8=True) is not None
    assert parse_mailbox_address("Üser@example.com", allow_smtputf8=False) is None

    domain_253 = f"{'a' * 63}.{'b' * 63}.{'c' * 63}.{'d' * 61}"
    assert len(domain_253) == 253
    assert normalize_domain(domain_253) == domain_253
    assert parse_mailbox_address(f"a@{domain_253}") is None
    with pytest.raises(ValueError):
        normalize_domain(f"{domain_253}e")

    for invalid_domain in (
        "-bad.example",
        "bad-.example",
        "bad_domain.example",
        "bad domain.example",
        "bad/domain.example",
        "Ａ-.example",
    ):
        with pytest.raises((UnicodeError, ValueError)):
            normalize_domain(invalid_domain)

    matcher = DomainMatcher(
        [
            DomainRule(
                domain_id=5,
                root_domain_ascii="example.com",
                accept_exact=True,
                accept_subdomains=True,
                plus_addressing_mode="strip",
            )
        ]
    )
    assert matcher.match_address("foo.+tag@example.com") is None


@pytest.mark.asyncio
async def test_domain_reload_does_not_block_event_loop_and_publishes_size_limit(runtime, monkeypatch) -> None:
    original_reload = runtime.domains.reload

    def slow_reload() -> None:
        time.sleep(0.1)
        original_reload()

    monkeypatch.setattr(runtime.domains, "reload", slow_reload)
    creation = asyncio.create_task(
        runtime.create_domain("atomic.example", max_message_size_bytes=123_456)
    )
    started = time.perf_counter()
    await asyncio.sleep(0.01)
    timer_elapsed = time.perf_counter() - started
    domain = await creation

    match, size_limit = runtime.domains.match_address_with_size_limit("box@atomic.example")
    assert timer_elapsed < 0.06
    assert match is not None
    assert match.domain_id == domain["id"]
    assert size_limit == 123_456
