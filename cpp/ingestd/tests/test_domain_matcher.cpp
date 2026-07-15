#include "../src/domain_matcher.h"

#include <stdexcept>
#include <string>
#include <vector>

namespace test {
inline void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
}

namespace {

using rapid_inbox::ingestd::DomainMatch;
using rapid_inbox::ingestd::DomainMatcher;
using rapid_inbox::ingestd::DomainRule;

DomainMatch require_match(const DomainMatcher& matcher, const std::string& address) {
    auto match = matcher.match_address(address);
    test::check(match.has_value(), "expected match for " + address);
    return *match;
}

void require_normalize_rejects(const std::string& domain, const std::string& message) {
    bool rejected = false;
    try {
        (void)rapid_inbox::ingestd::normalize_domain(domain);
    } catch (const std::exception&) {
        rejected = true;
    }
    test::check(rejected, message);
}

}  // namespace

void test_domain_matcher_exact_subdomain_and_longest_suffix() {
    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 1,
                .root_domain_ascii = "adb.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "Code@adb.com");
        test::check(match.domain_id == 1, "exact root domain id");
        test::check(match.domain_ascii == "adb.com", "exact normalized recipient domain");
        test::check(match.root_domain_ascii == "adb.com", "exact normalized root domain");
        test::check(match.local_part == "Code", "exact original local part");
        test::check(match.local_part_canonical == "code", "exact canonical local part");
        test::check(match.address_canonical == "code@adb.com", "exact canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 1,
                .root_domain_ascii = "adb.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
            DomainRule{
                .domain_id = 2,
                .root_domain_ascii = "x.adb.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "User@deep.x.adb.com");
        test::check(match.domain_id == 2, "longest suffix domain id");
        test::check(match.domain_ascii == "deep.x.adb.com", "longest suffix recipient domain");
        test::check(match.address_canonical == "user@deep.x.adb.com",
                    "longest suffix canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 3,
                .root_domain_ascii = "exact-only.test",
                .accept_exact = true,
                .accept_subdomains = false,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        test::check(!matcher.match_address("a@sub.exact-only.test").has_value(),
                    "subdomain disabled returns no match");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 1,
                .root_domain_ascii = "adb.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
            DomainRule{
                .domain_id = 2,
                .root_domain_ascii = "x.adb.com",
                .accept_exact = true,
                .accept_subdomains = false,
                .plus_addressing_mode = "strip",
                .local_part_case_sensitive = false,
            },
        });

        test::check(!matcher.match_address("Foo+tag@b.x.adb.com").has_value(),
                    "longest disabled subdomain blocks parent fallback");

        const DomainMatch exact_match = require_match(matcher, "Foo+tag@x.adb.com");
        test::check(exact_match.domain_id == 2, "exact longest rule id");
        test::check(exact_match.address_canonical == "foo@x.adb.com",
                    "exact longest rule strips plus tag");

        const DomainMatch parent_match = require_match(matcher, "Foo+tag@z.adb.com");
        test::check(parent_match.domain_id == 1, "parent rule id");
        test::check(parent_match.address_canonical == "foo+tag@z.adb.com",
                    "parent rule keeps plus tag");
    }
}

void test_domain_matcher_plus_and_case_modes() {
    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 1,
                .root_domain_ascii = "strip.test",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "strip",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "User+tag@strip.test");
        test::check(match.local_part == "User+tag", "plus strip original local part");
        test::check(match.local_part_canonical == "user", "plus strip canonical local part");
        test::check(match.address_canonical == "user@strip.test",
                    "plus strip canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 2,
                .root_domain_ascii = "case.test",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = true,
            },
        });

        const DomainMatch match = require_match(matcher, "User@case.test");
        test::check(match.local_part_canonical == "User", "case-sensitive local part preserved");
        test::check(match.address_canonical == "User@case.test",
                    "case-sensitive canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 3,
                .root_domain_ascii = "case.test",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "\xC3\x9C" "ser@case.test");
        test::check(match.local_part_canonical == "\xC3\xBC" "ser",
                    "unicode lowercase local part");
        test::check(match.address_canonical == "\xC3\xBC" "ser@case.test",
                    "unicode lowercase canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 4,
                .root_domain_ascii = "case.test",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "\xC4\xB0@case.test");
        test::check(match.local_part_canonical == "i\xCC\x87",
                    "dotted capital i canonical local part");
        test::check(match.address_canonical == "i\xCC\x87@case.test",
                    "dotted capital i canonical address");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 4,
                .root_domain_ascii = "example.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const std::string invalid_local_part_address("\xFF@example.com",
                                                     sizeof("\xFF@example.com") - 1);
        test::check(!matcher.match_address(invalid_local_part_address).has_value(),
                    "case-insensitive invalid UTF-8 local part returns no match");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 5,
                .root_domain_ascii = "case.test",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = true,
            },
        });

        const DomainMatch match = require_match(matcher, "\xC4\xB0@case.test");
        test::check(match.local_part_canonical == "\xC4\xB0",
                    "unicode case-sensitive local part preserved");
        test::check(match.address_canonical == "\xC4\xB0@case.test",
                    "unicode case-sensitive canonical address");
    }
}

void test_domain_matcher_enforces_mailbox_syntax_and_length_limits() {
    using rapid_inbox::ingestd::parse_mailbox_address;
    const std::string local_64(64, 'a');
    test::check(parse_mailbox_address(local_64 + "@example.com").has_value(),
                "64-byte local part is accepted");
    test::check(!parse_mailbox_address(std::string(65, 'a') + "@example.com").has_value(),
                "65-byte local part is rejected");
    test::check(!parse_mailbox_address(".user@example.com").has_value(),
                "leading local dot is rejected");
    test::check(!parse_mailbox_address("user.@example.com").has_value(),
                "trailing local dot is rejected");
    test::check(!parse_mailbox_address("user..tag@example.com").has_value(),
                "consecutive local dots are rejected");
    test::check(!parse_mailbox_address("user name@example.com").has_value(),
                "local whitespace is rejected");
    test::check(!parse_mailbox_address("user@@example.com").has_value(),
                "multiple at signs are rejected");
    test::check(!parse_mailbox_address("user@example.com.").has_value(),
                "trailing mailbox domain dot is rejected");
    test::check(!parse_mailbox_address("user@ example.com").has_value(),
                "mailbox domain whitespace is rejected");
    test::check(!parse_mailbox_address("user@bad_domain.example").has_value(),
                "non-LDH mailbox domain is rejected");

    const std::string utf8_address = "\xC3\x9C" "ser@example.com";
    test::check(parse_mailbox_address(utf8_address, true).has_value(),
                "SMTPUTF8 local part is accepted when enabled");
    test::check(!parse_mailbox_address(utf8_address, false).has_value(),
                "SMTPUTF8 local part is rejected when disabled");

    const std::string domain_253 = std::string(63, 'a') + "." + std::string(63, 'b') +
                                   "." + std::string(63, 'c') + "." + std::string(61, 'd');
    test::check(domain_253.size() == 253, "test domain reaches DNS maximum");
    test::check(rapid_inbox::ingestd::normalize_domain(domain_253) == domain_253,
                "253-byte domain is accepted for domain configuration");
    test::check(!parse_mailbox_address("a@" + domain_253).has_value(),
                "mailbox path enforces the 254-byte total limit");
    require_normalize_rejects(domain_253 + "e", "domain over 253 bytes is rejected");

    DomainMatcher strip_matcher({
        DomainRule{
            .domain_id = 20,
            .root_domain_ascii = "example.com",
            .accept_exact = true,
            .accept_subdomains = true,
            .plus_addressing_mode = "strip",
            .local_part_case_sensitive = false,
        },
    });
    test::check(!strip_matcher.match_address("foo.+tag@example.com").has_value(),
                "plus stripping cannot create an invalid trailing-dot local part");
}

void test_domain_matcher_normalizes_unicode_domain_to_idna() {
    test::check(rapid_inbox::ingestd::normalize_domain(
                    "\xC2\xA0" "example.com" "\xC2\xA0") == "example.com",
                "unicode NBSP at domain edges is stripped");
    test::check(rapid_inbox::ingestd::normalize_domain("example.com\xE3\x80\x80") ==
                    "example.com",
                "unicode ideographic space at domain edge is stripped");
    test::check(rapid_inbox::ingestd::normalize_domain(
                    "\xE2\x80\x83" "example.com" "\xE2\x80\x89") == "example.com",
                "unicode em and thin spaces at domain edges are stripped");
    test::check(rapid_inbox::ingestd::normalize_domain(std::string(63, 'a') + ".com") ==
                    std::string(63, 'a') + ".com",
                "63-byte ASCII label is accepted");
    test::check(rapid_inbox::ingestd::normalize_domain(
                    "\xE4\xBE\x8B\xE5\xAD\x90\xE3\x80\x82\xE6\xB5\x8B\xE8\xAF\x95") ==
                    "xn--fsqu00a.xn--0zwm56d",
                "python idna treats ideographic full stop as a dot separator");
    test::check(rapid_inbox::ingestd::normalize_domain("example" "\xEF\xBC\x8E" "com") ==
                    "example.com",
                "python idna treats fullwidth full stop as a dot separator");
    test::check(rapid_inbox::ingestd::normalize_domain("example" "\xEF\xBD\xA1" "com") ==
                    "example.com",
                "python idna treats halfwidth ideographic full stop as a dot separator");
    test::check(rapid_inbox::ingestd::normalize_domain("\xE1\x8E\xA0.com") ==
                    "xn--kz9a.com",
                "python lower then idna normalizes Cherokee capital letter a");
    test::check(rapid_inbox::ingestd::normalize_domain("\xE1\x83\xBC.com") ==
                    "xn--upd.com",
                "python lower then idna normalizes Georgian modifier letter nar");

    require_normalize_rejects("bad..com", "empty interior ASCII label is rejected");
    require_normalize_rejects(".leading.com", "leading empty ASCII label is rejected");
    require_normalize_rejects("-bad.com", "leading hyphen is rejected");
    require_normalize_rejects("bad-.com", "trailing hyphen is rejected");
    require_normalize_rejects("a_b.com", "underscore in domain is rejected");
    require_normalize_rejects("bad com.com", "space in domain is rejected");
    require_normalize_rejects("bad/com.com", "slash in domain is rejected");
    require_normalize_rejects("\xEF\xBC\xA1-.com",
                              "unicode label with trailing hyphen is rejected");
    require_normalize_rejects(std::string(64, 'a') + ".com",
                              "64-byte ASCII label is rejected");
    require_normalize_rejects("\xEE\x80\x80.com",
                              "python idna rejects private-use codepoints");
    require_normalize_rejects("\xE2\x80\xAE.com",
                              "python idna rejects right-to-left override");
    require_normalize_rejects("\xD7\x90" "a.com",
                              "python idna rejects mixed bidi labels");

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 5,
                .root_domain_ascii = "example.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch normal_match = require_match(matcher, "User@example.com");
        test::check(normal_match.domain_id == 5, "normal example.com domain id");
        test::check(normal_match.address_canonical == "user@example.com",
                    "normal example.com canonical address");

        const DomainMatch halfwidth_dot_match =
            require_match(matcher, "User@example" "\xEF\xBD\xA1" "com");
        test::check(halfwidth_dot_match.domain_id == 5, "halfwidth dot domain id");
        test::check(halfwidth_dot_match.address_canonical == "user@example.com",
                    "halfwidth dot canonical address");

        const std::string embedded_nul_domain("example.com\0.evil.com",
                                              sizeof("example.com\0.evil.com") - 1);
        bool rejected_embedded_nul_domain = false;
        try {
            (void)rapid_inbox::ingestd::normalize_domain(embedded_nul_domain);
        } catch (const std::invalid_argument&) {
            rejected_embedded_nul_domain = true;
        }
        test::check(rejected_embedded_nul_domain,
                    "embedded NUL domain is rejected before C ABI normalization");

        const std::string embedded_nul_address("User@example.com\0.evil.com",
                                               sizeof("User@example.com\0.evil.com") - 1);
        test::check(!matcher.match_address(embedded_nul_address).has_value(),
                    "embedded NUL recipient domain returns no match");
        test::check(!matcher.match_address("User@bad..com").has_value(),
                    "invalid empty ASCII recipient label returns no match");
        test::check(!matcher.match_address("User@\xEE\x80\x80.com").has_value(),
                    "private-use recipient domain returns no match");
    }

    {
        DomainMatcher matcher({
            DomainRule{
                .domain_id = 11,
                .root_domain_ascii = "xn--kz9a.com",
                .accept_exact = true,
                .accept_subdomains = true,
                .plus_addressing_mode = "keep",
                .local_part_case_sensitive = false,
            },
        });

        const DomainMatch match = require_match(matcher, "User@\xE1\x8E\xA0.com");
        test::check(match.domain_id == 11, "cherokee normalized domain id");
        test::check(match.domain_ascii == "xn--kz9a.com", "cherokee normalized domain");
        test::check(match.address_canonical == "user@xn--kz9a.com",
                    "cherokee normalized canonical address");
    }

    test::check(rapid_inbox::ingestd::normalize_domain("stra" "\xC3\x9F" "e.de") == "strasse.de",
                "unicode domain normalizes to IDNA transitional form");

    const std::string invalid_unicode_edge_domain = "ma" "\xC3\xB1" "ana-.com";
    require_normalize_rejects(invalid_unicode_edge_domain,
                              "unicode label with trailing hyphen is rejected");

    DomainMatcher matcher({
        DomainRule{
            .domain_id = 3,
            .root_domain_ascii = "strasse.de",
            .accept_exact = true,
            .accept_subdomains = true,
            .plus_addressing_mode = "keep",
            .local_part_case_sensitive = false,
        },
    });

    const std::string unicode_domain = "stra" "\xC3\x9F" "e.de";
    const DomainMatch match = require_match(matcher, "User@" + unicode_domain);

    test::check(match.domain_id == 3, "idna unicode domain id");
    test::check(match.domain_ascii == "strasse.de", "idna normalized domain");
    test::check(match.root_domain_ascii == "strasse.de", "idna normalized root domain");
    test::check(match.address_canonical == "user@strasse.de",
                "idna canonical address");

    DomainMatcher idna_matcher({
        DomainRule{
            .domain_id = 4,
            .root_domain_ascii = "xn--fsqu00a.xn--0zwm56d",
            .accept_exact = true,
            .accept_subdomains = true,
            .plus_addressing_mode = "keep",
            .local_part_case_sensitive = false,
        },
    });

    const std::string chinese_example_domain =
        "\xE4\xBE\x8B\xE5\xAD\x90.\xE6\xB5\x8B\xE8\xAF\x95";
    const DomainMatch idna_match = require_match(idna_matcher, "Inbox@" + chinese_example_domain);

    test::check(idna_match.domain_id == 4, "python idna example domain id");
    test::check(idna_match.domain_ascii == "xn--fsqu00a.xn--0zwm56d",
                "python idna example normalized domain");
    test::check(idna_match.address_canonical == "inbox@xn--fsqu00a.xn--0zwm56d",
                "python idna example canonical address");
}

void test_domain_matcher_uses_explicit_catch_all_as_fallback() {
    DomainMatcher matcher({
        DomainRule{1, "managed.example", true, true, "keep", false},
        DomainRule{9, "*", true, true, "strip", false},
    });

    const DomainMatch managed = require_match(matcher, "User@managed.example");
    test::check(managed.domain_id == 1, "specific route wins before catch-all");

    const DomainMatch fallback = require_match(matcher, "User+tag@Other.Example");
    test::check(fallback.domain_id == 9, "catch-all domain id");
    test::check(fallback.root_domain_ascii == "*", "catch-all root retained");
    test::check(fallback.domain_ascii == "other.example", "actual recipient domain retained");
    test::check(fallback.address_canonical == "user@other.example",
                "catch-all policy canonicalizes local part");
    test::check(!matcher.match_address("@other.example").has_value(),
                "catch-all rejects empty local part");
}
