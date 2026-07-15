#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace rapid_inbox::ingestd {

struct DomainRule {
    int domain_id;
    std::string root_domain_ascii;
    bool accept_exact;
    bool accept_subdomains;
    std::string plus_addressing_mode;
    bool local_part_case_sensitive;
};

struct DomainMatch {
    int domain_id;
    std::string domain_ascii;
    std::string root_domain_ascii;
    std::string local_part;
    std::string local_part_canonical;
    std::string address_canonical;
};

struct ParsedMailboxAddress {
    std::string local_part;
    std::string domain_ascii;
};

inline constexpr std::size_t kMaximumLocalPartBytes = 64;
inline constexpr std::size_t kMaximumDomainBytes = 253;
inline constexpr std::size_t kMaximumMailboxBytes = 254;

std::string normalize_domain(std::string domain);
std::optional<ParsedMailboxAddress> parse_mailbox_address(const std::string& address,
                                                          bool allow_smtputf8 = true);

class DomainMatcher {
public:
    explicit DomainMatcher(std::vector<DomainRule> rules);
    std::optional<DomainMatch> match_address(const std::string& address) const;

private:
    struct TransparentStringHash {
        using is_transparent = void;

        std::size_t operator()(std::string_view value) const noexcept {
            return std::hash<std::string_view>{}(value);
        }
    };

    using RuleIndex =
        std::unordered_map<std::string, DomainRule, TransparentStringHash, std::equal_to<>>;

    RuleIndex rules_by_root_;
    std::optional<DomainRule> catch_all_rule_;
};

}
