#include "domain_cache.h"

#include "sqlite_db.h"

#include <sqlite3.h>

#include <cstdint>
#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

namespace rapid_inbox::ingestd {
namespace {

std::string column_text_or_default(sqlite3_stmt* statement,
                                   int column,
                                   const std::string& fallback) {
    if (sqlite3_column_type(statement, column) == SQLITE_NULL) {
        return fallback;
    }
    const unsigned char* text = sqlite3_column_text(statement, column);
    if (text == nullptr) {
        return fallback;
    }
    const int bytes = sqlite3_column_bytes(statement, column);
    return std::string(reinterpret_cast<const char*>(text), static_cast<std::size_t>(bytes));
}

int column_int_or_default(sqlite3_stmt* statement, int column, int fallback) {
    if (sqlite3_column_type(statement, column) == SQLITE_NULL) {
        return fallback;
    }
    return sqlite3_column_int(statement, column);
}

std::int64_t column_int64_or_default(sqlite3_stmt* statement,
                                     int column,
                                     std::int64_t fallback) {
    if (sqlite3_column_type(statement, column) == SQLITE_NULL) {
        return fallback;
    }
    return sqlite3_column_int64(statement, column);
}

}  // namespace

DomainCache::DomainCache(std::filesystem::path database_path, int busy_timeout_ms)
    : database_path_(std::move(database_path)),
      busy_timeout_ms_(busy_timeout_ms),
      rules_(std::make_shared<const DomainRulesSnapshot>(DomainRulesSnapshot{
          .matcher = DomainMatcher(std::vector<DomainRule>{}),
          .policies = {},
          .generation = 0,
      })) {}

void DomainCache::reload() {
    SqliteDb db(database_path_, busy_timeout_ms_);
    Statement statement = db.prepare(R"SQL(
SELECT id,
       root_domain_ascii,
       root_domain_unicode,
       accept_exact,
       accept_subdomains,
       public_web_enabled,
       public_api_enabled,
       is_active,
       is_hidden,
       plus_addressing_mode,
       local_part_case_sensitive,
       max_message_size_bytes,
       retention_days,
       dns_status
FROM domains
WHERE is_active = 1
ORDER BY id ASC
)SQL");

    std::vector<DomainRule> rules;
    std::unordered_map<int, DomainPolicySnapshot> domain_policies;
    while (statement.step_row()) {
        sqlite3_stmt* row = statement.get();
        std::string root_domain_ascii = column_text_or_default(row, 1, "");
        if (root_domain_ascii.empty()) {
            continue;
        }

        const int domain_id = sqlite3_column_int(row, 0);
        std::string root_domain_unicode = column_text_or_default(row, 2, root_domain_ascii);
        if (root_domain_unicode.empty()) {
            root_domain_unicode = root_domain_ascii;
        }
        std::string plus_addressing_mode = column_text_or_default(row, 9, "keep");
        if (plus_addressing_mode.empty()) {
            plus_addressing_mode = "keep";
        }
        std::string dns_status = column_text_or_default(row, 13, "unknown");
        if (dns_status.empty()) {
            dns_status = "unknown";
        }

        domain_policies.emplace(domain_id,
                                DomainPolicySnapshot{
                                    .root_domain_unicode = std::move(root_domain_unicode),
                                    .accept_exact = column_int_or_default(row, 3, 1) != 0,
                                    .accept_subdomains = column_int_or_default(row, 4, 1) != 0,
                                    .public_web_enabled = column_int_or_default(row, 5, 1) != 0,
                                    .public_api_enabled = column_int_or_default(row, 6, 1) != 0,
                                    .is_active = column_int_or_default(row, 7, 1) != 0,
                                    .is_hidden = column_int_or_default(row, 8, 0) != 0,
                                    .plus_addressing_mode = plus_addressing_mode,
                                    .local_part_case_sensitive =
                                        column_int_or_default(row, 10, 0) != 0,
                                    .max_message_size_bytes =
                                        column_int64_or_default(row, 11, 52428800),
                                    .retention_days = sqlite3_column_type(row, 12) == SQLITE_NULL
                                                           ? std::optional<int>{}
                                                           : std::optional<int>{
                                                                 sqlite3_column_int(row, 12)},
                                    .dns_status = std::move(dns_status),
                                });

        rules.push_back(DomainRule{
            .domain_id = domain_id,
            .root_domain_ascii = std::move(root_domain_ascii),
            .accept_exact = column_int_or_default(row, 3, 1) != 0,
            .accept_subdomains = column_int_or_default(row, 4, 1) != 0,
            .plus_addressing_mode = std::move(plus_addressing_mode),
            .local_part_case_sensitive = column_int_or_default(row, 10, 0) != 0,
        });
    }

    DomainMatcher next_matcher(std::move(rules));
    const std::lock_guard lock(mutex_);
    const std::uint64_t next_generation = generation_.load(std::memory_order_relaxed) + 1;
    auto next_rules = std::make_shared<const DomainRulesSnapshot>(DomainRulesSnapshot{
        .matcher = std::move(next_matcher),
        .policies = std::move(domain_policies),
        .generation = next_generation,
    });
    // Publish the immutable snapshot before its generation. An acquire load of
    // the new generation therefore guarantees the matching snapshot is visible.
    rules_.store(std::move(next_rules), std::memory_order_release);
    generation_.store(next_generation, std::memory_order_release);
}

std::optional<DomainMatch> DomainCache::match_address(const std::string& address) const {
    return rules_.load(std::memory_order_acquire)->matcher.match_address(address);
}

std::shared_ptr<const DomainRulesSnapshot> DomainCache::snapshot_rules() const noexcept {
    return rules_.load(std::memory_order_acquire);
}

std::shared_ptr<const DomainRulesSnapshot> DomainCache::snapshot_rules_if_changed(
    std::uint64_t known_generation) const noexcept {
    if (generation_.load(std::memory_order_acquire) == known_generation) {
        return nullptr;
    }
    auto snapshot = rules_.load(std::memory_order_acquire);
    if (snapshot->generation == known_generation) {
        return nullptr;
    }
    return snapshot;
}

std::uint64_t DomainCache::generation() const noexcept {
    return generation_.load(std::memory_order_acquire);
}

DomainMatcher DomainCache::snapshot_matcher() const {
    return rules_.load(std::memory_order_acquire)->matcher;
}

std::unordered_map<int, DomainPolicySnapshot> DomainCache::snapshot_policies() const {
    return rules_.load(std::memory_order_acquire)->policies;
}

}  // namespace rapid_inbox::ingestd
