#pragma once

#include "domain_matcher.h"
#include "mail_job.h"

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace rapid_inbox::ingestd {

struct DomainRulesSnapshot {
    DomainMatcher matcher;
    std::unordered_map<int, DomainPolicySnapshot> policies;
    std::uint64_t generation;
};

class DomainCache {
public:
    DomainCache(std::filesystem::path database_path, int busy_timeout_ms);

    DomainCache(const DomainCache&) = delete;
    DomainCache& operator=(const DomainCache&) = delete;

    void reload();
    std::optional<DomainMatch> match_address(const std::string& address) const;
    std::shared_ptr<const DomainRulesSnapshot> snapshot_rules() const noexcept;
    std::shared_ptr<const DomainRulesSnapshot> snapshot_rules_if_changed(
        std::uint64_t known_generation) const noexcept;
    std::uint64_t generation() const noexcept;
    DomainMatcher snapshot_matcher() const;
    std::unordered_map<int, DomainPolicySnapshot> snapshot_policies() const;

private:
    std::filesystem::path database_path_;
    int busy_timeout_ms_;
    mutable std::mutex mutex_;
    std::atomic<std::shared_ptr<const DomainRulesSnapshot>> rules_;
    std::atomic<std::uint64_t> generation_{0};
};

}  // namespace rapid_inbox::ingestd
