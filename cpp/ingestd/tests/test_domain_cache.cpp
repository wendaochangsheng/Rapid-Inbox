#include "../src/domain_cache.h"
#include "../src/sqlite_db.h"

#include <filesystem>
#include <stdexcept>
#include <string>

namespace test {
inline void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
}

namespace {

namespace fs = std::filesystem;

using rapid_inbox::ingestd::DomainCache;
using rapid_inbox::ingestd::DomainMatch;
using rapid_inbox::ingestd::SqliteDb;

void remove_db_files(const fs::path& db_path) {
    fs::remove(db_path);
    fs::remove(db_path.string() + "-wal");
    fs::remove(db_path.string() + "-shm");
}

fs::path fresh_db_path(const std::string& filename) {
    const fs::path db_path = fs::temp_directory_path() / filename;
    remove_db_files(db_path);
    return db_path;
}

void create_domains_table(SqliteDb& db) {
    db.exec("CREATE TABLE domains (id INTEGER PRIMARY KEY, root_domain_ascii TEXT, "
            "root_domain_unicode TEXT, accept_exact INTEGER DEFAULT 1, "
            "accept_subdomains INTEGER DEFAULT 1, public_web_enabled INTEGER DEFAULT 1, "
            "public_api_enabled INTEGER DEFAULT 1, is_active INTEGER DEFAULT 1, "
            "is_hidden INTEGER DEFAULT 0, plus_addressing_mode TEXT DEFAULT 'keep', "
            "local_part_case_sensitive INTEGER DEFAULT 0, "
            "max_message_size_bytes INTEGER DEFAULT 52428800, retention_days INTEGER, "
            "dns_status TEXT DEFAULT 'unknown')");
}

DomainMatch require_match(DomainCache& cache,
                          const std::string& address,
                          const std::string& message) {
    auto match = cache.match_address(address);
    test::check(match.has_value(), message);
    return *match;
}

}  // namespace

void test_domain_cache_loads_active_rules() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (1, 'adb.com', 1, 1, 'keep', 0, 1)");
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (2, 'disabled.com', 1, 1, 'keep', 0, 0)");

    DomainCache cache(db_path, 5000);
    cache.reload();

    auto allowed = cache.match_address("A@adb.com");
    test::check(allowed.has_value(), "active domain matches");
    test::check(allowed->address_canonical == "a@adb.com", "active domain canonical");
    auto disabled = cache.match_address("a@disabled.com");
    test::check(!disabled.has_value(), "inactive domain skipped");
}

void test_domain_cache_respects_matcher_flags() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache-flags.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (1, 'exact-off.test', 0, 1, 'keep', 0, 1)");
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (2, 'sub-off.test', 1, 0, 'keep', 0, 1)");

    DomainCache cache(db_path, 5000);
    cache.reload();

    test::check(!cache.match_address("a@exact-off.test").has_value(),
                "accept_exact false rejects exact domain");
    test::check(cache.match_address("a@sub.exact-off.test").has_value(),
                "accept_subdomains true still accepts subdomain");
    test::check(cache.match_address("a@sub-off.test").has_value(),
                "accept_exact true accepts exact domain");
    test::check(!cache.match_address("a@sub.sub-off.test").has_value(),
                "accept_subdomains false rejects subdomain");
}

void test_domain_cache_loads_plus_and_case_modes() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache-plus-case.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (1, 'strip-case.test', 1, 1, 'strip', 1, 1)");

    DomainCache cache(db_path, 5000);
    cache.reload();

    const DomainMatch match =
        require_match(cache, "User+Tag@strip-case.test", "strip/case domain matches");
    test::check(match.local_part == "User+Tag", "original local part preserved");
    test::check(match.local_part_canonical == "User", "plus tag stripped with case preserved");
    test::check(match.address_canonical == "User@strip-case.test",
                "canonical address strips plus and preserves case");
}

void test_domain_cache_reload_sees_rule_changes() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache-reload.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (1, 'reload-active.test', 1, 1, 'keep', 0, 1)");
    db.exec("INSERT INTO domains (id, root_domain_ascii, accept_exact, accept_subdomains, "
            "plus_addressing_mode, local_part_case_sensitive, is_active) "
            "VALUES (2, 'reload-toggle.test', 1, 1, 'strip', 1, 0)");

    DomainCache cache(db_path, 5000);
    cache.reload();
    const DomainMatch initial_match =
        require_match(cache, "User+Tag@reload-active.test", "initial active rule matches");
    test::check(initial_match.address_canonical == "user+tag@reload-active.test",
                "initial active rule uses first loaded matcher");
    test::check(!cache.match_address("User+Tag@reload-toggle.test").has_value(),
                "initial inactive rule is absent");

    db.exec("UPDATE domains SET is_active = 1 WHERE id = 2");
    cache.reload();

    const DomainMatch match =
        require_match(cache, "User+Tag@reload-toggle.test", "reload loads newly active rule");
    test::check(match.address_canonical == "User@reload-toggle.test",
                "reload sees updated rule flags");
}

void test_domain_cache_snapshots_domain_policies() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache-policy.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, accept_exact, "
            "accept_subdomains, public_web_enabled, public_api_enabled, is_active, is_hidden, "
            "plus_addressing_mode, local_part_case_sensitive, max_message_size_bytes, "
            "retention_days, dns_status) VALUES (1, 'policy.test', 'Policy.Test', 1, 0, "
            "0, 1, 1, 1, 'strip', 1, 12345, 9, 'warning')");
    db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, is_active) "
            "VALUES (2, 'inactive.test', 'Inactive.Test', 0)");

    DomainCache cache(db_path, 5000);
    cache.reload();

    const auto policies = cache.snapshot_policies();
    test::check(policies.size() == 1, "active policies only");
    const auto found = policies.find(1);
    test::check(found != policies.end(), "policy found by domain id");
    test::check(found->second.root_domain_unicode == "Policy.Test", "policy root unicode");
    test::check(found->second.accept_exact == true, "policy accept exact");
    test::check(found->second.accept_subdomains == false, "policy accept subdomains");
    test::check(found->second.public_web_enabled == false, "policy public web");
    test::check(found->second.public_api_enabled == true, "policy public api");
    test::check(found->second.is_hidden == true, "policy hidden");
    test::check(found->second.plus_addressing_mode == "strip", "policy plus mode");
    test::check(found->second.local_part_case_sensitive == true, "policy case mode");
    test::check(found->second.max_message_size_bytes == 12345, "policy max size");
    test::check(found->second.retention_days.has_value() && *found->second.retention_days == 9,
                "policy retention");
    test::check(found->second.dns_status == "warning", "policy dns status");
}

void test_domain_cache_snapshots_matcher_and_policies_together() {
    const fs::path db_path = fresh_db_path("rapid-inbox-domain-cache-rules-snapshot.sqlite");
    SqliteDb db(db_path, 5000);
    create_domains_table(db);
    db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, is_active, "
            "plus_addressing_mode) VALUES (1, 'snapshot.test', 'Snapshot.Test', 1, 'strip')");

    DomainCache cache(db_path, 5000);
    cache.reload();

    const auto snapshot = cache.snapshot_rules();
    test::check(snapshot->generation == cache.generation(), "snapshot exposes cache generation");
    test::check(cache.snapshot_rules_if_changed(snapshot->generation) == nullptr,
                "unchanged generation avoids loading another shared snapshot");
    const auto match = snapshot->matcher.match_address("Code+Tag@snapshot.test");
    test::check(match.has_value(), "snapshot matcher matches loaded domain");
    test::check(match->domain_id == 1, "snapshot matcher domain id");
    test::check(match->address_canonical == "code@snapshot.test", "snapshot matcher flags");
    const auto policy = snapshot->policies.find(match->domain_id);
    test::check(policy != snapshot->policies.end(), "snapshot policy matches matcher domain id");
    test::check(policy->second.root_domain_unicode == "Snapshot.Test",
                "snapshot policy from same rules snapshot");
    test::check(policy->second.plus_addressing_mode == "strip",
                "snapshot policy keeps same rule fields");

    db.exec("UPDATE domains SET is_active = 0 WHERE id = 1");
    cache.reload();
    const auto refreshed = cache.snapshot_rules_if_changed(snapshot->generation);
    test::check(refreshed != nullptr, "reload publishes a new generation");
    test::check(refreshed->generation > snapshot->generation, "generation increases after reload");
    test::check(!refreshed->matcher.match_address("Code@snapshot.test").has_value(),
                "new snapshot sees disabled rule");
    test::check(snapshot->matcher.match_address("Code@snapshot.test").has_value(),
                "old immutable snapshot remains valid for in-flight work");
}
