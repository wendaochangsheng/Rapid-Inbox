#include "../src/config.h"
#include "../src/domain_matcher.h"
#include "../src/ingest_app.h"
#include "../src/mail_job.h"
#include "../src/sha256.h"
#include "../src/sqlite_db.h"
#include "../src/storage_path.h"

#include <filesystem>
#include <fstream>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include <sqlite3.h>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

namespace fs = std::filesystem;

rapid_inbox::ingestd::MailJob make_job(const std::string& message_id, int domain_id) {
    using namespace rapid_inbox::ingestd;
    MailJob job;
    job.smtp_session_id = "smtp_" + message_id;
    job.remote_ip = "192.0.2.10";
    job.message_id = message_id;
    job.envelope_from = "sender@example.test";
    job.received_at = "2026-05-12T03:04:05Z";
    job.raw_content = "Subject: " + message_id + "\r\n\r\nbody\r\n";
    job.raw_sha256 = sha256_hex(job.raw_content);
    job.raw_path = raw_message_path(job.message_id, job.received_at);
    job.manifest_path = manifest_path(job.message_id, job.received_at);

    DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    DomainMatch match{domain_id, "adb.com", "adb.com", "code", "code", "code@adb.com"};
    job.recipients.push_back(
        RecipientDelivery{"dlv_" + message_id, "code@adb.com", match, policy});
    return job;
}

void initialize_test_database(const fs::path& database_path) {
    using namespace rapid_inbox::ingestd;
    SqliteDb db(database_path, 5000);
    const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
    std::ifstream schema(schema_path);
    const std::string sql((std::istreambuf_iterator<char>(schema)),
                          std::istreambuf_iterator<char>());
    db.exec(sql);
    db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
            "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
            "'2026-05-12T03:04:05Z')");
}

std::string read_file(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}

bool wait_for_condition(const std::function<bool()>& condition,
                        std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (condition()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return condition();
}

std::int64_t rejected_metric_total(const fs::path& database_path) {
    rapid_inbox::ingestd::SqliteDb db(database_path, 5000);
    auto statement = db.prepare("SELECT COALESCE(SUM(rejected), 0) FROM mail_metric_buckets");
    if (!statement.step_row()) {
        throw std::runtime_error("rejected metric aggregate returned no row");
    }
    return sqlite3_column_int64(statement.get(), 0);
}

}  // namespace

void test_ingest_app_isolates_poison_message_and_drains() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-ingest-app-isolation";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_test_database(database_path);

    Config config;
    config.storage_root = root;
    config.database_path = database_path;
    config.ingest_queue_max_messages = 10;
    config.ingest_queue_max_bytes = 1024 * 1024;
    config.ingest_batch_max_messages = 2;
    config.ingest_flush_interval_ms = 1;
    config.ingest_worker_count = 2;
    config.ingest_max_retries = 0;
    config.domain_reload_interval_ms = 50;

    IngestApp app(config);
    app.start_writer();
    test::check(app.queue().try_push(make_job("msg_good", 1)), "good job queued");
    MailJob poison_job = make_job("msg_poison", 1);
    poison_job.recipients.front().domain_policy.reset();
    test::check(app.queue().try_push(std::move(poison_job)), "poison job queued");
    app.stop_and_drain();

    {
        SqliteDb db(database_path, 5000);
        auto good = db.prepare("SELECT 1 FROM messages WHERE id = 'msg_good'");
        auto poison = db.prepare("SELECT 1 FROM messages WHERE id = 'msg_poison'");
        test::check(good.step_row(), "good message survives failed peer in original batch");
        test::check(!poison.step_row(), "poison message is not partially committed");
    }
    test::check(fs::is_regular_file(root / "quarantine/2026/05/12/msg_poison.error.json"),
                "poison message receives quarantine record");
    fs::remove_all(root);
}

void test_ingest_app_hot_reloads_domains() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-ingest-app-domain-reload";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_test_database(database_path);

    Config config;
    config.storage_root = root;
    config.database_path = database_path;
    config.ingest_worker_count = 1;
    config.domain_reload_interval_ms = 50;
    IngestApp app(config);
    app.start_writer();
    test::check(!app.domains().match_address("code@new.example").has_value(),
                "new domain absent before database update");

    {
        SqliteDb db(database_path, 5000);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (2, 'new.example', 'new.example', "
                "'2026-05-12T03:04:05Z', '2026-05-12T03:04:05Z')");
    }

    bool reloaded = false;
    for (int attempt = 0; attempt < 20; ++attempt) {
        if (app.domains().match_address("code@new.example").has_value()) {
            reloaded = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    app.stop_and_drain();
    test::check(reloaded, "domain cache hot reload observes newly added domain");
    fs::remove_all(root);
}

void test_ingest_app_enforces_storage_root_singleton_lifecycle() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-ingest-app-singleton";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_test_database(database_path);

    Config config;
    config.storage_root = root;
    config.database_path = database_path;
    config.ingest_worker_count = 1;
    config.domain_reload_interval_ms = 50;
    IngestApp first(config);
    IngestApp second(config);
    first.start_writer();

    bool rejected = false;
    try {
        second.start_writer();
    } catch (const std::runtime_error& exc) {
        rejected = std::string(exc.what()).find("only one ingestd process") != std::string::npos;
    }
    test::check(rejected, "second ingest app fails clearly before touching shared heartbeat");
    test::check(fs::is_regular_file(root / ".ingestd.status.json"),
                "rejected ingest app leaves active owner's heartbeat intact");

    first.stop_and_drain();
    second.start_writer();
    test::check(fs::is_regular_file(root / ".ingestd.status.json"),
                "successor starts after first app releases singleton lock");
    second.stop_and_drain();
    test::check(!fs::exists(root / ".ingestd.status.json"),
                "successor removes its heartbeat before releasing singleton lock");
    fs::remove_all(root);
}

void test_ingest_app_retries_rejected_metric_flush_without_loss() {
    using namespace rapid_inbox::ingestd;
    const fs::path root =
        fs::temp_directory_path() / "rapid-inbox-ingest-app-rejected-metric-retry";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_test_database(database_path);
    {
        SqliteDb db(database_path, 5000);
        db.exec("CREATE TRIGGER fail_rejected_metric_insert "
                "BEFORE INSERT ON mail_metric_buckets BEGIN "
                "SELECT RAISE(ABORT, 'forced rejected metric failure'); END");
    }

    Config config;
    config.storage_root = root;
    config.database_path = database_path;
    config.ingest_worker_count = 1;
    config.domain_reload_interval_ms = 50;
    auto runtime_stats = std::make_shared<IngestRuntimeStats>();
    IngestApp app(config, runtime_stats);
    app.start_writer();

    runtime_stats->rejected_recipients_pending.fetch_add(3, std::memory_order_relaxed);
    test::check(
        wait_for_condition(
            [&] { return app.durable_writer().sqlite_stats().connections_opened > 0; },
            std::chrono::seconds(2)),
        "status thread attempts rejected metric persistence");
    test::check(
        wait_for_condition(
            [&] {
                return runtime_stats->rejected_recipients_pending.load(
                           std::memory_order_acquire) == 3;
            },
            std::chrono::seconds(2)),
        "failed rejected metric write restores the complete pending count");
    test::check(rejected_metric_total(database_path) == 0,
                "failed rejected metric transaction writes no partial count");

    {
        SqliteDb db(database_path, 5000);
        db.exec("DROP TRIGGER fail_rejected_metric_insert");
    }
    test::check(
        wait_for_condition(
            [&] {
                return runtime_stats->rejected_recipients_pending.load(
                           std::memory_order_acquire) == 0 &&
                       rejected_metric_total(database_path) == 3;
            },
            std::chrono::seconds(2)),
        "restored pending rejection count is retried and persisted exactly once");

    runtime_stats->rejected_recipients_pending.fetch_add(2, std::memory_order_relaxed);
    app.stop_and_drain();
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_acquire) == 0,
                "graceful shutdown drains the final pending rejection snapshot");
    test::check(rejected_metric_total(database_path) == 5,
                "shutdown/status flush interleaving persists each rejection exactly once");
    fs::remove_all(root);
}

void test_ingest_app_heartbeat_and_token_bound_drain_ack() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-ingest-app-maintenance";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_test_database(database_path);

    Config config;
    config.storage_root = root;
    config.database_path = database_path;
    config.ingest_worker_count = 1;
    config.domain_reload_interval_ms = 50;
    config.smtp_max_connections = 321;
    auto runtime_stats = std::make_shared<IngestRuntimeStats>();
    IngestApp app(config, runtime_stats);
    app.start_writer();

    const fs::path status_path = root / ".ingestd.status.json";
    const fs::path drained_path = root / ".maintenance.drained.json";
    test::check(fs::is_regular_file(status_path),
                "first ingest status is published synchronously before start returns");
    const std::string initial_status = read_file(status_path);
    test::check(initial_status.find("\"pid\":") != std::string::npos,
                "status includes process id");
    test::check(initial_status.find("\"queue_messages\":0") != std::string::npos,
                "status includes queue message count");
    test::check(initial_status.find("\"queue_bytes\":0") != std::string::npos,
                "status includes queue byte count");
    test::check(initial_status.find("\"active_connections\":0") != std::string::npos,
                "status includes active SMTP connection count");
    test::check(initial_status.find("\"max_connections\":321") != std::string::npos,
                "status includes configured SMTP connection limit");

    runtime_stats->active_connections.store(2, std::memory_order_release);
    test::check(
        wait_for_condition(
            [&] {
                return read_file(status_path).find("\"active_connections\":2") !=
                       std::string::npos;
            },
            std::chrono::seconds(2)),
        "heartbeat publishes current active SMTP connections");
    runtime_stats->active_connections.store(0, std::memory_order_release);

    test::check(app.queue().try_push(make_job("maintenance_session_seed", 1)),
                "maintenance test seeds persistent SQLite session");
    test::check(
        wait_for_condition(
            [&] { return app.queue().total_size() == 0; },
            std::chrono::seconds(2)),
        "maintenance seed batch drains");
    test::check(app.durable_writer().sqlite_stats().connection_active,
                "writer keeps SQLite session active between ordinary batches");

    test::check(app.queue().try_reserve(128), "test reserves an in-progress SMTP transaction");
    {
        std::ofstream lock(root / ".maintenance.lock", std::ios::binary | std::ios::trunc);
        lock << "{\"operation\":\"clear-mail\",\"token\":\"token_test_123\"}";
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    test::check(!fs::exists(drained_path), "reservation prevents premature drained ack");

    runtime_stats->rejected_recipients_pending.fetch_add(4, std::memory_order_relaxed);
    app.queue().cancel_reservation(128);
    test::check(
        wait_for_condition(
            [&] {
                return fs::is_regular_file(drained_path) &&
                       read_file(drained_path).find("\"token\":\"token_test_123\"") !=
                           std::string::npos;
            },
            std::chrono::seconds(2)),
        "drained ack binds the maintenance token after queue reaches zero");
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_acquire) == 0,
                "new maintenance token flushes the pre-ack rejection snapshot");
    test::check(rejected_metric_total(database_path) == 4,
                "pre-ack rejected metrics cannot reappear after clear-all");
    const BatchWriterSqliteStats drained_stats = app.durable_writer().sqlite_stats();
    test::check(!drained_stats.connection_active,
                "maintenance acknowledgement is published only after SQLite session closes");
    test::check(drained_stats.connections_opened == 1,
                "maintenance close does not create a replacement connection prematurely");
    {
        SqliteDb maintenance_db(database_path, 5000);
        maintenance_db.exec("VACUUM");
    }
    test::check(!app.durable_writer().sqlite_stats().connection_active,
                "external maintenance can compact SQLite without reopening ingest writer");
    test::check(!app.queue().try_reserve(1),
                "drained queue remains closed to new reservations while lock exists");

    runtime_stats->rejected_recipients_pending.fetch_add(5, std::memory_order_relaxed);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_acquire) == 5,
                "maintenance defers rejected metric persistence without dropping the count");
    test::check(!app.durable_writer().sqlite_stats().connection_active,
                "deferred rejected metrics do not reopen SQLite during maintenance");
    test::check(rejected_metric_total(database_path) == 4,
                "post-ack maintenance leaves rejected metric buckets untouched");

    fs::remove(root / ".maintenance.lock");
    test::check(
        wait_for_condition(
            [&] {
                if (!app.queue().try_reserve(1)) {
                    return false;
                }
                app.queue().cancel_reservation(1);
                return true;
            },
            std::chrono::seconds(2)),
        "queue accepts reservations again after maintenance lock removal");

    test::check(
        wait_for_condition(
            [&] {
                return runtime_stats->rejected_recipients_pending.load(
                           std::memory_order_acquire) == 0 &&
                       rejected_metric_total(database_path) == 9;
            },
            std::chrono::seconds(2)),
        "deferred rejected metrics flush after maintenance ends");

    test::check(app.queue().try_push(make_job("maintenance_session_reopen", 1)),
                "writer accepts a batch after maintenance");
    test::check(
        wait_for_condition(
            [&] { return app.queue().total_size() == 0; },
            std::chrono::seconds(2)),
        "post-maintenance batch drains");
    const BatchWriterSqliteStats reopened_stats = app.durable_writer().sqlite_stats();
    test::check(reopened_stats.connection_active && reopened_stats.connections_opened == 2,
                "post-maintenance metric flush opens one fresh SQLite session reused by batch");

    app.stop_and_drain();
    test::check(!fs::exists(status_path), "graceful stop removes its heartbeat");
    fs::remove_all(root);
}
