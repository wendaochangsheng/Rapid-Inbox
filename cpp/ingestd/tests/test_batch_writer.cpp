#include "../src/batch_writer.h"
#include "../src/mail_job.h"
#include "../src/sqlite_db.h"
#include "../src/storage_path.h"

#include <sqlite3.h>

#include <exception>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

namespace fs = std::filesystem;

rapid_inbox::ingestd::DomainPolicySnapshot sample_policy() {
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.example";
    policy.accept_exact = false;
    policy.accept_subdomains = true;
    policy.public_web_enabled = false;
    policy.public_api_enabled = true;
    policy.is_active = true;
    policy.is_hidden = true;
    policy.plus_addressing_mode = "strip";
    policy.local_part_case_sensitive = true;
    policy.max_message_size_bytes = 12345;
    policy.retention_days = 7;
    policy.dns_status = "warning";
    return policy;
}

rapid_inbox::ingestd::MailJob sample_job() {
    rapid_inbox::ingestd::MailJob job;
    job.smtp_session_id = "smtp_1";
    job.message_id = "msg_1";
    job.envelope_from = "sender@example.com";
    job.received_at = "2026-05-12T03:04:05Z";
    job.raw_content =
        "From: Sender <sender@example.com>\r\n"
        "To: code@adb.com\r\n"
        "Subject: Hello\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Your verification code is 123456.\r\n";
    job.raw_sha256 = "digest";
    job.raw_path = rapid_inbox::ingestd::raw_message_path(job.message_id, job.received_at);
    job.manifest_path = rapid_inbox::ingestd::manifest_path(job.message_id, job.received_at);
    rapid_inbox::ingestd::DomainMatch match{1, "adb.com", "adb.com", "code", "code", "code@adb.com"};
    job.recipients.push_back({"dlv_1", "code@adb.com", match, sample_policy()});
    return job;
}

void initialize_schema(rapid_inbox::ingestd::SqliteDb& db) {
    const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
    std::ifstream schema(schema_path);
    std::string sql((std::istreambuf_iterator<char>(schema)),
                    std::istreambuf_iterator<char>());
    db.exec(sql);
}

rapid_inbox::ingestd::MailJob mailbox_job(
    const std::string& message_id,
    const std::string& delivery_id,
    const std::string& session_id,
    const std::string& received_at,
    rapid_inbox::ingestd::DomainMatch match,
    const std::string& rcpt_to) {
    rapid_inbox::ingestd::MailJob job = sample_job();
    job.message_id = message_id;
    job.smtp_session_id = session_id;
    job.received_at = received_at;
    job.raw_path = rapid_inbox::ingestd::raw_message_path(message_id, received_at);
    job.manifest_path = rapid_inbox::ingestd::manifest_path(message_id, received_at);
    job.recipients.clear();
    job.recipients.push_back(
        {delivery_id, rcpt_to, std::move(match), sample_policy()});
    return job;
}

std::string read_text_file(const fs::path& path) {
    std::ifstream input(path);
    return std::string((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
}

void write_text_file(const fs::path& path, const std::string& content) {
    std::ofstream output(path, std::ios::binary);
    output << content;
}

fs::path old_style_part_path(const fs::path& final_path) {
    return final_path.parent_path() / ("." + final_path.filename().string() + ".part");
}

void check_private_permissions(const fs::path& path,
                               fs::perms expected,
                               const std::string& message) {
    constexpr fs::perms mask = fs::perms::owner_all | fs::perms::group_all | fs::perms::others_all;
    const fs::perms actual = fs::status(path).permissions() & mask;
    test::check(actual == expected, message);
}

}  // namespace

void test_batch_writer_writes_raw_and_manifest() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-storage";
    fs::remove_all(root);
    const fs::path db_path = root / "app.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();
    writer.write_storage_artifacts({job});
    const fs::path raw = root / job.raw_path;
    const fs::path manifest = root / job.manifest_path;
    test::check(fs::exists(raw), "raw file exists");
    test::check(fs::exists(manifest), "manifest file exists");
    const std::string raw_content = read_text_file(raw);
    test::check(raw_content == job.raw_content, "raw content");
    const std::string manifest_content = read_text_file(manifest);
    test::check(manifest_content.find("\"message_id\":\"msg_1\"") != std::string::npos,
                "manifest message id");
    test::check(manifest_content.find("\"rcpt_to\":\"code@adb.com\"") != std::string::npos,
                "manifest recipient");
}

void test_batch_writer_writes_quarantine_error_record() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-quarantine";
    fs::remove_all(root);
    rapid_inbox::ingestd::BatchWriter writer(root, root / "unused.db", 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();

    writer.write_quarantine_record(job, "permanent failure", 3);

    const fs::path record = root / "quarantine/2026/05/12/msg_1.error.json";
    test::check(fs::is_regular_file(record), "quarantine record exists");
    const std::string content = read_text_file(record);
    test::check(content.find("\"attempts\":3") != std::string::npos,
                "quarantine record includes attempts");
    test::check(content.find("permanent failure") != std::string::npos,
                "quarantine record includes error");
}

void test_batch_writer_writes_private_storage_permissions() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-permissions";
    fs::remove_all(root);
    const fs::path db_path = root / "app.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();

    writer.write_storage_artifacts({job});

    const fs::perms private_dir = fs::perms::owner_all;
    const fs::perms private_file = fs::perms::owner_read | fs::perms::owner_write;
    check_private_permissions(root, private_dir, "storage root is private");
    check_private_permissions(root / "raw", private_dir, "raw dir is private");
    check_private_permissions(root / "raw" / "2026", private_dir, "raw year dir is private");
    check_private_permissions(root / "raw" / "2026" / "05", private_dir,
                              "raw month dir is private");
    check_private_permissions(root / "raw" / "2026" / "05" / "12", private_dir,
                              "raw day dir is private");
    check_private_permissions(root / "manifests", private_dir, "manifest dir is private");
    check_private_permissions(root / "manifests" / "2026", private_dir,
                              "manifest year dir is private");
    check_private_permissions(root / "manifests" / "2026" / "05", private_dir,
                              "manifest month dir is private");
    check_private_permissions(root / "manifests" / "2026" / "05" / "12", private_dir,
                              "manifest day dir is private");
    check_private_permissions(root / job.raw_path, private_file, "raw file is private");
    check_private_permissions(root / job.manifest_path, private_file, "manifest file is private");
}

void test_batch_writer_manifest_includes_domain_policy_snapshot() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-domain-policy";
    fs::remove_all(root);
    const fs::path db_path = root / "app.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();

    writer.write_storage_artifacts({job});

    const std::string manifest_content = read_text_file(root / job.manifest_path);
    test::check(manifest_content.find("\"domain_policy\":{") != std::string::npos,
                "manifest includes domain policy object");
    test::check(manifest_content.find("\"root_domain_unicode\":\"adb.example\"") !=
                    std::string::npos,
                "manifest domain policy unicode root");
    test::check(manifest_content.find("\"accept_exact\":false") != std::string::npos,
                "manifest domain policy accept_exact");
    test::check(manifest_content.find("\"accept_subdomains\":true") != std::string::npos,
                "manifest domain policy accept_subdomains");
    test::check(manifest_content.find("\"public_web_enabled\":false") != std::string::npos,
                "manifest domain policy public_web_enabled");
    test::check(manifest_content.find("\"public_api_enabled\":true") != std::string::npos,
                "manifest domain policy public_api_enabled");
    test::check(manifest_content.find("\"is_active\":true") != std::string::npos,
                "manifest domain policy is_active");
    test::check(manifest_content.find("\"is_hidden\":true") != std::string::npos,
                "manifest domain policy is_hidden");
    test::check(manifest_content.find("\"plus_addressing_mode\":\"strip\"") !=
                    std::string::npos,
                "manifest domain policy plus mode");
    test::check(manifest_content.find("\"local_part_case_sensitive\":true") !=
                    std::string::npos,
                "manifest domain policy case sensitivity");
    test::check(manifest_content.find("\"max_message_size_bytes\":12345") !=
                    std::string::npos,
                "manifest domain policy max message size");
    test::check(manifest_content.find("\"retention_days\":7") != std::string::npos,
                "manifest domain policy retention");
    test::check(manifest_content.find("\"dns_status\":\"warning\"") != std::string::npos,
                "manifest domain policy dns status");
}

void test_batch_writer_missing_domain_policy_rejects_without_creating_database() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-missing-domain-policy";
    fs::remove_all(root);
    const fs::path db_path = root / "missing.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    rapid_inbox::ingestd::MailJob job = sample_job();
    job.recipients[0].domain_policy.reset();

    bool threw = false;
    try {
        writer.write_storage_artifacts({job});
    } catch (const std::runtime_error&) {
        threw = true;
    }

    test::check(threw, "missing domain policy rejects storage write");
    test::check(!fs::exists(db_path), "missing domain policy does not create db");
    test::check(!fs::exists(db_path.string() + "-wal"), "missing domain policy does not create wal");
    test::check(!fs::exists(db_path.string() + "-shm"), "missing domain policy does not create shm");
}

void test_batch_writer_uses_job_policy_without_touching_database() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-job-policy";
    fs::remove_all(root);
    const fs::path db_path = root / "missing.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();

    writer.write_storage_artifacts({job});

    test::check(fs::exists(root / job.raw_path), "raw file exists from job policy write");
    test::check(fs::exists(root / job.manifest_path), "manifest exists from job policy write");
    test::check(!fs::exists(db_path), "job policy write does not create db");
    test::check(!fs::exists(db_path.string() + "-wal"), "job policy write does not create wal");
    test::check(!fs::exists(db_path.string() + "-shm"), "job policy write does not create shm");
}

void test_batch_writer_ignores_preexisting_part_symlinks() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-part-symlink";
    fs::remove_all(root);
    const fs::path outside_raw = fs::temp_directory_path() / "rapid-inbox-writer-outside-raw.txt";
    const fs::path outside_manifest =
        fs::temp_directory_path() / "rapid-inbox-writer-outside-manifest.txt";
    write_text_file(outside_raw, "outside-safe");
    write_text_file(outside_manifest, "outside-safe");

    const fs::path db_path = root / "app.db";
    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();

    const fs::path raw_final = root / job.raw_path;
    const fs::path manifest_final = root / job.manifest_path;
    fs::create_directories(raw_final.parent_path());
    fs::create_directories(manifest_final.parent_path());
    fs::remove(old_style_part_path(raw_final));
    fs::remove(old_style_part_path(manifest_final));
    fs::create_symlink(outside_raw, old_style_part_path(raw_final));
    fs::create_symlink(outside_manifest, old_style_part_path(manifest_final));

    writer.write_storage_artifacts({job});

    test::check(read_text_file(outside_raw) == "outside-safe", "outside raw file unchanged");
    test::check(read_text_file(outside_manifest) == "outside-safe",
                "outside manifest file unchanged");
    test::check(read_text_file(raw_final) == job.raw_content, "raw file written correctly");
    const std::string manifest_content = read_text_file(manifest_final);
    test::check(manifest_content.find("\"message_id\":\"msg_1\"") != std::string::npos,
                "manifest written correctly");
}

void test_batch_writer_writes_sqlite_parsed_records() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-sqlite";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
        std::ifstream schema(schema_path);
        std::string sql((std::istreambuf_iterator<char>(schema)),
                        std::istreambuf_iterator<char>());
        db.exec(sql);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                "'2026-05-12T03:04:05Z')");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    const rapid_inbox::ingestd::MailJob job = sample_job();
    writer.write_batch({job});
    writer.write_batch({job});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto message = db.prepare(
        "SELECT parse_status, raw_path, envelope_from, subject, text_preview, "
        "text_body_path, html_body_path, verification_code "
        "FROM messages WHERE id = 'msg_1'");
    test::check(message.step_row(), "message row exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 0))) ==
                    "parsed",
                "message parsed");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 1))) ==
                    job.raw_path,
                "message raw path");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 2))) ==
                    "sender@example.com",
                "message envelope from");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 3))) ==
                    "Hello",
                "message subject");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 4)))
                        .find("Your verification code is 123456") == 0,
                "message preview");
    const unsigned char* text_body_path_text = sqlite3_column_text(message.get(), 5);
    test::check(text_body_path_text != nullptr, "message text body path exists");
    const std::string text_body_path_value =
        reinterpret_cast<const char*>(text_body_path_text);
    test::check(sqlite3_column_type(message.get(), 6) == SQLITE_NULL, "message html path null");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 7))) ==
                    "123456",
                "message verification code");
    test::check(fs::exists(root / text_body_path_value), "text body file exists");
    test::check(read_text_file(root / text_body_path_value).find("Your verification code is 123456") ==
                    0,
                "text body content");
    const std::string manifest_content = read_text_file(root / job.manifest_path);
    test::check(manifest_content.find("\"parsed\":{\"status\":\"parsed\"") != std::string::npos,
                "manifest parsed status");
    test::check(manifest_content.find("\"text_body_path\":\"" + text_body_path_value + "\"") !=
                    std::string::npos,
                "manifest text body path");
    test::check(manifest_content.find("\"verification_code\":\"123456\"") != std::string::npos,
                "manifest verification code");

    auto mailbox =
        db.prepare("SELECT message_count, address_canonical FROM mailboxes WHERE "
                   "address_canonical = 'code@adb.com'");
    test::check(mailbox.step_row(), "mailbox row exists");
    test::check(sqlite3_column_int(mailbox.get(), 0) == 1, "mailbox count");

    auto delivery = db.prepare(
        "SELECT id, rcpt_to, expires_at FROM message_deliveries WHERE message_id = 'msg_1'");
    test::check(delivery.step_row(), "delivery exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(delivery.get(), 0))) ==
                    "dlv_1",
                "delivery id");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(delivery.get(), 1))) ==
                    "code@adb.com",
                "delivery rcpt");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(delivery.get(), 2))) ==
                    "2026-05-19T03:04:05Z",
                "delivery expiry follows domain retention snapshot");

    auto session = db.prepare("SELECT remote_ip, status, message_count, bytes_received, "
                              "last_command_at FROM smtp_sessions WHERE id = 'smtp_1'");
    test::check(session.step_row(), "smtp session row exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(session.get(), 0))) ==
                    "unknown",
                "smtp remote ip");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(session.get(), 1))) ==
                    "closed",
                "smtp status");
    test::check(sqlite3_column_int(session.get(), 2) == 1, "smtp message count");
    test::check(sqlite3_column_int64(session.get(), 3) ==
                    static_cast<sqlite3_int64>(job.raw_content.size()),
                "smtp bytes received");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(session.get(), 4))) ==
                    job.received_at,
                "smtp last command at");

    auto metric = db.prepare("SELECT deliveries FROM mail_metric_buckets WHERE bucket_ts = "
                             "'2026-05-12T03:04:00Z'");
    test::check(metric.step_row(), "metric bucket exists");
    test::check(sqlite3_column_int(metric.get(), 0) == 1, "metric deliveries");
}

void test_batch_writer_marks_parse_failure_without_rejecting_raw() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-parse-failure";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
        std::ifstream schema(schema_path);
        std::string sql((std::istreambuf_iterator<char>(schema)),
                        std::istreambuf_iterator<char>());
        db.exec(sql);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                "'2026-05-12T03:04:05Z')");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    rapid_inbox::ingestd::MailJob job = sample_job();
    job.raw_content =
        "Subject: Broken\r\n"
        "Content-Type: multipart/mixed; boundary=\"missing\"\r\n"
        "\r\n"
        "body without boundary\r\n";
    writer.write_batch({job});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto message = db.prepare(
        "SELECT parse_status, parse_error, text_body_path, verification_code "
        "FROM messages WHERE id = 'msg_1'");
    test::check(message.step_row(), "failed message row exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(message.get(), 0))) ==
                    "failed",
                "message failed");
    test::check(sqlite3_column_text(message.get(), 1) != nullptr, "message parse error");
    test::check(sqlite3_column_type(message.get(), 2) == SQLITE_NULL, "failed text path null");
    test::check(sqlite3_column_type(message.get(), 3) == SQLITE_NULL,
                "failed verification code null");
    test::check(fs::exists(root / job.raw_path), "failed raw file exists");
    test::check(fs::exists(root / job.manifest_path), "failed manifest file exists");
    const std::string manifest_content = read_text_file(root / job.manifest_path);
    test::check(manifest_content.find("\"parsed\":{\"status\":\"failed\"") != std::string::npos,
                "failed manifest parsed status");
    test::check(manifest_content.find("\"parse_error\":\"invalid multipart boundary\"") !=
                    std::string::npos,
                "failed manifest parse error");
}

void test_batch_writer_aggregates_minute_metrics_idempotently() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-minute-metrics";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        initialize_schema(db);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:00Z', "
                "'2026-05-12T03:04:00Z')");
        db.exec("INSERT INTO mail_metric_buckets "
                "(bucket_ts, received, deliveries, parse_failures, rejected) "
                "VALUES ('2026-05-12T03:04:00Z', 0, 0, 0, 7)");
        db.exec("CREATE TABLE metric_write_observer (writes INTEGER NOT NULL)");
        db.exec("INSERT INTO metric_write_observer (writes) VALUES (0)");
        db.exec("CREATE TRIGGER observe_metric_update "
                "AFTER UPDATE ON mail_metric_buckets BEGIN "
                "UPDATE metric_write_observer SET writes = writes + 1; END");
    }

    rapid_inbox::ingestd::MailJob parsed_job = sample_job();
    rapid_inbox::ingestd::DomainMatch other_match{
        1, "adb.com", "adb.com", "other", "other", "other@adb.com"};
    parsed_job.recipients.push_back(
        {"dlv_2", "other@adb.com", std::move(other_match), sample_policy()});

    rapid_inbox::ingestd::MailJob failed_job = sample_job();
    failed_job.smtp_session_id = "smtp_2";
    failed_job.message_id = "msg_2";
    failed_job.received_at = "2026-05-12T03:04:59Z";
    failed_job.raw_path =
        rapid_inbox::ingestd::raw_message_path(failed_job.message_id, failed_job.received_at);
    failed_job.manifest_path =
        rapid_inbox::ingestd::manifest_path(failed_job.message_id, failed_job.received_at);
    failed_job.recipients[0].delivery_id = "dlv_3";
    failed_job.raw_content =
        "Subject: Broken\r\n"
        "Content-Type: multipart/mixed; boundary=\"missing\"\r\n"
        "\r\n"
        "body without boundary\r\n";

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    writer.write_batch({parsed_job, failed_job, failed_job});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto metric = db.prepare(
        "SELECT bucket_ts, received, deliveries, parse_failures, rejected "
        "FROM mail_metric_buckets");
    test::check(metric.step_row(), "minute metric bucket exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(metric.get(), 0))) ==
                    "2026-05-12T03:04:00Z",
                "metric timestamp is truncated to the minute");
    test::check(sqlite3_column_int(metric.get(), 1) == 2,
                "metric received counts only new messages");
    test::check(sqlite3_column_int(metric.get(), 2) == 3,
                "metric deliveries count persisted deliveries");
    test::check(sqlite3_column_int(metric.get(), 3) == 1,
                "metric parse failures count only new failed messages");
    test::check(sqlite3_column_int(metric.get(), 4) == 7,
                "metric upsert preserves rejected count");
    test::check(!metric.step_row(), "same-minute messages share one metric bucket");

    auto writes = db.prepare("SELECT writes FROM metric_write_observer");
    test::check(writes.step_row(), "metric write observer exists");
    test::check(sqlite3_column_int(writes.get(), 0) == 1,
                "batch writes each minute metric once");

    writer.write_batch({parsed_job, failed_job});

    auto repeated_metric = db.prepare(
        "SELECT received, deliveries, parse_failures, rejected FROM mail_metric_buckets");
    test::check(repeated_metric.step_row(), "metric bucket remains after duplicate replay");
    test::check(sqlite3_column_int(repeated_metric.get(), 0) == 2,
                "duplicate replay does not increment received");
    test::check(sqlite3_column_int(repeated_metric.get(), 1) == 3,
                "duplicate replay does not increment deliveries");
    test::check(sqlite3_column_int(repeated_metric.get(), 2) == 1,
                "duplicate replay does not increment parse failures");
    test::check(sqlite3_column_int(repeated_metric.get(), 3) == 7,
                "duplicate replay preserves rejected count");

    auto repeated_writes = db.prepare("SELECT writes FROM metric_write_observer");
    test::check(repeated_writes.step_row(), "metric observer remains after duplicate replay");
    test::check(sqlite3_column_int(repeated_writes.get(), 0) == 1,
                "duplicate-only batch does not write metrics");
}

void test_batch_writer_accumulates_rejected_metrics() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-rejected-metrics";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        initialize_schema(db);
        db.exec("INSERT INTO mail_metric_buckets "
                "(bucket_ts, received, deliveries, parse_failures, rejected) "
                "VALUES ('2026-05-12T03:04:00Z', 2, 3, 1, 7)");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    writer.write_rejected_metric("2026-05-12T03:04:01Z", 0);
    test::check(writer.sqlite_stats().connections_opened == 0,
                "zero rejected count is a connection-free no-op");

    writer.write_rejected_metric("2026-05-12T03:04:05Z", 2);
    writer.write_rejected_metric("2026-05-12T03:04:59Z", 3);
    const auto stats = writer.sqlite_stats();
    test::check(stats.connections_opened == 1,
                "rejected metric writes reuse the persistent sqlite session");
    test::check(stats.statement_sets_prepared == 1,
                "rejected metric writes reuse prepared statements");

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto metric = db.prepare(
        "SELECT bucket_ts, received, deliveries, parse_failures, rejected "
        "FROM mail_metric_buckets");
    test::check(metric.step_row(), "rejected metric bucket exists");
    test::check(std::string(reinterpret_cast<const char*>(sqlite3_column_text(metric.get(), 0))) ==
                    "2026-05-12T03:04:00Z",
                "rejected metric uses minute granularity");
    test::check(sqlite3_column_int(metric.get(), 1) == 2,
                "rejected metric preserves received count");
    test::check(sqlite3_column_int(metric.get(), 2) == 3,
                "rejected metric preserves delivery count");
    test::check(sqlite3_column_int(metric.get(), 3) == 1,
                "rejected metric preserves parse failure count");
    test::check(sqlite3_column_int(metric.get(), 4) == 12,
                "rejected metric accumulates counts");
    test::check(!metric.step_row(), "same-minute rejected writes share one bucket");
}

void test_batch_writer_writes_parsed_attachment_records() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-attachments";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
        std::ifstream schema(schema_path);
        std::string sql((std::istreambuf_iterator<char>(schema)),
                        std::istreambuf_iterator<char>());
        db.exec(sql);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                "'2026-05-12T03:04:05Z')");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    rapid_inbox::ingestd::MailJob job = sample_job();
    job.raw_content =
        "From: Sender <sender@example.com>\r\n"
        "To: code@adb.com\r\n"
        "Subject: Attachment\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: multipart/mixed; boundary=\"mixed-boundary\"\r\n"
        "\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "\r\n"
        "Body.\r\n"
        "--mixed-boundary\r\n"
        "Content-Type: text/plain; charset=utf-8\r\n"
        "Content-Disposition: attachment; filename=\"report.txt\"\r\n"
        "Content-Transfer-Encoding: base64\r\n"
        "\r\n"
        "UXVhcnRlcmx5IHJlcG9ydAo=\r\n"
        "--mixed-boundary--\r\n";
    writer.write_batch({job});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto message =
        db.prepare("SELECT has_attachments, attachment_count FROM messages WHERE id = 'msg_1'");
    test::check(message.step_row(), "attachment message row exists");
    test::check(sqlite3_column_int(message.get(), 0) == 1, "message has attachments");
    test::check(sqlite3_column_int(message.get(), 1) == 1, "message attachment count");

    auto attachment = db.prepare(
        "SELECT filename, safe_filename, content_type, storage_path, size_bytes "
        "FROM attachments WHERE message_id = 'msg_1'");
    test::check(attachment.step_row(), "attachment row exists");
    test::check(
        std::string(reinterpret_cast<const char*>(sqlite3_column_text(attachment.get(), 0))) ==
            "report.txt",
        "attachment filename");
    test::check(
        std::string(reinterpret_cast<const char*>(sqlite3_column_text(attachment.get(), 1))) ==
            "report.txt",
        "attachment safe filename");
    test::check(
        std::string(reinterpret_cast<const char*>(sqlite3_column_text(attachment.get(), 2))) ==
            "text/plain",
        "attachment content type");
    const std::string storage_path =
        reinterpret_cast<const char*>(sqlite3_column_text(attachment.get(), 3));
    test::check(sqlite3_column_int(attachment.get(), 4) == 17, "attachment size");
    test::check(fs::exists(root / storage_path), "attachment file exists");
    test::check(read_text_file(root / storage_path) == "Quarterly report\n",
                "attachment file content");
}

void test_batch_writer_upgrades_catch_all_mailbox_without_downgrade() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-mailbox-upgrade";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        initialize_schema(db);
        db.exec(
            "INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, "
            "plus_addressing_mode, local_part_case_sensitive, created_at, updated_at) VALUES "
            "(1, '*', '任意域名', 'keep', 0, '2026-01-01T00:00:00Z', "
            "'2026-01-01T00:00:00Z'), "
            "(2, 'managed.example', 'managed.example', 'keep', 0, "
            "'2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z'), "
            "(3, 'deep.managed.example', 'deep.managed.example', 'keep', 0, "
            "'2026-01-03T00:00:00Z', '2026-01-03T00:00:00Z');"
            "INSERT INTO mailboxes (id, domain_id, local_part_canonical, rcpt_domain_ascii, "
            "address_canonical, address_display, first_seen_at, last_seen_at, "
            "latest_message_at, message_count) VALUES "
            "(10, 1, 'code', 'managed.example', 'code@managed.example', "
            "'code@managed.example', '2026-01-03T00:00:00Z', '2026-01-04T00:00:00Z', "
            "'2026-01-04T00:00:00Z', 2), "
            "(11, 2, 'other', 'deep.managed.example', 'other@deep.managed.example', "
            "'other@deep.managed.example', '2026-01-05T00:00:00Z', "
            "'2026-01-05T00:00:00Z', NULL, 0);"
            "INSERT INTO messages (id, raw_path, raw_sha256, raw_size_bytes, received_at) VALUES "
            "('hist_upgrade_1', 'raw/hist_upgrade_1.eml', 'one', 1, "
            "'2026-01-03T00:00:00Z'), "
            "('hist_upgrade_2', 'raw/hist_upgrade_2.eml', 'two', 1, "
            "'2026-01-04T00:00:00Z');"
            "INSERT INTO message_deliveries (id, message_id, mailbox_id, rcpt_to, delivered_at) "
            "VALUES ('hist_upgrade_d1', 'hist_upgrade_1', 10, 'code@managed.example', "
            "'2026-01-03T00:00:00Z'), "
            "('hist_upgrade_d2', 'hist_upgrade_2', 10, 'code@managed.example', "
            "'2026-01-04T00:00:00Z');");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    writer.write_batch({mailbox_job(
        "managed_upgrade_new",
        "managed_upgrade_delivery",
        "managed_upgrade_session",
        "2026-05-12T03:04:05Z",
        rapid_inbox::ingestd::DomainMatch{
            2, "managed.example", "managed.example", "Code", "code", "code@managed.example"},
        "Code@managed.example")});

    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        auto mailbox = db.prepare(
            "SELECT id, domain_id, local_part_canonical, rcpt_domain_ascii, message_count "
            "FROM mailboxes WHERE address_canonical = 'code@managed.example'");
        test::check(mailbox.step_row(), "upgraded mailbox exists");
        test::check(sqlite3_column_int64(mailbox.get(), 0) == 10,
                    "catch-all mailbox upgraded in place");
        test::check(sqlite3_column_int(mailbox.get(), 1) == 2,
                    "catch-all mailbox assigned to managed domain");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(mailbox.get(), 2))) == "code",
                    "upgraded mailbox local part follows managed match");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(mailbox.get(), 3))) == "managed.example",
                    "upgraded mailbox domain part follows managed match");
        test::check(sqlite3_column_int(mailbox.get(), 4) == 3,
                    "upgraded mailbox preserves historical count");
        auto audit = db.prepare(
            "SELECT actor_ref, action, resource_ref, details_json FROM audit_logs "
            "WHERE action = 'mailboxes.rehome' ORDER BY id DESC LIMIT 1");
        test::check(audit.step_row(), "mailbox upgrade emits an audit row");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(audit.get(), 0))) == "smtp-ingest",
                    "mailbox upgrade audit identifies ingest actor");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(audit.get(), 1))) == "mailboxes.rehome",
                    "mailbox upgrade audit action");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(audit.get(), 2))) == "10",
                    "mailbox upgrade audit targets surviving mailbox");
        const std::string audit_details = reinterpret_cast<const char*>(
            sqlite3_column_text(audit.get(), 3));
        test::check(audit_details.find("\"destination_domain_id\":2") != std::string::npos,
                    "mailbox upgrade audit records destination domain");
        db.exec("UPDATE domains SET is_active = 0 WHERE id = 2");
    }

    writer.write_batch({mailbox_job(
        "managed_suffix_promotion",
        "managed_suffix_promotion_delivery",
        "managed_suffix_promotion_session",
        "2026-05-12T04:04:05Z",
        rapid_inbox::ingestd::DomainMatch{3,
                                         "deep.managed.example",
                                         "deep.managed.example",
                                         "other",
                                         "other",
                                         "other@deep.managed.example"},
        "other@deep.managed.example")});
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        auto promoted = db.prepare(
            "SELECT domain_id, message_count FROM mailboxes WHERE id = 11");
        test::check(promoted.step_row(), "managed suffix promotion mailbox remains");
        test::check(sqlite3_column_int(promoted.get(), 0) == 3,
                    "current more-specific managed winner replaces shorter suffix owner");
        test::check(sqlite3_column_int(promoted.get(), 1) == 1,
                    "managed suffix promotion keeps new delivery");
    }

    writer.write_batch({mailbox_job(
        "catch_all_after_managed",
        "catch_all_after_managed_delivery",
        "catch_all_after_managed_session",
        "2026-05-13T03:04:05Z",
        rapid_inbox::ingestd::DomainMatch{
            1, "managed.example", "*", "code", "code", "code@managed.example"},
        "code@managed.example")});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto mailbox = db.prepare(
        "SELECT domain_id, message_count FROM mailboxes "
        "WHERE address_canonical = 'code@managed.example'");
    test::check(mailbox.step_row(), "managed mailbox remains after stale catch-all write");
    test::check(sqlite3_column_int(mailbox.get(), 0) == 2,
                "managed mailbox is never downgraded to catch-all");
    test::check(sqlite3_column_int(mailbox.get(), 1) == 4,
                "stale catch-all delivery is retained on managed mailbox");
    auto delivery_count = db.prepare(
        "SELECT COUNT(*) FROM message_deliveries WHERE mailbox_id = 10");
    test::check(delivery_count.step_row(), "upgraded mailbox delivery count row");
    test::check(sqlite3_column_int(delivery_count.get(), 0) == 4,
                "upgraded mailbox keeps all historical deliveries");
}

void test_batch_writer_merges_catch_all_canonical_collision() {
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-mailbox-merge";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
        initialize_schema(db);
        db.exec(
            "INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, "
            "plus_addressing_mode, local_part_case_sensitive, retention_days, "
            "created_at, updated_at) VALUES "
            "(1, '*', '任意域名', 'keep', 0, 7, '2026-01-01T00:00:00Z', "
            "'2026-01-01T00:00:00Z'), "
            "(2, 'managed.example', 'managed.example', 'strip', 0, 30, "
            "'2026-01-02T00:00:00Z', '2026-01-02T00:00:00Z');"
            "INSERT INTO mailboxes (id, domain_id, local_part_canonical, rcpt_domain_ascii, "
            "address_canonical, address_display, first_seen_at, last_seen_at, "
            "latest_message_at, message_count, public_enabled, is_hidden, notes) VALUES "
            "(10, 1, 'foo+tag', 'managed.example', 'foo+tag@managed.example', "
            "'foo+tag@managed.example', '2026-01-01T00:00:00Z', "
            "'2026-01-05T00:00:00Z', '2026-01-05T00:00:00Z', 3, 0, 1, "
            "'source mailbox'), "
            "(20, 2, 'foo', 'managed.example', 'foo@managed.example', "
            "'foo@managed.example', '2026-01-02T00:00:00Z', "
            "'2026-01-04T00:00:00Z', '2026-01-02T00:00:00Z', 1, 1, 0, NULL);"
            "INSERT INTO messages (id, raw_path, raw_sha256, raw_size_bytes, received_at) VALUES "
            "('merge_source_only', 'raw/merge_source_only.eml', 'a', 1, "
            "'2026-01-01T00:00:00Z'), "
            "('merge_target_only', 'raw/merge_target_only.eml', 'b', 1, "
            "'2026-01-02T00:00:00Z'), "
            "('merge_overlap_later', 'raw/merge_overlap_later.eml', 'c', 1, "
            "'2026-01-03T00:00:00Z'), "
            "('merge_overlap_null', 'raw/merge_overlap_null.eml', 'd', 1, "
            "'2026-01-04T00:00:00Z');"
            "INSERT INTO message_deliveries "
            "(id, message_id, mailbox_id, rcpt_to, delivered_at, status, deleted_at, "
            "expires_at, notes) VALUES "
            "('source_only_delivery', 'merge_source_only', 10, "
            "'Foo+tag@managed.example', '2026-01-01T00:00:00Z', 'active', NULL, NULL, "
            "'source only'), "
            "('source_overlap_later', 'merge_overlap_later', 10, "
            "'Foo+tag@managed.example', '2026-01-03T00:00:00Z', 'active', NULL, "
            "'2026-04-01T00:00:00Z', 'source later note'), "
            "('source_overlap_null', 'merge_overlap_null', 10, "
            "'Foo+tag@managed.example', '2026-01-04T00:00:00Z', 'active', NULL, "
            "'2026-05-01T00:00:00Z', 'source null note'), "
            "('target_only_delivery', 'merge_target_only', 20, 'foo@managed.example', "
            "'2026-01-02T00:00:00Z', 'active', NULL, NULL, 'target only'), "
            "('target_overlap_later', 'merge_overlap_later', 20, 'foo@managed.example', "
            "'2026-01-04T00:00:00Z', 'hidden', NULL, '2026-03-01T00:00:00Z', "
            "'target later note'), "
            "('target_overlap_null', 'merge_overlap_null', 20, 'foo@managed.example', "
            "'2026-01-05T00:00:00Z', 'deleted', '2026-01-06T00:00:00Z', NULL, "
            "'target null note');"
            "UPDATE mailboxes SET bulk_delete_generation = 5 WHERE id = 20;");
    }

    rapid_inbox::ingestd::BatchWriter writer(root, db_path, 5000, false);
    writer.write_batch({mailbox_job(
        "merge_new_message",
        "merge_new_delivery",
        "merge_new_session",
        "2026-05-12T03:04:05Z",
        rapid_inbox::ingestd::DomainMatch{2,
                                         "managed.example",
                                         "managed.example",
                                         "Foo+tag",
                                         "foo",
                                         "foo@managed.example"},
        "Foo+tag@managed.example")});

    rapid_inbox::ingestd::MailJob collapsed_job = mailbox_job(
        "collapsed_stale_message",
        "collapsed_stale_first",
        "collapsed_stale_session",
        "2026-05-12T04:04:05Z",
        rapid_inbox::ingestd::DomainMatch{1,
                                         "managed.example",
                                         "*",
                                         "Foo+next",
                                         "foo+next",
                                         "foo+next@managed.example"},
        "Foo+next@managed.example");
    collapsed_job.recipients.push_back(
        {"collapsed_stale_second",
         "foo@managed.example",
         rapid_inbox::ingestd::DomainMatch{
             1, "managed.example", "*", "foo", "foo", "foo@managed.example"},
         sample_policy()});
    writer.write_batch({collapsed_job});

    rapid_inbox::ingestd::SqliteDb db(db_path, 5000);
    auto source_mailbox = db.prepare("SELECT 1 FROM mailboxes WHERE id = 10");
    test::check(!source_mailbox.step_row(), "merged catch-all source mailbox is removed");
    auto target_mailbox = db.prepare(
        "SELECT domain_id, local_part_canonical, rcpt_domain_ascii, first_seen_at, "
        "last_seen_at, latest_message_at, message_count, public_enabled, is_hidden, notes "
        "FROM mailboxes WHERE id = 20");
    test::check(target_mailbox.step_row(), "canonical target mailbox remains");
    test::check(sqlite3_column_int(target_mailbox.get(), 0) == 2,
                "merged mailbox remains managed");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 1))) == "foo",
                "merged mailbox uses managed canonical local part");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 2))) == "managed.example",
                "merged mailbox uses managed domain part");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 3))) ==
                    "2026-01-01T00:00:00Z",
                "merged mailbox preserves earliest first seen time");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 4))) ==
                    "2026-05-12T04:04:05Z",
                "merged mailbox advances last seen time");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 5))) ==
                    "2026-05-12T04:04:05Z",
                "merged mailbox latest message follows active deliveries");
    test::check(sqlite3_column_int(target_mailbox.get(), 6) == 6,
                "merged mailbox recomputes distinct active delivery count");
    test::check(sqlite3_column_int(target_mailbox.get(), 7) == 0,
                "mailbox merge keeps the stricter public flag");
    test::check(sqlite3_column_int(target_mailbox.get(), 8) == 1,
                "mailbox merge keeps hidden state");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(target_mailbox.get(), 9))) == "source mailbox",
                "mailbox merge preserves source notes when target is empty");

    auto delivery_count = db.prepare(
        "SELECT COUNT(*) FROM message_deliveries WHERE mailbox_id = 20");
    test::check(delivery_count.step_row(), "merged delivery count row");
    test::check(sqlite3_column_int(delivery_count.get(), 0) == 6,
                "duplicate message deliveries are collapsed before migration");
    auto collapsed_deliveries = db.prepare(
        "SELECT COUNT(*), MIN(id), MAX(id), MIN(expires_at) FROM message_deliveries "
        "WHERE message_id = 'collapsed_stale_message'");
    test::check(collapsed_deliveries.step_row(), "collapsed delivery count row");
    test::check(sqlite3_column_int(collapsed_deliveries.get(), 0) == 1,
                "stale catch-all recipients collapsing to one managed mailbox are deduplicated");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(collapsed_deliveries.get(), 1))) ==
                    "collapsed_stale_first",
                "collapsed delivery deterministically keeps first recipient id");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(collapsed_deliveries.get(), 3))) ==
                    "2026-06-11T04:04:05Z",
                "refreshed managed match uses current domain retention policy");
    auto collapsed_metric = db.prepare(
        "SELECT deliveries FROM mail_metric_buckets "
        "WHERE bucket_ts = '2026-05-12T04:04:00Z'");
    test::check(collapsed_metric.step_row(), "collapsed delivery metric row");
    test::check(sqlite3_column_int(collapsed_metric.get(), 0) == 1,
                "delivery metric counts persisted canonical deliveries");
    auto moved_delivery = db.prepare(
        "SELECT mailbox_id, mailbox_generation FROM message_deliveries "
        "WHERE id = 'source_only_delivery'");
    test::check(moved_delivery.step_row(), "non-conflicting source delivery is retained");
    test::check(sqlite3_column_int64(moved_delivery.get(), 0) == 20,
                "non-conflicting source delivery moves to target mailbox");
    test::check(sqlite3_column_int64(moved_delivery.get(), 1) == 5,
                "moved delivery inherits target mailbox generation");
    auto new_delivery_generation = db.prepare(
        "SELECT mailbox_generation FROM message_deliveries "
        "WHERE id = 'merge_new_delivery'");
    test::check(new_delivery_generation.step_row(), "new managed delivery remains");
    test::check(sqlite3_column_int64(new_delivery_generation.get(), 0) == 5,
                "new delivery binds the current mailbox generation");
    auto source_duplicate = db.prepare(
        "SELECT 1 FROM message_deliveries WHERE id IN "
        "('source_overlap_later', 'source_overlap_null')");
    test::check(!source_duplicate.step_row(), "source duplicate delivery ids are removed");

    auto later_overlap = db.prepare(
        "SELECT id, status, delivered_at, expires_at, deleted_at, notes, mailbox_generation "
        "FROM message_deliveries WHERE message_id = 'merge_overlap_later'");
    test::check(later_overlap.step_row(), "later-expiry overlap remains");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(later_overlap.get(), 0))) == "target_overlap_later",
                "overlap retains target delivery id");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(later_overlap.get(), 1))) == "active",
                "overlap promotes status to active");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(later_overlap.get(), 2))) ==
                    "2026-01-03T00:00:00Z",
                "overlap keeps earliest delivery time");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(later_overlap.get(), 3))) ==
                    "2026-04-01T00:00:00Z",
                "overlap keeps later non-null expiry");
    test::check(sqlite3_column_type(later_overlap.get(), 4) == SQLITE_NULL,
                "active overlap clears deletion time");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(later_overlap.get(), 5))) == "target later note",
                "overlap prefers target notes");
    test::check(sqlite3_column_int64(later_overlap.get(), 6) == 5,
                "merged duplicate inherits target mailbox generation");

    auto null_overlap = db.prepare(
        "SELECT id, status, expires_at, notes, mailbox_generation FROM message_deliveries "
        "WHERE message_id = 'merge_overlap_null'");
    test::check(null_overlap.step_row(), "null-expiry overlap remains");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(null_overlap.get(), 0))) == "target_overlap_null",
                "null-expiry overlap retains target delivery id");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(null_overlap.get(), 1))) == "active",
                "null-expiry overlap promotes deleted target to active");
    test::check(sqlite3_column_type(null_overlap.get(), 2) == SQLITE_NULL,
                "any null expiry keeps merged delivery unbounded");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(null_overlap.get(), 3))) == "target null note",
                "null-expiry overlap still prefers target notes");
    test::check(sqlite3_column_int64(null_overlap.get(), 4) == 5,
                "reactivated duplicate enters the current mailbox generation");

    auto audit = db.prepare(
        "SELECT resource_ref, details_json FROM audit_logs "
        "WHERE action = 'mailboxes.rehome' ORDER BY id DESC LIMIT 1");
    test::check(audit.step_row(), "mailbox merge emits an audit row");
    test::check(std::string(reinterpret_cast<const char*>(
                    sqlite3_column_text(audit.get(), 0))) == "20",
                "mailbox merge audit targets survivor");
    const std::string audit_details =
        reinterpret_cast<const char*>(sqlite3_column_text(audit.get(), 1));
    test::check(audit_details.find("\"deliveries_moved\":1") != std::string::npos,
                "mailbox merge audit counts moved deliveries");
    test::check(audit_details.find("\"deliveries_deduplicated\":2") != std::string::npos,
                "mailbox merge audit counts deduplicated deliveries");
}

void test_batch_writer_reuses_sqlite_session_and_recovers_after_failure() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-session-reuse";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    {
        SqliteDb db(db_path, 5000);
        initialize_schema(db);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                "'2026-05-12T03:04:05Z')");
    }

    BatchWriter writer(root, db_path, 5000, false);
    constexpr int thread_count = 4;
    constexpr int batches_per_thread = 8;
    std::exception_ptr thread_error;
    std::mutex thread_error_mutex;
    std::vector<std::thread> threads;
    threads.reserve(thread_count);
    for (int thread_index = 0; thread_index < thread_count; ++thread_index) {
        threads.emplace_back([&, thread_index] {
            try {
                for (int batch_index = 0; batch_index < batches_per_thread; ++batch_index) {
                    const std::string suffix = std::to_string(thread_index) + "_" +
                                               std::to_string(batch_index);
                    writer.write_batch({mailbox_job(
                        "reuse_message_" + suffix,
                        "reuse_delivery_" + suffix,
                        "reuse_session_" + suffix,
                        "2026-05-12T03:04:05Z",
                        DomainMatch{1, "adb.com", "adb.com", "code", "code", "code@adb.com"},
                        "code@adb.com")});
                }
            } catch (...) {
                const std::lock_guard guard(thread_error_mutex);
                if (thread_error == nullptr) {
                    thread_error = std::current_exception();
                }
            }
        });
    }
    for (std::thread& thread : threads) {
        thread.join();
    }
    if (thread_error != nullptr) {
        std::rethrow_exception(thread_error);
    }

    BatchWriterSqliteStats stats = writer.sqlite_stats();
    test::check(stats.connections_opened == 1,
                "concurrent serialized batches share one SQLite connection");
    test::check(stats.statement_sets_prepared == 1,
                "hot batches prepare the write statement set only once");
    test::check(stats.connection_active, "successful hot writer keeps SQLite session active");

    {
        SqliteDb db(db_path, 5000);
        db.exec("ALTER TABLE messages ADD COLUMN session_reuse_probe INTEGER");
    }
    writer.write_batch({mailbox_job(
        "schema_reprepare_message",
        "schema_reprepare_delivery",
        "schema_reprepare_session",
        "2026-05-12T03:04:05Z",
        DomainMatch{1, "adb.com", "adb.com", "code", "code", "code@adb.com"},
        "code@adb.com")});
    stats = writer.sqlite_stats();
    test::check(stats.connections_opened == 1 && stats.statement_sets_prepared == 1,
                "persistent v3 statements survive compatible schema changes without reconnecting");

    MailJob failed = mailbox_job(
        "rollback_probe_message",
        "reuse_delivery_0_0",
        "rollback_probe_session",
        "2026-05-12T03:04:05Z",
        DomainMatch{1, "adb.com", "adb.com", "code", "code", "code@adb.com"},
        "code@adb.com");
    bool failed_as_expected = false;
    try {
        writer.write_batch({failed});
    } catch (const std::exception&) {
        failed_as_expected = true;
    }
    test::check(failed_as_expected, "constraint failure escapes the batch transaction");
    stats = writer.sqlite_stats();
    test::check(!stats.connection_active,
                "failed statement discards the complete connection and statement cache");

    failed.recipients.front().delivery_id = "rollback_probe_delivery";
    writer.write_batch({failed});
    stats = writer.sqlite_stats();
    test::check(stats.connections_opened == 2 && stats.statement_sets_prepared == 2,
                "retry opens one fresh session after rollback failure isolation");
    test::check(stats.connection_active, "successful retry retains the replacement session");

    SqliteDb db(db_path, 5000);
    auto counts = db.prepare(
        "SELECT (SELECT COUNT(*) FROM messages), "
        "(SELECT COUNT(*) FROM message_deliveries), "
        "(SELECT message_count FROM mailboxes WHERE address_canonical = 'code@adb.com')");
    test::check(counts.step_row(), "session reuse aggregate row exists");
    constexpr int expected_messages = thread_count * batches_per_thread + 2;
    test::check(sqlite3_column_int(counts.get(), 0) == expected_messages,
                "rollback removes failed message while retry and schema-reprepare writes persist");
    test::check(sqlite3_column_int(counts.get(), 1) == expected_messages,
                "rollback removes failed delivery without losing valid concurrent writes");
    test::check(sqlite3_column_int(counts.get(), 2) == expected_messages,
                "mailbox summary remains exact across reuse, schema reprepare, and retry");
}

void test_batch_writer_reconnects_after_database_file_replacement() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-writer-db-replacement";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path db_path = root / "app.db";
    const fs::path replacement_path = root / "replacement.db";
    const auto initialize = [](const fs::path& path) {
        SqliteDb db(path, 5000);
        initialize_schema(db);
        db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, created_at, "
                "updated_at) VALUES (1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                "'2026-05-12T03:04:05Z')");
    };
    initialize(db_path);

    BatchWriter writer(root, db_path, 5000, false);
    writer.write_batch({mailbox_job(
        "old_database_message",
        "old_database_delivery",
        "old_database_session",
        "2026-05-12T03:04:05Z",
        DomainMatch{1, "adb.com", "adb.com", "code", "code", "code@adb.com"},
        "code@adb.com")});
    initialize(replacement_path);

    const fs::path old_path = root / "old.db";
    const auto move_if_present = [](const fs::path& from, const fs::path& to) {
        if (fs::exists(from)) {
            fs::rename(from, to);
        }
    };
    move_if_present(fs::path(db_path.string() + "-wal"),
                    fs::path(old_path.string() + "-wal"));
    move_if_present(fs::path(db_path.string() + "-shm"),
                    fs::path(old_path.string() + "-shm"));
    fs::rename(db_path, old_path);
    fs::rename(replacement_path, db_path);
    move_if_present(fs::path(replacement_path.string() + "-wal"),
                    fs::path(db_path.string() + "-wal"));
    move_if_present(fs::path(replacement_path.string() + "-shm"),
                    fs::path(db_path.string() + "-shm"));

    writer.write_batch({mailbox_job(
        "replacement_database_message",
        "replacement_database_delivery",
        "replacement_database_session",
        "2026-05-12T03:04:05Z",
        DomainMatch{1, "adb.com", "adb.com", "code", "code", "code@adb.com"},
        "code@adb.com")});
    const BatchWriterSqliteStats stats = writer.sqlite_stats();
    test::check(stats.connections_opened == 2 && stats.statement_sets_prepared == 2,
                "database inode replacement forces connection and statement-cache refresh");

    {
        SqliteDb db(db_path, 5000);
        auto new_messages = db.prepare("SELECT id FROM messages");
        test::check(new_messages.step_row(), "replacement database receives next batch");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(new_messages.get(), 0))) ==
                        "replacement_database_message",
                    "new batch never writes through stale database inode");
        test::check(!new_messages.step_row(), "replacement database contains only its own batch");
    }
    {
        SqliteDb db(old_path, 5000);
        auto old_messages = db.prepare("SELECT id FROM messages");
        test::check(old_messages.step_row(), "old database retains pre-replacement batch");
        test::check(std::string(reinterpret_cast<const char*>(
                        sqlite3_column_text(old_messages.get(), 0))) ==
                        "old_database_message",
                    "persistent writer closed stale inode before next transaction");
        test::check(!old_messages.step_row(), "old database receives no post-replacement batch");
    }
}

void test_batch_writer_revalidates_domain_identity_before_commit() {
    using namespace rapid_inbox::ingestd;
    const auto run_case = [](const std::string& policy_change, bool expect_success) {
        const fs::path root = fs::temp_directory_path() /
                              ("rapid-inbox-writer-domain-race-" + policy_change);
        fs::remove_all(root);
        fs::create_directories(root);
        const fs::path db_path = root / "app.db";
        {
            SqliteDb db(db_path, 5000);
            initialize_schema(db);
            db.exec("INSERT INTO domains (id, root_domain_ascii, root_domain_unicode, "
                    "created_at, updated_at) VALUES "
                    "(1, 'adb.com', 'adb.com', '2026-05-12T03:04:05Z', "
                    "'2026-05-12T03:04:05Z')");
        }

        MailJob job = sample_job();
        job.message_id = "msg_domain_race_" + policy_change;
        job.smtp_session_id = "smtp_domain_race_" + policy_change;
        job.recipients.front().delivery_id = "dlv_domain_race_" + policy_change;
        job.raw_path = raw_message_path(job.message_id, job.received_at);
        job.manifest_path = manifest_path(job.message_id, job.received_at);

        BatchWriter writer(root, db_path, 5000, false);
        {
            SqliteDb db(db_path, 5000);
            if (policy_change == "rename") {
                db.exec("UPDATE domains SET root_domain_ascii = 'renamed.example', "
                        "root_domain_unicode = 'renamed.example' WHERE id = 1");
            } else if (policy_change == "delete") {
                db.exec("DELETE FROM domains WHERE id = 1");
            } else if (policy_change == "flags") {
                db.exec("UPDATE domains SET accept_exact = 0, accept_subdomains = 0, "
                        "max_message_size_bytes = 1 WHERE id = 1");
            } else {
                db.exec("UPDATE domains SET is_active = 0 WHERE id = 1");
            }
        }

        bool succeeded = false;
        std::string error;
        try {
            writer.write_batch({job});
            succeeded = true;
        } catch (const std::exception& exc) {
            error = exc.what();
        }
        test::check(succeeded == expect_success,
                    policy_change + " commit follows fail-closed routing semantics");

        SqliteDb db(db_path, 5000);
        auto counts = db.prepare(
            "SELECT (SELECT COUNT(*) FROM messages), "
            "(SELECT COUNT(*) FROM message_deliveries), "
            "(SELECT COUNT(*) FROM mailboxes)");
        test::check(counts.step_row(), policy_change + " aggregate row exists");
        const int expected_rows = expect_success ? 1 : 0;
        test::check(sqlite3_column_int(counts.get(), 0) == expected_rows,
                    policy_change + " message transaction is atomic");
        test::check(sqlite3_column_int(counts.get(), 1) == expected_rows,
                    policy_change + " delivery transaction is atomic");
        test::check(sqlite3_column_int(counts.get(), 2) == expected_rows,
                    policy_change + " mailbox transaction is atomic");

        if (!expect_success) {
            test::check(error.find("recipient policy conflict") != std::string::npos,
                        policy_change + " reports an explicit policy conflict");
            test::check(fs::exists(root / job.raw_path),
                        policy_change + " preserves durable raw for quarantine");
            test::check(fs::exists(root / job.manifest_path),
                        policy_change + " preserves durable manifest for quarantine");
            writer.write_quarantine_record(job, error, 1);
            const fs::path quarantine_path =
                root / fs::path(job.manifest_path).replace_extension(".error.json");
            const std::string quarantine_content = read_text_file(
                root / "quarantine" /
                fs::relative(quarantine_path, root / "manifests"));
            test::check(
                quarantine_content.find("recipient policy conflict") != std::string::npos,
                policy_change + " quarantine record retains the policy conflict reason");
        }
    };

    run_case("rename", false);
    run_case("delete", false);
    run_case("disable", true);
    run_case("flags", true);
}
