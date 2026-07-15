#pragma once

#include "mail_job.h"
#include "parsed_mail.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace rapid_inbox::ingestd {

struct MaintenanceState {
    bool active = false;
    std::optional<std::string> token;
};

struct BatchWriterSqliteStats {
    std::uint64_t connections_opened = 0;
    std::uint64_t statement_sets_prepared = 0;
    bool connection_active = false;
};

class BatchWriterSqliteSession;

class BatchWriter {
public:
    BatchWriter(std::filesystem::path storage_root,
                std::filesystem::path database_path,
                int busy_timeout_ms,
                bool fsync_storage);
    ~BatchWriter();

    void write_storage_artifacts(const std::vector<MailJob>& jobs) const;
    void write_pending_artifacts(const MailJob& job) const;
    MaintenanceState maintenance_state() const;
    bool maintenance_active() const;
    std::optional<std::string> maintenance_token() const;
    void publish_ingest_status(const std::string& instance_id,
                               std::size_t queue_messages,
                               std::size_t queue_bytes,
                               std::size_t active_connections,
                               std::size_t max_connections,
                               const std::optional<std::string>& maintenance_token) const;
    void write_maintenance_drained(const std::string& instance_id,
                                   const std::string& maintenance_token) const;
    void release_sqlite_session() const;
    BatchWriterSqliteStats sqlite_stats() const;
    void write_rejected_metric(const std::string& timestamp, std::uint64_t count) const;
    void remove_ingest_status(const std::string& instance_id) const;
    void write_batch(const std::vector<MailJob>& jobs) const;
    void write_quarantine_record(const MailJob& job,
                                 const std::string& error,
                                 int attempts) const;

private:
    std::filesystem::path resolve_storage_path(const std::string& relative_path) const;
    void write_file_atomic(const std::string& relative_path,
                           const std::string& content,
                           bool durable = true) const;
    void write_parsed_artifacts(const MailJob& job, ParsedMail& parsed) const;
    void write_storage_artifacts(
        const std::vector<MailJob>& jobs,
        std::vector<std::variant<ParsedMail, ParseFailure>>& parse_results) const;
    std::string build_manifest(const MailJob& job,
                               const ParsedMail* parsed,
                               const ParseFailure* failure) const;
    void write_sqlite_records(
        const std::vector<MailJob>& jobs,
        const std::vector<std::variant<ParsedMail, ParseFailure>>& parse_results) const;

    std::filesystem::path storage_root_;
    std::filesystem::path database_path_;
    int busy_timeout_ms_;
    bool fsync_storage_;
    mutable std::mutex sqlite_mutex_;
    mutable std::unique_ptr<BatchWriterSqliteSession> sqlite_session_;
    mutable BatchWriterSqliteStats sqlite_stats_;
};

}
