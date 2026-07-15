#pragma once

#include "logger.h"

#include <filesystem>
#include <string>

namespace rapid_inbox::ingestd {

struct Config {
    std::filesystem::path base_dir;
    std::filesystem::path storage_root;
    std::filesystem::path database_path;
    std::string host = "127.0.0.1";
    int port = 8000;
    std::string smtp_host = "127.0.0.1";
    int smtp_port = 25;
    int max_message_size_bytes = 52428800;
    int max_recipients_per_message = 20;
    int smtp_idle_timeout_seconds = 30;
    int smtp_max_connections = 1024;
    int smtp_max_line_length = 1000;
    int smtp_listen_backlog = 1024;
    int smtp_connection_rate_limit_count = 60000;
    int smtp_connection_rate_limit_window_seconds = 60;
    int ingest_queue_max_messages = 10000;
    int ingest_queue_max_bytes = 536870912;
    int ingest_reservation_chunk_bytes = 65536;
    int ingest_batch_max_messages = 250;
    int ingest_flush_interval_ms = 5;
    int ingest_sqlite_busy_timeout_ms = 5000;
    int ingest_worker_count = 4;
    int ingest_max_retries = 3;
    int domain_reload_interval_ms = 1000;
    bool ingest_durable_ack = true;
    bool ingest_storage_fsync = false;
    LogLevel log_level = LogLevel::Info;
    LogFormat log_format = LogFormat::Json;

    static Config load(const std::filesystem::path& base_dir);
};

}
