#include "config.h"

#include <charconv>
#include <cctype>
#include <cstdlib>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>

namespace rapid_inbox::ingestd {
namespace {

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

std::string unquote(std::string value) {
    if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                              (value.front() == '\'' && value.back() == '\''))) {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

std::optional<std::string> configured_value(
    const std::unordered_map<std::string, std::string>& values,
    const std::string& key) {
    if (const char* env_value = std::getenv(key.c_str())) {
        return std::string(env_value);
    }
    const auto found = values.find(key);
    if (found == values.end()) {
        return std::nullopt;
    }
    return found->second;
}

std::string value_for(const std::unordered_map<std::string, std::string>& values,
                      const std::string& key,
                      const std::string& fallback) {
    const auto value = configured_value(values, key);
    return value.has_value() ? *value : fallback;
}

std::optional<std::string> normalized_configured_value(
    const std::unordered_map<std::string, std::string>& values,
    const std::string& key) {
    const auto value = configured_value(values, key);
    if (!value.has_value()) {
        return std::nullopt;
    }
    std::string normalized = trim(*value);
    if (normalized.empty()) {
        return std::nullopt;
    }
    return normalized;
}

std::runtime_error invalid_integer_error(const std::string& key, const std::string& value) {
    return std::runtime_error("invalid " + key + ": " + value);
}

int int_for(const std::unordered_map<std::string, std::string>& values,
            const std::string& key,
            int fallback) {
    const auto value = normalized_configured_value(values, key);
    if (!value.has_value()) {
        return fallback;
    }
    int parsed = 0;
    const char* first = value->data();
    const char* last = first + value->size();
    const auto [ptr, ec] = std::from_chars(first, last, parsed);
    if (ec != std::errc{} || ptr != last) {
        throw invalid_integer_error(key, *value);
    }
    return parsed;
}

bool bool_for(const std::unordered_map<std::string, std::string>& values,
              const std::string& key,
              bool fallback) {
    const auto configured = normalized_configured_value(values, key);
    if (!configured.has_value()) {
        return fallback;
    }
    std::string value = *configured;
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        return false;
    }
    throw std::runtime_error("invalid " + key + ": " + value);
}

void require_range(const std::string& key, int value, int minimum, int maximum) {
    if (value < minimum || value > maximum) {
        throw std::runtime_error("invalid " + key + ": " + std::to_string(value));
    }
}

std::filesystem::path resolve_path(const std::string& value,
                                   const std::filesystem::path& fallback,
                                   const std::filesystem::path& base_dir) {
    const auto normalized = trim(value);
    if (normalized.empty()) {
        return fallback;
    }
    std::filesystem::path path = [&normalized]() {
        if (normalized.front() != '~' || (normalized.size() > 1 && normalized[1] != '/')) {
            return std::filesystem::path(normalized);
        }
        if (const char* home = std::getenv("HOME"); home != nullptr && home[0] != '\0') {
            if (normalized.size() == 1) {
                return std::filesystem::path(home);
            }
            return std::filesystem::path(home) / normalized.substr(2);
        }
        return std::filesystem::path(normalized);
    }();
    if (path.is_relative()) {
        path = base_dir / path;
    }
    return path.lexically_normal();
}

std::unordered_map<std::string, std::string> load_dotenv(const std::filesystem::path& dotenv_path) {
    std::unordered_map<std::string, std::string> values;
    std::ifstream input(dotenv_path);
    std::string line;
    while (std::getline(input, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }
        if (line.rfind("export ", 0) == 0) {
            line = trim(line.substr(7));
        }
        const auto equals = line.find('=');
        if (equals == std::string::npos) {
            continue;
        }
        std::string key = trim(line.substr(0, equals));
        std::string value = unquote(trim(line.substr(equals + 1)));
        if (!key.empty()) {
            values[key] = value;
        }
    }
    return values;
}

}

Config Config::load(const std::filesystem::path& base) {
    Config config;
    config.base_dir = std::filesystem::absolute(base).lexically_normal();
    const auto dotenv = load_dotenv(config.base_dir / ".env");

    config.host = value_for(dotenv, "HOST", "127.0.0.1");
    config.port = int_for(dotenv, "PORT", 8000);
    config.smtp_host = value_for(dotenv, "SMTP_HOST", "127.0.0.1");
    config.smtp_port = int_for(dotenv, "SMTP_PORT", 25);
    config.max_message_size_bytes = int_for(dotenv, "MAX_MESSAGE_SIZE_BYTES", 52428800);
    config.max_recipients_per_message = int_for(dotenv, "MAX_RECIPIENTS_PER_MESSAGE", 20);
    config.smtp_idle_timeout_seconds = int_for(dotenv, "SMTP_IDLE_TIMEOUT_SECONDS", 30);
    config.smtp_max_connections = int_for(dotenv, "SMTP_MAX_CONNECTIONS", 1024);
    config.smtp_max_line_length = int_for(dotenv, "SMTP_MAX_LINE_LENGTH", 1000);
    config.smtp_listen_backlog = int_for(dotenv, "SMTP_LISTEN_BACKLOG", 1024);
    config.smtp_connection_rate_limit_count =
        int_for(dotenv, "SMTP_CONNECTION_RATE_LIMIT_COUNT", 60000);
    config.smtp_connection_rate_limit_window_seconds =
        int_for(dotenv, "SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS", 60);
    config.ingest_queue_max_messages = int_for(dotenv, "INGEST_QUEUE_MAX_MESSAGES", 10000);
    config.ingest_queue_max_bytes = int_for(dotenv, "INGEST_QUEUE_MAX_BYTES", 536870912);
    config.ingest_reservation_chunk_bytes =
        int_for(dotenv, "INGEST_RESERVATION_CHUNK_BYTES", 65536);
    config.ingest_batch_max_messages = int_for(dotenv, "INGEST_BATCH_MAX_MESSAGES", 250);
    config.ingest_flush_interval_ms = int_for(dotenv, "INGEST_FLUSH_INTERVAL_MS", 5);
    config.ingest_sqlite_busy_timeout_ms = int_for(dotenv, "INGEST_SQLITE_BUSY_TIMEOUT_MS", 5000);
    config.ingest_worker_count = int_for(dotenv, "INGEST_WORKER_COUNT", 4);
    config.ingest_max_retries = int_for(dotenv, "INGEST_MAX_RETRIES", 3);
    config.domain_reload_interval_ms = int_for(dotenv, "DOMAIN_RELOAD_INTERVAL_MS", 1000);
    config.ingest_durable_ack = bool_for(dotenv, "INGEST_DURABLE_ACK", true);
    config.ingest_storage_fsync = bool_for(dotenv, "INGEST_STORAGE_FSYNC", false);
    try {
        config.log_level =
            parse_log_level(normalized_configured_value(dotenv, "LOG_LEVEL").value_or("INFO"));
        config.log_format =
            parse_log_format(normalized_configured_value(dotenv, "LOG_FORMAT").value_or("json"));
    } catch (const std::invalid_argument& exc) {
        throw std::runtime_error(exc.what());
    }

    require_range("PORT", config.port, 1, 65535);
    require_range("SMTP_PORT", config.smtp_port, 1, 65535);
    require_range("MAX_MESSAGE_SIZE_BYTES", config.max_message_size_bytes, 1, 1073741824);
    require_range("MAX_RECIPIENTS_PER_MESSAGE", config.max_recipients_per_message, 1, 10000);
    require_range("SMTP_IDLE_TIMEOUT_SECONDS", config.smtp_idle_timeout_seconds, 1, 86400);
    require_range("SMTP_MAX_CONNECTIONS", config.smtp_max_connections, 1, 100000);
    require_range("SMTP_MAX_LINE_LENGTH", config.smtp_max_line_length, 512, 1048576);
    require_range("SMTP_LISTEN_BACKLOG", config.smtp_listen_backlog, 1, 65535);
    require_range("SMTP_CONNECTION_RATE_LIMIT_COUNT",
                  config.smtp_connection_rate_limit_count,
                  0,
                  10000000);
    require_range("SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS",
                  config.smtp_connection_rate_limit_window_seconds,
                  1,
                  86400);
    require_range("INGEST_QUEUE_MAX_MESSAGES", config.ingest_queue_max_messages, 1, 1000000);
    require_range("INGEST_QUEUE_MAX_BYTES", config.ingest_queue_max_bytes, 1, 2147483647);
    require_range("INGEST_RESERVATION_CHUNK_BYTES",
                  config.ingest_reservation_chunk_bytes,
                  4096,
                  16777216);
    require_range("INGEST_BATCH_MAX_MESSAGES", config.ingest_batch_max_messages, 1,
                  config.ingest_queue_max_messages);
    require_range("INGEST_FLUSH_INTERVAL_MS", config.ingest_flush_interval_ms, 1, 60000);
    require_range("INGEST_SQLITE_BUSY_TIMEOUT_MS", config.ingest_sqlite_busy_timeout_ms, 0,
                  600000);
    require_range("INGEST_WORKER_COUNT", config.ingest_worker_count, 1, 128);
    require_range("INGEST_MAX_RETRIES", config.ingest_max_retries, 0, 100);
    require_range("DOMAIN_RELOAD_INTERVAL_MS", config.domain_reload_interval_ms, 50, 600000);
    if (config.ingest_queue_max_bytes < config.max_message_size_bytes) {
        throw std::runtime_error("invalid INGEST_QUEUE_MAX_BYTES: must be at least "
                                 "MAX_MESSAGE_SIZE_BYTES");
    }

    config.storage_root = resolve_path(value_for(dotenv, "STORAGE_ROOT", ""),
                                       config.base_dir / "storage",
                                       config.base_dir);
    config.database_path = resolve_path(value_for(dotenv, "DATABASE_PATH", ""),
                                        config.storage_root / "app.db",
                                        config.base_dir);
    return config;
}

}
