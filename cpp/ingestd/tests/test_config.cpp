#include "../src/config.h"

#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector>

namespace test {
inline void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}
}

namespace {

constexpr std::array<const char*, 28> kConfigEnvVars = {
    "HOST",
    "PORT",
    "SMTP_HOST",
    "SMTP_PORT",
    "HOME",
    "STORAGE_ROOT",
    "DATABASE_PATH",
    "MAX_MESSAGE_SIZE_BYTES",
    "MAX_RECIPIENTS_PER_MESSAGE",
    "SMTP_IDLE_TIMEOUT_SECONDS",
    "SMTP_MAX_CONNECTIONS",
    "SMTP_MAX_LINE_LENGTH",
    "SMTP_LISTEN_BACKLOG",
    "SMTP_CONNECTION_RATE_LIMIT_COUNT",
    "SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS",
    "INGEST_QUEUE_MAX_MESSAGES",
    "INGEST_QUEUE_MAX_BYTES",
    "INGEST_RESERVATION_CHUNK_BYTES",
    "INGEST_BATCH_MAX_MESSAGES",
    "INGEST_FLUSH_INTERVAL_MS",
    "INGEST_SQLITE_BUSY_TIMEOUT_MS",
    "INGEST_WORKER_COUNT",
    "INGEST_MAX_RETRIES",
    "DOMAIN_RELOAD_INTERVAL_MS",
    "INGEST_DURABLE_ACK",
    "INGEST_STORAGE_FSYNC",
    "LOG_LEVEL",
    "LOG_FORMAT",
};

class ScopedEnvGuard {
public:
    ScopedEnvGuard() {
        saved_.reserve(kConfigEnvVars.size());
        for (const char* name : kConfigEnvVars) {
            const char* value = std::getenv(name);
            if (value != nullptr) {
                saved_.push_back({name, std::string(value)});
            } else {
                saved_.push_back({name, std::nullopt});
            }
            unsetenv(name);
        }
    }

    ~ScopedEnvGuard() {
        for (const auto& entry : saved_) {
            if (entry.value.has_value()) {
                setenv(entry.name.c_str(), entry.value->c_str(), 1);
            } else {
                unsetenv(entry.name.c_str());
            }
        }
    }

    void set(const std::string& name, const std::string& value) {
        setenv(name.c_str(), value.c_str(), 1);
    }

private:
    struct Entry {
        std::string name;
        std::optional<std::string> value;
    };

    std::vector<Entry> saved_;
};

class ScopedTempDir {
public:
    ScopedTempDir() {
        const auto base = std::filesystem::temp_directory_path();
        const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
        for (int attempt = 0; attempt < 100; ++attempt) {
            root_ = base / ("rapid-inbox-config-" + std::to_string(now) + "-" +
                            std::to_string(attempt));
            std::error_code ec;
            if (std::filesystem::create_directory(root_, ec)) {
                return;
            }
        }
        throw std::runtime_error("failed to create unique temp directory");
    }

    ~ScopedTempDir() {
        std::error_code ec;
        std::filesystem::remove_all(root_, ec);
    }

    const std::filesystem::path& path() const {
        return root_;
    }

private:
    std::filesystem::path root_;
};

void write_env_file(const std::filesystem::path& dir, const std::string& contents) {
    std::ofstream env(dir / ".env", std::ios::trunc);
    env << contents;
}

template <typename Fn>
void expect_runtime_error_contains(Fn&& fn, const std::string& expected) {
    try {
        fn();
        throw std::runtime_error("expected runtime_error");
    } catch (const std::runtime_error& exc) {
        test::check(std::string(exc.what()).find(expected) != std::string::npos,
                    std::string("unexpected error: ") + exc.what());
    }
}

void expect_invalid_dotenv_integer(const std::string& dotenv, const std::string& expected) {
    ScopedEnvGuard env_guard;
    ScopedTempDir temp_dir;
    write_env_file(temp_dir.path(), dotenv);

    expect_runtime_error_contains(
        [&] { rapid_inbox::ingestd::Config::load(temp_dir.path()); },
        expected);
}

}  // namespace

void test_config_defaults() {
    ScopedEnvGuard env_guard;
    ScopedTempDir temp_dir;

    rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
    test::check(config.base_dir == std::filesystem::absolute(temp_dir.path()).lexically_normal(),
                "default base dir");
    test::check(config.host == "127.0.0.1", "default host");
    test::check(config.port == 8000, "default HTTP port mirror");
    test::check(config.smtp_host == "127.0.0.1", "default SMTP host");
    test::check(config.smtp_port == 25, "default SMTP port");
    test::check(config.storage_root == temp_dir.path() / "storage", "default storage root");
    test::check(config.database_path == temp_dir.path() / "storage" / "app.db",
                "default database path");
    test::check(config.ingest_batch_max_messages == 250, "default ingest batch size");
    test::check(config.ingest_reservation_chunk_bytes == 65536,
                "default reservation chunk size");
    test::check(config.smtp_listen_backlog == 1024, "default listen backlog");
    test::check(config.smtp_connection_rate_limit_count == 60000,
                "C++ connection rate limit has a bounded high-throughput default");
    test::check(config.ingest_flush_interval_ms == 5, "default flush interval");
    test::check(config.ingest_durable_ack, "durable ACK defaults on");
    test::check(config.ingest_worker_count == 4, "default worker count");
    test::check(config.log_level == rapid_inbox::ingestd::LogLevel::Info,
                "default C++ log level");
    test::check(config.log_format == rapid_inbox::ingestd::LogFormat::Json,
                "default C++ log format");
}

void test_config_dotenv_and_environment_override() {
    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        write_env_file(temp_dir.path(),
                       R"(# comment

export HOST = 127.0.0.2
SMTP_HOST = "0.0.0.0"
SMTP_PORT = 2525
STORAGE_ROOT = custom-storage
DATABASE_PATH = custom-db/app.db
MAX_MESSAGE_SIZE_BYTES = 4096
MAX_RECIPIENTS_PER_MESSAGE = 33
SMTP_IDLE_TIMEOUT_SECONDS = 11
SMTP_MAX_CONNECTIONS = 321
SMTP_MAX_LINE_LENGTH = 2048
SMTP_LISTEN_BACKLOG = 777
SMTP_CONNECTION_RATE_LIMIT_COUNT = 44
SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS = 12
INGEST_QUEUE_MAX_MESSAGES = 1234
INGEST_QUEUE_MAX_BYTES = 1048576
INGEST_RESERVATION_CHUNK_BYTES = 32768
INGEST_BATCH_MAX_MESSAGES = 55
INGEST_FLUSH_INTERVAL_MS = 77
INGEST_SQLITE_BUSY_TIMEOUT_MS = 88
INGEST_WORKER_COUNT = 6
INGEST_MAX_RETRIES = 2
DOMAIN_RELOAD_INTERVAL_MS = 250
INGEST_DURABLE_ACK = false
INGEST_STORAGE_FSYNC = true
LOG_LEVEL = warning
LOG_FORMAT = text
)");

        env_guard.set("SMTP_PORT", "2526");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.host == "127.0.0.2", "export syntax with trimmed key/value");
        test::check(config.smtp_host == "0.0.0.0", "quoted dotenv value");
        test::check(config.smtp_port == 2526, "environment overrides dotenv");
        test::check(config.storage_root == temp_dir.path() / "custom-storage",
                    "relative storage root resolves from base dir");
        test::check(config.database_path == temp_dir.path() / "custom-db" / "app.db",
                    "relative database path resolves from base dir");
        test::check(config.max_message_size_bytes == 4096, "parsed max message size");
        test::check(config.max_recipients_per_message == 33, "parsed recipient limit");
        test::check(config.smtp_idle_timeout_seconds == 11, "parsed smtp idle timeout");
        test::check(config.smtp_max_connections == 321, "parsed smtp connection limit");
        test::check(config.smtp_max_line_length == 2048, "parsed smtp line limit");
        test::check(config.smtp_listen_backlog == 777, "parsed listen backlog");
        test::check(config.smtp_connection_rate_limit_count == 44,
                    "parsed C++ connection rate limit");
        test::check(config.smtp_connection_rate_limit_window_seconds == 12,
                    "parsed C++ connection rate window");
        test::check(config.ingest_queue_max_messages == 1234, "parsed ingest queue size");
        test::check(config.ingest_queue_max_bytes == 1048576, "parsed ingest byte limit");
        test::check(config.ingest_reservation_chunk_bytes == 32768,
                    "parsed reservation chunk size");
        test::check(config.ingest_batch_max_messages == 55, "parsed ingest batch size");
        test::check(config.ingest_flush_interval_ms == 77, "parsed flush interval");
        test::check(config.ingest_sqlite_busy_timeout_ms == 88, "parsed sqlite busy timeout");
        test::check(config.ingest_worker_count == 6, "parsed worker count");
        test::check(config.ingest_max_retries == 2, "parsed retry count");
        test::check(config.domain_reload_interval_ms == 250, "parsed domain reload interval");
        test::check(!config.ingest_durable_ack, "parsed durable ACK flag");
        test::check(config.ingest_storage_fsync, "parsed fsync boolean");
        test::check(config.log_level == rapid_inbox::ingestd::LogLevel::Warning,
                    "parsed C++ log level");
        test::check(config.log_format == rapid_inbox::ingestd::LogFormat::Text,
                    "parsed C++ log format");
    }

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        write_env_file(temp_dir.path(), "PORT=\nLOG_LEVEL=\nLOG_FORMAT=   \n");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.port == 8000, "blank PORT falls back to default");
        test::check(config.log_level == rapid_inbox::ingestd::LogLevel::Info,
                    "blank LOG_LEVEL falls back to default");
        test::check(config.log_format == rapid_inbox::ingestd::LogFormat::Json,
                    "blank LOG_FORMAT falls back to default");
    }

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        env_guard.set("PORT", "  9001  ");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.port == 9001, "trimmed PORT parses");
    }

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        env_guard.set("STORAGE_ROOT", "   ");
        env_guard.set("DATABASE_PATH", "\t ");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.storage_root == temp_dir.path() / "storage",
                    "blank STORAGE_ROOT falls back to default");
        test::check(config.database_path == temp_dir.path() / "storage" / "app.db",
                    "blank DATABASE_PATH falls back to default");
    }

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        write_env_file(temp_dir.path(), "STORAGE_ROOT=custom-storage\n");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.storage_root == temp_dir.path() / "custom-storage",
                    "custom storage root resolves from base dir");
        test::check(config.database_path == config.storage_root / "app.db",
                    "custom storage root keeps default database path");
    }

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        env_guard.set("HOME", (temp_dir.path() / "home").string());
        env_guard.set("STORAGE_ROOT", "~/rapid-inbox-storage");

        rapid_inbox::ingestd::Config config = rapid_inbox::ingestd::Config::load(temp_dir.path());
        test::check(config.storage_root == temp_dir.path() / "home" / "rapid-inbox-storage",
                    "tilde storage root expands from HOME");
        test::check(config.database_path == config.storage_root / "app.db",
                    "tilde storage root keeps default database path");
    }

    expect_invalid_dotenv_integer("SMTP_PORT=25abc\n", "invalid SMTP_PORT: 25abc");
    expect_invalid_dotenv_integer("SMTP_IDLE_TIMEOUT_SECONDS=abc\n",
                                  "invalid SMTP_IDLE_TIMEOUT_SECONDS: abc");
    expect_invalid_dotenv_integer("INGEST_QUEUE_MAX_MESSAGES=999999999999999999999999\n",
                                  "invalid INGEST_QUEUE_MAX_MESSAGES: 999999999999999999999999");

    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        write_env_file(temp_dir.path(), "LOG_LEVEL=verbose\n");
        expect_runtime_error_contains(
            [&] { rapid_inbox::ingestd::Config::load(temp_dir.path()); },
            "invalid LOG_LEVEL");
    }
    {
        ScopedEnvGuard env_guard;
        ScopedTempDir temp_dir;
        write_env_file(temp_dir.path(), "LOG_FORMAT=yaml\n");
        expect_runtime_error_contains(
            [&] { rapid_inbox::ingestd::Config::load(temp_dir.path()); },
            "invalid LOG_FORMAT");
    }
    expect_invalid_dotenv_integer("INGEST_WORKER_COUNT=0\n", "invalid INGEST_WORKER_COUNT: 0");
    expect_invalid_dotenv_integer("SMTP_MAX_CONNECTIONS=-1\n", "invalid SMTP_MAX_CONNECTIONS: -1");
    expect_invalid_dotenv_integer("SMTP_LISTEN_BACKLOG=0\n", "invalid SMTP_LISTEN_BACKLOG: 0");
    expect_invalid_dotenv_integer("SMTP_CONNECTION_RATE_LIMIT_COUNT=-1\n",
                                  "invalid SMTP_CONNECTION_RATE_LIMIT_COUNT: -1");
    expect_invalid_dotenv_integer("SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS=0\n",
                                  "invalid SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS: 0");
    expect_invalid_dotenv_integer("INGEST_RESERVATION_CHUNK_BYTES=1024\n",
                                  "invalid INGEST_RESERVATION_CHUNK_BYTES: 1024");
    expect_invalid_dotenv_integer("INGEST_DURABLE_ACK=perhaps\n", "invalid INGEST_DURABLE_ACK: perhaps");
    expect_invalid_dotenv_integer(
        "MAX_MESSAGE_SIZE_BYTES=4096\nINGEST_QUEUE_MAX_BYTES=1024\n",
        "invalid INGEST_QUEUE_MAX_BYTES: must be at least MAX_MESSAGE_SIZE_BYTES");
}
