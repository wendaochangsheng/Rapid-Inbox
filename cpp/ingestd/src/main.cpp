#include "config.h"
#include "ingest_app.h"
#include "logger.h"
#include "smtp_server.h"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <csignal>
#include <exception>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

namespace {

std::atomic<bool> stop_requested{false};

void request_stop(int) {
    stop_requested.store(true);
}

}

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--help") {
        std::fputs("usage: rapid-inbox-ingestd [--base-dir PATH] [--writer-smoke]\n", stdout);
        return 0;
    }

    std::filesystem::path base_dir = std::filesystem::current_path();
    bool writer_smoke = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--base-dir" && i + 1 < argc) {
            base_dir = argv[++i];
        } else if (arg == "--writer-smoke") {
            writer_smoke = true;
        }
    }

    try {
        stop_requested.store(false);
        std::signal(SIGTERM, request_stop);
        std::signal(SIGINT, request_stop);

        auto config = rapid_inbox::ingestd::Config::load(base_dir);
        auto& logger = rapid_inbox::ingestd::Logger::instance();
        logger.configure(config.log_level, config.log_format);
        logger.log(rapid_inbox::ingestd::LogLevel::Info,
                   "process.starting",
                   {
                       {"smtp_host", config.smtp_host},
                       {"smtp_port", config.smtp_port},
                       {"worker_count", config.ingest_worker_count},
                       {"durable_ack", config.ingest_durable_ack},
                   });
        auto runtime_stats = std::make_shared<rapid_inbox::ingestd::IngestRuntimeStats>();
        rapid_inbox::ingestd::IngestApp app(config, runtime_stats);
        app.start_writer();
        if (writer_smoke) {
            app.stop_and_drain();
            logger.log(rapid_inbox::ingestd::LogLevel::Info,
                       "process.writer_smoke_succeeded");
            return 0;
        }
        rapid_inbox::ingestd::SmtpServer server(config.smtp_host,
                                                config.smtp_port,
                                                app.domains(),
                                                app.queue(),
                                                config.max_recipients_per_message,
                                                config.max_message_size_bytes,
                                                config.smtp_idle_timeout_seconds,
                                                &app.durable_writer(),
                                                config.ingest_durable_ack,
                                                config.smtp_max_connections,
                                                static_cast<std::size_t>(config.smtp_max_line_length),
                                                runtime_stats,
                                                static_cast<std::size_t>(
                                                    config.ingest_reservation_chunk_bytes),
                                                config.smtp_listen_backlog,
                                                config.smtp_connection_rate_limit_count,
                                                config.smtp_connection_rate_limit_window_seconds);
        server.start();
        logger.log(rapid_inbox::ingestd::LogLevel::Info,
                   "smtp.listener_started",
                   {
                       {"host", config.smtp_host},
                       {"port", config.smtp_port},
                       {"max_connections", config.smtp_max_connections},
                       {"max_message_size_bytes", config.max_message_size_bytes},
                   });
        while (!stop_requested.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        logger.log(rapid_inbox::ingestd::LogLevel::Info, "process.shutdown_started");
        server.stop();
        app.stop_and_drain();
        logger.log(rapid_inbox::ingestd::LogLevel::Info,
                   "process.stopped",
                   {{"queue_drained", true}});
    } catch (const std::exception& exc) {
        rapid_inbox::ingestd::Logger::instance().log(
            rapid_inbox::ingestd::LogLevel::Critical,
            "process.failed",
            {{"error", exc.what()}});
        return 1;
    }
}
