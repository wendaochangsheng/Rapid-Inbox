#pragma once

#include "domain_cache.h"
#include "mail_queue.h"
#include "runtime_stats.h"

#include <atomic>
#include <cstddef>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

namespace rapid_inbox::ingestd {

class BatchWriter;

class SmtpServer {
public:
    SmtpServer(std::string host,
               int port,
               DomainCache& domains,
               MailQueue& queue,
               int max_recipients,
               int max_message_size_bytes,
               int idle_timeout_seconds,
               BatchWriter* durable_writer = nullptr,
               bool durable_ack = false,
               int max_connections = 1024,
               std::size_t max_line_length = 1000,
               std::shared_ptr<IngestRuntimeStats> runtime_stats = nullptr,
               std::size_t reservation_chunk_bytes = 65536,
               int listen_backlog = 1024,
               int connection_rate_limit_count = 0,
               int connection_rate_limit_window_seconds = 60);
    ~SmtpServer();

    void start();
    void stop();

private:
    void accept_loop();
    void handle_client(int client_fd, std::string remote_ip);
    bool register_client_fd(int client_fd);
    void shutdown_active_clients();
    void close_client_fd(int client_fd);

    std::string host_;
    int port_;
    DomainCache& domains_;
    MailQueue& queue_;
    int max_recipients_;
    int max_message_size_bytes_;
    int idle_timeout_seconds_;
    BatchWriter* durable_writer_;
    bool durable_ack_;
    int max_connections_;
    std::size_t max_line_length_;
    std::size_t reservation_chunk_bytes_;
    int listen_backlog_;
    int connection_rate_limit_count_;
    int connection_rate_limit_window_seconds_;
    std::shared_ptr<IngestRuntimeStats> runtime_stats_;
    std::atomic<bool> running_{false};
    int listen_fd_ = -1;
    std::thread accept_thread_;
    std::mutex client_fds_mutex_;
    std::condition_variable client_fds_cv_;
    std::unordered_set<int> active_client_fds_;
};

}
