#include "smtp_server.h"

#include "batch_writer.h"
#include "logger.h"
#include "smtp_session.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstring>
#include <chrono>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

namespace rapid_inbox::ingestd {
namespace {

std::runtime_error socket_error(const std::string& action) {
    return std::runtime_error(action + ": " + std::strerror(errno));
}

void close_fd(int fd) {
    if (fd >= 0) {
        (void)::close(fd);
    }
}

bool send_all(int fd, const char* data, std::size_t size) {
    std::size_t sent_total = 0;
    while (sent_total < size) {
        const ssize_t sent =
            ::send(fd, data + sent_total, size - sent_total, MSG_NOSIGNAL);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        if (sent == 0) {
            return false;
        }
        sent_total += static_cast<std::size_t>(sent);
    }
    return true;
}

bool send_line(int fd, const std::string& line) {
    const std::string payload = line + "\r\n";
    return send_all(fd, payload.data(), payload.size());
}

bool set_receive_timeout(int fd, int timeout_seconds) {
    if (timeout_seconds <= 0) {
        return true;
    }
    timeval timeout{};
    timeout.tv_sec = timeout_seconds;
    timeout.tv_usec = 0;
    return ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0;
}

class ClientLineReader {
public:
    explicit ClientLineReader(int fd) : fd_(fd) {}

    bool recv_line(std::string& line, bool& too_long, std::size_t max_length) {
        line.clear();
        too_long = false;
        for (;;) {
            if (cursor_ == end_ && !fill()) {
                return false;
            }

            const char ch = buffer_[cursor_++];
            if (ch == '\n') {
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }
                return true;
            }
            if (line.size() < max_length) {
                line.push_back(ch);
            } else {
                too_long = true;
            }
        }
    }

private:
    bool fill() {
        for (;;) {
            const ssize_t received = ::recv(fd_, buffer_.data(), buffer_.size(), 0);
            if (received == 0) {
                return false;
            }
            if (received < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            cursor_ = 0;
            end_ = static_cast<std::size_t>(received);
            return true;
        }
    }

    int fd_;
    std::array<char, 4096> buffer_{};
    std::size_t cursor_ = 0;
    std::size_t end_ = 0;
};

int create_listen_socket(const std::string& host, int port, int backlog) {
    if (port < 0 || port > 65535) {
        throw std::runtime_error("invalid SMTP_PORT: " + std::to_string(port));
    }
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;
    hints.ai_flags = AI_NUMERICSERV;
    const bool passive = host.empty() || host == "*";
    if (passive) {
        hints.ai_flags |= AI_PASSIVE;
    }
    addrinfo* raw_addresses = nullptr;
    const std::string service = std::to_string(port);
    const int lookup = ::getaddrinfo(passive ? nullptr : host.c_str(),
                                     service.c_str(),
                                     &hints,
                                     &raw_addresses);
    if (lookup != 0) {
        throw std::runtime_error("SMTP_HOST resolution failed: " +
                                 std::string(::gai_strerror(lookup)));
    }
    std::unique_ptr<addrinfo, decltype(&::freeaddrinfo)> addresses(raw_addresses,
                                                                  &::freeaddrinfo);

    int last_error = EADDRNOTAVAIL;
    for (const addrinfo* address = addresses.get(); address != nullptr; address = address->ai_next) {
        const int fd = ::socket(address->ai_family, address->ai_socktype, address->ai_protocol);
        if (fd < 0) {
            last_error = errno;
            continue;
        }
        const int enabled = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled)) < 0) {
            last_error = errno;
            close_fd(fd);
            continue;
        }
        if (address->ai_family == AF_INET6) {
            const int dual_stack = 0;
            (void)::setsockopt(fd,
                               IPPROTO_IPV6,
                               IPV6_V6ONLY,
                               &dual_stack,
                               sizeof(dual_stack));
        }
        if (::bind(fd, address->ai_addr, address->ai_addrlen) < 0) {
            last_error = errno;
            close_fd(fd);
            continue;
        }
        if (::listen(fd, backlog) < 0) {
            last_error = errno;
            close_fd(fd);
            continue;
        }
        return fd;
    }
    errno = last_error;
    throw socket_error("bind/listen");
}

std::string peer_ip_string(const sockaddr_storage& peer) {
    std::array<char, INET6_ADDRSTRLEN> buffer{};
    const void* address = nullptr;
    if (peer.ss_family == AF_INET) {
        address = &reinterpret_cast<const sockaddr_in*>(&peer)->sin_addr;
    } else if (peer.ss_family == AF_INET6) {
        address = &reinterpret_cast<const sockaddr_in6*>(&peer)->sin6_addr;
    }
    if (address == nullptr ||
        ::inet_ntop(peer.ss_family, address, buffer.data(), buffer.size()) == nullptr) {
        return "unknown";
    }
    return std::string(buffer.data());
}

class ConnectionRateLimiter {
public:
    ConnectionRateLimiter(int limit,
                          std::chrono::seconds window,
                          std::size_t maximum_entries)
        : limit_(limit),
          window_(window),
          maximum_entries_(std::max<std::size_t>(maximum_entries, 1)) {
        if (limit_ > 0) {
            entries_.reserve(maximum_entries_);
        }
    }

    bool allow(const std::string& remote_ip, std::chrono::steady_clock::time_point now) {
        if (limit_ == 0) {
            return true;
        }
        if ((++operations_ & 0xffU) == 0) {
            remove_expired(now);
        }
        auto found = entries_.find(remote_ip);
        if (found == entries_.end()) {
            if (entries_.size() >= maximum_entries_) {
                // Rotating-source attacks must not turn bounded memory into an
                // O(n) accept-path scan. Periodic expiry handles normal churn;
                // at the hard cap, evict one arbitrary stale-or-live bucket.
                entries_.erase(entries_.begin());
            }
            found = entries_.emplace(remote_ip, Entry{.bucket_started_at = now, .last_seen = now})
                        .first;
        }
        Entry& entry = found->second;
        advance(entry, now);
        entry.last_seen = now;
        const double elapsed =
            std::chrono::duration<double>(now - entry.bucket_started_at).count();
        const double window_seconds = static_cast<double>(window_.count());
        const double previous_weight = std::max(1.0 - elapsed / window_seconds, 0.0);
        const double estimated = static_cast<double>(entry.current_count) +
                                 static_cast<double>(entry.previous_count) * previous_weight;
        if (estimated >= static_cast<double>(limit_)) {
            return false;
        }
        ++entry.current_count;
        return true;
    }

private:
    struct Entry {
        std::chrono::steady_clock::time_point bucket_started_at;
        std::chrono::steady_clock::time_point last_seen;
        std::uint64_t current_count = 0;
        std::uint64_t previous_count = 0;
    };

    void advance(Entry& entry, std::chrono::steady_clock::time_point now) const {
        const auto elapsed = now - entry.bucket_started_at;
        if (elapsed < window_) {
            return;
        }
        const auto windows = elapsed / window_;
        entry.previous_count = windows == 1 ? entry.current_count : 0;
        entry.current_count = 0;
        entry.bucket_started_at += window_ * windows;
    }

    void remove_expired(std::chrono::steady_clock::time_point now) {
        const auto cutoff = now - window_ * 2;
        for (auto iterator = entries_.begin(); iterator != entries_.end();) {
            if (iterator->second.last_seen < cutoff) {
                iterator = entries_.erase(iterator);
            } else {
                ++iterator;
            }
        }
    }

    int limit_;
    std::chrono::seconds window_;
    std::size_t maximum_entries_;
    std::uint64_t operations_ = 0;
    std::unordered_map<std::string, Entry> entries_;
};

}  // namespace

SmtpServer::SmtpServer(std::string host,
                       int port,
                       DomainCache& domains,
                       MailQueue& queue,
                       int max_recipients,
                       int max_message_size_bytes,
                       int idle_timeout_seconds,
                       BatchWriter* durable_writer,
                       bool durable_ack,
                       int max_connections,
                       std::size_t max_line_length,
                       std::shared_ptr<IngestRuntimeStats> runtime_stats,
                       std::size_t reservation_chunk_bytes,
                       int listen_backlog,
                       int connection_rate_limit_count,
                       int connection_rate_limit_window_seconds)
    : host_(std::move(host)),
      port_(port),
      domains_(domains),
      queue_(queue),
      max_recipients_(max_recipients),
      max_message_size_bytes_(max_message_size_bytes),
      idle_timeout_seconds_(idle_timeout_seconds),
      durable_writer_(durable_writer),
      durable_ack_(durable_ack),
      max_connections_(max_connections),
      max_line_length_(max_line_length),
      reservation_chunk_bytes_(reservation_chunk_bytes),
      listen_backlog_(listen_backlog),
      connection_rate_limit_count_(connection_rate_limit_count),
      connection_rate_limit_window_seconds_(connection_rate_limit_window_seconds),
      runtime_stats_(runtime_stats == nullptr ? std::make_shared<IngestRuntimeStats>()
                                              : std::move(runtime_stats)) {
    if (max_message_size_bytes_ < 0) {
        throw std::runtime_error("invalid MAX_MESSAGE_SIZE_BYTES: " +
                                 std::to_string(max_message_size_bytes_));
    }
    if (idle_timeout_seconds_ < 0) {
        throw std::runtime_error("invalid SMTP_IDLE_TIMEOUT_SECONDS: " +
                                 std::to_string(idle_timeout_seconds_));
    }
    if (max_connections_ <= 0) {
        throw std::runtime_error("invalid SMTP_MAX_CONNECTIONS: " +
                                 std::to_string(max_connections_));
    }
    if (max_line_length_ == 0) {
        throw std::runtime_error("invalid SMTP_MAX_LINE_LENGTH: 0");
    }
    if (reservation_chunk_bytes_ == 0) {
        throw std::runtime_error("invalid INGEST_RESERVATION_CHUNK_BYTES: 0");
    }
    if (listen_backlog_ <= 0) {
        throw std::runtime_error("invalid SMTP_LISTEN_BACKLOG: " +
                                 std::to_string(listen_backlog_));
    }
    if (connection_rate_limit_count_ < 0) {
        throw std::runtime_error("invalid SMTP_CONNECTION_RATE_LIMIT_COUNT: " +
                                 std::to_string(connection_rate_limit_count_));
    }
    if (connection_rate_limit_window_seconds_ <= 0) {
        throw std::runtime_error("invalid SMTP_CONNECTION_RATE_LIMIT_WINDOW_SECONDS: " +
                                 std::to_string(connection_rate_limit_window_seconds_));
    }
    if (durable_ack_ && durable_writer_ == nullptr) {
        throw std::runtime_error("INGEST_DURABLE_ACK requires a durable writer");
    }
}

SmtpServer::~SmtpServer() {
    stop();
}

void SmtpServer::start() {
    bool expected = false;
    if (!running_.compare_exchange_strong(expected, true)) {
        return;
    }

    try {
        listen_fd_ = create_listen_socket(host_, port_, listen_backlog_);
        accept_thread_ = std::thread([this] { accept_loop(); });
    } catch (...) {
        running_ = false;
        close_fd(listen_fd_);
        listen_fd_ = -1;
        throw;
    }
}

void SmtpServer::stop() {
    const bool was_running = running_.exchange(false, std::memory_order_acq_rel);

    if (listen_fd_ >= 0) {
        (void)::shutdown(listen_fd_, SHUT_RDWR);
        close_fd(listen_fd_);
        listen_fd_ = -1;
    }
    shutdown_active_clients();

    if (accept_thread_.joinable()) {
        accept_thread_.join();
    }

    {
        std::unique_lock lock(client_fds_mutex_);
        client_fds_cv_.wait(lock, [this] { return active_client_fds_.empty(); });
    }
    if (was_running && Logger::instance().enabled(LogLevel::Debug)) {
        Logger::instance().log(LogLevel::Debug,
                               "smtp.listener_stopped",
                               {{"active_connections", 0}});
    }
}

void SmtpServer::accept_loop() {
    const int fd = listen_fd_;
    auto next_accept_error_log = std::chrono::steady_clock::time_point::min();
    auto next_connection_limit_log = std::chrono::steady_clock::time_point::min();
    auto next_rate_limit_log = std::chrono::steady_clock::time_point::min();
    std::size_t connection_limit_rejections = 0;
    std::size_t rate_limit_rejections = 0;
    const std::size_t maximum_rate_entries =
        std::min<std::size_t>(65536,
                              std::max<std::size_t>(1024,
                                                    static_cast<std::size_t>(max_connections_) * 4));
    ConnectionRateLimiter rate_limiter(
        connection_rate_limit_count_,
        std::chrono::seconds(connection_rate_limit_window_seconds_),
        maximum_rate_entries);
    while (running_) {
        sockaddr_storage peer{};
        socklen_t peer_size = sizeof(peer);
        const int client_fd =
            ::accept(fd, reinterpret_cast<sockaddr*>(&peer), &peer_size);
        if (client_fd < 0) {
            const int accept_error = errno;
            if (accept_error == EINTR) {
                continue;
            }
            if (!running_) {
                break;
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_accept_error_log) {
                Logger::instance().log(LogLevel::Warning,
                                       "smtp.accept_failed",
                                       {
                                           {"error_number", accept_error},
                                           {"error", std::strerror(accept_error)},
                                       });
                next_accept_error_log = now + std::chrono::seconds(30);
            }
            continue;
        }

        if (!running_) {
            close_fd(client_fd);
            break;
        }

        try {
            std::string remote_ip = peer_ip_string(peer);
            const auto accepted_at = std::chrono::steady_clock::now();
            if (!rate_limiter.allow(remote_ip, accepted_at)) {
                ++rate_limit_rejections;
                if (accepted_at >= next_rate_limit_log) {
                    Logger::instance().log(
                        LogLevel::Warning,
                        "smtp.connection_rejected",
                        {
                            {"reason", "connection_rate_limit"},
                            {"remote_ip", remote_ip},
                            {"rejections_since_last_log", rate_limit_rejections},
                        });
                    rate_limit_rejections = 0;
                    next_rate_limit_log = accepted_at + std::chrono::seconds(1);
                }
                (void)send_line(client_fd, "421 connection rate limit exceeded");
                close_fd(client_fd);
                continue;
            }
            if (!register_client_fd(client_fd)) {
                ++connection_limit_rejections;
                const auto now = std::chrono::steady_clock::now();
                if (now >= next_connection_limit_log) {
                    Logger::instance().log(
                        LogLevel::Warning,
                        "smtp.connection_rejected",
                        {
                            {"reason", "connection_limit"},
                            {"remote_ip", remote_ip},
                            {"rejections_since_last_log", connection_limit_rejections},
                            {"active_connections",
                             runtime_stats_->active_connections.load(std::memory_order_acquire)},
                            {"max_connections", max_connections_},
                        });
                    connection_limit_rejections = 0;
                    next_connection_limit_log = now + std::chrono::seconds(1);
                }
                (void)send_line(client_fd, "421 too many concurrent connections");
                close_fd(client_fd);
                continue;
            }
            if (Logger::instance().enabled(LogLevel::Debug)) {
                Logger::instance().log(
                    LogLevel::Debug,
                    "smtp.connection_opened",
                    {
                        {"remote_ip", remote_ip},
                        {"active_connections",
                         runtime_stats_->active_connections.load(std::memory_order_acquire)},
                    });
            }
            std::thread client_thread([this, client_fd, remote_ip = std::move(remote_ip)] {
                try {
                    handle_client(client_fd, remote_ip);
                } catch (const std::exception& exc) {
                    Logger::instance().log(LogLevel::Error,
                                           "smtp.client_handler_failed",
                                           {
                                               {"remote_ip", remote_ip},
                                               {"error", exc.what()},
                                           });
                    close_client_fd(client_fd);
                } catch (...) {
                    Logger::instance().log(LogLevel::Error,
                                           "smtp.client_handler_failed",
                                           {
                                               {"remote_ip", remote_ip},
                                               {"error", "unknown exception"},
                                           });
                    close_client_fd(client_fd);
                }
            });
            client_thread.detach();
        } catch (const std::exception& exc) {
            Logger::instance().log(LogLevel::Error,
                                   "smtp.connection_dispatch_failed",
                                   {{"error", exc.what()}});
            close_client_fd(client_fd);
        } catch (...) {
            Logger::instance().log(LogLevel::Error,
                                   "smtp.connection_dispatch_failed",
                                   {{"error", "unknown exception"}});
            close_client_fd(client_fd);
        }
    }
}

void SmtpServer::handle_client(int client_fd, std::string remote_ip) {
    if (!set_receive_timeout(client_fd, idle_timeout_seconds_)) {
        Logger::instance().log(LogLevel::Warning,
                               "smtp.connection_rejected",
                               {
                                   {"reason", "receive_timeout_configuration_failed"},
                                   {"remote_ip", remote_ip},
                               });
        close_client_fd(client_fd);
        return;
    }
    if (durable_writer_ != nullptr && durable_writer_->maintenance_active()) {
        Logger::instance().log(LogLevel::Info,
                               "smtp.connection_rejected",
                               {
                                   {"reason", "maintenance"},
                                   {"remote_ip", remote_ip},
                               });
        (void)send_line(client_fd, "421 storage maintenance in progress");
        close_client_fd(client_fd);
        return;
    }

    auto domain_rules = domains_.snapshot_rules();
    SmtpSession session(std::move(domain_rules),
                        &domains_,
                        queue_,
                        max_recipients_,
                        static_cast<std::size_t>(max_message_size_bytes_),
                        durable_writer_,
                        durable_ack_,
                        remote_ip,
                        reservation_chunk_bytes_,
                        runtime_stats_);

    if (!send_line(client_fd, session.greeting())) {
        if (Logger::instance().enabled(LogLevel::Debug)) {
            Logger::instance().log(LogLevel::Debug,
                                   "smtp.greeting_send_failed",
                                   {{"remote_ip", remote_ip}});
        }
        close_client_fd(client_fd);
        return;
    }

    ClientLineReader reader(client_fd);
    std::string line;
    bool line_too_long = false;
    while (running_ && reader.recv_line(line, line_too_long, max_line_length_)) {
        if (line_too_long) {
            if (session.in_data()) {
                session.reject_overlong_data_line();
                continue;
            }
            Logger::instance().log(LogLevel::Warning,
                                   "smtp.command_rejected",
                                   {
                                       {"reason", "line_too_long"},
                                       {"remote_ip", remote_ip},
                                       {"max_line_length", max_line_length_},
                                   });
            (void)send_line(client_fd, "500 line too long");
            break;
        }
        std::string response = session.handle_line(line);
        if (response.empty()) {
            continue;
        }
        if (!send_line(client_fd, response)) {
            break;
        }
        if (response.rfind("221", 0) == 0) {
            break;
        }
    }

    close_client_fd(client_fd);
}

bool SmtpServer::register_client_fd(int client_fd) {
    const std::lock_guard lock(client_fds_mutex_);
    if (active_client_fds_.size() >= static_cast<std::size_t>(max_connections_)) {
        return false;
    }
    active_client_fds_.insert(client_fd);
    runtime_stats_->active_connections.store(active_client_fds_.size(),
                                             std::memory_order_release);
    return true;
}

void SmtpServer::shutdown_active_clients() {
    const std::lock_guard lock(client_fds_mutex_);
    for (const int client_fd : active_client_fds_) {
        (void)::shutdown(client_fd, SHUT_RDWR);
    }
}

void SmtpServer::close_client_fd(int client_fd) {
    bool was_active = false;
    std::size_t active_connections = 0;
    {
        const std::lock_guard lock(client_fds_mutex_);
        was_active = active_client_fds_.erase(client_fd) != 0;
        active_connections = active_client_fds_.size();
        runtime_stats_->active_connections.store(active_connections, std::memory_order_release);
    }
    (void)::shutdown(client_fd, SHUT_RDWR);
    close_fd(client_fd);
    client_fds_cv_.notify_all();
    if (was_active && Logger::instance().enabled(LogLevel::Debug)) {
        Logger::instance().log(LogLevel::Debug,
                               "smtp.connection_closed",
                               {{"active_connections", active_connections}});
    }
}

}  // namespace rapid_inbox::ingestd
