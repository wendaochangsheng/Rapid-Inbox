#include "../src/domain_cache.h"
#include "../src/mail_queue.h"
#include "../src/smtp_server.h"
#include "../src/sqlite_db.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <future>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

using namespace std::chrono_literals;
namespace fs = std::filesystem;

void close_fd(int fd) {
    if (fd >= 0) {
        (void)::close(fd);
    }
}

std::runtime_error socket_error(const std::string& action) {
    return std::runtime_error(action + ": " + std::strerror(errno));
}

int reserve_loopback_port() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        throw socket_error("socket");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(0);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        const int saved_errno = errno;
        close_fd(fd);
        errno = saved_errno;
        throw socket_error("bind");
    }

    socklen_t address_size = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) < 0) {
        const int saved_errno = errno;
        close_fd(fd);
        errno = saved_errno;
        throw socket_error("getsockname");
    }

    const int port = ntohs(address.sin_port);
    close_fd(fd);
    return port;
}

int reserve_ipv6_loopback_port() {
    const int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    address.sin6_port = htons(0);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        close_fd(fd);
        return -1;
    }
    socklen_t address_size = sizeof(address);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &address_size) < 0) {
        close_fd(fd);
        return -1;
    }
    const int port = ntohs(address.sin6_port);
    close_fd(fd);
    return port;
}

int connect_loopback(int port) {
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = htons(static_cast<uint16_t>(port));

    for (int attempt = 0; attempt < 50; ++attempt) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw socket_error("socket");
        }
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) {
            return fd;
        }

        const int saved_errno = errno;
        close_fd(fd);
        if (saved_errno != ECONNREFUSED && saved_errno != EINTR) {
            errno = saved_errno;
            throw socket_error("connect");
        }
        std::this_thread::sleep_for(10ms);
    }

    throw std::runtime_error("connect timed out");
}

int connect_ipv6_loopback(int port) {
    sockaddr_in6 address{};
    address.sin6_family = AF_INET6;
    address.sin6_addr = in6addr_loopback;
    address.sin6_port = htons(static_cast<uint16_t>(port));
    for (int attempt = 0; attempt < 50; ++attempt) {
        const int fd = ::socket(AF_INET6, SOCK_STREAM, 0);
        if (fd < 0) {
            throw socket_error("socket IPv6");
        }
        if (::connect(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0) {
            return fd;
        }
        const int saved_errno = errno;
        close_fd(fd);
        if (saved_errno != ECONNREFUSED && saved_errno != EINTR) {
            errno = saved_errno;
            throw socket_error("connect IPv6");
        }
        std::this_thread::sleep_for(10ms);
    }
    throw std::runtime_error("IPv6 connect timed out");
}

std::string recv_line_with_timeout(int fd, std::chrono::milliseconds timeout) {
    std::string line;
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    for (;;) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            throw std::runtime_error("recv line timed out");
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        pollfd poll_fd{.fd = fd, .events = POLLIN, .revents = 0};
        const int ready = ::poll(&poll_fd, 1, static_cast<int>(remaining.count()));
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socket_error("poll");
        }
        if (ready == 0) {
            throw std::runtime_error("recv line timed out");
        }

        char ch = '\0';
        const ssize_t received = ::recv(fd, &ch, 1, 0);
        if (received == 0) {
            throw std::runtime_error("connection closed while reading line");
        }
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socket_error("recv");
        }
        if (ch == '\n') {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return line;
        }
        line.push_back(ch);
    }
}

void recv_eof_with_timeout(int fd, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    for (;;) {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            throw std::runtime_error("recv eof timed out");
        }

        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        pollfd poll_fd{.fd = fd, .events = POLLIN, .revents = 0};
        const int ready = ::poll(&poll_fd, 1, static_cast<int>(remaining.count()));
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socket_error("poll");
        }
        if (ready == 0) {
            throw std::runtime_error("recv eof timed out");
        }

        char ch = '\0';
        const ssize_t received = ::recv(fd, &ch, 1, 0);
        if (received == 0) {
            return;
        }
        if (received < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw socket_error("recv");
        }
        throw std::runtime_error("received data while waiting for eof");
    }
}

void send_text(int fd, const std::string& payload) {
    std::size_t offset = 0;
    while (offset < payload.size()) {
        const ssize_t sent = ::send(fd, payload.data() + offset, payload.size() - offset, 0);
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent <= 0) {
            throw socket_error("send");
        }
        offset += static_cast<std::size_t>(sent);
    }
}

void initialize_domain_database(const fs::path& database_path) {
    rapid_inbox::ingestd::SqliteDb db(database_path, 5000);
    const fs::path schema_path = fs::path(RAPID_INBOX_REPO_ROOT) / "sqlite_schema.sql";
    std::ifstream schema(schema_path);
    const std::string sql((std::istreambuf_iterator<char>(schema)),
                          std::istreambuf_iterator<char>());
    db.exec(sql);
    db.exec("INSERT INTO domains (root_domain_ascii, root_domain_unicode, created_at, updated_at) "
            "VALUES ('adb.com', 'adb.com', '2026-07-15T00:00:00Z', "
            "'2026-07-15T00:00:00Z')");
}

}  // namespace

void test_smtp_server_stop_wakes_idle_client() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-test.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server(
        "127.0.0.1", port, domains, queue, 20, 1024 * 1024, 30);

    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        const std::string greeting = recv_line_with_timeout(client_fd, 1s);
        test::check(greeting == "220 rapid-inbox-ingestd", "smtp server greeting");

        auto stop_future = std::async(std::launch::async, [&server] { server.stop(); });
        if (stop_future.wait_for(300ms) != std::future_status::ready) {
            (void)::shutdown(client_fd, SHUT_RDWR);
            close_fd(client_fd);
            client_fd = -1;
            test::check(stop_future.wait_for(2s) == std::future_status::ready,
                        "smtp server stop remained blocked after client cleanup");
            stop_future.get();
            throw std::runtime_error("smtp server stop timed out with an idle client");
        }

        stop_future.get();
        close_fd(client_fd);
        client_fd = -1;
    } catch (...) {
        if (client_fd >= 0) {
            (void)::shutdown(client_fd, SHUT_RDWR);
            close_fd(client_fd);
        }
        server.stop();
        throw;
    }
}

void test_smtp_server_idle_client_times_out() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-timeout.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server("127.0.0.1", port, domains, queue, 20, 1024 * 1024, 1);

    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        const std::string greeting = recv_line_with_timeout(client_fd, 1s);
        test::check(greeting == "220 rapid-inbox-ingestd", "smtp server timeout greeting");

        recv_eof_with_timeout(client_fd, 3s);
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
    } catch (...) {
        if (client_fd >= 0) {
            (void)::shutdown(client_fd, SHUT_RDWR);
            close_fd(client_fd);
        }
        server.stop();
        throw;
    }
}

void test_smtp_server_rejects_connections_over_limit() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-limit.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    auto runtime_stats = std::make_shared<rapid_inbox::ingestd::IngestRuntimeStats>();
    rapid_inbox::ingestd::SmtpServer server("127.0.0.1",
                                             port,
                                             domains,
                                             queue,
                                             20,
                                             1024 * 1024,
                                             30,
                                             nullptr,
                                             false,
                                             1,
                                             1000,
                                             runtime_stats);

    server.start();
    int first_fd = -1;
    int second_fd = -1;
    try {
        first_fd = connect_loopback(port);
        test::check(recv_line_with_timeout(first_fd, 1s) == "220 rapid-inbox-ingestd",
                    "first connection accepted");
        test::check(runtime_stats->active_connections.load(std::memory_order_acquire) == 1,
                    "accepted connection increments shared runtime count");
        second_fd = connect_loopback(port);
        test::check(recv_line_with_timeout(second_fd, 1s).rfind("421", 0) == 0,
                    "connection over limit rejected with 421");
        test::check(runtime_stats->active_connections.load(std::memory_order_acquire) == 1,
                    "rejected over-limit connection does not increment runtime count");
        close_fd(second_fd);
        second_fd = -1;
        close_fd(first_fd);
        first_fd = -1;
        for (int attempt = 0;
             attempt < 100 &&
             runtime_stats->active_connections.load(std::memory_order_acquire) != 0;
             ++attempt) {
            std::this_thread::sleep_for(10ms);
        }
        test::check(runtime_stats->active_connections.load(std::memory_order_acquire) == 0,
                    "closed connection decrements shared runtime count");
        server.stop();
    } catch (...) {
        close_fd(second_fd);
        close_fd(first_fd);
        server.stop();
        throw;
    }
}

void test_smtp_server_closes_overlong_lines() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-line.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server(
        "127.0.0.1", port, domains, queue, 20, 1024 * 1024, 30, nullptr, false, 10, 16);

    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        (void)recv_line_with_timeout(client_fd, 1s);
        send_text(client_fd, "EHLO " + std::string(32, 'x') + "\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "500 line too long",
                    "overlong command rejected");
        recv_eof_with_timeout(client_fd, 1s);
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        throw;
    }
}

void test_smtp_server_serves_esmtp_capabilities_and_pipeline_over_socket() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-esmtp.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server(
        "127.0.0.1", port, domains, queue, 20, 4096, 30);
    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        test::check(recv_line_with_timeout(client_fd, 1s) == "220 rapid-inbox-ingestd",
                    "socket ESMTP greeting");
        send_text(client_fd, "EHLO client.example\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250-rapid-inbox-ingestd",
                    "EHLO identity line");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250-SIZE 4096",
                    "EHLO SIZE capability");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250-8BITMIME",
                    "EHLO 8BITMIME capability");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250-PIPELINING",
                    "EHLO PIPELINING capability");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 SMTPUTF8",
                    "EHLO SMTPUTF8 capability");

        send_text(client_fd, "NOOP health\r\nMAIL FROM:<> SIZE=0\r\nQUIT\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK",
                    "pipelined NOOP response");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK",
                    "pipelined null reverse-path response");
        test::check(recv_line_with_timeout(client_fd, 1s) == "221 2.0.0 Bye",
                    "pipelined QUIT response");
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        throw;
    }
}

void test_smtp_server_rate_limits_connections_by_peer_ip() {
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-rate.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server("127.0.0.1",
                                             port,
                                             domains,
                                             queue,
                                             20,
                                             4096,
                                             30,
                                             nullptr,
                                             false,
                                             10,
                                             1000,
                                             nullptr,
                                             65536,
                                             128,
                                             2,
                                             60);
    server.start();
    int client_fd = -1;
    try {
        for (int accepted = 0; accepted < 2; ++accepted) {
            client_fd = connect_loopback(port);
            test::check(recv_line_with_timeout(client_fd, 1s) == "220 rapid-inbox-ingestd",
                        "connection within peer rate limit accepted");
            send_text(client_fd, "QUIT\r\n");
            test::check(recv_line_with_timeout(client_fd, 1s) == "221 2.0.0 Bye",
                        "rate-test connection quits cleanly");
            close_fd(client_fd);
            client_fd = -1;
        }
        client_fd = connect_loopback(port);
        test::check(recv_line_with_timeout(client_fd, 1s) ==
                        "421 connection rate limit exceeded",
                    "connection above per-peer window rejected");
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        throw;
    }
}

void test_smtp_server_supports_ipv6_loopback_bind() {
    const int port = reserve_ipv6_loopback_port();
    if (port < 0) {
        return;
    }
    rapid_inbox::ingestd::DomainCache domains("/tmp/rapid-inbox-smtp-server-ipv6.sqlite", 5000);
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpServer server("::", port, domains, queue, 20, 4096, 30);
    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_ipv6_loopback(port);
        test::check(recv_line_with_timeout(client_fd, 1s) == "220 rapid-inbox-ingestd",
                    "IPv6 listener accepts loopback connection");
        send_text(client_fd, "QUIT\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "221 2.0.0 Bye",
                    "IPv6 SMTP session responds");
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        throw;
    }
}

void test_smtp_server_defers_overlong_data_response_over_socket() {
    const fs::path root =
        fs::temp_directory_path() / "rapid-inbox-smtp-server-data-framing";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_domain_database(database_path);
    rapid_inbox::ingestd::DomainCache domains(database_path, 5000);
    domains.reload();
    rapid_inbox::ingestd::MailQueue queue(10, 4096);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server(
        "127.0.0.1", port, domains, queue, 20, 4096, 30, nullptr, false, 10, 512);
    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        (void)recv_line_with_timeout(client_fd, 1s);
        send_text(client_fd,
                  "HELO client.example\r\nMAIL FROM:<sender@example.com>\r\n"
                  "RCPT TO:<code@adb.com>\r\nDATA\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("250", 0) == 0, "HELO");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "MAIL");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "RCPT");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("354", 0) == 0, "DATA");

        send_text(client_fd, std::string(600, 'x') + "\r\n");
        pollfd poll_fd{.fd = client_fd, .events = POLLIN, .revents = 0};
        test::check(::poll(&poll_fd, 1, 150) == 0,
                    "overlong DATA line produces no early SMTP response");

        send_text(client_fd, "NOOP inside-body\r\n.\r\nNOOP\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "552 message too large",
                    "DATA failure returned once at terminator");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK",
                    "command after terminator remains frame-aligned");
        send_text(client_fd, "QUIT\r\n");
        (void)recv_line_with_timeout(client_fd, 1s);
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
        test::check(queue.total_size() == 0, "rejected overlong DATA leaves queue empty");
        fs::remove_all(root);
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        fs::remove_all(root);
        throw;
    }
}

void test_smtp_server_refreshes_domain_rules_at_mail_boundaries() {
    const fs::path root =
        fs::temp_directory_path() / "rapid-inbox-smtp-server-domain-refresh";
    fs::remove_all(root);
    fs::create_directories(root);
    const fs::path database_path = root / "app.db";
    initialize_domain_database(database_path);
    rapid_inbox::ingestd::DomainCache domains(database_path, 5000);
    domains.reload();
    rapid_inbox::ingestd::MailQueue queue(10, 4096);
    const int port = reserve_loopback_port();
    rapid_inbox::ingestd::SmtpServer server(
        "127.0.0.1", port, domains, queue, 20, 4096, 30);
    server.start();
    int client_fd = -1;
    try {
        client_fd = connect_loopback(port);
        (void)recv_line_with_timeout(client_fd, 1s);
        send_text(client_fd, "HELO client.example\r\nMAIL FROM:<sender@example.com>\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("250", 0) == 0, "HELO");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK",
                    "first MAIL captures initial domain generation");

        rapid_inbox::ingestd::SqliteDb db(database_path, 5000);
        db.exec("UPDATE domains SET max_message_size_bytes = 5, retention_days = 2 "
                "WHERE root_domain_ascii = 'adb.com'");
        const std::uint64_t initial_generation = domains.generation();
        domains.reload();
        test::check(domains.generation() > initial_generation, "domain reload advances generation");

        // The already-open transaction remains internally consistent even
        // though the cache has changed between MAIL and RCPT.
        send_text(client_fd, "RCPT TO:<code@adb.com>\r\nDATA\r\n1234\r\n.\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK",
                    "current transaction retains old routing snapshot");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("354", 0) == 0, "first DATA");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("250 queued as msg_", 0) == 0,
                    "current transaction retains old size policy");
        auto old_batch = queue.pop_batch(10, 100ms);
        test::check(old_batch.size() == 1, "old-policy transaction queued");
        test::check(old_batch[0].recipients[0].domain_policy.has_value(),
                    "old transaction carries a policy snapshot");
        test::check(!old_batch[0].recipients[0].domain_policy->retention_days.has_value(),
                    "old transaction does not mix in new retention policy");
        queue.complete_batch(1, old_batch[0].raw_content.size());

        // The next valid MAIL is the transaction boundary that adopts the new
        // immutable matcher/policy snapshot.
        send_text(client_fd,
                  "MAIL FROM:<sender@example.com>\r\nRCPT TO:<code@adb.com>\r\n"
                  "DATA\r\n1234\r\n.\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "second MAIL");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "second RCPT");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("354", 0) == 0, "second DATA");
        test::check(recv_line_with_timeout(client_fd, 1s) == "552 message too large",
                    "next transaction enforces reloaded size policy");

        send_text(client_fd,
                  "MAIL FROM:<sender@example.com>\r\nRCPT TO:<code@adb.com>\r\n"
                  "DATA\r\nx\r\n.\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "third MAIL");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "third RCPT");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("354", 0) == 0, "third DATA");
        test::check(recv_line_with_timeout(client_fd, 1s).rfind("250 queued as msg_", 0) == 0,
                    "message within reloaded size policy queues");
        auto new_batch = queue.pop_batch(10, 100ms);
        test::check(new_batch.size() == 1, "new-policy transaction queued");
        const auto& new_policy = new_batch[0].recipients[0].domain_policy;
        test::check(new_policy.has_value() && new_policy->max_message_size_bytes == 5,
                    "queued recipient stores reloaded size policy");
        test::check(new_policy->retention_days.has_value() && *new_policy->retention_days == 2,
                    "queued recipient stores reloaded retention policy");
        queue.complete_batch(1, new_batch[0].raw_content.size());

        db.exec("UPDATE domains SET is_active = 0 WHERE root_domain_ascii = 'adb.com'");
        domains.reload();
        send_text(client_fd,
                  "MAIL FROM:<sender@example.com>\r\nRCPT TO:<code@adb.com>\r\nQUIT\r\n");
        test::check(recv_line_with_timeout(client_fd, 1s) == "250 OK", "MAIL after disable");
        test::check(recv_line_with_timeout(client_fd, 1s) == "550 domain not allowed",
                    "long connection sees disabled domain at next MAIL boundary");
        test::check(recv_line_with_timeout(client_fd, 1s) == "221 2.0.0 Bye", "QUIT");
        close_fd(client_fd);
        client_fd = -1;
        server.stop();
        fs::remove_all(root);
    } catch (...) {
        close_fd(client_fd);
        server.stop();
        fs::remove_all(root);
        throw;
    }
}
