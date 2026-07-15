#pragma once

#include "domain_cache.h"
#include "logger.h"
#include "mail_queue.h"
#include "runtime_stats.h"

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace rapid_inbox::ingestd {

class BatchWriter;

class SmtpSession {
public:
    SmtpSession(const DomainMatcher& matcher,
                MailQueue& queue,
                int max_recipients,
                std::size_t max_message_size_bytes);
    SmtpSession(const DomainMatcher& matcher,
                MailQueue& queue,
                int max_recipients,
                std::size_t max_message_size_bytes,
                std::unordered_map<int, DomainPolicySnapshot> domain_policies,
                BatchWriter* durable_writer = nullptr,
                bool durable_ack = false,
                std::string remote_ip = "unknown",
                std::size_t reservation_chunk_bytes = 65536,
                std::shared_ptr<IngestRuntimeStats> runtime_stats = nullptr);
    SmtpSession(std::shared_ptr<const DomainRulesSnapshot> domain_rules,
                const DomainCache* domain_cache,
                MailQueue& queue,
                int max_recipients,
                std::size_t max_message_size_bytes,
                BatchWriter* durable_writer = nullptr,
                bool durable_ack = false,
                std::string remote_ip = "unknown",
                std::size_t reservation_chunk_bytes = 65536,
                std::shared_ptr<IngestRuntimeStats> runtime_stats = nullptr);
    ~SmtpSession();

    std::string greeting() const;
    std::string handle_line(const std::string& line);
    bool in_data() const noexcept { return in_data_; }
    void reject_overlong_data_line();

private:
    std::string handle_command(const std::string& line);
    std::string ehlo_response() const;
    std::string finish_data();
    bool ensure_data_reservation(std::size_t required_bytes);
    void mark_data_too_large(std::string_view reason);
    void mark_data_queue_full();
    void refresh_domain_rules();
    void clear_transaction_state();
    void release_data_buffer();
    void release_queue_reservation();
    void log_rejection(LogLevel level,
                       std::string_view stage,
                       std::string_view reason) const;
    void count_recipient_rejection() const noexcept;

    std::shared_ptr<const DomainRulesSnapshot> domain_rules_;
    const DomainCache* domain_cache_;
    MailQueue& queue_;
    int max_recipients_;
    std::size_t max_message_size_bytes_;
    std::size_t effective_message_size_bytes_;
    BatchWriter* durable_writer_;
    bool durable_ack_;
    std::size_t reservation_chunk_bytes_;
    std::string remote_ip_;
    std::shared_ptr<IngestRuntimeStats> runtime_stats_;
    std::string session_id_;
    std::string mail_from_;
    bool mail_from_seen_ = false;
    bool extended_smtp_ = false;
    bool mail_smtputf8_ = false;
    std::optional<std::size_t> declared_message_size_;
    std::vector<RecipientDelivery> recipients_;
    bool in_data_ = false;
    bool data_too_large_ = false;
    bool data_queue_full_ = false;
    std::size_t data_octets_received_ = 0;
    bool queue_reservation_active_ = false;
    std::size_t queue_reservation_bytes_ = 0;
    std::string data_;
};

}
