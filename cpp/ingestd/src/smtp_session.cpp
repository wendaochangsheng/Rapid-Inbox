#include "smtp_session.h"

#include "batch_writer.h"
#include "id.h"
#include "sha256.h"
#include "storage_path.h"
#include "time_utils.h"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

namespace rapid_inbox::ingestd {
namespace {

std::string upper_ascii(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return value;
}

bool starts_with_ci(const std::string& value, const std::string& prefix) {
    return upper_ascii(value.substr(0, prefix.size())) == upper_ascii(prefix);
}

bool matches_command_ci(const std::string& value, const std::string& command) {
    if (!starts_with_ci(value, command)) {
        return false;
    }
    return value.size() == command.size() ||
           std::isspace(static_cast<unsigned char>(value[command.size()]));
}

bool matches_no_arg_command_ci(const std::string& value, const std::string& command) {
    if (!starts_with_ci(value, command)) {
        return false;
    }
    return std::all_of(value.begin() + static_cast<std::string::difference_type>(command.size()),
                       value.end(),
                       [](unsigned char ch) { return std::isspace(ch); });
}

bool is_ascii_space(char ch) {
    return ch == ' ' || ch == '\t';
}

std::string upper_ascii_copy(std::string_view value) {
    std::string output(value);
    for (char& ch : output) {
        if (ch >= 'a' && ch <= 'z') {
            ch = static_cast<char>(ch - ('a' - 'A'));
        }
    }
    return output;
}

struct EsmtpParameter {
    std::string name;
    std::optional<std::string> value;
};

struct ParsedPathCommand {
    std::string path;
    std::vector<EsmtpParameter> parameters;
};

std::optional<ParsedPathCommand> parse_path_command(const std::string& line,
                                                    std::string_view command,
                                                    std::string_view path_keyword,
                                                    bool allow_empty_path) {
    if (!matches_command_ci(line, std::string(command))) {
        return std::nullopt;
    }
    std::size_t position = command.size();
    if (position >= line.size() || !is_ascii_space(line[position])) {
        return std::nullopt;
    }
    while (position < line.size() && is_ascii_space(line[position])) {
        ++position;
    }
    if (position + path_keyword.size() > line.size() ||
        upper_ascii_copy(std::string_view(line).substr(position, path_keyword.size())) !=
            upper_ascii_copy(path_keyword)) {
        return std::nullopt;
    }
    position += path_keyword.size();
    while (position < line.size() && is_ascii_space(line[position])) {
        ++position;
    }
    if (position >= line.size() || line[position] != '<') {
        return std::nullopt;
    }
    const std::size_t path_start = ++position;
    const std::size_t close = line.find('>', path_start);
    if (close == std::string::npos) {
        return std::nullopt;
    }
    std::string path = line.substr(path_start, close - path_start);
    if ((!allow_empty_path && path.empty()) || path.find('<') != std::string::npos ||
        std::any_of(path.begin(), path.end(), [](unsigned char ch) {
            return ch <= 0x20 || ch == 0x7f;
        })) {
        return std::nullopt;
    }

    position = close + 1;
    if (position < line.size() && !is_ascii_space(line[position])) {
        return std::nullopt;
    }
    std::vector<EsmtpParameter> parameters;
    std::unordered_set<std::string> seen_names;
    while (position < line.size()) {
        while (position < line.size() && is_ascii_space(line[position])) {
            ++position;
        }
        if (position == line.size()) {
            break;
        }
        const std::size_t token_start = position;
        while (position < line.size() && !is_ascii_space(line[position])) {
            const unsigned char ch = static_cast<unsigned char>(line[position]);
            if (ch < 0x21 || ch > 0x7e) {
                return std::nullopt;
            }
            ++position;
        }
        const std::string_view token(line.data() + token_start, position - token_start);
        const std::size_t equals = token.find('=');
        const std::string_view name = token.substr(0, equals);
        const auto is_keyword_alnum = [](unsigned char ch) {
            return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                   (ch >= '0' && ch <= '9');
        };
        if (name.empty() || !is_keyword_alnum(static_cast<unsigned char>(name.front())) ||
            !std::all_of(name.begin(), name.end(), [](unsigned char ch) {
                return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
                       (ch >= '0' && ch <= '9') || ch == '-';
            })) {
            return std::nullopt;
        }
        const std::string normalized_name = upper_ascii_copy(name);
        if (!seen_names.insert(normalized_name).second) {
            return std::nullopt;
        }
        std::optional<std::string> value;
        if (equals != std::string_view::npos) {
            const std::string_view raw_value = token.substr(equals + 1);
            if (raw_value.empty() || raw_value.find('=') != std::string_view::npos) {
                return std::nullopt;
            }
            value = std::string(raw_value);
        }
        parameters.push_back(EsmtpParameter{normalized_name, std::move(value)});
    }
    return ParsedPathCommand{std::move(path), std::move(parameters)};
}

bool parse_size_parameter(std::string_view value, std::size_t& parsed) {
    if (value.empty() ||
        !std::all_of(value.begin(), value.end(), [](unsigned char ch) {
            return ch >= '0' && ch <= '9';
        })) {
        return false;
    }
    std::uint64_t number = 0;
    const auto [end, error] = std::from_chars(value.data(), value.data() + value.size(), number);
    if (error != std::errc{} || end != value.data() + value.size() ||
        number > std::numeric_limits<std::size_t>::max()) {
        return false;
    }
    parsed = static_cast<std::size_t>(number);
    return true;
}

bool contains_non_ascii(std::string_view value) {
    return std::any_of(value.begin(), value.end(), [](unsigned char ch) { return ch >= 0x80; });
}

std::optional<std::string> command_argument(const std::string& line, std::string_view command) {
    if (!matches_command_ci(line, std::string(command))) {
        return std::nullopt;
    }
    std::size_t position = command.size();
    while (position < line.size() && is_ascii_space(line[position])) {
        ++position;
    }
    if (position == line.size()) {
        return std::nullopt;
    }
    const std::string argument = line.substr(position);
    if (argument.size() > 255 ||
        std::any_of(argument.begin(), argument.end(), [](unsigned char ch) {
            return ch <= 0x20 || ch == 0x7f;
        })) {
        return std::nullopt;
    }
    return argument;
}

bool claim_rejection_log_slot(std::atomic<std::int64_t>& next_log_ns) {
    using namespace std::chrono;
    const std::int64_t now =
        duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count();
    std::int64_t expected = next_log_ns.load(std::memory_order_relaxed);
    while (now >= expected) {
        if (next_log_ns.compare_exchange_weak(expected,
                                              now + duration_cast<nanoseconds>(seconds(1)).count(),
                                              std::memory_order_relaxed,
                                              std::memory_order_relaxed)) {
            return true;
        }
    }
    return false;
}

std::shared_ptr<const DomainRulesSnapshot> make_static_domain_rules(
    const DomainMatcher& matcher,
    std::unordered_map<int, DomainPolicySnapshot> policies) {
    return std::make_shared<const DomainRulesSnapshot>(DomainRulesSnapshot{
        .matcher = matcher,
        .policies = std::move(policies),
        .generation = 0,
    });
}

}

SmtpSession::SmtpSession(const DomainMatcher& matcher,
                         MailQueue& queue,
                         int max_recipients,
                         std::size_t max_message_size_bytes)
    : SmtpSession(matcher,
                  queue,
                  max_recipients,
                  max_message_size_bytes,
                  std::unordered_map<int, DomainPolicySnapshot>{},
                  nullptr,
                  false,
                  "unknown") {}

SmtpSession::SmtpSession(const DomainMatcher& matcher,
                         MailQueue& queue,
                         int max_recipients,
                         std::size_t max_message_size_bytes,
                         std::unordered_map<int, DomainPolicySnapshot> domain_policies,
                         BatchWriter* durable_writer,
                         bool durable_ack,
                         std::string remote_ip,
                         std::size_t reservation_chunk_bytes,
                         std::shared_ptr<IngestRuntimeStats> runtime_stats)
    : SmtpSession(make_static_domain_rules(matcher, std::move(domain_policies)),
                  nullptr,
                  queue,
                  max_recipients,
                  max_message_size_bytes,
                  durable_writer,
                  durable_ack,
                  std::move(remote_ip),
                  reservation_chunk_bytes,
                  std::move(runtime_stats)) {}

SmtpSession::SmtpSession(std::shared_ptr<const DomainRulesSnapshot> domain_rules,
                         const DomainCache* domain_cache,
                         MailQueue& queue,
                         int max_recipients,
                         std::size_t max_message_size_bytes,
                         BatchWriter* durable_writer,
                         bool durable_ack,
                         std::string remote_ip,
                         std::size_t reservation_chunk_bytes,
                         std::shared_ptr<IngestRuntimeStats> runtime_stats)
    : domain_rules_(std::move(domain_rules)),
      domain_cache_(domain_cache),
      queue_(queue),
      max_recipients_(max_recipients),
      max_message_size_bytes_(max_message_size_bytes),
      effective_message_size_bytes_(max_message_size_bytes),
      durable_writer_(durable_writer),
      durable_ack_(durable_ack),
      reservation_chunk_bytes_(std::max<std::size_t>(reservation_chunk_bytes, 1)),
      remote_ip_(std::move(remote_ip)),
      runtime_stats_(std::move(runtime_stats)),
      session_id_(make_prefixed_id("smtp_")) {
    if (domain_rules_ == nullptr) {
        throw std::invalid_argument("SMTP session requires domain rules");
    }
    if (Logger::instance().enabled(LogLevel::Debug)) {
        Logger::instance().log(LogLevel::Debug,
                               "smtp.session_started",
                               {
                                   {"session_id", session_id_},
                                   {"remote_ip", remote_ip_},
                               });
    }
}

SmtpSession::~SmtpSession() {
    release_queue_reservation();
    if (Logger::instance().enabled(LogLevel::Debug)) {
        Logger::instance().log(LogLevel::Debug,
                               "smtp.session_finished",
                               {
                                   {"session_id", session_id_},
                                   {"remote_ip", remote_ip_},
                               });
    }
}

std::string SmtpSession::greeting() const {
    return "220 rapid-inbox-ingestd";
}

std::string SmtpSession::ehlo_response() const {
    return "250-rapid-inbox-ingestd\r\n250-SIZE " +
           std::to_string(max_message_size_bytes_) +
           "\r\n250-8BITMIME\r\n250-PIPELINING\r\n250 SMTPUTF8";
}

std::string SmtpSession::handle_line(const std::string& line) {
    if (in_data_) {
        if (line == ".") {
            if (data_too_large_) {
                clear_transaction_state();
                return "552 message too large";
            }
            if (data_queue_full_) {
                clear_transaction_state();
                return "451 temporary queue full";
            }
            return finish_data();
        }
        if (data_too_large_) {
            return "";
        }
        const std::size_t content_offset = !line.empty() && line.front() == '.' ? 1 : 0;
        const std::size_t content_size = line.size() - content_offset;
        if (data_octets_received_ > effective_message_size_bytes_ ||
            content_size > effective_message_size_bytes_ - data_octets_received_ ||
            2 > effective_message_size_bytes_ - data_octets_received_ - content_size) {
            mark_data_too_large("message_too_large");
            return "";
        }
        data_octets_received_ += content_size + 2;
        if (data_queue_full_) {
            return "";
        }
        if (!ensure_data_reservation(data_octets_received_)) {
            mark_data_queue_full();
            return "";
        }
        data_.append(line.data() + static_cast<std::ptrdiff_t>(content_offset), content_size);
        data_ += "\r\n";
        return "";
    }
    return handle_command(line);
}

void SmtpSession::reject_overlong_data_line() {
    if (in_data_) {
        mark_data_too_large("data_line_too_long");
    }
}

std::string SmtpSession::handle_command(const std::string& line) {
    if (matches_command_ci(line, "EHLO")) {
        if (!command_argument(line, "EHLO").has_value()) {
            log_rejection(LogLevel::Debug, "ehlo", "invalid_argument");
            return "501 invalid EHLO argument";
        }
        clear_transaction_state();
        extended_smtp_ = true;
        return ehlo_response();
    }
    if (matches_command_ci(line, "HELO")) {
        if (!command_argument(line, "HELO").has_value()) {
            log_rejection(LogLevel::Debug, "helo", "invalid_argument");
            return "501 invalid HELO argument";
        }
        clear_transaction_state();
        extended_smtp_ = false;
        return "250 rapid-inbox-ingestd";
    }
    if (matches_command_ci(line, "NOOP")) {
        return "250 OK";
    }
    if (matches_command_ci(line, "VRFY")) {
        std::size_t position = 4;
        while (position < line.size() && is_ascii_space(line[position])) {
            ++position;
        }
        if (position == line.size()) {
            return "501 invalid VRFY argument";
        }
        return "252 Cannot VRFY user";
    }
    if (matches_no_arg_command_ci(line, "QUIT")) {
        return "221 2.0.0 Bye";
    }
    if (matches_no_arg_command_ci(line, "RSET")) {
        clear_transaction_state();
        return "250 OK";
    }
    if (matches_command_ci(line, "MAIL")) {
        auto parsed = parse_path_command(line, "MAIL", "FROM:", true);
        if (!parsed.has_value()) {
            log_rejection(LogLevel::Debug, "mail_from", "invalid_sender");
            return "501 invalid sender";
        }
        if (!parsed->parameters.empty() && !extended_smtp_) {
            log_rejection(LogLevel::Debug, "mail_from", "ehlo_required");
            return "503 send EHLO before ESMTP parameters";
        }

        std::optional<std::size_t> declared_size;
        bool smtputf8 = false;
        for (const EsmtpParameter& parameter : parsed->parameters) {
            if (parameter.name == "SIZE") {
                std::size_t size = 0;
                if (!parameter.value.has_value() || !parse_size_parameter(*parameter.value, size)) {
                    log_rejection(LogLevel::Debug, "mail_from", "invalid_size_parameter");
                    return "501 invalid SIZE parameter";
                }
                if (size > max_message_size_bytes_) {
                    log_rejection(LogLevel::Debug, "mail_from", "declared_size_too_large");
                    return "552 message size exceeds fixed maximum";
                }
                declared_size = size;
                continue;
            }
            if (parameter.name == "SMTPUTF8") {
                if (parameter.value.has_value()) {
                    log_rejection(LogLevel::Debug, "mail_from", "invalid_smtputf8_parameter");
                    return "501 invalid SMTPUTF8 parameter";
                }
                smtputf8 = true;
                continue;
            }
            if (parameter.name == "BODY") {
                if (!parameter.value.has_value()) {
                    log_rejection(LogLevel::Debug, "mail_from", "invalid_body_parameter");
                    return "501 invalid BODY parameter";
                }
                const std::string body = upper_ascii_copy(*parameter.value);
                if (body != "7BIT" && body != "8BITMIME") {
                    log_rejection(LogLevel::Debug, "mail_from", "unsupported_body_parameter");
                    return "555 unsupported BODY parameter";
                }
                continue;
            }
            log_rejection(LogLevel::Debug, "mail_from", "unsupported_parameter");
            return "555 unsupported MAIL FROM parameter";
        }

        if (!parsed->path.empty()) {
            if (contains_non_ascii(parsed->path) && !smtputf8) {
                log_rejection(LogLevel::Debug, "mail_from", "smtputf8_required");
                return "553 SMTPUTF8 required";
            }
            if (!parse_mailbox_address(parsed->path, smtputf8).has_value()) {
                log_rejection(LogLevel::Debug, "mail_from", "invalid_sender");
                return "501 invalid sender";
            }
        }

        // A valid MAIL command starts a new transaction. Refresh immutable
        // routing rules here so a long-lived connection cannot retain disabled
        // domains or stale per-domain policy indefinitely.
        refresh_domain_rules();
        mail_from_ = std::move(parsed->path);
        mail_from_seen_ = true;
        mail_smtputf8_ = smtputf8;
        declared_message_size_ = declared_size;
        recipients_.clear();
        release_data_buffer();
        data_too_large_ = false;
        data_queue_full_ = false;
        data_octets_received_ = 0;
        effective_message_size_bytes_ = max_message_size_bytes_;
        return "250 OK";
    }
    if (matches_command_ci(line, "RCPT")) {
        if (!mail_from_seen_) {
            log_rejection(LogLevel::Debug, "rcpt_to", "mail_from_required");
            return "503 need MAIL FROM first";
        }
        auto parsed = parse_path_command(line, "RCPT", "TO:", false);
        if (!parsed.has_value()) {
            log_rejection(LogLevel::Debug, "rcpt_to", "invalid_recipient");
            return "501 invalid recipient";
        }
        if (!parsed->parameters.empty()) {
            log_rejection(LogLevel::Debug, "rcpt_to", "unsupported_parameter");
            return "555 unsupported RCPT TO parameter";
        }
        if (contains_non_ascii(parsed->path) && !mail_smtputf8_) {
            log_rejection(LogLevel::Debug, "rcpt_to", "smtputf8_required");
            return "553 SMTPUTF8 required";
        }
        auto match = domain_rules_->matcher.match_address(parsed->path);
        if (!match.has_value()) {
            log_rejection(LogLevel::Debug, "rcpt_to", "domain_not_allowed");
            count_recipient_rejection();
            return "550 domain not allowed";
        }
        const auto duplicate = std::find_if(
            recipients_.begin(), recipients_.end(), [&](const RecipientDelivery& recipient) {
                return recipient.match.address_canonical == match->address_canonical;
            });
        if (duplicate != recipients_.end()) {
            return "250 OK";
        }
        if (static_cast<int>(recipients_.size()) >= max_recipients_) {
            log_rejection(LogLevel::Warning, "rcpt_to", "recipient_limit");
            count_recipient_rejection();
            return "552 too many recipients";
        }
        std::optional<DomainPolicySnapshot> domain_policy;
        const auto policy = domain_rules_->policies.find(match->domain_id);
        if (policy != domain_rules_->policies.end()) {
            domain_policy = policy->second;
            if (policy->second.max_message_size_bytes > 0) {
                const std::size_t candidate_limit =
                    std::min(effective_message_size_bytes_,
                             static_cast<std::size_t>(policy->second.max_message_size_bytes));
                if (declared_message_size_.has_value() &&
                    *declared_message_size_ > candidate_limit) {
                    log_rejection(LogLevel::Debug,
                                  "rcpt_to",
                                  "declared_size_exceeds_recipient_limit");
                    return "552 message size exceeds recipient limit";
                }
                effective_message_size_bytes_ = candidate_limit;
            }
        }
        recipients_.push_back(
            RecipientDelivery{make_prefixed_id("dlv_"),
                              parsed->path,
                              *match,
                              std::move(domain_policy)});
        return "250 OK";
    }
    if (matches_no_arg_command_ci(line, "DATA")) {
        if (recipients_.empty()) {
            log_rejection(LogLevel::Debug, "data", "valid_recipient_required");
            return "554 no valid recipients";
        }
        if (durable_writer_ != nullptr && durable_writer_->maintenance_active()) {
            log_rejection(LogLevel::Info, "data", "maintenance");
            return "451 storage maintenance in progress";
        }
        if (!queue_.try_reserve(0)) {
            log_rejection(LogLevel::Warning, "data", "queue_capacity");
            return "451 temporary queue full";
        }
        queue_reservation_active_ = true;
        queue_reservation_bytes_ = 0;
        in_data_ = true;
        data_too_large_ = false;
        data_queue_full_ = false;
        data_octets_received_ = 0;
        release_data_buffer();
        return "354 End data with <CR><LF>.<CR><LF>";
    }
    log_rejection(LogLevel::Debug, "command", "unsupported_command");
    return "502 command not implemented";
}

void SmtpSession::count_recipient_rejection() const noexcept {
    if (runtime_stats_ != nullptr) {
        runtime_stats_->rejected_recipients_pending.fetch_add(1, std::memory_order_relaxed);
    }
}

void SmtpSession::refresh_domain_rules() {
    if (domain_cache_ == nullptr) {
        return;
    }
    auto refreshed = domain_cache_->snapshot_rules_if_changed(domain_rules_->generation);
    if (refreshed != nullptr) {
        domain_rules_ = std::move(refreshed);
    }
}

bool SmtpSession::ensure_data_reservation(std::size_t required_bytes) {
    if (!queue_reservation_active_) {
        return false;
    }
    if (required_bytes <= queue_reservation_bytes_) {
        return true;
    }
    const std::size_t minimum_additional = required_bytes - queue_reservation_bytes_;
    std::size_t preferred_target = effective_message_size_bytes_;
    if (required_bytes <=
        std::numeric_limits<std::size_t>::max() - (reservation_chunk_bytes_ - 1)) {
        const std::size_t rounded =
            ((required_bytes + reservation_chunk_bytes_ - 1) / reservation_chunk_bytes_) *
            reservation_chunk_bytes_;
        preferred_target = std::min(rounded, effective_message_size_bytes_);
    }
    preferred_target = std::max(preferred_target, required_bytes);
    const std::size_t preferred_additional = preferred_target - queue_reservation_bytes_;
    const std::size_t added =
        queue_.try_grow_reservation(minimum_additional, preferred_additional);
    if (added == 0) {
        return false;
    }
    queue_reservation_bytes_ += added;
    return true;
}

void SmtpSession::mark_data_too_large(std::string_view reason) {
    if (!data_too_large_) {
        log_rejection(LogLevel::Warning, "data", reason);
    }
    data_too_large_ = true;
    release_data_buffer();
    release_queue_reservation();
}

void SmtpSession::mark_data_queue_full() {
    if (!data_queue_full_) {
        log_rejection(LogLevel::Warning, "data", "queue_capacity");
    }
    data_queue_full_ = true;
    release_data_buffer();
    release_queue_reservation();
}

std::string SmtpSession::finish_data() {
    in_data_ = false;
    if (durable_writer_ != nullptr && durable_writer_->maintenance_active()) {
        log_rejection(LogLevel::Warning, "data_commit", "maintenance");
        clear_transaction_state();
        return "451 storage maintenance in progress";
    }
    const std::string received_at = utc_now();
    MailJob job;
    job.smtp_session_id = session_id_;
    job.remote_ip = remote_ip_;
    job.message_id = make_prefixed_id("msg_");
    job.envelope_from = mail_from_;
    job.received_at = received_at;
    job.raw_content = std::move(data_);
    job.raw_sha256 = sha256_hex(job.raw_content);
    job.raw_path = raw_message_path(job.message_id, received_at);
    job.manifest_path = manifest_path(job.message_id, received_at);
    job.recipients = recipients_;
    const std::size_t raw_size = job.raw_content.size();
    if (!queue_reservation_active_) {
        log_rejection(LogLevel::Warning, "data_commit", "reservation_unavailable");
        clear_transaction_state();
        return "451 temporary queue full";
    }
    if (raw_size > queue_reservation_bytes_) {
        log_rejection(LogLevel::Warning, "data_commit", "reservation_too_small");
        clear_transaction_state();
        return "451 temporary queue full";
    }
    if (durable_ack_) {
        if (durable_writer_ == nullptr) {
            Logger::instance().log(LogLevel::Error,
                                   "smtp.durable_persist_failed",
                                   {
                                       {"session_id", session_id_},
                                       {"remote_ip", remote_ip_},
                                       {"reason", "writer_unavailable"},
                                   });
            clear_transaction_state();
            return "451 durable storage unavailable";
        }
        try {
            durable_writer_->write_pending_artifacts(job);
            job.artifacts_persisted = true;
        } catch (const std::exception& exc) {
            Logger::instance().log(LogLevel::Error,
                                   "smtp.durable_persist_failed",
                                   {
                                       {"session_id", session_id_},
                                       {"remote_ip", remote_ip_},
                                       {"reason", "storage_failure"},
                                       {"error", exc.what()},
                                   });
            clear_transaction_state();
            return "451 temporary storage failure";
        }
    }
    if (!job.artifacts_persisted && durable_writer_ != nullptr &&
        durable_writer_->maintenance_active()) {
        log_rejection(LogLevel::Warning, "data_commit", "maintenance");
        clear_transaction_state();
        return "451 storage maintenance in progress";
    }
    const std::string message_id = job.message_id;
    const bool durable_owned = job.artifacts_persisted;
    const std::size_t recipient_count = job.recipients.size();
    const std::size_t reserved_bytes = queue_reservation_bytes_;
    if (!queue_.push_reserved(std::move(job), reserved_bytes)) {
        queue_reservation_active_ = false;
        queue_reservation_bytes_ = 0;
        clear_transaction_state();
        if (durable_owned) {
            Logger::instance().log(LogLevel::Warning,
                                   "smtp.message_recovery_deferred",
                                   {
                                       {"session_id", session_id_},
                                       {"message_id", message_id},
                                       {"remote_ip", remote_ip_},
                                       {"reason", "queue_unavailable_after_durable_persist"},
                                   });
            return "250 queued as " + message_id;
        }
        log_rejection(LogLevel::Warning, "data_commit", "queue_unavailable");
        return "451 temporary queue unavailable";
    }
    queue_reservation_active_ = false;
    queue_reservation_bytes_ = 0;
    clear_transaction_state();
    if (Logger::instance().enabled(LogLevel::Debug)) {
        Logger::instance().log(LogLevel::Debug,
                               "smtp.message_accepted",
                               {
                                   {"session_id", session_id_},
                                   {"message_id", message_id},
                                   {"remote_ip", remote_ip_},
                                   {"raw_size_bytes", raw_size},
                                   {"recipient_count", recipient_count},
                                   {"durable", durable_owned},
                               });
    }
    return "250 queued as " + message_id;
}

void SmtpSession::log_rejection(LogLevel level,
                                std::string_view stage,
                                std::string_view reason) const {
    if (!Logger::instance().enabled(level)) {
        return;
    }
    static std::atomic<std::int64_t> next_capacity_log_ns{0};
    static std::atomic<std::int64_t> next_maintenance_log_ns{0};
    if ((reason == "queue_capacity" || reason == "queue_unavailable") &&
        !claim_rejection_log_slot(next_capacity_log_ns)) {
        return;
    }
    if (reason == "maintenance" && !claim_rejection_log_slot(next_maintenance_log_ns)) {
        return;
    }
    Logger::instance().log(level,
                           "smtp.transaction_rejected",
                           {
                               {"session_id", session_id_},
                               {"remote_ip", remote_ip_},
                               {"stage", stage},
                               {"reason", reason},
                           });
}

void SmtpSession::clear_transaction_state() {
    mail_from_.clear();
    mail_from_seen_ = false;
    mail_smtputf8_ = false;
    declared_message_size_.reset();
    recipients_.clear();
    release_data_buffer();
    release_queue_reservation();
    in_data_ = false;
    data_too_large_ = false;
    data_queue_full_ = false;
    data_octets_received_ = 0;
    effective_message_size_bytes_ = max_message_size_bytes_;
}

void SmtpSession::release_data_buffer() {
    std::string empty;
    data_.swap(empty);
}

void SmtpSession::release_queue_reservation() {
    if (!queue_reservation_active_) {
        return;
    }
    queue_.cancel_reservation(queue_reservation_bytes_);
    queue_reservation_active_ = false;
    queue_reservation_bytes_ = 0;
}

}
