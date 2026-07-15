#pragma once

#include "domain_matcher.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace rapid_inbox::ingestd {

struct DomainPolicySnapshot {
    std::string root_domain_unicode;
    bool accept_exact = true;
    bool accept_subdomains = true;
    bool public_web_enabled = true;
    bool public_api_enabled = true;
    bool is_active = true;
    bool is_hidden = false;
    std::string plus_addressing_mode = "keep";
    bool local_part_case_sensitive = false;
    std::int64_t max_message_size_bytes = 52428800;
    std::optional<int> retention_days;
    std::string dns_status = "unknown";
};

struct RecipientDelivery {
    std::string delivery_id;
    std::string rcpt_to;
    DomainMatch match;
    std::optional<DomainPolicySnapshot> domain_policy;
};

struct MailJob {
    std::string smtp_session_id;
    std::string remote_ip = "unknown";
    std::string message_id;
    std::string envelope_from;
    std::string received_at;
    std::string raw_path;
    std::string manifest_path;
    std::string raw_sha256;
    std::string raw_content;
    std::vector<RecipientDelivery> recipients;
    bool artifacts_persisted = false;
};

}
