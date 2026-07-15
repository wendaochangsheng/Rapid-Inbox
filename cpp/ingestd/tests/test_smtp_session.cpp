#include "../src/batch_writer.h"
#include "../src/domain_matcher.h"
#include "../src/mail_queue.h"
#include "../src/smtp_session.h"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

std::string expected_ehlo(std::size_t maximum_size) {
    return "250-rapid-inbox-ingestd\r\n250-SIZE " + std::to_string(maximum_size) +
           "\r\n250-8BITMIME\r\n250-PIPELINING\r\n250 SMTPUTF8";
}

}  // namespace

void test_smtp_session_accepts_valid_message() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("EHLO client") == expected_ehlo(1024 * 1024), "ehlo");
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("Subject: Hi") == "", "data line no response");
    test::check(session.handle_line("") == "", "blank data line");
    const std::string queued = session.handle_line(".");
    test::check(queued.rfind("250 queued as msg_", 0) == 0, "queued response");
    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "queued one job");
    test::check(batch[0].recipients[0].match.address_canonical == "code@adb.com", "canonical recipient");
}

void test_smtp_session_advertises_esmtp_and_supports_noop() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10, 1024);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1234);
    test::check(session.handle_line("EHLO") == "501 invalid EHLO argument",
                "EHLO requires an argument");
    test::check(session.handle_line("EHLO client.example") == expected_ehlo(1234),
                "EHLO advertises required extensions and SIZE limit");
    test::check(session.handle_line("NOOP") == "250 OK", "bare NOOP accepted");
    test::check(session.handle_line("NOOP health-check") == "250 OK",
                "NOOP optional text accepted");
    test::check(session.handle_line("HELO client.example") == "250 rapid-inbox-ingestd",
                "HELO remains supported");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=1") ==
                    "503 send EHLO before ESMTP parameters",
                "ESMTP parameters require EHLO mode");
}

void test_smtp_session_refuses_vrfy_without_disclosing_input() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10, 1024);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1234);

    const std::string response = session.handle_line("VRFY private-token@adb.com");
    test::check(response == "252 Cannot VRFY user", "VRFY returns privacy-preserving 252");
    test::check(response.find("private-token") == std::string::npos,
                "VRFY response never echoes attacker-controlled input");
    test::check(session.handle_line("vrfy Display Name") == "252 Cannot VRFY user",
                "VRFY accepts an opaque string without enumeration");
    test::check(session.handle_line("VRFY") == "501 invalid VRFY argument",
                "bare VRFY requires an argument");
    test::check(session.handle_line("VRFYX user") == "502 command not implemented",
                "VRFY prefix collision is not treated as VRFY");
}

void test_smtp_session_parses_mail_and_rcpt_esmtp_parameters() {
    using namespace rapid_inbox::ingestd;
    DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    policy.max_message_size_bytes = 100;
    MailQueue queue(10, 1024);
    SmtpSession session(matcher,
                        queue,
                        20,
                        1024,
                        std::unordered_map<int, DomainPolicySnapshot>{{1, policy}});

    test::check(session.handle_line("EHLO client") == expected_ehlo(1024), "EHLO enabled");
    test::check(session.handle_line("MAIL FROM:<> SIZE=20 BODY=8bitmime SMTPUTF8") == "250 OK",
                "null reverse path and supported MAIL parameters accepted");
    test::check(session.handle_line("RCPT TO:<code@adb.com> NOTIFY=SUCCESS") ==
                    "555 unsupported RCPT TO parameter",
                "unadvertised RCPT parameter rejected");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK",
                "recipient remains usable after parameter rejection");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("x") == "", "body");
    test::check(session.handle_line(".").rfind("250 queued as msg_", 0) == 0, "queued");
    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1 && batch[0].envelope_from.empty(),
                "null reverse path is stored as an empty envelope sender");

    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=abc") ==
                    "501 invalid SIZE parameter",
                "non-numeric SIZE rejected");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=1025") ==
                    "552 message size exceeds fixed maximum",
                "declared global oversize rejected before DATA");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> RET=FULL") ==
                    "555 unsupported MAIL FROM parameter",
                "unadvertised MAIL parameter rejected");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=1 SIZE=2") ==
                    "501 invalid sender",
                "duplicate ESMTP parameter rejected as malformed");
    test::check(session.handle_line("MAIL FROM:<sender@example.com>SIZE=1") ==
                    "501 invalid sender",
                "parameter requires whitespace after path");
}

void test_smtp_session_rejects_declared_size_at_recipient_policy() {
    using namespace rapid_inbox::ingestd;
    DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    policy.max_message_size_bytes = 10;
    MailQueue queue(10, 1024);
    SmtpSession session(matcher,
                        queue,
                        20,
                        1024,
                        std::unordered_map<int, DomainPolicySnapshot>{{1, policy}});
    (void)session.handle_line("EHLO client");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=20") == "250 OK",
                "SIZE fits global limit");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") ==
                    "552 message size exceeds recipient limit",
                "recipient domain size rejects before DATA");
    test::check(session.handle_line("MAIL FROM:<sender@example.com> SIZE=10") == "250 OK",
                "smaller transaction accepted");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK",
                "SIZE at domain limit accepted");
}

void test_smtp_session_enforces_smtputf8_parameter() {
    using namespace rapid_inbox::ingestd;
    DomainMatcher matcher({{1, "example.com", true, true, "keep", false}});
    MailQueue queue(10, 1024);
    SmtpSession session(matcher, queue, 20, 1024);
    (void)session.handle_line("EHLO client");
    const std::string utf8_sender = "MAIL FROM:<\xC3\x9C" "ser@example.com>";
    test::check(session.handle_line(utf8_sender) == "553 SMTPUTF8 required",
                "UTF-8 sender requires SMTPUTF8");
    test::check(session.handle_line(utf8_sender + " SMTPUTF8") == "250 OK",
                "UTF-8 sender accepted with SMTPUTF8");
    test::check(session.handle_line("RCPT TO:<\xC3\x9C" "ser@example.com>") == "250 OK",
                "UTF-8 recipient accepted in SMTPUTF8 transaction");
    test::check(session.handle_line("RSET") == "250 OK", "reset UTF-8 transaction");
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK",
                "ASCII transaction accepted");
    test::check(session.handle_line("RCPT TO:<\xC3\x9C" "ser@example.com>") ==
                    "553 SMTPUTF8 required",
                "UTF-8 recipient rejected without MAIL SMTPUTF8");
}

void test_smtp_session_reserves_bytes_in_chunks_and_defers_pressure_response() {
    using namespace rapid_inbox::ingestd;
    DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    MailQueue queue(20, 100);
    std::vector<std::unique_ptr<SmtpSession>> sessions;
    for (int index = 0; index < 10; ++index) {
        auto session = std::make_unique<SmtpSession>(
            matcher,
            queue,
            20,
            50,
            std::unordered_map<int, DomainPolicySnapshot>{},
            nullptr,
            false,
            "192.0.2.1",
            8);
        test::check(session->handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail");
        test::check(session->handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
        test::check(session->handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>",
                    "many DATA transactions reserve message slots without maximum bytes");
        sessions.push_back(std::move(session));
    }
    auto stats = queue.stats();
    test::check(stats.reserved_messages == 10, "all DATA message slots are reserved");
    test::check(stats.reserved_bytes == 0, "DATA start does not reserve whole-message bytes");

    for (auto& session : sessions) {
        test::check(session->handle_line("x") == "", "small body line accepted");
    }
    stats = queue.stats();
    test::check(stats.reserved_bytes == 80, "body bytes grow in 8-byte chunks");
    sessions.clear();
    test::check(queue.total_size() == 0 && queue.total_size_bytes() == 0,
                "disconnect releases chunk reservations exactly");

    MailQueue pressured_queue(2, 5);
    SmtpSession pressured(matcher,
                          pressured_queue,
                          20,
                          100,
                          std::unordered_map<int, DomainPolicySnapshot>{},
                          nullptr,
                          false,
                          "192.0.2.2",
                          8);
    (void)pressured.handle_line("MAIL FROM:<sender@example.com>");
    (void)pressured.handle_line("RCPT TO:<code@adb.com>");
    test::check(pressured.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>",
                "DATA slot accepted before byte pressure");
    test::check(pressured.handle_line("1234") == "",
                "byte pressure is consumed without an early SMTP response");
    test::check(pressured_queue.total_size() == 0,
                "failed body growth releases slot and bytes while discarding");
    test::check(pressured.handle_line("more") == "", "remaining body is discarded");
    test::check(pressured.handle_line(".") == "451 temporary queue full",
                "byte pressure is reported once at DATA terminator");
}

void test_smtp_session_defers_overlong_data_line_failure_to_terminator() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(2, 1024);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024);
    (void)session.handle_line("MAIL FROM:<sender@example.com>");
    (void)session.handle_line("RCPT TO:<code@adb.com>");
    (void)session.handle_line("DATA");
    session.reject_overlong_data_line();
    test::check(session.handle_line("discarded") == "", "overlong DATA transaction is discarded");
    test::check(session.handle_line(".") == "552 message too large",
                "overlong DATA line failure is emitted at terminator");
}

void test_smtp_session_attaches_domain_policy_snapshot() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "strip", true}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "ADB.COM";
    policy.accept_exact = true;
    policy.accept_subdomains = false;
    policy.public_web_enabled = false;
    policy.public_api_enabled = true;
    policy.is_active = true;
    policy.is_hidden = true;
    policy.plus_addressing_mode = "strip";
    policy.local_part_case_sensitive = true;
    policy.max_message_size_bytes = 12345;
    policy.retention_days = 7;
    policy.dns_status = "warning";

    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024 * 1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}});

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<User+Tag@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("Subject: Policy") == "", "body");
    test::check(session.handle_line(".").rfind("250 queued as msg_", 0) == 0, "queued");

    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "queued one job");
    test::check(batch[0].recipients.size() == 1, "queued one recipient");
    const auto& snapshot = batch[0].recipients[0].domain_policy;
    test::check(snapshot.has_value(), "recipient has domain policy");
    test::check(snapshot->root_domain_unicode == "ADB.COM", "domain policy root unicode");
    test::check(snapshot->accept_subdomains == false, "domain policy accept subdomains");
    test::check(snapshot->public_web_enabled == false, "domain policy public web");
    test::check(snapshot->is_hidden == true, "domain policy hidden");
    test::check(snapshot->plus_addressing_mode == "strip", "domain policy plus mode");
    test::check(snapshot->local_part_case_sensitive == true, "domain policy case mode");
    test::check(snapshot->max_message_size_bytes == 12345, "domain policy max size");
    test::check(snapshot->retention_days.has_value() && *snapshot->retention_days == 7,
                "domain policy retention");
    test::check(snapshot->dns_status == "warning", "domain policy dns");
}

void test_smtp_session_rejects_unknown_domain() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@unknown.com>") == "550 domain not allowed", "unknown rejected");
}

void test_smtp_session_counts_only_dashboard_recipient_rejections() {
    using namespace rapid_inbox::ingestd;
    DomainMatcher matcher({{1, "adb.com", true, true, "strip", false}});
    MailQueue queue(10, 1024 * 1024);
    auto runtime_stats = std::make_shared<IngestRuntimeStats>();
    SmtpSession session(matcher,
                        queue,
                        1,
                        1024 * 1024,
                        std::unordered_map<int, DomainPolicySnapshot>{},
                        nullptr,
                        false,
                        "192.0.2.44",
                        65536,
                        runtime_stats);

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK",
                "mail from");
    test::check(session.handle_line("RCPT TO:<User+one@adb.com>") == "250 OK",
                "first legal recipient accepted");
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_relaxed) == 0,
                "legal recipient is not counted as rejected");
    test::check(session.handle_line("RCPT TO:<user+two@adb.com>") == "250 OK",
                "canonical duplicate recipient accepted idempotently");
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_relaxed) == 0,
                "canonical duplicate recipient is not counted as rejected");

    test::check(session.handle_line("RCPT TO:<other@unknown.example>") ==
                    "550 domain not allowed",
                "unknown domain recipient rejected");
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_relaxed) == 1,
                "domain-not-allowed rejection increments dashboard counter");
    test::check(session.handle_line("RCPT TO:<other@adb.com>") == "552 too many recipients",
                "distinct recipient above transaction limit rejected");
    test::check(runtime_stats->rejected_recipients_pending.load(std::memory_order_relaxed) == 2,
                "recipient-limit rejection increments dashboard counter");
}

void test_smtp_session_rejects_prefix_collision_commands() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("EHLOX client") == "502 command not implemented", "ehlox rejected");
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATAX") == "502 command not implemented", "datax rejected");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data still accepted");
}

void test_smtp_session_clears_transaction_after_queueing() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("Subject: First") == "", "body");
    test::check(session.handle_line(".").rfind("250 queued as msg_", 0) == 0, "queued");
    test::check(session.handle_line("DATA") == "554 no valid recipients", "second data rejects stale recipients");
    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "only first message queued");
}

void test_smtp_session_rejects_rcpt_before_mail_from() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "503 need MAIL FROM first", "rcpt before mail");
    test::check(session.handle_line("DATA") == "554 no valid recipients", "no recipient after rejected rcpt");
}

void test_smtp_session_accepts_null_reverse_path_and_preserves_state_on_invalid_mail() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("MAIL FROM:<>") == "250 OK", "null reverse path accepted");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK",
                "null reverse path establishes MAIL state");
    test::check(session.handle_line("RSET") == "250 OK", "reset null reverse-path transaction");
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "valid sender");
    test::check(session.handle_line("MAIL FROM:   ") == "501 invalid sender", "blank sender rejected");
    test::check(session.handle_line("MAIL FROM:<broken@example.com") == "501 invalid sender", "malformed sender rejected");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "valid sender retained");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line(".").rfind("250 queued as msg_", 0) == 0, "queued");
    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "queued one message");
    test::check(batch[0].envelope_from == "sender@example.com",
                "invalid sender did not replace valid sender");
}

void test_smtp_session_rejects_data_arguments() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA anything") == "502 command not implemented", "data arguments rejected");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "bare data accepted");
}

void test_smtp_session_discards_oversized_data_until_terminator() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(10);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 5);
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("123456") == "", "oversize line is consumed silently");
    test::check(session.handle_line("EHLO body") == "", "oversize body discarded");
    test::check(session.handle_line(".") == "552 message too large",
                "oversize failure is returned once at terminator");
    test::check(session.handle_line("DATA") == "554 no valid recipients", "oversize transaction cleared");
    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.empty(), "oversize message not queued");
}

void test_smtp_session_reports_queue_full() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(0);
    rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 1024 * 1024);
    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<Code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "451 temporary queue full",
                "queue pressure rejected before accepting body");
    test::check(queue.size() == 0, "queue remains empty");
}

void test_smtp_session_deduplicates_canonical_recipients() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "strip", false}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    policy.plus_addressing_mode = "strip";
    rapid_inbox::ingestd::MailQueue queue(10, 1024 * 1024);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024 * 1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}});

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<User+one@adb.com>") == "250 OK", "first alias");
    test::check(session.handle_line("RCPT TO:<user+two@adb.com>") == "250 OK", "second alias deduped");
    test::check(session.handle_line("RCPT TO:<User+one@adb.com>") == "250 OK", "exact duplicate deduped");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("body") == "", "body");
    test::check(session.handle_line(".").rfind("250 queued as msg_", 0) == 0, "queued");

    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "one message queued");
    test::check(batch[0].recipients.size() == 1, "canonical aliases produce one delivery");
    test::check(batch[0].recipients[0].match.address_canonical == "user@adb.com",
                "deduplicated canonical address");
}

void test_smtp_session_persists_pending_artifacts_before_durable_ack() {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-smtp-durable-ack";
    fs::remove_all(root);
    rapid_inbox::ingestd::BatchWriter writer(root, root / "unused.db", 5000, false);
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    rapid_inbox::ingestd::MailQueue queue(10, 1024 * 1024);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024 * 1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}},
        &writer,
        true,
        "203.0.113.7");

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("Subject: Durable") == "", "body");
    const std::string response = session.handle_line(".");
    test::check(response.rfind("250 queued as msg_", 0) == 0, "durable ACK returned");

    auto batch = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(batch.size() == 1, "durable job queued");
    test::check(batch[0].artifacts_persisted, "job records pending artifacts persisted");
    test::check(batch[0].remote_ip == "203.0.113.7", "peer IP captured on job");
    test::check(fs::is_regular_file(root / batch[0].raw_path), "raw exists before ACK completes");
    test::check(fs::is_regular_file(root / batch[0].manifest_path),
                "pending manifest exists before ACK completes");
    fs::remove_all(root);
}

void test_smtp_session_keeps_durable_ownership_when_queue_closes_after_data() {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-smtp-durable-closed-queue";
    fs::remove_all(root);
    rapid_inbox::ingestd::BatchWriter writer(root, root / "unused.db", 5000, false);
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    rapid_inbox::ingestd::MailQueue queue(10, 1024 * 1024);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024 * 1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}},
        &writer,
        true,
        "203.0.113.8");

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("Subject: Durable close") == "", "body");
    queue.close();

    const std::string response = session.handle_line(".");
    test::check(response.rfind("250 queued as msg_", 0) == 0,
                "persisted manifest keeps ownership when queue closes");
    test::check(queue.total_size() == 0, "failed enqueue releases reservation");

    std::size_t raw_files = 0;
    std::size_t manifest_files = 0;
    for (const auto& entry : fs::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() == ".eml") {
            ++raw_files;
        } else if (entry.path().extension() == ".json") {
            ++manifest_files;
        }
    }
    test::check(raw_files == 1, "durably owned raw remains recoverable");
    test::check(manifest_files == 1, "durably owned manifest remains recoverable");
    fs::remove_all(root);
}

void test_smtp_session_applies_domain_message_size_limit() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    policy.max_message_size_bytes = 5;
    rapid_inbox::ingestd::MailQueue queue(10, 1024);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}});

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>", "data");
    test::check(session.handle_line("1234") == "",
                "domain policy overflow is consumed until terminator");
    test::check(session.handle_line(".") == "552 message too large",
                "domain policy size failure is returned at terminator");
    test::check(queue.size() == 0, "domain oversized message not queued");
}

void test_smtp_session_respects_cross_process_maintenance_lock() {
    namespace fs = std::filesystem;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-smtp-maintenance-lock";
    fs::remove_all(root);
    fs::create_directories(root);
    std::ofstream(root / ".maintenance.lock") << "clear-all";

    rapid_inbox::ingestd::BatchWriter writer(root, root / "unused.db", 5000, false);
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::DomainPolicySnapshot policy;
    policy.root_domain_unicode = "adb.com";
    rapid_inbox::ingestd::MailQueue queue(10, 1024);
    rapid_inbox::ingestd::SmtpSession session(
        matcher,
        queue,
        20,
        1024,
        std::unordered_map<int, rapid_inbox::ingestd::DomainPolicySnapshot>{{1, policy}},
        &writer,
        true);

    test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
    test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
    test::check(session.handle_line("DATA") == "451 storage maintenance in progress",
                "maintenance lock blocks DATA before body acceptance");
    test::check(queue.size() == 0, "maintenance lock leaves queue empty");
    fs::remove_all(root);
}

void test_smtp_session_releases_data_reservation_on_disconnect() {
    rapid_inbox::ingestd::DomainMatcher matcher({{1, "adb.com", true, true, "keep", false}});
    rapid_inbox::ingestd::MailQueue queue(1, 10);
    {
        rapid_inbox::ingestd::SmtpSession session(matcher, queue, 20, 10);
        test::check(session.handle_line("MAIL FROM:<sender@example.com>") == "250 OK", "mail from");
        test::check(session.handle_line("RCPT TO:<code@adb.com>") == "250 OK", "rcpt");
        test::check(session.handle_line("DATA") == "354 End data with <CR><LF>.<CR><LF>",
                    "DATA reserves queue budget");
        test::check(!queue.try_reserve(1), "active DATA consumes message reservation");
    }
    test::check(queue.try_reserve(10), "disconnect releases DATA byte reservation");
    queue.cancel_reservation(10);
}
