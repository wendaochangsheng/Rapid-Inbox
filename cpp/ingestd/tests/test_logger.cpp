#include "../src/logger.h"

#include <stdexcept>
#include <string>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

template <typename Fn>
void expect_invalid_argument(Fn&& fn, const std::string& expected) {
    try {
        fn();
        throw std::runtime_error("expected invalid_argument");
    } catch (const std::invalid_argument& exc) {
        test::check(std::string(exc.what()).find(expected) != std::string::npos,
                    "logger configuration error includes setting name");
    }
}

}  // namespace

void test_logger_parses_configuration_and_levels() {
    using namespace rapid_inbox::ingestd;
    test::check(parse_log_level(" debug ") == LogLevel::Debug, "parse DEBUG log level");
    test::check(parse_log_level("WARNING") == LogLevel::Warning, "parse WARNING log level");
    test::check(parse_log_level("critical") == LogLevel::Critical, "parse CRITICAL log level");
    test::check(parse_log_format("JSON") == LogFormat::Json, "parse JSON log format");
    test::check(parse_log_format(" text ") == LogFormat::Text, "parse text log format");
    expect_invalid_argument([] { (void)parse_log_level("trace"); }, "LOG_LEVEL");
    expect_invalid_argument([] { (void)parse_log_level("warn"); }, "LOG_LEVEL");
    expect_invalid_argument([] { (void)parse_log_format("xml"); }, "LOG_FORMAT");

    Logger::instance().configure(LogLevel::Warning, LogFormat::Json);
    test::check(!Logger::instance().enabled(LogLevel::Info), "INFO filtered at WARNING");
    test::check(Logger::instance().enabled(LogLevel::Error), "ERROR enabled at WARNING");
    Logger::instance().configure(LogLevel::Info, LogFormat::Json);
}

void test_logger_renders_single_line_json_and_redacts_secrets() {
    using namespace rapid_inbox::ingestd;
    const std::string line = render_log_line(LogFormat::Json,
                                             LogLevel::Warning,
                                             "smtp.rejected",
                                             {
                                                 {"reason", "queue\nfull"},
                                                 {"active_connections", 7},
                                                 {"durable", true},
                                                 {"authorization", "Bearer do-not-log"},
                                                 {"maintenance_token", "do-not-log"},
                                                 {"api_key", "do-not-log"},
                                                 {"signing_key", "do-not-log"},
                                                 {"event", "cannot-override"},
                                             },
                                             "2026-07-15T01:02:03.004Z",
                                             1234);
    test::check(line.find("\"ts\":\"2026-07-15T01:02:03.004Z\"") !=
                    std::string::npos,
                "JSON log has UTC timestamp");
    test::check(line.find("\"level\":\"WARNING\"") != std::string::npos,
                "JSON log has level");
    test::check(line.find("\"event\":\"smtp.rejected\"") != std::string::npos,
                "JSON log has stable event");
    test::check(line.find("\"active_connections\":7") != std::string::npos,
                "JSON log preserves numeric field type");
    test::check(line.find("\"durable\":true") != std::string::npos,
                "JSON log preserves boolean field type");
    test::check(line.find("queue\\nfull") != std::string::npos,
                "JSON log escapes embedded newline");
    test::check(line.find("do-not-log") == std::string::npos,
                "JSON log redacts credential-like fields");
    test::check(line.find("[REDACTED]") != std::string::npos,
                "JSON log identifies redacted fields");
    test::check(line.find('\n') == std::string::npos, "rendered JSON remains one physical line");
    test::check(line.find("cannot-override") == std::string::npos,
                "reserved base fields cannot be overridden");
}

void test_logger_preserves_valid_utf8_when_bounding_fields() {
    using namespace rapid_inbox::ingestd;
    std::string crossing_boundary(4095, 'a');
    crossing_boundary.append("\xe4\xb8\xad");
    std::string invalid_utf8 = "before";
    invalid_utf8.push_back(static_cast<char>(0xff));
    invalid_utf8.append("after");
    const std::string line = render_log_line(LogFormat::Json,
                                             LogLevel::Info,
                                             "logger.utf8",
                                             {
                                                 {"bounded", crossing_boundary},
                                                 {"invalid", invalid_utf8},
                                             },
                                             "2026-07-15T01:02:03.004Z",
                                             1);
    test::check(line.find("...[truncated]") != std::string::npos,
                "bounded UTF-8 string identifies truncation");
    test::check(line.find("\xe4") == std::string::npos,
                "UTF-8 truncation does not keep a partial multibyte sequence");
    test::check(line.find("before\xef\xbf\xbd" "after") != std::string::npos,
                "invalid UTF-8 byte is replaced with U+FFFD");
    test::check(line.find('\n') == std::string::npos,
                "bounded UTF-8 JSON remains one physical line");
}

void test_logger_renders_single_line_text() {
    using namespace rapid_inbox::ingestd;
    const std::string line = render_log_line(LogFormat::Text,
                                             LogLevel::Info,
                                             "process.started",
                                             {
                                                 {"host", "127.0.0.1"},
                                                 {"port", 2525},
                                                 {"unsafe\nkey", "safe\nvalue"},
                                             },
                                             "2026-07-15T01:02:03.004Z",
                                             99);
    test::check(line.rfind("2026-07-15T01:02:03.004Z level=INFO ", 0) == 0,
                "text log starts with timestamp and level");
    test::check(line.find("event=\"process.started\"") != std::string::npos,
                "text log has event");
    test::check(line.find("host=\"127.0.0.1\"") != std::string::npos,
                "text log quotes strings");
    test::check(line.find("port=2525") != std::string::npos,
                "text log preserves numeric fields");
    test::check(line.find("unsafe\\nkey=\"safe\\nvalue\"") != std::string::npos,
                "text log escapes control characters in keys and values");
    test::check(line.find('\n') == std::string::npos, "rendered text remains one physical line");
}
