#include "logger.h"

#include "json_util.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>

namespace rapid_inbox::ingestd {
namespace {

constexpr std::size_t kMaximumStringFieldBytes = 4096;

std::string trim_and_upper(std::string_view value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string_view::npos) {
        return "";
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    std::string normalized(value.substr(first, last - first + 1));
    std::transform(normalized.begin(), normalized.end(), normalized.begin(), [](unsigned char ch) {
        if (ch >= 'a' && ch <= 'z') {
            return static_cast<char>(ch - ('a' - 'A'));
        }
        return static_cast<char>(ch);
    });
    return normalized;
}

std::string utc_timestamp_with_milliseconds() {
    const auto now = std::chrono::system_clock::now();
    const auto seconds = std::chrono::time_point_cast<std::chrono::seconds>(now);
    const auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(now - seconds).count();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &now_time);
#else
    gmtime_r(&now_time, &utc_tm);
#endif
    std::ostringstream output;
    output << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%S") << '.' << std::setw(3)
           << std::setfill('0') << millis << 'Z';
    return output.str();
}

bool is_reserved_key(std::string_view key) {
    return key.empty() || key == "ts" || key == "timestamp" || key == "level" ||
           key == "event" || key == "service" || key == "pid";
}

bool is_sensitive_key(std::string_view key) {
    std::string normalized = trim_and_upper(key);
    return normalized.find("AUTHORIZATION") != std::string::npos ||
           normalized.find("PASSWORD") != std::string::npos ||
           normalized.find("CREDENTIAL") != std::string::npos ||
           normalized.find("SECRET") != std::string::npos ||
           normalized == "TOKEN" || normalized.ends_with("_TOKEN") ||
           normalized == "KEY" || normalized == "API_KEY" || normalized.ends_with("_KEY") ||
           normalized.find("COOKIE") != std::string::npos;
}

std::size_t valid_utf8_sequence_length(std::string_view value, std::size_t offset) {
    const auto byte = [&](std::size_t index) {
        return static_cast<unsigned char>(value[offset + index]);
    };
    const unsigned char first = byte(0);
    if (first <= 0x7f) {
        return 1;
    }
    if (first >= 0xc2 && first <= 0xdf) {
        return offset + 2 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0xbf ? 2 : 0;
    }
    if (first == 0xe0) {
        return offset + 3 <= value.size() && byte(1) >= 0xa0 && byte(1) <= 0xbf &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf
                   ? 3
                   : 0;
    }
    if (first >= 0xe1 && first <= 0xec) {
        return offset + 3 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0xbf &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf
                   ? 3
                   : 0;
    }
    if (first == 0xed) {
        return offset + 3 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0x9f &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf
                   ? 3
                   : 0;
    }
    if (first >= 0xee && first <= 0xef) {
        return offset + 3 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0xbf &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf
                   ? 3
                   : 0;
    }
    if (first == 0xf0) {
        return offset + 4 <= value.size() && byte(1) >= 0x90 && byte(1) <= 0xbf &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf && byte(3) >= 0x80 &&
                       byte(3) <= 0xbf
                   ? 4
                   : 0;
    }
    if (first >= 0xf1 && first <= 0xf3) {
        return offset + 4 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0xbf &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf && byte(3) >= 0x80 &&
                       byte(3) <= 0xbf
                   ? 4
                   : 0;
    }
    if (first == 0xf4) {
        return offset + 4 <= value.size() && byte(1) >= 0x80 && byte(1) <= 0x8f &&
                       byte(2) >= 0x80 && byte(2) <= 0xbf && byte(3) >= 0x80 &&
                       byte(3) <= 0xbf
                   ? 4
                   : 0;
    }
    return 0;
}

std::string bounded_valid_utf8(std::string_view value) {
    constexpr std::string_view replacement = "\xef\xbf\xbd";
    std::string bounded;
    bounded.reserve(std::min(value.size(), kMaximumStringFieldBytes) + 14);
    std::size_t offset = 0;
    while (offset < value.size()) {
        const std::size_t sequence_length = valid_utf8_sequence_length(value, offset);
        if (sequence_length == 0) {
            if (bounded.size() + replacement.size() > kMaximumStringFieldBytes) {
                break;
            }
            bounded.append(replacement);
            ++offset;
            continue;
        }
        if (bounded.size() + sequence_length > kMaximumStringFieldBytes) {
            break;
        }
        bounded.append(value.substr(offset, sequence_length));
        offset += sequence_length;
    }
    if (offset < value.size()) {
        bounded.append("...[truncated]");
    }
    return bounded;
}

void append_json_value(std::ostringstream& output, const LogField& field) {
    if (is_sensitive_key(field.key)) {
        output << "\"[REDACTED]\"";
        return;
    }
    std::visit(
        [&output](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>) {
                output << '"' << json_escape(bounded_valid_utf8(value)) << '"';
            } else if constexpr (std::is_same_v<T, bool>) {
                output << (value ? "true" : "false");
            } else if constexpr (std::is_same_v<T, double>) {
                if (std::isfinite(value)) {
                    output << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
                } else {
                    output << "null";
                }
            } else {
                output << value;
            }
        },
        field.value);
}

void append_text_value(std::ostringstream& output, const LogField& field) {
    if (is_sensitive_key(field.key)) {
        output << "\"[REDACTED]\"";
        return;
    }
    std::visit(
        [&output](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::string>) {
                output << '"' << json_escape(bounded_valid_utf8(value)) << '"';
            } else if constexpr (std::is_same_v<T, bool>) {
                output << (value ? "true" : "false");
            } else if constexpr (std::is_same_v<T, double>) {
                if (std::isfinite(value)) {
                    output << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
                } else {
                    output << "null";
                }
            } else {
                output << value;
            }
        },
        field.value);
}

void write_stderr_line(const std::string& line) noexcept {
    std::size_t offset = 0;
    while (offset < line.size()) {
        const ssize_t written = ::write(STDERR_FILENO, line.data() + offset, line.size() - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return;
        }
        offset += static_cast<std::size_t>(written);
    }
}

}  // namespace

LogLevel parse_log_level(std::string_view value) {
    const std::string normalized = trim_and_upper(value);
    if (normalized == "DEBUG") {
        return LogLevel::Debug;
    }
    if (normalized == "INFO") {
        return LogLevel::Info;
    }
    if (normalized == "WARNING") {
        return LogLevel::Warning;
    }
    if (normalized == "ERROR") {
        return LogLevel::Error;
    }
    if (normalized == "CRITICAL") {
        return LogLevel::Critical;
    }
    throw std::invalid_argument("invalid LOG_LEVEL");
}

LogFormat parse_log_format(std::string_view value) {
    const std::string normalized = trim_and_upper(value);
    if (normalized == "JSON") {
        return LogFormat::Json;
    }
    if (normalized == "TEXT") {
        return LogFormat::Text;
    }
    throw std::invalid_argument("invalid LOG_FORMAT");
}

std::string_view log_level_name(LogLevel level) noexcept {
    switch (level) {
        case LogLevel::Debug:
            return "DEBUG";
        case LogLevel::Info:
            return "INFO";
        case LogLevel::Warning:
            return "WARNING";
        case LogLevel::Error:
            return "ERROR";
        case LogLevel::Critical:
            return "CRITICAL";
    }
    return "ERROR";
}

std::string render_log_line(LogFormat format,
                            LogLevel level,
                            std::string_view event,
                            std::initializer_list<LogField> fields,
                            std::string_view timestamp,
                            std::int64_t pid) {
    std::ostringstream output;
    if (format == LogFormat::Json) {
        output << "{\"ts\":\"" << json_escape(bounded_valid_utf8(timestamp)) << "\"";
        output << ",\"level\":\"" << log_level_name(level) << "\"";
        output << ",\"event\":\"" << json_escape(bounded_valid_utf8(event)) << "\"";
        output << ",\"service\":\"rapid-inbox-ingestd\"";
        output << ",\"pid\":" << pid;
        for (const LogField& field : fields) {
            if (is_reserved_key(field.key)) {
                continue;
            }
            output << ",\"" << json_escape(bounded_valid_utf8(field.key)) << "\":";
            append_json_value(output, field);
        }
        output << '}';
        return output.str();
    }

    output << json_escape(bounded_valid_utf8(timestamp)) << " level=" << log_level_name(level)
           << " service=rapid-inbox-ingestd event=\""
           << json_escape(bounded_valid_utf8(event)) << "\" pid=" << pid;
    for (const LogField& field : fields) {
        if (is_reserved_key(field.key)) {
            continue;
        }
        output << ' ' << json_escape(bounded_valid_utf8(field.key)) << '=';
        append_text_value(output, field);
    }
    return output.str();
}

Logger& Logger::instance() noexcept {
    static Logger logger;
    return logger;
}

void Logger::configure(LogLevel level, LogFormat format) noexcept {
    minimum_level_.store(static_cast<int>(level), std::memory_order_release);
    format_.store(static_cast<int>(format), std::memory_order_release);
}

bool Logger::enabled(LogLevel level) const noexcept {
    return static_cast<int>(level) >= minimum_level_.load(std::memory_order_acquire);
}

void Logger::log(LogLevel level,
                 std::string_view event,
                 std::initializer_list<LogField> fields) noexcept {
    if (!enabled(level)) {
        return;
    }
    try {
        const auto format = static_cast<LogFormat>(format_.load(std::memory_order_acquire));
        std::string line = render_log_line(format,
                                           level,
                                           event,
                                           fields,
                                           utc_timestamp_with_milliseconds(),
                                           static_cast<std::int64_t>(::getpid()));
        line.push_back('\n');
        const std::lock_guard lock(output_mutex_);
        write_stderr_line(line);
    } catch (...) {
        // Logging must never change SMTP acknowledgement or shutdown behavior.
    }
}

}  // namespace rapid_inbox::ingestd
