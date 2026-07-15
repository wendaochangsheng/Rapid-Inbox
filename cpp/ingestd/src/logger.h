#pragma once

#include <atomic>
#include <concepts>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace rapid_inbox::ingestd {

enum class LogLevel : int {
    Debug = 10,
    Info = 20,
    Warning = 30,
    Error = 40,
    Critical = 50,
};

enum class LogFormat : int {
    Json = 0,
    Text = 1,
};

using LogFieldValue =
    std::variant<std::string, std::int64_t, std::uint64_t, double, bool>;

struct LogField {
    std::string key;
    LogFieldValue value;

    LogField(std::string field_key, std::string field_value)
        : key(std::move(field_key)), value(std::move(field_value)) {}
    LogField(std::string field_key, std::string_view field_value)
        : key(std::move(field_key)), value(std::string(field_value)) {}
    LogField(std::string field_key, const char* field_value)
        : key(std::move(field_key)), value(std::string(field_value == nullptr ? "" : field_value)) {}
    LogField(std::string field_key, bool field_value)
        : key(std::move(field_key)), value(field_value) {}
    LogField(std::string field_key, double field_value)
        : key(std::move(field_key)), value(field_value) {}

    template <std::integral T>
        requires(!std::same_as<std::remove_cv_t<T>, bool>)
    LogField(std::string field_key, T field_value) : key(std::move(field_key)) {
        if constexpr (std::is_signed_v<T>) {
            value = static_cast<std::int64_t>(field_value);
        } else {
            value = static_cast<std::uint64_t>(field_value);
        }
    }
};

LogLevel parse_log_level(std::string_view value);
LogFormat parse_log_format(std::string_view value);
std::string_view log_level_name(LogLevel level) noexcept;

// Public primarily so deterministic rendering can be tested without redirecting
// the process-wide stderr file descriptor. The returned value never contains a
// literal newline; Logger appends exactly one newline while holding its lock.
std::string render_log_line(LogFormat format,
                            LogLevel level,
                            std::string_view event,
                            std::initializer_list<LogField> fields,
                            std::string_view timestamp,
                            std::int64_t pid);

class Logger {
public:
    static Logger& instance() noexcept;

    void configure(LogLevel level, LogFormat format) noexcept;
    bool enabled(LogLevel level) const noexcept;
    void log(LogLevel level,
             std::string_view event,
             std::initializer_list<LogField> fields = {}) noexcept;

private:
    Logger() = default;

    std::atomic<int> minimum_level_{static_cast<int>(LogLevel::Info)};
    std::atomic<int> format_{static_cast<int>(LogFormat::Json)};
    std::mutex output_mutex_;
};

}  // namespace rapid_inbox::ingestd
