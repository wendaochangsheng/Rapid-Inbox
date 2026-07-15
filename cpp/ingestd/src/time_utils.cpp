#include "time_utils.h"

#include <charconv>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace rapid_inbox::ingestd {
namespace {

bool is_digit(char ch) {
    return ch >= '0' && ch <= '9';
}

bool has_iso_date_prefix(const std::string& timestamp) {
    return timestamp.size() >= 10 && is_digit(timestamp[0]) && is_digit(timestamp[1]) &&
           is_digit(timestamp[2]) && is_digit(timestamp[3]) && timestamp[4] == '-' &&
           is_digit(timestamp[5]) && is_digit(timestamp[6]) && timestamp[7] == '-' &&
           is_digit(timestamp[8]) && is_digit(timestamp[9]);
}

bool parse_fixed_int(const std::string& value, std::size_t offset, std::size_t length, int& out) {
    const char* first = value.data() + offset;
    const char* last = first + length;
    const auto result = std::from_chars(first, last, out);
    return result.ec == std::errc{} && result.ptr == last;
}

bool parse_two_digit_range(const std::string& value,
                           std::size_t offset,
                           int min_value,
                           int max_value,
                           int& out) {
    return offset + 2 <= value.size() && parse_fixed_int(value, offset, 2, out) &&
           out >= min_value && out <= max_value;
}

bool parse_fraction(const std::string& value, std::size_t& pos) {
    if (pos >= value.size() || value[pos] != '.') {
        return true;
    }
    ++pos;
    const std::size_t fraction_start = pos;
    while (pos < value.size() && is_digit(value[pos])) {
        ++pos;
    }
    return pos > fraction_start;
}

bool parse_time_of_day(const std::string& timestamp, std::size_t& pos) {
    int hour = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 23, hour)) {
        return false;
    }
    pos += 2;

    if (pos == timestamp.size() || timestamp[pos] == 'Z' || timestamp[pos] == '+' ||
        timestamp[pos] == '-') {
        return true;
    }
    if (timestamp[pos] != ':') {
        return false;
    }
    ++pos;

    int minute = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 59, minute)) {
        return false;
    }
    pos += 2;

    if (pos == timestamp.size() || timestamp[pos] == 'Z' || timestamp[pos] == '+' ||
        timestamp[pos] == '-') {
        return true;
    }
    if (timestamp[pos] != ':') {
        return false;
    }
    ++pos;

    int second = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 59, second)) {
        return false;
    }
    pos += 2;

    return parse_fraction(timestamp, pos);
}

bool parse_timezone_suffix(const std::string& timestamp, std::size_t& pos) {
    if (pos == timestamp.size()) {
        return true;
    }
    if (timestamp[pos] == 'Z') {
        ++pos;
        return pos == timestamp.size();
    }
    if (timestamp[pos] != '+' && timestamp[pos] != '-') {
        return false;
    }

    ++pos;
    int offset_hour = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 23, offset_hour)) {
        return false;
    }
    pos += 2;

    if (pos == timestamp.size()) {
        return true;
    }
    if (timestamp[pos] != ':') {
        int offset_minute = 0;
        if (!parse_two_digit_range(timestamp, pos, 0, 59, offset_minute)) {
            return false;
        }
        pos += 2;
        return pos == timestamp.size();
    }

    ++pos;
    int offset_minute = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 59, offset_minute)) {
        return false;
    }
    pos += 2;

    if (pos == timestamp.size()) {
        return true;
    }
    if (timestamp[pos] != ':') {
        return false;
    }

    ++pos;
    int offset_second = 0;
    if (!parse_two_digit_range(timestamp, pos, 0, 59, offset_second)) {
        return false;
    }
    pos += 2;

    return parse_fraction(timestamp, pos) && pos == timestamp.size();
}

bool is_valid_timestamp_suffix(const std::string& timestamp) {
    if (timestamp.size() == 10) {
        return true;
    }
    if (timestamp[10] != 'T' && timestamp[10] != ' ') {
        return false;
    }

    std::size_t pos = 11;
    if (!parse_time_of_day(timestamp, pos)) {
        return false;
    }
    if (pos == timestamp.size()) {
        return true;
    }
    return parse_timezone_suffix(timestamp, pos) && pos == timestamp.size();
}

}

std::string utc_now() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);

    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &now_time);
#else
    gmtime_r(&now_time, &utc_tm);
#endif

    std::ostringstream output;
    output << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return output.str();
}

std::string utc_add_days(const std::string& timestamp, int days_to_add) {
    if (days_to_add < 0 || timestamp.size() != 20 || timestamp[4] != '-' ||
        timestamp[7] != '-' || timestamp[10] != 'T' || timestamp[13] != ':' ||
        timestamp[16] != ':' || timestamp[19] != 'Z') {
        throw std::invalid_argument("malformed UTC timestamp: " + timestamp);
    }

    int year_value = 0;
    int month_value = 0;
    int day_value = 0;
    int hour_value = 0;
    int minute_value = 0;
    int second_value = 0;
    if (!parse_fixed_int(timestamp, 0, 4, year_value) ||
        !parse_fixed_int(timestamp, 5, 2, month_value) ||
        !parse_fixed_int(timestamp, 8, 2, day_value) ||
        !parse_fixed_int(timestamp, 11, 2, hour_value) ||
        !parse_fixed_int(timestamp, 14, 2, minute_value) ||
        !parse_fixed_int(timestamp, 17, 2, second_value)) {
        throw std::invalid_argument("malformed UTC timestamp: " + timestamp);
    }

    const std::chrono::year_month_day date{
        std::chrono::year{year_value},
        std::chrono::month{static_cast<unsigned>(month_value)},
        std::chrono::day{static_cast<unsigned>(day_value)},
    };
    if (!date.ok() || hour_value < 0 || hour_value > 23 || minute_value < 0 ||
        minute_value > 59 || second_value < 0 || second_value > 59) {
        throw std::invalid_argument("malformed UTC timestamp: " + timestamp);
    }

    const auto instant = std::chrono::sys_days{date} + std::chrono::days{days_to_add} +
                         std::chrono::hours{hour_value} + std::chrono::minutes{minute_value} +
                         std::chrono::seconds{second_value};
    const std::time_t result_time =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::time_point{instant});
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &result_time);
#else
    gmtime_r(&result_time, &utc_tm);
#endif
    std::ostringstream output;
    output << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return output.str();
}

DateParts path_date_parts(const std::string& timestamp) {
    if (!has_iso_date_prefix(timestamp) || !is_valid_timestamp_suffix(timestamp)) {
        throw std::invalid_argument("malformed timestamp: " + timestamp);
    }
    int year = 0;
    int month = 0;
    int day = 0;
    if (!parse_fixed_int(timestamp, 0, 4, year) || !parse_fixed_int(timestamp, 5, 2, month) ||
        !parse_fixed_int(timestamp, 8, 2, day)) {
        throw std::invalid_argument("malformed timestamp: " + timestamp);
    }
    if (year < 1 || year > 9999) {
        throw std::invalid_argument("malformed timestamp: " + timestamp);
    }
    const std::chrono::year_month_day date{
        std::chrono::year{year},
        std::chrono::month{static_cast<unsigned>(month)},
        std::chrono::day{static_cast<unsigned>(day)},
    };
    if (!date.ok()) {
        throw std::invalid_argument("malformed timestamp: " + timestamp);
    }
    return DateParts{
        timestamp.substr(0, 4),
        timestamp.substr(5, 2),
        timestamp.substr(8, 2),
    };
}

}
