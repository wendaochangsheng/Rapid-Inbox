#include "../src/id.h"
#include "../src/json_util.h"
#include "../src/sha256.h"
#include "../src/storage_path.h"
#include "../src/time_utils.h"

#include <regex>
#include <stdexcept>
#include <string>

namespace test {
inline void check(bool condition, const std::string& message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename Exception, typename Fn>
void expect_throw(Fn&& fn, const std::string& message) {
    try {
        fn();
    } catch (const Exception&) {
        return;
    } catch (const std::exception& ex) {
        throw std::runtime_error(message + ": unexpected exception: " + ex.what());
    }
    throw std::runtime_error(message);
}
}

void test_time_and_path_parts() {
    const std::regex utc_timestamp_pattern(R"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)");
    test::check(std::regex_match(rapid_inbox::ingestd::utc_now(), utc_timestamp_pattern),
                "utc_now format");

    const auto parts = rapid_inbox::ingestd::path_date_parts("2026-05-12T03:04:05Z");
    test::check(parts.year == "2026", "date parts year");
    test::check(parts.month == "05", "date parts month");
    test::check(parts.day == "12", "date parts day");

    const auto date_only_parts = rapid_inbox::ingestd::path_date_parts("2026-05-12");
    test::check(date_only_parts.year == "2026", "date-only parts year");
    test::check(date_only_parts.month == "05", "date-only parts month");
    test::check(date_only_parts.day == "12", "date-only parts day");

    const auto leap_day_parts = rapid_inbox::ingestd::path_date_parts("2024-02-29T00:00:00Z");
    test::check(leap_day_parts.year == "2024", "leap day year");
    test::check(leap_day_parts.month == "02", "leap day month");
    test::check(leap_day_parts.day == "29", "leap day day");
    test::check(rapid_inbox::ingestd::utc_add_days("2024-02-28T23:59:59Z", 2) ==
                    "2024-03-01T23:59:59Z",
                "UTC retention expiry crosses leap day");

    const std::string accepted_timestamps[] = {
        "2026-05-12T03",
        "2026-05-12T03:04",
        "2026-05-12T03:04:05+0000",
        "2026-05-12T03:04:05+00",
        "2026-05-12T03:04:05+00:00:30",
        "2026-05-12T03:04:05.123456Z",
        "2026-05-12 03:04:05",
    };
    for (const auto& timestamp : accepted_timestamps) {
        const auto accepted_parts = rapid_inbox::ingestd::path_date_parts(timestamp);
        test::check(accepted_parts.year == "2026", "accepted timestamp year: " + timestamp);
        test::check(accepted_parts.month == "05", "accepted timestamp month: " + timestamp);
        test::check(accepted_parts.day == "12", "accepted timestamp day: " + timestamp);
    }

    test::expect_throw<std::invalid_argument>(
        [] { rapid_inbox::ingestd::path_date_parts("2026-05-1"); }, "short date rejected");
    test::expect_throw<std::invalid_argument>(
        [] { rapid_inbox::ingestd::path_date_parts("2026-99-99T03:04:05Z"); },
        "invalid date rejected");
    test::expect_throw<std::invalid_argument>(
        [] { rapid_inbox::ingestd::path_date_parts("0000-01-01"); }, "year zero rejected");
    test::expect_throw<std::invalid_argument>(
        [] { rapid_inbox::ingestd::path_date_parts("2026-05-12garbage"); },
        "malformed suffix rejected");
    test::expect_throw<std::invalid_argument>(
        [] { rapid_inbox::ingestd::path_date_parts("2026-05-12T99:99:99Z"); },
        "invalid time rejected");
}

void test_ids_have_expected_prefixes() {
    const std::string message_id = rapid_inbox::ingestd::make_prefixed_id("msg_");
    const std::string delivery_id = rapid_inbox::ingestd::make_prefixed_id("dlv_");
    const std::string second_message_id = rapid_inbox::ingestd::make_prefixed_id("msg_");

    const std::regex message_id_pattern(R"(msg_[0-9a-f]{32})");
    const std::regex delivery_id_pattern(R"(dlv_[0-9a-f]{32})");

    test::check(std::regex_match(message_id, message_id_pattern), "message id shape");
    test::check(std::regex_match(delivery_id, delivery_id_pattern), "delivery id shape");
    test::check(message_id != second_message_id, "message ids vary");
}

void test_sha256_known_digest() {
    test::check(rapid_inbox::ingestd::sha256_hex("") ==
                    "e3b0c44298fc1c149afbf4c8996fb924"
                    "27ae41e4649b934ca495991b7852b855",
                "sha256 empty digest");
    test::check(rapid_inbox::ingestd::sha256_hex("abc") ==
                    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                "sha256 abc digest");
}

void test_storage_paths_match_python_layout() {
    const std::string received_at = "2026-05-12T03:04:05Z";

    test::check(rapid_inbox::ingestd::raw_message_path("msg_abc", received_at) ==
                    "raw/2026/05/12/msg_abc.eml",
                "raw message path");
    test::check(rapid_inbox::ingestd::manifest_path("msg_abc", received_at) ==
                    "manifests/2026/05/12/msg_abc.json",
                "manifest path");
    test::check(rapid_inbox::ingestd::text_body_path("msg_abc", received_at) ==
                    "text/2026/05/12/msg_abc.txt",
                "text body path");
    test::check(rapid_inbox::ingestd::html_body_path("msg_abc", received_at) ==
                    "html/2026/05/12/msg_abc.html",
                "html body path");
    test::check(rapid_inbox::ingestd::attachment_path("msg_abc", "att_1", "report.txt") ==
                    "attachments/msg_abc/att_1-report.txt",
                "attachment path");
    test::check(rapid_inbox::ingestd::safe_filename("a/b c?.txt") == "a_b_c_.txt",
                "safe filename");
    test::check(rapid_inbox::ingestd::safe_filename("a//b") == "a_b", "collapsed safe filename");
    test::check(rapid_inbox::ingestd::safe_filename("") == "attachment.bin", "empty filename");
    test::check(rapid_inbox::ingestd::safe_filename("...___") == "attachment.bin",
                "punctuation-only filename");
    const std::string long_filename(300, 'a');
    test::check(rapid_inbox::ingestd::safe_filename(long_filename + ".txt").size() <= 160,
                "safe filename is bounded for filesystem NAME_MAX");
    test::check(rapid_inbox::ingestd::safe_filename(long_filename + ".txt").ends_with(".txt"),
                "safe filename preserves short extension when truncated");
}

void test_json_escape() {
    test::check(rapid_inbox::ingestd::json_escape("a\"b\\c\n") == "a\\\"b\\\\c\\n",
                "json escape quote backslash newline");
    test::check(rapid_inbox::ingestd::json_escape(std::string("\x01\0", 2)) ==
                    "\\u0001\\u0000",
                "json escape control bytes");
}
