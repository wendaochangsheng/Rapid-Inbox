#include "storage_path.h"

#include "time_utils.h"

namespace rapid_inbox::ingestd {
namespace {

constexpr std::size_t kMaxSafeFilenameBytes = 160;

bool is_safe_filename_char(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-';
}

std::string dated_path(const std::string& root,
                       const std::string& message_id,
                       const std::string& received_at,
                       const std::string& extension) {
    const DateParts parts = path_date_parts(received_at);
    return root + "/" + parts.year + "/" + parts.month + "/" + parts.day + "/" + message_id +
           extension;
}

}

std::string safe_filename(const std::string& filename) {
    std::string safe;
    safe.reserve(filename.size());
    bool previous_char_was_invalid = false;
    for (char ch : filename) {
        if (is_safe_filename_char(ch)) {
            safe.push_back(ch);
            previous_char_was_invalid = false;
        } else if (!previous_char_was_invalid) {
            safe.push_back('_');
            previous_char_was_invalid = true;
        }
    }

    const auto first = safe.find_first_not_of("._");
    if (first == std::string::npos) {
        return "attachment.bin";
    }
    const auto last = safe.find_last_not_of("._");
    safe = safe.substr(first, last - first + 1);

    if (safe.empty()) {
        return "attachment.bin";
    }
    if (safe.size() <= kMaxSafeFilenameBytes) {
        return safe;
    }

    const auto dot = safe.rfind('.');
    if (dot != std::string::npos && dot > 0 && safe.size() - dot <= 16) {
        const std::string extension = safe.substr(dot);
        safe.resize(kMaxSafeFilenameBytes - extension.size());
        safe += extension;
    } else {
        safe.resize(kMaxSafeFilenameBytes);
    }
    while (!safe.empty() && (safe.back() == '.' || safe.back() == '_')) {
        safe.pop_back();
    }
    return safe.empty() ? "attachment.bin" : safe;
}

std::string raw_message_path(const std::string& message_id, const std::string& received_at) {
    return dated_path("raw", message_id, received_at, ".eml");
}

std::string manifest_path(const std::string& message_id, const std::string& received_at) {
    return dated_path("manifests", message_id, received_at, ".json");
}

std::string text_body_path(const std::string& message_id, const std::string& received_at) {
    return dated_path("text", message_id, received_at, ".txt");
}

std::string html_body_path(const std::string& message_id, const std::string& received_at) {
    return dated_path("html", message_id, received_at, ".html");
}

std::string attachment_path(const std::string& message_id,
                            const std::string& attachment_id,
                            const std::string& safe_name) {
    return "attachments/" + message_id + "/" + attachment_id + "-" + safe_name;
}

}
