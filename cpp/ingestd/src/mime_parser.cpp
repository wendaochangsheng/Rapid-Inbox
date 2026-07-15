#include "mime_parser.h"

#include "id.h"
#include "json_util.h"
#include "sha256.h"
#include "storage_path.h"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdint>
#include <iconv.h>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <unicode/utf8.h>

namespace rapid_inbox::ingestd {
namespace {

struct HeaderBlock {
    std::vector<std::pair<std::string, std::string>> ordered;
};

struct ParameterizedHeader {
    std::string value;
    std::vector<std::pair<std::string, std::string>> params;
};

struct ParseContext {
    ParsedMail mail;
    int next_part_index = 0;
};

std::string decode_charset(const std::string& bytes, const std::string& charset);

bool ascii_space(unsigned char ch) {
    return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '\f' || ch == '\v';
}

std::string trim(std::string_view value) {
    std::size_t first = 0;
    while (first < value.size() && ascii_space(static_cast<unsigned char>(value[first]))) {
        ++first;
    }

    std::size_t last = value.size();
    while (last > first && ascii_space(static_cast<unsigned char>(value[last - 1]))) {
        --last;
    }

    return std::string(value.substr(first, last - first));
}

std::string trim_trailing_lwsp(std::string_view value) {
    std::size_t last = value.size();
    while (last > 0 && (value[last - 1] == ' ' || value[last - 1] == '\t')) {
        --last;
    }
    return std::string(value.substr(0, last));
}

std::string lower_ascii(std::string_view value) {
    std::string lowered;
    lowered.reserve(value.size());
    for (unsigned char ch : value) {
        lowered.push_back(static_cast<char>(std::tolower(ch)));
    }
    return lowered;
}

bool starts_with_ignore_case(std::string_view value, std::string_view prefix) {
    if (value.size() < prefix.size()) {
        return false;
    }
    for (std::size_t index = 0; index < prefix.size(); ++index) {
        const auto left = static_cast<unsigned char>(value[index]);
        const auto right = static_cast<unsigned char>(prefix[index]);
        if (std::tolower(left) != std::tolower(right)) {
            return false;
        }
    }
    return true;
}

bool is_safe_synthesized_char(char ch) {
    return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
           (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-';
}

std::string normalize_line_endings(std::string_view value) {
    std::string normalized;
    normalized.reserve(value.size());
    for (std::size_t index = 0; index < value.size(); ++index) {
        const char ch = value[index];
        if (ch == '\r') {
            if (index + 1 < value.size() && value[index + 1] == '\n') {
                ++index;
            }
            normalized.push_back('\n');
        } else {
            normalized.push_back(ch);
        }
    }
    return normalized;
}

std::pair<std::string, std::string> split_header_body(const std::string& message) {
    std::size_t line_start = 0;
    while (line_start <= message.size()) {
        const std::size_t line_end = message.find('\n', line_start);
        const bool has_newline = line_end != std::string::npos;
        const std::size_t effective_end = has_newline ? line_end : message.size();
        const std::string line = message.substr(line_start, effective_end - line_start);
        if (trim(line).empty()) {
            const std::size_t body_start = has_newline ? line_end + 1 : message.size();
            return {message.substr(0, line_start), message.substr(body_start)};
        }
        if (!has_newline) {
            break;
        }
        line_start = line_end + 1;
    }
    return {message, ""};
}

HeaderBlock parse_headers(const std::string& header_block) {
    HeaderBlock headers;
    std::optional<std::pair<std::string, std::string>> current;

    std::size_t line_start = 0;
    while (line_start <= header_block.size()) {
        const std::size_t line_end = header_block.find('\n', line_start);
        const bool has_newline = line_end != std::string::npos;
        const std::size_t effective_end = has_newline ? line_end : header_block.size();
        const std::string line = header_block.substr(line_start, effective_end - line_start);

        if (line.empty()) {
            break;
        }

        if ((line[0] == ' ' || line[0] == '\t') && current.has_value()) {
            const std::string unfolded = trim(line);
            if (!unfolded.empty()) {
                current->second += " " + unfolded;
            }
        } else {
            if (current.has_value()) {
                headers.ordered.push_back(*current);
            }
            const std::size_t colon = line.find(':');
            if (colon == std::string::npos) {
                current.reset();
            } else {
                current = {trim(std::string_view(line).substr(0, colon)),
                           trim(std::string_view(line).substr(colon + 1))};
            }
        }

        if (!has_newline) {
            break;
        }
        line_start = line_end + 1;
    }

    if (current.has_value()) {
        headers.ordered.push_back(*current);
    }

    return headers;
}

std::optional<std::string> header_value(const HeaderBlock& headers, std::string_view name) {
    const std::string wanted = lower_ascii(name);
    for (const auto& [header_name, value] : headers.ordered) {
        if (lower_ascii(header_name) == wanted) {
            return value;
        }
    }
    return std::nullopt;
}

std::optional<std::string> decoded_header_value(const HeaderBlock& headers, std::string_view name) {
    const auto value = header_value(headers, name);
    if (!value.has_value()) {
        return std::nullopt;
    }
    return decode_rfc2047_words(*value);
}

std::string headers_to_json(const HeaderBlock& headers) {
    std::string output = "[";
    bool first = true;
    for (const auto& [name, value] : headers.ordered) {
        if (!first) {
            output.push_back(',');
        }
        first = false;
        output += "[\"";
        output += json_escape(name);
        output += "\",\"";
        output += json_escape(decode_rfc2047_words(value));
        output += "\"]";
    }
    output.push_back(']');
    return output;
}

std::vector<std::string> split_parameter_segments(std::string_view value) {
    std::vector<std::string> segments;
    bool quoted = false;
    bool escaped = false;
    std::size_t segment_start = 0;

    for (std::size_t index = 0; index < value.size(); ++index) {
        const char ch = value[index];
        if (escaped) {
            escaped = false;
            continue;
        }
        if (quoted && ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            quoted = !quoted;
            continue;
        }
        if (!quoted && ch == ';') {
            segments.push_back(trim(value.substr(segment_start, index - segment_start)));
            segment_start = index + 1;
        }
    }

    segments.push_back(trim(value.substr(segment_start)));
    return segments;
}

std::string unquote_parameter(std::string_view value) {
    const std::string trimmed = trim(value);
    if (trimmed.size() < 2 || trimmed.front() != '"' || trimmed.back() != '"') {
        return trimmed;
    }

    std::string unquoted;
    unquoted.reserve(trimmed.size() - 2);
    bool escaped = false;
    for (std::size_t index = 1; index + 1 < trimmed.size(); ++index) {
        const char ch = trimmed[index];
        if (escaped) {
            unquoted.push_back(ch);
            escaped = false;
        } else if (ch == '\\') {
            escaped = true;
        } else {
            unquoted.push_back(ch);
        }
    }
    if (escaped) {
        unquoted.push_back('\\');
    }
    return unquoted;
}

ParameterizedHeader parse_parameterized_header(std::string_view raw_value) {
    const std::vector<std::string> segments = split_parameter_segments(raw_value);
    ParameterizedHeader parsed;
    if (segments.empty()) {
        return parsed;
    }

    parsed.value = lower_ascii(trim(segments[0]));
    for (std::size_t index = 1; index < segments.size(); ++index) {
        const std::string& segment = segments[index];
        const std::size_t equals = segment.find('=');
        if (equals == std::string::npos) {
            continue;
        }
        const std::string key = lower_ascii(trim(std::string_view(segment).substr(0, equals)));
        if (key.empty()) {
            continue;
        }
        const std::string value =
            decode_rfc2047_words(unquote_parameter(std::string_view(segment).substr(equals + 1)));
        parsed.params.push_back({key, value});
    }

    return parsed;
}

std::optional<std::string> parameter_value(const ParameterizedHeader& header,
                                           std::string_view name) {
    const std::string wanted = lower_ascii(name);
    for (const auto& [key, value] : header.params) {
        if (key == wanted) {
            return value;
        }
    }
    return std::nullopt;
}

ParameterizedHeader content_type_for(const HeaderBlock& headers) {
    const std::string raw_value = header_value(headers, "Content-Type").value_or("text/plain");
    ParameterizedHeader parsed = parse_parameterized_header(raw_value);
    if (parsed.value.empty()) {
        parsed.value = "text/plain";
    }
    return parsed;
}

std::optional<ParameterizedHeader> content_disposition_for(const HeaderBlock& headers) {
    const auto raw_value = header_value(headers, "Content-Disposition");
    if (!raw_value.has_value()) {
        return std::nullopt;
    }
    ParameterizedHeader parsed = parse_parameterized_header(*raw_value);
    if (parsed.value.empty()) {
        return std::nullopt;
    }
    return parsed;
}

std::optional<std::string> part_filename(const ParameterizedHeader& content_type,
                                         const std::optional<ParameterizedHeader>& disposition) {
    if (disposition.has_value()) {
        const auto filename = parameter_value(*disposition, "filename");
        if (filename.has_value() && !filename->empty()) {
            return filename;
        }
    }

    const auto content_type_name = parameter_value(content_type, "name");
    if (content_type_name.has_value() && !content_type_name->empty()) {
        return content_type_name;
    }
    return std::nullopt;
}

std::string extension_for_content_type(std::string_view content_type) {
    const std::string lowered = lower_ascii(content_type);
    if (lowered == "image/png") {
        return ".png";
    }
    if (lowered == "image/jpeg" || lowered == "image/jpg") {
        return ".jpg";
    }
    if (lowered == "image/gif") {
        return ".gif";
    }
    if (lowered == "image/webp") {
        return ".webp";
    }
    if (lowered == "image/svg+xml") {
        return ".svg";
    }
    if (lowered == "text/plain") {
        return ".txt";
    }
    if (lowered == "text/html") {
        return ".html";
    }
    if (lowered == "application/pdf") {
        return ".pdf";
    }
    return "";
}

std::string normalize_synthesized_name(const std::optional<std::string>& value) {
    if (!value.has_value()) {
        return "";
    }

    std::string trimmed = trim(*value);
    if (trimmed.size() >= 2 && trimmed.front() == '<' && trimmed.back() == '>') {
        trimmed = trimmed.substr(1, trimmed.size() - 2);
    }

    std::string normalized;
    bool previous_was_separator = false;
    for (char ch : trimmed) {
        if (is_safe_synthesized_char(ch)) {
            normalized.push_back(ch);
            previous_was_separator = false;
        } else if (!previous_was_separator) {
            normalized.push_back('-');
            previous_was_separator = true;
        }
    }

    const auto first = normalized.find_first_not_of("._-");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = normalized.find_last_not_of("._-");
    return normalized.substr(first, last - first + 1);
}

std::string synthesized_attachment_filename(int part_index,
                                            std::string_view content_type,
                                            const std::optional<std::string>& content_id) {
    std::string base_name = normalize_synthesized_name(content_id);
    if (base_name.empty()) {
        base_name = "inline-" + std::to_string(part_index);
    }
    return base_name + extension_for_content_type(content_type);
}

std::string decode_transfer_body(const std::string& body, const HeaderBlock& headers) {
    const std::string encoding = lower_ascii(trim(header_value(headers, "Content-Transfer-Encoding")
                                                     .value_or("")));
    if (encoding == "base64") {
        return decode_base64(body);
    }
    if (encoding == "quoted-printable") {
        return decode_quoted_printable(body);
    }
    return body;
}

std::vector<std::string> split_multipart_body(const std::string& body,
                                              const std::string& boundary) {
    const std::string marker = "--" + boundary;
    const std::string closing_marker = marker + "--";
    std::vector<std::string> parts;
    std::string current;
    bool saw_boundary = false;
    bool in_part = false;

    std::size_t line_start = 0;
    while (line_start <= body.size()) {
        const std::size_t line_end = body.find('\n', line_start);
        const bool has_newline = line_end != std::string::npos;
        const std::size_t effective_end = has_newline ? line_end : body.size();
        const std::string line = body.substr(line_start, effective_end - line_start);

        const std::string delimiter_line = trim_trailing_lwsp(line);
        if (delimiter_line == marker || delimiter_line == closing_marker) {
            saw_boundary = true;
            if (in_part) {
                parts.push_back(current);
                current.clear();
            }
            in_part = delimiter_line != closing_marker;
            if (delimiter_line == closing_marker) {
                break;
            }
        } else if (in_part) {
            current += line;
            if (has_newline) {
                current.push_back('\n');
            }
        }

        if (!has_newline) {
            break;
        }
        line_start = line_end + 1;
    }

    if (!saw_boundary) {
        throw ParseFailure{"invalid multipart boundary"};
    }
    if (in_part) {
        parts.push_back(current);
    }
    return parts;
}

std::pair<std::optional<std::string>, std::optional<std::string>> parse_address_header(
    const std::string& raw_value) {
    const std::string decoded = decode_rfc2047_words(raw_value);
    const std::size_t left = decoded.rfind('<');
    const std::size_t right = left == std::string::npos ? std::string::npos : decoded.find('>', left);
    if (left != std::string::npos && right != std::string::npos && right > left) {
        std::string name = trim(std::string_view(decoded).substr(0, left));
        if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
            name = unquote_parameter(name);
        }
        const std::string addr = trim(std::string_view(decoded).substr(left + 1, right - left - 1));
        return {name.empty() ? std::nullopt : std::optional<std::string>(name),
                addr.empty() ? std::nullopt : std::optional<std::string>(addr)};
    }

    const std::string trimmed = trim(decoded);
    if (trimmed.find('@') != std::string::npos) {
        return {std::nullopt, trimmed};
    }
    return {trimmed.empty() ? std::nullopt : std::optional<std::string>(trimmed), std::nullopt};
}

void assign_top_level_headers(const HeaderBlock& headers, ParsedMail& mail) {
    mail.headers_json = headers_to_json(headers);

    const auto message_id = decoded_header_value(headers, "Message-ID");
    if (message_id.has_value() && !message_id->empty()) {
        mail.message_id_header = message_id;
    }

    const auto subject = decoded_header_value(headers, "Subject");
    if (subject.has_value() && !subject->empty()) {
        mail.subject = subject;
    }

    const auto reply_to = decoded_header_value(headers, "Reply-To");
    if (reply_to.has_value() && !reply_to->empty()) {
        mail.reply_to = reply_to;
    }

    const auto date = decoded_header_value(headers, "Date");
    if (date.has_value() && !date->empty()) {
        mail.date_header = date;
    }

    const auto from = header_value(headers, "From");
    if (from.has_value()) {
        const auto [from_name, from_addr] = parse_address_header(*from);
        mail.from_name = from_name;
        mail.from_addr = from_addr;
    }
}

std::string replace_all(std::string value, std::string_view needle, std::string_view replacement) {
    std::size_t position = 0;
    while ((position = value.find(needle, position)) != std::string::npos) {
        value.replace(position, needle.size(), replacement);
        position += replacement.size();
    }
    return value;
}

std::string decode_basic_html_entities(std::string value) {
    value = replace_all(std::move(value), "&nbsp;", " ");
    value = replace_all(std::move(value), "&amp;", "&");
    value = replace_all(std::move(value), "&lt;", "<");
    value = replace_all(std::move(value), "&gt;", ">");
    value = replace_all(std::move(value), "&quot;", "\"");
    value = replace_all(std::move(value), "&#39;", "'");
    value = replace_all(std::move(value), "&apos;", "'");
    return value;
}

std::string strip_html_tags(const std::string& html) {
    std::string text;
    text.reserve(html.size());
    bool in_tag = false;
    for (char ch : html) {
        if (ch == '<') {
            in_tag = true;
            text.push_back(' ');
        } else if (ch == '>') {
            in_tag = false;
            text.push_back(' ');
        } else if (!in_tag) {
            text.push_back(ch);
        }
    }
    return decode_basic_html_entities(std::move(text));
}

constexpr std::size_t kPreviewMaxBytes = 200;

std::string bounded_valid_utf8_preview(std::string_view value) {
    std::string output;
    output.reserve(std::min(value.size(), kPreviewMaxBytes));

    const auto* bytes = reinterpret_cast<const std::uint8_t*>(value.data());
    const auto length = static_cast<std::int32_t>(
        std::min(value.size(), static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())));
    std::int32_t offset = 0;

    while (offset < length) {
        UChar32 code_point;
        U8_NEXT_OR_FFFD(bytes, offset, length, code_point);

        std::uint8_t encoded[U8_MAX_LENGTH];
        std::int32_t encoded_length = 0;
        U8_APPEND_UNSAFE(encoded, encoded_length, code_point);

        const auto next_size = static_cast<std::size_t>(encoded_length);
        if (next_size > kPreviewMaxBytes - output.size()) {
            break;
        }
        output.append(reinterpret_cast<const char*>(encoded), next_size);
    }

    return output;
}

std::optional<std::string> build_preview(const ParsedMail& mail) {
    std::string source;
    if (mail.has_text && !mail.text_body.empty()) {
        source = mail.text_body;
    } else if (mail.has_html && !mail.html_body.empty()) {
        source = strip_html_tags(mail.html_body);
    }

    std::string collapsed;
    bool in_whitespace = false;
    for (unsigned char ch : source) {
        if (std::isspace(ch)) {
            if (!collapsed.empty()) {
                in_whitespace = true;
            }
        } else {
            if (in_whitespace) {
                collapsed.push_back(' ');
                in_whitespace = false;
            }
            collapsed.push_back(static_cast<char>(ch));
        }
    }

    if (collapsed.empty()) {
        return std::nullopt;
    }
    std::string polished;
    polished.reserve(collapsed.size());
    for (char ch : collapsed) {
        if ((ch == '.' || ch == ',' || ch == ';' || ch == ':' || ch == '!' || ch == '?' ||
             ch == ')' || ch == ']' || ch == '}') &&
            !polished.empty() && polished.back() == ' ') {
            polished.pop_back();
        }
        polished.push_back(ch);
    }
    collapsed = std::move(polished);
    return bounded_valid_utf8_preview(collapsed);
}

void add_attachment(ParseContext& context,
                    int part_index,
                    const HeaderBlock& headers,
                    const ParameterizedHeader& content_type,
                    const std::optional<ParameterizedHeader>& disposition,
                    const std::optional<std::string>& filename,
                    const std::string& content) {
    const auto content_id = decoded_header_value(headers, "Content-ID");
    const std::string attachment_filename =
        filename.value_or(synthesized_attachment_filename(part_index, content_type.value, content_id));

    ParsedAttachment attachment;
    attachment.attachment_id = make_prefixed_id("att_");
    attachment.part_index = part_index;
    attachment.filename = attachment_filename;
    attachment.safe_filename = safe_filename(attachment_filename);
    attachment.content_type = content_type.value;
    if (disposition.has_value()) {
        attachment.content_disposition = disposition->value;
        attachment.is_inline = disposition->value == "inline";
    }
    attachment.content_id = content_id;
    attachment.sha256 = sha256_hex(content);
    attachment.content = content;
    context.mail.attachments.push_back(std::move(attachment));
}

void parse_entity(const std::string& entity, ParseContext& context, bool root) {
    const auto [header_block, body] = split_header_body(entity);
    const HeaderBlock headers = parse_headers(header_block);
    if (root) {
        assign_top_level_headers(headers, context.mail);
    }

    const ParameterizedHeader content_type = content_type_for(headers);
    if (starts_with_ignore_case(content_type.value, "multipart/")) {
        const auto boundary = parameter_value(content_type, "boundary");
        if (!boundary.has_value() || boundary->empty()) {
            throw ParseFailure{"invalid multipart boundary"};
        }
        for (const std::string& part : split_multipart_body(body, *boundary)) {
            parse_entity(part, context, false);
        }
        return;
    }

    const int part_index = context.next_part_index++;
    const std::string decoded_body = decode_transfer_body(body, headers);
    const std::string text_charset = parameter_value(content_type, "charset").value_or("UTF-8");
    const auto disposition = content_disposition_for(headers);
    const std::string disposition_value = disposition.has_value() ? disposition->value : "";
    const auto filename = part_filename(content_type, disposition);
    const auto content_id = decoded_header_value(headers, "Content-ID");
    bool selected_as_body = false;

    if (disposition_value != "attachment" && content_type.value == "text/plain" &&
        !context.mail.has_text) {
        context.mail.text_body = decode_charset(decoded_body, text_charset);
        context.mail.has_text = true;
        selected_as_body = true;
    } else if (disposition_value != "attachment" && content_type.value == "text/html" &&
               !context.mail.has_html) {
        context.mail.html_body = decode_charset(decoded_body, text_charset);
        context.mail.has_html = true;
        selected_as_body = true;
    }

    const bool should_attach = disposition_value == "attachment" || disposition_value == "inline" ||
                               content_id.has_value() || filename.has_value();
    if (!selected_as_body && should_attach) {
        add_attachment(context, part_index, headers, content_type, disposition, filename, decoded_body);
    }
}

int base64_value(unsigned char ch) {
    if (ch >= 'A' && ch <= 'Z') {
        return ch - 'A';
    }
    if (ch >= 'a' && ch <= 'z') {
        return ch - 'a' + 26;
    }
    if (ch >= '0' && ch <= '9') {
        return ch - '0' + 52;
    }
    if (ch == '+') {
        return 62;
    }
    if (ch == '/') {
        return 63;
    }
    return -1;
}

int hex_value(unsigned char ch) {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    return -1;
}

std::string normalized_charset_name(std::string_view charset) {
    const std::string lowered = lower_ascii(trim(charset));
    if (lowered.empty() || lowered == "utf8" || lowered == "utf-8" || lowered == "us-ascii" ||
        lowered == "ascii") {
        return "UTF-8";
    }
    if (lowered == "iso-8859-1" || lowered == "latin1" || lowered == "latin-1" ||
        lowered == "iso8859-1") {
        return "ISO-8859-1";
    }
    if (lowered == "windows-1252" || lowered == "cp1252") {
        return "windows-1252";
    }
    if (lowered == "gbk" || lowered == "cp936") {
        return "GBK";
    }
    if (lowered == "gb2312") {
        return "GB18030";
    }
    return std::string(trim(charset));
}

std::string decode_charset(const std::string& bytes, const std::string& charset) {
    if (bytes.empty()) {
        return bytes;
    }
    if (bytes.size() > static_cast<std::size_t>(std::numeric_limits<int32_t>::max())) {
        return bytes;
    }

    const std::string normalized_charset = normalized_charset_name(charset);
    if (normalized_charset == "UTF-8") {
        return bytes;
    }

    iconv_t converter = iconv_open("UTF-8", normalized_charset.c_str());
    if (converter == reinterpret_cast<iconv_t>(-1)) {
        return bytes;
    }

    std::string output(bytes.size() * 4 + 16, '\0');
    char* input = const_cast<char*>(bytes.data());
    std::size_t input_left = bytes.size();
    char* output_cursor = output.data();
    std::size_t output_left = output.size();

    while (input_left > 0) {
        const std::size_t result = iconv(converter, &input, &input_left, &output_cursor, &output_left);
        if (result != static_cast<std::size_t>(-1)) {
            continue;
        }
        if (errno == E2BIG) {
            const std::size_t used = output.size() - output_left;
            if (output.size() > static_cast<std::size_t>(std::numeric_limits<int32_t>::max()) / 2) {
                iconv_close(converter);
                return bytes;
            }
            output.resize(output.size() * 2);
            output_cursor = output.data() + used;
            output_left = output.size() - used;
            continue;
        }
        iconv_close(converter);
        return bytes;
    }

    const std::size_t used = output.size() - output_left;
    iconv_close(converter);
    output.resize(used);
    return output;
}

std::optional<std::string> decode_rfc2047_word_at(const std::string& value,
                                                  std::size_t start,
                                                  std::size_t& end) {
    if (start + 2 > value.size() || value.compare(start, 2, "=?") != 0) {
        return std::nullopt;
    }

    const std::size_t charset_end = value.find('?', start + 2);
    if (charset_end == std::string::npos) {
        return std::nullopt;
    }
    const std::size_t encoding_end = value.find('?', charset_end + 1);
    if (encoding_end == std::string::npos) {
        return std::nullopt;
    }
    const std::size_t word_end = value.find("?=", encoding_end + 1);
    if (word_end == std::string::npos) {
        return std::nullopt;
    }

    const std::string charset = value.substr(start + 2, charset_end - start - 2);
    const std::string encoding = lower_ascii(
        std::string_view(value).substr(charset_end + 1, encoding_end - charset_end - 1));
    const std::string encoded = value.substr(encoding_end + 1, word_end - encoding_end - 1);

    std::string decoded;
    if (encoding == "b") {
        decoded = decode_base64(encoded);
    } else if (encoding == "q") {
        std::string qp = encoded;
        std::replace(qp.begin(), qp.end(), '_', ' ');
        decoded = decode_quoted_printable(qp);
    } else {
        return std::nullopt;
    }

    end = word_end + 2;
    return decode_charset(decoded, charset);
}

}  // namespace

std::string decode_base64(const std::string& value) {
    std::string output;
    output.reserve((value.size() * 3) / 4);

    int buffer = 0;
    int bits = -8;
    for (unsigned char ch : value) {
        if (ascii_space(ch)) {
            continue;
        }
        if (ch == '=') {
            break;
        }

        const int decoded = base64_value(ch);
        if (decoded < 0) {
            continue;
        }

        buffer = (buffer << 6) | decoded;
        bits += 6;
        if (bits >= 0) {
            output.push_back(static_cast<char>((buffer >> bits) & 0xff));
            bits -= 8;
        }
    }

    return output;
}

std::string decode_quoted_printable(const std::string& value) {
    std::string output;
    output.reserve(value.size());

    for (std::size_t index = 0; index < value.size();) {
        const char ch = value[index];
        if (ch != '=') {
            output.push_back(ch);
            ++index;
            continue;
        }

        if (index + 1 < value.size() && value[index + 1] == '\n') {
            index += 2;
            continue;
        }
        if (index + 2 < value.size() && value[index + 1] == '\r' && value[index + 2] == '\n') {
            index += 3;
            continue;
        }

        if (index + 2 < value.size()) {
            const int high = hex_value(static_cast<unsigned char>(value[index + 1]));
            const int low = hex_value(static_cast<unsigned char>(value[index + 2]));
            if (high >= 0 && low >= 0) {
                output.push_back(static_cast<char>((high << 4) | low));
                index += 3;
                continue;
            }
        }

        output.push_back('=');
        ++index;
    }

    return output;
}

std::string decode_rfc2047_words(const std::string& value) {
    std::string output;
    output.reserve(value.size());

    std::size_t index = 0;
    while (index < value.size()) {
        std::size_t word_end = index;
        const auto decoded_word = decode_rfc2047_word_at(value, index, word_end);
        if (!decoded_word.has_value()) {
            output.push_back(value[index]);
            ++index;
            continue;
        }

        output += *decoded_word;
        index = word_end;

        const std::size_t whitespace_start = index;
        while (index < value.size() && ascii_space(static_cast<unsigned char>(value[index]))) {
            ++index;
        }

        std::size_t next_word_end = index;
        if (index < value.size() &&
            decode_rfc2047_word_at(value, index, next_word_end).has_value()) {
            continue;
        }

        index = whitespace_start;
    }

    return output;
}

ParsedMail MimeParser::parse(const std::string& raw_message) const {
    ParseContext context;
    parse_entity(normalize_line_endings(raw_message), context, true);
    context.mail.has_attachments = !context.mail.attachments.empty();
    context.mail.attachment_count = static_cast<int>(context.mail.attachments.size());
    context.mail.text_preview = build_preview(context.mail);
    return context.mail;
}

}  // namespace rapid_inbox::ingestd
