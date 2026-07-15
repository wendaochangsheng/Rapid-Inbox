#include "domain_matcher.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <utility>
#include <vector>

#include <unicode/uidna.h>
#include <unicode/uchar.h>
#include <unicode/ustring.h>
#include <unicode/utf16.h>

extern "C" {
// Small hand-declared libunistring ABI surface; this target links the shared library directly.
unsigned char* u8_tolower(const unsigned char* s,
                          std::size_t n,
                          const char* iso639_language,
                          void* nf,
                          unsigned char* resultbuf,
                          std::size_t* lengthp);
}

namespace rapid_inbox::ingestd {
namespace {

constexpr UErrorCode kUZeroError = U_ZERO_ERROR;
constexpr UErrorCode kUBufferOverflowError = U_BUFFER_OVERFLOW_ERROR;
constexpr std::int32_t kUidnaAllowUnassigned = 1;

constexpr std::string_view kUtf8PythonWhitespace[] = {
    "\xC2\x85",     "\xC2\xA0",     "\xE1\x9A\x80", "\xE2\x80\x80",
    "\xE2\x80\x81", "\xE2\x80\x82", "\xE2\x80\x83", "\xE2\x80\x84",
    "\xE2\x80\x85", "\xE2\x80\x86", "\xE2\x80\x87", "\xE2\x80\x88",
    "\xE2\x80\x89", "\xE2\x80\x8A", "\xE2\x80\xA8", "\xE2\x80\xA9",
    "\xE2\x80\xAF", "\xE2\x81\x9F", "\xE3\x80\x80",
};

bool is_ascii_python_whitespace(unsigned char ch) {
    return (ch >= 0x09 && ch <= 0x0D) || (ch >= 0x1C && ch <= 0x20);
}

bool equals_at(const std::string& value, std::size_t pos, std::string_view expected) {
    return pos <= value.size() && expected.size() <= value.size() - pos &&
           std::equal(expected.begin(), expected.end(), value.begin() + pos);
}

std::size_t whitespace_prefix_length(const std::string& value, std::size_t pos, std::size_t end) {
    if (pos >= end) {
        return 0;
    }
    if (is_ascii_python_whitespace(static_cast<unsigned char>(value[pos]))) {
        return 1;
    }
    for (std::string_view expected : kUtf8PythonWhitespace) {
        if (expected.size() <= end - pos && equals_at(value, pos, expected)) {
            return expected.size();
        }
    }
    return 0;
}

std::size_t whitespace_suffix_length(const std::string& value,
                                     std::size_t begin,
                                     std::size_t end) {
    if (end <= begin) {
        return 0;
    }
    if (is_ascii_python_whitespace(static_cast<unsigned char>(value[end - 1]))) {
        return 1;
    }
    for (std::string_view expected : kUtf8PythonWhitespace) {
        if (expected.size() > end - begin) {
            continue;
        }
        if (equals_at(value, end - expected.size(), expected)) {
            return expected.size();
        }
    }
    return 0;
}

std::string strip_unicode_whitespace(std::string value) {
    std::size_t begin = 0;
    std::size_t end = value.size();

    while (begin < end) {
        const std::size_t width = whitespace_prefix_length(value, begin, end);
        if (width == 0) {
            break;
        }
        begin += width;
    }

    while (end > begin) {
        const std::size_t width = whitespace_suffix_length(value, begin, end);
        if (width == 0) {
            break;
        }
        end -= width;
    }

    return value.substr(begin, end - begin);
}

std::string utf8_lower(std::string value) {
    if (value.empty()) {
        return value;
    }

    std::size_t length = 0;
    using LoweredPtr = std::unique_ptr<unsigned char, decltype(&std::free)>;
    LoweredPtr lowered(u8_tolower(reinterpret_cast<const unsigned char*>(value.data()),
                                  value.size(),
                                  nullptr,
                                  nullptr,
                                  nullptr,
                                  &length),
                       &std::free);
    if (!lowered) {
        throw std::runtime_error("unicode lowercase failed");
    }

    return std::string(reinterpret_cast<const char*>(lowered.get()), length);
}

bool is_ascii_domain(std::string_view value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) {
        return ch < 0x80;
    });
}

std::string map_idna_dot_separators(std::string domain) {
    constexpr std::string_view separators[] = {"\xE3\x80\x82", "\xEF\xBC\x8E", "\xEF\xBD\xA1"};
    std::string mapped;
    mapped.reserve(domain.size());
    for (std::size_t position = 0; position < domain.size();) {
        bool replaced = false;
        for (std::string_view separator : separators) {
            if (equals_at(domain, position, separator)) {
                mapped.push_back('.');
                position += separator.size();
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            mapped.push_back(domain[position++]);
        }
    }
    return mapped;
}

void validate_raw_domain_label_edges(const std::string& domain) {
    const std::string mapped = map_idna_dot_separators(domain);
    std::size_t label_start = 0;
    while (label_start < mapped.size()) {
        const std::size_t dot = mapped.find('.', label_start);
        const std::size_t label_end = dot == std::string::npos ? mapped.size() : dot;
        if (label_end == label_start || mapped[label_start] == '-' || mapped[label_end - 1] == '-') {
            throw std::invalid_argument("domain label cannot start or end with hyphen");
        }
        if (dot == std::string::npos) {
            return;
        }
        label_start = dot + 1;
    }
    throw std::invalid_argument("empty domain label");
}

void ascii_lower_in_place(std::string& value) {
    for (char& ch : value) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
}

void validate_ascii_domain_labels(const std::string& domain) {
    if (domain.empty() || domain.front() == '.' || domain.back() == '.') {
        throw std::invalid_argument("empty domain label");
    }
    if (domain.size() > kMaximumDomainBytes) {
        throw std::invalid_argument("domain is too long");
    }
    std::size_t label_start = 0;
    while (label_start < domain.size()) {
        const std::size_t dot = domain.find('.', label_start);
        const std::size_t label_end = dot == std::string::npos ? domain.size() : dot;
        const std::size_t label_length = label_end - label_start;
        if (label_length == 0) {
            throw std::invalid_argument("empty domain label");
        }
        if (label_length > 63) {
            throw std::invalid_argument("domain label too long");
        }
        if (domain[label_start] == '-' || domain[label_end - 1] == '-') {
            throw std::invalid_argument("domain label cannot start or end with hyphen");
        }
        for (std::size_t index = label_start; index < label_end; ++index) {
            const unsigned char ch = static_cast<unsigned char>(domain[index]);
            const bool is_letter = (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z');
            const bool is_digit = ch >= '0' && ch <= '9';
            if (!is_letter && !is_digit && ch != '-') {
                throw std::invalid_argument("invalid domain label character");
            }
        }
        if (dot == std::string::npos) {
            return;
        }
        label_start = dot + 1;
    }
}

std::int32_t checked_icu_length(std::size_t length) {
    if (length > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())) {
        throw std::invalid_argument("domain is too long for IDNA normalization");
    }
    return static_cast<std::int32_t>(length);
}

std::int32_t grow_icu_capacity(std::int32_t current, std::int32_t required) {
    if (required < 0 || required == std::numeric_limits<std::int32_t>::max()) {
        throw std::invalid_argument("domain is too long for IDNA normalization");
    }
    const std::int64_t doubled = static_cast<std::int64_t>(std::max(current, 1)) * 2;
    const std::int64_t wanted = std::max<std::int64_t>(doubled, required + 1LL);
    if (wanted > std::numeric_limits<std::int32_t>::max()) {
        throw std::invalid_argument("domain is too long for IDNA normalization");
    }
    return static_cast<std::int32_t>(wanted);
}

std::vector<UChar> utf8_to_uchars(std::string_view value) {
    const std::int32_t src_length = checked_icu_length(value.size());
    std::int32_t capacity = grow_icu_capacity(src_length, src_length);

    for (;;) {
        std::vector<UChar> output(static_cast<std::size_t>(capacity));
        std::int32_t output_length = 0;
        UErrorCode status = kUZeroError;
        (void)u_strFromUTF8(output.data(),
                            capacity,
                            &output_length,
                            value.data(),
                            src_length,
                            &status);
        if (status == kUZeroError && output_length <= capacity) {
            output.resize(static_cast<std::size_t>(output_length));
            return output;
        }
        if (status == kUBufferOverflowError || output_length > capacity) {
            capacity = grow_icu_capacity(capacity, output_length);
            continue;
        }
        throw std::invalid_argument("invalid UTF-8 domain");
    }
}

std::vector<UChar> idna_to_ascii_uchars(const std::vector<UChar>& input) {
    const std::int32_t src_length = checked_icu_length(input.size());
    const std::int64_t initial_capacity =
        std::max<std::int64_t>(static_cast<std::int64_t>(src_length) * 2 + 1, 32);
    if (initial_capacity > std::numeric_limits<std::int32_t>::max()) {
        throw std::invalid_argument("domain is too long for IDNA normalization");
    }
    std::int32_t capacity = static_cast<std::int32_t>(initial_capacity);

    for (;;) {
        std::vector<UChar> output(static_cast<std::size_t>(capacity));
        UErrorCode status = kUZeroError;
        const std::int32_t output_length = uidna_IDNToASCII(input.data(),
                                                           src_length,
                                                           output.data(),
                                                           capacity,
                                                           kUidnaAllowUnassigned,
                                                           nullptr,
                                                           &status);
        if (status == kUZeroError && output_length <= capacity) {
            output.resize(static_cast<std::size_t>(output_length));
            return output;
        }
        if (status == kUBufferOverflowError || output_length > capacity) {
            capacity = grow_icu_capacity(capacity, output_length);
            continue;
        }
        throw std::invalid_argument("IDNA domain normalization failed");
    }
}

std::string uchars_to_utf8(const std::vector<UChar>& input) {
    const std::int32_t src_length = checked_icu_length(input.size());
    std::int32_t capacity = grow_icu_capacity(src_length, src_length);

    for (;;) {
        std::string output(static_cast<std::size_t>(capacity), '\0');
        std::int32_t output_length = 0;
        UErrorCode status = kUZeroError;
        (void)u_strToUTF8(output.data(),
                          capacity,
                          &output_length,
                          input.data(),
                          src_length,
                          &status);
        if (status == kUZeroError && output_length <= capacity) {
            output.resize(static_cast<std::size_t>(output_length));
            return output;
        }
        if (status == kUBufferOverflowError || output_length > capacity) {
            capacity = grow_icu_capacity(capacity, output_length);
            continue;
        }
        throw std::invalid_argument("IDNA UTF-8 conversion failed");
    }
}

std::string normalize_domain_icu_idna(std::string_view domain) {
    return uchars_to_utf8(idna_to_ascii_uchars(utf8_to_uchars(domain)));
}

bool is_ascii_atext(unsigned char ch) {
    return (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z') ||
           (ch >= '0' && ch <= '9') || std::string_view("!#$%&'*+-/=?^_`{|}~").find(ch) !=
                                                std::string_view::npos;
}

bool valid_local_part(std::string_view local_part, bool allow_smtputf8) {
    if (local_part.empty() || local_part.size() > kMaximumLocalPartBytes ||
        local_part.front() == '.' || local_part.back() == '.') {
        return false;
    }
    const bool ascii_only = is_ascii_domain(local_part);
    if (!ascii_only) {
        if (!allow_smtputf8) {
            return false;
        }
        std::vector<UChar> unicode;
        try {
            unicode = utf8_to_uchars(local_part);
        } catch (const std::exception&) {
            return false;
        }
        for (std::int32_t offset = 0; offset < static_cast<std::int32_t>(unicode.size());) {
            UChar32 code_point = 0;
            U16_NEXT(unicode.data(), offset, static_cast<std::int32_t>(unicode.size()), code_point);
            const UCharCategory category =
                code_point < 0 ? U_UNASSIGNED
                               : static_cast<UCharCategory>(u_charType(code_point));
            if (code_point < 0 || u_isUWhiteSpace(code_point) || category == U_CONTROL_CHAR ||
                category == U_FORMAT_CHAR || category == U_SURROGATE ||
                category == U_PRIVATE_USE_CHAR || category == U_UNASSIGNED) {
                return false;
            }
        }
    }

    bool previous_dot = false;
    for (unsigned char ch : local_part) {
        if (ch >= 0x80) {
            if (!allow_smtputf8) {
                return false;
            }
            previous_dot = false;
            continue;
        }
        if (ch == '.') {
            if (previous_dot) {
                return false;
            }
            previous_dot = true;
            continue;
        }
        if (!is_ascii_atext(ch)) {
            return false;
        }
        previous_dot = false;
    }
    return true;
}

std::string canonicalize_local_part(const std::string& local_part, const DomainRule& rule) {
    std::string canonical = local_part;
    if (rule.plus_addressing_mode == "strip") {
        const std::string::size_type plus = canonical.find('+');
        if (plus != std::string::npos) {
            canonical.erase(plus);
        }
    }
    if (!rule.local_part_case_sensitive) {
        if (is_ascii_domain(canonical)) {
            ascii_lower_in_place(canonical);
        } else {
            canonical = utf8_lower(std::move(canonical));
        }
    }
    if (!valid_local_part(canonical, true)) {
        throw std::invalid_argument("invalid canonical local part");
    }
    return canonical;
}

}  // namespace

std::string normalize_domain(std::string domain) {
    domain = strip_unicode_whitespace(std::move(domain));
    while (!domain.empty() && domain.back() == '.') {
        domain.pop_back();
    }

    if (domain.empty()) {
        throw std::invalid_argument("empty domain");
    }
    if (domain == "*") {
        return domain;
    }
    if (domain.find('\0') != std::string::npos) {
        throw std::invalid_argument("embedded NUL in domain");
    }
    validate_raw_domain_label_edges(domain);
    if (is_ascii_domain(domain)) {
        ascii_lower_in_place(domain);
        validate_ascii_domain_labels(domain);
        return domain;
    }

    domain = utf8_lower(std::move(domain));
    domain = normalize_domain_icu_idna(domain);
    if (!is_ascii_domain(domain)) {
        throw std::invalid_argument("IDNA normalization returned non-ASCII domain");
    }
    ascii_lower_in_place(domain);
    validate_ascii_domain_labels(domain);
    return domain;
}

std::optional<ParsedMailboxAddress> parse_mailbox_address(const std::string& address,
                                                          bool allow_smtputf8) {
    if (address.empty() || address.size() > kMaximumMailboxBytes) {
        return std::nullopt;
    }
    const std::string::size_type at = address.find('@');
    if (at == std::string::npos || at != address.rfind('@')) {
        return std::nullopt;
    }
    const std::string local_part = address.substr(0, at);
    const std::string raw_domain = address.substr(at + 1);
    if (!valid_local_part(local_part, allow_smtputf8) || raw_domain.empty() ||
        raw_domain.back() == '.' || strip_unicode_whitespace(raw_domain) != raw_domain) {
        return std::nullopt;
    }
    if (!allow_smtputf8 && !is_ascii_domain(raw_domain)) {
        return std::nullopt;
    }

    std::string domain_ascii;
    try {
        domain_ascii = normalize_domain(raw_domain);
    } catch (const std::exception&) {
        return std::nullopt;
    }
    if (domain_ascii == "*" || domain_ascii.size() > kMaximumDomainBytes ||
        local_part.size() + 1 + domain_ascii.size() > kMaximumMailboxBytes) {
        return std::nullopt;
    }
    return ParsedMailboxAddress{local_part, domain_ascii};
}

DomainMatcher::DomainMatcher(std::vector<DomainRule> rules) {
    rules_by_root_.reserve(rules.size());
    for (DomainRule& rule : rules) {
        rule.root_domain_ascii = normalize_domain(rule.root_domain_ascii);
        if (rule.root_domain_ascii == "*") {
            if (!catch_all_rule_.has_value()) {
                catch_all_rule_ = std::move(rule);
            }
            continue;
        }
        // Preserve the previous stable-sort semantics for duplicate roots: the
        // first configured rule wins. DomainCache loads rules by ascending id.
        std::string root = rule.root_domain_ascii;
        rules_by_root_.try_emplace(std::move(root), std::move(rule));
    }
}

std::optional<DomainMatch> DomainMatcher::match_address(const std::string& address) const {
    const auto parsed = parse_mailbox_address(address, true);
    if (!parsed.has_value()) {
        return std::nullopt;
    }
    const std::string& local_part = parsed->local_part;
    const std::string& domain_ascii = parsed->domain_ascii;

    const DomainRule* matched_rule = nullptr;
    bool is_exact = false;

    const auto exact = rules_by_root_.find(std::string_view(domain_ascii));
    if (exact != rules_by_root_.end()) {
        matched_rule = &exact->second;
        is_exact = true;
    } else {
        // Candidate suffixes are visited from longest to shortest. Heterogeneous
        // string_view lookup avoids allocating a temporary string per DNS label.
        for (std::size_t dot = domain_ascii.find('.'); dot != std::string::npos;
             dot = domain_ascii.find('.', dot + 1)) {
            const std::string_view suffix(domain_ascii.data() + dot + 1,
                                          domain_ascii.size() - dot - 1);
            const auto candidate = rules_by_root_.find(suffix);
            if (candidate != rules_by_root_.end()) {
                matched_rule = &candidate->second;
                break;
            }
        }
    }

    if (matched_rule != nullptr) {
        const DomainRule& rule = *matched_rule;
        if (is_exact && !rule.accept_exact) {
            return std::nullopt;
        }
        if (!is_exact && !rule.accept_subdomains) {
            return std::nullopt;
        }

        std::string local_part_canonical;
        try {
            local_part_canonical = canonicalize_local_part(local_part, rule);
        } catch (const std::exception&) {
            return std::nullopt;
        }
        if (local_part_canonical.empty() ||
            local_part_canonical.size() + 1 + domain_ascii.size() > kMaximumMailboxBytes) {
            return std::nullopt;
        }
        return DomainMatch{
            .domain_id = rule.domain_id,
            .domain_ascii = domain_ascii,
            .root_domain_ascii = rule.root_domain_ascii,
            .local_part = local_part,
            .local_part_canonical = local_part_canonical,
            .address_canonical = local_part_canonical + "@" + domain_ascii,
        };
    }

    if (catch_all_rule_.has_value()) {
        const DomainRule& catch_all_rule = *catch_all_rule_;
        std::string local_part_canonical;
        try {
            local_part_canonical = canonicalize_local_part(local_part, catch_all_rule);
        } catch (const std::exception&) {
            return std::nullopt;
        }
        if (local_part_canonical.empty() ||
            local_part_canonical.size() + 1 + domain_ascii.size() > kMaximumMailboxBytes) {
            return std::nullopt;
        }
        return DomainMatch{
            .domain_id = catch_all_rule.domain_id,
            .domain_ascii = domain_ascii,
            .root_domain_ascii = "*",
            .local_part = local_part,
            .local_part_canonical = local_part_canonical,
            .address_canonical = local_part_canonical + "@" + domain_ascii,
        };
    }

    return std::nullopt;
}

}
