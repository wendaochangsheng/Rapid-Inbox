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
#include <unicode/ustring.h>

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

bool ends_with(const std::string& value, const std::string& suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool is_ascii_domain(std::string_view value) {
    return std::all_of(value.begin(), value.end(), [](unsigned char ch) {
        return ch < 0x80;
    });
}

void ascii_lower_in_place(std::string& value) {
    for (char& ch : value) {
        if (ch >= 'A' && ch <= 'Z') {
            ch = static_cast<char>(ch - 'A' + 'a');
        }
    }
}

void validate_ascii_domain_labels(const std::string& domain) {
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

void validate_utf8_text(std::string_view value) {
    (void)utf8_to_uchars(value);
}

std::string canonicalize_local_part(const std::string& local_part, const DomainRule& rule) {
    if (!rule.local_part_case_sensitive) {
        validate_utf8_text(local_part);
    }

    std::string canonical = local_part;
    if (rule.plus_addressing_mode == "strip") {
        const std::string::size_type plus = canonical.find('+');
        if (plus != std::string::npos) {
            canonical.erase(plus);
        }
    }
    if (!rule.local_part_case_sensitive) {
        canonical = utf8_lower(std::move(canonical));
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
        return domain;
    }
    if (domain.find('\0') != std::string::npos) {
        throw std::invalid_argument("embedded NUL in domain");
    }
    if (is_ascii_domain(domain)) {
        ascii_lower_in_place(domain);
        validate_ascii_domain_labels(domain);
        return domain;
    }

    domain = utf8_lower(std::move(domain));
    return normalize_domain_icu_idna(domain);
}

DomainMatcher::DomainMatcher(std::vector<DomainRule> rules) : rules_(std::move(rules)) {
    for (DomainRule& rule : rules_) {
        rule.root_domain_ascii = normalize_domain(rule.root_domain_ascii);
    }
    std::stable_sort(rules_.begin(), rules_.end(), [](const DomainRule& lhs, const DomainRule& rhs) {
        return lhs.root_domain_ascii.size() > rhs.root_domain_ascii.size();
    });
}

std::optional<DomainMatch> DomainMatcher::match_address(const std::string& address) const {
    const std::string::size_type at = address.rfind('@');
    if (at == std::string::npos) {
        return std::nullopt;
    }

    const std::string local_part = address.substr(0, at);
    std::string domain_ascii;
    try {
        domain_ascii = normalize_domain(address.substr(at + 1));
    } catch (const std::exception&) {
        return std::nullopt;
    }

    for (const DomainRule& rule : rules_) {
        const bool is_exact = domain_ascii == rule.root_domain_ascii;
        const bool is_subdomain = ends_with(domain_ascii, "." + rule.root_domain_ascii);
        if (!is_exact && !is_subdomain) {
            continue;
        }
        if (is_exact && !rule.accept_exact) {
            return std::nullopt;
        }
        if (is_subdomain && !rule.accept_subdomains) {
            return std::nullopt;
        }

        std::string local_part_canonical;
        try {
            local_part_canonical = canonicalize_local_part(local_part, rule);
        } catch (const std::exception&) {
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

    return std::nullopt;
}

}
