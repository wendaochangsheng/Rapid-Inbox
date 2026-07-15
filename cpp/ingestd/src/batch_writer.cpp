#include "batch_writer.h"

#include "id.h"
#include "json_util.h"
#include "mime_parser.h"
#include "sha256.h"
#include "sqlite_db.h"
#include "storage_path.h"
#include "time_utils.h"
#include "verification_code.h"

#include <sqlite3.h>

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <system_error>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <unordered_set>
#include <unistd.h>
#include <utility>
#include <variant>

namespace rapid_inbox::ingestd {
namespace {

constexpr std::string_view kMaintenanceLockPath = ".maintenance.lock";
constexpr std::string_view kMaintenanceDrainedPath = ".maintenance.drained.json";
constexpr std::string_view kIngestStatusPath = ".ingestd.status.json";

class UniqueFd {
public:
    explicit UniqueFd(int fd) : fd_(fd) {}

    ~UniqueFd() {
        if (fd_ >= 0) {
            (void)::close(fd_);
        }
    }

    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    UniqueFd(UniqueFd&& other) noexcept : fd_(std::exchange(other.fd_, -1)) {}

    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            if (fd_ >= 0) {
                (void)::close(fd_);
            }
            fd_ = std::exchange(other.fd_, -1);
        }
        return *this;
    }

    int get() const {
        return fd_;
    }

    void close_or_throw(const std::string& context) {
        if (fd_ < 0) {
            return;
        }
        const int fd = std::exchange(fd_, -1);
        if (::close(fd) != 0) {
            throw std::system_error(errno, std::generic_category(), context);
        }
    }

private:
    int fd_;
};

UniqueFd open_for_fsync(const std::filesystem::path& path, int extra_flags) {
    const int fd = ::open(path.c_str(), O_RDONLY | extra_flags);
    if (fd < 0) {
        const int error = errno;
        throw std::system_error(error,
                                std::generic_category(),
                                "open failed for fsync: " + path.string());
    }
    return UniqueFd(fd);
}

void fsync_path(const std::filesystem::path& path, int extra_flags) {
    UniqueFd fd = open_for_fsync(path, extra_flags);
    if (::fsync(fd.get()) != 0) {
        const int error = errno;
        throw std::system_error(error, std::generic_category(), "fsync failed: " + path.string());
    }
    fd.close_or_throw("close failed after fsync: " + path.string());
}

void fsync_directory(const std::filesystem::path& path) {
    fsync_path(path, O_DIRECTORY);
}

bool path_is_at_or_inside_root(const std::filesystem::path& root,
                               const std::filesystem::path& target) {
    if (target == root) {
        return true;
    }
    const auto relative = target.lexically_relative(root);
    if (relative.empty()) {
        return false;
    }
    for (const auto& part : relative) {
        if (part == "..") {
            return false;
        }
    }
    return true;
}

void throw_errno(const std::string& context, int error) {
    throw std::system_error(error, std::generic_category(), context);
}

void chmod_private(const std::filesystem::path& path, bool directory) {
    const auto permissions = directory
                                 ? std::filesystem::perms::owner_all
                                 : std::filesystem::perms::owner_read |
                                       std::filesystem::perms::owner_write;
    std::filesystem::permissions(path, permissions, std::filesystem::perm_options::replace);
}

void mkdir_private(const std::filesystem::path& path) {
    if (::mkdir(path.c_str(), 0700) == 0) {
        return;
    }
    const int error = errno;
    if (error == EEXIST) {
        struct stat status {};
        if (::stat(path.c_str(), &status) != 0) {
            const int stat_error = errno;
            throw_errno("stat failed for directory: " + path.string(), stat_error);
        }
        if (!S_ISDIR(status.st_mode)) {
            throw std::runtime_error("storage path component is not a directory: " +
                                     path.string());
        }
        return;
    }
    throw_errno("mkdir failed: " + path.string(), error);
}

void ensure_private_directory_chain(const std::filesystem::path& root,
                                    const std::filesystem::path& directory) {
    const auto canonical_root = std::filesystem::weakly_canonical(root);
    const auto canonical_directory = std::filesystem::weakly_canonical(directory);
    if (!path_is_at_or_inside_root(canonical_root, canonical_directory)) {
        throw std::runtime_error("storage directory path escapes storage root");
    }

    std::filesystem::path current = canonical_directory.root_path();
    for (const auto& part : canonical_directory.relative_path()) {
        current /= part;
        mkdir_private(current);
        if (path_is_at_or_inside_root(canonical_root, current)) {
            chmod_private(current, true);
        }
    }
}

void fsync_directory_chain_to_filesystem_root(const std::filesystem::path& root,
                                              const std::filesystem::path& directory) {
    const auto canonical_root = std::filesystem::weakly_canonical(root);
    auto current = std::filesystem::weakly_canonical(directory);
    if (!path_is_at_or_inside_root(canonical_root, current)) {
        throw std::runtime_error("fsync directory path escapes storage root");
    }

    while (true) {
        fsync_directory(current);
        if (current == canonical_root) {
            break;
        }
        current = current.parent_path();
    }
}

const char* json_bool(int value) {
    return value == 0 ? "false" : "true";
}

void write_all(UniqueFd& fd, const std::filesystem::path& path, const std::string& content) {
    const char* cursor = content.data();
    std::size_t remaining = content.size();
    while (remaining > 0) {
        const ssize_t written = ::write(fd.get(), cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            const int write_error = errno;
            throw_errno("write failed: " + path.string(), write_error);
        }
        if (written == 0) {
            throw std::runtime_error("write made no progress: " + path.string());
        }
        cursor += written;
        remaining -= static_cast<std::size_t>(written);
    }
}

std::pair<UniqueFd, std::filesystem::path> create_temp_file(const std::filesystem::path& target) {
    const std::string filename = target.filename().string();
    std::string temp_filename;
    temp_filename.reserve(filename.size() + 12);
    temp_filename.push_back('.');
    temp_filename.append(filename);
    temp_filename.append(".tmp.XXXXXX");
    const auto temp_path = target.parent_path() / temp_filename;
    std::string temp_template = temp_path.string();
    const int fd = ::mkstemp(temp_template.data());
    if (fd < 0) {
        const int mkstemp_error = errno;
        throw_errno("mkstemp failed: " + temp_path.string(), mkstemp_error);
    }
    return {UniqueFd(fd), std::filesystem::path(temp_template)};
}

std::string build_domain_policy(const DomainPolicySnapshot& policy) {
    std::ostringstream output;
    output << "{";
    output << "\"root_domain_unicode\":\"" << json_escape(policy.root_domain_unicode) << "\",";
    output << "\"accept_exact\":" << json_bool(policy.accept_exact ? 1 : 0) << ",";
    output << "\"accept_subdomains\":" << json_bool(policy.accept_subdomains ? 1 : 0) << ",";
    output << "\"public_web_enabled\":" << json_bool(policy.public_web_enabled ? 1 : 0) << ",";
    output << "\"public_api_enabled\":" << json_bool(policy.public_api_enabled ? 1 : 0) << ",";
    output << "\"is_active\":" << json_bool(policy.is_active ? 1 : 0) << ",";
    output << "\"is_hidden\":" << json_bool(policy.is_hidden ? 1 : 0) << ",";
    output << "\"plus_addressing_mode\":\"" << json_escape(policy.plus_addressing_mode) << "\",";
    output << "\"local_part_case_sensitive\":"
           << json_bool(policy.local_part_case_sensitive ? 1 : 0) << ",";
    output << "\"max_message_size_bytes\":" << policy.max_message_size_bytes << ",";
    output << "\"retention_days\":";
    if (policy.retention_days.has_value()) {
        output << *policy.retention_days;
    } else {
        output << "null";
    }
    output << ",";
    output << "\"dns_status\":\"" << json_escape(policy.dns_status) << "\"";
    output << "}";
    return output.str();
}

std::runtime_error sqlite_bind_error(sqlite3_stmt* statement,
                                     int rc,
                                     std::string_view context) {
    sqlite3* db = sqlite3_db_handle(statement);
    const char* message = db == nullptr ? sqlite3_errstr(rc) : sqlite3_errmsg(db);
    std::string rendered;
    rendered.reserve(context.size() + 2 + std::char_traits<char>::length(message));
    rendered.append(context);
    rendered.append(": ");
    rendered.append(message);
    return std::runtime_error(rendered);
}

void bind_text(Statement& statement,
               int index,
               std::string_view value,
               std::string_view context) {
    const int rc = sqlite3_bind_text64(statement.get(),
                                       index,
                                       value.data(),
                                       static_cast<sqlite3_uint64>(value.size()),
                                       SQLITE_TRANSIENT,
                                       SQLITE_UTF8);
    if (rc != SQLITE_OK) {
        throw sqlite_bind_error(statement.get(), rc, context);
    }
}

void bind_int64(Statement& statement,
                int index,
                sqlite3_int64 value,
                std::string_view context) {
    const int rc = sqlite3_bind_int64(statement.get(), index, value);
    if (rc != SQLITE_OK) {
        throw sqlite_bind_error(statement.get(), rc, context);
    }
}

void bind_optional_text(Statement& statement,
                        int index,
                        const std::optional<std::string>& value,
                        std::string_view context) {
    if (!value.has_value()) {
        const int rc = sqlite3_bind_null(statement.get(), index);
        if (rc != SQLITE_OK) {
            throw sqlite_bind_error(statement.get(), rc, context);
        }
        return;
    }
    bind_text(statement, index, *value, context);
}

void bind_null(Statement& statement, int index, std::string_view context) {
    const int rc = sqlite3_bind_null(statement.get(), index);
    if (rc != SQLITE_OK) {
        throw sqlite_bind_error(statement.get(), rc, context);
    }
}

struct MailboxRecord {
    sqlite3_int64 id;
    sqlite3_int64 domain_id;
    bool is_catch_all;
    std::string first_seen_at;
    std::string last_seen_at;
    int public_enabled;
    int is_hidden;
    std::optional<std::string> notes;
    sqlite3_int64 bulk_delete_generation;
};

struct DeliveryMergeUpdate {
    std::string id;
    std::string status;
    std::string delivered_at;
    std::optional<std::string> expires_at;
    std::optional<std::string> deleted_at;
    std::optional<std::string> notes;
};

struct MailboxMergeStats {
    sqlite3_int64 moved = 0;
    sqlite3_int64 deduplicated = 0;
};

std::optional<std::string> optional_column_text(sqlite3_stmt* statement, int column) {
    const unsigned char* value = sqlite3_column_text(statement, column);
    if (value == nullptr) {
        return std::nullopt;
    }
    return std::string(reinterpret_cast<const char*>(value));
}

std::string required_column_text(sqlite3_stmt* statement,
                                 int column,
                                 std::string_view context) {
    const auto value = optional_column_text(statement, column);
    if (!value.has_value()) {
        throw std::runtime_error(std::string(context) + " is null");
    }
    return *value;
}

std::vector<MailboxRecord> load_mailboxes_by_address(Statement& statement,
                                                     const std::string& address) {
    bind_text(statement, 1, address, "bind mailbox address lookup");
    std::vector<MailboxRecord> records;
    while (statement.step_row()) {
        records.push_back(MailboxRecord{
            .id = sqlite3_column_int64(statement.get(), 0),
            .domain_id = sqlite3_column_int64(statement.get(), 1),
            .is_catch_all =
                required_column_text(statement.get(), 2, "mailbox domain root") == "*",
            .first_seen_at = required_column_text(statement.get(), 3, "mailbox first_seen_at"),
            .last_seen_at = required_column_text(statement.get(), 4, "mailbox last_seen_at"),
            .public_enabled = sqlite3_column_int(statement.get(), 5),
            .is_hidden = sqlite3_column_int(statement.get(), 6),
            .notes = optional_column_text(statement.get(), 7),
            .bulk_delete_generation = sqlite3_column_int64(statement.get(), 8),
        });
    }
    statement.reset();
    return records;
}

int delivery_status_rank(std::string_view status) {
    if (status == "active") {
        return 2;
    }
    if (status == "hidden") {
        return 1;
    }
    return 0;
}

const ParsedMail* parsed_result(const std::variant<ParsedMail, ParseFailure>& result) {
    return std::holds_alternative<ParsedMail>(result) ? &std::get<ParsedMail>(result) : nullptr;
}

const ParseFailure* failure_result(const std::variant<ParsedMail, ParseFailure>& result) {
    return std::holds_alternative<ParseFailure>(result) ? &std::get<ParseFailure>(result) : nullptr;
}

void prepare_parsed_artifact_metadata(const MailJob& job, ParsedMail& parsed) {
    if (!parsed.text_body.empty()) {
        parsed.has_text = true;
        parsed.text_body_path = text_body_path(job.message_id, job.received_at);
    }
    if (!parsed.html_body.empty()) {
        parsed.has_html = true;
        parsed.html_body_path = html_body_path(job.message_id, job.received_at);
    }
    for (ParsedAttachment& attachment : parsed.attachments) {
        attachment.attachment_id =
            "att_" + sha256_hex(job.message_id + ":" + std::to_string(attachment.part_index))
                         .substr(0, 32);
        attachment.safe_filename =
            safe_filename(attachment.filename.value_or("attachment.bin"));
        attachment.storage_path =
            attachment_path(job.message_id, attachment.attachment_id, attachment.safe_filename);
        attachment.sha256 = sha256_hex(attachment.content);
    }
    parsed.attachment_count = static_cast<int>(parsed.attachments.size());
    parsed.has_attachments = parsed.attachment_count > 0;
}

std::string parsed_manifest_json(const ParsedMail& parsed) {
    std::ostringstream output;
    output << "{";
    output << "\"status\":\"parsed\",";
    output << "\"message_id_header\":";
    if (parsed.message_id_header.has_value()) {
        output << "\"" << json_escape(*parsed.message_id_header) << "\"";
    } else {
        output << "null";
    }
    output << ",\"subject\":";
    if (parsed.subject.has_value()) {
        output << "\"" << json_escape(*parsed.subject) << "\"";
    } else {
        output << "null";
    }
    output << ",\"from_name\":";
    if (parsed.from_name.has_value()) {
        output << "\"" << json_escape(*parsed.from_name) << "\"";
    } else {
        output << "null";
    }
    output << ",\"from_addr\":";
    if (parsed.from_addr.has_value()) {
        output << "\"" << json_escape(*parsed.from_addr) << "\"";
    } else {
        output << "null";
    }
    output << ",\"reply_to\":";
    if (parsed.reply_to.has_value()) {
        output << "\"" << json_escape(*parsed.reply_to) << "\"";
    } else {
        output << "null";
    }
    output << ",\"date_header\":";
    if (parsed.date_header.has_value()) {
        output << "\"" << json_escape(*parsed.date_header) << "\"";
    } else {
        output << "null";
    }
    output << ",\"has_text\":" << json_bool(parsed.has_text ? 1 : 0);
    output << ",\"has_html\":" << json_bool(parsed.has_html ? 1 : 0);
    output << ",\"has_attachments\":" << json_bool(parsed.has_attachments ? 1 : 0);
    output << ",\"attachment_count\":" << parsed.attachment_count;
    output << ",\"text_preview\":";
    if (parsed.text_preview.has_value()) {
        output << "\"" << json_escape(*parsed.text_preview) << "\"";
    } else {
        output << "null";
    }
    output << ",\"text_body_path\":";
    if (parsed.text_body_path.has_value()) {
        output << "\"" << json_escape(*parsed.text_body_path) << "\"";
    } else {
        output << "null";
    }
    output << ",\"html_body_path\":";
    if (parsed.html_body_path.has_value()) {
        output << "\"" << json_escape(*parsed.html_body_path) << "\"";
    } else {
        output << "null";
    }
    output << ",\"headers_json\":" << parsed.headers_json;
    output << ",\"verification_code\":";
    if (parsed.verification_code.has_value()) {
        output << "\"" << json_escape(*parsed.verification_code) << "\"";
    } else {
        output << "null";
    }
    output << ",\"attachments\":[";
    for (std::size_t index = 0; index < parsed.attachments.size(); ++index) {
        const ParsedAttachment& attachment = parsed.attachments[index];
        if (index != 0) {
            output << ",";
        }
        output << "{";
        output << "\"id\":\"" << json_escape(attachment.attachment_id) << "\",";
        output << "\"part_index\":" << attachment.part_index << ",";
        output << "\"filename\":";
        if (attachment.filename.has_value()) {
            output << "\"" << json_escape(*attachment.filename) << "\"";
        } else {
            output << "null";
        }
        output << ",\"safe_filename\":\"" << json_escape(attachment.safe_filename) << "\",";
        output << "\"content_type\":\"" << json_escape(attachment.content_type) << "\",";
        output << "\"content_disposition\":";
        if (attachment.content_disposition.has_value()) {
            output << "\"" << json_escape(*attachment.content_disposition) << "\"";
        } else {
            output << "null";
        }
        output << ",\"content_id\":";
        if (attachment.content_id.has_value()) {
            output << "\"" << json_escape(*attachment.content_id) << "\"";
        } else {
            output << "null";
        }
        output << ",\"storage_path\":\"" << json_escape(attachment.storage_path) << "\",";
        output << "\"sha256\":\"" << json_escape(attachment.sha256) << "\",";
        output << "\"size_bytes\":" << attachment.content.size() << ",";
        output << "\"is_inline\":" << json_bool(attachment.is_inline ? 1 : 0);
        output << "}";
    }
    output << "]}";
    return output.str();
}

std::string metric_bucket_ts(const std::string& received_at) {
    return received_at.substr(0, 16) + ":00Z";
}

struct MetricBucketDelta {
    sqlite3_int64 received = 0;
    sqlite3_int64 deliveries = 0;
    sqlite3_int64 parse_failures = 0;
};

std::optional<std::string> read_small_text_file(const std::filesystem::path& path) {
    std::error_code error;
    const auto size = std::filesystem::file_size(path, error);
    if (error || size > 16 * 1024) {
        return std::nullopt;
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        return std::nullopt;
    }
    std::string content((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    if (!input.eof() && input.fail()) {
        return std::nullopt;
    }
    return content;
}

std::optional<std::string> json_string_field(const std::string& content,
                                             std::string_view field) {
    std::string needle;
    needle.reserve(field.size() + 2);
    needle.push_back('"');
    needle.append(field);
    needle.push_back('"');
    std::size_t position = content.find(needle);
    if (position == std::string::npos) {
        return std::nullopt;
    }
    position = content.find(':', position + needle.size());
    if (position == std::string::npos) {
        return std::nullopt;
    }
    ++position;
    while (position < content.size() &&
           std::isspace(static_cast<unsigned char>(content[position])) != 0) {
        ++position;
    }
    if (position >= content.size() || content[position] != '"') {
        return std::nullopt;
    }
    const std::size_t start = ++position;
    while (position < content.size() && content[position] != '"') {
        const unsigned char ch = static_cast<unsigned char>(content[position]);
        if (!(std::isalnum(ch) != 0 || ch == '-' || ch == '_')) {
            return std::nullopt;
        }
        ++position;
    }
    if (position == content.size() || position == start || position - start > 128) {
        return std::nullopt;
    }
    return content.substr(start, position - start);
}

struct DatabaseFileIdentity {
    dev_t device;
    ino_t inode;

    bool operator==(const DatabaseFileIdentity&) const = default;
};

std::optional<DatabaseFileIdentity> database_file_identity(
    const std::filesystem::path& database_path) noexcept {
    struct stat status {};
    if (::stat(database_path.c_str(), &status) != 0 || !S_ISREG(status.st_mode)) {
        return std::nullopt;
    }
    return DatabaseFileIdentity{status.st_dev, status.st_ino};
}

}

class BatchWriterSqliteSession {
public:
    BatchWriterSqliteSession(const std::filesystem::path& database_path, int busy_timeout_ms)
        : db(database_path, busy_timeout_ms),
          begin_transaction(db.prepare_persistent("BEGIN IMMEDIATE")),
          commit_transaction(db.prepare_persistent("COMMIT")),
          rollback_transaction(db.prepare_persistent("ROLLBACK")),
          upsert_session(db.prepare_persistent(
              "INSERT INTO smtp_sessions (id, remote_ip, status, tls_used, connect_at, "
              "first_command_at, last_command_at, last_mail_from, bytes_received, message_count) "
              "VALUES (?, ?, 'closed', 0, ?, ?, ?, ?, ?, 1) "
              "ON CONFLICT(id) DO UPDATE SET "
              "last_command_at = excluded.last_command_at, "
              "last_mail_from = excluded.last_mail_from, "
              "message_count = smtp_sessions.message_count + 1, "
              "bytes_received = smtp_sessions.bytes_received + excluded.bytes_received")),
          insert_message(db.prepare_persistent(
              "INSERT INTO messages (id, smtp_session_id, raw_path, raw_sha256, raw_size_bytes, "
              "envelope_from, from_addr, received_at, indexed_at, parse_status, parse_error, "
              "message_id_header, subject, from_name, reply_to, date_header, "
              "has_text, has_html, has_attachments, attachment_count, "
              "text_preview, text_body_path, html_body_path, headers_json, verification_code) "
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")),
          select_message(db.prepare_persistent("SELECT 1 FROM messages WHERE id = ?")),
          select_mailboxes(db.prepare_persistent(
              "SELECT mailbox.id, mailbox.domain_id, domain.root_domain_ascii, "
              "mailbox.first_seen_at, mailbox.last_seen_at, mailbox.public_enabled, "
              "mailbox.is_hidden, mailbox.notes, mailbox.bulk_delete_generation "
              "FROM mailboxes AS mailbox "
              "JOIN domains AS domain ON domain.id = mailbox.domain_id "
              "WHERE mailbox.address_canonical = ? "
              "ORDER BY mailbox.id ASC")),
          insert_mailbox(db.prepare_persistent(
              "INSERT INTO mailboxes (domain_id, local_part_canonical, rcpt_domain_ascii, "
              "address_canonical, address_display, first_seen_at, last_seen_at, latest_message_at, "
              "message_count) "
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)")),
          update_mailbox_identity(db.prepare_persistent(
              "UPDATE mailboxes SET domain_id = ?, local_part_canonical = ?, "
              "rcpt_domain_ascii = ?, address_canonical = ?, address_display = ? WHERE id = ?")),
          update_and_touch_mailbox_identity(db.prepare_persistent(
              "UPDATE mailboxes SET domain_id = ?, local_part_canonical = ?, "
              "rcpt_domain_ascii = ?, address_canonical = ?, address_display = ?, "
              "last_seen_at = MAX(last_seen_at, ?6), "
              "latest_message_at = MAX(COALESCE(latest_message_at, ?6), ?6), "
              "message_count = message_count + 1 WHERE id = ?7")),
          touch_mailbox(db.prepare_persistent(
              "UPDATE mailboxes SET last_seen_at = MAX(last_seen_at, ?1), "
              "latest_message_at = MAX(COALESCE(latest_message_at, ?1), ?1), "
              "message_count = message_count + 1 WHERE id = ?2")),
          select_duplicate_deliveries(db.prepare_persistent(
              "SELECT target.id, target.status, target.delivered_at, target.expires_at, "
              "target.deleted_at, target.notes, source.status, source.delivered_at, "
              "source.expires_at, source.deleted_at, source.notes "
              "FROM message_deliveries AS source "
              "JOIN message_deliveries AS target ON target.message_id = source.message_id "
              "WHERE source.mailbox_id = ? AND target.mailbox_id = ?")),
          update_merged_delivery(db.prepare_persistent(
              "UPDATE message_deliveries SET status = ?, delivered_at = ?, expires_at = ?, "
              "deleted_at = ?, notes = ?, mailbox_generation = "
              "(SELECT bulk_delete_generation FROM mailboxes WHERE id = ?) WHERE id = ?")),
          delete_duplicate_deliveries(db.prepare_persistent(
              "DELETE FROM message_deliveries WHERE mailbox_id = ? AND message_id IN "
              "(SELECT message_id FROM message_deliveries WHERE mailbox_id = ?)")),
          move_deliveries(db.prepare_persistent(
              "UPDATE message_deliveries SET mailbox_id = ?, mailbox_generation = "
              "(SELECT bulk_delete_generation FROM mailboxes WHERE id = ?) "
              "WHERE mailbox_id = ?")),
          merge_mailbox_metadata(db.prepare_persistent(
              "UPDATE mailboxes SET "
              "first_seen_at = MIN(first_seen_at, ?), "
              "last_seen_at = MAX(last_seen_at, ?), "
              "public_enabled = MIN(public_enabled, ?), "
              "is_hidden = MAX(is_hidden, ?), "
              "notes = COALESCE(notes, ?) "
              "WHERE id = ?")),
          refresh_mailbox_summary(db.prepare_persistent(
              "UPDATE mailboxes SET "
              "message_count = (SELECT COUNT(*) FROM message_deliveries "
              "WHERE mailbox_id = ? AND status = 'active'), "
              "latest_message_at = (SELECT MAX(delivered_at) FROM message_deliveries "
              "WHERE mailbox_id = ? AND status = 'active') "
              "WHERE id = ?")),
          delete_mailbox(db.prepare_persistent("DELETE FROM mailboxes WHERE id = ?")),
          insert_rehome_audit(db.prepare_persistent(
              "INSERT INTO audit_logs (actor_type, actor_ref, action, resource_type, "
              "resource_ref, status, details_json, created_at) "
              "VALUES ('system', 'smtp-ingest', 'mailboxes.rehome', 'mailbox', ?, "
              "'success', ?, ?)")),
          insert_delivery(db.prepare_persistent(
              "INSERT INTO message_deliveries (id, message_id, mailbox_id, rcpt_to, delivered_at, "
              "expires_at, mailbox_generation) VALUES (?, ?, ?, ?, ?, ?, ?)")),
          insert_attachment(db.prepare_persistent(
              "INSERT INTO attachments (id, message_id, part_index, filename, safe_filename, "
              "content_type, content_disposition, content_id, storage_path, sha256, size_bytes, "
              "is_inline, created_at) "
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")),
          upsert_metric(db.prepare_persistent(
              "INSERT INTO mail_metric_buckets "
              "(bucket_ts, received, deliveries, parse_failures) "
              "VALUES (?, ?, ?, ?) "
              "ON CONFLICT(bucket_ts) DO UPDATE SET "
              "received = mail_metric_buckets.received + excluded.received, "
              "deliveries = mail_metric_buckets.deliveries + excluded.deliveries, "
              "parse_failures = mail_metric_buckets.parse_failures + excluded.parse_failures")),
          upsert_rejected_metric(db.prepare_persistent(
              "INSERT INTO mail_metric_buckets (bucket_ts, rejected) VALUES (?, ?) "
              "ON CONFLICT(bucket_ts) DO UPDATE SET "
              "rejected = mail_metric_buckets.rejected + excluded.rejected")),
          select_catch_all_rule(db.prepare_persistent(
              "SELECT id, accept_exact, accept_subdomains, plus_addressing_mode, "
              "local_part_case_sensitive FROM domains "
              "WHERE root_domain_ascii = '*' AND is_active = 1 LIMIT 1")),
          select_managed_rule(db.prepare_persistent(
              "SELECT id, root_domain_ascii, accept_exact, accept_subdomains, "
              "plus_addressing_mode, local_part_case_sensitive "
              "FROM domains WHERE root_domain_ascii = ? AND root_domain_ascii <> '*' "
              "AND is_active = 1")),
          select_domain_identity(db.prepare_persistent(
              "SELECT root_domain_ascii, is_active FROM domains WHERE id = ?")),
          select_domain_retention(
              db.prepare_persistent("SELECT retention_days FROM domains WHERE id = ?")),
          identity_(database_file_identity(database_path)) {
        if (!identity_.has_value() || !matches_database_file(database_path)) {
            throw std::runtime_error(
                "sqlite database file changed while opening persistent writer");
        }
    }

    bool matches_database_file(const std::filesystem::path& database_path) const noexcept {
        int moved = 0;
        const int file_control_rc = sqlite3_file_control(
            db.handle(), "main", SQLITE_FCNTL_HAS_MOVED, &moved);
        if (file_control_rc == SQLITE_OK) {
            return moved == 0;
        }
        if (file_control_rc != SQLITE_NOTFOUND) {
            return false;
        }
        // Non-standard VFS implementations may not support HAS_MOVED. Retain
        // the inode comparison as a portable fallback for those builds.
        const auto current = database_file_identity(database_path);
        return current.has_value() && current == identity_;
    }

    void begin() {
        begin_transaction.step_done();
        begin_transaction.reset();
    }

    void commit() {
        commit_transaction.step_done();
        commit_transaction.reset();
    }

    void rollback_noexcept() noexcept {
        try {
            rollback_transaction.step_done();
            rollback_transaction.reset();
        } catch (...) {
        }
    }

    SqliteDb db;
    Statement begin_transaction;
    Statement commit_transaction;
    Statement rollback_transaction;
    Statement upsert_session;
    Statement insert_message;
    Statement select_message;
    Statement select_mailboxes;
    Statement insert_mailbox;
    Statement update_mailbox_identity;
    Statement update_and_touch_mailbox_identity;
    Statement touch_mailbox;
    Statement select_duplicate_deliveries;
    Statement update_merged_delivery;
    Statement delete_duplicate_deliveries;
    Statement move_deliveries;
    Statement merge_mailbox_metadata;
    Statement refresh_mailbox_summary;
    Statement delete_mailbox;
    Statement insert_rehome_audit;
    Statement insert_delivery;
    Statement insert_attachment;
    Statement upsert_metric;
    Statement upsert_rejected_metric;
    Statement select_catch_all_rule;
    Statement select_managed_rule;
    Statement select_domain_identity;
    Statement select_domain_retention;

private:
    std::optional<DatabaseFileIdentity> identity_;
};

BatchWriter::BatchWriter(std::filesystem::path storage_root,
                         std::filesystem::path database_path,
                         int busy_timeout_ms,
                         bool fsync_storage)
    : storage_root_(std::move(storage_root)),
      database_path_(std::move(database_path)),
      busy_timeout_ms_(busy_timeout_ms),
      fsync_storage_(fsync_storage) {}

BatchWriter::~BatchWriter() = default;

void BatchWriter::release_sqlite_session() const {
    const std::lock_guard sqlite_guard(sqlite_mutex_);
    sqlite_session_.reset();
    sqlite_stats_.connection_active = false;
}

BatchWriterSqliteStats BatchWriter::sqlite_stats() const {
    const std::lock_guard sqlite_guard(sqlite_mutex_);
    return sqlite_stats_;
}

void BatchWriter::write_rejected_metric(const std::string& timestamp,
                                        std::uint64_t count) const {
    if (count == 0) {
        return;
    }
    if (count > static_cast<std::uint64_t>(std::numeric_limits<sqlite3_int64>::max())) {
        throw std::overflow_error("rejected metric count exceeds sqlite integer range");
    }

    const std::lock_guard sqlite_guard(sqlite_mutex_);
    try {
        if (sqlite_session_ != nullptr &&
            !sqlite_session_->matches_database_file(database_path_)) {
            sqlite_session_.reset();
            sqlite_stats_.connection_active = false;
        }
        if (sqlite_session_ == nullptr) {
            sqlite_session_ =
                std::make_unique<BatchWriterSqliteSession>(database_path_, busy_timeout_ms_);
            ++sqlite_stats_.connections_opened;
            ++sqlite_stats_.statement_sets_prepared;
            sqlite_stats_.connection_active = true;
        }

        BatchWriterSqliteSession& session = *sqlite_session_;
        session.begin();
        bind_text(session.upsert_rejected_metric,
                  1,
                  metric_bucket_ts(timestamp),
                  "bind rejected metric bucket");
        bind_int64(session.upsert_rejected_metric,
                   2,
                   static_cast<sqlite3_int64>(count),
                   "bind rejected metric count");
        session.upsert_rejected_metric.step_done();
        session.upsert_rejected_metric.reset();
        session.commit();
    } catch (...) {
        if (sqlite_session_ != nullptr) {
            sqlite_session_->rollback_noexcept();
        }
        sqlite_session_.reset();
        sqlite_stats_.connection_active = false;
        throw;
    }
}

std::filesystem::path BatchWriter::resolve_storage_path(const std::string& relative_path) const {
    std::filesystem::path relative(relative_path);
    if (relative.is_absolute()) {
        throw std::runtime_error("storage path must be relative");
    }
    const auto root = std::filesystem::weakly_canonical(storage_root_);
    const auto target = std::filesystem::weakly_canonical(root / relative);
    if (!path_is_at_or_inside_root(root, target)) {
        throw std::runtime_error("storage path escapes storage root");
    }
    return target;
}

void BatchWriter::write_file_atomic(const std::string& relative_path,
                                    const std::string& content,
                                    bool durable) const {
    const auto target = resolve_storage_path(relative_path);
    ensure_private_directory_chain(storage_root_, target.parent_path());
    auto [part_fd, part] = create_temp_file(target);
    chmod_private(part, false);
    try {
        write_all(part_fd, part, content);
        if (durable && fsync_storage_) {
            if (::fsync(part_fd.get()) != 0) {
                const int fsync_error = errno;
                throw_errno("fsync failed: " + part.string(), fsync_error);
            }
        }
        part_fd.close_or_throw("close failed: " + part.string());
        std::filesystem::rename(part, target);
    } catch (...) {
        std::error_code ec;
        std::filesystem::remove(part, ec);
        throw;
    }
    chmod_private(target, false);
    if (durable && fsync_storage_) {
        fsync_directory_chain_to_filesystem_root(storage_root_, target.parent_path());
    }
}

void BatchWriter::write_parsed_artifacts(const MailJob& job, ParsedMail& parsed) const {
    prepare_parsed_artifact_metadata(job, parsed);
    if (parsed.text_body_path.has_value()) {
        write_file_atomic(*parsed.text_body_path, parsed.text_body);
    }
    if (parsed.html_body_path.has_value()) {
        write_file_atomic(*parsed.html_body_path, parsed.html_body);
    }
    for (ParsedAttachment& attachment : parsed.attachments) {
        write_file_atomic(attachment.storage_path, attachment.content);
    }
}

std::string BatchWriter::build_manifest(const MailJob& job,
                                        const ParsedMail* parsed,
                                        const ParseFailure* failure) const {
    std::ostringstream output;
    output << "{";
    output << "\"message_id\":\"" << json_escape(job.message_id) << "\",";
    output << "\"smtp_session_id\":\"" << json_escape(job.smtp_session_id) << "\",";
    output << "\"remote_ip\":\"" << json_escape(job.remote_ip) << "\",";
    output << "\"envelope_from\":\"" << json_escape(job.envelope_from) << "\",";
    output << "\"received_at\":\"" << json_escape(job.received_at) << "\",";
    output << "\"raw_path\":\"" << json_escape(job.raw_path) << "\",";
    output << "\"raw_sha256\":\"" << json_escape(job.raw_sha256) << "\",";
    output << "\"raw_size_bytes\":" << job.raw_content.size() << ",";
    output << "\"rcpt_tos\":[";
    for (std::size_t i = 0; i < job.recipients.size(); ++i) {
        if (i != 0) {
            output << ",";
        }
        output << "\"" << json_escape(job.recipients[i].rcpt_to) << "\"";
    }
    output << "],\"recipients\":[";
    for (std::size_t i = 0; i < job.recipients.size(); ++i) {
        const auto& recipient = job.recipients[i];
        if (i != 0) {
            output << ",";
        }
        output << "{";
        output << "\"rcpt_to\":\"" << json_escape(recipient.rcpt_to) << "\",";
        output << "\"domain_id\":" << recipient.match.domain_id << ",";
        output << "\"domain_ascii\":\"" << json_escape(recipient.match.domain_ascii) << "\",";
        output << "\"root_domain_ascii\":\"" << json_escape(recipient.match.root_domain_ascii) << "\",";
        output << "\"local_part_canonical\":\""
               << json_escape(recipient.match.local_part_canonical) << "\",";
        output << "\"address_canonical\":\"" << json_escape(recipient.match.address_canonical)
               << "\",";
        if (!recipient.domain_policy.has_value()) {
            throw std::runtime_error("recipient missing domain policy snapshot");
        }
        output << "\"domain_policy\":" << build_domain_policy(*recipient.domain_policy);
        output << "}";
    }
    output << "]";
    if (parsed != nullptr) {
        output << ",\"parsed\":" << parsed_manifest_json(*parsed);
    } else if (failure != nullptr) {
        output << ",\"parsed\":{\"status\":\"failed\",\"parse_error\":\""
               << json_escape(failure->message) << "\"}";
    }
    output << "}";
    return output.str();
}

void BatchWriter::write_storage_artifacts(const std::vector<MailJob>& jobs) const {
    for (const MailJob& job : jobs) {
        write_pending_artifacts(job);
    }
}

void BatchWriter::write_pending_artifacts(const MailJob& job) const {
    // Raw must become visible before its recovery manifest. A crash between the
    // two can leave an unreferenced raw blob, never a manifest that points at a
    // missing raw message.
    const std::string pending_manifest = build_manifest(job, nullptr, nullptr);
    write_file_atomic(job.raw_path, job.raw_content);
    write_file_atomic(job.manifest_path, pending_manifest);
}

MaintenanceState BatchWriter::maintenance_state() const {
    const auto lock_path = storage_root_ / kMaintenanceLockPath;
    std::error_code error;
    const auto status = std::filesystem::symlink_status(lock_path, error);
    if (error) {
        if (error == std::errc::no_such_file_or_directory) {
            return {};
        }
        return {true, std::nullopt};
    }
    if (!std::filesystem::exists(status)) {
        return {};
    }

    const auto content = read_small_text_file(lock_path);
    if (!content.has_value()) {
        return {true, std::nullopt};
    }
    return {true, json_string_field(*content, "token")};
}

bool BatchWriter::maintenance_active() const {
    return maintenance_state().active;
}

std::optional<std::string> BatchWriter::maintenance_token() const {
    return maintenance_state().token;
}

void BatchWriter::publish_ingest_status(
    const std::string& instance_id,
    std::size_t queue_messages,
    std::size_t queue_bytes,
    std::size_t active_connections,
    std::size_t max_connections,
    const std::optional<std::string>& current_maintenance_token) const {
    std::ostringstream output;
    output << "{";
    output << "\"instance_id\":\"" << json_escape(instance_id) << "\",";
    output << "\"pid\":" << static_cast<long long>(::getpid()) << ",";
    output << "\"updated_at\":\"" << json_escape(utc_now()) << "\",";
    output << "\"token\":";
    if (current_maintenance_token.has_value()) {
        output << "\"" << json_escape(*current_maintenance_token) << "\"";
    } else {
        output << "null";
    }
    output << ",\"queue_messages\":" << queue_messages;
    output << ",\"queue_bytes\":" << queue_bytes;
    output << ",\"active_connections\":" << active_connections;
    output << ",\"max_connections\":" << max_connections;
    output << "}";
    write_file_atomic(std::string(kIngestStatusPath), output.str(), false);
}

void BatchWriter::write_maintenance_drained(const std::string& instance_id,
                                            const std::string& current_maintenance_token) const {
    std::ostringstream output;
    output << "{";
    output << "\"instance_id\":\"" << json_escape(instance_id) << "\",";
    output << "\"pid\":" << static_cast<long long>(::getpid()) << ",";
    output << "\"drained_at\":\"" << json_escape(utc_now()) << "\",";
    output << "\"token\":\"" << json_escape(current_maintenance_token) << "\",";
    output << "\"queue_messages\":0,\"queue_bytes\":0";
    output << "}";
    write_file_atomic(std::string(kMaintenanceDrainedPath), output.str(), false);
}

void BatchWriter::remove_ingest_status(const std::string& instance_id) const {
    const auto status_path = storage_root_ / kIngestStatusPath;
    const auto content = read_small_text_file(status_path);
    if (!content.has_value() || json_string_field(*content, "instance_id") != instance_id) {
        return;
    }
    std::error_code error;
    std::filesystem::remove(status_path, error);
}

void BatchWriter::write_quarantine_record(const MailJob& job,
                                          const std::string& error,
                                          int attempts) const {
    std::string relative_path = job.manifest_path;
    constexpr std::string_view manifest_prefix = "manifests/";
    if (relative_path.rfind(manifest_prefix, 0) == 0) {
        relative_path.replace(0, manifest_prefix.size(), "quarantine/");
    } else {
        relative_path = "quarantine/" + job.message_id + ".json";
    }
    const auto extension = relative_path.rfind(".json");
    if (extension != std::string::npos) {
        relative_path.insert(extension, ".error");
    }

    std::ostringstream output;
    output << "{";
    output << "\"message_id\":\"" << json_escape(job.message_id) << "\",";
    output << "\"received_at\":\"" << json_escape(job.received_at) << "\",";
    output << "\"raw_path\":\"" << json_escape(job.raw_path) << "\",";
    output << "\"attempts\":" << attempts << ",";
    output << "\"error\":\"" << json_escape(error) << "\"";
    output << "}";
    write_file_atomic(relative_path, output.str());
}

void BatchWriter::write_storage_artifacts(
    const std::vector<MailJob>& jobs,
    std::vector<std::variant<ParsedMail, ParseFailure>>& parse_results) const {
    if (jobs.size() != parse_results.size()) {
        throw std::runtime_error("batch writer parse result count mismatch");
    }
    for (std::size_t index = 0; index < jobs.size(); ++index) {
        const MailJob& job = jobs[index];
        auto& result = parse_results[index];
        if (ParsedMail* parsed = std::get_if<ParsedMail>(&result)) {
            prepare_parsed_artifact_metadata(job, *parsed);
        }
        if (!job.artifacts_persisted) {
            write_pending_artifacts(job);
        }
        if (ParsedMail* parsed = std::get_if<ParsedMail>(&result)) {
            write_parsed_artifacts(job, *parsed);
        }
        write_file_atomic(job.manifest_path,
                          build_manifest(job, parsed_result(result), failure_result(result)));
    }
}

void BatchWriter::write_sqlite_records(
    const std::vector<MailJob>& jobs,
    const std::vector<std::variant<ParsedMail, ParseFailure>>& parse_results) const {
    if (jobs.empty()) {
        return;
    }
    if (jobs.size() != parse_results.size()) {
        throw std::runtime_error("batch writer parse result count mismatch");
    }

    const std::lock_guard sqlite_guard(sqlite_mutex_);
    try {
        if (sqlite_session_ != nullptr &&
            !sqlite_session_->matches_database_file(database_path_)) {
            sqlite_session_.reset();
            sqlite_stats_.connection_active = false;
        }
        if (sqlite_session_ == nullptr) {
            sqlite_session_ =
                std::make_unique<BatchWriterSqliteSession>(database_path_, busy_timeout_ms_);
            ++sqlite_stats_.connections_opened;
            ++sqlite_stats_.statement_sets_prepared;
            sqlite_stats_.connection_active = true;
        }

        BatchWriterSqliteSession& session = *sqlite_session_;
        SqliteDb& db = session.db;
        session.begin();

        Statement& upsert_session = session.upsert_session;
        Statement& insert_message = session.insert_message;
        Statement& select_message = session.select_message;
        Statement& select_mailboxes = session.select_mailboxes;
        Statement& insert_mailbox = session.insert_mailbox;
        Statement& update_mailbox_identity = session.update_mailbox_identity;
        Statement& update_and_touch_mailbox_identity = session.update_and_touch_mailbox_identity;
        Statement& touch_mailbox = session.touch_mailbox;
        Statement& select_duplicate_deliveries = session.select_duplicate_deliveries;
        Statement& update_merged_delivery = session.update_merged_delivery;
        Statement& delete_duplicate_deliveries = session.delete_duplicate_deliveries;
        Statement& move_deliveries = session.move_deliveries;
        Statement& merge_mailbox_metadata = session.merge_mailbox_metadata;
        Statement& refresh_mailbox_summary = session.refresh_mailbox_summary;
        Statement& delete_mailbox = session.delete_mailbox;
        Statement& insert_rehome_audit = session.insert_rehome_audit;
        Statement& insert_delivery = session.insert_delivery;
        Statement& insert_attachment = session.insert_attachment;
        Statement& upsert_metric = session.upsert_metric;
        Statement& select_catch_all_rule = session.select_catch_all_rule;
        Statement& select_managed_rule = session.select_managed_rule;
        Statement& select_domain_identity = session.select_domain_identity;
        Statement& select_domain_retention = session.select_domain_retention;

        std::optional<DomainMatcher> catch_all_matcher;
        if (select_catch_all_rule.step_row()) {
            catch_all_matcher.emplace(std::vector<DomainRule>{DomainRule{
                .domain_id = sqlite3_column_int(select_catch_all_rule.get(), 0),
                .root_domain_ascii = "*",
                .accept_exact = sqlite3_column_int(select_catch_all_rule.get(), 1) != 0,
                .accept_subdomains = sqlite3_column_int(select_catch_all_rule.get(), 2) != 0,
                .plus_addressing_mode = required_column_text(select_catch_all_rule.get(),
                                                             3,
                                                             "catch-all plus mode"),
                .local_part_case_sensitive =
                    sqlite3_column_int(select_catch_all_rule.get(), 4) != 0,
            }});
        }
        select_catch_all_rule.reset();

        const auto current_domain_match = [&](const RecipientDelivery& recipient) -> DomainMatch {
            const DomainMatch& cached_match = recipient.match;
            bind_int64(select_domain_identity,
                       1,
                       static_cast<sqlite3_int64>(cached_match.domain_id),
                       "bind accepted domain identity");
            const bool identity_exists = select_domain_identity.step_row();
            std::string current_root;
            bool current_active = false;
            if (identity_exists) {
                current_root = required_column_text(select_domain_identity.get(),
                                                    0,
                                                    "accepted domain current root");
                current_active = sqlite3_column_int(select_domain_identity.get(), 1) != 0;
            }
            select_domain_identity.reset();
            if (!identity_exists || current_root != cached_match.root_domain_ascii) {
                throw std::runtime_error("recipient policy conflict after domain rename/delete: " +
                                         recipient.rcpt_to);
            }
            if (!current_active) {
                return cached_match;
            }

            std::vector<DomainRule> candidates;
            std::string_view candidate_root = cached_match.domain_ascii;
            while (!candidate_root.empty()) {
                bind_text(select_managed_rule,
                          1,
                          candidate_root,
                          "bind current managed domain candidate");
                if (select_managed_rule.step_row()) {
                    candidates.push_back(DomainRule{
                        .domain_id = sqlite3_column_int(select_managed_rule.get(), 0),
                        .root_domain_ascii = required_column_text(select_managed_rule.get(),
                                                                 1,
                                                                 "managed domain root"),
                        .accept_exact = sqlite3_column_int(select_managed_rule.get(), 2) != 0,
                        .accept_subdomains = sqlite3_column_int(select_managed_rule.get(), 3) != 0,
                        .plus_addressing_mode = required_column_text(select_managed_rule.get(),
                                                                    4,
                                                                    "managed plus mode"),
                        .local_part_case_sensitive =
                            sqlite3_column_int(select_managed_rule.get(), 5) != 0,
                    });
                }
                select_managed_rule.reset();

                const std::size_t dot = candidate_root.find('.');
                if (dot == std::string_view::npos) {
                    break;
                }
                candidate_root.remove_prefix(dot + 1);
            }
            std::optional<DomainMatch> refreshed;
            if (!candidates.empty()) {
                refreshed = DomainMatcher(std::move(candidates)).match_address(recipient.rcpt_to);
            }
            if (!refreshed.has_value() && catch_all_matcher.has_value()) {
                refreshed = catch_all_matcher->match_address(recipient.rcpt_to);
            }

            if (refreshed.has_value()) {
                if (refreshed->domain_id == cached_match.domain_id) {
                    if (refreshed->root_domain_ascii != cached_match.root_domain_ascii) {
                        throw std::runtime_error("recipient policy conflict after domain rename: " +
                                                 recipient.rcpt_to);
                    }
                    return *refreshed;
                }
                const bool catch_all_promotion =
                    cached_match.root_domain_ascii == "*" &&
                    refreshed->root_domain_ascii != "*";
                const bool managed_suffix_promotion =
                    cached_match.root_domain_ascii != "*" &&
                    refreshed->root_domain_ascii != "*" &&
                    refreshed->root_domain_ascii.ends_with("." +
                                                            cached_match.root_domain_ascii);
                if (catch_all_promotion || managed_suffix_promotion) {
                    return *refreshed;
                }
            }

            // Durable ACK may already have been returned before an operator
            // disables the exact same domain. Completing that in-flight job is
            // safe because ownership did not change. Rename/delete, and any
            // fallback to another tenant, remain fail-closed.
            // The accepted domain identity is unchanged. A concurrent flag,
            // size, or other same-tenant policy edit must not turn an already
            // durable-ACKed job into a permanent poison receipt: finish it
            // with the RCPT snapshot instead of redirecting to a fallback.
            return cached_match;
        };

        const auto merge_mailbox_into = [&](sqlite3_int64 target_id,
                                            const MailboxRecord& source) -> MailboxMergeStats {
            if (target_id == source.id) {
                return {};
            }

            bind_int64(select_duplicate_deliveries,
                       1,
                       source.id,
                       "bind duplicate delivery source mailbox");
            bind_int64(select_duplicate_deliveries,
                       2,
                       target_id,
                       "bind duplicate delivery target mailbox");
            std::vector<DeliveryMergeUpdate> delivery_updates;
            while (select_duplicate_deliveries.step_row()) {
                const std::string target_status = required_column_text(
                    select_duplicate_deliveries.get(), 1, "target delivery status");
                const std::string source_status = required_column_text(
                    select_duplicate_deliveries.get(), 6, "source delivery status");
                const std::string merged_status =
                    delivery_status_rank(source_status) > delivery_status_rank(target_status)
                        ? source_status
                        : target_status;
                const std::string target_delivered_at = required_column_text(
                    select_duplicate_deliveries.get(), 2, "target delivery timestamp");
                const std::string source_delivered_at = required_column_text(
                    select_duplicate_deliveries.get(), 7, "source delivery timestamp");
                const auto target_expires_at =
                    optional_column_text(select_duplicate_deliveries.get(), 3);
                const auto source_expires_at =
                    optional_column_text(select_duplicate_deliveries.get(), 8);
                std::optional<std::string> merged_expires_at;
                if (target_expires_at.has_value() && source_expires_at.has_value()) {
                    merged_expires_at =
                        std::max(*target_expires_at, *source_expires_at);
                }

                std::optional<std::string> merged_deleted_at;
                if (merged_status == "deleted") {
                    const auto target_deleted_at =
                        optional_column_text(select_duplicate_deliveries.get(), 4);
                    const auto source_deleted_at =
                        optional_column_text(select_duplicate_deliveries.get(), 9);
                    if (target_deleted_at.has_value() && source_deleted_at.has_value()) {
                        merged_deleted_at = std::min(*target_deleted_at, *source_deleted_at);
                    } else if (target_deleted_at.has_value()) {
                        merged_deleted_at = target_deleted_at;
                    } else {
                        merged_deleted_at = source_deleted_at;
                    }
                }
                auto merged_notes = optional_column_text(select_duplicate_deliveries.get(), 5);
                if (!merged_notes.has_value()) {
                    merged_notes = optional_column_text(select_duplicate_deliveries.get(), 10);
                }
                delivery_updates.push_back(DeliveryMergeUpdate{
                    .id = required_column_text(select_duplicate_deliveries.get(),
                                               0,
                                               "target delivery id"),
                    .status = merged_status,
                    .delivered_at = std::min(target_delivered_at, source_delivered_at),
                    .expires_at = std::move(merged_expires_at),
                    .deleted_at = std::move(merged_deleted_at),
                    .notes = std::move(merged_notes),
                });
            }
            select_duplicate_deliveries.reset();

            for (const DeliveryMergeUpdate& update : delivery_updates) {
                bind_text(update_merged_delivery, 1, update.status, "bind merged delivery status");
                bind_text(update_merged_delivery,
                          2,
                          update.delivered_at,
                          "bind merged delivery timestamp");
                bind_optional_text(update_merged_delivery,
                                   3,
                                   update.expires_at,
                                   "bind merged delivery expiry");
                bind_optional_text(update_merged_delivery,
                                   4,
                                   update.deleted_at,
                                   "bind merged delivery deletion timestamp");
                bind_optional_text(update_merged_delivery,
                                   5,
                                   update.notes,
                                   "bind merged delivery notes");
                bind_int64(update_merged_delivery,
                           6,
                           target_id,
                           "bind merged delivery mailbox generation");
                bind_text(update_merged_delivery, 7, update.id, "bind merged delivery id");
                update_merged_delivery.step_done();
                update_merged_delivery.reset();
            }

            bind_int64(delete_duplicate_deliveries,
                       1,
                       source.id,
                       "bind duplicate source mailbox delete");
            bind_int64(delete_duplicate_deliveries,
                       2,
                       target_id,
                       "bind duplicate target mailbox delete");
            delete_duplicate_deliveries.step_done();
            delete_duplicate_deliveries.reset();

            bind_int64(move_deliveries, 1, target_id, "bind delivery target mailbox");
            bind_int64(move_deliveries,
                       2,
                       target_id,
                       "bind delivery target mailbox generation");
            bind_int64(move_deliveries, 3, source.id, "bind delivery source mailbox");
            move_deliveries.step_done();
            const sqlite3_int64 moved = sqlite3_changes(db.handle());
            move_deliveries.reset();

            bind_text(merge_mailbox_metadata,
                      1,
                      source.first_seen_at,
                      "bind source mailbox first seen");
            bind_text(merge_mailbox_metadata,
                      2,
                      source.last_seen_at,
                      "bind source mailbox last seen");
            bind_int64(merge_mailbox_metadata,
                       3,
                       source.public_enabled,
                       "bind source mailbox public flag");
            bind_int64(merge_mailbox_metadata,
                       4,
                       source.is_hidden,
                       "bind source mailbox hidden flag");
            bind_optional_text(merge_mailbox_metadata,
                               5,
                               source.notes,
                               "bind source mailbox notes");
            bind_int64(merge_mailbox_metadata, 6, target_id, "bind mailbox merge target");
            merge_mailbox_metadata.step_done();
            merge_mailbox_metadata.reset();

            bind_int64(delete_mailbox, 1, source.id, "bind merged source mailbox delete");
            delete_mailbox.step_done();
            delete_mailbox.reset();
            return MailboxMergeStats{
                .moved = moved,
                .deduplicated = static_cast<sqlite3_int64>(delivery_updates.size()),
            };
        };

        std::unordered_map<std::string, MetricBucketDelta> metric_deltas;
        metric_deltas.reserve(jobs.size());

        for (std::size_t job_index = 0; job_index < jobs.size(); ++job_index) {
            const MailJob& job = jobs[job_index];
            const auto& parse_result = parse_results[job_index];
            const ParsedMail* parsed = parsed_result(parse_result);
            const ParseFailure* failure = failure_result(parse_result);
            const auto raw_size = static_cast<sqlite3_int64>(job.raw_content.size());

            bind_text(select_message, 1, job.message_id, "bind existing message id");
            const bool message_exists = select_message.step_row();
            select_message.reset();
            if (message_exists) {
                continue;
            }

            bind_text(upsert_session, 1, job.smtp_session_id, "bind smtp session id");
            bind_text(upsert_session, 2, job.remote_ip, "bind smtp remote ip");
            bind_text(upsert_session, 3, job.received_at, "bind smtp connect time");
            bind_text(upsert_session, 4, job.received_at, "bind smtp first command time");
            bind_text(upsert_session, 5, job.received_at, "bind smtp last command time");
            bind_text(upsert_session, 6, job.envelope_from, "bind smtp last mail from");
            bind_int64(upsert_session, 7, raw_size, "bind smtp bytes received");
            upsert_session.step_done();
            upsert_session.reset();

            bind_text(insert_message, 1, job.message_id, "bind message id");
            bind_text(insert_message, 2, job.smtp_session_id, "bind message smtp session id");
            bind_text(insert_message, 3, job.raw_path, "bind message raw path");
            bind_text(insert_message, 4, job.raw_sha256, "bind message raw sha256");
            bind_int64(insert_message, 5, raw_size, "bind message raw size");
            bind_text(insert_message, 6, job.envelope_from, "bind message envelope from");
            bind_optional_text(insert_message,
                               7,
                               parsed == nullptr ? std::nullopt : parsed->from_addr,
                               "bind message from addr");
            bind_text(insert_message, 8, job.received_at, "bind message received at");
            bind_text(insert_message, 9, job.received_at, "bind message indexed at");
            bind_text(insert_message,
                      10,
                      parsed == nullptr ? "failed" : "parsed",
                      "bind message parse status");
            if (failure == nullptr) {
                bind_null(insert_message, 11, "bind message parse error");
            } else {
                bind_text(insert_message, 11, failure->message, "bind message parse error");
            }
            bind_optional_text(insert_message,
                               12,
                               parsed == nullptr ? std::nullopt : parsed->message_id_header,
                               "bind message id header");
            bind_optional_text(insert_message,
                               13,
                               parsed == nullptr ? std::nullopt : parsed->subject,
                               "bind message subject");
            bind_optional_text(insert_message,
                               14,
                               parsed == nullptr ? std::nullopt : parsed->from_name,
                               "bind message from name");
            bind_optional_text(insert_message,
                               15,
                               parsed == nullptr ? std::nullopt : parsed->reply_to,
                               "bind message reply to");
            bind_optional_text(insert_message,
                               16,
                               parsed == nullptr ? std::nullopt : parsed->date_header,
                               "bind message date header");
            bind_int64(insert_message,
                       17,
                       parsed != nullptr && parsed->has_text ? 1 : 0,
                       "bind message has text");
            bind_int64(insert_message,
                       18,
                       parsed != nullptr && parsed->has_html ? 1 : 0,
                       "bind message has html");
            bind_int64(insert_message,
                       19,
                       parsed != nullptr && parsed->has_attachments ? 1 : 0,
                       "bind message has attachments");
            bind_int64(insert_message,
                       20,
                       parsed == nullptr ? 0 : parsed->attachment_count,
                       "bind message attachment count");
            bind_optional_text(insert_message,
                               21,
                               parsed == nullptr ? std::nullopt : parsed->text_preview,
                               "bind message text preview");
            bind_optional_text(insert_message,
                               22,
                               parsed == nullptr ? std::nullopt : parsed->text_body_path,
                               "bind message text body path");
            bind_optional_text(insert_message,
                               23,
                               parsed == nullptr ? std::nullopt : parsed->html_body_path,
                               "bind message html body path");
            if (parsed == nullptr) {
                bind_null(insert_message, 24, "bind message headers json");
            } else {
                bind_text(insert_message, 24, parsed->headers_json, "bind message headers json");
            }
            bind_optional_text(insert_message,
                               25,
                               parsed == nullptr ? std::nullopt : parsed->verification_code,
                               "bind message verification code");
            insert_message.step_done();
            insert_message.reset();

            if (parsed != nullptr) {
                for (const ParsedAttachment& attachment : parsed->attachments) {
                    bind_text(insert_attachment,
                              1,
                              attachment.attachment_id,
                              "bind attachment id");
                    bind_text(insert_attachment, 2, job.message_id, "bind attachment message id");
                    bind_int64(insert_attachment,
                               3,
                               attachment.part_index,
                               "bind attachment part index");
                    bind_optional_text(insert_attachment,
                                       4,
                                       attachment.filename,
                                       "bind attachment filename");
                    bind_text(insert_attachment,
                              5,
                              attachment.safe_filename,
                              "bind attachment safe filename");
                    bind_text(insert_attachment,
                              6,
                              attachment.content_type,
                              "bind attachment content type");
                    bind_optional_text(insert_attachment,
                                       7,
                                       attachment.content_disposition,
                                       "bind attachment content disposition");
                    bind_optional_text(insert_attachment,
                                       8,
                                       attachment.content_id,
                                       "bind attachment content id");
                    bind_text(insert_attachment,
                              9,
                              attachment.storage_path,
                              "bind attachment storage path");
                    bind_text(insert_attachment, 10, attachment.sha256, "bind attachment sha256");
                    bind_int64(insert_attachment,
                               11,
                               static_cast<sqlite3_int64>(attachment.content.size()),
                               "bind attachment size");
                    bind_int64(insert_attachment,
                               12,
                               attachment.is_inline ? 1 : 0,
                               "bind attachment inline flag");
                    bind_text(insert_attachment, 13, job.received_at, "bind attachment created at");
                    insert_attachment.step_done();
                    insert_attachment.reset();
                }
            }

            std::unordered_set<std::string> delivered_mailbox_keys;
            delivered_mailbox_keys.reserve(job.recipients.size());
            sqlite3_int64 persisted_deliveries = 0;
            for (const RecipientDelivery& recipient : job.recipients) {
                const DomainMatch match = current_domain_match(recipient);
                std::string mailbox_key = std::to_string(match.domain_id);
                mailbox_key.push_back('\0');
                mailbox_key.append(match.address_canonical);
                if (!delivered_mailbox_keys.insert(std::move(mailbox_key)).second) {
                    continue;
                }
                const bool matched_managed_domain = match.root_domain_ascii != "*";
                std::vector<MailboxRecord> exact_mailboxes =
                    load_mailboxes_by_address(select_mailboxes, match.address_canonical);

                std::optional<MailboxRecord> target_mailbox;
                if (matched_managed_domain) {
                    for (const MailboxRecord& mailbox : exact_mailboxes) {
                        if (mailbox.domain_id == match.domain_id) {
                            target_mailbox = mailbox;
                            break;
                        }
                    }
                    if (!target_mailbox.has_value()) {
                        for (const MailboxRecord& mailbox : exact_mailboxes) {
                            if (!mailbox.is_catch_all) {
                                target_mailbox = mailbox;
                                break;
                            }
                        }
                    }
                } else {
                    // A stale catch-all routing snapshot must never steal a mailbox
                    // that has already been assigned to a managed domain.
                    for (const MailboxRecord& mailbox : exact_mailboxes) {
                        if (!mailbox.is_catch_all) {
                            target_mailbox = mailbox;
                            break;
                        }
                    }
                    if (!target_mailbox.has_value()) {
                        for (const MailboxRecord& mailbox : exact_mailboxes) {
                            if (mailbox.domain_id == match.domain_id) {
                                target_mailbox = mailbox;
                                break;
                            }
                        }
                    }
                }
                if (!target_mailbox.has_value() && !exact_mailboxes.empty()) {
                    target_mailbox = exact_mailboxes.front();
                }

                std::vector<MailboxRecord> catch_all_sources;
                const auto append_catch_all_source = [&](const MailboxRecord& mailbox) {
                    if (!mailbox.is_catch_all ||
                        (target_mailbox.has_value() && mailbox.id == target_mailbox->id)) {
                        return;
                    }
                    for (const MailboxRecord& source : catch_all_sources) {
                        if (source.id == mailbox.id) {
                            return;
                        }
                    }
                    catch_all_sources.push_back(mailbox);
                };
                for (const MailboxRecord& mailbox : exact_mailboxes) {
                    append_catch_all_source(mailbox);
                }

                std::optional<std::string> old_catch_all_address;
                if (matched_managed_domain) {
                    if (recipient.match.root_domain_ascii == "*") {
                        old_catch_all_address = recipient.match.address_canonical;
                    } else if (catch_all_matcher.has_value()) {
                        const auto old_match =
                            catch_all_matcher->match_address(recipient.rcpt_to);
                        if (old_match.has_value()) {
                            old_catch_all_address = old_match->address_canonical;
                        }
                    }
                }
                if (old_catch_all_address.has_value() &&
                    *old_catch_all_address != match.address_canonical) {
                    for (const MailboxRecord& mailbox :
                         load_mailboxes_by_address(select_mailboxes, *old_catch_all_address)) {
                        append_catch_all_source(mailbox);
                    }
                }

                if (!target_mailbox.has_value() && !catch_all_sources.empty()) {
                    target_mailbox = catch_all_sources.front();
                    catch_all_sources.erase(catch_all_sources.begin());
                }

                sqlite3_int64 mailbox_id = 0;
                sqlite3_int64 mailbox_generation = 0;
                bool mailbox_rehomed = false;
                sqlite3_int64 audit_source_mailbox_id = 0;
                sqlite3_int64 deliveries_moved = 0;
                sqlite3_int64 deliveries_deduplicated = 0;
                sqlite3_int64 source_mailboxes_merged = 0;
                bool mailbox_touched = false;
                if (!target_mailbox.has_value()) {
                    bind_int64(insert_mailbox,
                               1,
                               static_cast<sqlite3_int64>(match.domain_id),
                               "bind mailbox domain id");
                    bind_text(insert_mailbox,
                              2,
                              match.local_part_canonical,
                              "bind mailbox local part");
                    bind_text(insert_mailbox,
                              3,
                              match.domain_ascii,
                              "bind mailbox domain ascii");
                    bind_text(insert_mailbox,
                              4,
                              match.address_canonical,
                              "bind mailbox address canonical");
                    bind_text(insert_mailbox,
                              5,
                              match.address_canonical,
                              "bind mailbox address display");
                    bind_text(insert_mailbox,
                              6,
                              job.received_at,
                              "bind mailbox first seen at");
                    bind_text(insert_mailbox,
                              7,
                              job.received_at,
                              "bind mailbox last seen at");
                    bind_text(insert_mailbox,
                              8,
                              job.received_at,
                              "bind mailbox latest message at");
                    insert_mailbox.step_done();
                    insert_mailbox.reset();
                    mailbox_id = sqlite3_last_insert_rowid(db.handle());
                    mailbox_touched = true;
                } else {
                    mailbox_id = target_mailbox->id;
                    mailbox_generation = target_mailbox->bulk_delete_generation;
                    const bool same_domain = target_mailbox->domain_id == match.domain_id;
                    const bool promotes_to_current_managed = matched_managed_domain;
                    if (same_domain || promotes_to_current_managed) {
                        if (!same_domain) {
                            mailbox_rehomed = true;
                            audit_source_mailbox_id = target_mailbox->id;
                        }
                        if (same_domain && catch_all_sources.empty()) {
                            bind_int64(update_and_touch_mailbox_identity,
                                       1,
                                       static_cast<sqlite3_int64>(match.domain_id),
                                       "bind mailbox current domain id");
                            bind_text(update_and_touch_mailbox_identity,
                                      2,
                                      match.local_part_canonical,
                                      "bind mailbox current local part");
                            bind_text(update_and_touch_mailbox_identity,
                                      3,
                                      match.domain_ascii,
                                      "bind mailbox current domain part");
                            bind_text(update_and_touch_mailbox_identity,
                                      4,
                                      match.address_canonical,
                                      "bind mailbox current canonical address");
                            bind_text(update_and_touch_mailbox_identity,
                                      5,
                                      match.address_canonical,
                                      "bind mailbox current display address");
                            bind_text(update_and_touch_mailbox_identity,
                                      6,
                                      job.received_at,
                                      "bind mailbox current last seen");
                            bind_int64(update_and_touch_mailbox_identity,
                                       7,
                                       mailbox_id,
                                       "bind current mailbox row id");
                            update_and_touch_mailbox_identity.step_done();
                            update_and_touch_mailbox_identity.reset();
                            mailbox_touched = true;
                        } else {
                            bind_int64(update_mailbox_identity,
                                       1,
                                       static_cast<sqlite3_int64>(match.domain_id),
                                       "bind mailbox upgraded domain id");
                            bind_text(update_mailbox_identity,
                                      2,
                                      match.local_part_canonical,
                                      "bind mailbox upgraded local part");
                            bind_text(update_mailbox_identity,
                                      3,
                                      match.domain_ascii,
                                      "bind mailbox upgraded domain part");
                            bind_text(update_mailbox_identity,
                                      4,
                                      match.address_canonical,
                                      "bind mailbox upgraded canonical address");
                            bind_text(update_mailbox_identity,
                                      5,
                                      match.address_canonical,
                                      "bind mailbox upgraded display address");
                            bind_int64(update_mailbox_identity,
                                       6,
                                       mailbox_id,
                                       "bind mailbox identity row id");
                            update_mailbox_identity.step_done();
                            update_mailbox_identity.reset();
                        }
                    }
                }

                for (const MailboxRecord& source : catch_all_sources) {
                    const MailboxMergeStats merge = merge_mailbox_into(mailbox_id, source);
                    mailbox_rehomed = true;
                    if (audit_source_mailbox_id == 0) {
                        audit_source_mailbox_id = source.id;
                    }
                    deliveries_moved += merge.moved;
                    deliveries_deduplicated += merge.deduplicated;
                    ++source_mailboxes_merged;
                }

                if (mailbox_rehomed) {
                    bind_int64(refresh_mailbox_summary,
                               1,
                               mailbox_id,
                               "bind summary count mailbox");
                    bind_int64(refresh_mailbox_summary,
                               2,
                               mailbox_id,
                               "bind summary latest mailbox");
                    bind_int64(refresh_mailbox_summary,
                               3,
                               mailbox_id,
                               "bind summary target mailbox");
                    refresh_mailbox_summary.step_done();
                    refresh_mailbox_summary.reset();

                    std::ostringstream details;
                    details << "{";
                    details << "\"source_mailbox_id\":" << audit_source_mailbox_id << ",";
                    details << "\"destination_mailbox_id\":" << mailbox_id << ",";
                    details << "\"destination_domain_id\":" << match.domain_id << ",";
                    details << "\"source_mailboxes_merged\":" << source_mailboxes_merged
                            << ",";
                    details << "\"deliveries_moved\":" << deliveries_moved << ",";
                    details << "\"deliveries_deduplicated\":"
                            << deliveries_deduplicated << ",";
                    details << "\"reason\":\"smtp.write\"";
                    details << "}";
                    bind_text(insert_rehome_audit,
                              1,
                              std::to_string(mailbox_id),
                              "bind rehome audit mailbox");
                    bind_text(insert_rehome_audit,
                              2,
                              details.str(),
                              "bind rehome audit details");
                    bind_text(insert_rehome_audit,
                              3,
                              job.received_at,
                              "bind rehome audit timestamp");
                    insert_rehome_audit.step_done();
                    insert_rehome_audit.reset();
                }

                if (!mailbox_touched) {
                    bind_text(touch_mailbox, 1, job.received_at, "bind mailbox last seen at");
                    bind_int64(touch_mailbox, 2, mailbox_id, "bind touched mailbox id");
                    touch_mailbox.step_done();
                    touch_mailbox.reset();
                }

                bind_text(insert_delivery, 1, recipient.delivery_id, "bind delivery id");
                bind_text(insert_delivery, 2, job.message_id, "bind delivery message id");
                bind_int64(insert_delivery, 3, mailbox_id, "bind delivery mailbox id");
                bind_text(insert_delivery, 4, recipient.rcpt_to, "bind delivery rcpt to");
                bind_text(insert_delivery, 5, job.received_at, "bind delivery delivered at");
                std::optional<std::string> expires_at;
                std::optional<int> retention_days;
                if (match.domain_id == recipient.match.domain_id) {
                    if (recipient.domain_policy.has_value()) {
                        retention_days = recipient.domain_policy->retention_days;
                    }
                } else {
                    bind_int64(select_domain_retention,
                               1,
                               static_cast<sqlite3_int64>(match.domain_id),
                               "bind refreshed domain retention lookup");
                    if (select_domain_retention.step_row() &&
                        sqlite3_column_type(select_domain_retention.get(), 0) != SQLITE_NULL) {
                        retention_days = sqlite3_column_int(select_domain_retention.get(), 0);
                    }
                    select_domain_retention.reset();
                }
                if (retention_days.has_value() && *retention_days > 0) {
                    expires_at = utc_add_days(job.received_at, *retention_days);
                }
                bind_optional_text(insert_delivery, 6, expires_at, "bind delivery expires at");
                bind_int64(insert_delivery,
                           7,
                           mailbox_generation,
                           "bind delivery mailbox generation");
                insert_delivery.step_done();
                insert_delivery.reset();
                ++persisted_deliveries;
            }

            MetricBucketDelta& metric = metric_deltas[metric_bucket_ts(job.received_at)];
            ++metric.received;
            metric.deliveries += persisted_deliveries;
            metric.parse_failures += failure == nullptr ? 0 : 1;
        }

        for (const auto& [bucket_ts, metric] : metric_deltas) {
            bind_text(upsert_metric, 1, bucket_ts, "bind metric bucket");
            bind_int64(upsert_metric, 2, metric.received, "bind metric received");
            bind_int64(upsert_metric, 3, metric.deliveries, "bind metric deliveries");
            bind_int64(upsert_metric,
                       4,
                       metric.parse_failures,
                       "bind metric parse failures");
            upsert_metric.step_done();
            upsert_metric.reset();
        }

        session.commit();
    } catch (...) {
        if (sqlite_session_ != nullptr) {
            sqlite_session_->rollback_noexcept();
        }
        // A failed step can leave one or more statements in an error state.
        // Discard the complete session so the caller's retry starts from a
        // freshly opened connection and freshly compiled statement set.
        sqlite_session_.reset();
        sqlite_stats_.connection_active = false;
        throw;
    }
}

void BatchWriter::write_batch(const std::vector<MailJob>& jobs) const {
    std::vector<std::variant<ParsedMail, ParseFailure>> parse_results;
    parse_results.reserve(jobs.size());
    for (const MailJob& job : jobs) {
        try {
            ParsedMail parsed = MimeParser().parse(job.raw_content);
            parsed.verification_code = extract_verification_code(parsed.subject.value_or(""),
                                                                 parsed.from_addr.value_or(""),
                                                                 parsed.text_body,
                                                                 parsed.html_body,
                                                                 parsed.text_preview.value_or(""));
            parse_results.emplace_back(std::move(parsed));
        } catch (const ParseFailure& failure) {
            parse_results.emplace_back(failure);
        }
    }
    write_storage_artifacts(jobs, parse_results);
    write_sqlite_records(jobs, parse_results);
}

}
