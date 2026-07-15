#include "sqlite_db.h"

#include <sqlite3.h>

#include <cctype>
#include <stdexcept>
#include <string>
#include <utility>

namespace rapid_inbox::ingestd {
namespace {

std::string sqlite_message(sqlite3* db, int rc) {
    if (db != nullptr) {
        return sqlite3_errmsg(db);
    }
    return sqlite3_errstr(rc);
}

std::runtime_error sqlite_error(sqlite3* db, int rc, const std::string& context) {
    return std::runtime_error(context + ": " + sqlite_message(db, rc));
}

void throw_if_null_statement(sqlite3_stmt* stmt, const std::string& operation) {
    if (stmt == nullptr) {
        throw std::runtime_error("sqlite statement is null during " + operation);
    }
}

std::string sqlite_result_name(int rc) {
    if (rc == SQLITE_DONE) {
        return "SQLITE_DONE";
    }
    if (rc == SQLITE_ROW) {
        return "SQLITE_ROW";
    }
    return std::to_string(rc);
}

std::string ascii_lower(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return value;
}

void close_db_handle(sqlite3*& db) noexcept {
    if (db == nullptr) {
        return;
    }

    // Statements may intentionally outlive the SqliteDb wrapper. close_v2
    // marks the handle for closing once the last prepared statement finalizes.
    (void)sqlite3_close_v2(db);
    db = nullptr;
}

void enable_and_verify_wal(sqlite3* db) {
    sqlite3_stmt* statement = nullptr;
    constexpr const char* sql = "PRAGMA journal_mode = WAL";
    int rc = sqlite3_prepare_v2(db, sql, -1, &statement, nullptr);
    if (rc != SQLITE_OK) {
        throw sqlite_error(db, rc, "sqlite prepare failed while enabling WAL");
    }

    rc = sqlite3_step(statement);
    if (rc != SQLITE_ROW) {
        const int finalize_rc = sqlite3_finalize(statement);
        if (finalize_rc != SQLITE_OK) {
            throw sqlite_error(db, finalize_rc, "sqlite finalize failed after WAL enable failure");
        }
        throw sqlite_error(db, rc, "sqlite WAL mode failed without a journal_mode row");
    }

    const unsigned char* raw_mode = sqlite3_column_text(statement, 0);
    const std::string journal_mode =
        raw_mode == nullptr ? "" : reinterpret_cast<const char*>(raw_mode);

    rc = sqlite3_finalize(statement);
    if (rc != SQLITE_OK) {
        throw sqlite_error(db, rc, "sqlite finalize failed while enabling WAL");
    }

    if (ascii_lower(journal_mode) != "wal") {
        throw std::runtime_error("sqlite WAL mode failed: requested WAL but SQLite returned [" +
                                 journal_mode + "]");
    }
}

}  // namespace

Statement::Statement(sqlite3* db, sqlite3_stmt* statement, std::string sql)
    : db_(db), stmt_(statement), sql_(std::move(sql)) {}

Statement::~Statement() {
    if (stmt_ != nullptr) {
        (void)sqlite3_finalize(stmt_);
    }
}

Statement::Statement(Statement&& other) noexcept
    : db_(std::exchange(other.db_, nullptr)),
      stmt_(std::exchange(other.stmt_, nullptr)),
      sql_(std::move(other.sql_)) {}

Statement& Statement::operator=(Statement&& other) noexcept {
    if (this != &other) {
        if (stmt_ != nullptr) {
            (void)sqlite3_finalize(stmt_);
        }
        db_ = std::exchange(other.db_, nullptr);
        stmt_ = std::exchange(other.stmt_, nullptr);
        sql_ = std::move(other.sql_);
    }
    return *this;
}

sqlite3_stmt* Statement::get() const noexcept {
    return stmt_;
}

bool Statement::step_row() {
    throw_if_null_statement(stmt_, "step_row");
    const int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
        return true;
    }
    if (rc == SQLITE_DONE) {
        return false;
    }
    throw sqlite_error(db_, rc, "sqlite step_row failed for SQL [" + sql_ + "]");
}

void Statement::step_done() {
    throw_if_null_statement(stmt_, "step_done");
    const int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_DONE) {
        return;
    }
    throw sqlite_error(db_,
                       rc,
                       "sqlite step_done expected SQLITE_DONE for SQL [" + sql_ +
                           "] but sqlite3_step returned " + sqlite_result_name(rc));
}

void Statement::reset() {
    throw_if_null_statement(stmt_, "reset");
    const int rc = sqlite3_reset(stmt_);
    if (rc != SQLITE_OK) {
        throw sqlite_error(db_, rc, "sqlite reset failed for SQL [" + sql_ + "]");
    }
    const int clear_rc = sqlite3_clear_bindings(stmt_);
    if (clear_rc != SQLITE_OK) {
        throw sqlite_error(db_, clear_rc, "sqlite clear bindings failed for SQL [" + sql_ + "]");
    }
}

SqliteDb::SqliteDb(const std::filesystem::path& database_path, int busy_timeout_ms)
    : db_(nullptr) {
    const std::string database_name = database_path.string();
    const int rc = sqlite3_open_v2(database_name.c_str(),
                                   &db_,
                                   SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                                   nullptr);
    if (rc != SQLITE_OK) {
        const std::string message = sqlite_message(db_, rc);
        if (db_ != nullptr) {
            close_db_handle(db_);
        }
        throw std::runtime_error("sqlite open failed for [" + database_name + "]: " + message);
    }

    try {
        (void)sqlite3_busy_timeout(db_, busy_timeout_ms);
        exec("PRAGMA busy_timeout = " + std::to_string(busy_timeout_ms));
        enable_and_verify_wal(db_);
        exec("PRAGMA foreign_keys = ON");
        exec("PRAGMA synchronous = FULL");
    } catch (...) {
        close_db_handle(db_);
        throw;
    }
}

SqliteDb::~SqliteDb() {
    close_db_handle(db_);
}

SqliteDb::SqliteDb(SqliteDb&& other) noexcept : db_(std::exchange(other.db_, nullptr)) {}

SqliteDb& SqliteDb::operator=(SqliteDb&& other) noexcept {
    if (this != &other) {
        close_db_handle(db_);
        db_ = std::exchange(other.db_, nullptr);
    }
    return *this;
}

sqlite3* SqliteDb::handle() const noexcept {
    return db_;
}

void SqliteDb::exec(const std::string& sql) {
    char* raw_error = nullptr;
    const int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &raw_error);
    if (rc == SQLITE_OK) {
        return;
    }

    const std::string db_message = sqlite_message(db_, rc);
    std::string message = "sqlite exec failed for SQL [" + sql + "]: " + db_message;
    if (raw_error != nullptr) {
        const std::string exec_message = raw_error;
        sqlite3_free(raw_error);
        if (exec_message != db_message) {
            message += " (" + exec_message + ")";
        }
    }
    throw std::runtime_error(message);
}

Statement SqliteDb::prepare(const std::string& sql) {
    sqlite3_stmt* statement = nullptr;
    const int rc = sqlite3_prepare_v2(db_, sql.c_str(), -1, &statement, nullptr);
    if (rc != SQLITE_OK) {
        throw sqlite_error(db_, rc, "sqlite prepare failed for SQL [" + sql + "]");
    }
    if (statement == nullptr) {
        throw std::runtime_error("sqlite prepare produced no statement for SQL [" + sql + "]");
    }
    return Statement(db_, statement, sql);
}

Statement SqliteDb::prepare_persistent(const std::string& sql) {
    sqlite3_stmt* statement = nullptr;
    const int rc = sqlite3_prepare_v3(
        db_, sql.c_str(), -1, SQLITE_PREPARE_PERSISTENT, &statement, nullptr);
    if (rc != SQLITE_OK) {
        throw sqlite_error(db_, rc, "sqlite persistent prepare failed for SQL [" + sql + "]");
    }
    if (statement == nullptr) {
        throw std::runtime_error("sqlite persistent prepare produced no statement for SQL [" +
                                 sql + "]");
    }
    return Statement(db_, statement, sql);
}

}  // namespace rapid_inbox::ingestd
