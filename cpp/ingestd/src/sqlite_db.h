#pragma once

#include <filesystem>
#include <string>

struct sqlite3;
struct sqlite3_stmt;

namespace rapid_inbox::ingestd {

class Statement {
public:
    ~Statement();

    Statement(const Statement&) = delete;
    Statement& operator=(const Statement&) = delete;

    Statement(Statement&& other) noexcept;
    Statement& operator=(Statement&& other) noexcept;

    sqlite3_stmt* get() const noexcept;
    bool step_row();
    void step_done();
    void reset();

private:
    friend class SqliteDb;

    Statement(sqlite3* db, sqlite3_stmt* statement, std::string sql);

    sqlite3* db_;
    sqlite3_stmt* stmt_;
    std::string sql_;
};

class SqliteDb {
public:
    SqliteDb(const std::filesystem::path& database_path, int busy_timeout_ms);
    ~SqliteDb();

    SqliteDb(const SqliteDb&) = delete;
    SqliteDb& operator=(const SqliteDb&) = delete;

    SqliteDb(SqliteDb&& other) noexcept;
    SqliteDb& operator=(SqliteDb&& other) noexcept;

    sqlite3* handle() const noexcept;
    void exec(const std::string& sql);
    Statement prepare(const std::string& sql);
    Statement prepare_persistent(const std::string& sql);

private:
    sqlite3* db_;
};

}  // namespace rapid_inbox::ingestd
