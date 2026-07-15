#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace rapid_inbox::ingestd {

inline constexpr std::string_view kIngestInstanceLockFilename = ".ingestd.instance.lock";

class IngestInstanceLock {
public:
    IngestInstanceLock() = default;
    ~IngestInstanceLock();

    IngestInstanceLock(const IngestInstanceLock&) = delete;
    IngestInstanceLock& operator=(const IngestInstanceLock&) = delete;

    void acquire(const std::filesystem::path& storage_root,
                 const std::string& instance_id);
    void release() noexcept;
    bool owns_lock() const noexcept;
    const std::filesystem::path& path() const noexcept;

private:
    int fd_ = -1;
    std::filesystem::path path_;
};

}  // namespace rapid_inbox::ingestd
