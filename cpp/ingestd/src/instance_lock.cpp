#include "instance_lock.h"

#include "json_util.h"
#include "time_utils.h"

#include <cerrno>
#include <fcntl.h>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

namespace rapid_inbox::ingestd {
namespace {

void close_fd(int fd) noexcept {
    if (fd >= 0) {
        (void)::close(fd);
    }
}

[[noreturn]] void throw_errno(const std::string& context, int error) {
    throw std::system_error(error, std::generic_category(), context);
}

void write_all(int fd, const std::string& content, const std::filesystem::path& path) {
    const char* cursor = content.data();
    std::size_t remaining = content.size();
    while (remaining > 0) {
        const ssize_t written = ::write(fd, cursor, remaining);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            throw_errno("write failed for ingestd instance lock [" + path.string() + "]",
                        errno);
        }
        if (written == 0) {
            throw std::runtime_error("write made no progress for ingestd instance lock [" +
                                     path.string() + "]");
        }
        cursor += written;
        remaining -= static_cast<std::size_t>(written);
    }
}

}  // namespace

IngestInstanceLock::~IngestInstanceLock() {
    release();
}

void IngestInstanceLock::acquire(const std::filesystem::path& storage_root,
                                 const std::string& instance_id) {
    if (fd_ >= 0) {
        throw std::logic_error("ingestd instance lock is already owned by this object");
    }

    std::error_code error;
    std::filesystem::create_directories(storage_root, error);
    if (error) {
        throw std::system_error(error,
                                "create storage root for ingestd instance lock failed [" +
                                    storage_root.string() + "]");
    }
    std::filesystem::permissions(storage_root,
                                 std::filesystem::perms::owner_all,
                                 std::filesystem::perm_options::replace,
                                 error);
    if (error) {
        throw std::system_error(error,
                                "secure storage root for ingestd instance lock failed [" +
                                    storage_root.string() + "]");
    }

    const std::filesystem::path lock_path = storage_root / kIngestInstanceLockFilename;
    const int fd = ::open(lock_path.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        throw_errno("open ingestd instance lock failed [" + lock_path.string() + "]", errno);
    }

    try {
        struct stat status {};
        if (::fstat(fd, &status) != 0) {
            throw_errno("stat ingestd instance lock failed [" + lock_path.string() + "]", errno);
        }
        if (!S_ISREG(status.st_mode) || status.st_nlink != 1) {
            throw std::runtime_error(
                "ingestd instance lock must be a regular file with one link [" +
                lock_path.string() + "]");
        }
        if (::fchmod(fd, 0600) != 0) {
            throw_errno("chmod ingestd instance lock failed [" + lock_path.string() + "]", errno);
        }
        if (::flock(fd, LOCK_EX | LOCK_NB) != 0) {
            const int lock_error = errno;
            if (lock_error == EWOULDBLOCK || lock_error == EAGAIN) {
                throw std::runtime_error(
                    "ingestd instance lock is already held for storage root [" +
                    storage_root.string() +
                    "]; only one ingestd process may use a storage root");
            }
            throw_errno("acquire ingestd instance lock failed [" + lock_path.string() + "]",
                        lock_error);
        }

        std::ostringstream metadata;
        metadata << "{";
        metadata << "\"instance_id\":\"" << json_escape(instance_id) << "\",";
        metadata << "\"pid\":" << static_cast<long long>(::getpid()) << ",";
        metadata << "\"started_at\":\"" << json_escape(utc_now()) << "\"";
        metadata << "}\n";
        if (::ftruncate(fd, 0) != 0 || ::lseek(fd, 0, SEEK_SET) < 0) {
            throw_errno("reset ingestd instance lock metadata failed [" + lock_path.string() +
                            "]",
                        errno);
        }
        write_all(fd, metadata.str(), lock_path);
        if (::fsync(fd) != 0) {
            throw_errno("fsync ingestd instance lock failed [" + lock_path.string() + "]", errno);
        }
    } catch (...) {
        close_fd(fd);
        throw;
    }

    fd_ = fd;
    path_ = lock_path;
}

void IngestInstanceLock::release() noexcept {
    const int fd = fd_;
    fd_ = -1;
    path_.clear();
    close_fd(fd);
}

bool IngestInstanceLock::owns_lock() const noexcept {
    return fd_ >= 0;
}

const std::filesystem::path& IngestInstanceLock::path() const noexcept {
    return path_;
}

}  // namespace rapid_inbox::ingestd
