#include "../src/instance_lock.h"

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

namespace test {
void check(bool condition, const std::string& message);
}

namespace {

namespace fs = std::filesystem;

int wait_for_child(pid_t pid) {
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0) {
        if (errno == EINTR) {
            continue;
        }
        throw std::runtime_error("waitpid failed for instance lock test");
    }
    if (!WIFEXITED(status)) {
        throw std::runtime_error("instance lock child did not exit normally");
    }
    return WEXITSTATUS(status);
}

std::string read_file(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(input)),
                       std::istreambuf_iterator<char>());
}

}  // namespace

void test_ingest_instance_lock_rejects_competitor_and_releases_cleanly() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-instance-lock-lifecycle";
    fs::remove_all(root);

    IngestInstanceLock owner;
    owner.acquire(root, "ingest_owner");
    test::check(owner.owns_lock(), "instance lock reports ownership after acquire");
    const fs::path lock_path = root / std::string(kIngestInstanceLockFilename);
    test::check(fs::is_regular_file(lock_path), "instance lock file exists");
    const fs::perms mode = fs::status(lock_path).permissions() &
                           (fs::perms::owner_all | fs::perms::group_all |
                            fs::perms::others_all);
    test::check(mode == (fs::perms::owner_read | fs::perms::owner_write),
                "instance lock metadata is private");
    const std::string metadata = read_file(lock_path);
    test::check(metadata.find("\"instance_id\":\"ingest_owner\"") != std::string::npos,
                "instance lock records owning instance id");
    test::check(metadata.find("\"pid\":") != std::string::npos,
                "instance lock records owning process id");

    IngestInstanceLock contender;
    bool rejected = false;
    try {
        contender.acquire(root, "ingest_contender");
    } catch (const std::runtime_error& exc) {
        rejected = std::string(exc.what()).find("only one ingestd process") != std::string::npos;
    }
    test::check(rejected, "second owner receives a clear singleton startup error");
    test::check(!contender.owns_lock(), "rejected contender owns no descriptor");

    owner.release();
    test::check(!owner.owns_lock(), "explicit release drops instance lock ownership");
    contender.acquire(root, "ingest_successor");
    test::check(contender.owns_lock(), "successor acquires persistent lock file after release");
    contender.release();
    test::check(fs::is_regular_file(lock_path),
                "release leaves lock inode in place to prevent unlink/recreate races");
    fs::remove_all(root);
}

void test_ingest_instance_lock_is_cross_process_and_crash_safe() {
    using namespace rapid_inbox::ingestd;
    const fs::path root = fs::temp_directory_path() / "rapid-inbox-instance-lock-process";
    fs::remove_all(root);

    IngestInstanceLock parent_owner;
    parent_owner.acquire(root, "parent_owner");

    const pid_t blocked_child = ::fork();
    if (blocked_child < 0) {
        throw std::runtime_error("fork failed for blocked instance lock child");
    }
    if (blocked_child == 0) {
        try {
            IngestInstanceLock contender;
            contender.acquire(root, "blocked_child");
            ::_exit(20);
        } catch (const std::runtime_error& exc) {
            const bool clear_error =
                std::string(exc.what()).find("only one ingestd process") != std::string::npos;
            ::_exit(clear_error ? 0 : 21);
        } catch (...) {
            ::_exit(22);
        }
    }
    test::check(wait_for_child(blocked_child) == 0,
                "separate process cannot acquire an owned storage-root lock");

    parent_owner.release();
    const pid_t crashing_child = ::fork();
    if (crashing_child < 0) {
        throw std::runtime_error("fork failed for crash instance lock child");
    }
    if (crashing_child == 0) {
        try {
            IngestInstanceLock crash_owner;
            crash_owner.acquire(root, "crashing_child");
            // _exit deliberately bypasses the C++ destructor. The kernel must
            // still release flock ownership when it closes process descriptors.
            ::_exit(0);
        } catch (...) {
            ::_exit(23);
        }
    }
    test::check(wait_for_child(crashing_child) == 0,
                "child acquires lock before simulated abrupt process exit");

    IngestInstanceLock post_crash_owner;
    post_crash_owner.acquire(root, "post_crash_owner");
    test::check(post_crash_owner.owns_lock(),
                "kernel releases singleton lock automatically after process crash");
    post_crash_owner.release();
    fs::remove_all(root);
}
