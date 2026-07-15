#pragma once

#include <atomic>
#include <cstddef>

namespace rapid_inbox::ingestd {

struct IngestRuntimeStats {
    std::atomic<std::size_t> active_connections{0};
    // RCPT rejections are counted on connection threads and drained by the
    // status thread in one SQLite write.  This keeps the SMTP hot path free of
    // database locks while preserving dashboard history.
    std::atomic<std::size_t> rejected_recipients_pending{0};
};

}  // namespace rapid_inbox::ingestd
