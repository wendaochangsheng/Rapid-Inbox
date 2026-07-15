#pragma once

#include "mail_job.h"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <limits>
#include <vector>

namespace rapid_inbox::ingestd {

struct MailQueueStats {
    std::size_t queued_messages = 0;
    std::size_t reserved_messages = 0;
    std::size_t in_flight_messages = 0;
    std::size_t total_messages = 0;
    std::size_t queue_bytes = 0;
    std::size_t reserved_bytes = 0;
    std::size_t total_bytes = 0;
    bool reservations_paused = false;
    bool closed = false;
};

class MailQueue {
public:
    explicit MailQueue(std::size_t capacity_messages,
                       std::size_t capacity_bytes = std::numeric_limits<std::size_t>::max());
    bool try_push(MailJob job);
    bool try_reserve(std::size_t bytes);
    std::size_t try_grow_reservation(std::size_t minimum_additional_bytes,
                                     std::size_t preferred_additional_bytes);
    bool push_reserved(MailJob job, std::size_t reserved_bytes);
    void cancel_reservation(std::size_t reserved_bytes);
    void set_reservations_paused(bool paused);
    void complete_batch(std::size_t message_count, std::size_t bytes);
    std::vector<MailJob> pop_batch(std::size_t max_items, std::chrono::milliseconds wait_for);
    void close();
    bool closed() const;
    std::size_t size() const;
    std::size_t size_bytes() const;
    std::size_t total_size() const;
    std::size_t total_size_bytes() const;
    MailQueueStats stats() const;

private:
    bool has_capacity_unlocked(std::size_t bytes) const;

    std::size_t capacity_messages_;
    std::size_t capacity_bytes_;
    mutable std::mutex mutex_;
    std::condition_variable changed_;
    std::queue<MailJob> queue_;
    std::size_t queue_bytes_ = 0;
    std::size_t reserved_messages_ = 0;
    std::size_t reserved_bytes_ = 0;
    std::size_t in_flight_messages_ = 0;
    bool reservations_paused_ = false;
    bool closed_ = false;
};

}
