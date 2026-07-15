#include "mail_queue.h"

#include <algorithm>
#include <utility>

namespace rapid_inbox::ingestd {

MailQueue::MailQueue(std::size_t capacity_messages, std::size_t capacity_bytes)
    : capacity_messages_(capacity_messages), capacity_bytes_(capacity_bytes) {}

bool MailQueue::try_push(MailJob job) {
    const std::size_t bytes = job.raw_content.size();
    if (!try_reserve(bytes)) {
        return false;
    }
    return push_reserved(std::move(job), bytes);
}

bool MailQueue::has_capacity_unlocked(std::size_t bytes) const {
    if (reservations_paused_) {
        return false;
    }
    if (queue_.size() + reserved_messages_ + in_flight_messages_ >= capacity_messages_) {
        return false;
    }
    if (bytes > capacity_bytes_ || queue_bytes_ > capacity_bytes_ - bytes) {
        return false;
    }
    return reserved_bytes_ <= capacity_bytes_ - bytes - queue_bytes_;
}

void MailQueue::set_reservations_paused(bool paused) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        reservations_paused_ = paused;
    }
    changed_.notify_all();
}

bool MailQueue::try_reserve(std::size_t bytes) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (closed_ || !has_capacity_unlocked(bytes)) {
        return false;
    }
    ++reserved_messages_;
    reserved_bytes_ += bytes;
    return true;
}

std::size_t MailQueue::try_grow_reservation(std::size_t minimum_additional_bytes,
                                            std::size_t preferred_additional_bytes) {
    if (minimum_additional_bytes == 0 ||
        preferred_additional_bytes < minimum_additional_bytes) {
        return 0;
    }
    std::lock_guard<std::mutex> guard(mutex_);
    // A reservation established before maintenance is the drain contract: it
    // may keep growing while new reservations are paused. Closing the queue is
    // different and prevents any further ownership transfer.
    if (closed_ || reserved_messages_ == 0 || queue_bytes_ > capacity_bytes_ ||
        reserved_bytes_ > capacity_bytes_ - queue_bytes_) {
        return 0;
    }
    const std::size_t available = capacity_bytes_ - queue_bytes_ - reserved_bytes_;
    if (available < minimum_additional_bytes) {
        return 0;
    }
    const std::size_t added = std::min(preferred_additional_bytes, available);
    reserved_bytes_ += added;
    return added;
}

bool MailQueue::push_reserved(MailJob job, std::size_t reserved_bytes) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (reserved_messages_ == 0 || reserved_bytes_ < reserved_bytes) {
            return false;
        }
        --reserved_messages_;
        reserved_bytes_ -= reserved_bytes;
        if (closed_ || job.raw_content.size() > reserved_bytes) {
            return false;
        }
        queue_bytes_ += job.raw_content.size();
        queue_.push(std::move(job));
    }
    changed_.notify_one();
    return true;
}

void MailQueue::cancel_reservation(std::size_t reserved_bytes) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (reserved_messages_ == 0 || reserved_bytes_ < reserved_bytes) {
            return;
        }
        --reserved_messages_;
        reserved_bytes_ -= reserved_bytes;
    }
    changed_.notify_all();
}

void MailQueue::complete_batch(std::size_t message_count, std::size_t bytes) {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (message_count > in_flight_messages_ || bytes > queue_bytes_) {
            return;
        }
        in_flight_messages_ -= message_count;
        queue_bytes_ -= bytes;
    }
    changed_.notify_all();
}

std::vector<MailJob> MailQueue::pop_batch(std::size_t max_items,
                                          std::chrono::milliseconds wait_for) {
    std::unique_lock<std::mutex> lock(mutex_);
    changed_.wait_for(lock, wait_for, [&] { return closed_ || !queue_.empty(); });
    if (!closed_ && !queue_.empty() && queue_.size() < max_items && wait_for.count() > 0) {
        const auto deadline = std::chrono::steady_clock::now() + wait_for;
        changed_.wait_until(lock, deadline, [&] { return closed_ || queue_.size() >= max_items; });
    }
    std::vector<MailJob> batch;
    while (!queue_.empty() && batch.size() < max_items) {
        batch.push_back(std::move(queue_.front()));
        queue_.pop();
    }
    in_flight_messages_ += batch.size();
    return batch;
}

void MailQueue::close() {
    {
        std::lock_guard<std::mutex> guard(mutex_);
        closed_ = true;
    }
    changed_.notify_all();
}

bool MailQueue::closed() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return closed_;
}

std::size_t MailQueue::size() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return queue_.size();
}

std::size_t MailQueue::size_bytes() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return queue_bytes_;
}

std::size_t MailQueue::total_size() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return queue_.size() + reserved_messages_ + in_flight_messages_;
}

std::size_t MailQueue::total_size_bytes() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return queue_bytes_ + reserved_bytes_;
}

MailQueueStats MailQueue::stats() const {
    std::lock_guard<std::mutex> guard(mutex_);
    const std::size_t queued_messages = queue_.size();
    return MailQueueStats{
        queued_messages,
        reserved_messages_,
        in_flight_messages_,
        queued_messages + reserved_messages_ + in_flight_messages_,
        queue_bytes_,
        reserved_bytes_,
        queue_bytes_ + reserved_bytes_,
        reservations_paused_,
        closed_,
    };
}

}
