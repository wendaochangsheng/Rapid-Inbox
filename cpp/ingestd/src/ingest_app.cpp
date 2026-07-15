#include "ingest_app.h"

#include "id.h"
#include "logger.h"
#include "time_utils.h"

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <exception>
#include <iterator>
#include <thread>
#include <utility>
#include <vector>

namespace rapid_inbox::ingestd {

IngestApp::IngestApp(Config config, std::shared_ptr<IngestRuntimeStats> runtime_stats)
    : config_(std::move(config)),
      queue_(static_cast<std::size_t>(config_.ingest_queue_max_messages),
             static_cast<std::size_t>(config_.ingest_queue_max_bytes)),
      domains_(config_.database_path, config_.ingest_sqlite_busy_timeout_ms),
      writer_(config_.storage_root,
              config_.database_path,
              config_.ingest_sqlite_busy_timeout_ms,
              config_.ingest_storage_fsync),
      runtime_stats_(runtime_stats == nullptr ? std::make_shared<IngestRuntimeStats>()
                                              : std::move(runtime_stats)),
      instance_id_(make_prefixed_id("ingest_")) {}

IngestApp::~IngestApp() {
    stop_and_drain();
}

void IngestApp::start_writer() {
    instance_lock_.acquire(config_.storage_root, instance_id_);
    try {
        domains_.reload();
        running_ = true;
        status_running_ = true;
        // Publish synchronously before the SMTP listener can start. Otherwise a
        // concurrent clear operation could mistake this live process for an
        // absent ingest daemon during the status thread's scheduling window.
        const MaintenanceState maintenance = writer_.maintenance_state();
        apply_maintenance_state(maintenance);
        const MailQueueStats stats = queue_.stats();
        if (maintenance.token.has_value() && stats.total_messages == 0) {
            // Persist rejections observed before this maintenance boundary. The
            // drained acknowledgement is the clear-all owner's permission to
            // mutate SQLite, so no pre-boundary counters may remain in memory
            // when it is published.
            flush_rejected_metrics();
            writer_.release_sqlite_session();
            writer_.write_maintenance_drained(instance_id_, *maintenance.token);
            acknowledged_maintenance_token_ = *maintenance.token;
        }
        writer_.publish_ingest_status(instance_id_,
                                      stats.total_messages,
                                      stats.total_bytes,
                                      runtime_stats_->active_connections.load(
                                          std::memory_order_acquire),
                                      static_cast<std::size_t>(config_.smtp_max_connections),
                                      maintenance.token);
        writer_threads_.reserve(static_cast<std::size_t>(config_.ingest_worker_count));
        for (int index = 0; index < config_.ingest_worker_count; ++index) {
            writer_threads_.emplace_back([this] { writer_loop(); });
        }
        domain_reload_thread_ = std::thread([this] { domain_reload_loop(); });
        ingest_status_thread_ = std::thread([this] { ingest_status_loop(); });
        Logger::instance().log(LogLevel::Info,
                               "ingest.workers_started",
                               {
                                   {"instance_id", instance_id_},
                                   {"worker_count", config_.ingest_worker_count},
                               });
    } catch (...) {
        running_ = false;
        status_running_ = false;
        stop_cv_.notify_all();
        if (!writer_threads_.empty() || domain_reload_thread_.joinable() ||
            ingest_status_thread_.joinable()) {
            queue_.close();
        }
        for (std::thread& thread : writer_threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }
        writer_threads_.clear();
        if (domain_reload_thread_.joinable()) {
            domain_reload_thread_.join();
        }
        if (ingest_status_thread_.joinable()) {
            ingest_status_thread_.join();
        }
        writer_.remove_ingest_status(instance_id_);
        instance_lock_.release();
        throw;
    }
}

void IngestApp::stop_and_drain() {
    if (!instance_lock_.owns_lock()) {
        queue_.close();
        return;
    }
    const bool was_active = running_.load(std::memory_order_acquire) ||
                            status_running_.load(std::memory_order_acquire);
    queue_.close();
    running_ = false;
    stop_cv_.notify_all();
    for (std::thread& thread : writer_threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
    writer_threads_.clear();
    if (domain_reload_thread_.joinable()) {
        domain_reload_thread_.join();
    }
    status_running_ = false;
    stop_cv_.notify_all();
    if (ingest_status_thread_.joinable()) {
        ingest_status_thread_.join();
    }
    // The status thread must be stopped before the final exchange/flush so it
    // cannot race the final maintenance acknowledgement or metric snapshot.
    publish_final_ingest_status();
    writer_.remove_ingest_status(instance_id_);
    instance_lock_.release();
    if (was_active) {
        Logger::instance().log(LogLevel::Info,
                               "ingest.workers_stopped",
                               {
                                   {"instance_id", instance_id_},
                                   {"queue_drained", queue_.total_size() == 0},
                               });
    }
}

void IngestApp::writer_loop() {
    while (running_ || queue_.size() > 0) {
        auto batch = queue_.pop_batch(static_cast<std::size_t>(config_.ingest_batch_max_messages),
                                      std::chrono::milliseconds(config_.ingest_flush_interval_ms));
        if (batch.empty()) {
            continue;
        }
        std::size_t batch_bytes = 0;
        for (const MailJob& job : batch) {
            batch_bytes += job.raw_content.size();
        }
        const std::size_t batch_messages = batch.size();
        try {
            write_batch_with_isolation(std::move(batch));
        } catch (const std::exception& exc) {
            Logger::instance().log(LogLevel::Error,
                                   "ingest.worker_abandoned_batch",
                                   {
                                       {"batch_messages", batch_messages},
                                       {"batch_bytes", batch_bytes},
                                       {"error", exc.what()},
                                   });
        } catch (...) {
            Logger::instance().log(LogLevel::Error,
                                   "ingest.worker_abandoned_batch",
                                   {
                                       {"batch_messages", batch_messages},
                                       {"batch_bytes", batch_bytes},
                                       {"error", "unknown exception"},
                                   });
        }
        queue_.complete_batch(batch_messages, batch_bytes);
    }
}

bool IngestApp::try_write_batch(const std::vector<MailJob>& batch, std::string& last_error) {
    // A failed multi-message transaction is split immediately so one permanent
    // poison item does not make every healthy peer repeat expensive MIME work.
    const int attempts = batch.size() == 1 ? config_.ingest_max_retries + 1 : 1;
    std::size_t batch_bytes = 0;
    for (const MailJob& job : batch) {
        batch_bytes += job.raw_content.size();
    }
    for (int attempt = 1; attempt <= attempts; ++attempt) {
        try {
            writer_.write_batch(batch);
            if (attempt > 1) {
                Logger::instance().log(LogLevel::Info,
                                       "ingest.batch_retry_succeeded",
                                       {
                                           {"batch_messages", batch.size()},
                                           {"batch_bytes", batch_bytes},
                                           {"attempt", attempt},
                                       });
            } else if (Logger::instance().enabled(LogLevel::Debug)) {
                Logger::instance().log(LogLevel::Debug,
                                       "ingest.batch_committed",
                                       {
                                           {"batch_messages", batch.size()},
                                           {"batch_bytes", batch_bytes},
                                       });
            }
            return true;
        } catch (const std::exception& exc) {
            last_error = exc.what();
            Logger::instance().log(LogLevel::Warning,
                                   "ingest.batch_write_failed",
                                   {
                                       {"batch_messages", batch.size()},
                                       {"batch_bytes", batch_bytes},
                                       {"attempt", attempt},
                                       {"max_attempts", attempts},
                                       {"will_retry", attempt < attempts},
                                       {"error", last_error},
                                   });
            if (attempt < attempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50 * attempt));
            }
        }
    }
    return false;
}

void IngestApp::write_batch_with_isolation(std::vector<MailJob> batch) {
    if (batch.empty()) {
        return;
    }

    std::string last_error;
    if (try_write_batch(batch, last_error)) {
        return;
    }

    if (batch.size() > 1) {
        Logger::instance().log(LogLevel::Warning,
                               "ingest.batch_split",
                               {
                                   {"batch_messages", batch.size()},
                                   {"reason", "transaction_failed"},
                               });
        const auto midpoint = batch.begin() + static_cast<std::ptrdiff_t>(batch.size() / 2);
        std::vector<MailJob> right;
        right.reserve(static_cast<std::size_t>(batch.end() - midpoint));
        std::move(midpoint, batch.end(), std::back_inserter(right));
        batch.erase(midpoint, batch.end());
        write_batch_with_isolation(std::move(batch));
        write_batch_with_isolation(std::move(right));
        return;
    }

    const MailJob& poison = batch.front();
    try {
        writer_.write_quarantine_record(poison, last_error, config_.ingest_max_retries + 1);
        Logger::instance().log(LogLevel::Error,
                               "ingest.message_quarantined",
                               {
                                   {"message_id", poison.message_id},
                                   {"attempts", config_.ingest_max_retries + 1},
                                   {"error", last_error},
                               });
    } catch (const std::exception& exc) {
        Logger::instance().log(LogLevel::Critical,
                               "ingest.quarantine_write_failed",
                               {
                                   {"message_id", poison.message_id},
                                   {"attempts", config_.ingest_max_retries + 1},
                                   {"error", exc.what()},
                               });
    }
}

void IngestApp::domain_reload_loop() {
    const auto interval = std::chrono::milliseconds(config_.domain_reload_interval_ms);
    bool error_active = false;
    auto next_error_log = std::chrono::steady_clock::time_point::min();
    std::unique_lock lock(stop_mutex_);
    while (running_) {
        if (stop_cv_.wait_for(lock, interval, [this] { return !running_.load(); })) {
            break;
        }
        lock.unlock();
        try {
            domains_.reload();
            if (error_active) {
                Logger::instance().log(LogLevel::Info, "domain.reload_recovered");
                error_active = false;
            }
        } catch (const std::exception& exc) {
            const auto now = std::chrono::steady_clock::now();
            if (!error_active || now >= next_error_log) {
                Logger::instance().log(LogLevel::Warning,
                                       "domain.reload_failed",
                                       {{"error", exc.what()}});
                next_error_log = now + std::chrono::seconds(30);
            }
            error_active = true;
        }
        lock.lock();
    }
}

void IngestApp::ingest_status_loop() {
    using namespace std::chrono_literals;
    constexpr auto status_interval = 500ms;
    constexpr auto maintenance_poll_interval = 50ms;
    auto next_status = std::chrono::steady_clock::now();
    auto next_error_log = std::chrono::steady_clock::time_point::min();
    bool error_active = false;
    std::unique_lock lock(stop_mutex_);
    while (status_running_) {
        lock.unlock();
        try {
            const MaintenanceState maintenance = writer_.maintenance_state();
            apply_maintenance_state(maintenance);
            const MailQueueStats stats = queue_.stats();
            if (!maintenance.active) {
                flush_rejected_metrics();
            }
            if (!maintenance.token.has_value()) {
                acknowledged_maintenance_token_.clear();
            } else if (stats.total_messages == 0 &&
                       acknowledged_maintenance_token_ != *maintenance.token) {
                // Flush exactly at the new-token drain boundary. Once the ACK
                // is visible, active maintenance must not reopen SQLite; new
                // rejections stay pending until this token is removed.
                flush_rejected_metrics();
                // Closing the persistent SQLite handle is part of the drain
                // contract. It lets the maintenance owner VACUUM or atomically
                // replace the database before this process acknowledges it.
                writer_.release_sqlite_session();
                writer_.write_maintenance_drained(instance_id_, *maintenance.token);
                acknowledged_maintenance_token_ = *maintenance.token;
                Logger::instance().log(LogLevel::Info,
                                       "maintenance.queue_drained",
                                       {
                                           {"instance_id", instance_id_},
                                           {"queue_messages", stats.total_messages},
                                           {"queue_bytes", stats.total_bytes},
                                       });
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= next_status) {
                writer_.publish_ingest_status(
                    instance_id_,
                    stats.total_messages,
                    stats.total_bytes,
                    runtime_stats_->active_connections.load(std::memory_order_acquire),
                    static_cast<std::size_t>(config_.smtp_max_connections),
                    maintenance.token);
                next_status = now + status_interval;
            }
            if (error_active) {
                Logger::instance().log(LogLevel::Info, "status.publish_recovered");
                error_active = false;
            }
        } catch (const std::exception& exc) {
            const auto now = std::chrono::steady_clock::now();
            if (!error_active || now >= next_error_log) {
                Logger::instance().log(LogLevel::Warning,
                                       "status.publish_failed",
                                       {{"error", exc.what()}});
                next_error_log = now + std::chrono::seconds(30);
            }
            error_active = true;
        }
        lock.lock();
        stop_cv_.wait_for(lock,
                          maintenance_poll_interval,
                          [this] { return !status_running_.load(); });
    }
}

void IngestApp::apply_maintenance_state(const MaintenanceState& maintenance) {
    // This queue mutex is the linearization point for maintenance draining.
    // A reservation that wins before the pause is counted; one that arrives
    // after it is rejected. Only then may the monitor observe total_messages=0
    // and publish the drained acknowledgement.
    queue_.set_reservations_paused(maintenance.active);
    const bool previous =
        maintenance_active_.exchange(maintenance.active, std::memory_order_acq_rel);
    if (previous != maintenance.active) {
        Logger::instance().log(LogLevel::Info,
                               maintenance.active ? "maintenance.started"
                                                  : "maintenance.finished",
                               {{"instance_id", instance_id_}});
    }
}

void IngestApp::publish_final_ingest_status() {
    try {
        const MaintenanceState maintenance = writer_.maintenance_state();
        apply_maintenance_state(maintenance);
        if (!maintenance.active) {
            flush_rejected_metrics();
        }
        const MailQueueStats stats = queue_.stats();
        writer_.publish_ingest_status(instance_id_,
                                      stats.total_messages,
                                      stats.total_bytes,
                                      runtime_stats_->active_connections.load(
                                          std::memory_order_acquire),
                                      static_cast<std::size_t>(config_.smtp_max_connections),
                                      maintenance.token);
        if (maintenance.token.has_value() && stats.total_messages == 0 &&
            acknowledged_maintenance_token_ != *maintenance.token) {
            flush_rejected_metrics();
            writer_.release_sqlite_session();
            writer_.write_maintenance_drained(instance_id_, *maintenance.token);
            acknowledged_maintenance_token_ = *maintenance.token;
        }
    } catch (const std::exception& exc) {
        Logger::instance().log(LogLevel::Warning,
                               "status.final_publish_failed",
                               {{"error", exc.what()}});
    }
}

void IngestApp::flush_rejected_metrics() {
    const std::size_t pending = runtime_stats_->rejected_recipients_pending.exchange(
        0, std::memory_order_acq_rel);
    if (pending == 0) {
        return;
    }
    try {
        writer_.write_rejected_metric(utc_now(), pending);
    } catch (...) {
        runtime_stats_->rejected_recipients_pending.fetch_add(
            pending, std::memory_order_release);
        throw;
    }
}

}
