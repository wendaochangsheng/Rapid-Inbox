#pragma once

#include "batch_writer.h"
#include "config.h"
#include "domain_cache.h"
#include "instance_lock.h"
#include "mail_queue.h"
#include "runtime_stats.h"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace rapid_inbox::ingestd {

class IngestApp {
public:
    explicit IngestApp(Config config,
                       std::shared_ptr<IngestRuntimeStats> runtime_stats = nullptr);
    ~IngestApp();
    IngestApp(const IngestApp&) = delete;
    IngestApp& operator=(const IngestApp&) = delete;

    void start_writer();
    void stop_and_drain();
    MailQueue& queue() { return queue_; }
    DomainCache& domains() { return domains_; }
    BatchWriter& durable_writer() { return writer_; }
    std::shared_ptr<IngestRuntimeStats> runtime_stats() const { return runtime_stats_; }

private:
    void writer_loop();
    void domain_reload_loop();
    void ingest_status_loop();
    void apply_maintenance_state(const MaintenanceState& maintenance);
    void publish_final_ingest_status();
    void flush_rejected_metrics();
    void write_batch_with_isolation(std::vector<MailJob> batch);
    bool try_write_batch(const std::vector<MailJob>& batch, std::string& last_error);

    Config config_;
    MailQueue queue_;
    DomainCache domains_;
    BatchWriter writer_;
    std::shared_ptr<IngestRuntimeStats> runtime_stats_;
    std::string instance_id_;
    std::string acknowledged_maintenance_token_;
    IngestInstanceLock instance_lock_;
    std::atomic<bool> running_{false};
    std::atomic<bool> status_running_{false};
    std::atomic<bool> maintenance_active_{false};
    std::vector<std::thread> writer_threads_;
    std::thread domain_reload_thread_;
    std::thread ingest_status_thread_;
    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
};

}
