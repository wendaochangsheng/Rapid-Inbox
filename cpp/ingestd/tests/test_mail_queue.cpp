#include "../src/mail_queue.h"

#include <chrono>
#include <future>
#include <thread>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace test {
void check(bool condition, const std::string& message);
}

static_assert(
    std::is_same_v<decltype(&rapid_inbox::ingestd::MailQueue::try_push),
                   bool (rapid_inbox::ingestd::MailQueue::*)(rapid_inbox::ingestd::MailJob)>,
    "MailQueue::try_push should accept MailJob by value so callers can move large payloads");

void test_mail_queue_capacity_and_close() {
    rapid_inbox::ingestd::MailQueue queue(1);
    rapid_inbox::ingestd::MailJob job;
    job.message_id = "msg_1";
    test::check(queue.try_push(job), "first push fits");
    test::check(!queue.try_push(job), "second push rejected by capacity");
    auto popped = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(popped.size() == 1, "pop one item");
    test::check(popped[0].message_id == "msg_1", "popped message id");
    queue.close();
    test::check(!queue.try_push(job), "push rejected after close");
    auto empty = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(empty.empty(), "closed empty queue returns empty batch");
}

void test_mail_queue_try_push_accepts_lvalues_and_rvalues() {
    rapid_inbox::ingestd::MailQueue queue(2);
    rapid_inbox::ingestd::MailJob first;
    first.message_id = "msg_lvalue";
    rapid_inbox::ingestd::MailJob second;
    second.message_id = "msg_rvalue";

    test::check(queue.try_push(first), "lvalue push works");
    test::check(queue.try_push(std::move(second)), "rvalue push works");

    auto popped = queue.pop_batch(10, std::chrono::milliseconds(1));
    test::check(popped.size() == 2, "lvalue and rvalue pushes pop together");
    test::check(popped[0].message_id == "msg_lvalue", "lvalue message id preserved");
    test::check(popped[1].message_id == "msg_rvalue", "rvalue message id preserved");
}

void test_mail_queue_close_wakes_waiting_pop_batch() {
    rapid_inbox::ingestd::MailQueue queue(1);
    std::promise<void> consumer_started;
    auto started = consumer_started.get_future();
    std::promise<std::vector<rapid_inbox::ingestd::MailJob>> result;
    auto result_future = result.get_future();

    std::thread consumer([&] {
        consumer_started.set_value();
        result.set_value(queue.pop_batch(10, std::chrono::seconds(5)));
    });

    started.wait();
    const auto wait_status = result_future.wait_for(std::chrono::milliseconds(50));
    queue.close();

    auto popped = result_future.get();
    consumer.join();
    test::check(wait_status == std::future_status::timeout, "consumer waits before close");
    test::check(popped.empty(), "close wakes waiting consumer with empty batch");
}

void test_mail_queue_enforces_byte_capacity_and_reservations() {
    rapid_inbox::ingestd::MailQueue queue(3, 5);
    rapid_inbox::ingestd::MailJob first;
    first.message_id = "msg_first";
    first.raw_content = "1234";
    test::check(queue.try_push(std::move(first)), "first payload fits byte budget");
    test::check(queue.size_bytes() == 4, "queued byte size tracked");
    test::check(queue.total_size() == 1, "total count includes queued message");
    test::check(queue.total_size_bytes() == 4, "total bytes include queued payload");

    test::check(!queue.try_reserve(2), "reservation exceeding remaining bytes rejected");
    test::check(queue.try_reserve(1), "reservation fitting remaining byte accepted");
    test::check(queue.total_size() == 2, "total count includes reservation");
    test::check(queue.total_size_bytes() == 5, "total bytes include reservation");
    rapid_inbox::ingestd::MailJob second;
    second.message_id = "msg_second";
    second.raw_content.push_back('5');
    test::check(queue.push_reserved(std::move(second), 1), "reserved payload committed");
    test::check(queue.size_bytes() == 5, "reserved payload counted");

    auto batch = queue.pop_batch(3, std::chrono::milliseconds(1));
    test::check(batch.size() == 2, "byte-limited queue pops both messages");
    test::check(queue.size_bytes() == 5, "in-flight batch retains byte reservation");
    const auto in_flight = queue.stats();
    test::check(in_flight.queued_messages == 0, "popped messages leave queued count");
    test::check(in_flight.in_flight_messages == 2, "popped messages enter in-flight count");
    test::check(in_flight.total_messages == 2, "total count includes in-flight messages");
    queue.complete_batch(batch.size(), 5);
    test::check(queue.size_bytes() == 0, "popping releases byte capacity");
    test::check(queue.total_size() == 0, "completed batch leaves queue fully drained");
}

void test_mail_queue_maintenance_pause_linearizes_new_reservations() {
    rapid_inbox::ingestd::MailQueue queue(3, 32);
    test::check(queue.try_reserve(8), "reservation established before maintenance pause");
    queue.set_reservations_paused(true);
    test::check(queue.stats().reservations_paused, "queue reports maintenance pause");
    test::check(!queue.try_reserve(1), "maintenance pause rejects new reservations");
    test::check(queue.try_grow_reservation(2, 4) == 4,
                "pre-pause reservation may grow in chunks while draining");

    rapid_inbox::ingestd::MailJob accepted_before_pause;
    accepted_before_pause.message_id = "msg_before_pause";
    accepted_before_pause.raw_content = "1234567890";
    test::check(queue.push_reserved(std::move(accepted_before_pause), 12),
                "pre-pause reservation may finish and drain");

    queue.set_reservations_paused(false);
    test::check(queue.try_reserve(1), "reservations resume after maintenance");
    queue.cancel_reservation(1);
}

void test_mail_queue_grows_byte_reservations_without_consuming_message_slots() {
    rapid_inbox::ingestd::MailQueue queue(3, 10);
    test::check(queue.try_reserve(0), "first DATA reserves only a message slot");
    test::check(queue.total_size() == 1, "zero-byte reservation counts as one message");
    test::check(queue.total_size_bytes() == 0, "zero-byte reservation consumes no byte budget");
    test::check(queue.try_grow_reservation(3, 8) == 8,
                "reservation grows to preferred chunk when capacity permits");
    test::check(queue.try_reserve(0), "second DATA can reserve another message slot");
    test::check(queue.try_grow_reservation(3, 8) == 0,
                "growth fails when minimum actual bytes do not fit");
    test::check(queue.try_grow_reservation(2, 8) == 2,
                "growth uses remaining capacity when it covers actual bytes");

    rapid_inbox::ingestd::MailJob first;
    first.message_id = "msg_chunk_first";
    first.raw_content = "123";
    test::check(queue.push_reserved(std::move(first), 8),
                "push releases unused bytes from first chunk");
    test::check(queue.total_size_bytes() == 5,
                "actual queued bytes plus second reservation remain accounted");

    rapid_inbox::ingestd::MailJob second;
    second.message_id = "msg_chunk_second";
    second.raw_content = "45";
    test::check(queue.push_reserved(std::move(second), 2), "second reservation commits");
    test::check(queue.total_size_bytes() == 5, "queued byte accounting is exact after push");
}

void test_mail_queue_group_commit_wait_collects_arrivals() {
    rapid_inbox::ingestd::MailQueue queue(10, 1024);
    std::promise<std::vector<rapid_inbox::ingestd::MailJob>> result;
    auto result_future = result.get_future();
    std::thread consumer([&] {
        result.set_value(queue.pop_batch(2, std::chrono::milliseconds(200)));
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    rapid_inbox::ingestd::MailJob first;
    first.message_id = "msg_group_1";
    test::check(queue.try_push(std::move(first)), "first group item queued");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    rapid_inbox::ingestd::MailJob second;
    second.message_id = "msg_group_2";
    test::check(queue.try_push(std::move(second)), "second group item queued");

    test::check(result_future.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready,
                "batch wakes early when group reaches max size");
    auto batch = result_future.get();
    consumer.join();
    test::check(batch.size() == 2, "group commit collected both arrivals");
}
