
#include "BufferPool.h"
#include "PageGuard.h"
#include "ThreadPool.h"

#include <cassert>
#include <memory_resource>
#include <ostream>
#include <iostream>
#include <random>

void basic_test() {
    constexpr int page_size  = 1024;
    constexpr int page_count = 10;
    const auto* const fp = "./Test/basic.test";
    BufferPool bp(fp, page_size, page_count);

    // WPG
    auto [wpg, w_rc] = bp.get_write_page_guard(0);
    if (w_rc != PageGuardFailRC::ok) {
        std::cerr << "Failed to get write page\n"; exit(1); }
    std::cout << "Got write page" << std::endl;

    const auto* const msg = "hello world";
    wpg.write(msg, 0);
    wpg.release();

    // RPG
    auto [rpg, r_rc] = bp.get_read_page_guard(0);
    if (r_rc != PageGuardFailRC::ok) {
        std::cerr << "Failed to get read page\n"; exit(1); }

    auto read_msg = rpg.read();
    std::cout << "Got read page message. (" << read_msg << ")" << std::endl;
    rpg.release();
}

template <typename T>
bool vec_contains(const std::vector<T>& vec, const T& element) {
    for (const auto& elem : vec) {
        if (elem == element) { return true; }
    }
    return false;
}

template <typename T>
bool vec_loe_any(const std::vector<T>& vec, const T& element, size_t max = std::numeric_limits<size_t>::max()) {
    if (max == std::numeric_limits<size_t>::max()) { max = vec.size(); }
    for (int i = 0; i < max; i++) {
        const auto& elem = vec[i];
        if (element <= elem) { return true; }
    }
    return false;
}

void read_func(BufferPool<std::allocator<char>>& bp, std::atomic<int>& reader_count, const unsigned int timer, const unsigned int num_loops) {
    int MAX_PID = num_loops;

    // std::cout << "Read work started\n";
    std::this_thread::sleep_for(std::chrono::microseconds(timer));

    std::vector<ReadPageGuard> rpgs;
    rpgs.resize(num_loops);

    FastRandom_XORShift page_gen;
    std::vector<page_id_t> pids;
    pids.resize(num_loops);

    // Strictly increasing pid -> doesn't deadlock
    for (int i = 0; i < num_loops; i++) {
        MAX_PID++;
        page_id_t pid = page_gen.next() % MAX_PID;
        while (vec_loe_any(pids, pid)) {
            pid = page_gen.next() % MAX_PID;
        }
        pids[i] = pid;
    }

    // Random unique pid -> eventually deadlocks
    // for (int i = 0; i < num_loops; i++) {
    //     page_id_t pid = page_dst(gen);
    //     while (vec_contains(pids, pid)) {
    //         pid = page_dst(gen);
    //     }
    //     pids[i] = pid;
    // }
    STACK_TRACE_ASSERT(pids.size() == num_loops);

    

    for (int i = 0; i < num_loops; i++) {
        uint32_t backoff = 64;
        constexpr uint32_t max_backoff = 1'000'000; // 1ms worst-case
        while (true) {
            const auto pid = pids[i];

            THREAD_PRINT("atempting read  guard for pid (" + std::to_string(pid) + ")");
            auto [rpg, rc] = bp.get_read_page_guard(pid);
            if (rc == PageGuardFailRC::ok) {
                rpgs[i] = std::move(rpg);
                THREAD_PRINT("acquired  read  guard for pid (" + std::to_string(pid) + ")");
                break; 
            }
            THREAD_PRINT("fail acq  read  guard for pid (" + std::to_string(pid) + ")");

            // Release pages or deadlock
            for (int j = 0; j < i; j++) {
                THREAD_PRINT("releasing read  guard for pid (" + std::to_string(rpgs[j].pid()) + ") early");
                rpgs[j].release();
            }
            i = 0;
            
            // std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::nanoseconds(backoff));
            backoff *= 2;
            backoff = std::min(backoff * 2, max_backoff);
        }
    }

    for (auto& rpg : rpgs) {
        auto read_msg = rpg.read();
        THREAD_PRINT("releasing read  guard for pid (" + std::to_string(rpg.pid()) + ")");
        rpg.release();
    }

    reader_count.fetch_sub(1);
};

void write_func(BufferPool<std::allocator<char>>& bp, std::atomic<int>& writer_count, const unsigned int timer, const unsigned int num_loops) {
    // std::cout << "Write work started\n";
    int MAX_PID = num_loops;

    std::this_thread::sleep_for(std::chrono::nanoseconds(timer));

    std::vector<WritePageGuard> wpgs;
    wpgs.resize(num_loops);

    FastRandom_XORShift page_gen;
    std::vector<page_id_t> pids;
    pids.resize(num_loops);

    // Strictly increasing pid -> doesn't deadlock
    for (int i = 0; i < num_loops; i++) {
        MAX_PID++;
        page_id_t pid = page_gen.next() % MAX_PID;
        while (vec_loe_any(pids, pid)) {
            pid = page_gen.next() % MAX_PID;
        }
        pids[i] = pid;
    }

    // Random unique pid -> eventually deadlocks
    // for (int i = 0; i < num_loops; i++) {
    //     page_id_t pid = page_dst(gen);
    //     while (vec_contains(pids, pid)) {
    //         pid = page_dst(gen);
    //     }
    //     pids[i] = pid;
    // }
    STACK_TRACE_ASSERT(pids.size() == num_loops);



    for (int i = 0; i < num_loops; i++) {
        uint32_t backoff = 1;
        constexpr uint32_t max_backoff = 1'000'000; // 1ms worst-case
        while (true) {
            const auto pid = pids[i];

            THREAD_PRINT("atempting write guard for pid (" + std::to_string(pid) + ")");
            auto [wpg, rc] = bp.get_write_page_guard(pid);
            if (rc == PageGuardFailRC::ok) {
                wpgs[i] = std::move(wpg);
                THREAD_PRINT("acquired  write guard for pid (" + std::to_string(pid) + ")");
                break; 
            }
            THREAD_PRINT("fail acq  write guard for pid (" + std::to_string(pid) + ")");

            // Release pages or deadlock
            for (int j = 0; j < i; j++) {
                THREAD_PRINT("releasing write guard for pid (" + std::to_string(wpgs[j].pid()) + ") early");
                wpgs[j].release();
            }
            i = 0;

            // std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::nanoseconds(backoff));
            backoff *= 2;
            backoff = std::min(backoff * 2, max_backoff);
            if (backoff == max_backoff) { backoff = 1; } // So writers don't get starved
        }
    }

    for (auto& wpg : wpgs) {
        const char msg = wpg.pid();
        try {
            wpg.write({&msg, 1}, 0);
        } catch (std::exception& e) {
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Exception during 'write()': " + std::string(e.what()));
        }
        std::string_view read_msg = wpg.read();
        STACK_TRACE_ASSERT(read_msg[0] == std::string(1, msg)[0]);
        THREAD_PRINT("releasing write guard for pid (" + std::to_string(wpg.pid()) + ")");
        wpg.release();
    }

    writer_count.fetch_sub(1);
};

void thread_test() {
    constexpr int page_size  = 1024 * 4;
    constexpr int page_count = 20;
    const auto* const fp = "./Test/thread.test";
    BufferPool bp(fp, page_size, page_count);

    FastRandom_XORShift op_gen;
    constexpr int MAX_OP = 1;
    FastRandom_XORShift timer_gen;
    constexpr int MAX_TIMER = 100;
    FastRandom_XORShift loop_gen;
    constexpr int MIN_LOOP = 1;
    constexpr int MAX_LOOP = 15;


    std::byte buffer[1024 * 64]; // local arena
    std::pmr::monotonic_buffer_resource allocator(buffer, sizeof(buffer));
    constexpr int num_workers = 2;
    PmrThreadPool pool(num_workers, &allocator);
    static std::atomic<int> reader_count = 0;
    static std::atomic<int> writer_count = 0;
    constexpr int num_ops = 600000;
    for (int i = 0; i < num_ops; i++) {
        const int op = op_gen.next() % MAX_OP;
        switch (op) {
            case 0: { // Write
                const unsigned int wait = timer_gen.next() % MAX_TIMER;
                const unsigned int num_loops = (loop_gen.next() % MAX_LOOP) + MIN_LOOP;

                writer_count.fetch_add(1);
                pool.give_work(write_func, std::ref(bp),  std::ref(writer_count), wait, num_loops);
            } break; 
            case 1: { // Read
                const unsigned int wait = timer_gen.next() % MAX_TIMER;
                const unsigned int num_loops = (loop_gen.next() % MAX_LOOP) + MIN_LOOP;

                reader_count.fetch_add(1);
                pool.give_work(read_func, std::ref(bp), std::ref(reader_count), wait, num_loops);
            } break;
            default:
                FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Shouldn't be here");
        }
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // bp.mu.lock();
    THREAD_PRINT("Joining (" + std::to_string(num_workers) + ") threads. Writers (" + std::to_string(writer_count) + "), Readers (" + std::to_string(reader_count) + ")");
    // bp.mu.unlock();
    // pool.wait_until_idle();
}


void disk_test() {
    int loop_count = 0;
    auto total_start = std::chrono::high_resolution_clock::now(); 
    for (int i = 0; i < 2; i++) {
        // THREAD_PRINT("Loop count: " + std::to_string(loop_count++));
        // auto start = ::chrono::high_resolution_clock::now();
        thread_test();
        // auto end = std::chrono::high_resolution_clock::now();
        // auto elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
        // std::cout << "Elapsed: " << elapsed_ms << " ms\n"; 
    }
    auto total_end = std::chrono::high_resolution_clock::now();
    auto total_elapsed_ms = std::chrono::duration<double, std::milli>(total_end - total_start).count();
    std::cout << "Total elapsed: " << total_elapsed_ms << " ms\n"; 
}

void write_correctness_test() { // Should create a file with 10 "hello world"s next to eachother
    constexpr int page_size  = 11;
    constexpr int num_writes = 10;
    constexpr int page_count = num_writes;
    const auto* const fp = "./Test/write_correctness.test";
    BufferPool bp(fp, page_size, page_count);

    auto write_lambda = [&](const page_id_t pid) {
        auto [wpg, rc] = bp.get_write_page_guard(pid);
        if (rc != PageGuardFailRC::ok) {
            std::cerr << "Failed to get write page\n"; exit(1); }
        std::cout << "Got write page" << std::endl;
    
        const auto* const msg = "hello world";
        wpg.write({msg, 11}, 0);
        wpg.release();
    };

    for (int i = 0; i < num_writes; i++) {
        write_lambda(i);
    }
}


// void disk_write_test() { // Should create a file with 10 "hello world"s next to eachother
//     constexpr int page_size  = 11;
//     constexpr int num_writes = 10;
//     constexpr int page_count = num_writes;
//     const auto* const fp = "./Test/write_correctness.test";
//     BufferPool bp(fp, page_size, page_count);

//     for (int i = 0; i < num_writes; i++) {
//         auto wpg_opt = bp.get_write_page_guard(i);
//         auto wpg = std::move(wpg_opt.value());
//         wpg.write({"hello world", page_size}, 0);
//         bp.disk_write(i);
//     }
// }


