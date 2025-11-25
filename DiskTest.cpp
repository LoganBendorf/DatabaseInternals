
#include "BufferPool.h"
#include "PageGuard.h"
#include "ThreadPool.h"

#include <ostream>
#include <iostream>
#include <random>

void basic_test() {
    constexpr int page_size  = 1024;
    constexpr int page_count = 10;
    const auto* const fp = "./Test/basic.test";
    BufferPool bp(fp, page_size, page_count);

    // WPG
    auto wpg_opt = bp.get_write_page_guard(0);
    if (!wpg_opt.has_value()) {
        std::cerr << "Failed to get write page\n"; exit(1); }
    std::cout << "Got write page" << std::endl;

    WritePageGuard wpg = std::move(wpg_opt.value());

    const auto* const msg = "hello world";
    wpg.write(msg, 0);
    wpg.release();

    // RPG
    auto rpg_opt = bp.get_read_page_guard(0);
    if (!rpg_opt.has_value()) {
        std::cerr << "Failed to get read page\n"; exit(1); }

    ReadPageGuard rpg = std::move(rpg_opt.value());

    auto read_msg = rpg.read();
    std::cout << "Got read page message. (" << read_msg << ")" << std::endl;
    rpg.release();
}




void read_func(BufferPool<std::allocator<char>>& bp, std::atomic<int>& reader_count, const unsigned int timer, const unsigned int pid) __attribute__((used)) {
    // std::cout << "Read work started\n";
    std::this_thread::sleep_for(std::chrono::microseconds(timer));
    reader_count.fetch_add(1);
    ReadPageGuard rpg;
    while (true) {
        try {
            auto opt = bp.get_read_page_guard(pid);
            if (opt.has_value()) {
                rpg = std::move(opt.value());
                std::cout << "Thread (" << std::this_thread::get_id() << ") acquired  read  guard for pid (" << pid << ")\n";
                break; 
            }
        } catch (std::exception& e) {
            FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Exception during 'get_write_page_guard()': " + std::string(e.what()));
        }
        // FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("No looping allowed for now");
    }
    auto read_msg = rpg.read();
    std::cout << "Thread (" << std::this_thread::get_id() << ") releasing read  guard for pid (" << pid << ")\n";
    reader_count.fetch_sub(1);
};

void write_func(BufferPool<std::allocator<char>>& bp, std::atomic<int>& writer_count, const unsigned int timer, const unsigned int pid) __attribute__((used)) {
    // std::cout << "Write work started\n";
    std::this_thread::sleep_for(std::chrono::nanoseconds(timer));
    writer_count.fetch_add(1);
    WritePageGuard wpg;
    int loop_count = 0;
    while (true) {
        try {
            auto opt = bp.get_write_page_guard(pid);
            if (opt.has_value()) {
                wpg = std::move(opt.value());
                std::cout << "Thread (" << std::this_thread::get_id() << ") acquired  write guard for pid (" << pid << ")\n";
                break; 
            }
        } catch (std::exception& e) {
            FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Exception during 'get_write_page_guard()': " + std::string(e.what()));
        }
        // std::cout << "Loop " << loop_count << "...\n";
        // FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("No looping allowed for now");
    }

    const char msg = pid;
    try {
        wpg.write({&msg, 1}, 0);
    } catch (std::exception& e) {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Exception during 'write()': " + std::string(e.what()));
    }
    std::string_view read_msg = wpg.read();
    STACK_TRACE_ASSERT(read_msg[0] == std::string(1, msg)[0]);
    std::cout << "Thread (" << std::this_thread::get_id() << ") releasing write guard for pid (" << pid << ")\n";
    writer_count.fetch_sub(1);
};

void thread_test() {
    constexpr int page_size  = 11;
    constexpr int page_count = 2;
    const auto* const fp = "./Test/thread.test";
    BufferPool bp(fp, page_size, page_count);

    std::random_device rd; // seed
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> op_dst(0, 1); // range
    std::uniform_int_distribution<unsigned int> offset_dst(0, 100); // range
    std::uniform_int_distribution<unsigned int> timer_dst(0, 100); // range
    std::uniform_int_distribution<unsigned int> page_dst(0, 10); // range

    page_id_t pid = 0;
    constexpr int num_ops = 50;
    ThreadPool pool{15};
    static std::atomic<int> reader_count = 0;
    static std::atomic<int> writer_count = 0;
    for (int i = 0; i < 30; i++) {
        const int op = op_dst(gen);
        switch (op) {
            case 0: { // Write
                const unsigned int wait = timer_dst(gen);

                // if (op_dst(gen) == 0) { pid++; }
                // pid++;
                pid = page_dst(gen);

                pool.give_work(write_func, std::ref(bp),  std::ref(writer_count), wait, pid);
            } break; 
            case 1: { // Read
                const unsigned int wait = timer_dst(gen);                
                // if (op_dst(gen) == 0) { pid++; }
                // pid++;
                pid = page_dst(gen);


                pool.give_work(read_func, std::ref(bp), std::ref(reader_count), wait, pid);
            } break;
            default:
                FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Shouldn't be here");
        }
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(200));

    bp.mu.lock();
    std::cout << std::flush << "Joining (" << num_ops << ") threads. Writers (" << writer_count << "), Readers (" << reader_count << ")\n";
    bp.mu.unlock();
}


void disk_test() {
    // basic_test();
    // write_correctness_test();
    
    for (int i = 0; i < 1000; i++) {
        // std::cout << "Loop count: " << loop_count++ << "\n";
        // sequential_test();
        thread_test();
    }
}

void write_correctness_test() { // Should create a file with 10 "hello world"s next to eachother
    constexpr int page_size  = 11;
    constexpr int num_writes = 10;
    constexpr int page_count = num_writes;
    const auto* const fp = "./Test/write_correctness.test";
    BufferPool bp(fp, page_size, page_count);

    auto write_lambda = [&](const page_id_t pid) {
        auto wpg_opt = bp.get_write_page_guard(pid);
        if (!wpg_opt.has_value()) {
            std::cerr << "Failed to get write page\n"; exit(1); }
        std::cout << "Got write page" << std::endl;
    
        WritePageGuard wpg = std::move(wpg_opt.value());
    
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


