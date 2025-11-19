
#include "ThreadPool.h"
#include <cassert>
#include <random>


std::atomic<int> count{0};

void job(const int time) {
    std::this_thread::sleep_for(std::chrono::milliseconds(time));
    count.fetch_add(1);
}

void thread_pool_test() {
    ThreadPool pool{10};
    std::random_device rd; // seed
    std::mt19937 gen(rd());
    std::uniform_int_distribution<unsigned int> timer_dst(0, 10); // range

    constexpr int num_jobs = 50;
    for (int i = 0; i < num_jobs; i++) {
        const int wait_time = timer_dst(gen);
        pool.give_work(job, wait_time);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    assert(count.load() == num_jobs);
}