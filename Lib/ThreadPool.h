#pragma once

#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <deque>
#include <condition_variable>
#include <atomic>

class ThreadPool {

    using work_func_t = std::function<void()>;
    
    std::vector<std::thread> workers;
    std::atomic<int> num_workers;
    std::atomic<int> ready_workers;
    std::deque<work_func_t> work;
    std::mutex mu;
    std::condition_variable work_cv;
    std::atomic<bool> should_stop{false};

    [[nodiscard]] auto get_work() noexcept -> std::pair<work_func_t, bool> {
        // std::cout << "worker getting lock\n";
        std::unique_lock lock{mu};
        work_cv.wait(lock, [this]() { 
            return !work.empty() || should_stop.load(); 
        });
        // if (should_stop.load() == true) { return {nullptr, false}; } // If == true, die before all work is done. Ignore check == do work then die
        // std::cout << "worker getting work\n";
        if (!work.empty()) {
            work_func_t f = std::move(work.front()); work.pop_front();
            // std::cout << "worker got work\n";
            return std::pair{std::move(f), true};
        }
            // std::cout << "worker failed to get work\n";
        return std::pair{nullptr, false};
    }
    
    public:
    template<typename Func, typename... Args>
    void give_work(Func&& f, Args&&... args) {
        // std::cout << "giving work\n";
        while (ready_workers.load(std::memory_order_acquire) != num_workers) {
            std::this_thread::yield();
        }
        mu.lock();
        work.emplace_back([f = std::forward<Func>(f), ...args = std::forward<Args>(args)]() mutable {
            f(args...);
        });
        mu.unlock();
        work_cv.notify_one();
        // std::cout << "gave work\n";
    }

    ThreadPool(const size_t num_workers) : num_workers(num_workers) {
        if (num_workers == 0) { throw std::runtime_error("ThreadPool: Number of workers must be > 0"); }
        workers.reserve(num_workers);
        for (size_t i = 0; i < num_workers; i++) {
            auto worker_lambda = [this]() {
                bool init = false;
                while (should_stop.load() == false) {
                    // std::cout << "worker waiting for notification\n";
                    if (!init) { ready_workers++; init = true; }
                    // Check for work //
                    if (auto p = get_work(); p.second) {
                        // std::cout << "Got job\n";
                        p.first();
                    } else {
                        // std::cout << "no job\n";
                    }
                }
                // std::cout << "worker died\n";
            };
            workers.emplace_back(worker_lambda);
        }
    }

    ~ThreadPool() {
        should_stop.store(true);
        work_cv.notify_all();
        for (auto& worker : workers) {
            worker.join(); }
    }
};