#pragma once

#include "helpers.h"

#include <functional>
#include <iostream>
#include <mutex>
#include <thread>
#include <vector>
#include <deque>
#include <condition_variable>
#include <atomic>


template<typename Func, typename... Args>
struct Task {
    Func func;  // Function pointer or callable
    std::tuple<std::decay_t<Args>...> args;  // Arguments stored in tuple
    
    void execute() {
        std::apply(func, args);  // C++17: unpack tuple and call function
    }
};


// Check CLAUDE for allocator
// Also add allocator to BufferPool

// template<typename Allocator = std::allocator<std::byte>>
class ThreadPool {

    struct TaskWrapper {
        virtual ~TaskWrapper() = default;
        virtual void execute() = 0;
    };

    template<typename Func, typename... Args>
    struct TaskImpl : TaskWrapper {
        Func func;
        std::tuple<std::decay_t<Args>...> args;
        
        // Remove && from constructor - just take by value since we're storing anyway
        TaskImpl(Func f, std::decay_t<Args>... a) 
            : func(std::move(f))
            , args(std::move(a)...) {}
        
        void execute() override {
            std::apply(func, args);
        }
    };

    std::deque<std::unique_ptr<TaskWrapper>> work;
    
    std::vector<std::thread> workers;
    int num_workers;
    std::atomic<int> ready_workers;
    std::atomic<int> tasks_in_flight; 
    std::mutex mu;
    std::condition_variable work_cv;
    std::atomic<bool> should_stop{false};

    [[nodiscard]] auto get_work() noexcept -> std::optional<std::unique_ptr<TaskWrapper>> {
        // std::cout << "worker getting lock\n";
        std::unique_lock lock{mu};
        work_cv.wait(lock, [this]() { 
            return !work.empty() || should_stop.load(); 
        });
        // if (should_stop.load() == true) { return {nullptr, false}; } // If == true, die before all work is done. Ignore check == do work then die
        // std::cout << "worker getting work\n";
        if (!work.empty()) {
            auto task = std::move(work.front());
            work.pop_front();
            return std::move(task);
        }
        // std::cout << "worker failed to get work\n";
        return std::nullopt;
    }
    
    public:
    template<typename Func, typename... Args>
    void give_work(Func&& f, Args&&... args) {
        // std::cout << "giving work\n";
        while (ready_workers.load(std::memory_order_acquire) != num_workers) {
            std::this_thread::yield();
        }

        tasks_in_flight.fetch_add(1, std::memory_order_release);

        auto task = std::make_unique<TaskImpl<std::decay_t<Func>, std::decay_t<Args>...>>(
            std::forward<Func>(f), 
            std::forward<Args>(args)...
        );
        
        mu.lock();
        work.emplace_back(std::move(task));
        mu.unlock();
        work_cv.notify_one();
    }

    ThreadPool(const size_t num_workers) : num_workers(num_workers) {
        if (num_workers == 0) { throw std::runtime_error("ThreadPool: Number of workers must be > 0"); }
        workers.reserve(num_workers);
        for (size_t i = 0; i < num_workers; i++) {
            auto worker_lambda = [this]() {
                bool init = false;
                while (should_stop.load(std::memory_order_acquire) == false) {
                    // std::cout << "worker waiting for notification\n";
                    if (!init) { ready_workers.fetch_add(1, std::memory_order_relaxed); init = true; }
                    // Check for work //
                    if (auto task_opt = get_work(); task_opt.has_value()) {
                        // std::cout << "Got job\n";
                        try {
                            task_opt.value()->execute();
                        } catch (std::exception e) {
                            THREAD_PRINT(": exception while executing work from ThreadPool");
                        }

                        tasks_in_flight.fetch_sub(1, std::memory_order_acq_rel);
                    } else {
                        // std::cout << "no job\n";
                    }
                }
                // std::cout << "worker died\n";
            };
            workers.emplace_back(worker_lambda);
        }
    }

    // Obsurdly slow, much faster to just call dtor and recreate pool lol (~4-10x in tests when recreating many times, so performance in practice is probably a lot worse)
    void wait_until_idle() const noexcept {
        uint32_t backoff = 1024 * 64 * 64;
        const uint32_t max_backoff = 1'000'000; // 1ms worst-case
        while (tasks_in_flight.load(std::memory_order_acquire) != 0) {
            // std::this_thread::yield();
            std::this_thread::sleep_for(std::chrono::nanoseconds(backoff));
            backoff *= 2;
            backoff = std::min(backoff * 2, max_backoff);
        }
    }

    ~ThreadPool() {
        should_stop.store(true  );
        work_cv.notify_all();
        for (auto& worker : workers) {
            worker.join(); }
    }
};