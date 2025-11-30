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


// Also add allocator to BufferPool

template<typename Allocator = std::allocator<std::byte>>
class ThreadPool {

    struct TaskWrapper {
        virtual ~TaskWrapper() = default;
        virtual void execute() = 0;
        virtual void destroy_with_allocator(void* alloc_ptr) = 0;
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

        void destroy_with_allocator(void* alloc_ptr) override {
            using TaskAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<TaskImpl>;
            TaskAllocator* allocator = static_cast<TaskAllocator*>(alloc_ptr);
            std::allocator_traits<TaskAllocator>::destroy(*allocator, this);
            std::allocator_traits<TaskAllocator>::deallocate(*allocator, this, 1);
        }
    };

    struct TaskDeleter {
        void* allocator_ptr;
        void operator()(TaskWrapper* ptr) const {
            if (ptr) { ptr->destroy_with_allocator(allocator_ptr); }
        }
    };

    Allocator allocator;

    using TaskWrapperPtr = std::unique_ptr<TaskWrapper, std::function<void(TaskWrapper*)>>;
    using DequeAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<TaskWrapperPtr>;
    
    std::deque<TaskWrapperPtr, DequeAllocator> work;
    
    using WorkersAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<std::thread>;
    std::vector<std::thread, WorkersAllocator> workers;

    int num_workers;
    std::atomic<int> ready_workers;
    std::atomic<int> tasks_in_flight; 
    std::mutex mu;
    std::condition_variable work_cv;
    std::atomic<bool> should_stop{false};

    [[nodiscard]] auto get_work() noexcept -> std::optional<TaskWrapperPtr> {

        std::unique_lock lock{mu};
        work_cv.wait(lock, [this]() { 
            return !work.empty() || should_stop.load(); 
        });
        // if (should_stop.load() == true) { return {nullptr, false}; } // If == true, die before all work is done. Ignore check == do work then die

        if (!work.empty()) {
            auto task = std::move(work.front());
            work.pop_front();
            return std::move(task);
        }

        return std::nullopt;
    }
    
    public:
    template<typename Func, typename... Args>
    void give_work(Func&& f, Args&&... args) {

        while (ready_workers.load(std::memory_order_acquire) != num_workers) {
            std::this_thread::yield(); }
        tasks_in_flight.fetch_add(1, std::memory_order_release);

        using TaskType = TaskImpl<std::decay_t<Func>, std::decay_t<Args>...>;
        using TaskAllocator = typename std::allocator_traits<Allocator>::template rebind_alloc<TaskType>;
        
        // Allocate the task allocator on the stack or store it somewhere accessible
        thread_local static TaskAllocator task_alloc_storage{allocator};
        // task_alloc_storage = TaskAllocator(allocator);
        
        TaskType* raw_ptr = std::allocator_traits<TaskAllocator>::allocate(task_alloc_storage, 1);
        
        try {
            std::allocator_traits<TaskAllocator>::construct(
                task_alloc_storage, 
                raw_ptr,
                std::forward<Func>(f),
                std::forward<Args>(args)...
            );
        } catch (...) {
            std::allocator_traits<TaskAllocator>::deallocate(task_alloc_storage, raw_ptr, 1);
            throw;
        }
        
        TaskWrapperPtr task(raw_ptr, TaskDeleter{&task_alloc_storage});
        
        mu.lock();
        work.emplace_back(std::move(task));
        mu.unlock();
        work_cv.notify_one();
    }

    ThreadPool(const size_t num_workers, const Allocator& alloc = Allocator()) : num_workers(num_workers), allocator(alloc) {
        if (num_workers == 0) { throw std::runtime_error("ThreadPool: Number of workers must be > 0"); }
        workers.reserve(num_workers);

        for (size_t i = 0; i < num_workers; i++) {
            auto worker_lambda = [this]() {
                bool init = false;
                while (should_stop.load(std::memory_order_acquire) == false) {
                    if (!init) { ready_workers.fetch_add(1, std::memory_order_relaxed); init = true; }

                    // Check for work //
                    if (auto task_opt = get_work(); task_opt.has_value()) {
                        try {
                            task_opt.value()->execute();
                        } catch (std::exception e) {
                            THREAD_PRINT(": exception while executing work from ThreadPool");
                        }

                        tasks_in_flight.fetch_sub(1, std::memory_order_acq_rel);
                    } 
                }
                // std::cout << "worker died\n";
            };
            workers.emplace_back(worker_lambda);
        }
    }

    // PMR constructor overload
    explicit ThreadPool(const size_t num_workers, std::pmr::memory_resource* mr)
        requires std::is_same_v<Allocator, std::pmr::polymorphic_allocator<std::byte>>
        : ThreadPool(num_workers, std::pmr::polymorphic_allocator<std::byte>(mr))
    {}

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

// Convenience alias for PMR version
using PmrThreadPool = ThreadPool<std::pmr::polymorphic_allocator<std::byte>>;

ThreadPool(size_t, std::pmr::memory_resource*) 
    -> ThreadPool<std::pmr::polymorphic_allocator<std::byte>>;