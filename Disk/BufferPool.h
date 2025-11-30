#pragma once

#include "helpers.h"
#include "structs_and_constants.h"
#include "macros.h"
#include "PageGuard.h"
#include "Page.h"

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <type_traits>
#include <concepts>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <optional>



class RAII_File {
    FILE* file;
    public:
    explicit RAII_File(FILE* file) : file(file) {}
    ~RAII_File() noexcept {
        if (file != nullptr) { fclose(file); }
    }

    void reassign(FILE* set_file) noexcept{
        if (file == set_file) { return; }
        if (file != nullptr) { fclose(file); }
        file = set_file; 
    }

    RAII_File& operator=(FILE* set_file) noexcept { 
        if (file == set_file) { return *this; }
        if (file != nullptr) { fclose(file); }
        file = set_file; 
        return *this;
    }

    bool operator==(const void*& ptr) const noexcept {
        return file == ptr;
    }
    // Implicit conversion
    operator FILE*() const noexcept { return file; }

    // Delete copy operations
    RAII_File(const RAII_File&) = delete;
    RAII_File& operator=(const RAII_File&) = delete;

    // Move constructor
    RAII_File(RAII_File&& other) noexcept : file(other.file) {
        other.file = nullptr;  // Transfer ownership
    }

    // Move assignment operator
    RAII_File& operator=(RAII_File&& other) noexcept {
        if (this == &other) { return *this; }
        
        if (file != nullptr) { fclose(file); }
    
        file = other.file;
        other.file = nullptr;
        
        return *this;
    }
};




enum PageGuardFailRC { ok, disk_error, page_in_use, bp_full };

template<typename T>
concept Allocator = requires(T& alloc, std::size_t n) {
    typename T::value_type;
    { alloc.allocate(n) } -> std::convertible_to<typename T::value_type*>;
    { alloc.deallocate(std::declval<typename T::value_type*>(), n) } -> std::same_as<void>;
};

template <Allocator alloc_t = std::allocator<char>>  
class BufferPool {

    enum AccessType { READ, WRITE };

    alloc_t allocator_{}; 
    using Traits = std::allocator_traits<alloc_t>;

    using frame_id_t = int;

    // Rebind allocators for each map's value type
    using FrameIDAllocator             = typename Traits::template rebind_alloc<frame_id_t>;
    using frame_accesses_Allocator     = typename Traits::template rebind_alloc<std::pair<const frame_id_t, unsigned int>>;
    using page_to_frame_map_Allocator  = typename Traits::template rebind_alloc<std::pair<const frame_id_t, page_id_t>>;
    using frame_to_page_map_Allocator  = typename Traits::template rebind_alloc<std::pair<const page_id_t, frame_id_t>>;


    const std::filesystem::path file_path;
    char* memory;
    const size_t page_size;
    const size_t page_count;

    std::unordered_set<frame_id_t, std::hash<frame_id_t>, std::equal_to<frame_id_t>, FrameIDAllocator> free_frames;
    std::unordered_map<frame_id_t, unsigned int, std::hash<unsigned int>, std::equal_to<unsigned int>, frame_accesses_Allocator> frame_accesses; // Todo: Decrement when removing page
    std::unordered_map<frame_id_t, page_id_t, std::hash<frame_id_t>, std::equal_to<frame_id_t>, page_to_frame_map_Allocator> frame_to_page_map;
    std::unordered_map<page_id_t, frame_id_t, std::hash<page_id_t>, std::equal_to<page_id_t>, frame_to_page_map_Allocator> page_to_frame_map;
    public:
    std::mutex mu;
    private:


    void sanity_check(std::unique_lock<std::mutex>& bp_lock) {
        STACK_TRACE_ASSERT(bp_lock.owns_lock());
        std::unordered_set<page_id_t> unique_pages;
        for (const auto& p : frame_to_page_map) {
            const page_id_t pid = p.second;
            if (unique_pages.contains(pid)) {
                FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("BP.frame_to_page_map contained multiple page to frame mappings. Supposed to be unique. i.e. 1 page -> 1 frame. Found n pages -> 1 frame");
            }
            unique_pages.emplace(pid);
        }
    }

    static constexpr unsigned int k = 2;

    class FrameLock {
        using shared_mutexAllocator = typename Traits::template rebind_alloc<std::shared_mutex>;
        using AtomicIntAllocator    = typename Traits::template rebind_alloc<std::atomic<int>>;

        BufferPool& bp;
        std::vector<std::shared_mutex, shared_mutexAllocator> frame_mu;        // Frame -> Read mutex
        std::vector<std::atomic<int>, AtomicIntAllocator> write_requests; // So the retard doesn't free an inuse frame AND so lock doesn't block
        std::vector<std::atomic<int>, AtomicIntAllocator> read_requests; // So the retard doesn't free an inuse frame AND so lock doesn't block

        public:
        explicit FrameLock(BufferPool& bp) : bp(bp), frame_mu(bp.page_count), write_requests(bp.page_count), read_requests(bp.page_count) {}

        void write_lock_frame(const frame_id_t frame, std::unique_lock<std::mutex>& bp_lock) {
            STACK_TRACE_ASSERT(bp_lock.owns_lock());
            write_requests[frame].fetch_add(1);
            bp_lock.unlock();
            frame_mu[frame].lock(); // Might need to be a try_lock() in the future and let the called deal with it
            bp_lock.lock();
            write_requests[frame].fetch_sub(1);
        }

        void read_lock_frame(const frame_id_t frame, std::unique_lock<std::mutex>& bp_lock) {
            STACK_TRACE_ASSERT(bp_lock.owns_lock());
            read_requests[frame].fetch_add(1);
            bp_lock.unlock();
            frame_mu[frame].lock_shared(); // Might need to be a try_lock() in the future and let the called deal with it
            bp_lock.lock();
            read_requests[frame].fetch_sub(1);
        }

        void lock_frame(const frame_id_t frame, const AccessType access_type, std::unique_lock<std::mutex>& bp_lock) {
            switch (access_type) {
                case READ:  read_lock_frame(frame, bp_lock);  break;
                case WRITE: write_lock_frame(frame, bp_lock); break;
            }  
        }

        void write_unlock_frame(const frame_id_t frame) {
            frame_mu[frame].unlock();
        }

        void read_unlock_frame(const frame_id_t frame) {
            frame_mu[frame].unlock_shared();
        }

        void unlock_frame(const frame_id_t frame, const AccessType access_type) {
            switch (access_type) {
                case READ:  read_unlock_frame(frame);  break;
                case WRITE: write_unlock_frame(frame); break;
            }  
        }

        [[nodiscard]] auto is_locked(const frame_id_t frame) -> bool {
            if (write_requests[frame].load() != 0 || read_requests[frame].load() != 0) {
                return true;
            }

            if (frame_mu[frame].try_lock()) {
                frame_mu[frame].unlock();
                return false;
            } else {
                return true;
            }
        }
    };
    FrameLock frame_lock;


    public:
    explicit BufferPool(const std::filesystem::path file_path, const size_t page_size, const size_t page_count, alloc_t allocator = alloc_t{}) 
        : allocator_(allocator), file_path(file_path), memory(Traits::allocate(allocator_, page_size * page_count)), page_size(page_size), page_count(page_count), frame_lock(*this),
        free_frames(page_count, allocator), frame_accesses(page_count, allocator), page_to_frame_map(allocator), frame_to_page_map(page_count, allocator) 
        {
            for (size_t i = 0; i < page_count; i++) {
                free_frames.emplace(i);
                frame_accesses.insert_or_assign(i, 0);
            }
        }

    ~BufferPool() {
        if (memory != nullptr) {
            std::allocator_traits<alloc_t>::deallocate(allocator_, memory, page_size * page_count);
        }
    }

    BufferPool(const BufferPool&) = delete;            // delete copy ctor
    BufferPool& operator=(const BufferPool&) = delete; // delete copy assignment
    BufferPool(BufferPool&&) = delete;                 // delete move ctor
    BufferPool& operator=(BufferPool&&) = delete;      // delete move assignment

    public:

    auto disk_write(const Page page, std::unique_lock<std::mutex>& bp_lock) -> bool { // Lock must be held
        STACK_TRACE_ASSERT(bp_lock.owns_lock());

        RAII_File file = RAII_File{fopen(file_path.c_str(), "rb+")};
        if (file == nullptr) { return false; }
        
        const unsigned int file_offset = page.pid * page_size;
        if (fseek(file, file_offset, SEEK_SET) != 0) {
            perror("fseek failed");
            return false;
        }

        const size_t n = fwrite(page.data, page.page_size, 1, file);
        STACK_TRACE_EXPECT(1, n);
        
        return true;
    }

    void increment_frame_accesses(const frame_id_t fid) { // Lock must be held.
        auto it = frame_accesses.find(fid);
        if (it == frame_accesses.end()) {
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("All frames should have a play in the frame access map"); }
        const unsigned int cur = it->second;
        frame_accesses.insert_or_assign(fid, (cur + 1) % k);
    }

    [[nodiscard]] auto evict(std::unique_lock<std::mutex>& bp_lock) -> bool {
        STACK_TRACE_ASSERT(bp_lock.owns_lock());

        // K, frame
        std::priority_queue<std::pair<int, frame_id_t>, std::vector<std::pair<int, frame_id_t>>, std::greater<>> q; // Sorts ascending [1, 2, 3...]

        for (auto& p : frame_accesses) { // Loop over frames instead maybe?
            const frame_id_t frame = p.first;
            const int k = p.second;
            if (frame_lock.is_locked(frame)) { continue; }
            q.emplace(k, frame);
        }
        
        if (!q.empty()) {
            const auto p = q.top();
            const frame_id_t frame = p.second;
            
            // Remove BP state
            free_frames.emplace(frame);
            const page_id_t cur_pid = frame_to_page_map[frame];
            THREAD_PRINT("evicting pid (" + std::to_string(cur_pid) + ", frame (" + std::to_string(frame) + ")");
            page_to_frame_map.erase(cur_pid);
            frame_to_page_map.erase(frame);
            frame_accesses.insert_or_assign(frame, 0);
            return true;
        } else {
            return false;
        }
    }

    void deallocate_page(const page_id_t pid, const AccessType access_type, std::unique_lock<std::mutex>& lock) {
        STACK_TRACE_ASSERT(lock.owns_lock());
        // std::cout << "Deallocating pid (" << pid << ", access type (" << (access_type == WRITE ? "WRITE" : "READ") << ")" << std::endl;

        auto frame_it = page_to_frame_map.find(pid);
        if (frame_it == page_to_frame_map.end()) {
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Tried to deallocate page with no associated frame"); }
        const frame_id_t frame = frame_it->second;
        lock.unlock();
        
        frame_lock.unlock_frame(frame, access_type);
        // std::cout << "Successfully deallocated pid (" << pid << ", access type (" << (access_type == WRITE ? "WRITE" : "READ") << ")" << std::endl;
    }

    [[nodiscard]] auto disk_read(const page_id_t pid, const frame_id_t frame, std::unique_lock<std::mutex>& lock) -> bool { // Must be called with lock held
        STACK_TRACE_ASSERT(lock.owns_lock());

        RAII_File file{fopen(file_path.c_str(), "rb+")};
        if (file == nullptr) {
            // File doesn't exist, create it
            file.reassign(fopen(file_path.c_str(), "wb+"));
            if (file == nullptr) {
                perror("fopen");
                return false;
            }
        }

        // Move to offset
        const int file_offset = pid * page_size;
        if (fseek(file, file_offset, SEEK_SET) != 0) {
            perror("fseek failed");
            return false;
        }

        const int bp_memory_offset = frame * page_size;
        char* bp_memory_location = memory + bp_memory_offset;
        
        const size_t bytes_read = fread(bp_memory_location, 1, page_size, file);
        // std::cout << "Read " << bytes_read << " bytes: " << std::string(bp_memory_location, bytes_read) << "\n";
        if (ferror(file) != 0) {
            perror("fread");
            return false;
        }

        return true;
    }

    // So all pages map to the same frame, so different threads don't alloc the same pid to a thousand different frames
    using frame_requests_set_Allocator  = typename Traits::template rebind_alloc<page_id_t>;
    std::unordered_set<page_id_t, std::hash<page_id_t>, std::equal_to<page_id_t>, frame_requests_set_Allocator> frame_requests;

    [[nodiscard]] auto get_frame(const page_id_t pid, AccessType access_type, std::unique_lock<std::mutex>& bp_lock) -> std::pair<frame_id_t, PageGuardFailRC> {
        STACK_TRACE_ASSERT(bp_lock.owns_lock());

        START:

        auto frame_it = page_to_frame_map.find(pid);
        // In memory
        if (frame_it != page_to_frame_map.end()) {
            const frame_id_t frame = frame_it->second;
            frame_lock.lock_frame(frame, access_type, bp_lock);

            STACK_TRACE_ASSERT(page_to_frame_map.find(pid)!= page_to_frame_map.end());
            return {frame, ok};
        }
        
        // Not in memory, make request

        // Someone already made the request
        if (frame_requests.contains(pid) ) {

            bp_lock.unlock();
            uint32_t backoff = 512;
            constexpr uint32_t max_backoff = 1'000'000; // 1ms worst-case
            while (true) {
                bp_lock.lock();
                if (!frame_requests.contains(pid)) { break; }
                bp_lock.unlock();
                // std::this_thread::yield();
                backoff *= 2;
                backoff = std::min(backoff * 2, max_backoff);
            }
            STACK_TRACE_ASSERT(bp_lock.owns_lock());
            goto START;
        } else { // Make the request
            
            if (free_frames.empty()) { // Evict if full
                const bool ok = evict(bp_lock);
                if (!ok) { return {{}, bp_full}; }
            }
            STACK_TRACE_ASSERT(!free_frames.empty());
            
            const frame_id_t frame = *free_frames.begin();
            free_frames.erase(frame);

            // Lock
            frame_requests.emplace(pid);
            frame_lock.lock_frame(frame, access_type, bp_lock); // Only time bp_lock unlocks
            STACK_TRACE_ASSERT(bp_lock.owns_lock());

            // Disk read
            const bool ok = disk_read(pid, frame, bp_lock);
            if (!ok) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("DISK READ FAILED"); frame_requests.erase(pid); return {{}, disk_error}; }
            // Add state to BP
            page_to_frame_map.emplace(pid, frame);
            frame_to_page_map.emplace(frame, pid);
            frame_requests.erase(pid);

            return {frame, PageGuardFailRC::ok};
        }
    }

    void write_unlock(Page page) noexcept { // Always assumes dirty
        std::unique_lock<std::mutex> lock(mu);
        disk_write(page, lock);
        deallocate_page(page.pid, WRITE, lock);
    }

    void read_unlock(Page page) noexcept {
        std::unique_lock<std::mutex> lock(mu);
        deallocate_page(page.pid, READ, lock);
    }


    // If guards are acquired in 2 different directions by different threads, it will deadlock
    //  i.e. thread 1 acquires pid 0 then pid 1, while thread 2 acquires pid 1 then pid 0
    //  The sequence must be the same even if more locks are held, i.e. acquire pids 0, 2, 5, must release in the order 0, 2, 5. Quite tricky with multiple threads
    //  Not much of a problem with B+-Tree because you always acquire them going down, or sideways. Only a problem if you do something dumb (probably)
    // Also if multiple write guards are taken out by the same thread it will deadlock

    // If threads only check out 2 pages in a strictly increasing order and release them in acquisition order, it will never deadlock
    //  This is not true with num threads >= 3. i.e. thread (1) 2, 5, 7; thread (2) 5, 7, 8; thread (3) 7, 8, 9; can deadlock. That might not be the right example idk but 3 threads do deadlock when 2 don't
    //  Might take a very very long time

    [[nodiscard]] auto get_write_page_guard(const page_id_t pid) -> std::pair<WritePageGuard, PageGuardFailRC> {
        std::unique_lock bp_lock(mu);

        const auto [frame, rc] = get_frame(pid, WRITE, bp_lock);
        if (rc != ok) { return {WritePageGuard{}, rc}; }

        increment_frame_accesses(frame);

        Page page{memory + page_size * frame, page_size, pid};
        sanity_check(bp_lock);
        return {WritePageGuard{ [this](Page p) { this->write_unlock(p); }, page}, ok};
    }

    [[nodiscard]] auto get_read_page_guard(const page_id_t pid) -> std::pair<ReadPageGuard, PageGuardFailRC> {
        std::unique_lock bp_lock(mu);

        const auto [frame, rc] = get_frame(pid, READ, bp_lock);
        if (rc != ok) { return {ReadPageGuard{}, rc}; }

        increment_frame_accesses(frame);

        Page page{memory + page_size * frame, page_size, pid};
        sanity_check(bp_lock);
        return {ReadPageGuard{ [this](Page p) { this->read_unlock(p); }, page}, ok};
    }
};

// Convenience alias for PMR version
using PMRBufferPool = BufferPool<std::pmr::polymorphic_allocator<char>>;

BufferPool(std::filesystem::path, size_t, size_t, std::pmr::memory_resource*) 
    -> BufferPool<std::pmr::polymorphic_allocator<std::byte>>;