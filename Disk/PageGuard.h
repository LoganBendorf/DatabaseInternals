#pragma once

#include "Page.h"
#include "macros.h"

#include <cstddef>
#include <string_view>
#include <functional>
#include <cstring>
#include <utility>

// Holds lock until dtor is called
class WritePageGuard {
    std::function<void(Page&)> release_func;
    bool valid;
    Page page;
    public:
    explicit WritePageGuard() noexcept : page(nullptr, 0, 0), valid(false) {}
    explicit WritePageGuard(std::function<void(Page&)> release_func, Page page) noexcept : release_func(std::move(release_func)), page(page), valid(true) {}

    // Disable copy ctor and copy assignment
    WritePageGuard(const WritePageGuard&) = delete;
    WritePageGuard& operator=(const WritePageGuard&) = delete;

    // Move Ctor
    WritePageGuard(WritePageGuard&& other) noexcept : release_func(std::move(other.release_func)), page(other.page), valid(other.valid) {
        other.valid = false;
    };

    // Move assignment
    WritePageGuard& operator=(WritePageGuard&& other) noexcept;

    ~WritePageGuard() noexcept;
    void release() noexcept;

    // Throw if not valid?
    void write(std::string_view msg, const size_t page_offset) {
        if (!valid) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("WritePageGuard::write(): Attempted to write to an invalid guard"); }
        if (page_offset >= page.page_size) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("WritePageGuard:write(): OOB offset (" + std::to_string(page_offset) + ") for page size (" + std::to_string(page.page_size) + ")"); }
        if (msg.size() + page_offset > page.page_size) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("WritePageGuard:write(): OOB write"); }
        std::memcpy(page.data + page_offset, msg.data(), msg.size());
    }
    
    [[nodiscard]] auto read() const -> std::string_view{
        if (!valid) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("WritePageGuard::read(): Attempted to read an invalid guard"); }
        return std::string_view{page.data, page.page_size};
    }

    [[nodiscard]] page_id_t pid() const { 
        if (!valid) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("WritePageGuard::pid(): Attempted to access pid of an invalid guard"); }
        return page.pid; 
    }
};

// Holds lock until dtor is called
class ReadPageGuard {
    std::function<void(Page&)> release_func;
    bool valid;
    Page page;
    public:
    explicit ReadPageGuard() noexcept : page(0, 0, 0), valid(false) {}
    explicit ReadPageGuard(std::function<void(Page&)> release_func, Page page) noexcept : release_func(std::move(release_func)), page(page), valid(true) {}

    // Disable copy ctor and copy assignment
    ReadPageGuard(const ReadPageGuard&) = delete;
    ReadPageGuard& operator=(const ReadPageGuard&) = delete;

    // Move Ctor
    ReadPageGuard(ReadPageGuard&& other) noexcept : release_func(std::move(other.release_func)), page(other.page), valid(other.valid) {
        other.valid = false;
    };

    // Move assignment
    ReadPageGuard& operator=(ReadPageGuard&& other) noexcept;

    ~ReadPageGuard() noexcept;
    void release() noexcept;

    // Throw if not valid?
    [[nodiscard]] auto read() const -> std::string_view{
        if (!valid) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("ReadPageGuard::read(): Attempted to read an invalid guard"); }
        return std::string_view{page.data, page.page_size};
    }

    [[nodiscard]] page_id_t pid() const { 
        if (!valid) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("readPageGuard::pid(): Attempted to access pid of an invalid guard"); }
        return page.pid; 
    }
};