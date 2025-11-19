#pragma once

#include <cstddef>
#include <string_view>

using page_id_t = int;
using frame_id_t = int;

struct Page {
    char* data;
    size_t page_size;
    page_id_t pid;
    explicit Page(char* data, size_t page_size, page_id_t pid) noexcept : data(data), page_size(page_size), pid(pid) {}
};
