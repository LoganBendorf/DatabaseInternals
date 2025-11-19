#pragma once

#include "structs_and_constants.h"

#include <string>

void clear_screen() __attribute__((used));

 [[nodiscard]] char* get_page(int index);

 [[nodiscard]] page_id_t allocate_page();

void deallocate_page(page_id_t pid);

 [[nodiscard]] std::string BPTreeNodeType_to_string(BPTreeNodeType type);

void g_print_bytes(const page_id_t pid, const size_t branching_factor) noexcept;
