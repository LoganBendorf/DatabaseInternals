

#include "helpers.h"
#include "bptree.h"

#include <cstddef>
#include <iostream>
#include <set>
#include <string>



void clear_screen() { // DEBUG
    std::cout << "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n" << std::endl;
}

std::string BPTreeNodeType_to_string(BPTreeNodeType type) {
    switch (type) {
        case INTERMEDIATE: return "INTERMEDIATE"; break;
        case BRANCH: return "BRANCH"; break;
        case LEAF: return "LEAF"; break;
        default: {
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Unknown type (" + std::to_string(type) + ")");
        }
    }
}

void g_print_bytes(const page_id_t pid, const size_t branching_factor) noexcept {
    BPTreeHeader header{G_PAGE_SIZE, branching_factor};
    BPTreeNode node{pid, header};
    node.print_bytes();
}


class PageAllocator {
    public:
    char* memslots;
    unsigned int n;
    unsigned int page_size;
    std::set<page_id_t> free_pages;
    explicit PageAllocator(char* memslots, unsigned int n, unsigned int page_size) : memslots(memslots), n(n), page_size(page_size) {
        assert(n > 2);
        for (unsigned int i = 2; i < n; i++) {
            free_pages.emplace(i);
        }
    }

    [[nodiscard]] char* get_page(int index) const {
        if (index >= n || index < 0) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Page OOB"); }
        char* const ret_val = memslots + (static_cast<ptrdiff_t>(index * page_size));
        return ret_val;
    }

    [[nodiscard]] page_id_t allocate_page() {
        if (free_pages.empty()) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("OOM"); }
        auto it = free_pages.begin();
        page_id_t pid = *it;
        free_pages.erase(it);
        return pid;
}

    void deallocate_page(page_id_t pid) {
        if (!(pid > 1)) {
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Tried to deallocate page (" + std::to_string(pid) + "). Bruh"); }
        if (free_pages.contains(pid)) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Double free"); }
        free_pages.emplace(pid);
    }
};

static char memslots[G_PAGE_SIZE * MAX_SLOTS];
static PageAllocator page_allocator{memslots, MAX_SLOTS, G_PAGE_SIZE};

char* get_page(int index) {
    return page_allocator.get_page(index);
}

page_id_t allocate_page() {
    return page_allocator.allocate_page();
}

void deallocate_page(page_id_t pid) {
    page_allocator.deallocate_page(pid);
}