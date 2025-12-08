

#include "helpers.h"
#include "bptree.h"

#include <cstddef>
#include <iostream>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>


std::mutex thread_log_mu;

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



#include <unistd.h>
#include <sys/syscall.h>
static pid_t get_kernel_thread_id() {
    return syscall(SYS_gettid);  // returns kernel thread ID
}


void thread_print(std::string msg) noexcept {
    static std::mutex mu;
    static int cid = 0;
    static std::unordered_map<std::__thread_id, int> tid_to_cid_map;
    const auto kernel_tid = get_kernel_thread_id();
    const auto cpp_tid = std::this_thread::get_id();
    std::lock_guard lock{mu};
    if (!tid_to_cid_map.contains(cpp_tid)) {
        tid_to_cid_map[cpp_tid] = cid++; }
    switch (tid_to_cid_map[cpp_tid]) {
        case 0:  std::cout << "Thread (" << ASCII_BLUE   << kernel_tid << ASCII_RESET << ") " << msg << "\n"; break;
        case 1:  std::cout << "Thread (" << ASCII_GREEN  << kernel_tid << ASCII_RESET << ") " << msg << "\n"; break;
        case 2:  std::cout << "Thread (" << ASCII_YELLOW << kernel_tid << ASCII_RESET << ") " << msg << "\n"; break;
        default: std::cout << "Thread (" << kernel_tid << ") " << msg << std::endl; break;
    }
}