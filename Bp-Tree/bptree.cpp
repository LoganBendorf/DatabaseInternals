
#include <algorithm>
#include <cassert>
#include <climits>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <expected>
#include <iostream>
#include <sstream>
#include <vector>
#include <unordered_set>
#include <deque>
#include <map>
#include <set>

#include "macros.h"
#include "bptree.h"
#include "helpers.h"

/////////// TODO ////////////
//
// 1. Store page size and branching factor in page 0
// 2. Add delete
// 3. Add update?
// 4. Overflow pages
//
/////////////////////////////



static constexpr std::string ASCII_BLACK = "\033[30m";
static constexpr std::string ASCII_GREEN = "\033[32m";
static constexpr std::string ASCII_RESET  = "\033[0m";

void BPTreeNode::print_bytes() const noexcept {
    const auto type       = header.get_type();
    const auto n          = header.get_n();
    const auto num_free   = header.get_num_free();
    const auto free_start = type == LEAF ? header.get_free_start() : header.get_free_start_noexcept();
    const auto num_frag = header.get_num_fragmented();
    const auto left_sib   = header.get_left_sibling();
    const auto right_sib  = header.get_right_sibling();
    const auto overflow = type == LEAF ? header.get_next_overflow() : header.get_next_overflow_noexcept();

    std::cout << "Printing bytes for pid: " << page_id << ", ";
    std::cout << "Type: " << BPTreeNodeType_to_string(type) << ", n: " << n << ", number of free slots: " << num_free << ", " ;
    if (type == LEAF) {
        std::cout << ASCII_GREEN << "free space start/offset: " << free_start;
    } else if (type == BRANCH) {
        std::cout << "child pid: " << free_start;
    } else if (type == INTERMEDIATE) {
        std::cout << ASCII_BLACK << "UNUSED: " << free_start;
    }
    std::cout << ASCII_RESET << ", number of bytes fragmented: " << num_frag << ", left sibling: " << left_sib << ", right sibling: " << right_sib << ", overflow: " << overflow << "\n    ";

    unsigned short next_freeblock = free_start;
    int count = 0;
    const int max_chars_per_line = 60;
    char* i = type == LEAF ? header.get_records_begin() : header.get_char_keys_begin();
    int cur_chars = 0;
    const unsigned int PAGE_SIZE = tree_header.get_page_size();
    while (i < (data + PAGE_SIZE)) {
        char c = *i;
        std::byte b = static_cast<std::byte>(c);

        const int index = i - data;
        if (type == LEAF && index == next_freeblock) { // Color starting free block
            std::memcpy(&next_freeblock, i, sizeof(unsigned short)); // Currently i is pointing to freeblock{int next_offset, int size}
            for (int j = 0; j < FREEBLOCK_SIZE; j++) {
                c = *i;
                i++;
                b = static_cast<std::byte>(c);
                std::cout << ASCII_GREEN << std::to_integer<int>(b) << ' ' << ASCII_RESET;
            }
        } else {
            std::cout << std::to_integer<int>(b) << ' ';
            i++;
        }

        if (cur_chars++ == max_chars_per_line) {
            cur_chars = 0;
            std::cout << "\n    ";
        }
    }
    std::cout << "\n";
}

void BPTreeNode::print_bytes(const page_id_t pid, const size_t page_size) noexcept {
    const int dummy_branching_factor = 3;
    BPTreeHeader header{page_size, dummy_branching_factor};
    BPTreeNode dummy_node{pid, header};
    dummy_node.print_bytes();
}

void BPTreeNode::print_bytes(const page_id_t pid) noexcept {
    print_bytes(pid, G_PAGE_SIZE);
}



void BPTreeNode::wipe_clean() noexcept { std::memset(data, 0, tree_header.get_page_size()); }

static void write_freeblock(char* const slot, FreeBlock freeblock) noexcept {
    std::memcpy(slot, &freeblock, FREEBLOCK_SIZE);
}


void BPTreeNode::write_freeblock(int offset, FreeBlock freeblock) noexcept {
    std::memcpy(data + offset, &freeblock, FREEBLOCK_SIZE);
}


auto BPTreeNode::get_bytes_used() const noexcept -> int { return get_bytes_used(header.get_n());}

auto BPTreeNode::get_bytes_used(const int n) const noexcept -> int {
    const auto type = header.get_type();
    const int header_size = header.get_header_size();
    if (type == LEAF) {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Shouldn't use as LEAF!");
        return 1;
    } else if (type == BRANCH) {
        const int key_bytes    = n * sizeof(int);
        const int c_pid_bytes  = n * sizeof(int);
        const int offset_bytes = n * sizeof(int);
        return header_size + key_bytes + c_pid_bytes + offset_bytes;
    } else if (type == INTERMEDIATE) {
        const int key_bytes    = n == 0 ? 0 : (n-1) * sizeof(int);
        const int c_pid_bytes  = n * sizeof(int);
        return header_size + key_bytes + c_pid_bytes;
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("TODO");
        return 0;
    }
}

auto BPTreeNode::is_full() const noexcept -> NodeFullStatus { 
    const int n = header.get_n();
    return is_full(n);
}

auto BPTreeNode::is_full(const int n) const noexcept -> NodeFullStatus { 
    const int branching_factor = tree_header.get_branching_factor();
    if (n == branching_factor) { return AT_CAPACITY;   } 
    if (n >  branching_factor) { return PAST_CAPACITY; } 
    
    const auto type = header.get_type();
    int minimum_space = 0;
    if (type == INTERMEDIATE) {
        minimum_space = sizeof(int) * 2; // [Key, page_id]
    } else if (type == BRANCH) {
        minimum_space = sizeof(int) * 3; // [Key, page_id, offset]
    } else if (type == LEAF) {
        return NOT_FULL;
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("TODO");
    }
    
    const int page_size = tree_header.get_page_size();
    assert(minimum_space < page_size); // Bad! Should never have allowed the tree to be constructed
    const int bytes_left = page_size - get_bytes_used();
    if (bytes_left < minimum_space) { return BYTES_FULL; }

    return NOT_FULL;
}

auto BPTreeNode::get_record_from_offset(const int offset) const noexcept -> char* { return data + offset; }

void BPTreeNode::print_leaf(const int indent, std::queue<int>& offsets) const noexcept {
    assert(header.get_type() == LEAF);
    const int n = header.get_n();
    
    // Print header
    for (int i = 0; i < indent; i++) { std::cout << "  "; }
    std::cout << " { ";
    std::cout << "LEAF, pid: " << page_id << ", n: " << n << ", records: [";
    
    // Print keys
    bool first = true;
    for (int i = 0; i < n; i++) { 
        if (!first) { std::cout << ", "; }
        const int offset = offsets.front(); offsets.pop();
        const Record record{data + offset};
        std::cout << record;
        first = false;
    }
    std::cout << "] }"; 
}

#include <csignal>

void BPTreeNode::print_branch(const int indent) const noexcept {
    assert(header.get_type() == BRANCH);
    const int n = header.get_n();
    
    // Print header
    for (int i = 0; i < indent; i++) { std::cout << "  "; }
    std::cout << "BRANCH, pid: " << page_id << ", n: " << n << ", (key, pid, off): [";
    
    // Print keys
    bool first = true;
    int* const keys_begin = header.get_int_keys_begin();
    std::set<page_id_t> c_pids;
    std::queue<int> offsets;
    for (int i = 0; i < n; i++) { 
        if (!first) { std::cout << ", "; }
        const int index = i * 3; // [key, pid, offset]
        const int key = keys_begin[index]; 
        const int pid = keys_begin[index + 1];
        const int off = keys_begin[index + 2];
        c_pids.emplace(pid);
        offsets.emplace(off);
        std::cout << "(" << key << ", " << pid << ", " << off << ")";
        first = false;
    }

    // Print LEAF(s)
    for (const page_id_t pid : c_pids) {
        const BPTreeNode leaf{pid, tree_header};
        const auto child_type = leaf.header.get_type();
        if (child_type != LEAF) {
            std::cout << "\nFound child with type: " << BPTreeNodeType_to_string(child_type) << std::endl;
            leaf.print_bytes();
            raise(SIGTRAP); 
        }
        leaf.print_leaf(0, offsets);
    }
    STACK_TRACE_EXPECT(0, offsets.size());

    std::cout << "]\n";        
}

void BPTreeNode::print_intermediate(const int indent) const noexcept {
    const unsigned int n = header.get_n();

    // Print header
    for (int i = 0; i < indent; i++) { std::cout << "  "; }
    std::cout << "INTERMEDIATE, pid: " << page_id << ", n: " << n << ", keys: [";
    
    // Print keys
    bool first = true;
    int* const keys_begin = header.get_int_keys_begin();
    for (int i = 0; i < n - 1; i++) {
        if (!first) { std::cout << ", "; }
        const int key = keys_begin[i];
        std::cout << key;
        first = false;
    }
    std::cout << "]\n";
    
    // Recurse and print children
    for (int i = 0; i < n; i++) {
        const int index = i * 2;
        const page_id_t c_pid = index_page_back(i);
        assert(c_pid != 0);
        BPTreeNode node{c_pid, tree_header};
        node.print_inorder(indent + 1);
    }
    // std::cout << std::endl;
}

void BPTreeNode::print_inorder(const int indent) const noexcept {
    const BPTreeNodeType type = header.get_type();
    if (type == INTERMEDIATE) { 
        print_intermediate(indent);
    } else if (type == BRANCH) {
        print_branch(indent);
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("IDK, call parent instead bruh");
    }
}

void BPTreeNode::sort_keys() noexcept {
    const int  n = header.get_n();
    if (n == 0) { return; }
    const auto type = header.get_type();
    if (type == INTERMEDIATE) {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("sort_keys(): TODO: Add sort for INTERMEDIATE nodes");
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("sort_keys(): TODO: Add sort for non-INTERMEDIATE nodes");
    }
}

void BPTreeNode::sort_branch() {
    const int n = header.get_n();
    const BPTreeNodeType type = header.get_type();
    if (type != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Tried to insert record into non-branch"); }
    
    // Put in vec //
    int* const keys_begin = header.get_int_keys_begin();
    std::vector<std::tuple<int, int, int>> vec;
    vec.reserve(n);
    for (int i = 0; i < n; i++) {
        const int index = i * 3;
        const int key = keys_begin[index];
        const int pid = keys_begin[index + 1];
        const int off = keys_begin[index + 2];
        vec.emplace_back(key, pid, off);
    }

    // Sort vec based on key //
    std::sort(vec.begin(), vec.end(),
        [](const auto& a, const auto& b) {
            return std::get<0>(a) < std::get<0>(b);
        }
    );

    // Write back //
    for (int i = 0; i < n; i++) {
        const auto& tup = vec[i];
        const int index = i * 3;
        keys_begin[index] = std::get<0>(tup);
        keys_begin[index + 1] = std::get<1>(tup);
        keys_begin[index + 2] = std::get<2>(tup);
    }
}

void BPTreeNode::insert_into_branch(const int key, const page_id_t c_pid, const Record record) {
    const int n = header.get_n();
    const BPTreeNodeType type = header.get_type();

    if (type != BRANCH)             { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Tried to insert record into non-branch"); }
    if (is_full() == PAST_CAPACITY) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Tried to insert record into node that was past capacity"); }
    if (is_full() == BYTES_FULL)    { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Can't deal with byte overflow yet, so die instead"); }
    
    int* const keys_begin             = header.get_int_keys_begin();
    int* const key_location           = keys_begin + static_cast<ptrdiff_t>(n * 3);
    int* const pid_location           = key_location + 1;
    int* const record_offset_location = key_location + 2;

    // Update header
    header.set_n(n+1);

    // Update Key, child_pid, offset
    *key_location = key;

    STACK_TRACE_ASSERT(c_pid != 0);
    
    // Insert into leaf
    BPTreeNode child{c_pid, tree_header};
    STACK_TRACE_ASSERT(child.is_full() == NOT_FULL);
    STACK_TRACE_ASSERT(child.header.get_type() == LEAF);
    const auto [record_offset, pid] = child.insert_into_leaf(record);
    *pid_location = pid;
    *record_offset_location = record_offset;

    sort_branch();
}

auto BPTreeNode::allocate_leaf() const -> BPTreeNode {
    const page_id_t pid = allocate_page();
    BPTreeNode leaf{pid, tree_header};
    leaf.wipe_clean();
    leaf.header.set_n(0);
    leaf.header.set_type(LEAF);
    leaf.header.set_num_free(1);
    const unsigned short start = leaf.header.get_records_begin() - leaf.data;
    leaf.header.set_free_start(start);
    
    assert(tree_header.get_page_size() <= USHRT_MAX); // TODO: Handle bigger page sizes later 
    const unsigned short page_size = tree_header.get_page_size();
    const unsigned short size      = page_size - start;
    assert(page_size > start);
    FreeBlock freeblock{0, size};
    leaf.write_freeblock(start, freeblock);

    return leaf;
}

void BPTreeNode::insert_into_branch(const int key, const Record record) {
    const int n = header.get_n();
    const BPTreeNodeType type = header.get_type();

    if (type != BRANCH)             { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Tried to insert record into non-branch"); }
    if (is_full() == PAST_CAPACITY) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Tried to insert record into node that was past capacity"); }
    if (is_full() == BYTES_FULL)    { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_branch(): Can't deal with byte overflow yet, so die instead"); }
    
    int* const keys_begin             = header.get_int_keys_begin();
    int* const key_location           = keys_begin + static_cast<ptrdiff_t>(n * 3);
    int* const pid_location     = key_location + 1;
    int* const record_offset_location = key_location + 2;

    if (n == 0) {
        *key_location = key;
        BPTreeNode child = allocate_leaf();
        const auto [record_offset, pid] = child.insert_into_leaf(record);
        *pid_location = pid;
        *record_offset_location = record_offset;
        header.set_n(n+1);
        header.set_c_pid(child.page_id);
        return;
    }

    // Update header
    header.set_n(n+1);

    // Update Key, child_pid, offset
    *key_location = key;
    const int c_pid = header.get_c_pid();
    
    // Insert into leaf
    BPTreeNode child{c_pid, tree_header};
    assert(child.is_full() == NOT_FULL);
    assert(child.header.get_type() == LEAF);
    const auto [record_offset, pid] = child.insert_into_leaf(record);
    *pid_location = pid;
    *record_offset_location = record_offset;

    sort_branch();
}

void BPTreeNode::update_branch(const int key, const Record record) {
    const int n = header.get_n();
    const BPTreeNodeType type = header.get_type();

    if (type != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("update_branch(): Tried to update non-branch"); }

    int* const keys_begin = header.get_int_keys_begin();
    
    // Get offset and child pid //
    int offset = 0;
    page_id_t c_pid = 0;
    int i = 0;
    bool found = false;
    while (i < n) {
        const int  index = i * 3;
        const int  k     = keys_begin[index];
        const int  pid   = keys_begin[index + 1];
        const int  off   = keys_begin[index + 2];
        offset = off;
        c_pid  = pid;
        if (key == k) { found = true; break; }
        i++;
    }
    STACK_TRACE_ASSERT(c_pid != 0);
    assert(found == true);

    // Update leaf //
    BPTreeNode child{c_pid, tree_header};
    assert(child.header.get_type() == LEAF);
    std::deque<page_id_t> path{page_id};
    child.update_leaf(path, key, offset, record);
}

void BPTreeNode::delete_from_intemediate(const int key) {
    const int n = header.get_n();
    assert(header.get_type() == INTERMEDIATE);

    int* const keys_begin = header.get_int_keys_begin();
    int i = 0;
    bool found = false;
    while (i < n - 1) {
        const int k = keys_begin[i];
        if (key == k) { found = true; break; }
        i++;
    }
    assert(found == true);

    for (int j = i; j < n - 1; j++) {
        keys_begin[j] = keys_begin[j + 1];
    }

    int* const page_back = offset_page_back(i);
    for (int j = 0; j < i; j++) {
        page_back[j] = page_back[j - 1];
    }

    header.set_n(n-1);
}

// Don't touch parent
void BPTreeNode::delete_branch_node() {
    const int n = header.get_n();
    int* const keys_begin = header.get_int_keys_begin();
    std::vector<page_id_t> child_pids;
    for (int i = 0; i < n; i++) {
        const int index = i * 3;
        const int pid = keys_begin[index + 1];
        child_pids.emplace_back(pid);
    }
    for (const page_id_t pid : child_pids) {
        deallocate_page(pid);
    }
    deallocate_page(page_id);
}

auto BPTreeNode::branch_merge(std::deque<page_id_t>& path) -> bool {
    const int n = header.get_n();
    const int branching_factor = tree_header.get_branching_factor();
    assert(page_id != 1);
    if (header.get_type() != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("branch_merge(): Tried to merge non-branch"); }

    
    const page_id_t parent_pid    = path.front();
    const page_id_t left_sib_pid  = header.get_left_sibling();
    const page_id_t right_sib_pid = header.get_right_sibling();

    static std::vector<page_id_t> sibs(2); 
    sibs[0] = left_sib_pid; sibs[1] = right_sib_pid;
    for (const page_id_t sib_pid : sibs) {
        if (sib_pid != 0) {
            BPTreeNode sib{sib_pid, tree_header};
            if (sib.header.get_n() > branching_factor / 2) { continue; }
            const int old_sib_min_key = sib.header.get_int_keys_begin()[0]; // For deletes later
            // merge into neighbors
            int* const keys_begin = header.get_int_keys_begin();
            assert(sib.header.get_type() == BRANCH);
            for (int i = 0; i < n; i++) {
                const int index = i * 3;
                const int key  = keys_begin[index];
                const int pid  = keys_begin[index + 1];
                const int off  = keys_begin[index + 2];
                BPTreeNode leaf{pid, tree_header};
                Record* record = (Record*) leaf.data + off;
                sib.insert_into_branch(key, *record);
            }

            // Update parent //
            if (parent_pid != 0) {
                BPTreeNode parent{parent_pid, tree_header};
                assert(parent.header.get_type() == INTERMEDIATE);
                const int cur_min_key = keys_begin[0];
                parent.delete_from_intemediate(old_sib_min_key);
                parent.delete_from_intemediate(cur_min_key);
                const int new_sib_min_key = sib.header.get_int_keys_begin()[0]; // For deletes later
                parent.insert_into_intermediate(new_sib_min_key, sib_pid);
            }

            delete_branch_node(); // Delete cur and leaf nodes
            return true;
        }
    }
    return false;
}

// Take from neighbors
auto BPTreeNode::branch_redistribute() -> bool {
    const int n = header.get_n();
    const int branching_factor = tree_header.get_branching_factor();
    assert(page_id != 1);
    if (header.get_type() != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("branch_redistribute(): Tried to redistribute non-branch"); }

    const page_id_t left_sib  = header.get_left_sibling();
    const page_id_t right_sib = header.get_right_sibling();

    static std::vector<page_id_t> sibs(2);
    sibs[0] = left_sib;
    sibs[1] = right_sib;
    for (const page_id_t sib_pid : sibs) {
        if (sib_pid != 0) {
            BPTreeNode sib{sib_pid, tree_header};
            if (sib.header.get_n() <= branching_factor / 2) { continue; }
            // take key somehow idk
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("TODO");
            return true;
        }
    }
    return false;
}

void BPTreeNode::delete_from_branch(std::deque<page_id_t>& path, const int key) {
    const int n = header.get_n();
    const BPTreeNodeType type = header.get_type();

    if (type != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("delete_from_branch(): Tried to delte from non-branch"); }
    assert(n != 0);

    int* const keys_begin = header.get_int_keys_begin();
    
    // Get offset and child pid
    int offset = 0;
    page_id_t c_pid = 0;
    int i = 0;
    bool found = false;
    while (i < n) {
        const int  index = i * 3;
        const int  k     = keys_begin[index];
        const int  pid   = keys_begin[index + 1];
        const int  off   = keys_begin[index + 2];
        offset = off;
        c_pid  = pid;
        if (key == k) { found = true; break; }
        i++;
    }
    STACK_TRACE_ASSERT(c_pid != 0);
    assert(found == true);

    header.set_n(n - 1);

    // Shift keys down //
    const int shift_end    = i;
    const int shift_begin  = i + 1;
    const int shift_amount = n - (i + 1);
    std::memmove(keys_begin + static_cast<ptrdiff_t>(shift_end * 3), keys_begin + static_cast<ptrdiff_t>(shift_begin * 3), shift_amount * sizeof(int) * 3);
    const int set_begin  = n - 1;
    const int set_amount = 1;
    std::memset(keys_begin + static_cast<ptrdiff_t>(set_begin * 3), 0, set_amount * sizeof(int) * 3); // Zero out, not necessary more readable

    // Delete from leaf //
    BPTreeNode child{c_pid, tree_header};
    assert(child.header.get_type() == LEAF);
    child.delete_from_leaf(offset);
}

// auto BPTreeNode::leaf_get_free_slot(const unsigned int record_size) const -> std::optional<unsigned short*> {
//     if (header.get_type() != LEAF)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("leaf_get_free_slot(): Called with non-leaf"); }

//     const unsigned int req_size = record_size; 
//     const unsigned int num_free = header.get_num_free();
    
//     unsigned short* prev_offset = header.get_free_start_as_ushrt_ptr();
//     FreeBlock freeblock = *reinterpret_cast<FreeBlock*>(data + *prev_offset);

//     bool found = false;
//     for (int i = 0; i < num_free; i++) {
//         if (freeblock.size >= req_size) {
//             return prev_offset; // Overwrite
//         }
//         if (freeblock.next_offset == 0) {
//             FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Shouldn't happen"); }
//         prev_offset = reinterpret_cast<unsigned short*>(data + freeblock.next_offset);
//         freeblock = *reinterpret_cast<FreeBlock*>(data + *prev_offset);
//     }
    
//     return std::nullopt;
// }



static unsigned short charptr_to_ushrt(char* ptr) noexcept {
    unsigned short temp{};
    std::memcpy(&temp, ptr, sizeof(unsigned short));
    return temp;
}

static FreeBlock charptr_to_freeblock(char* ptr) noexcept {
    FreeBlock temp{};
    std::memcpy(&temp, ptr, FREEBLOCK_SIZE);
    return temp;
}

class FreeListIterator {
    BPTreeNode node;
    char* prev_offset_loc;
    unsigned short previous_offset;
    FreeBlock freeblock;
    int index;

    public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<char*, FreeBlock>; // Return both location and block
    using difference_type = std::ptrdiff_t;
    using pointer = const value_type*;
    using reference = value_type;

    explicit FreeListIterator(BPTreeNode node, char* start, int index = 0) 
      : node(node),
        prev_offset_loc(start), 
        previous_offset(charptr_to_ushrt(prev_offset_loc)), 
        freeblock(charptr_to_freeblock(node.data + previous_offset)) ,
        index(index)
    {}

    FreeListIterator& operator++() noexcept {
        const auto page_size = node.tree_header.get_page_size();
        if (previous_offset >= page_size) { // Overflow page
            const auto pid = node.header.get_next_overflow();
            BPTreeNode::discount_ass_copy_assignment(node, pid);
            previous_offset -= page_size;
        }

        prev_offset_loc = node.data + previous_offset;
        previous_offset = freeblock.next_offset;
        freeblock = charptr_to_freeblock(node.data + previous_offset);
        ++index;
        return *this;
    }

    FreeListIterator operator++(int) noexcept {
        FreeListIterator tmp = *this;
        ++(*this);
        return tmp;
    }

    // Return both the location and the freeblock
    reference operator*() const noexcept {
        return {prev_offset_loc, freeblock};
    }

    bool operator==(const FreeListIterator& other) const noexcept {
        return index == other.index;
    }

    bool operator!=(const FreeListIterator& other) const noexcept {
        return index != other.index;
    }

    // Getters for individual components if needed
    [[nodiscard]] char* get_location() const noexcept { return prev_offset_loc; }
    [[nodiscard]] FreeBlock get_block() const noexcept { return freeblock; }
};

// Wrapper class for range-based for loop
class FreeListRange {
    BPTreeNode node;
    char* start;
    unsigned int count;

public:
    FreeListRange(BPTreeNode node, char* start, unsigned int count) : node(node), start(start), count(count) {}

    FreeListIterator begin() const {
        return FreeListIterator(node, start, 0);
    }

    FreeListIterator end() const {
        return FreeListIterator(node, start, count);
    }
};



auto BPTreeNode::leaf_get_free_slot(const unsigned int record_size) const -> std::pair<char*, bool> {
    if (header.get_type() != LEAF)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("leaf_get_free_slot(): Called with non-leaf"); }

    const unsigned int req_size = record_size + RECORD_HEADER_SIZE; 
    const auto page_size = tree_header.get_page_size();

    FreeListRange freelist(*this, header.get_free_start_as_char_ptr(), header.get_num_free());

    for (const auto& p: freelist) {
        const auto& prev_offset_loc = p.first;
        const FreeBlock& freeblock = p.second;

        if (freeblock.size >= req_size) {
            return {prev_offset_loc, true}; 
        } else if (freeblock.next_offset == 0) {
            return {prev_offset_loc, false};
         }
    }

    FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Probably shouldn't be here idk");
    
    return {nullptr, false};
};


void BPTreeNode::write_record(const unsigned int offset, const Record record) noexcept {
    STACK_TRACE_ASSERT(offset < tree_header.get_page_size()); // Missed overflow page?
    std::memcpy(data + offset, &record, RECORD_HEADER_SIZE);
    std::memcpy(data + offset + RECORD_HEADER_SIZE, record.data, record.header.size);
}



auto BPTreeNode::allocate_overflow() const -> BPTreeNode {
    const page_id_t pid = allocate_page();
    BPTreeNode leaf{pid, tree_header};
    leaf.wipe_clean();
    leaf.header.set_n(0);
    leaf.header.set_type(LEAF);

    return leaf;
}

auto BPTreeNode::leaf_overflow_insert(char* const last_freeblock_addr, const Record record) -> std::pair<int, page_id_t> {
    // For now I guess never deallocate them, maybe store their n?

    BPTreeNode overflow_node = allocate_overflow();

    header.set_next_overflow(overflow_node.page_id);

    // Write record //
    const int offset = overflow_node.header.get_records_begin() - overflow_node.data;
    const int page_size = tree_header.get_page_size();
    overflow_node.write_record(offset, record);

    // Write freeblock //
    char* const prev_offset_loc = last_freeblock_addr;
    // const unsigned short cur_freeblock_offset = charptr_to_ushrt(prev_offset_loc);
    const unsigned short freeblock_offset = offset + record.header.size + RECORD_HEADER_SIZE;
    const unsigned short remaining_size = page_size - freeblock_offset;
    overflow_node.write_freeblock(freeblock_offset, FreeBlock{0, remaining_size});
    overflow_node.header.set_num_free(1);
    overflow_node.header.set_n(1);
    overflow_node.header.set_free_start(freeblock_offset);

    return {offset, overflow_node.page_id};
}

// Returns offset, pid
auto BPTreeNode::insert_into_leaf(const Record record) -> std::pair<int, page_id_t> {
    if (header.get_type() != LEAF)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_leaf(): Tried to insert record into non-leaf"); }
    if (is_full() == PAST_CAPACITY) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_leaf(): Tried to insert record into node that was past capacity"); }

    const auto [addr, found_slot] = leaf_get_free_slot(record.header.size);
    if (!found_slot) {
        // Couldn't find a free slot, overflow page time //
        const int overflow_pid = header.get_next_overflow();
        if (overflow_pid == 0) { // Allocate new overflow and insert
            return leaf_overflow_insert(addr, record);
        } else { // Recurse throw overflow pages
            BPTreeNode child{overflow_pid, tree_header};
            return child.insert_into_leaf(record);
        }
    }
    
    header.set_n(header.get_n() + 1);

    char* const prev_offset_loc = addr;
    const unsigned short  offset      = charptr_to_ushrt(prev_offset_loc);
    FreeBlock             freeblock   = charptr_to_freeblock(data + offset);
    unsigned short        next_offset = freeblock.next_offset;
    const unsigned short  prev_size   = freeblock.size;

    write_record(offset, record);

    const int total_record_size = record.header.size + RECORD_HEADER_SIZE;
    const unsigned short remaining_size = prev_size - total_record_size;
    if (remaining_size < 4) { // Not enough room for freeblock
        header.set_num_fragmented(header.get_num_fragmented() + remaining_size);
        std::memcpy(prev_offset_loc, &next_offset, sizeof(unsigned short)); // *prev_offset = next_offset;
    } else {
        const unsigned short free_block_offset = offset + total_record_size;
        const auto page_size = tree_header.get_page_size();
        // Check if freeblock would be contigous, then combine
        unsigned short total_size = remaining_size;
        while (next_offset != 0  && next_offset < page_size /*overflow case*/ && free_block_offset + total_size == next_offset) {
            FreeBlock next_freeblock = charptr_to_freeblock(data + next_offset);
            total_size += next_freeblock.size;
            next_offset = next_freeblock.next_offset;
        }
        write_freeblock(free_block_offset, FreeBlock{next_offset, total_size});

        std::memcpy(prev_offset_loc, &free_block_offset, sizeof(unsigned short)); // *prev_offset = free_block_offset;
    }

    return {offset, page_id};
}


void BPTreeNode::update_leaf(std::deque<page_id_t>& path, const int key, const int offset, const Record record) {
    if (header.get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("update_leaf(): Tried to update non-leaf"); }

    const int n = header.get_n();

    Record current_record{data + offset};

    if (current_record.header.size < record.header.size) { // Not enough room
        assert(path.size() > 0);
        BPTreeNode parent{path.front(), tree_header};
        assert(parent.header.get_type() == BRANCH);
        parent.delete_from_branch(path, key);
        parent.insert_into_branch(key, record);
    } else {
        write_record(offset, record);
    }
}

auto BPTreeNode::offset_page_back(const int off) const noexcept -> int* {
    const int page_size = tree_header.get_page_size();
    const unsigned int offset = sizeof(int) * (off);
    assert(offset < page_size);
    char* const char_back = data + page_size - offset;
    int*  const back      = reinterpret_cast<int*>(char_back);
    return back;
}

auto BPTreeNode::index_page_back(const int index) const noexcept -> int {
    const int page_size = tree_header.get_page_size();
    const unsigned int offset = sizeof(int) * (index + 1);
    assert(offset < page_size);
    char* const char_back = data + page_size - offset;
    int*  const back      = reinterpret_cast<int*>(char_back);
    return *back;
}

auto BPTreeNode::get_page_back_char(const int index) const noexcept -> char* {
    const int page_size = tree_header.get_page_size();
    const unsigned int offset = index;
    assert(offset < page_size);
    char* const char_back = data + page_size - offset;
    return char_back;
}

void BPTreeNode::insert_into_intermediate(const int key, const page_id_t left, const page_id_t right) {
    const int n = header.get_n();
    if (is_full() == BYTES_FULL)           { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called with full bytes");  }
    if (is_full() == PAST_CAPACITY)        { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called when past capacity"); }
    if (header.get_type() != INTERMEDIATE) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called on non-intermediate node"); }

    int* const int_keys_begin = header.get_int_keys_begin();
    int* const key_location   = int_keys_begin + n;

    int* const back = offset_page_back(n + 2); // TODO: CHECK
    int* const left_location  = back + 1;
    int* const right_location = back;

    *key_location   = key;
    *left_location  = left;
    *right_location = right;

    header.set_n(n + 2);
}

void BPTreeNode::insert_into_intermediate(const int key, const page_id_t other) {
    const int n = header.get_n();
    if (is_full() == BYTES_FULL)           { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called with full bytes");  }
    if (is_full() == PAST_CAPACITY)        { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called when past capacity"); }
    if (header.get_type() != INTERMEDIATE) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("insert_into_intermediate(): Called on non-intermediate node"); }

    int* const int_keys_begin = header.get_int_keys_begin();
    assert(n >= 2);
    int* const key_location   = int_keys_begin + n - 1;
    int* const other_location = offset_page_back(n + 1); 
    
    *key_location   = key;
    *other_location = other;
    
    header.set_n(n + 1);
}

void BPTreeNode::split_root_intermediate() {
    const auto type = header.get_type();
    if (type != INTERMEDIATE) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root_intermediate(): Called with non-intermediate root"); }

    const int n = header.get_n();
    const unsigned int left_partition  = 0;
    const unsigned int right_partition = n / 2;
    const unsigned int right_size      = n - right_partition;
    const unsigned int left_size       = right_partition;

    // Init left and right nodes //
    const page_id_t left_pid  = allocate_page();
    BPTreeNode left_node{left_pid, tree_header};
    left_node.wipe_clean();
    left_node.header.set_n(0);
    left_node.header.set_type(INTERMEDIATE);
    const page_id_t right_pid = allocate_page();
    BPTreeNode right_node{right_pid, tree_header};
    right_node.wipe_clean();
    right_node.header.set_n(0);
    right_node.header.set_type(INTERMEDIATE);

    int* const keys_begin  = header.get_int_keys_begin();

    // Insert into intermediates //
    // Left intermediate //
    // Keys
    int* const left_keys_begin = left_node.header.get_int_keys_begin();
    std::memcpy(left_keys_begin, keys_begin, left_size * sizeof(int));
    // PIDs
    assert(left_size >= 1);
    int* const left_pids_begin = left_node.offset_page_back(left_size);
    std::memcpy(left_pids_begin, offset_page_back(left_size), left_size * sizeof(int));
    // Header
    left_node.header.set_n(left_size);

    // Right intermediate //
    // Keys
    assert(right_size >= 1);
    assert(right_partition >= 1);
    int* const right_keys_begin = right_node.header.get_int_keys_begin();
    std::memcpy(right_keys_begin, keys_begin + right_partition, right_size * sizeof(int));
    // PIDs
    int* const right_pids_begin = right_node.offset_page_back(right_size);
    std::memcpy(right_pids_begin, offset_page_back(n), right_size * sizeof(int));
    // Header
    right_node.header.set_n(right_size);

    // Fix root node //
    const int min_key = keys_begin[right_partition - 1];
    wipe_clean();
    header.set_type(INTERMEDIATE);
    header.set_n(0);
    insert_into_intermediate(min_key, left_pid, right_pid);

    // Sanity check //
    assert(header.get_n() == 2);
    assert(header.get_type() == INTERMEDIATE);
    assert(left_node.header.get_n()  == left_size);
    assert(right_node.header.get_n() == right_size);
    assert(left_node.header.get_type() == INTERMEDIATE);
    assert(left_node.header.get_type() == INTERMEDIATE);
}

auto BPTreeNode::allocate_branch() const -> BPTreeNode {
    const page_id_t pid  = allocate_page();
    BPTreeNode node{pid, tree_header};
    node.wipe_clean();
    node.header.set_n(0);
    node.header.set_type(BRANCH);
    return node;
}

void BPTreeNode::leaf_deallocate() {
    if (header.get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("leaf_deallocate(): Called with non-LEAF node"); }
    std::vector<page_id_t> pids;
    pids.emplace_back(page_id);
    page_id_t overflow_pid = header.get_next_overflow();
    while (overflow_pid != 0) {
        pids.emplace_back(overflow_pid);
        overflow_pid = header.get_next_overflow();
    }
    for (const page_id_t pid : pids) {
        deallocate_page(pid);
    }
}

// Splist root (branch) into 2 child nodes (branches), creates a new leaf node and fixes the old leaf root previously contained
// Doesn't work with overflow pages
void BPTreeNode::split_root_branch() {
    if (header.get_type() != BRANCH) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root_branch(): Called with non-BRANCH root"); }
    
    const int n = header.get_n();
    const unsigned int left_partition  = 0;
    const unsigned int right_partition = n / 2;
    const unsigned int right_size      = n - right_partition;
    const unsigned int left_size       = right_partition;

    BPTreeNode old_child{header.get_c_pid(), tree_header};

    int* const keys_begin = header.get_int_keys_begin();
    
    // Insert into left //
    BPTreeNode left_node  = allocate_branch();
    for (int i = left_partition; i < right_partition; i++) {
        const int index = i * 3;
        const int key = keys_begin[index];
        const int pid = keys_begin[index + 1];
        const int off = keys_begin[index + 2];
        const BPTreeNode record_container{pid, tree_header};
        left_node.insert_into_branch(key, Record{record_container.data + off});
    }
    
    // Insert into right //
    BPTreeNode right_node = allocate_branch();
    for (int i = right_partition; i < n; i++) {
        const int index = i * 3;
        const int key = keys_begin[index];
        const int pid = keys_begin[index + 1];
        const int off = keys_begin[index + 2];
        const BPTreeNode record_container{pid, tree_header};
        right_node.insert_into_branch(key, Record{record_container.data + off});
    }

    old_child.leaf_deallocate();
    
    // Fix root node //
    const int min_key = keys_begin[static_cast<size_t>(right_partition * 3)];
    wipe_clean();
    header.set_type(INTERMEDIATE);
    header.set_n(0);
    insert_into_intermediate(min_key, left_node.page_id, right_node.page_id);

    // Sanity check //
    assert(header.get_n() == 2);
    assert(header.get_type() == INTERMEDIATE);
    assert(left_node.header.get_n()     == left_size);
    assert(left_node.header.get_type()  == BRANCH);
    assert(right_node.header.get_n()    == right_size);
    assert(right_node.header.get_type() == BRANCH);
}

void BPTreeNode::split_root() {
    if (page_id != ROOT_PAGE_ID)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root(): Called on non-root node"); }
    if (is_full() == BYTES_FULL)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root(): Called with full bytes");  }
    if (!(is_full() == AT_CAPACITY || is_full() == PAST_CAPACITY)) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root(): Called when not at or exceeding capacity (under capacity)"); }

    const auto type = header.get_type();
    if (type == INTERMEDIATE) {
        split_root_intermediate();
    } else if (type == BRANCH) {
        split_root_branch();
    } else if (type == LEAF) {
        FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_root(): Root should never be LEAF");
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("TODO");
    }
}

void BPTreeNode::delete_from_leaf(const int offset) {
    assert(offset <= USHRT_MAX);
    const auto type = header.get_type();
    if (type != LEAF) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("delete_from_leaf(): Called with non-LEAF"); }
    
    // Zero memory //
    const Record record{data + offset};
    assert(record.header.size <= USHRT_MAX);
    const unsigned short record_size = record.header.size + RECORD_HEADER_SIZE;
    std::memset(data + offset, 0, record_size);
    header.set_n(header.get_n() - 1);

    // Fix freeblock chain //
    FreeListRange freelist(*this, header.get_free_start_as_char_ptr(), header.get_num_free());

    char* last_offset_loc = nullptr;
    for (const auto& p : freelist) {
        const auto& prev_offset_loc = p.first;
        const auto& freeblock = p.second;
        const unsigned short prev_offset = charptr_to_ushrt(prev_offset_loc);

        // TODO: Overflow case. Next freeblock to point to is in another page

        if (prev_offset > offset) { // Insert into middle of chain

            // Check if freeblock would be contigous, then combine
            const auto page_size = tree_header.get_page_size();
            unsigned short total_size = record_size;
            unsigned short next_offset = prev_offset;
            while (next_offset != 0 && next_offset < page_size /*overflow case*/ &&  offset + total_size == next_offset) {
                FreeBlock next_freeblock = charptr_to_freeblock(data + next_offset);
                total_size += next_freeblock.size;
                next_offset = next_freeblock.next_offset;
            }

            write_freeblock(offset, FreeBlock{next_offset, total_size});
            std::memcpy(prev_offset_loc, &offset, sizeof(unsigned short)); // *prev_offset = offset
            header.set_num_free(header.get_num_free() + 1);
            return;
        } else if (prev_offset == 0) { // Insert into end
            write_freeblock(offset, FreeBlock{0, record_size});
            std::memcpy(prev_offset_loc, &offset, sizeof(unsigned short)); // *prev_offset = offset;
            header.set_num_free(header.get_num_free() + 1);
            return;
        }
        last_offset_loc = prev_offset_loc;
    }

    FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Shouldn't be here, probably failed to deal with overflow");
}

void BPTreeNode::split_branch(std::deque<page_id_t>& path) {
    BPTreeNode parent_node{path.front(), tree_header};
    const int parent_n = parent_node.header.get_n();
    const int n = header.get_n();
    const unsigned int left_partition  = 0;
    const unsigned int right_partition = n / 2;
    const unsigned int right_size      = n - right_partition;
    const unsigned int left_size       = right_partition;

    int* const keys_begin = header.get_int_keys_begin();

    // Init other //
    const page_id_t other_pid = allocate_page();
    BPTreeNode other_node{other_pid, tree_header};
    other_node.wipe_clean();
    other_node.header.set_n(0);
    other_node.header.set_type(BRANCH);

    // Insert into other //
    const page_id_t cur_leaf_pid = keys_begin[1]; // [key, pid, offset]
    BPTreeNode cur_leaf{cur_leaf_pid, tree_header};
    const page_id_t other_c_pid = allocate_page();
    BPTreeNode other_child{other_c_pid, tree_header};
    other_child.wipe_clean();
    other_child.header.set_type(LEAF);
    for (int i = right_partition; i < n; i++) {
        const int index = i * 3;
        const int key           = keys_begin[index];
        const int record_offset = keys_begin[index + 2];
        other_node.insert_into_branch(key, other_c_pid, Record{cur_leaf.data + record_offset});
    }

    // Fix parent //
    STACK_TRACE_ASSERT(parent_node.header.get_type() == INTERMEDIATE);
    int* const other_keys_begin = other_node.header.get_int_keys_begin();
    const int min_key = keys_begin[right_partition * 3];
    parent_node.insert_into_intermediate(min_key, other_pid);

    // Fix LEAF(s?) //
    for (int i = right_partition; i < n; i++) {
        const int index = i * 3;
        const page_id_t c_pid         = keys_begin[index + 1];
        const page_id_t record_offset = keys_begin[index + 2];
        BPTreeNode node{c_pid, tree_header};
        assert(node.header.get_type() == LEAF);
        node.delete_from_leaf(record_offset);
    }

    // Fix current node //
    std::memset(keys_begin + (static_cast<size_t>(right_partition * 3)), 0,  sizeof(int) * right_size * 3); // [key, pid, offset]
    header.set_n(left_size);
    
    // Sanity check //
    assert(parent_node.header.get_n() == parent_n + 1);
    assert(parent_node.header.get_type() == INTERMEDIATE);
    assert(header.get_n() == left_size);
    assert(other_node.header.get_n() == right_size);
    BPTreeNode other_c{other_c_pid, tree_header};
    assert(other_c.header.get_n() == right_size);
}

void BPTreeNode::split_intermediate(std::deque<page_id_t>& path) {
    const auto type = header.get_type();
    if (type != INTERMEDIATE) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_intermediate(): Called with non-intermediate node"); }
    BPTreeNode parent_node{path.front(), tree_header};
    const int parent_n = parent_node.header.get_n();

    const int n = header.get_n();
    const unsigned int left_partition  = 0;
    const unsigned int right_partition = n / 2;
    const unsigned int right_size      = n - right_partition;
    const unsigned int left_size       = right_partition;

    // Init other //
    const page_id_t other_pid = allocate_page();
    BPTreeNode other_node{other_pid, tree_header};
    other_node.wipe_clean();
    other_node.header.set_n(0);
    other_node.header.set_type(INTERMEDIATE);

    // Insert into other //
    // Keys
    int* const keys_begin       = header.get_int_keys_begin();
    int* const other_keys_begin = other_node.header.get_int_keys_begin();
    std::memcpy(other_keys_begin, keys_begin + right_partition, right_size  * sizeof(int));
    // PIDs
    int* const pids_begin       = offset_page_back(n);
    int* const other_pids_begin = other_node.offset_page_back(right_size);
    std::memcpy(other_pids_begin, pids_begin, right_size * sizeof(int));
    // Header
    other_node.header.set_n(right_size);

    // Fix parent //
    assert(right_partition != 0);
    const int min_key = keys_begin[right_partition - 1];
    parent_node.insert_into_intermediate(min_key, other_pid);

    // Fix current node //
    // Zero out moved from items //
    // Keys
    std::memset(keys_begin + right_partition, 0,  right_size * sizeof(int)); 
    // PIDs
    std::memset(pids_begin, 0, right_size * sizeof(int));
    // header
    header.set_n(left_size);

    // Sanity check //
    STACK_TRACE_EXPECT(parent_node.header.get_n(), parent_n  + 1);
    assert(parent_node.header.get_type() == INTERMEDIATE);
    assert(header.get_n() == left_size);
    assert(other_node.header.get_n() == right_size);
}

void BPTreeNode::split_node(std::deque<page_id_t>& path) {
    if (page_id == ROOT_PAGE_ID) { split_root(); return; }

    if (path.empty()) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Path too small");  }
    BPTreeNode parent_node{path.front(), tree_header};
    if (parent_node.is_full() == BYTES_FULL)    { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Called with parent full bytes");  }
    if (parent_node.is_full() == PAST_CAPACITY) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Called when parent is already past capacity");  }
    if (parent_node.header.get_type() == LEAF)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Somehow parent is a LEAF node"); }

    if (is_full() == BYTES_FULL)  { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Called with full bytes");  }
    if (!(is_full() == AT_CAPACITY || is_full() == PAST_CAPACITY)) { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Called when not at or exceeding capacity (under capacity)"); }

    const int  n    = header.get_n();
    const auto type = header.get_type();
    if (type == INTERMEDIATE) {
        split_intermediate(path);
    } else if (type == BRANCH) {
        split_branch(path);
    } else if (type == LEAF) {
        FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("split_node(): Node being split should never be a LEAF");
    } else {
        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("TODO");
    }
}