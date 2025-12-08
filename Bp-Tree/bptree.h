#pragma once

#include "Types.h"
#include "structs_and_constants.h"
#include "macros.h"
#include "helpers.h"

#include <cassert>
#include <cstddef>
#include <cstring>
#include <deque>
#include <queue>
#include <iostream>
#include <map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <climits>
#include <set>

// Type + n + num_free + free_start + num_fragmented + left sibling + right sibling + overflow
static constexpr int bp_tree_node_header_size = sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(int) + sizeof(page_id_t) + sizeof(page_id_t) + sizeof(page_id_t);


class BPTreeLog {
    public:
    enum Operation : int { INSERT, UPDATE, DELETE, SPLIT_INTERMEDIATE, SPLIT_BRANCH, ALLOCATE_PAGE};
    std::vector<std::pair<Operation, page_id_t>> ops;

    void add_op(Operation op, page_id_t pid) { ops.emplace_back(op, pid); }

    void clear() { ops.clear(); }

    [[nodiscard]] std::vector<page_id_t> search(Operation op) {
        std::vector<page_id_t> ret;
        for (const auto& [o, pid] : ops) {
            if (o == op) {
                ret.emplace_back(pid);
            }
        }
        return ret;
    }

    [[nodiscard]] static std::string op_to_str(Operation op) noexcept {
        switch (op) {
            case INSERT:             return "INSERT";
            case UPDATE:             return "UPDATE";
            case DELETE:             return "DELETE";
            case SPLIT_INTERMEDIATE: return "SPLIT_INTERMEDIATE";
            case SPLIT_BRANCH:       return "SPLIT_BRANCH";
            case ALLOCATE_PAGE:      return "ALLOCATE_PAGE";
        }
    }

    void print() {
        for (const auto& [op, pid] : ops) {
            std::cerr << "pid (" << pid << "): " << ASCII_BG_YELLOW << op_to_str(op) << ASCII_RESET << "\n";
        }
    }
};

class PageAllocator {
    public:
    virtual ~PageAllocator() = default;
    [[nodiscard]] char* get_page(int index) const;
    [[nodiscard]] page_id_t allocate_page() const;
    void deallocate_page(page_id_t pid) const;
    #ifdef LOG_BP_TREE
    virtual void log_add_op(BPTreeLog::Operation, page_id_t) const = 0;
    #endif
};


class BPTreeHeader : PageAllocator {
    public:
    #ifdef LOG_BP_TREE
    mutable BPTreeLog* log;
    void log_add_op(BPTreeLog::Operation op, page_id_t pid) const override { log->add_op(op, pid); }
    #endif
    static constexpr page_id_t tree_header_page_id = 0;
    char* data;
    std::vector<std::tuple<unsigned int, SQL_data_type, char*>> fields;
    
    void init() {
        // assert(PAGE_SIZE >= kib / 2); // FIXME: Temp disabled for testing
        const int page_size        = get_page_size();
        const int branching_factor = get_branching_factor();
        STACK_TRACE_ASSERT(page_size <= kib * 256);
        STACK_TRACE_ASSERT(page_size % 32 == 0);
        STACK_TRACE_ASSERT(branching_factor >= 2 and branching_factor <= 2048);
        
        const unsigned int num_fields = get_number_of_record_fields();
        fields.reserve(num_fields);
        int* record_field_data = get_record_field_data_int_begin();

        unsigned int records_size = 0;

        for (unsigned int i = 0; i < num_fields; i++) {
            const unsigned int record_size = record_field_data[0]; 
            record_field_data++;
            
            records_size += record_size;
            
            const SQL_data_type type = (SQL_data_type) record_field_data[0]; 
            record_field_data++;
            
            fields.emplace_back(record_size, type, (char*) record_field_data);
        }

        if (records_size > (int) page_size - 4 /* page_size */) { 
            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Page size (" + std::to_string(page_size) + ") is too small to contain record metadata ( 4 bytes for page size + " + std::to_string(records_size) + " bytes for metadata)"); 
        }   

        // ///
        // n + 1 cause lazy inserts
        unsigned int required_keys_size_in_bytes = (branching_factor + 1) * (sizeof(key) + sizeof(page_id_t) + sizeof(int)); // [key, pid, offset]
        if (page_size - bp_tree_node_header_size - required_keys_size_in_bytes < 0) {

            FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Page size (" + std::to_string(page_size) + ") is too small to contain the number of keys (" + std::to_string(branching_factor) 
                                                    + ", " + std::to_string(required_keys_size_in_bytes) + " bytes) specified by the branching factor");
        }
    }

    #ifdef LOG_BP_TREE
    BPTreeHeader(BPTreeLog* log = nullptr) : log(log), data(get_page(tree_header_page_id)) {
        init();    
    }
    BPTreeHeader(const size_t page_size, const size_t branching_factor, BPTreeLog* log = nullptr) : log(log), data(get_page(tree_header_page_id)) {
        set_page_size(page_size);
        set_branching_factor(branching_factor);
        init();    
    }
    void set_log(BPTreeLog* set_log) noexcept { log = set_log; }
    #else
    BPTreeHeader() : data(get_page(tree_header_page_id)) {
        init();    
    }
    BPTreeHeader(const size_t page_size, const size_t branching_factor) : data(get_page(tree_header_page_id)) {
        set_page_size(page_size);
        set_branching_factor(branching_factor);
        init();    
    }
    #endif


    
    [[nodiscard]] auto get_page_size() const noexcept -> unsigned int  { return reinterpret_cast<int*>(data)[0]; }
    void set_page_size(const unsigned int size) noexcept { reinterpret_cast<int*>(data)[0] = size; }

    [[nodiscard]] auto get_branching_factor() const noexcept -> unsigned int  { return reinterpret_cast<int*>(data)[1]; }
    void set_branching_factor(const unsigned int size) noexcept { reinterpret_cast<int*>(data)[1] = size; }
    
    [[nodiscard]] auto get_number_of_record_fields() const noexcept -> unsigned int { return reinterpret_cast<int*>(data)[2]; }
    void set_number_of_record_fields(const unsigned int set) noexcept {reinterpret_cast<int*>(data)[2] = set; }
    
    [[nodiscard]] auto get_record_field_data_int_begin()  const noexcept -> int*  { return reinterpret_cast<int*>(data) + 3; }
    [[nodiscard]] auto get_record_field_data_char_begin() const noexcept -> char* { return data + 3 * sizeof(int); }
};

class BPTreeNodeHeader {
    static constexpr int size = bp_tree_node_header_size;
    public:
    char* data;

    // Read field tuple data
    BPTreeNodeHeader(char* data) noexcept : data(data) {}

    [[nodiscard]] static constexpr int get_header_size()  noexcept { return size; }

    // Getters and setters in the same order as the binary format

    // 0
    [[nodiscard]] auto get_type() const noexcept -> BPTreeNodeType { return reinterpret_cast<BPTreeNodeType*>(data)[0]; }
    void set_type(const BPTreeNodeType set) noexcept { reinterpret_cast<BPTreeNodeType*>(data)[0] = set;  }

    // 1
    // Number of children == n.
    [[nodiscard]] auto get_n() const noexcept -> unsigned int { return reinterpret_cast<unsigned int*>(data)[1]; }
    void set_n(const int set) noexcept { reinterpret_cast<unsigned int*>(data)[1] = set;  }

    // 2
    [[nodiscard]] auto get_num_free() const noexcept -> unsigned int { 
        // if (get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Called with non-LEAFs"); }
        return reinterpret_cast<unsigned int*>(data)[2]; }
    void set_num_free(const int set) noexcept { reinterpret_cast<unsigned int*>(data)[2] = set; }

    // 3
    [[nodiscard]] auto get_free_start_as_char_ptr() const noexcept -> char* { 
        if (get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Called with non-LEAF"); }
        return reinterpret_cast<char*>(reinterpret_cast<int*>(data) + 3); }
    [[nodiscard]] auto get_free_start() const noexcept -> unsigned int { 
        if (get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Called with non-LEAF"); }
        return reinterpret_cast<unsigned int*>(data)[3]; }
    [[nodiscard]] auto get_free_start_noexcept() const noexcept -> unsigned int { return reinterpret_cast<unsigned int*>(data)[3]; } // Careful
    void set_free_start(const int set) noexcept { reinterpret_cast<unsigned int*>(data)[3] = set; }

    [[nodiscard]] auto get_c_pid() const noexcept -> page_id_t { 
        if (get_type() != BRANCH) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Called with non-BRANCH"); }
        return reinterpret_cast<page_id_t*>(data)[3]; }
    void set_c_pid(const page_id_t set) noexcept { reinterpret_cast<page_id_t*>(data)[3] = set; }


    // 4
    [[nodiscard]] auto get_num_fragmented() const noexcept -> unsigned int { return reinterpret_cast<unsigned int*>(data)[4]; }
    void set_num_fragmented(const int set) noexcept { reinterpret_cast<unsigned int*>(data)[4] = set; }

    // 5
    // Empty sibling = page 0. Root's siblings are always 0
    [[nodiscard]] auto get_left_sibling() const noexcept -> page_id_t { return reinterpret_cast<page_id_t*>(data)[5]; }
    void set_left_sibling(const page_id_t set) noexcept { reinterpret_cast<page_id_t*>(data)[5] = set; }

    // 6
    [[nodiscard]] auto get_right_sibling() const noexcept -> page_id_t { return reinterpret_cast<page_id_t*>(data)[6]; }
    void set_right_sibling(const page_id_t set) noexcept { reinterpret_cast<page_id_t*>(data)[6] = set; }

    // 7
    [[nodiscard]] auto get_next_overflow() const noexcept -> page_id_t { 
        if (get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Called with non-LEAFs"); }
        return reinterpret_cast<page_id_t*>(data)[7]; }
    [[nodiscard]] auto get_next_overflow_noexcept() const noexcept -> page_id_t { return reinterpret_cast<page_id_t*>(data)[7]; }
    void set_next_overflow(const page_id_t set) noexcept { reinterpret_cast<page_id_t*>(data)[7] = set; }




    [[nodiscard]] auto get_int_keys_begin()  const noexcept -> int*  { 
        if (get_type() == LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("get_int_keys_begin(): Called with LEAF type. LEAFs do not contain keys"); }
        return reinterpret_cast<int*>(data + size); 
    }
    [[nodiscard]] auto get_char_keys_begin() const noexcept -> char* { 
        if (get_type() == LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("get_char_keys_begin(): Called with LEAF type. LEAFs do not contain keys"); }
        return data + size; 
    }
    [[nodiscard]] auto get_records_begin() const noexcept  -> char* { 
        if (get_type() != LEAF) { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("get_records_begin(): Called with non-LEAF type. Non-LEAFs do not contain records"); }
        return data + size; 
    }
};

class BPTreeNode : PageAllocator {
    public:

    page_id_t page_id;
    char* data; 
    BPTreeNodeHeader header;
    const BPTreeHeader& tree_header;
    #ifdef LOG_BP_TREE
    void log_add_op(BPTreeLog::Operation op, page_id_t pid) const override { tree_header.log->add_op(op, pid); }
    #endif

    BPTreeNode() = delete;
    BPTreeNode(page_id_t) = delete;

    // Given page by bufer pool manager
    // Eventually use page->get_data() instead of having raw char*
    explicit BPTreeNode(const page_id_t page_id, const BPTreeHeader& tree_header) : page_id(page_id), data(get_page(page_id)), header(data), tree_header(tree_header) {
        STACK_TRACE_ASSERT(page_id > 0);
    }

    void discount_ass_copy_assignment(const page_id_t new_pid) {
        page_id = new_pid;
        data = get_page(new_pid);
        header = BPTreeNodeHeader{data};
    }


    ///////////////////// INFO ////////////////////////
    // For all node types, max number of children == n
    //
    // INTERMEDIATE = header + [key, pid] pairs
    // Max number of keys == n - 1,
    //
    // BRANCH       = header + [key, pid, offset] pairs
    // LEAF         = header + records
    // Only way to locate a record for a LEAF is to go through its parent.
    ///////////////////////////////////////////////////



    void print_bytes() const noexcept;

    static void print_bytes(const page_id_t pid, const size_t page_size) noexcept;
    static void print_bytes(const page_id_t pid) noexcept;

    void wipe_clean() noexcept;

    void write_freeblock(int offset, FreeBlock freeblock) noexcept;

    [[nodiscard]] auto get_bytes_used() const noexcept -> int;

    [[nodiscard]] auto get_bytes_used(const int n) const noexcept -> int;

    [[nodiscard]] auto is_full() const noexcept -> NodeFullStatus;

    [[nodiscard]] auto is_full(const int n) const noexcept -> NodeFullStatus;

    void print_leaf(const int indent, std::queue<int>& offsets) const noexcept;

    void print_branch(const int indent) const noexcept;

    void print_intermediate(const int indent) const noexcept;

    void print_inorder(const int indent) const noexcept;

    void sort_branch() noexcept;

    void leaf_deallocate();

    [[nodiscard]] auto allocate_branch() const -> BPTreeNode;

    [[nodiscard]] auto allocate_leaf() const -> BPTreeNode;

    void insert_into_branch(const int key, const Record record);

    void update_branch(const int key, const Record record);

    void delete_from_intemediate(const int key);

    void delete_branch_node();

    // Take from neighbors
    [[nodiscard]] auto branch_merge(std::deque<page_id_t>& path) -> bool;

    [[nodiscard]] auto branch_redistribute(std::deque<page_id_t>& path, const int self_index) -> bool;

    void delete_from_branch(const int key);

    // Returns address of available freeslot or last available freeblock. True on success, False on fail
    [[nodiscard]] auto leaf_get_free_slot(const unsigned int record_size) const -> std::tuple<char*, page_id_t, bool>;

    void write_record(const unsigned int offset, const Record record) noexcept;
    
    [[nodiscard]] auto allocate_overflow() const -> BPTreeNode;
    
    [[nodiscard]] auto insert_into_leaf(const Record record) -> std::pair<int, page_id_t> ;
    
    void update_leaf(std::deque<page_id_t>& path, const int key, const int offset, const Record record);

    [[nodiscard]] auto offset_page_back(const int off) const noexcept -> int*;

    [[nodiscard]] auto index_page_back(const int index) const noexcept -> int;

    [[nodiscard]] auto get_page_back_char(const int index) const noexcept -> char*;

    void insert_into_intermediate(const int key, const page_id_t left, const page_id_t right) noexcept;

    void insert_into_intermediate(const int key, const page_id_t other) noexcept;

    void split_root_intermediate();

    // Splist root (branch) into 2 child nodes (branches), creates a new leaf node and fixes the old leaf root previously contained
    void split_root_branch();

    void split_root();

    void delete_from_leaf(const offset_t offset) noexcept;
    
    void split_branch(std::deque<page_id_t>& path);

    void split_intermediate(std::deque<page_id_t>& path);

    void split_node(std::deque<page_id_t>& path);
};






enum bptree_pretty_print_color { WHITE, LIGHT_BROWN, BROWN, GREEN };

inline std::string bptree_pretty_print_color_to_string(const bptree_pretty_print_color color) noexcept {
    switch (color) {
        case WHITE:       return "\033[37m";
        case LIGHT_BROWN: return "\033[38;5;180m";
        case BROWN:       return "\033[38;5;130m";
        case GREEN:       return "\033[32m";
    }
}

inline void pretty_print_print_rectangles(const int page_width, const int page_height, const std::vector<std::vector<char>>& screen, const std::vector<bptree_pretty_print_color>& page_colors, const int max) noexcept {
    const int n = max + 1;
    for (int i = 0; i < page_height; i++) {
        std::stringstream row;
        for (int j = 0; j < page_width * n; j++) {
            const int  pid      = j / page_width;
            const bool new_page = j % page_width == 0;
            if (new_page) {
                row << bptree_pretty_print_color_to_string(page_colors[pid]);
            }
            row << screen[i][j];
        }
        std::cout << row.str() << std::endl;
    }
    std::cout << std::endl;        
}

inline void pretty_print_create_rectangle(const int page_width, const int page_height, std::vector<std::vector<char>>& screen, std::vector<bptree_pretty_print_color>& page_colors, 
        const bptree_pretty_print_color color, const int max, const page_id_t pid, const auto& node, std::deque<int>& offsets)
{
    page_colors.resize(max + 1);
    page_colors[max] = color;
    // page_colors.emplace_back(color);
    
    const int x_offset = max * page_width;
    for (auto& row : screen) {
        row.resize(x_offset + page_width);
    }

    // Top row
    for (int i = x_offset; i < x_offset + page_width; i++) {
        screen[0][i] = '#';
    }
    // Right side
    for (int i = 0; i < page_height; i++) {
        screen[i][x_offset + page_width - 1] = '#';
    }
    // Bottom row
    for (int i = x_offset; i < x_offset + page_width; i++) {
        screen[page_height - 1][i] = '#';
    }
    // Left Side
    for (int i = 0; i < page_height; i++) {
        screen[i][x_offset] = '#';
    }
    // Fill inside with '_'
    for (int i = 1; i < page_height - 1; i++) {
        const int y = i;
        for (int j = 1; j < page_width - 1; j++) {
            const int x = j + x_offset;
            screen[y][x] = '_';
        }
    }

    // Print pid
    char* const pid_location = screen[1].data() + x_offset + 1;
    const std::string pid_msg = "p=" + std::to_string(pid);
    std::memcpy(pid_location, pid_msg.data(), 3);

    if (pid == 0) { return; }

    // Print n
    const int n = node.header.get_n();
    char* const n_location = screen[2].data() + x_offset + 1;
    const std::string n_msg = "n=" + std::to_string(n);
    std::memcpy(n_location, n_msg.data(), 3);

    // Print data
    const auto type = node.header.get_type();
    if (type == LEAF) {
        int i = 0;
        for (int i = 0; i < n; i++) {
            const int   offset          = offsets.front(); offsets.pop_front();
            char* const screen_location = screen[3 + i++].data() + x_offset + 1;
            char* const record_location = node.data + static_cast<ptrdiff_t>(offset);
            const Record record{record_location};
            std::memcpy(screen_location, record.data, record.header.size);
        }
    } else if (type == BRANCH) {
        int* const keys_begin = node.header.get_int_keys_begin();
        for (int i = 0; i < n; i++) {
            const int  index = i * 3;
            char* const screen_location = screen[3 + i].data() + x_offset + 1;
            const int  key = keys_begin[index];
            const int  pid = keys_begin[index + 1];
            const int  off = keys_begin[index + 2];
            const std::string msg = std::to_string(key) + ", " + std::to_string(pid) + ", " + std::to_string(off);
            std::memcpy(screen_location, msg.data(), msg.size());
        }
    } else { // INTERMEDIATE
        int* const keys_begin = node.header.get_int_keys_begin();
        for (int i = 0; i < n - 1; i++) { // keys
            char* const screen_location = screen[3 + i].data() + x_offset + 1;
            const int  key = keys_begin[i];
            const std::string msg = std::to_string(key);
            std::memcpy(screen_location, msg.data(), msg.size());
        }
        for (int i = 0; i < n; i++) { // pid
            const int  pid = node.index_page_back(i);
            const std::string msg = std::to_string(pid);
            char* const screen_location = screen[page_height - 2 - i].data() + x_offset -1 + page_width - msg.size();
            std::memcpy(screen_location, msg.data(), msg.size());
        }
    }
}

__attribute__((used))
inline void print_bytes_with_pid(page_id_t pid, BPTreeNode node) {
    node.discount_ass_copy_assignment(pid);
    node.print_bytes();
}

class BPTree : PageAllocator {
    public:
    #ifdef LOG_BP_TREE
    mutable BPTreeLog log{};
    void log_add_op(BPTreeLog::Operation op, page_id_t pid) const override { log.add_op(op, pid); }
    #endif

    static constexpr page_id_t tree_header_page_id = 0;
    BPTreeHeader header;
    BPTreeNode root;


    explicit BPTree() : root(ROOT_PAGE_ID, header) {} // Already exists, read from disk
    explicit BPTree(BPTreeHeader set_header) : header(std::move(set_header)), root(ROOT_PAGE_ID, header) {}

    static BPTree create_tree_from_disk() { 
        BPTree tree{};
        tree.root.wipe_clean();
        tree.root.header.set_type(LEAF);
        return tree; 
    }

    static BPTree create_tree(const size_t page_size, const size_t branching_factor, std::vector<SQL_data_type> fields) { 
        assert(page_size <= USHRT_MAX); // TODO: Handle bigger page sizes later 

        BPTreeHeader header{page_size, branching_factor};

        char* record_metadata = header.get_record_field_data_char_begin();
        
        int index = 0;
        for (const auto& type : fields) {
            if (index + 2 > page_size - (header.get_record_field_data_char_begin() - header.data)) { // Should just be PAGE_SIZE - 4 bytes for page size
                FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC("Page size too small to contain record metadata");
            }
            
            record_metadata[index++] = type;
        }
        
        header.set_number_of_record_fields(fields.size());

        BPTree tree{std::move(header)};
        tree.root.wipe_clean();
        tree.root.header.set_type(BRANCH);

        #ifdef LOG_BP_TREE
        tree.header.set_log(&tree.log);
        #endif

        return tree; 
    }

    void print_inorder() const noexcept { root.print_inorder(0); std::cout << std::endl; }

    // Uses linear search for now
    void insert(const int key, const Record record) {

        
        std::deque<page_id_t> path;
        page_id_t x_id = ROOT_PAGE_ID;
        
        while (true) {
            
            #ifdef LOG_BP_TREE
            log.add_op(BPTreeLog::Operation::INSERT, x_id);
            #endif

            BPTreeNode x{x_id, header};
            if (x.is_full() == AT_CAPACITY || x.is_full() == PAST_CAPACITY) {
                x.split_node(path);
                // Go back up to parent incase x is no longer valid. i.e. when inserting 5, x == [1, 2, 3, 4] -> x == [1, 2], y == [3, 4], you must go back up to the parent and then insert into y instead.
                if (path.size() >= 1) {
                    const page_id_t parent_pid = path.front();
                    assert(parent_pid != 0 && parent_pid < MAX_SLOTS);
                    x_id = parent_pid;
                    x.discount_ass_copy_assignment(x_id);
                }
            }
            
            const BPTreeNodeType type = x.header.get_type();
            assert(type != LEAF);
            
            if (type == BRANCH) {
                x.insert_into_branch(key, record);
                return;
            } else { // X is an intermediate node, recurse to find leaf node that can contain key and record
                const int n = x.header.get_n();
                assert(n >= 1);
                int* const keys_begin = x.header.get_int_keys_begin();
                int i = 0;
                while (i < n - 1) {
                    const int  k = keys_begin[i];
                    const bool g_o_e = key >= k;
                    if (!g_o_e) { break; }
                    i++;
                }

                const page_id_t x_child = x.index_page_back(i);
                STACK_TRACE_ASSERT(x_child != 0);

                path.emplace_front(x_id);
                x_id = x_child;
            }
        }
    }

    void update(const int key, const Record record) {

        std::deque<page_id_t> path;
        page_id_t x_id = ROOT_PAGE_ID;
        
        while (true) {
            
            BPTreeNode x{x_id, header};
            
            const BPTreeNodeType type = x.header.get_type();
            assert(type != LEAF);
            
            if (type == BRANCH) {
                x.update_branch(key, record);
                return;
            } else { // X is an intermediate node, recurse to find leaf node that can contain key and record
                const int n = x.header.get_n();
                assert(n >= 1);
                int* const keys_begin = x.header.get_int_keys_begin();
                int i = 0;
                while (i < n - 1) {
                    const int  k = keys_begin[i];
                    const bool g_o_e = key >= k;
                    if (!g_o_e) { break; }
                    i++;
                }

                const page_id_t x_child = x.index_page_back(i);
                STACK_TRACE_ASSERT(x_child != 0);

                path.emplace_front(x_id);
                x_id = x_child;
            }
        }
    }

    void delete_key(const int key) {

        std::deque<page_id_t> path;
        page_id_t x_id = ROOT_PAGE_ID;
        int self_index = 0; // For resitributions and merges

        while (true) {
            
            BPTreeNode x{x_id, header};

            // TODO;
            // lazy merge/spill because they can propgate up to the parent
            // Re-distribute/Merge if too small //
            // Don't touch root
            const int n = x.header.get_n();
            const int branching_factor = header.get_branching_factor();
            if (x_id != ROOT_PAGE_ID && n < branching_factor / 2) {
                bool ok = x.branch_redistribute(path, self_index);
                if (!ok) {
                    ok = x.branch_merge(path);
                    if (!ok) {
                        FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("idk");
                    }
                }

                // Go back up
                const page_id_t parent_pid = path.front();
                assert(parent_pid != 0 && parent_pid < MAX_SLOTS);
                x_id = parent_pid;
                x.discount_ass_copy_assignment(x_id);
            } 

            const BPTreeNodeType type = x.header.get_type();
            assert(type != LEAF);
            
            if (type == BRANCH) {
                x.delete_from_branch(key);
                return;
            } else { // X is an intermediate node, recurse to find leaf node that can contain key and record
                assert(n >= 1);
                int* const keys_begin = x.header.get_int_keys_begin();
                int i = 0;
                while (i < n - 1) {
                    const int  k = keys_begin[i];
                    const bool g_o_e = key >= k;
                    if (!g_o_e) { break; }
                    i++;
                }

                self_index = i;
                const page_id_t x_child = x.index_page_back(i);
                STACK_TRACE_ASSERT(x_child != 0);

                path.emplace_front(x_id);
                x_id = x_child;
            }
        }
    }

    // Uses linear search for now
    [[nodiscard]] std::optional<Record> search(const int key) const noexcept{

        page_id_t x_id = ROOT_PAGE_ID;
        
        while (true) {
            
            BPTreeNode x{x_id, header};
            
            const int n = x.header.get_n();
            if (n == 0) { return std::nullopt; }
            const BPTreeNodeType type = x.header.get_type();
            assert(type != LEAF);
            
            if (type == BRANCH) {
                assert(n >= 1);
                int* const keys_begin = x.header.get_int_keys_begin();
                int i = 0;
                bool found = false;
                while (i < n) {
                    const int index = i * 3;
                    const int k = keys_begin[index];
                    if (k == key) { found = true; break; }
                    i++;
                }

                if (!found) { return std::nullopt; }

                const page_id_t c_pid         = keys_begin[i * 3 + 1]; // [key, pid, offset]
                const int       record_offset = keys_begin[i * 3 + 2]; // [key, pid, offset]
                BPTreeNode child{c_pid, header};
                STACK_TRACE_ASSERT(child.header.get_type() == LEAF);

                return Record{child.data + record_offset};
            } else { // X is an intermediate node, recurse to find leaf container node that can insert key
                assert(n >= 1);
                int* const keys_begin = x.header.get_int_keys_begin();
                int i = 0;
                while (i < n - 1) {
                    const int  k = keys_begin[i];
                    const bool g_o_e = key >= k;
                    if (!g_o_e) { break; }
                    i++;
                }

                const page_id_t x_child = x.index_page_back(i);

                x_id = x_child;
            }
        }
    }

    void pretty_print() const {
        // Clear screen

        // Print page 0 metadata
        std::vector<std::vector<char>> screen;
        std::vector<bptree_pretty_print_color> page_colors;
        
        constexpr int page_width  = 12;
        constexpr int page_height = 8;

        static_assert(page_width > 2);
        static_assert(page_height > 2);

        screen.resize(page_height);

        std::deque<int> offsets; // For leaf nodes

        const BPTreeNode MOCK_NODE{1, header};
        pretty_print_create_rectangle(page_width, page_height, screen, page_colors, WHITE, 0, 0, MOCK_NODE, offsets);
        // pretty_print_print_rectangles(page_width, page_height, screen, page_colors, 0);

        // Print root and nodes
        int max = 1;
        std::deque<page_id_t> q;
        q.emplace_front(1);
        while (!q.empty()) {

            const page_id_t pid = q.front(); q.pop_front();
            max++;
            const BPTreeNode node{pid, header};

            const int  n    = node.header.get_n();
            const auto type = node.header.get_type();
            
            if (type == LEAF) {
                pretty_print_create_rectangle(page_width, page_height, screen, page_colors, GREEN, max, pid, node, offsets);
            } else if (type == BRANCH) {
                pretty_print_create_rectangle(page_width, page_height, screen, page_colors, LIGHT_BROWN, max, pid, node, offsets);

                assert(offsets.size() == 0);
                int* const keys_begin = node.header.get_int_keys_begin();
                std::unordered_set<page_id_t> unique;
                for (int i = 0; i < n; i++) {
                    const int index = i * 3;
                    const page_id_t c_pid = keys_begin[index + 1]; // [key, pid, offset];
                    const int       off   = keys_begin[index + 1];
                    unique.emplace(c_pid);
                    offsets.emplace_front(off);
                }

                for (const page_id_t& c_pid: unique) { 
                    q.emplace_front(c_pid); }

            } else { // Intermediate
                pretty_print_create_rectangle(page_width, page_height, screen, page_colors, BROWN, max, pid, node, offsets);
                for (int i = 0; i < n; i++) {
                    const page_id_t c_pid = node.index_page_back(i);
                    q.emplace_front(c_pid);
                }
            }
        }

        pretty_print_print_rectangles(page_width, page_height, screen, page_colors, max);

    }

    void print_bytes() const {
        std::deque<page_id_t> pids{root.page_id};
        while (!pids.empty()) {
            const page_id_t pid = pids.front(); pids.pop_front();
            BPTreeNode node{pid, header};
            const auto type = node.header.get_type();
            node.print_bytes();
            // Leaf just prints
            if (type == BRANCH) {
                const int n = node.header.get_n();
                int* const keys_begin = node.header.get_int_keys_begin();
                std::set<page_id_t> new_pids;
                for (int i = 0; i < n; i++) {
                    const int index = i * 3;
                    const page_id_t pid = keys_begin[index + 1]; // [key, pid, offset]
                    new_pids.emplace(pid);
                }
                for (const page_id_t pid : new_pids) {
                    pids.emplace_front(pid);
                }
            } else if (type == INTERMEDIATE) {
                const int n = node.header.get_n();
                std::set<page_id_t> new_pids;
                for (int i = 0; i < n; i++) {
                    const page_id_t pid =  node.index_page_back(i);
                    new_pids.emplace(pid);
                }
                for (const page_id_t pid : new_pids) {
                    pids.emplace_front(pid);
                }
            }
        }
    }
};



class RecordValidator {
    public:
    std::map<int, Record> key_record_pairs;
    BPTree& tree;

    RecordValidator(BPTree& tree) : tree(tree) {}

    void insert(const int key, const Record record) {
        key_record_pairs.emplace(key, record);
    }

    void remove_key(const int key) {
        key_record_pairs.erase(key);
    }

    bool update(const int key, const Record record) {
        if (key_record_pairs.find(key) == key_record_pairs.end()) {
            std::cerr << "\nValidator: Tried to update a key (" << key << ") that didn't exist\n";
            return false;
        }
        key_record_pairs.insert_or_assign(key, record);
        return true;
    }

    [[nodiscard]] bool validate_records_exist() const noexcept {
        for (const auto& p : key_record_pairs) {
            const int    key    = p.first;
            const Record record = p.second;

            const std::optional<Record> record_location = tree.search(key);
            if (!record_location.has_value()) {
                std::cerr << "\nValidator: Could not find record for key " << key << " even though it exists\n"; 
                return false;
            } else {
                const Record got = record_location.value();
                const unsigned int got_size    = got.header.size;
                const unsigned int record_size = record.header.size;
                const unsigned int got_type    = got.header.type;
                const unsigned int record_type = record.header.type;
                const std::string  got_str{got.data, got_size}; 
                const std::string  record_str{record.data, record_size}; 
                if (got_size != record_size) {
                    std::cerr << "\nValidator: Found record for key " << key << " but the contained value had an incorrect SIZE."
                        << "Expected (" << record_str << ", " << record_size << "), got (" << got_str << ", " << got_size << ")\n"; 
                    return false;
                } else if (got_type != record_type) {
                    std::cerr << "\nValidator: Found record for key " << key << " but the contained value had an incorrect TYPE."
                        << "Expected (" << record_str << ", " << record_size << "), got (" << got_str << ", " << got_size << ")\n"; 
                    return false;
                } else if (got_str != record_str) {
                    std::cerr << "\nValidator: Found record for key " << key << " but it contained the wrong value. "
                        << "Expected (" << record_str << "), got (" << got_str << ")\n"; 
                    return false;   
                }
                // std::cout << "\nRecord found = " << *record_location << std::endl;
            }
        }
        return true;
    }

    bool validate(const int key, const Record record) noexcept {
        insert(key, record);
        return validate_records_exist(); 
    }

    bool validate() const noexcept {
        return validate_records_exist(); 
    }
};



