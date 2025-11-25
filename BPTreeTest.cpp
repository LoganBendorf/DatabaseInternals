
#include "bptree.h"

#include <vector>
#include <random>
#include <set>


void insert_print() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);

    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    std::random_device rd; // seed
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> dist(0, 25); // range

    clear_screen();
    tree.pretty_print();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    
    const int num_inserts = 10;
    for (int i = 0; i < num_inserts; i++) {
        int num = dist(gen);
        const char c = num + 'a';
        const std::string record_str(1, c);   
        char* const data = const_cast<char* const>(record_str.data());
        const Record record{0, 1, data};
        tree.insert((i + 1), record);
        clear_screen();
        tree.pretty_print();
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

static constexpr std::string ASCII_BG_YELLOW = "\033[103m";
static constexpr std::string ASCII_BG_GREEN  = "\033[102m";
static constexpr std::string ASCII_RESET = "\033[0m";

void insert_and_validate(auto& tree, RecordValidator& validator, const int key, const Record record) {
    std::cout << ASCII_BG_YELLOW << "insert(" << key << ", " << record << ")" << ASCII_RESET << std::endl;
    tree.insert(key, record);
    tree.print_inorder();
    tree.print_bytes();
    if (! validator.validate(key, record)) { 
        std::cerr << "Bruh. Exiting...\n"; 
        exit(1); 
    }
}

void update_and_validate(auto& tree, RecordValidator& validator, const int key, const Record record) {
    std::cout << ASCII_BG_YELLOW << "update(" << key << ", " << record << ")" << ASCII_RESET << std::endl;
    tree.update(key, record);
    tree.print_inorder();
    tree.print_bytes();
    if (! validator.update(key, record) ) { 
        std::cerr << "Bruh. Exiting...\n"; 
        exit(1);  
    }
    if (! validator.validate() ) { 
        std::cerr << "Bruh. Exiting...\n"; 
        exit(1); 
    }
}

void delete_and_validate(auto& tree, RecordValidator& validator, const int key) {
    std::cout << ASCII_BG_YELLOW << "delete(" << key << ")" << ASCII_RESET << std::endl;
    tree.delete_key(key);
    validator.remove_key(key);
    tree.print_inorder();
    tree.print_bytes();
    if (! validator.validate() ) { 
        std::cerr << "Bruh. Exiting...\n"; 
        exit(1); 
    }
}


void random_test() noexcept {
    Record record{1162167621, 4, "kill"};
    Record record2{16843009, 2, "to"};

    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);

    auto tree = BPTree::create_tree(G_PAGE_SIZE, 6, fields);
    tree.print_inorder();
    tree.root.print_bytes();
    RecordValidator validator{tree};

    std::random_device rd; // seed
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int> len_dst(1, 15); // range
    std::uniform_int_distribution<int> char_dist(0, 25); // range
    std::uniform_int_distribution<int> method_dist(0, 2); // range

    std::vector<char*> free_list;
    int insert_key = 100;
    std::set<int> used_keys;
    const int num_ops = 15;
    for (int i = 0; i < num_ops; i++) {
        const unsigned int size = len_dst(gen);
        char* data = new char[size];
        free_list.emplace_back(data);
        for (int j = 0; j < size; j++) {
            data[j] = 'a' + char_dist(gen);
        }
 
        const Record record{1162167621, size, data};
        const int method = method_dist(gen);
        switch (method) {
            case 0: { // Insert
                insert_key++;
                insert_and_validate(tree, validator, insert_key, record);
                used_keys.emplace(insert_key);
                break;
            } case 1: { // Delete
                if (used_keys.size() == 0) { continue; }
                const int index = std::rand() % used_keys.size(); 
                auto it = used_keys.begin();
                std::advance(it, index);
                const int key = *it;
                delete_and_validate(tree, validator, key);
                used_keys.erase(key);
                break;
            } case 2: { // Update
                if (used_keys.size() == 0) { continue; }
                const int index = std::rand() % used_keys.size(); 
                auto it = used_keys.begin();
                std::advance(it, index);
                const int key = *it;
                update_and_validate(tree, validator, key, record);
                break;
            } default:
                FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC("Shouldn't be here");
        }
        tree.print_inorder();
    }

    for (auto* data : free_list) {
        delete[] data;
    }
}

void test1() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 102, Record{1162167621, 3, "sdn"});
    update_and_validate(tree, validator, 102, Record{1162167621, 3, "tuz"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 5, "zzzhk"});
    update_and_validate(tree, validator, 102, Record{1162167621, 4, "sxmm"});
    delete_and_validate(tree, validator, 103);
    std::cout << ASCII_BG_GREEN << "test1(): Pass" << ASCII_RESET << "\n";
}

void test2() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 102, Record{1162167621, 5, "mslqw"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 1, "f"});
    insert_and_validate(tree, validator, 104, Record{1162167621, 1, "i"});
    update_and_validate(tree, validator, 103, Record{1162167621, 4, "yooa"});
    update_and_validate(tree, validator, 103, Record{1162167621, 1, "s"});
    std::cout << ASCII_BG_GREEN << "test2(): Pass" << ASCII_RESET << "\n";
}

void test3() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 101, Record{1162167621, 5, "gdfwx"});
    insert_and_validate(tree, validator, 102, Record{1162167621, 4, "ugrk"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 4, "fjhk"});
    delete_and_validate(tree, validator, 103);
    insert_and_validate(tree, validator, 104, Record{1162167621, 5, "wtjcc"});
    // delete_and_validate(tree, validator, 104);
    std::cout << ASCII_BG_GREEN << "test3(): Pass" << ASCII_RESET << "\n";
}

void test4() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 101, Record{1162167621, 14, "ucwoevwazyfqak"});
    update_and_validate(tree, validator, 101, Record{1162167621, 9, "adqdxmypj"});
    insert_and_validate(tree, validator, 102, Record{1162167621, 11, "fpwaxdydbxg"});
    update_and_validate(tree, validator, 101, Record{1162167621, 6, "rflgaw"});
    update_and_validate(tree, validator, 102, Record{1162167621, 7, "tmkigls"});
    update_and_validate(tree, validator, 102, Record{1162167621, 2, "ok"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 4, "tqyv"});
    delete_and_validate(tree, validator, 103);
    update_and_validate(tree, validator, 102, Record{1162167621, 12, "cvzabdiwlpxo"});
    update_and_validate(tree, validator, 101, Record{1162167621, 9, "raayvppim"});
    insert_and_validate(tree, validator, 104, Record{1162167621, 7, "jpdeody"});

    std::cout << ASCII_BG_GREEN << "test4(): Pass" << ASCII_RESET << "\n";
}


void test5() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 101, Record{1162167621, 4, "aaaa"});
    insert_and_validate(tree, validator, 102, Record{1162167621, 4, "bbbb"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 4, "cccc"});
    insert_and_validate(tree, validator, 104, Record{1162167621, 4, "dddd"});
    insert_and_validate(tree, validator, 105, Record{1162167621, 4, "eeee"});

    std::cout << ASCII_BG_GREEN << "test5(): Pass" << ASCII_RESET << "\n";
}

void test6() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 101, Record{1162167621, 11, "cxmtvdrlofv"});
    delete_and_validate(tree, validator, 101);
    insert_and_validate(tree, validator, 102, Record{1162167621, 9, "jpcahufqt"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 13, "yksnstsfsyqzn"});
    insert_and_validate(tree, validator, 104, Record{1162167621, 8, "sbihfamz"});
    update_and_validate(tree, validator, 103, Record{1162167621, 3, "tao"});
    update_and_validate(tree, validator, 102, Record{1162167621, 4, "qqkr"});
    delete_and_validate(tree, validator, 103);
    update_and_validate(tree, validator, 104, Record{1162167621, 13, "girvuzvmmjjrm"});
    insert_and_validate(tree, validator, 105, Record{1162167621, 14, "swxkwtwzgmfdto"});
    delete_and_validate(tree, validator, 104);
    insert_and_validate(tree, validator, 106, Record{1162167621, 15, "ueldmisijldqodz"});
    insert_and_validate(tree, validator, 107, Record{1162167621, 8, "yhhocufx"});

    std::cout << ASCII_BG_GREEN << "test6(): Pass" << ASCII_RESET << "\n";
}

void test7() {
    std::vector<SQL_data_type> fields;
    fields.emplace_back(VARCHAR);
    BPTree tree = BPTree::create_tree(G_PAGE_SIZE, 4, fields);
    RecordValidator validator{tree};

    insert_and_validate(tree, validator, 101, Record{1162167621, 12, "smowbvdlutzg"});
    update_and_validate(tree, validator, 101, Record{1162167621, 4, "jwti"});
    update_and_validate(tree, validator, 101, Record{1162167621, 5, "dzujv"});
    update_and_validate(tree, validator, 101, Record{1162167621, 3, "int"});
    insert_and_validate(tree, validator, 102, Record{1162167621, 14, "eosivnpmahjbux"});
    insert_and_validate(tree, validator, 103, Record{1162167621, 13, "dvzbvbcstafpn"});
    insert_and_validate(tree, validator, 104, Record{1162167621, 3, "ofc"});
    insert_and_validate(tree, validator, 105, Record{1162167621, 11, "ltfjxldlobt"});
    update_and_validate(tree, validator, 101, Record{1162167621, 2, "bb"});
    update_and_validate(tree, validator, 104, Record{1162167621, 11, "jrneudcnojc"});
    insert_and_validate(tree, validator, 106, Record{1162167621, 13, "fzqvqjpqvlisi"});
    insert_and_validate(tree, validator, 107, Record{1162167621, 6, "lexhrv"});
    std::cout << ASCII_BG_GREEN << "test7(): Pass" << ASCII_RESET << "\n";
}


void bp_tree_test() {
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();
    // return;
    // clear_screen();
    random_test(); 
}