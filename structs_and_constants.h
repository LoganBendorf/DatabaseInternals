
#pragma once

#include <cstddef>
#include "Types.h"

enum SQL_data_type : unsigned int { INT, FLOAT, VARCHAR };

enum RecordType : unsigned short { NULL_T, INT8_T, INT16_T, INT32_T, INT64_T, BLOB_T, STR_T };

enum NodeFullStatus : unsigned int { NOT_FULL, AT_CAPACITY, PAST_CAPACITY, BYTES_FULL };

enum BPTreeNodeType : int { INTERMEDIATE, BRANCH, LEAF};

using key = int;
using page_id_t = int;

constexpr size_t kib = 1024;

constexpr page_id_t ROOT_PAGE_ID = 1;

constexpr size_t G_PAGE_SIZE = 1024 / 8;

constexpr int MAX_SLOTS = 10000;

#include <iostream>
#include <cstring>
constexpr int RECORD_HEADER_SIZE = 8;
struct Record {
    struct Header {
        unsigned int type; // TODO: convert to std::bitset<32>. Bitmap for NULL columns, max 32 columns ig
        unsigned int size; // Size is only the size of the actually record data. Does not include the header. e.q. real_size == size + RECORD_HEADER_SIZE;
    };  
    Header header;
    char* data;

    explicit Record(unsigned int type, unsigned int size, char* data) : header(type, size), data(data) {};
    explicit Record(char* record) : header(), data(record + RECORD_HEADER_SIZE) {
        std::memcpy(&header, record, RECORD_HEADER_SIZE);
    };

    friend std::ostream& operator<<(std::ostream& os, const Record& r) {
        os << "(type="  << r.header.type
           << ", size=" << r.header.size
           << ", data=";
        if (r.data == nullptr) {
            os << "NULLPTR)";
        } else {
            os <<std::string(r.data, r.header.size) << ")";
        }
        return os;
    }
};

constexpr int FREEBLOCK_SIZE = 4;
struct FreeBlock {
    offset_t next_offset{0};
    unsigned short size{}; // (remaining) size includes the 4 bytes of this class. i.e total free space
};