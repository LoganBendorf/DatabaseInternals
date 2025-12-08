#pragma once

#include "structs_and_constants.h"

#include <mutex>
#include <string>

void clear_screen() __attribute__((used));

[[nodiscard]] std::string BPTreeNodeType_to_string(BPTreeNodeType type);

void g_print_bytes(const page_id_t pid, const size_t branching_factor) noexcept;

extern std::mutex thread_log_mu;

void thread_print(std::string msg) noexcept;
// #define THREAD_PRINT(x) \
//     do {\
//     thread_log_mu.lock(); \
//     thread_print(x); \
//     thread_log_mu.unlock();\
//     } while (0);
#define THREAD_PRINT(x) \
    do {\
    } while (0);
    

constexpr auto ASCII_RESET = "\033[0m";
constexpr auto ASCII_GREEN = "\033[32m";
constexpr auto ASCII_YELLOW = "\033[33m";
constexpr auto ASCII_BLUE = "\033[34m";

constexpr auto ASCII_BG_YELLOW = "\033[103m";
constexpr auto ASCII_BG_GREEN  = "\033[102m";
constexpr auto ASCII_BG_RED = "\033[100m";




class FastRandom_XORShift {
    uint64_t s[2]{};
    public:
    FastRandom_XORShift(uint64_t seed1 = 123456789, uint64_t seed2 = 987654321) {
        s[0] = seed1;
        s[1] = seed2;
    }
    
    uint64_t next() {
        uint64_t s1 = s[0];
        uint64_t s0 = s[1];
        s[0] = s0;
        s1 ^= s1 << 23;
        s[1] = s1 ^ s0 ^ (s1 >> 18) ^ (s0 >> 5);
        return s[0] + s[1];
    }
};




#include <exception>

class QueryFailException : public std::exception {
    const char* msg{};
    public:
    explicit QueryFailException(const char* msg) noexcept : msg(msg) {}

    // must be noexcept
    [[nodiscard]] const char* what() const noexcept override {
        return msg;
    }
};