#pragma once

#include <filesystem>
#include <source_location>
#include <array>
#include <stdexcept>
#include <string>
#include <sstream>
#include <iostream>
#include <thread>
#include <chrono> 


#define CUR_LOC std::source_location::current()



std::string get_relative_path(const std::string& file_path);

std::string get_function_name(std::string name);

class StackTrace {
private:
    static constexpr size_t MAX_FRAMES = 50;
    
    struct FrameInfo {
        void* address;
        std::string binary_name;
        std::string function_name;
        std::string source_file;
        int line_number;
        size_t offset;
        uintptr_t relative_addr;
    };
    
    static std::string demangle_symbol(const char* mangled);
    
    static std::pair<std::string, std::string> get_source_and_function(void* addr, const char* binary_path);
    
    public:
    static std::string capture(size_t max_depth);

};

// Need max_depth cause slow
std::string get_stack_trace(const size_t max_depth = 50);




#define FILE_NAME_STR \
    std::filesystem::path(CUR_LOC.file_name()).filename().string()

#define GET_FILE_NAME_STR(x) \
    std::filesystem::path((x).file_name()).filename().string()

#define GET_ERROR_LOCATION(loc) \
    get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column() 

#define GET_FORMATTED_LOCATION(loc) \
    get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column() 



struct note {
    int freq;
    int dur;
};

void FATAL_ERROR_EXIT(const std::string& msg, std::source_location loc) noexcept;                

void FATAL_ERROR_THROW(const std::string& msg, std::source_location loc);

void FATAL_ERROR_STACK_TRACE_THROW(const std::string& msg, std::source_location loc);

void FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC_inner(const std::string& msg, std::string stack_trace, std::source_location loc = std::source_location::current());
#define FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC(msg) \
    do { FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC_inner(msg, get_stack_trace()); } while (0)                     


void FATAL_ERROR_STACK_TRACE_EXIT(const std::string& msg, std::source_location loc) noexcept;

void FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC_inner(const std::string& msg, std::string stack_trace, std::source_location loc = std::source_location::current()) noexcept;
#define FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC(msg) \
    do { FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC_inner(msg, get_stack_trace(), std::source_location::current()); } while (0)           

void STACK_TRACE_ASSERT_inner(const bool x, std::string assertion, std::string stack_trace, std::source_location loc) noexcept;
#define STACK_TRACE_ASSERT(x) \
    if (!(x)) { STACK_TRACE_ASSERT_inner(x, #x, get_stack_trace(), std::source_location::current()); }               



void STACK_TRACE_EXPECT_inner(auto x, auto y, std::string x_str, std::string y_str, std::string stack_trace, std::source_location loc) noexcept {
    std::cerr << "\n\nEXPECT FAIL: " << x_str << " == " << y_str << ". Expected (" << x << "), got (" << y << ") . In function "
            << (loc).function_name() << "\n";
    std::cerr << stack_trace << "\n";
    constexpr int dur = 80;
    constexpr std::array<note, 5> notes{{{349, dur}, {523, dur}, {493, dur}, {440, dur}, {415, dur}}};
    for (const auto& note : notes) {    
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    exit(1);
}
#define STACK_TRACE_EXPECT(expect, other)           \
    do {                                \
    if ((expect) != (other)) {                           \
        STACK_TRACE_EXPECT_inner((expect), (other), #expect, #other, get_stack_trace(), std::source_location::current());    \
    }           \
    } while (0)                     

