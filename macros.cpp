#include "macros.h"
#include <cxxabi.h>
#include <execinfo.h>
#include <dlfcn.h>

std::string get_relative_path(const std::string& file_path) {
    try {
        return std::filesystem::relative(file_path, "/home/logan/CodeProjects/C++Sandbox").string();
    } catch (const std::exception&) {
        // Fallback to just filename if relative path fails
        return std::filesystem::path(file_path).filename().string();
    }
}

std::string get_function_name(std::string name) {
    // Remove return type
    const size_t first_space = name.find(' ');
    if (first_space != std::string::npos) {
        name = name.substr(first_space + 1);
    }

    // Remove parameters
    const size_t first_paren = name.find('(');
    const size_t last_paren = name.rfind(')');
    if (first_paren != std::string::npos && last_paren != std::string::npos) {
        name = name.substr(0, first_paren + 1) + ")";
    }
    
    return name;
}



std::string StackTrace::demangle_symbol(const char* mangled) {
    if (mangled == nullptr) { return ""; }
    
    int status = -1;
    char* demangled = abi::__cxa_demangle(mangled, nullptr, nullptr, &status);
    
    if (status == 0 && demangled != nullptr) {
        std::string result(demangled);
        free(demangled);
        return result;
    }
    
    return {mangled};
}

    
std::pair<std::string, std::string> StackTrace::get_source_and_function(void* addr, const char* binary_path) {
    if (binary_path == nullptr) { return {"", ""}; }
    
    Dl_info info;
    uintptr_t relative_addr = 0;
    
    if (dladdr(addr, &info) != 0) {
        relative_addr = std::bit_cast<uintptr_t>(addr) - std::bit_cast<uintptr_t>(info.dli_fbase);
    } else {
        relative_addr = std::bit_cast<uintptr_t>(addr);
    }
    
    std::stringstream cmd_ss;
    cmd_ss << "addr2line -C -f -e " << binary_path << " 0x" << std::hex << relative_addr << " 2>/dev/null";
    const std::string cmd = cmd_ss.str();
    // const std::string cmd = std::format("addr2line -C -f -e {} 0x{:x} 2>/dev/null", binary_path, relative_addr);

    // FILE* pipe = popen(cmd.str().c_str(), "r");
    FILE* pipe = popen(cmd.c_str(), "r");
    if (pipe == nullptr) { return {"", ""}; }
    
    std::array<char, size_t(1024) * 2> buffer{};
    std::string function_name;
    function_name.reserve(32);
    std::string source_location;
    source_location.reserve(32);
    
    // First line is function name
    if (fgets(buffer.data(), sizeof(buffer), pipe) != nullptr) {
        function_name = buffer.data();
        if (!function_name.empty() && function_name.back() == '\n') {
            function_name.pop_back();
        }
        
        // Second line is file:line
        if (fgets(buffer.data(), sizeof(buffer), pipe) != nullptr) {
            source_location = buffer.data();
            if (!source_location.empty() && source_location.back() == '\n') {
                source_location.pop_back();
            }
        }
    }
    
    pclose(pipe);
    
    // Clean up function name if it's valid
    if (function_name == "??" || function_name.empty()) [[unlikely]] {
        function_name = "";
    }
    
    // Clean up source location if it's invalid
    if (source_location == "??:0" || source_location == "??:?" || source_location.empty()) [[unlikely]] {
        source_location = "";
    }
    
    return {function_name, source_location};
}
    

std::string StackTrace::capture(size_t max_depth) {

    void* array[MAX_FRAMES];
    const int size_as_int = backtrace(array, MAX_FRAMES); // C func
    const size_t size = static_cast<size_t>(size_as_int);
    

    
    constexpr size_t skip_useless_trace_info_size = 2;
    constexpr size_t initializer_junk_size = 3;

    max_depth += skip_useless_trace_info_size;

    size_t iterations = size - initializer_junk_size;
    iterations = iterations > max_depth ? max_depth : iterations; 


    std::string out;
    out.reserve(size_t(1024) * 2);
    // out += "Detailed stack trace (" + std::to_string(max_depth) + " frames):\n";
    out += (std::string(80, '=') + "\n");
    
    for (size_t i = skip_useless_trace_info_size; i < iterations && size > skip_useless_trace_info_size; i++) {
        Dl_info info;
        // ss << std::setw(2) << i << ": ";
        // ss << "0x" << std::hex << std::setfill('0') << std::setw(12) 
        //    << (uintptr_t)array[i] << std::dec << std::setfill(' ');
        
        if (dladdr(array[i], &info) != 0) [[likely]] {
            // uintptr_t relative_addr = (uintptr_t)array[i] - (uintptr_t)info.dli_fbase;
            // ss << " (+0x" << std::hex << relative_addr << std::dec << ")";
            
            if (info.dli_sname != nullptr) {
                const std::string demangled = demangle_symbol(info.dli_sname);
                // ss << " in " << demangled;
                out += (demangled);
                
                if (info.dli_saddr != nullptr) {
                    // uintptr_t offset = (uintptr_t)array[i] - (uintptr_t)info.dli_saddr;
                    // ss << " + " << offset;
                }
            } else {
                // Try addr2line for static functions
                const auto [func_name, source_loc] = get_source_and_function(array[i], info.dli_fname);
                if (!func_name.empty()) {
                    out += (func_name);
                } else {
                    out += (" in <unknown>");
                }
            }
            
            // ss << "\n    from " << (info.dli_fname ? info.dli_fname : "<unknown>");
            
            // Try to get source location
            const auto [func_name, source_loc] = get_source_and_function(array[i], info.dli_fname);
            if (!source_loc.empty()) {
                out += "\n    at " + source_loc;
            }
        } else {
            out += (" in <unknown>\n    from <unknown>");
        }
        
        out.push_back('\n');
    }
    
    return out;
}

std::string get_stack_trace(const size_t max_depth) {
    return StackTrace::capture(max_depth);
}



// Asserts and what not

static constexpr int dur = 80;

static constexpr std::array<note, 5> notes{{{349, dur}, {523, dur}, {493, dur}, {440, dur}, {415, dur}}};


void FATAL_ERROR_EXIT(const std::string& msg, std::source_location loc) noexcept {
    std::cerr << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column() 
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    exit(1);
}                

void FATAL_ERROR_THROW(const std::string& msg, std::source_location loc) {
    std::stringstream full_error;
    full_error << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column()
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    throw std::runtime_error(full_error.str());
}

void FATAL_ERROR_STACK_TRACE_THROW(const std::string& msg, std::source_location loc) {
    std::stringstream full_error;
    full_error << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column()
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    full_error << get_stack_trace() << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    throw std::runtime_error(full_error.str());
}

void FATAL_ERROR_STACK_TRACE_THROW_CUR_LOC_inner(const std::string& msg, std::string stack_trace, std::source_location loc) {
    std::stringstream full_error;
    full_error << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column()
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    full_error << stack_trace << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    throw std::runtime_error(full_error.str());
}

void FATAL_ERROR_STACK_TRACE_EXIT(const std::string& msg, std::source_location loc) noexcept {
    std::cerr << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column()
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    std::cerr << get_stack_trace() << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";\
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    exit(1);
}

void FATAL_ERROR_STACK_TRACE_EXIT_CUR_LOC_inner(const std::string& msg, std::string stack_trace, std::source_location loc) noexcept {
    std::cerr << get_relative_path((loc).file_name()) << ":" << (loc).line() << ":" << (loc).column()
              << ": FATAL ERROR: " << msg << ". In function "
              << (loc).function_name() << "\n";
    std::cerr << stack_trace << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";\
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    exit(1);
}

void STACK_TRACE_ASSERT_inner(const bool x, std::string assertion, std::string stack_trace, std::source_location loc) noexcept {
    if (x) { return; }

    std::cerr << "\n\nASSERT FAIL: " << assertion << ". In function "
            << (loc).function_name() << "\n";
    std::cerr << stack_trace << "\n";
    for (const auto& note : notes) {
        std::stringstream system_stream;
        system_stream << "play -n synth " << (note.dur / 1000.0) << " sine " << note.freq << " triangle " << ((note.freq * 3) / 2) << " vol 0.12 2>/dev/null &";
        std::system(system_stream.str().c_str());
        std::this_thread::sleep_for(std::chrono::milliseconds(note.dur * 3));
    }
    exit(1);
}
