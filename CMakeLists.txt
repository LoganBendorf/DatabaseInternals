cmake_minimum_required(VERSION 3.28)

# Use Clang 20 compiler
set(CMAKE_CXX_COMPILER clang++-20)
set(CMAKE_C_COMPILER clang-20)

project(squel LANGUAGES CXX)

# Make CMake emit compile_commands.json in the build directory
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# C++ standard
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use libc++ instead of libstdc++
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -stdlib=libc++ -lc++abi")

# Default build type if none provided
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
endif()

# Compiler flags
add_compile_options(-g -O1 -Wno-writable-strings)
# add_compile_options(-g -O2 -fsanitize=address,undefined,signed-integer-overflow -Wno-writable-strings)

# Linker option
# set(SANITIZER_FLAGS "-fsanitize=address,undefined,signed-integer-overflow")
# 
# LTO
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)

# Source and include directories (match your Makefile: ".", "Disk", "Bp-Tree")
set(SRC_DIRS
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/Disk
  ${PROJECT_SOURCE_DIR}/Bp-Tree
  ${PROJECT_SOURCE_DIR}/Lib
)

# Add include paths
include_directories(${SRC_DIRS})

# Collect .cpp files from the source directories (non-recursive)
file(GLOB SRCS
  "${PROJECT_SOURCE_DIR}/*.cpp"
  "${PROJECT_SOURCE_DIR}/Disk/*.cpp"
  "${PROJECT_SOURCE_DIR}/Bp-Tree/*.cpp"
)

# Main executable
add_executable(main ${SRCS})


# Coz profiler
# target_compile_options(main PRIVATE -gdwarf-4)
# target_link_libraries(main PRIVATE dl pthread profiler)


# Color
if(NOT DEFINED CMAKE_COLOR_MAKEFILE)
  set(CMAKE_COLOR_MAKEFILE ON)
endif()
# helper: commonly supported flags for colored diagnostics
set(COLOR_FLAGS "-fdiagnostics-color=always" "-fcolor-diagnostics")
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(main PRIVATE ${COLOR_FLAGS})
endif()

# Ensure the executable is written into the build directory
set_target_properties(main PROPERTIES
  RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

# Apply linker sanitizer flags (CMake 3.13+ supports target_link_options)
if (COMMAND target_link_options)
  target_link_options(main PRIVATE ${SANITIZER_FLAGS})
else()
  # fallback
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${SANITIZER_FLAGS}")
endif()



# Also propagate compile options to the target (in case add_compile_options wasn't enough)
# target_compile_options(main PRIVATE -g -Wno-writable-strings -fsanitize=undefined,signed-integer-overflow)