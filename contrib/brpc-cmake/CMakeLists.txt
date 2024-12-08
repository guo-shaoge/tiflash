# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


set (_brpc_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/brpc")
set (_brpc_BINARY_DIR "${TiFlash_BINARY_DIR}/contrib/brpc")
set (GFLAGS_INCLUDE_PATH "${TiFlash_BINARY_DIR}/contrib/gflags/include")
set (LEVELDB_INCLUDE_PATH "${TiFlash_SOURCE_DIR}/contrib/leveldb/include")
# check grpc-cmake/CMakeLists.txt
set (PROTOBUF_INCLUDE_DIRS ${Protobuf_INCLUDE_DIR})
set (PROTOBUF_LIBRARIES ${Protobuf_LIBRARY})
set (PROTOC_LIB ${Protobuf_PROTOC_LIBRARY})
set (PROTOBUF_PROTOC_EXECUTABLE ${Protobuf_PROTOC_EXECUTABLE})
message ("gjt debug exec ${PROTOBUF_PROTOC_EXECUTABLE}")
message ("gjt debug exec ${PROTOBUF_INCLUDE_DIRS}")

# cmake_minimum_required(VERSION 2.8.12)
# project(brpc C CXX)

option(WITH_GLOG "With glog" OFF)
option(WITH_MESALINK "With MesaLink" OFF)
option(WITH_BORINGSSL "With BoringSSL" OFF)
option(DEBUG "Print debug logs" OFF)
option(WITH_DEBUG_SYMBOLS "With debug symbols" ON)
option(WITH_THRIFT "With thrift framed protocol supported" OFF)
option(WITH_SNAPPY "With snappy" OFF)
option(WITH_RDMA "With RDMA" OFF)
option(WITH_DEBUG_BTHREAD_SCHE_SAFETY "With debugging bthread sche safety" OFF)
option(WITH_DEBUG_LOCK "With debugging lock" OFF)
option(BUILD_UNIT_TESTS "Whether to build unit tests" OFF)
option(BUILD_FUZZ_TESTS "Whether to build fuzz tests" OFF)
option(BUILD_BRPC_TOOLS "Whether to build brpc tools" OFF)
option(DOWNLOAD_GTEST "Download and build a fresh copy of googletest. Requires Internet access." ON)

# Enable MACOSX_RPATH. Run "cmake --help-policy CMP0042" for policy details.
if(POLICY CMP0042)
    cmake_policy(SET CMP0042 NEW)
endif()

set(BRPC_VERSION 1.11.0)

SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "brpc authors")
INCLUDE(CPack)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    # require at least gcc 4.8
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8)
        message(FATAL_ERROR "GCC is too old, please install a newer version supporting C++11")
    endif()
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # require at least clang 3.3
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 3.3)
        message(FATAL_ERROR "Clang is too old, please install a newer version supporting C++11")
    endif()
else()
    message(WARNING "You are using an unsupported compiler! Compilation has only been tested with Clang and GCC.")
endif()

set(WITH_GLOG_VAL "0")
# if(WITH_GLOG)
#     set(WITH_GLOG_VAL "1")
#     set(BRPC_WITH_GLOG 1)
# endif()
# 
# if(WITH_DEBUG_SYMBOLS)
#     set(DEBUG_SYMBOL "-g")
# endif()
# 
set(WITH_DEBUG_LOCK_VAL "0")
# if(WITH_DEBUG_LOCK)
#     set(WITH_DEBUG_LOCK_VAL "1")
# endif()
# 
# if(WITH_THRIFT)
#     set(THRIFT_CPP_FLAG "-DENABLE_THRIFT_FRAMED_PROTOCOL")
#     find_library(THRIFT_LIB NAMES thrift)
#     if (NOT THRIFT_LIB)
#         message(FATAL_ERROR "Fail to find Thrift")
#     endif()
# endif()
# 
set(WITH_RDMA_VAL "0")
# if(WITH_RDMA)
#     set(WITH_RDMA_VAL "1")
# endif()
# 
set(WITH_DEBUG_BTHREAD_SCHE_SAFETY_VAL "0")
# if(WITH_DEBUG_BTHREAD_SCHE_SAFETY)
#     set(WITH_DEBUG_BTHREAD_SCHE_SAFETY_VAL "1")
# endif()

include(GNUInstallDirs)

configure_file(${_brpc_SOURCE_DIR}/config.h.in ${_brpc_SOURCE_DIR}/src/butil/config.h @ONLY)

list(APPEND CMAKE_MODULE_PATH "${_brpc_SOURCE_DIR}/cmake")

# find_package(GFLAGS REQUIRED)

execute_process(
    COMMAND bash -c "grep \"namespace [_A-Za-z0-9]\\+ {\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $2}' | tr -d '\n'"
    OUTPUT_VARIABLE GFLAGS_NS
)
if(${GFLAGS_NS} STREQUAL "GFLAGS_NAMESPACE")
    execute_process(
        COMMAND bash -c "grep \"#define GFLAGS_NAMESPACE [_A-Za-z0-9]\\+\" ${GFLAGS_INCLUDE_PATH}/gflags/gflags_declare.h | head -1 | awk '{print $3}' | tr -d '\n'"
        OUTPUT_VARIABLE GFLAGS_NS
    )
endif()

include_directories(
    ${_brpc_SOURCE_DIR}/src
    ${CMAKE_CURRENT_BINARY_DIR}
)

message ("gjt debug execute_process bef")
execute_process(
    COMMAND bash -c "${_brpc_SOURCE_DIR}/tools/get_brpc_revision.sh ${_brpc_SOURCE_DIR} | tr -d '\n'"
    OUTPUT_VARIABLE BRPC_REVISION
)
message ("gjt debug execute_process after")

if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    include(CheckFunctionExists)
    CHECK_FUNCTION_EXISTS(clock_gettime HAVE_CLOCK_GETTIME)
    if(NOT HAVE_CLOCK_GETTIME)
        set(DEFINE_CLOCK_GETTIME "-DNO_CLOCK_GETTIME_IN_MAC")
    endif()
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -Wno-deprecated-declarations -Wno-inconsistent-missing-override")
endif()

set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEFINE_CLOCK_GETTIME} -DBRPC_WITH_GLOG=${WITH_GLOG_VAL} -DBRPC_WITH_RDMA=${WITH_RDMA_VAL} -DGFLAGS_NS=${GFLAGS_NS} -DBRPC_DEBUG_BTHREAD_SCHE_SAFETY=${WITH_DEBUG_BTHREAD_SCHE_SAFETY_VAL} -DBRPC_DEBUG_LOCK=${WITH_DEBUG_LOCK_VAL}")
if(WITH_MESALINK)
    set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DUSE_MESALINK")
endif()
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} -DBTHREAD_USE_FAST_PTHREAD_MUTEX -D__const__=__unused__ -D_GNU_SOURCE -DUSE_SYMBOLIZE -DNO_TCMALLOC -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DBRPC_REVISION=\\\"${BRPC_REVISION}\\\" -D__STRICT_ANSI__")
set(CMAKE_CPP_FLAGS "${CMAKE_CPP_FLAGS} ${DEBUG_SYMBOL} ${THRIFT_CPP_FLAG}")
set(CMAKE_CXX_FLAGS "${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-invalid-offsetof -Wno-unused-parameter -fno-omit-frame-pointer")
set(CMAKE_C_FLAGS "${CMAKE_CPP_FLAGS} -O2 -pipe -Wall -W -fPIC -fstrict-aliasing -Wno-unused-parameter -fno-omit-frame-pointer")

macro(use_cxx11)
if(CMAKE_VERSION VERSION_LESS "3.1.3")
    if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
    endif()
else()
    set(CMAKE_CXX_STANDARD 11)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()
endmacro(use_cxx11)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    #required by butil/crc32.cc to boost performance for 10x
    if((CMAKE_SYSTEM_PROCESSOR MATCHES "(x86)|(X86)|(amd64)|(AMD64)") AND NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.4))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4 -msse4.2")
    elseif((CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64"))
        # segmentation fault in libcontext
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-gcse")
    endif()
    if(NOT (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 7.0))
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-aligned-new")
    endif()
endif()

# find_package(Protobuf REQUIRED)
# if(Protobuf_VERSION GREATER 4.21)
#     # required by absl
#     set(CMAKE_CXX_STANDARD 17)
# 
#     find_package(absl REQUIRED CONFIG)
#     set(protobuf_ABSL_USED_TARGETS
#         absl::absl_check
#         absl::absl_log
#         absl::algorithm
#         absl::base
#         absl::bind_front
#         absl::bits
#         absl::btree
#         absl::cleanup
#         absl::cord
#         absl::core_headers
#         absl::debugging
#         absl::die_if_null
#         absl::dynamic_annotations
#         absl::flags
#         absl::flat_hash_map
#         absl::flat_hash_set
#         absl::function_ref
#         absl::hash
#         absl::layout
#         absl::log_initialize
#         absl::log_severity
#         absl::memory
#         absl::node_hash_map
#         absl::node_hash_set
#         absl::optional
#         absl::span
#         absl::status
#         absl::statusor
#         absl::strings
#         absl::synchronization
#         absl::time
#         absl::type_traits
#         absl::utility
#         absl::variant
#     )
# else()
    use_cxx11()
    # endif()
find_package(Threads REQUIRED)

# find_path(LEVELDB_INCLUDE_PATH NAMES leveldb/db.h)
# find_library(LEVELDB_LIB NAMES leveldb)
# if ((NOT LEVELDB_INCLUDE_PATH) OR (NOT LEVELDB_LIB))
#     message(FATAL_ERROR "Fail to find leveldb")
# endif()

# if(WITH_SNAPPY)
#     find_path(SNAPPY_INCLUDE_PATH NAMES snappy.h)
#     find_library(SNAPPY_LIB NAMES snappy)
#     if ((NOT SNAPPY_INCLUDE_PATH) OR (NOT SNAPPY_LIB))
#         message(FATAL_ERROR "Fail to find snappy")
#     endif()
#     include_directories(${SNAPPY_INCLUDE_PATH})
# endif()
# 
# if(WITH_GLOG)
#     find_path(GLOG_INCLUDE_PATH NAMES glog/logging.h)
#     find_library(GLOG_LIB NAMES glog)
#     if((NOT GLOG_INCLUDE_PATH) OR (NOT GLOG_LIB))
#         message(FATAL_ERROR "Fail to find glog")
#     endif()
#     include_directories(${GLOG_INCLUDE_PATH})
# endif()
# 
# if(WITH_MESALINK)
#     find_path(MESALINK_INCLUDE_PATH NAMES mesalink/openssl/ssl.h)
#     find_library(MESALINK_LIB NAMES mesalink)
#     if((NOT MESALINK_INCLUDE_PATH) OR (NOT MESALINK_LIB))
#         message(FATAL_ERROR "Fail to find MesaLink")
#     else()
#         message(STATUS "Found MesaLink: ${MESALINK_LIB}")
#     endif()
#     include_directories(${MESALINK_INCLUDE_PATH})
# endif()
# 
# if(WITH_RDMA)
#     message("brpc compile with rdma")
#     find_path(RDMA_INCLUDE_PATH NAMES infiniband/verbs.h)
#     find_library(RDMA_LIB NAMES ibverbs)
#     if((NOT RDMA_INCLUDE_PATH) OR (NOT RDMA_LIB))
#         message(FATAL_ERROR "Fail to find ibverbs")
#     endif()
# endif()

# find_library(PROTOC_LIB NAMES protoc)
# if(NOT PROTOC_LIB)
#     message(FATAL_ERROR "Fail to find protoc lib")
# endif()

# if(WITH_BORINGSSL)
#     find_package(BoringSSL)
#     include_directories(${BORINGSSL_INCLUDE_DIR})
# else()
#     if(CMAKE_SYSTEM_NAME STREQUAL "Darwin" AND NOT OPENSSL_ROOT_DIR)
#         set(OPENSSL_ROOT_DIR
#             "/usr/local/opt/openssl" # Homebrew installed OpenSSL
#         )
#     endif()
# 
#     find_package(OpenSSL)
include_directories(${OPENSSL_INCLUDE_DIR})
# endif()

include_directories(
        ${GFLAGS_INCLUDE_PATH}
        ${PROTOBUF_INCLUDE_DIRS}
        ${LEVELDB_INCLUDE_PATH}
        )

set(DYNAMIC_LIB
    # ${GFLAGS_LIBRARY}
    gflags::gflags
    ${PROTOBUF_LIBRARIES} ${protobuf_ABSL_USED_TARGETS}
    # ${LEVELDB_LIB}
    leveldb
    ${PROTOC_LIB}
    ${CMAKE_THREAD_LIBS_INIT}
    ${THRIFT_LIB}
    dl
    z)

# if(WITH_BORINGSSL)
#     list(APPEND DYNAMIC_LIB ${BORINGSSL_SSL_LIBRARY})
#     list(APPEND DYNAMIC_LIB ${BORINGSSL_CRYPTO_LIBRARY})
# else()
#     list(APPEND DYNAMIC_LIB ${OPENSSL_CRYPTO_LIBRARY})
#     if(WITH_MESALINK)
#         list(APPEND DYNAMIC_LIB ${MESALINK_LIB})
#     else()
#         list(APPEND DYNAMIC_LIB ${OPENSSL_SSL_LIBRARY})
#     endif()
# endif()
# list(APPEND DYNAMIC_LIB ${OPENSSL_CRYPTO_LIBRARY})
list(APPEND DYNAMIC_LIB ${OPENSSL_LIBRARIES})

# if(WITH_RDMA)
#     list(APPEND DYNAMIC_LIB ${RDMA_LIB})
# endif()

set(BRPC_PRIVATE_LIBS "-lgflags -lprotobuf -lleveldb -lprotoc -lssl -lcrypto -ldl -lz")

# if(WITH_GLOG)
#     set(DYNAMIC_LIB ${DYNAMIC_LIB} ${GLOG_LIB})
#     set(BRPC_PRIVATE_LIBS "${BRPC_PRIVATE_LIBS} -lglog")
# endif()
# 
# if(WITH_SNAPPY)
#     set(DYNAMIC_LIB ${DYNAMIC_LIB} ${SNAPPY_LIB})
#     set(BRPC_PRIVATE_LIBS "${BRPC_PRIVATE_LIBS} -lsnappy")
# endif()

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(DYNAMIC_LIB ${DYNAMIC_LIB} rt)
    set(BRPC_PRIVATE_LIBS "${BRPC_PRIVATE_LIBS} -lrt")
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(DYNAMIC_LIB ${DYNAMIC_LIB}
        pthread
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop"
        "-Wl,-U,__Z13GetStackTracePPvii"
        "-Wl,-U,_mallctl"
        "-Wl,-U,_malloc_stats_print"
    )
endif()

# for *.so
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/output/lib)
# for *.a
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/output/lib)

# the reason why not using file(GLOB_RECURSE...) is that we want to
# include different files on different platforms.
set(BUTIL_SOURCES
    ${_brpc_SOURCE_DIR}/src/butil/third_party/dmg_fp/g_fmt.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/dmg_fp/dtoa_wrapper.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/dynamic_annotations/dynamic_annotations.c
    ${_brpc_SOURCE_DIR}/src/butil/third_party/icu/icu_utf.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/superfasthash/superfasthash.c
    ${_brpc_SOURCE_DIR}/src/butil/third_party/modp_b64/modp_b64.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/symbolize/demangle.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/symbolize/symbolize.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/snappy/snappy-sinksource.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/snappy/snappy-stubs-internal.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/snappy/snappy.cc
    ${_brpc_SOURCE_DIR}/src/butil/third_party/murmurhash3/murmurhash3.cpp
    ${_brpc_SOURCE_DIR}/src/butil/arena.cpp
    ${_brpc_SOURCE_DIR}/src/butil/at_exit.cc
    ${_brpc_SOURCE_DIR}/src/butil/atomicops_internals_x86_gcc.cc
    ${_brpc_SOURCE_DIR}/src/butil/base64.cc
    ${_brpc_SOURCE_DIR}/src/butil/base64url.cc
    ${_brpc_SOURCE_DIR}/src/butil/big_endian.cc
    ${_brpc_SOURCE_DIR}/src/butil/cpu.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/alias.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/asan_invalid_access.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/crash_logging.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/debugger.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/debugger_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/dump_without_crashing.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/proc_maps_linux.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/stack_trace.cc
    ${_brpc_SOURCE_DIR}/src/butil/debug/stack_trace_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/environment.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file_enumerator.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file_enumerator_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file_path.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/file_path_constants.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/memory_mapped_file.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/memory_mapped_file_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/scoped_file.cc
    ${_brpc_SOURCE_DIR}/src/butil/files/scoped_temp_dir.cc
    ${_brpc_SOURCE_DIR}/src/butil/file_util.cc
    ${_brpc_SOURCE_DIR}/src/butil/file_util_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/guid.cc
    ${_brpc_SOURCE_DIR}/src/butil/guid_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/hash.cc
    ${_brpc_SOURCE_DIR}/src/butil/lazy_instance.cc
    ${_brpc_SOURCE_DIR}/src/butil/location.cc
    ${_brpc_SOURCE_DIR}/src/butil/memory/aligned_memory.cc
    ${_brpc_SOURCE_DIR}/src/butil/memory/ref_counted.cc
    ${_brpc_SOURCE_DIR}/src/butil/memory/ref_counted_memory.cc
    ${_brpc_SOURCE_DIR}/src/butil/memory/singleton.cc
    ${_brpc_SOURCE_DIR}/src/butil/memory/weak_ptr.cc
    ${_brpc_SOURCE_DIR}/src/butil/posix/file_descriptor_shuffle.cc
    ${_brpc_SOURCE_DIR}/src/butil/posix/global_descriptors.cc
    ${_brpc_SOURCE_DIR}/src/butil/process_util.cc
    ${_brpc_SOURCE_DIR}/src/butil/rand_util.cc
    ${_brpc_SOURCE_DIR}/src/butil/rand_util_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/fast_rand.cpp
    ${_brpc_SOURCE_DIR}/src/butil/safe_strerror_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/sha1_portable.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/latin1_string_conversions.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/nullable_string16.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/safe_sprintf.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string16.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string_number_conversions.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string_split.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string_piece.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string_util.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/string_util_constants.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/stringprintf.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/utf_offset_string_conversions.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/utf_string_conversion_utils.cc
    ${_brpc_SOURCE_DIR}/src/butil/strings/utf_string_conversions.cc
    ${_brpc_SOURCE_DIR}/src/butil/synchronization/cancellation_flag.cc
    ${_brpc_SOURCE_DIR}/src/butil/synchronization/condition_variable_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/synchronization/waitable_event_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/non_thread_safe_impl.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/platform_thread_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/simple_thread.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_checker_impl.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_collision_warner.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_id_name_manager.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_local_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_local_storage.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_local_storage_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/thread_restrictions.cc
    ${_brpc_SOURCE_DIR}/src/butil/threading/watchdog.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/clock.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/default_clock.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/default_tick_clock.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/tick_clock.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/time.cc
    ${_brpc_SOURCE_DIR}/src/butil/time/time_posix.cc
    ${_brpc_SOURCE_DIR}/src/butil/version.cc
    ${_brpc_SOURCE_DIR}/src/butil/logging.cc
    ${_brpc_SOURCE_DIR}/src/butil/class_name.cpp
    ${_brpc_SOURCE_DIR}/src/butil/errno.cpp
    ${_brpc_SOURCE_DIR}/src/butil/find_cstr.cpp
    ${_brpc_SOURCE_DIR}/src/butil/status.cpp
    ${_brpc_SOURCE_DIR}/src/butil/string_printf.cpp
    ${_brpc_SOURCE_DIR}/src/butil/thread_local.cpp
    ${_brpc_SOURCE_DIR}/src/butil/thread_key.cpp
    ${_brpc_SOURCE_DIR}/src/butil/unix_socket.cpp
    ${_brpc_SOURCE_DIR}/src/butil/endpoint.cpp
    ${_brpc_SOURCE_DIR}/src/butil/fd_utility.cpp
    ${_brpc_SOURCE_DIR}/src/butil/files/temp_file.cpp
    ${_brpc_SOURCE_DIR}/src/butil/files/file_watcher.cpp
    ${_brpc_SOURCE_DIR}/src/butil/time.cpp
    ${_brpc_SOURCE_DIR}/src/butil/zero_copy_stream_as_streambuf.cpp
    ${_brpc_SOURCE_DIR}/src/butil/crc32c.cc
    ${_brpc_SOURCE_DIR}/src/butil/containers/case_ignored_flat_map.cpp
    ${_brpc_SOURCE_DIR}/src/butil/iobuf.cpp
    ${_brpc_SOURCE_DIR}/src/butil/iobuf_profiler.cpp
    ${_brpc_SOURCE_DIR}/src/butil/binary_printer.cpp
    ${_brpc_SOURCE_DIR}/src/butil/recordio.cc
    ${_brpc_SOURCE_DIR}/src/butil/popen.cpp
    )

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(BUTIL_SOURCES ${BUTIL_SOURCES}
        ${_brpc_SOURCE_DIR}/src/butil/file_util_linux.cc
        ${_brpc_SOURCE_DIR}/src/butil/threading/platform_thread_linux.cc
        ${_brpc_SOURCE_DIR}/src/butil/strings/sys_string_conversions_posix.cc)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(BUTIL_SOURCES ${BUTIL_SOURCES}
        ${_brpc_SOURCE_DIR}/src/butil/mac/bundle_locations.mm
        ${_brpc_SOURCE_DIR}/src/butil/mac/foundation_util.mm
        ${_brpc_SOURCE_DIR}/src/butil/file_util_mac.mm
        ${_brpc_SOURCE_DIR}/src/butil/threading/platform_thread_mac.mm
        ${_brpc_SOURCE_DIR}/src/butil/strings/sys_string_conversions_mac.mm
        ${_brpc_SOURCE_DIR}/src/butil/time/time_mac.cc
        ${_brpc_SOURCE_DIR}/src/butil/mac/scoped_mach_port.cc)
endif()

file(GLOB_RECURSE BVAR_SOURCES "${_brpc_SOURCE_DIR}/src/bvar/*.cpp")
file(GLOB_RECURSE BTHREAD_SOURCES "${_brpc_SOURCE_DIR}/src/bthread/*.cpp")
file(GLOB_RECURSE JSON2PB_SOURCES "${_brpc_SOURCE_DIR}/src/json2pb/*.cpp")
file(GLOB_RECURSE BRPC_SOURCES "${_brpc_SOURCE_DIR}/src/brpc/*.cpp")
file(GLOB_RECURSE THRIFT_SOURCES "${_brpc_SOURCE_DIR}/src/brpc/thrift*.cpp")
file(GLOB_RECURSE EXCLUDE_SOURCES "${_brpc_SOURCE_DIR}/src/brpc/event_dispatcher_*.cpp")

# if(WITH_THRIFT)
#     message("brpc compile with thrift protocol")
# else()
    # Remove thrift sources
    foreach(v ${THRIFT_SOURCES})
        list(REMOVE_ITEM BRPC_SOURCES ${v})
    endforeach()
    set(THRIFT_SOURCES "")
# endif()

foreach(v ${EXCLUDE_SOURCES})
    list(REMOVE_ITEM BRPC_SOURCES ${v})
endforeach()

set(MCPACK2PB_SOURCES
    ${_brpc_SOURCE_DIR}/src/mcpack2pb/field_type.cpp
    ${_brpc_SOURCE_DIR}/src/mcpack2pb/mcpack2pb.cpp
    ${_brpc_SOURCE_DIR}/src/mcpack2pb/parser.cpp
    ${_brpc_SOURCE_DIR}/src/mcpack2pb/serializer.cpp)

function(compile_proto OUT_HDRS OUT_SRCS DESTDIR HDR_OUTPUT_DIR PROTO_DIR PROTO_FILES)
  foreach(P ${PROTO_FILES})
    string(REPLACE .proto .pb.h HDR ${P})
    set(HDR_RELATIVE ${HDR})
    set(HDR ${DESTDIR}/${HDR})
    string(REPLACE .proto .pb.cc SRC ${P})
    set(SRC ${DESTDIR}/${SRC})
    list(APPEND HDRS ${HDR})
    list(APPEND SRCS ${SRC})
    add_custom_command(
      OUTPUT ${HDR} ${SRC}
      COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} ${PROTOC_FLAGS} -I${PROTO_DIR} --cpp_out=${DESTDIR} ${PROTO_DIR}/${P}
      COMMAND ${CMAKE_COMMAND} -E copy ${HDR} ${HDR_OUTPUT_DIR}/${HDR_RELATIVE}
      DEPENDS ${PROTO_DIR}/${P}
    )
  endforeach()
  set(${OUT_HDRS} ${HDRS} PARENT_SCOPE)
  set(${OUT_SRCS} ${SRCS} PARENT_SCOPE)
endfunction()

# include(CompileProto)
set(PROTO_FILES idl_options.proto
                brpc/rtmp.proto
                brpc/rpc_dump.proto
                brpc/get_favicon.proto
                brpc/span.proto
                brpc/builtin_service.proto
                brpc/grpc_health_check.proto
                brpc/get_js.proto
                brpc/errno.proto
                brpc/nshead_meta.proto
                brpc/options.proto
                brpc/policy/baidu_rpc_meta.proto
                brpc/policy/hulu_pbrpc_meta.proto
                brpc/policy/public_pbrpc_meta.proto
                brpc/policy/sofa_pbrpc_meta.proto
                brpc/policy/mongo.proto
                brpc/trackme.proto
                brpc/streaming_rpc_meta.proto
                brpc/proto_base.proto)
file(MAKE_DIRECTORY ${PROJECT_BINARY_DIR}/output/include/brpc)
set(PROTOC_FLAGS ${PROTOC_FLAGS} -I${PROTOBUF_INCLUDE_DIRS})
compile_proto(PROTO_HDRS PROTO_SRCS ${PROJECT_BINARY_DIR}
                                    ${PROJECT_BINARY_DIR}/output/include
                                    ${_brpc_SOURCE_DIR}/src
                                    "${PROTO_FILES}")
add_library(PROTO_LIB OBJECT ${PROTO_SRCS} ${PROTO_HDRS})

set(SOURCES
    ${BVAR_SOURCES}
    ${BTHREAD_SOURCES}
    ${JSON2PB_SOURCES}
    ${MCPACK2PB_SOURCES}
    ${BRPC_SOURCES}
    ${THRIFT_SOURCES}
    )

# add_subdirectory(${_brpc_SOURCE_DIR}/src)
if(NOT DEBUG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DNDEBUG")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -DNDEBUG")
endif()

include_directories(${CMAKE_CURRENT_BINARY_DIR})
include_directories(${_brpc_SOURCE_DIR}/src)

add_library(BUTIL_LIB OBJECT ${BUTIL_SOURCES})
add_library(SOURCES_LIB OBJECT ${SOURCES})
add_dependencies(SOURCES_LIB PROTO_LIB)

# shared library needs POSITION_INDEPENDENT_CODE
set_property(TARGET ${SOURCES_LIB} PROPERTY POSITION_INDEPENDENT_CODE 1)
set_property(TARGET ${BUTIL_LIB} PROPERTY POSITION_INDEPENDENT_CODE 1)

add_library(brpc-static STATIC $<TARGET_OBJECTS:BUTIL_LIB>
                               $<TARGET_OBJECTS:SOURCES_LIB>
                               $<TARGET_OBJECTS:PROTO_LIB>)
target_link_libraries(brpc-static gflags::gflags)
target_link_libraries(brpc-static leveldb)

# function(check_thrift_version target_arg)
#     #use thrift command to get version
#     execute_process(
#         COMMAND thrift --version
#         OUTPUT_VARIABLE THRIFT_VERSION_OUTPUT
#         ERROR_QUIET
#         OUTPUT_STRIP_TRAILING_WHITESPACE
#     )
# 
#     string(REGEX MATCH "[0-9]+\\.[0-9]+\\.[0-9]+" THRIFT_VERSION ${THRIFT_VERSION_OUTPUT})
#     string(REGEX REPLACE "\\." ";" THRIFT_VERSION_LIST ${THRIFT_VERSION})
# 
#     list(GET THRIFT_VERSION_LIST 0 THRIFT_MAJOR_VERSION)
#     list(GET THRIFT_VERSION_LIST 1 THRIFT_MINOR_VERSION)
# 
#     if (THRIFT_MAJOR_VERSION EQUAL 0 AND THRIFT_MINOR_VERSION LESS 11)
#         message(STATUS "Thrift version is less than 0.11.0")
#         target_compile_definitions($(target_arg) PRIVATE _THRIFT_VERSION_LOWER_THAN_0_11_0_)
#     else()
#         message(STATUS "Thrift version is equal to or greater than 0.11.0")
#     endif()
# endfunction()


# if(WITH_THRIFT)
#    target_link_libraries(brpc-static ${THRIFT_LIB})
#    check_thrift_version(brpc-static)
# endif()

SET_TARGET_PROPERTIES(brpc-static PROPERTIES OUTPUT_NAME brpc CLEAN_DIRECT_OUTPUT 1)

# for protoc-gen-mcpack
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/output/bin)
    
set(protoc_gen_mcpack_SOURCES
    ${_brpc_SOURCE_DIR}/src/mcpack2pb/generator.cpp
 )
    
add_executable(protoc-gen-mcpack ${protoc_gen_mcpack_SOURCES})

# if(BUILD_SHARED_LIBS)
#     add_library(brpc-shared SHARED $<TARGET_OBJECTS:BUTIL_LIB> 
#                                    $<TARGET_OBJECTS:SOURCES_LIB>
#                                    $<TARGET_OBJECTS:PROTO_LIB>)
#     target_link_libraries(brpc-shared ${DYNAMIC_LIB})
#     if(WITH_GLOG)
#         target_link_libraries(brpc-shared ${GLOG_LIB})
#     endif()
#     if(WITH_THRIFT)
#         target_link_libraries(brpc-shared ${THRIFT_LIB})
#         check_thrift_version(brpc-shared)
#     endif()
#     SET_TARGET_PROPERTIES(brpc-shared PROPERTIES OUTPUT_NAME brpc CLEAN_DIRECT_OUTPUT 1)
# 
#     target_link_libraries(protoc-gen-mcpack brpc-shared ${DYNAMIC_LIB} pthread)
# 
#     install(TARGETS brpc-shared
#             RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
#             LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
#             )
# else()
    target_link_libraries(protoc-gen-mcpack brpc-static ${DYNAMIC_LIB} pthread)
# endif()



install(TARGETS brpc-static
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        )
# if(BUILD_UNIT_TESTS)
#     enable_testing()
#     add_subdirectory(test)
# endif()

if(BUILD_FUZZ_TESTS)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "Fuzzing is only supported with clang")
    endif()
    if(NOT BUILD_UNIT_TESTS)
        message(FATAL_ERROR "BUILD_UNIT_TESTS must be enabled to build fuzz tests")
    endif()
endif()

if(BUILD_BRPC_TOOLS)
    add_subdirectory(${_brpc_SOURCE_DIR}/tools)
endif()

file(COPY ${CMAKE_CURRENT_BINARY_DIR}/brpc/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/brpc/
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )
file(COPY ${_brpc_SOURCE_DIR}/src/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )
install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/output/include/
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
        FILES_MATCHING
        PATTERN "*.h"
        PATTERN "*.hpp"
        )

# Install pkgconfig
# configure_file(cmake/brpc.pc.in ${PROJECT_BINARY_DIR}/brpc.pc @ONLY)
# install(FILES ${PROJECT_BINARY_DIR}/brpc.pc DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)


## brpc example
set (BRPC_EXAMPLE_SOURCE_DIR ${_brpc_SOURCE_DIR}/example/streaming_echo_c++)
set (BRPC_EXAMPLE_BINARY_DIR ${_brpc_BINARY_DIR}/example/streaming_echo_c++)

set (BRPC_EXAMPLE_PROTOS echo.proto)
compile_proto(ECHO_PROTO_HDRS ECHO_PROTO_SRCS ${BRPC_EXAMPLE_SOURCE_DIR} ${BRPC_EXAMPLE_SOURCE_DIR} ${BRPC_EXAMPLE_SOURCE_DIR} ${BRPC_EXAMPLE_PROTOS})
add_executable(streaming_echo_client ${BRPC_EXAMPLE_SOURCE_DIR}/client.cpp ${ECHO_PROTO_HDRS} ${ECHO_PROTO_SRCS})
target_link_libraries(streaming_echo_client brpc-static ${DYNAMIC_LIB})

add_executable(streaming_echo_server ${BRPC_EXAMPLE_SOURCE_DIR}/server.cpp ${ECHO_PROTO_HDRS} ${ECHO_PROTO_SRCS})
target_link_libraries(streaming_echo_server brpc-static ${DYNAMIC_LIB})
