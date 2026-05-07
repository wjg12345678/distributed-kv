cmake_minimum_required(VERSION 3.16)
project(distributed_kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

include(CTest)

find_package(PkgConfig REQUIRED)
find_package(Protobuf REQUIRED)
pkg_check_modules(PROTOBUF_ABSL REQUIRED IMPORTED_TARGET protobuf)

find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h
  HINTS
    /usr/local/include
    /opt/homebrew/include
)

find_library(ROCKSDB_LIBRARY rocksdb
  HINTS
    /usr/local/lib
    /opt/homebrew/lib
)

if (NOT ROCKSDB_INCLUDE_DIR OR NOT ROCKSDB_LIBRARY)
  message(FATAL_ERROR "RocksDB headers or library not found. Install rocksdb first.")
endif()

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS protos/raft_rpc.proto)

add_library(distributed_kv
  src/storage/key_value_store.cpp
  src/core/raft_node.cpp
  src/core/cluster.cpp
  src/core/raft_persistence.cpp
  src/network/network_codec.cpp
  src/network/tcp_util.cpp
  src/network/task_executor.cpp
  ${PROTO_SRCS}
)

target_include_directories(distributed_kv PUBLIC include ${CMAKE_CURRENT_BINARY_DIR})
target_include_directories(distributed_kv PRIVATE ${ROCKSDB_INCLUDE_DIR} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(distributed_kv PUBLIC ${ROCKSDB_LIBRARY} protobuf::libprotobuf PkgConfig::PROTOBUF_ABSL)

add_executable(distributed_kv_single_process apps/single_process_main.cpp)
target_link_libraries(distributed_kv_single_process PRIVATE distributed_kv)

add_executable(raft_smoke_test tests/unit/raft_smoke_test.cpp)
target_link_libraries(raft_smoke_test PRIVATE distributed_kv)
add_test(NAME raft_smoke_test COMMAND $<TARGET_FILE:raft_smoke_test>)

add_executable(raft_prevote_test tests/unit/raft_prevote_test.cpp)
target_link_libraries(raft_prevote_test PRIVATE distributed_kv)
add_test(NAME raft_prevote_test COMMAND $<TARGET_FILE:raft_prevote_test>)

add_executable(raft_linearizable_read_test tests/unit/raft_linearizable_read_test.cpp)
target_link_libraries(raft_linearizable_read_test PRIVATE distributed_kv)
add_test(NAME raft_linearizable_read_test COMMAND $<TARGET_FILE:raft_linearizable_read_test>)

add_executable(raft_conflict_test tests/unit/raft_conflict_test.cpp)
target_link_libraries(raft_conflict_test PRIVATE distributed_kv)
add_test(NAME raft_conflict_test COMMAND $<TARGET_FILE:raft_conflict_test>)

add_executable(raft_persistence_test tests/unit/raft_persistence_test.cpp)
target_link_libraries(raft_persistence_test PRIVATE distributed_kv)
add_test(NAME raft_persistence_test COMMAND $<TARGET_FILE:raft_persistence_test>)

add_executable(raft_snapshot_test tests/unit/raft_snapshot_test.cpp)
target_link_libraries(raft_snapshot_test PRIVATE distributed_kv)
add_test(NAME raft_snapshot_test COMMAND $<TARGET_FILE:raft_snapshot_test>)

add_executable(raft_membership_test tests/unit/raft_membership_test.cpp)
target_link_libraries(raft_membership_test PRIVATE distributed_kv)
add_test(NAME raft_membership_test COMMAND $<TARGET_FILE:raft_membership_test>)

add_executable(raft_lock_test tests/unit/raft_lock_test.cpp)
target_link_libraries(raft_lock_test PRIVATE distributed_kv)
add_test(NAME raft_lock_test COMMAND $<TARGET_FILE:raft_lock_test>)

add_executable(raft_mvcc_test tests/unit/raft_mvcc_test.cpp)
target_link_libraries(raft_mvcc_test PRIVATE distributed_kv)
add_test(NAME raft_mvcc_test COMMAND $<TARGET_FILE:raft_mvcc_test>)

add_executable(distributed_kv_http_server
  src/network/http_server.cpp
  apps/http_server_main.cpp
)
target_link_libraries(distributed_kv_http_server PRIVATE distributed_kv)

add_executable(distributed_kv_network_node
  src/network/network_transport.cpp
  src/network/raft_rpc_server.cpp
  src/network/node_http_server.cpp
  apps/network_node_main.cpp
)
target_link_libraries(distributed_kv_network_node PRIVATE distributed_kv)
