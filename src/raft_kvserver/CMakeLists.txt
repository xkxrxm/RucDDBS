
add_executable(raft_kvserver raft_kvserver.cpp ${PROTO_SRC} ${PROTO_HEADER})
add_executable(get_leader handler_test.cpp ${PROTO_SRC} ${PROTO_HEADER})

set(DYNAMIC_LIB
    storage 
    proto
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${GLOG_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${GPERFTOOLS_LIBRARIES}
    ${LEVELDB_LIB}
    ${BRAFT_LIB}
    ${BRPC_LIB}
    rt
    ssl
    crypto
    dl
    z
    )

target_link_libraries(get_leader
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")

target_link_libraries(raft_kvserver
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")