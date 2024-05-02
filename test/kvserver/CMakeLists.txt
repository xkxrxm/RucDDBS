
set(DYNAMIC_LIB
    ${CMAKE_THREAD_LIBS_INIT}
    ${GFLAGS_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${GPERFTOOLS_LIBRARIES}
    ${LEVELDB_LIB}
    ${BRAFT_LIB}
    ${BRPC_LIB}
    storage 
    engine
    gmock_main 
    gtest
    rt
    ssl
    crypto
    dl
    z
    )
add_executable(basic_test ./basic_test.cpp)
target_link_libraries(basic_test
                      "-Xlinker \"-(\""
                      ${DYNAMIC_LIB}
                      "-Xlinker \"-)\"")
add_test(NAME basic_test COMMAND basic_test)
