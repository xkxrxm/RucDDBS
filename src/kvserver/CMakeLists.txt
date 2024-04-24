add_executable(kvserver KVServer.cpp)
target_link_libraries(kvserver storage proto gflags ${BRPC_LIB} ${DYNAMIC_LIB})