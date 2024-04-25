# add_executable(row_test
#     row_test.cpp 
#     ../../meta_server/build/meta_service.pb.cc 
# )

# add_executable(focc_test
#     focc_test.cpp 
#     ../../meta_server/build/meta_service.pb.cc 
# )

add_executable(txn_manager_test
    txn_manager_test.cpp 
    ../../meta_server/build/meta_service.pb.cc 
)

target_include_directories(txn_manager_test PRIVATE ${INCLUDE_DIR})
target_link_libraries(txn_manager_test transaction recovery config storage ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)
add_test(NAME txn_manager_test COMMAND txn_manager_test)