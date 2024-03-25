add_executable(row_test
    row_test.cpp 
    ${EXECUTION_SRC}
)

add_executable(focc_test
    focc_test.cpp 
    ${EXECUTION_SRC}
)

# add_executable(transaction_test2
#     transaction_test2.cpp 
#     execution_rpc.cpp
#     ${EXECUTION_SRC}
#     ../../meta_server/build/meta_service.pb.cc 
# )

# add_executable(LockManager_test
#     Lock_manager_test.cpp
#     ../../src/transaction/Lock_manager.cpp
    
# )
# target_include_directories(LockManager_test PRIVATE ${TRANSACTION_DIR} ${INCLUDE_DIR})
# target_link_libraries(LockManager_test gtest gtest_main gmock_main)

target_include_directories(row_test PRIVATE ${TRANSACTION_DIR})
target_include_directories(focc_test PRIVATE ${TRANSACTION_DIR})

# target_include_directories(transaction_test2 PRIVATE ${EXECUTION_DIR})
target_link_libraries(row_test transaction config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)
target_link_libraries(focc_test transaction config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)

# target_link_libraries(transaction_test2 transaction recovery config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)