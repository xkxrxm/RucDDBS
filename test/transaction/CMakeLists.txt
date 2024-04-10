# add_executable(row_test
#     row_test.cpp 
#     ../../meta_server/build/meta_service.pb.cc 
# )

# add_executable(focc_test
#     focc_test.cpp 
#     ../../meta_server/build/meta_service.pb.cc 
# )

# add_executable(txn_manager_test
#     txn_manager_test.cpp 
#     ../../meta_server/build/meta_service.pb.cc 
# )

# target_include_directories(row_test PRIVATE ${INCLUDE_DIR})
# target_include_directories(focc_test PRIVATE ${INCLUDE_DIR})
# target_include_directories(txn_manager_test PRIVATE ${INCLUDE_DIR})

# target_link_libraries(row_test transaction recovery config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)
# target_link_libraries(focc_test transaction recovery config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)
# target_link_libraries(txn_manager_test transaction recovery config ${BRPC_LIB} ${DYNAMIC_LIB} gtest gtest_main gmock_main)
