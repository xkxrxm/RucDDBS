add_executable(log_manager_test log_manager_test.cpp)
target_link_libraries(log_manager_test storage recovery config gmock_main gtest ${GFLAGS_LIBRARY})
add_test(NAME log_manager_test COMMAND log_manager_test)