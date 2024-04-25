option(STORAGE_TEST ON)
option(SKIPLIST_TEST "SKIPLIST_TEST" ON)
option(BLOCKBUILDER_TEST "block builder's test" ON)
option(TABLEBUILDER_TEST "table builder's test" ON)
option(LEVELZERO_TEST "level0's test" ON)
option(DISKSTORAGE_TEST "disk storage's test" ON)


# storage test
add_executable(correctness ./correctness.cpp)
target_link_libraries(correctness storage)
add_test(NAME correctness COMMAND correctness)
# storage googletest
add_executable(correct_gtest ./correct_gtest.cpp)
target_link_libraries(correct_gtest storage gmock_main gtest)
# add_test(NAME correct_gtest COMMAND correct_gtest) # not run this test become it needs long time

# skiplist
if(SKIPLIST_TEST)
    message("storage test: build skiplist_test")
    add_executable(skiplist_gtest 
                        ${PROJECT_SOURCE_DIR}/test/storage/skiplist_gtest.cpp 
                        ${PROJECT_SOURCE_DIR}/src/storage/SkipList.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Entry.cpp)
    target_include_directories(skiplist_gtest PRIVATE ${STORAGE_DIR})
    target_link_libraries(skiplist_gtest gmock_main gtest)
    add_test(NAME skiplist_gtest COMMAND skiplist_gtest)
endif()

# block builder test
if(BLOCKBUILDER_TEST) 
    message("storage test: build block builder test")
    add_executable(blockbuilder_gtest
                        ${PROJECT_SOURCE_DIR}/test/storage/blockbuilder_gtest.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/BlockBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Block.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Iterator.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Option.cpp)
    target_include_directories(blockbuilder_gtest PRIVATE ${STORAGE_DIR})
    target_link_libraries(blockbuilder_gtest gmock_main gtest)
    add_test(NAME blockbuilder_gtest COMMAND blockbuilder_gtest)
endif()

# table builder test
if(TABLEBUILDER_TEST)
    message("storage test: table builder test")
    add_executable(tablebuilder_gtest
                        ${PROJECT_SOURCE_DIR}/test/storage/tablebuilder_gtest.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/TableBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/BlockBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Iterator.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Block.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTable.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SkipList.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTableId.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Option.cpp
                        )
    target_include_directories(tablebuilder_gtest PRIVATE ${STORAGE_DIR})
    target_link_libraries(tablebuilder_gtest gmock_main gtest snappy)
    add_test(NAME tablebuilder_gtest COMMAND tablebuilder_gtest)
endif()


if(LEVELZERO_TEST)
    message("storage test: build level0's test")
    add_executable(level0_gtest
                        ${PROJECT_SOURCE_DIR}/test/storage/LevelZero_gtest.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/TableBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/BlockBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Iterator.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Block.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTable.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/LevelZero.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/TableCache.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTableId.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SkipList.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Option.cpp
                        )
    target_include_directories(level0_gtest PRIVATE ${STORAGE_DIR})
    target_link_libraries(level0_gtest gmock_main gtest snappy)
    # add_test(NAME level0_gtest COMMAND level0_gtest) # not run this test because it needs long time
endif()

if(DISKSTORAGE_TEST) 
    message("storage test: build disk storage's test")
    add_executable(diskstorage_gtest
                        ${PROJECT_SOURCE_DIR}/test/storage/diskstorage_gtest.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/DiskStorage.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/TableBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/BlockBuilder.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Iterator.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Block.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTable.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/LevelZero.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/TableCache.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SSTableId.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/SkipList.cpp
                        ${PROJECT_SOURCE_DIR}/src/storage/Option.cpp
                        )
    target_include_directories(diskstorage_gtest PRIVATE ${STORAGE_DIR})
    target_link_libraries(diskstorage_gtest gmock_main gtest snappy)
    add_test(NAME diskstorage_gtest COMMAND diskstorage_gtest)
endif()



