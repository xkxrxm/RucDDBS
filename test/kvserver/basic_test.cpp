#include <string>

#include "engine/raft_kv_handler.h"
#include "gtest/gtest.h"

RaftKVHandler storage = RaftKVHandler();

TEST(StorageTest, single_test) {
    EXPECT_EQ("", storage.get(std::to_string(1)));
    storage.put(std::to_string(1), "data");
    EXPECT_EQ("data", storage.get(std::to_string(1)));
    EXPECT_EQ(true, storage.del(std::to_string(1)));
    EXPECT_EQ("", storage.get(std::to_string(1)));
    EXPECT_EQ(false, storage.del(std::to_string(1)));
}

TEST(StorageTest, small_test) {
    const uint64_t small = 1024;
    // put
    for (uint64_t i = 1; i <= small; i++) {
        storage.put(std::to_string(i), std::string(i, 's'));
        EXPECT_EQ(std::string(i, 's'), storage.get(std::to_string(i)));
    }
    // get
    for (uint64_t i = 1; i <= small; i++) {
        EXPECT_EQ(std::string(i, 's'), storage.get(std::to_string(i)));
    }
    // delete
    for (uint64_t i = 1; i <= small; i++) {
        EXPECT_EQ(true, storage.del(std::to_string(i)));
    }
    // get
    for (uint64_t i = 1; i <= small; i++) {
        EXPECT_EQ("", storage.get(std::to_string(i)));
    }
}

int main(int argc, char **argv) {
    printf("Running main() from %s\n", __FILE__);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}