#include <filesystem>
#include <iostream>

#include "gtest/gtest.h"

#include "KVStore_beta.h"

class KVSTORE_BETA_TEST : public ::testing::Test {
 protected:
  void SetUp() override {
    // 在测试之前设置
    if(std::filesystem::exists(dir)) {
      std::filesystem::remove_all(dir);
      std::filesystem::remove(dir);
    }
  }

  void TearDown() override {
    // 在测试之后做清理工作
  }

  const std::string dir = "./kvstore_data";
};




TEST_F(KVSTORE_BETA_TEST, empty_test) {
    KVStore_beta storage(dir);

    EXPECT_EQ(std::make_pair(false, std::string("")), storage.get("key"));
}

TEST_F(KVSTORE_BETA_TEST, single_test) {
    KVStore_beta storage(dir);
    // const std::string empty_string = "";
    EXPECT_EQ(std::make_pair(false, std::string("")), storage.get("key"));
    storage.put("key", "value");
    EXPECT_EQ(std::make_pair(true, std::string("value")), storage.get("key"));
    EXPECT_EQ(true, storage.del("key"));
    EXPECT_EQ(std::make_pair(false, std::string("")), storage.get("key"));
    EXPECT_EQ(false, storage.del("key"));
}

TEST_F(KVSTORE_BETA_TEST, simple_test) {
    KVStore_beta storage(dir);
    const uint64_t N = 1024;
    // put
    for(uint64_t i = 1; i <= N; i++){
        std::string key = std::to_string(i);
        std::string value = key;
        storage.put(key, value);
        EXPECT_EQ(std::make_pair(true, value), storage.get(key));
    }
    // get
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        std::string value = key;
        EXPECT_EQ(std::make_pair(true, value), storage.get(key));
    }
    //delete
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        EXPECT_EQ(true, storage.del(key));
    }
    // get
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        EXPECT_EQ(std::make_pair(false, std::string("")), storage.get(key));
    }
}


TEST_F(KVSTORE_BETA_TEST, large_test) {
    KVStore_beta storage(dir);
    const uint64_t N = 1024 * 16;
    // put
    for(uint64_t i = 1; i <= N; i++){
        std::string key = std::to_string(i);
        std::string value = key;
        storage.put(key, value);
        EXPECT_EQ(std::make_pair(true, value), storage.get(key));
    }
    // get
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        std::string value = key;
        EXPECT_EQ(std::make_pair(true, value), storage.get(key));
    }
    //delete
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        EXPECT_EQ(true, storage.del(key));
    }
    // get
    for(uint64_t i = 1; i <= N; i++) {
        std::string key = std::to_string(i);
        EXPECT_EQ(std::make_pair(false, std::string("")), storage.get(key));
    }
}

int main(int argc, char **argv) {
  printf("Running main() from %s\n", __FILE__);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();   
}