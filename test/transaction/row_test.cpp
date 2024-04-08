#include "gtest/gtest.h"
#include "transaction_manager.h"
#include "KVStore_occ.h"
// #include "transaction_manager_rpc.h"
// #include "meta_server_rpc.h"
// #include "execution_rpc.h"

// #include <brpc/channel.h>
#include <filesystem>

// DEFINE_string(SERVER_NAME, "", "Server NAME");

class TxnManagerTest : public ::testing::Test
{
public:
    std::unique_ptr<LogStorage> log_storage_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<KVStore> kv_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<KVStorage> kv_storage_;

public:
    void SetUp() override
    {
        std::string dir = "./data";
        log_storage_ = std::make_unique<LogStorage>("test_db");
        log_manager_ = std::make_unique<LogManager>(log_storage_.get());
        if(std::filesystem::exists(dir)) {
            std::filesystem::remove_all(dir);
            std::filesystem::remove(dir);
        }
        kv_storage_ = std::make_unique<KVStorage>(dir);
        kv_ = std::make_unique<KVStore>(dir, log_manager_.get());
        transaction_manager_ = std::make_unique<TransactionManager>(kv_.get(), log_manager_.get());
    }
};
TEST_F(TxnManagerTest, RowTest)
{
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1, 1);
    ASSERT_EQ(txn1->get_txn_id(), 1);
    transaction_manager_->Begin(txn2, 2);
    ASSERT_EQ(txn2->get_txn_id(), 2);

    Row_occ *row1 = new Row_occ(std::string("key1"));
    row1->try_lock(1);
    ASSERT_EQ(row1->try_lock(1), true);
    ASSERT_EQ(row1->try_lock(2), false);
    ASSERT_EQ(row1->access(txn1, access_t::RD), true);
    ASSERT_EQ(row1->access(txn2, access_t::RD), false);
    ASSERT_EQ(txn1->get_read_set_size(), 1);
    
    delete row1;
}

TEST_F(TxnManagerTest, TxnManagerTest)
{
    auto batch = std::vector<Modify*>();
    auto put1 = new Put("cf1", "key1", "value1");
    auto put2 = new Put("cf1", "key2", "value2");
    batch.push_back(put1);
    batch.push_back(put2);
    kv_storage_->Write(batch);
    ASSERT_EQ(kv_storage_->GetCF("cf1","key1"), "value1");
    ASSERT_EQ(kv_storage_->GetCF("cf1","key2"), "value2");
}

int main(int argc, char **argv)
{
    // gflags::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}