#include "focc.h"
#include <filesystem>
#include "gtest/gtest.h"
#include "transaction_manager.h"

class TxnManagerTest : public ::testing::Test
{
public:
    std::unique_ptr<LogStorage> log_storage_;
    std::unique_ptr<LogManager> log_manager_;
    std::unique_ptr<KVStore> kv_;
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Focc> focc_;

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
        kv_ = std::make_unique<KVStore>(dir, log_manager_.get());
        focc_ = std::make_unique<Focc>();
        focc_->init();
        transaction_manager_ = std::make_unique<TransactionManager>(
            kv_.get(), log_manager_.get(), focc_.get());
    }
};

// R1a, W1a, R2a, W2a
TEST_F(TxnManagerTest, FoccTest1)
{
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1);
    transaction_manager_->Begin(txn2);
    Row_occ *row1 = new Row_occ(std::string("key1"));
    Row_occ *row2 = new Row_occ(std::string("key2"));

    row1->access(txn1, access_t::RD);
    focc_->active_storage(txn1);
    txn1->add_write_set(row1);
    row2->access(txn2, access_t::RD);
    focc_->active_storage(txn2);
    txn2->add_write_set(row2);
    ASSERT_EQ(focc_->validate(txn1), true);
    ASSERT_EQ(focc_->validate(txn2), true);
}

// R1a, W1a, R2a
TEST_F(TxnManagerTest, FoccTest2)
{
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1);
    transaction_manager_->Begin(txn2);
    Row_occ *row1 = new Row_occ(std::string("key1"));

    row1->access(txn1, access_t::RD);
    focc_->active_storage(txn1);
    txn1->add_write_set(row1);
    ASSERT_EQ(focc_->validate(txn1), true);

    ASSERT_EQ(row1->access(txn2, access_t::RD), false);

    focc_->finish(txn1);
    ASSERT_EQ(row1->access(txn2, access_t::RD), true);
}

// R1a, W1a, R2a, C1
TEST_F(TxnManagerTest, FoccTest3)
{
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1);
    transaction_manager_->Begin(txn2);
    Row_occ *row1 = new Row_occ(std::string("key1"));

    row1->access(txn1, access_t::RD);
    focc_->active_storage(txn1);
    txn1->add_write_set(row1);

    row1->access(txn2, access_t::RD);
    focc_->active_storage(txn2);

    ASSERT_EQ(focc_->validate(txn1), false);
}

int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}