#include <filesystem>

#include "focc.h"
#include "gtest/gtest.h"
#include "transaction_manager.h"

DEFINE_string(SERVER_NAME, "", "Server NAME");

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

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
        if (std::filesystem::exists(dir))
        {
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

TEST_F(TxnManagerTest, TxnManagerTest1)
{
    Transaction *txn = nullptr;
    transaction_manager_->Begin(txn);
    ASSERT_NE(txn, nullptr) << "txn_id: " << txn->get_txn_id() << std::endl;
    Row_occ *row1 = new Row_occ(std::string("key1"));
    transaction_manager_->ReadRow(txn, row1);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn), true);
}

TEST_F(TxnManagerTest, TxnManagerTest2)
{
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1);
    transaction_manager_->Begin(txn2);
    Row_occ *row1 = new Row_occ(std::string("key1"));

    transaction_manager_->ReadRow(txn1, row1);
    txn1->add_write_set(row1);
    transaction_manager_->ReadRow(txn2, row1);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn1), false);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn2), true);
}