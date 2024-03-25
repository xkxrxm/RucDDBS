#include "focc.h"

#include "gtest/gtest.h"
#include "transaction_manager.h"

class FoccTest : public ::testing::Test
{
public:
    std::unique_ptr<TransactionManager> transaction_manager_;
    std::unique_ptr<Focc> focc_;

public:
    void SetUp() override
    {
        focc_ = std::make_unique<Focc>();
        transaction_manager_ = std::make_unique<TransactionManager>();
        focc_->init();
    }
};

// R1a, W1a, R2a, W2a
TEST_F(FoccTest, FoccTest1)
{
    // 写入(key1,value1)
    // 定义一个空事务指针
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1, 1);
    transaction_manager_->Begin(txn2, 2);
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

    free(row1);
    free(row2);
    free(txn1);
    free(txn2);
}

// R1a, W1a, R2a
TEST_F(FoccTest, FoccTest2)
{
    // 写入(key1,value1)
    // 定义一个空事务指针
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1, 1);
    transaction_manager_->Begin(txn2, 2);
    Row_occ *row1 = new Row_occ(std::string("key1"));

    row1->access(txn1, access_t::RD);
    focc_->active_storage(txn1);
    txn1->add_write_set(row1);
    ASSERT_EQ(focc_->validate(txn1), true);

    ASSERT_EQ(row1->access(txn2, access_t::RD), false);

    focc_->finish(txn1);
    ASSERT_EQ(row1->access(txn2, access_t::RD), true);

    free(row1);
    free(txn1);
    free(txn2);
}

// R1a, W1a, R2a, C1
TEST_F(FoccTest, FoccTest3)
{
    // 写入(key1,value1)
    // 定义一个空事务指针
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn1, 1);
    transaction_manager_->Begin(txn2, 2);
    Row_occ *row1 = new Row_occ(std::string("key1"));

    row1->access(txn1, access_t::RD);
    focc_->active_storage(txn1);
    txn1->add_write_set(row1);

    row1->access(txn2, access_t::RD);
    focc_->active_storage(txn2);

    ASSERT_EQ(focc_->validate(txn1), false);

    free(row1);
    free(txn1);
    free(txn2);
}