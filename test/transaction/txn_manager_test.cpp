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
        
        shared_ptr<record> fake_record(new record);
        fake_record->string_to_rec("2 1 0");
        Transaction *txn = nullptr;
        transaction_manager_->Begin(txn);
        txn->put("x", fake_record);
        txn->put("y", fake_record);
        txn->put("z", fake_record);
        txn->commit();
    }
};

TEST_F(TxnManagerTest, TxnManagerTest1)
{
    Transaction *txn1 = nullptr;
    transaction_manager_->Begin(txn1);
    ASSERT_NE(txn1, nullptr) << "txn_id: " << txn1->get_txn_id() << std::endl;
    Transaction *txn2 = nullptr;
    transaction_manager_->Begin(txn2);
    ASSERT_NE(txn2, nullptr) << "txn_id: " << txn2->get_txn_id() << std::endl;

    shared_ptr<record> r(new record);
    txn1->get("key1",r);
    focc_->active_storage(txn1);

    txn2->del("key1");
    ASSERT_EQ(focc_->validate(txn2), false);


}

TEST_F(TxnManagerTest, TxnManagerTest2)
{
   /*
    T1: R(z0)
    T3: W(x1)
    T3: W(z1)
    T3: Commit --> fail
    T2: W(x1)
    T2: W(y1)
    T2: Commit --> success
    T1: R(y0)
    T1: Commit --> success
    */
    Transaction *txn1 = nullptr;
    Transaction *txn2 = nullptr;
    Transaction *txn3 = nullptr;
    transaction_manager_->Begin(txn1);
    transaction_manager_->Begin(txn2);
    transaction_manager_->Begin(txn3);
    
    shared_ptr<record> fake_record(new record);
    fake_record->string_to_rec("2 1 1");

    shared_ptr<record> r(new record);
    txn1->get("z",r);
    transaction_manager_->active_storage(txn1);
    txn3->put("x", fake_record);
    txn3->put("z", fake_record);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn3), false);
    txn2->put("x", fake_record);
    txn2->put("y", fake_record);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn2), false);
    txn1->get("y",r);
    ASSERT_EQ(transaction_manager_->CommitSingle(txn1), true);
}

// TEST_F(TxnManagerTest, TxnManagerTest3)
// {
//     /*
//     T1: R(z0)
//     T3: W(x1)
//     T3: W(z1)
//     T3: Commit
//     T2: W(x1)
//     T2: W(y1)
//     T2: Commit
//     */
//     Transaction *txn1 = nullptr;
//     Transaction *txn2 = nullptr;
//     Transaction *txn3 = nullptr;
//     transaction_manager_->Begin(txn1);
//     transaction_manager_->Begin(txn2);
//     transaction_manager_->Begin(txn3);
    
//     shared_ptr<record> fake_record(new record);
//     fake_record->string_to_rec("2 1 1");

//     shared_ptr<record> r(new record);
//     txn1->get("z",r);
//     txn3->put("x", fake_record);
//     txn3->put("z", fake_record);
//     ASSERT_EQ(transaction_manager_->CommitSingle(txn3), false);
//     txn2->put("x", fake_record);
//     txn2->put("y", fake_record);
//     ASSERT_EQ(transaction_manager_->CommitSingle(txn2), false);
//     ASSERT_EQ(transaction_manager_->CommitSingle(txn1), true);
// }