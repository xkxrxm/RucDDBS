#pragma once

#include "meta_service.pb.h"
#include "transaction.h"
// #include "Inmemory/KVStore_new.h"

#include <shared_mutex>

enum class ConcurrencyMode
{
    TWO_PHASE_LOCKING = 0,
    OCC = 1,
};

class TransactionManager
{
private:
    // LogManager *log_manager_;
    // KVStore *kv_;
    ConcurrencyMode concurrency_mode_;
    /** The global transaction latch is used for checkpointing. */
    std::shared_mutex global_txn_latch_;

public: 
    ~TransactionManager() = default;
    explicit TransactionManager(
        // KVStore *kv,
        // LogManager *log_manager = nullptr,
        ConcurrencyMode concurrency_mode = ConcurrencyMode::OCC)
    {
        // log_manager_ = log_manager;
        // kv_ = kv;
        concurrency_mode_ = concurrency_mode;
    }
    ConcurrencyMode getConcurrencyMode() { return concurrency_mode_; }
    void SetConcurrencyMode(ConcurrencyMode concurrency_mode)
    {
        concurrency_mode_ = concurrency_mode;
    }
    // KVStore* getKVstore() {return kv_; }
    
    /* The transaction map is a local list of all the running transactions in the local node. */
    static std::unordered_map<txn_id_t, Transaction *> txn_map;
    static std::shared_mutex txn_map_mutex;

    static Transaction *getTransaction(txn_id_t txn_id) {
        std::shared_lock<std::shared_mutex> l(txn_map_mutex);
        if(TransactionManager::txn_map.find(txn_id) == TransactionManager::txn_map.end()){
            return nullptr;
        }
        Transaction *txn = txn_map[txn_id];
        assert(txn != nullptr);
        return txn;
    }

    static uint64_t getTimestampFromServer();

    // 从meta_server获取时间戳作为txn id
    Transaction* Begin(Transaction*& txn, IsolationLevel isolation_level=IsolationLevel::SERIALIZABLE);

    // 传入txn_id作为事务id
    Transaction* Begin(Transaction*& txn, txn_id_t txn_id, IsolationLevel isolation_level=IsolationLevel::SERIALIZABLE); 
    
    bool Abort(Transaction * txn);
    bool AbortSingle(Transaction * txn, bool use_raft = false);
    
    bool Commit(Transaction * txn);
    bool CommitSingle(Transaction * txn, bool sync_write_set = false, bool use_raft = false);
    
    bool PrepareCommit(Transaction * txn);

    /** Prevents all transactions from performing operations, used for checkpointing. */
    void BlockAllTransactions(){global_txn_latch_.lock();}

    /** Resumes all transactions, used for checkpointing. */
    void ResumeTransactions(){global_txn_latch_.unlock();}
};
