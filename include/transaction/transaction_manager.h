#pragma once

#include <shared_mutex>

#include "Inmemory/KVStore_new.h"
#include "focc.h"
#include "meta_service.pb.h"
#include "transaction.h"

enum class ConcurrencyMode
{
    TWO_PHASE_LOCKING = 0,
    OCC = 1,
};

class TransactionManager
{
private:
    Focc *focc_;
    KVStore *kv_;
    LogManager *log_manager_;
    ConcurrencyMode concurrency_mode_;

public:
    /* The transaction map is a local list of all the running transactions in
     * the local node. */
    static std::unordered_map<txn_id_t, Transaction *> txn_map;
    static std::shared_mutex txn_map_mutex;

public:
    ~TransactionManager() = default;
    TransactionManager() = delete;
    explicit TransactionManager(
        KVStore *kv,
        LogManager *log_manager,
        Focc *focc,
        ConcurrencyMode concurrency_mode = ConcurrencyMode::OCC)
        : focc_(focc),
          kv_(kv),
          log_manager_(log_manager),
          concurrency_mode_(concurrency_mode)
    {
    }

    ConcurrencyMode getConcurrencyMode()
    {
        return concurrency_mode_;
    }

    void SetConcurrencyMode(ConcurrencyMode concurrency_mode)
    {
        concurrency_mode_ = concurrency_mode;
    }

    KVStore *getKVstore()
    {
        return kv_;
    }

    static Transaction *getTransaction(txn_id_t txn_id)
    {
        std::shared_lock<std::shared_mutex> l(txn_map_mutex);
        if (TransactionManager::txn_map.find(txn_id) ==
            TransactionManager::txn_map.end())
        {
            return nullptr;
        }
        Transaction *txn = txn_map[txn_id];
        assert(txn != nullptr);
        return txn;
    }

    static uint64_t getTimestampFromServer();
    void active_storage(Transaction *&txn){
        focc_->active_storage(txn);}

    // 传入txn_id作为事务id
    Transaction *Begin(
        Transaction *&txn,
        txn_id_t txn_id,
        IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE);

    // 从meta_server获取时间戳作为txn id
    Transaction *Begin(
        Transaction *&txn,
        IsolationLevel isolation_level = IsolationLevel::SERIALIZABLE);

    bool Abort(Transaction *txn);
    bool AbortSingle(Transaction *txn, bool use_raft = false);

    bool Commit(Transaction *txn);
    bool CommitSingle(Transaction *txn,
                      bool sync_write_set = false,
                      bool use_raft = false);

    bool PrepareCommit(Transaction *txn);
};
