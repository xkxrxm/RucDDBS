#pragma once

#include "recovery/log_manager.h"
#include "storage/KVStore_beta.h"
#include "transaction/transaction.h"

class KVStoreFocc : public KVStore_beta {
public:
    explicit KVStoreFocc(const std::string &dir, LogManager *log_manager)
        : KVStore_beta(dir), log_manager_(log_manager){};

    ~KVStoreFocc() { flush(); };

    using KVStore_beta::del;
    using KVStore_beta::get;
    using KVStore_beta::put;

    std::pair<bool, std::string> get(const std::string &key, Transaction *txn) {
        txn->add_read_set(key);
        return KVStore_beta::get(key);
    }
    // put(key, value)
    void put(const std::string &key,
             const std::string &value,
             Transaction *txn) {
        if (enable_logging) {
            // 写Put日志
            LogRecord record(txn->get_txn_id(),
                             txn->get_prev_lsn(),
                             LogRecordType::INSERT,
                             key.size(),
                             key.c_str(),
                             value.size(),
                             value.c_str());
            auto lsn = log_manager_->AppendLogRecord(record);
            txn->set_prev_lsn(lsn);
        }
        // add write record into write set.
        txn->add_write_set(key);
        txn->add_modify(Modify{Put{key, value}});
        KVStore_beta::put(key, value);
    }

    // del(key)
    bool del(const std::string &key, Transaction *txn) {
        std::unique_lock<std::mutex> l(mutex_);
        bool if_in_mem = memtable_.contains(key);
        if (if_in_mem) {
            if (enable_logging) {
                // 写Del日志
                LogRecord record(txn->get_txn_id(),
                                 txn->get_prev_lsn(),
                                 LogRecordType::DELETE,
                                 key.size(),
                                 key.c_str(),
                                 memtable_.get(key).second.size(),
                                 memtable_.get(key).second.c_str());
                auto lsn = log_manager_->AppendLogRecord(record);
                txn->set_prev_lsn(lsn);
            }
            // add write record into write set.
            txn->add_write_set(key);
            txn->add_modify(Modify{Delete{key}});
            memtable_.del(key);
            return true;
        }
        auto result = diskstorage_.search(key);
        if (result.first && result.second != "") {
            if (enable_logging) {
                // 写Del日志
                LogRecord record(txn->get_txn_id(),
                                 txn->get_prev_lsn(),
                                 LogRecordType::DELETE,
                                 key.size(),
                                 key.c_str(),
                                 result.second.size(),
                                 result.second.c_str());
                auto lsn = log_manager_->AppendLogRecord(record);
                txn->set_prev_lsn(lsn);
            }
            // add write record into write set.
            txn->add_write_set(key);
            txn->add_modify(Modify{Delete{key}});
            memtable_.put(key, "");
            return true;
        } else {
            LOG(INFO) << "Key not found: " << key;
            return false;
        }
    }

private:
    LogManager *log_manager_;
};