#include "storage/KVStore.h"

#include <string>

#include "Option.h"
#include "dbconfig.h"
#include "recovery/log_record.h"

KVStore::KVStore(const std::string &dir) : diskstorage_(dir) {}
KVStore::~KVStore() { flush(); }
// put(key, value)
bool KVStore::put(const std::string &key, const std::string &value) {
    std::unique_lock<std::mutex> l(mutex_);
    if (enable_logging) {
        // 写Put日志
        // LogRecord record(txn->get_txn_id(),
        //                  txn->get_prev_lsn(),
        //                  LogRecordType::INSERT,
        //                  key.size(),
        //                  key.c_str(),
        //                  value.size(),
        //                  value.c_str());
        // auto lsn = log_manager_->AppendLogRecord(record);
        // txn->set_prev_lsn(lsn);
    }
    memtable_.put(key, value);
    if (memtable_.space() > Option::SSTABLE_SPACE) {
        diskstorage_.add(memtable_);
        memtable_.clear();
    }
    return true;
}

// value = get(key)
std::string KVStore::get(const std::string &key) {
    std::unique_lock<std::mutex> l(mutex_);
    if (memtable_.contains(key)) {
        return memtable_.get(key).second;
    }
    auto result = diskstorage_.search(key);
    return result.second;
}

// del(key)
bool KVStore::del(const std::string &key) {
    std::unique_lock<std::mutex> l(mutex_);
    if (memtable_.contains(key)) {
        if (enable_logging) {
            // 写Put日志
            // LogRecord record(txn->get_txn_id(),
            //                  txn->get_prev_lsn(),
            //                  LogRecordType::DELETE,
            //                  key.size(),
            //                  key.c_str(),
            //                  memtable_.get(key).second.size(),
            //                  memtable_.get(key).second.c_str());
            // auto lsn = log_manager_->AppendLogRecord(record);
            // txn->set_prev_lsn(lsn);
        }
        memtable_.del(key);
        return true;
    }
    auto result = diskstorage_.search(key);
    if (result.first) {
        if (enable_logging) {
            // LogRecord record(txn->get_txn_id(),
            //                  txn->get_prev_lsn(),
            //                  LogRecordType::DELETE,
            //                  key.size(),
            //                  key.c_str(),
            //                  result.second.size(),
            //                  result.second.c_str());
            // auto lsn = log_manager_->AppendLogRecord(record);
            // txn->set_prev_lsn(lsn);
        }
        memtable_.put(key, "");
        return true;
    } else {
        return false;
    }
}

void KVStore::reset() { memtable_.clear(); }

void KVStore::flush() {
    if (!memtable_.empty()) {
        diskstorage_.add(memtable_);
    }
}