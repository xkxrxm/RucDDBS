#ifndef STORAGE_KV_STORE_H
#define STORAGE_KV_STORE_H

#include <cstdint>
#include <mutex>
#include <string>

#include "DiskStorage.h"
#include "KVStoreAPI.h"
#include "SkipList.h"

class Transaction;
class LogManager;
class KVStore : public KVStoreAPI {
public:
    explicit KVStore(const std::string &dir);

    ~KVStore();

    KVStore(const KVStore &) = delete;

    // put(key, value)
    bool put(const std::string &key, const std::string &value);
    // value = get(key)
    std::string get(const std::string &key);
    // del(key)
    bool del(const std::string &key);

    // clear memtable and disk
    void reset();
    // flush memtable to disk
    void flush();

private:
    SkipList memtable_;
    DiskStorage diskstorage_;
    std::mutex mutex_;
    LogManager *log_manager_;
};

#endif