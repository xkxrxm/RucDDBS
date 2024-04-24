#ifndef RECOVERY_MANAGER_H
#define RECOVERY_MANAGER_H

#include "log_manager.h"
#include "log_record.h"
// #include "KVStore.h"
#include <unordered_map>

#include "KVStore.h"

class RecoveryManager {
public:
    RecoveryManager(LogStorage *log_storage,
                    LogManager *log_manager,
                    KVStore *kv)
        : log_storage_(log_storage), log_manager_(log_manager), kv_(kv) {
        buffer_ = new char[LOG_BUFFER_SIZE];
        memset(buffer_, 0, LOG_BUFFER_SIZE);
    }

    ~RecoveryManager() { delete[] buffer_; }

    /**
     * @brief
     * Perform the recovery procedure
     */
    void ARIES();

private:
    // helper function
    void Scan();
    void Redo();
    void Undo();
    void RedoLog(LogRecord &log_record);
    void UndoLog(LogRecord &log_record);

    // buffer used to store log data
    char *buffer_{nullptr};

    LogStorage *log_storage_;
    LogManager *log_manager_;
    KVStore *kv_;
    // we need to keep track of what txn we need to undo
    // txn -> last lsn
    std::unordered_map<txn_id_t, lsn_t> active_txn_;
    // remember the offset of a log record
    // lsn -> (offset in disk, size of log)
    std::unordered_map<lsn_t, std::pair<int, int>> lsn_mapping_;
};

#endif