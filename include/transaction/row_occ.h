#pragma once

#include <semaphore.h>

#include <string>
using txn_id_t = uint64_t;

class Transaction;
enum access_t
{
    RD,
    WR,
    XP,
    SCAN
};
class Row_occ
{
public:
    void init(std::string key);
    bool access(Transaction *txn, access_t type);
    void latch();
    // ts is the start_ts of the validating txn
    bool validate(uint64_t ts);
    void write(std::string data, uint64_t ts);
    void release();

    /* --------------- only used for focc -----------------------*/
    bool try_lock(uint64_t tid);
    void release_lock(uint64_t tid);
    uint64_t check_lock();
    Row_occ(std::string key)
    {
        init(key);
    }
    /* --------------- only used for focc -----------------------*/
private:
    pthread_mutex_t *_latch;
    sem_t _semaphore;
    bool blatch;

    std::string _key;
    std::string _value;
    // the last update time
    uint64_t wts;

    /* --------------- only used for focc -----------------------*/
    uint64_t lock_tid;
    /* --------------- only used for focc -----------------------*/
};

class f_set_ent
{
public:
    f_set_ent(txn_id_t txn_id,
              Transaction *txn,
              u_int32_t set_size = 0,
              Row_occ **rows = nullptr,  //[MAX_WRITE_SET];
              f_set_ent *next = nullptr)
        : txn_id(txn_id), txn(txn), next(next), set_size(set_size), rows(rows)
    {
    }
    txn_id_t txn_id;
    Transaction *txn;
    u_int32_t set_size;
    Row_occ **rows;  //[MAX_WRITE_SET];
    f_set_ent *next;
};