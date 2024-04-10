#pragma once
#include <semaphore.h>

#include "txn.h"
#include "KVStore_occ.h"

#define STACK_PUSH(stack, entry) \
    {                            \
        entry->next = stack;     \
        stack = entry;           \
    }

class Transaction;
class rw_set;
class Focc
{
public:
    void init();
    bool validate(Transaction *&txn);
    void finish(Transaction *&txn);
    void active_storage(Transaction *&txn);
    // volatile bool lock_all;
    // uint64_t lock_txn_id;

private:
    // serial validation in the original OCC paper.
    bool central_validate(Transaction *&txn);

    void central_finish(Transaction *&txn);
    bool test_valid(rw_set *set1, rw_set *set2);
    // bool get_rw_set(Transaction *&txn, rw_set *&rset, rw_set *&wset);

    // // "history" stores write set of transactions with txn_id >= smallest
    // running txn_id

    rw_set *active;
    uint64_t active_len;
    volatile uint64_t tnc;  // transaction number counter
    pthread_mutex_t latch;
    sem_t _semaphore;

    sem_t _active_semaphore;
    KVStorage *storage;
};