#include "transaction.h"
#include <semaphore.h>
class Row_occ
{
public:
    void init(std::string key);
    // bool access(Transaction *txn, access_t type);
    void latch();
    // ts is the start_ts of the validating txn
    bool validate(uint64_t ts);
    void write(std::string data, uint64_t ts);
    void release();

    /* --------------- only used for focc -----------------------*/
    bool try_lock(uint64_t tid);
    void release_lock(uint64_t tid);
    uint64_t check_lock();
    /* --------------- only used for focc -----------------------*/
private:
    pthread_mutex_t *_latch;
    sem_t _semaphore;
    bool blatch;

    std::string _key;
    // the last update time
    uint64_t wts;

    /* --------------- only used for focc -----------------------*/
    uint64_t lock_tid;
    /* --------------- only used for focc -----------------------*/
};

class f_set_ent
{
public:
    f_set_ent();
    txn_id_t tn;
    Transaction *txn;
    u_int32_t set_size;
    Row_occ **rows;  //[MAX_WRITE_SET];
    f_set_ent *next;
};