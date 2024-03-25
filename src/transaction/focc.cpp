#include "focc.h"

#include "transaction.h"

void Focc::init()
{
    sem_init(&_semaphore, 0, 1);
    tnc = 0;
    active_len = 0;
    active = NULL;
    // lock_all = false;
}

bool Focc::validate(Transaction *&txn)
{
    return central_validate(txn);
}

void Focc::finish(Transaction *&txn)
{
    return central_finish(txn);
}

void Focc::active_storage(Transaction *&txn)
{
    f_set_ent *act = active;
    f_set_ent *rset = txn->get_read_set();
    if (rset->set_size == 0)
    {
        return;
    }
    sem_wait(&_semaphore);
    while (act != NULL && act->txn != txn)
    {
        act = act->next;
    }
    if (act == NULL)
    {
        active_len++;
        STACK_PUSH(active, rset);
    }
    else
    {
        act = rset;
    }
    sem_post(&_semaphore);
}

bool Focc::central_validate(Transaction *&txn)
{
    bool rc;
    // uint64_t starttime = TransactionManager::getTimestampFromServer();
    // uint64_t total_starttime = starttime;
    // uint64_t start_tn = txn->get_start_time();

    bool valid = true;
    // OptCC is centralized. No need to do per partition malloc.
    f_set_ent *wset = txn->get_write_set();
    bool readonly = (wset->set_size == 0);
    f_set_ent *ent;
    // uint64_t checked = 0;
    // uint64_t active_checked = 0;

    sem_wait(&_semaphore);
    // starttime = TransactionManager::getTimestampFromServer();

    ent = active;
    // In order to prevent cross between other read sets and the current
    // transaction write set during verification, the write set is first locked.
    if (!readonly)
    {
        for (uint64_t i = 0; i < wset->set_size; i++)
        {
            auto row = wset->rows[i];
            if (!row->try_lock(txn->get_txn_id()))
            {
                rc = false;
            }
        }
        if (rc == false)
        {
            for (uint64_t i = 0; i < wset->set_size; i++)
            {
                auto row = wset->rows[i];
                row->release_lock(txn->get_txn_id());
            }
        }
    }
    if (!readonly)
    {
        for (ent = active; ent != NULL; ent = ent->next)
        {
            f_set_ent *ract = ent;
            valid = test_valid(ract, wset);
            if (!valid)
            {
                goto final;
            }
        }
    }
    // starttime = TransactionManager::getTimestampFromServer();
final:
    if (valid)
    {
        rc = true;
    }
    else
    {
        rc = false;
        // Optimization: If this is aborting, remove from active set now
        f_set_ent *act = active;
        f_set_ent *prev = NULL;
        while (act != NULL && act->txn != txn)
        {
            prev = act;
            act = act->next;
        }
        if (act != NULL && act->txn == txn)
        {
            if (prev != NULL)
            {
                prev->next = act->next;
            }
            else
            {
                active = act->next;
            }
            active_len--;
        }
    }
    sem_post(&_semaphore);
    return rc;
}

void Focc::central_finish(Transaction *&txn)
{
    f_set_ent *wset = txn->get_write_set();

    // only update active & tnc for non-readonly transactions
    // uint64_t starttime = TransactionManager::getTimestampFromServer();
    f_set_ent *act = active;
    f_set_ent *prev = NULL;
    sem_wait(&_semaphore);
    while (act != NULL && act->txn != txn)
    {
        prev = act;
        act = act->next;
    }
    if (act == NULL)
    {
        sem_post(&_semaphore);
        return;
    }
    assert(act->txn == txn);
    if (prev != NULL)
    {
        prev->next = act->next;
    }
    else
    {
        active = act->next;
    }
    active_len--;

    for (uint64_t i = 0; i < wset->set_size; i++)
    {
        auto row = wset->rows[i];
        row->release_lock(txn->get_txn_id());
    }

    sem_post(&_semaphore);
}

bool Focc::test_valid(f_set_ent *set1, f_set_ent *set2)
{
    for (u_int32_t i = 0; i < set1->set_size; i++)
    {
        for (u_int32_t j = 0; j < set2->set_size; j++)
        {
            if (set1->txn == set2->txn)
                continue;
            if (set1->rows[i] == set2->rows[j])
            {
                return false;
            }
        }
    }
    return true;
}
