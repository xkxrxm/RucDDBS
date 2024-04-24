#include "focc.h"

#include <butil/logging.h>

#include "transaction.h"

void Focc::init() {
    sem_init(&_semaphore, 0, 1);
    tnc = 0;
    active_len = 0;
    active = NULL;
}

bool Focc::validate(Transaction *&txn) { return central_validate(txn); }

void Focc::finish(Transaction *&txn) { return central_finish(txn); }

void Focc::active_storage(Transaction *&txn) {
    rw_set *act = active;
    rw_set *rset = txn->get_read_set();
    if (rset->keys.size() == 0) {
        return;
    }
    sem_wait(&_semaphore);
    while (act != NULL && act->txn_id != txn->get_txn_id()) {
        act = act->next;
    }
    if (act == NULL) {
        active_len++;
        STACK_PUSH(active, rset);
    } else {
        act = rset;
    }
    sem_post(&_semaphore);
}

bool Focc::central_validate(Transaction *&txn) {
    bool rc;

    bool valid = true;
    // OptCC is centralized. No need to do per partition malloc.
    rw_set *wset = txn->get_write_set();
    bool readonly = (wset->keys.size() == 0);
    rw_set *ent;

    sem_wait(&_semaphore);

    ent = active;
    if (!readonly) {
        for (uint64_t i = 0; i < wset->keys.size(); i++) {
            if (txn->try_lock(wset->keys[i]) == false) {
                rc = false;
            }
        }
        if (rc == false) {
            for (uint64_t i = 0; i < wset->keys.size(); i++) {
                txn->release_lock(wset->keys[i]);
            }
            sem_post(&_semaphore);
            return false;
        }
    }
    if (!readonly) {
        for (ent = active; ent != NULL; ent = ent->next) {
            rw_set *ract = ent;
            valid = test_valid(ract, wset);
            if (!valid) {
                break;
            }
        }
    }

    if (valid) {
        rc = true;
    } else {
        rc = false;
        // Optimization: If this is aborting, remove from active set now
        rw_set *act = active;
        rw_set *prev = NULL;
        while (act != NULL && act->txn_id != txn->get_txn_id()) {
            prev = act;
            act = act->next;
        }
        if (act != NULL && act->txn_id != txn->get_txn_id()) {
            if (prev != NULL) {
                prev->next = act->next;
            } else {
                active = act->next;
            }
            active_len--;
        }
    }
    sem_post(&_semaphore);
    return rc;
}

void Focc::central_finish(Transaction *&txn) {
    rw_set *wset = txn->get_write_set();
    rw_set *act = active;
    rw_set *prev = NULL;
    sem_wait(&_semaphore);
    for (uint64_t i = 0; i < wset->keys.size(); i++) {
        txn->release_lock(wset->keys[i]);
    }
    while (act != NULL && act->txn_id != txn->get_txn_id()) {
        prev = act;
        act = act->next;
    }
    if (act == NULL) {
        sem_post(&_semaphore);
        return;
    }
    assert(act->txn_id == txn->get_txn_id());
    if (prev != NULL) {
        prev->next = act->next;
    } else {
        active = act->next;
    }
    active_len--;

    sem_post(&_semaphore);
}

bool Focc::test_valid(rw_set *set1, rw_set *set2) {
    for (u_int32_t i = 0; i < set1->keys.size(); i++) {
        for (u_int32_t j = 0; j < set2->keys.size(); j++) {
            if (set1->txn_id == set2->txn_id) continue;
            if (set1->keys[i] == set2->keys[j]) {
                return false;
            }
        }
    }
    return true;
}
