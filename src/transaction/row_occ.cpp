/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "row_occ.h"

#include "transaction.h"
#include "txn.h"

void Row_occ::init(std::string key)
{
    _key = key;
    _latch = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(_latch, NULL);
    sem_init(&_semaphore, 0, 1);
    wts = 0;
    lock_tid = 0;
    blatch = false;
}

bool Row_occ::access(Transaction *txn, access_t type)
{
    bool rc = true;
    sem_wait(&_semaphore);
    if (type == RD)
    {
        if (lock_tid != 0 && lock_tid != txn->get_txn_id())
        {
            rc = false;
        }
        else
        {
            txn->add_read_set(this);
            rc = true;
        }
    }
    else
        assert(false);
    sem_post(&_semaphore);
    return rc;
}

void Row_occ::latch()
{
    sem_wait(&_semaphore);
}

bool Row_occ::validate(uint64_t ts)
{
    if (ts < wts)
        return false;
    else
        return true;
}

void Row_occ::write(std::string data, uint64_t ts)
{
    _value = data;
}

void Row_occ::release()
{
    sem_post(&_semaphore);
}

/* --------------- only used for focc -----------------------*/

bool Row_occ::try_lock(uint64_t tid)
{
    bool success = true;
    sem_wait(&_semaphore);
    if (lock_tid == 0 || lock_tid == tid)
    {
        lock_tid = tid;
    }
    else
        success = false;
    sem_post(&_semaphore);
    return success;
}

void Row_occ::release_lock(uint64_t tid)
{
    sem_wait(&_semaphore);
    if (lock_tid == tid)
    {
        lock_tid = 0;
    }
    sem_post(&_semaphore);
}

uint64_t Row_occ::check_lock()
{
    sem_wait(&_semaphore);
    uint64_t tid = lock_tid;
    sem_post(&_semaphore);
    return tid;
}

/* --------------- only used for focc -----------------------*/
