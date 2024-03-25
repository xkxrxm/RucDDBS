#include "transaction_manager.h"

Transaction* TransactionManager::Begin(Transaction*& txn, txn_id_t txn_id, IsolationLevel isolation_level){
  global_txn_latch_.lock_shared();
    if (txn == nullptr) {
        txn = new Transaction(txn_id, isolation_level); 
    }
  return txn;
}

Transaction* TransactionManager::Begin(Transaction*& txn, IsolationLevel isolation_level){
  global_txn_latch_.lock_shared();
  txn_id_t txn_id = 0;    // fake txn_id
    if (txn == nullptr) {
        txn = new Transaction(txn_id, isolation_level); 
    }
  return txn;
}