#include "transaction_manager.h"
#include "transaction_manager.pb.h"
#include <brpc/channel.h>
#include <butil/logging.h>

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};
std::shared_mutex TransactionManager::txn_map_mutex = {};

Transaction* TransactionManager::Begin(Transaction*& txn, txn_id_t txn_id, IsolationLevel isolation_level){
  global_txn_latch_.lock_shared();
  if (txn == nullptr)
  {
      txn = new Transaction(txn_id, isolation_level);
  }
  LOG(WARNING)<< "Need to consider the lock here.";
//   std::unique_lock<std::shared_mutex> l(txn_map_mutex);
  assert(txn_map.find(txn_id) == txn_map.end());
  txn_map[txn_id] = txn;
  return txn;
}

Transaction* TransactionManager::Begin(Transaction*& txn, IsolationLevel isolation_level){
    if (txn == nullptr)
    {
        return Begin(txn, getTimestampFromServer(), isolation_level);
    }
  return txn;
}

uint64_t TransactionManager::getTimestampFromServer(){
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;

    if (channel.Init(FLAGS_META_SERVER_ADDR.c_str(), &options) != 0)
    {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
    meta_service::MetaService_Stub stub(&channel);
    brpc::Controller cntl;
    meta_service::getTimeStampRequest request;
    meta_service::getTimeStampResponse response;
    stub.GetTimeStamp(&cntl, &request, &response, NULL);

    if (cntl.Failed())
    {
        LOG(WARNING) << cntl.ErrorText();
    }
    return response.timestamp();
}

bool TransactionManager::Abort(Transaction *txn)
{
    assert(false);
}

bool TransactionManager::AbortSingle(Transaction *txn, bool use_raft)
{
    focc_->finish(txn);
    // Remove txn from txn_map
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    txn_map.erase(txn->get_txn_id());
    // Release the global transaction latch.
    global_txn_latch_.unlock_shared();
    return true;
}

bool TransactionManager::Commit(Transaction *txn)
{
    LOG(WARNING)<<"Commit is not implemented yet.";
    return true;
}
bool TransactionManager::CommitSingle(Transaction *txn,
                                      bool sync_write_set,
                                      bool use_raft)
{
    bool check = focc_->validate(txn);
    focc_->finish(txn);
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    txn_map.erase(txn->get_txn_id());
    // Release the global transaction latch.
    global_txn_latch_.unlock_shared();
    return check;
}

bool TransactionManager::PrepareCommit(Transaction *txn)
{
    return focc_->validate(txn);
}

bool TransactionManager::ReadRow(Transaction *txn, Row_occ *row)
{
    bool rc = row->access(txn, RD);
    if (rc == true)
        focc_->active_storage(txn);
    return rc;
}