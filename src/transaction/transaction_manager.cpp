#include "transaction_manager.h"
#include "transaction_manager.pb.h"
#include <brpc/channel.h>
#include <butil/logging.h>

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

uint64_t TransactionManager::getTimestampFromServer(){
    // brpc::Channel channel;
    // brpc::ChannelOptions options;
    // options.timeout_ms = 10000;
    // options.max_retry = 3;

    // if (channel.Init(FLAGS_META_SERVER_ADDR.c_str(), &options) != 0) {
    //     LOG(ERROR) << "Fail to initialize channel";
    //     return -1;
    // }
    // meta_service::MetaService_Stub stub(&channel);
    // brpc::Controller cntl;
    // meta_service::getTimeStampRequest request;
    // meta_service::getTimeStampResponse response;
    // stub.GetTimeStamp(&cntl, &request, &response, NULL);

    // if(cntl.Failed()) {
    //     LOG(WARNING) << cntl.ErrorText();
    // }
    // return response.timestamp();
}