#include "transaction_manager.h"

#include <brpc/channel.h>
#include <butil/logging.h>

#include <future>
#include <vector>

#include "transaction_manager.pb.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};
std::shared_mutex TransactionManager::txn_map_mutex = {};

void TransactionManager::Begin(Transaction *&txn,
                               txn_id_t txn_id,
                               IsolationLevel isolation_level) {
    if (txn == nullptr) {
        txn = new Transaction(txn_id, kv_, isolation_level);
    }
    LOG(DEBUG) << "Begin txn_id: " << txn_id;
    if (enable_logging) {
        LogRecord record(
            txn->get_txn_id(), txn->get_prev_lsn(), LogRecordType::BEGIN);
        lsn_t lsn = log_manager_->AppendLogRecord(record);
        txn->set_prev_lsn(lsn);
    }
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    assert(txn_map.find(txn_id) == txn_map.end());
    txn_map[txn_id] = txn;
}

void TransactionManager::Begin(Transaction *&txn,
                               IsolationLevel isolation_level) {
    if (txn == nullptr) {
        Begin(txn, getTimestampFromServer(), isolation_level);
    }
}

uint64_t TransactionManager::getTimestampFromServer() {
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;

    if (channel.Init(FLAGS_META_SERVER_ADDR.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return -1;
    }
    meta_service::MetaService_Stub stub(&channel);
    brpc::Controller cntl;
    meta_service::getTimeStampRequest request;
    meta_service::getTimeStampResponse response;
    stub.GetTimeStamp(&cntl, &request, &response, NULL);

    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
    }
    return response.timestamp();
}

bool TransactionManager::Abort(Transaction *txn) {
    focc_->finish(txn);
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    txn_map.erase(txn->get_txn_id());
    LOG(DEBUG) << "Abort txn_id: " << txn->get_txn_id();
    return true;
}

bool TransactionManager::AbortSingle(Transaction *txn, bool use_raft) {
    focc_->finish(txn);
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    txn_map.erase(txn->get_txn_id());
    LOG(DEBUG) << "Abort txn_id: " << txn->get_txn_id();
    return true;
}

bool TransactionManager::Commit(Transaction *txn) {
    if (txn->get_is_distributed() == false) {
        if ((*txn->get_distributed_node_set()->begin()).ip_addr ==
                FLAGS_SERVER_LISTEN_ADDR &&
            (*txn->get_distributed_node_set()->begin()).port ==
                FLAGS_SERVER_LISTEN_PORT) {
            // 本地事务 无需rpc直接可以调用本地函数
            LOG(DEBUG) << "本地事务";
            if (CommitSingle(txn, true, true) == true) {
                return true;
            } else {
                AbortSingle(txn);
                return false;
            }
        } else {
            LOG(DEBUG) << "非分布式事务，但数据不在协调者节点上";
            // 非分布式事务，但数据不在协调者节点上
            brpc::ChannelOptions options;
            options.timeout_ms = 10000;
            options.max_retry = 3;
            brpc::Channel channel;

            auto node = *txn->get_distributed_node_set()->begin();
            if (channel.Init(node.ip_addr.c_str(), node.port, &options) != 0) {
                LOG(ERROR) << "Fail to initialize channel";
                return false;
            }
            transaction_manager::TransactionManagerService_Stub stub(&channel);
            brpc::Controller cntl;
            transaction_manager::CommitRequest request;
            transaction_manager::CommitResponse response;
            request.set_txn_id(txn->get_txn_id());
            stub.CommitTransaction(&cntl, &request, &response, NULL);
            if (cntl.Failed()) {
                LOG(WARNING) << cntl.ErrorText();
            }
            if (response.ok() == false) {
                cntl.Reset();
                transaction_manager::AbortRequest abort_request;
                transaction_manager::AbortResponse abort_response;
                abort_request.set_txn_id(txn->get_txn_id());
                stub.AbortTransaction(
                    &cntl, &abort_request, &abort_response, NULL);
                if (cntl.Failed()) {
                    LOG(WARNING) << cntl.ErrorText();
                    return false;
                }
                return false;
            }
            return true;
        }
    } else {
        // 分布式事务, 2PC两阶段提交
        brpc::ChannelOptions options;
        options.timeout_ms = 10000;
        options.max_retry = 3;
        std::vector<std::future<bool>> futures_prepare;

        // 准备阶段
        for (auto node : *txn->get_distributed_node_set()) {
            futures_prepare.push_back(std::async(
                std::launch::async, [&txn, node, &options]() -> bool {
                    // LOG(INFO) << "prepare pause: " <<  node.ip_addr << " " <<
                    // node.port << std::endl;
                    brpc::Channel channel;
                    if (channel.Init(
                            node.ip_addr.c_str(), node.port, &options) != 0) {
                        LOG(ERROR) << "Fail to initialize channel";
                        return false;
                    }
                    transaction_manager::TransactionManagerService_Stub stub(
                        &channel);
                    brpc::Controller cntl;
                    transaction_manager::PrepareRequest request;
                    transaction_manager::PrepareResponse response;
                    request.set_txn_id(txn->get_txn_id());

                    // hcydebugfault
                    // 设置协调者监听ip
                    // transaction_manager::PrepareRequest_IP_Port t;
                    // t.set_ip_addr(FLAGS_SERVER_LISTEN_ADDR);
                    // t.set_port(FLAGS_SERVER_LISTEN_PORT);
                    // request.set_allocated_coor_ip(&t);

                    // // 为了容错, 将事务涉及到的所有节点发送给每一个参与者
                    // for(auto x : *txn->get_distributed_node_set() ){
                    //     transaction_manager::PrepareRequest_IP_Port* t =
                    //     request.add_ips(); t->set_ip_addr(x.ip_addr.c_str());
                    //     t->set_port(x.port);
                    // }

                    stub.PrepareTransaction(&cntl, &request, &response, NULL);
                    if (cntl.Failed()) {
                        LOG(WARNING) << cntl.ErrorText();
                    }
                    return response.ok();
                }));
        }
        // 检查各个参与者准备结果
        bool commit_flag = true;
        for (size_t i = 0; i < (*txn->get_distributed_node_set()).size(); i++) {
            if (futures_prepare[i].get() == false) {
                commit_flag = false;
                break;
            }
        }
        // TODO: write Coordinator backup

        // 提交阶段
        if (commit_flag == false) {
            // abort all
            Abort(txn);
        } else {
            // commit all
            std::vector<std::future<void>> futures_commit;
            for (auto node : *txn->get_distributed_node_set()) {
                // LOG(INFO) << "commit pause: " <<  node.ip_addr << " " <<
                // node.port << std::endl;
                futures_commit.push_back(
                    std::async(std::launch::async, [&txn, node, &options]() {
                        brpc::Channel channel;
                        if (channel.Init(node.ip_addr.c_str(),
                                         node.port,
                                         &options) != 0) {
                            LOG(ERROR) << "Fail to initialize channel";
                            // return false;
                        }
                        transaction_manager::TransactionManagerService_Stub
                            stub(&channel);
                        brpc::Controller cntl;
                        transaction_manager::CommitRequest request;
                        transaction_manager::CommitResponse response;
                        request.set_txn_id(txn->get_txn_id());
                        stub.CommitTransaction(
                            &cntl, &request, &response, NULL);
                        if (cntl.Failed()) {
                            LOG(WARNING) << cntl.ErrorText();
                        }
                        // return response.ok();
                    }));
            }
            for (size_t i = 0; i < (*txn->get_distributed_node_set()).size();
                 i++) {
                futures_commit[i].get();
            }
        }
        return commit_flag;
    }
    return true;
}
bool TransactionManager::CommitSingle(Transaction *txn,
                                      bool sync_write_set,
                                      bool use_raft) {
    bool check = focc_->validate(txn);
    if (check == false) {
        AbortSingle(txn);
        return false;
    }
    if (enable_logging) {
        LogRecord record(
            txn->get_txn_id(), txn->get_prev_lsn(), LogRecordType::COMMIT);
        auto lsn = log_manager_->AppendLogRecord(record);
        txn->set_prev_lsn(lsn);
        // force log
        log_manager_->Flush(lsn, true);
    }
    txn->commit();
    focc_->finish(txn);
    std::unique_lock<std::shared_mutex> l(txn_map_mutex);
    txn_map.erase(txn->get_txn_id());
    LOG(DEBUG) << "Commit txn_id: " << txn->get_txn_id();
    return check;
}

bool TransactionManager::PrepareCommit(Transaction *txn) {
    return focc_->validate(txn);
}