#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include "engine.h"
#include "server.h"
#include "session.pb.h"
#include "transaction_manager_rpc.h"

DEFINE_int32(port, 8005, "TCP Port of this server");
DEFINE_string(listen_addr,
              "",
              "Server listen address, may be IPV4/IPV6/UDS."
              " If this is set, the flag port will be ignored");
DEFINE_int32(idle_timeout_s,
             -1,
             "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_string(store_path,
              "/home/xkx/ruc_workspace/RucDDBS2/data1",
              "server1 store");
DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(
    election_timeout_ms,
    5000,
    "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf,
              "127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,",
              "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "RaftLog", "Id of the replication group");

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    // auto lock_manager_ = std::make_unique<Lock_manager>(true);
    auto log_storage_ = std::make_unique<LogStorage>("test_db");
    auto log_manager_ = std::make_unique<LogManager>(log_storage_.get());

    auto kv_ = std::make_unique<KVStore>(LOG_DIR, log_manager_.get());
    auto focc_ = std::make_unique<Focc>();
    focc_->init();
    auto transaction_manager_sql = std::make_unique<TransactionManager>(
        kv_.get(), log_manager_.get(), focc_.get());

    brpc::Server server;
    session::Session_Service_Impl session_service_impl(
        transaction_manager_sql.get());
    transaction_manager::TransactionManagerImpl trans_manager_impl(
        transaction_manager_sql.get());
    RaftLogServiceImpl raft_log_service_impl(log_manager_.get());

    if (server.AddService(&session_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    if (server.AddService(&trans_manager_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }

    if (server.AddService(&raft_log_service_impl,
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
    }

    butil::EndPoint point;
    if (!FLAGS_listen_addr.empty()) {
        if (butil::str2endpoint(FLAGS_listen_addr.c_str(), &point) < 0) {
            LOG(ERROR) << "Invalid listen address:" << FLAGS_listen_addr;
            return -1;
        }
    } else {
        point = butil::EndPoint(butil::IP_ANY, FLAGS_port);
    }

    // 对当前节点做一些操作
    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(point, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    if (log_manager_->start(FLAGS_port,
                            FLAGS_election_timeout_ms,
                            FLAGS_snapshot_interval,
                            FLAGS_disable_cli,
                            FLAGS_conf,
                            FLAGS_data_path,
                            FLAGS_group) != 0) {
        LOG(ERROR) << "Fail to start log manager";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}