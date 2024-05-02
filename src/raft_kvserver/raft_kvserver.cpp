#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/controller.h>      // brpc::Controller
#include <brpc/server.h>          // brpc::Server
#include <gflags/gflags.h>        // DEFINE_*

#include "kv.pb.h"
#include "storage/KVStore.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(
    election_timeout_ms,
    5000,
    "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf,
              "127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,",
              "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "KV", "Id of the replication group");

namespace kv {
class KV : public braft::StateMachine {
public:
    void get(const ::kv::GetRequest *request, ::kv::GetResponse *response) {
        // if (!is_leader()) {
        //     redirect(response);
        //     return;
        // }
        response->set_value(store_->get(request->key()));
    }
    void put(const ::kv::PutRequest *request, ::kv::PutResponse *response) {
        // if (!is_leader()) {
        //     redirect(response);
        //     return;
        // }
        store_->put(request->key(), request->value());
    }

    void del(const ::kv::DelRequest *request, ::kv::DelResponse *response) {
        // if (!is_leader()) {
        //     redirect(response);
        //     return;
        // }
        response->set_ok(store_->del(request->key()));
    }

    void reset(const ::kv::ResetRequest *request,
               ::kv::ResetResponse *response) {
        // if (!is_leader()) {
        //     redirect(response);
        //     return;
        // }
        store_->reset();
    }

    void flush(const ::kv::FlushRequest *request,
               ::kv::FlushResponse *response) {
        // if (!is_leader()) {
        //     redirect(response);
        //     return;
        // }
        store_->flush();
    }

public:
    KV() : _node(NULL), _leader_term(-1) {}
    ~KV() { delete _node; }
    int start() {
        butil::EndPoint addr(butil::my_ip(), FLAGS_port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << FLAGS_conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = FLAGS_election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = -1;
        std::string prefix = "local://" + FLAGS_data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = FLAGS_disable_cli;
        braft::Node *node = new braft::Node(FLAGS_group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        store_ = new KVStore(FLAGS_data_path + "/kv");
        return 0;
    }

    bool is_leader() const {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    // Shut this node down.
    void shutdown() {
        if (_node) {
            _node->shutdown(NULL);
        }
    }

    // Blocking this thread until the node is eventually down.
    void join() {
        if (_node) {
            _node->join();
        }
    }

private:
    // void redirect(::kv::GetResponse *response) {
    //     response->set_value("");
    //     if (_node) {
    //         braft::PeerId leader = _node->leader_id();
    //         if (!leader.is_empty()) {
    //             response->set_redirect(leader.to_string());
    //         }
    //     }
    // }
    void on_apply(braft::Iterator &iter) {
        LOG(ERROR) << "????";
        // do nothing
    }
    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status &status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }
    void on_shutdown() { LOG(INFO) << "This node is down"; }
    void on_error(const ::braft::Error &e) {
        LOG(ERROR) << "Met raft error " << e;
    }
    void on_configuration_committed(const ::braft::Configuration &conf) {
        LOG(INFO) << "Configuration of this group is " << conf;
    }
    void on_stop_following(const ::braft::LeaderChangeContext &ctx) {
        LOG(INFO) << "Node stops following " << ctx;
    }
    void on_start_following(const ::braft::LeaderChangeContext &ctx) {
        LOG(INFO) << "Node start following " << ctx;
    }

private:
    braft::Node *volatile _node;
    butil::atomic<int64_t> _leader_term;
    KVStore *store_;
};  // namespace braft::StateMachine
class KVServiceImpl final : public KVService {
private:
    KV *kv_;

public:
    explicit KVServiceImpl(KV *kv) : kv_(kv) {}
    ~KVServiceImpl() {}
    void Put(::google::protobuf::RpcController *controller,
             const ::kv::PutRequest *request,
             ::kv::PutResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        kv_->put(request, response);
        LOG(INFO) << "Put key: " << request->key()
                  << " value: " << request->value();
    };
    void Get(::google::protobuf::RpcController *controller,
             const ::kv::GetRequest *request,
             ::kv::GetResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        kv_->get(request, response);
        LOG(INFO) << "Get key: " << request->key()
                  << " value: " << response->value();
    };
    void Del(::google::protobuf::RpcController *controller,
             const ::kv::DelRequest *request,
             ::kv::DelResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        kv_->del(request, response);
        LOG(INFO) << "Del key: " << request->key();
    };
    void Reset(::google::protobuf::RpcController *controller,
               const ::kv::ResetRequest *request,
               ::kv::ResetResponse *response,
               ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        kv_->reset(request, response);
        LOG(INFO) << "Reset";
    };
    void Flush(::google::protobuf::RpcController *controller,
               const ::kv::FlushRequest *request,
               ::kv::FlushResponse *response,
               ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        kv_->flush(request, response);
        LOG(INFO) << "Flush";
    };
};
}  // namespace kv

int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    butil::AtExitManager exit_manager;

    // Generally you only need one Server.
    brpc::Server server;
    kv::KV kv;
    kv::KVServiceImpl service(&kv);

    // Add your service into RPC rerver
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    // raft can share the same RPC server. Notice the second parameter, because
    // adding services into a running server is not allowed and the listen
    // address of this server is impossible to get before the server starts. You
    // have to specify the address of the server.
    if (braft::add_service(&server, FLAGS_port) != 0) {
        LOG(ERROR) << "Fail to add raft service";
        return -1;
    }

    // It's recommended to start the server before Atomic is started to avoid
    // the case that it becomes the leader while the service is unreacheable by
    // clients.
    // Notice the default options of server is used here. Check out details from
    // the doc of brpc if you would like change some options;
    if (server.Start(FLAGS_port, NULL) != 0) {
        LOG(ERROR) << "Fail to start Server";
        return -1;
    }

    // It's ok to start kv;
    if (kv.start() != 0) {
        LOG(ERROR) << "Fail to start Atomic";
        return -1;
    }

    LOG(INFO) << "kv service is running on " << server.listen_address();
    // Wait until 'CTRL-C' is pressed. then Stop() and Join() the service
    while (!brpc::IsAskedToQuit()) {
        sleep(1);
    }

    LOG(INFO) << "kv service is going to quit";

    // Stop before server
    kv.shutdown();
    server.Stop(0);

    // Wait until all the processing tasks are over.
    kv.join();
    server.Join();
    return 0;
}