#pragma once
#include <braft/protobuf_file.h>  // braft::ProtoBufFile
#include <braft/raft.h>           // braft::Node braft::StateMachine
#include <braft/storage.h>        // braft::SnapshotWriter
#include <braft/util.h>           // braft::AsyncClosureGuard
#include <brpc/controller.h>      // brpc::Controller
#include <brpc/server.h>          // brpc::Server
#include <string.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "dbconfig.h"
#include "log_record.h"
#include "raft_log.pb.h"
#include "storage/LogStorage.h"
using namespace raft_log;

static constexpr int BUFFER_POOL_SIZE = 10;  // size of buffer pool
static constexpr int PAGE_SIZE = 4096;       // size of a data page in byte
static constexpr int LOG_BUFFER_SIZE =
    ((BUFFER_POOL_SIZE + 1) * PAGE_SIZE);  // size of a log buffer in byte
static constexpr auto LOG_TIMEOUT = std::chrono::milliseconds(30);

class LogManager;
class ApplyLogClosure : public braft::Closure {
public:
    ApplyLogClosure(LogManager *log_manager,
                    const SendRaftLogRequest *request,
                    SendRaftLogResponse *response,
                    google::protobuf::Closure *done)
        : _log_manager(log_manager),
          _request(request),
          _response(response),
          _done(done) {}
    ~ApplyLogClosure() {}
    const SendRaftLogRequest *request() const { return _request; }
    SendRaftLogResponse *response() const { return _response; }
    void Run() {
        std::unique_ptr<ApplyLogClosure> self_guard(this);
        // Repsond this RPC.
        brpc::ClosureGuard done_guard(_done);
        if (status().ok()) {
            return;
        }
    }

private:
    LogManager *_log_manager;
    const SendRaftLogRequest *_request;
    SendRaftLogResponse *_response;
    google::protobuf::Closure *_done;
};

class LogManager : public braft::StateMachine {
public:
    explicit LogManager(LogStorage *log_storage)
        : _node(NULL),
          _leader_term(-1),
          enable_flushing_(true),
          needFlush_(false),
          next_lsn_(0),
          persistent_lsn_(INVALID_LSN),
          flush_lsn_(INVALID_LSN),
          log_storage_(log_storage) {
        log_buffer_ = new char[LOG_BUFFER_SIZE];
        flush_buffer_ = new char[LOG_BUFFER_SIZE];
        RunFlushThread();
    }

    ~LogManager() {
        StopFlushThread();
        delete[] log_buffer_;
        delete[] flush_buffer_;
        log_buffer_ = nullptr;
        flush_buffer_ = nullptr;
    }

    void RunFlushThread();

    void StopFlushThread() {
        enable_flushing_.store(false);
        flush_thread_->join();
        delete flush_thread_;
    }

    void Flush(lsn_t lsn, bool force);

    lsn_t AppendLogRecord(LogRecord &log_record);

    void SwapBuffer();

    inline lsn_t GetNextLsn() const { return next_lsn_; }
    inline void SetNextLsn(lsn_t next_lsn) { next_lsn_ = next_lsn; }
    inline lsn_t GetFlushLsn() const { return flush_lsn_; }
    inline char *GetLogBuffer() const { return log_buffer_; }
    inline lsn_t GetPersistentLsn() const { return persistent_lsn_; }
    void SendRaftLog(const SendRaftLogRequest *request,
                     SendRaftLogResponse *response,
                     google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        const int64_t term = _leader_term.load(butil::memory_order_relaxed);
        if (term < 0) {
            return redirect(response);
        }
        butil::IOBuf log;
        butil::IOBufAsZeroCopyOutputStream wrapper(&log);
        if (!request->SerializeToZeroCopyStream(&wrapper)) {
            LOG(ERROR) << "Fail to serialize request";
            response->set_success(false);
            return;
        }
        // Apply this log as a braft::Task
        braft::Task task;
        task.data = &log;
        task.done =
            new ApplyLogClosure(this, request, response, done_guard.release());
        // if (FLAGS_check_term) { todo
        if (false) {
            // ABA problem can be avoid if expected_term is set
            task.expected_term = term;
        }
        // Now the task is applied to the group, waiting for the result.
        return _node->apply(task);
    }
    // braft::StateMachine
    bool is_leader() const {
        return _leader_term.load(butil::memory_order_acquire) > 0;
    }

    void join() {
        if (_node) {
            _node->join();
        }
    }
    void on_leader_start(int64_t term) {
        _leader_term.store(term, butil::memory_order_release);
        LOG(INFO) << "Node becomes leader";
    }
    void on_leader_stop(const butil::Status &status) {
        _leader_term.store(-1, butil::memory_order_release);
        LOG(INFO) << "Node stepped down : " << status;
    }
    void on_apply(braft::Iterator &iter) {
        for (; iter.valid(); iter.next()) {
            std::string log;
            SendRaftLogResponse *response = NULL;
            braft::AsyncClosureGuard done_guard(iter.done());
            butil::IOBuf data = iter.data();
            braft::AsyncClosureGuard closure_guard(iter.done());
            if (iter.done()) {
                // This task is applied by this node, get value from this
                // closure to avoid additional parsing.
                ApplyLogClosure *c =
                    dynamic_cast<ApplyLogClosure *>(iter.done());
                response = c->response();
                log = c->request()->raft_log();
            } else {
                // Have to parse FetchAddRequest from this log.
                butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
                SendRaftLogRequest request;
                CHECK(request.ParseFromZeroCopyStream(&wrapper));
                log = request.raft_log();
            }
            // Parse the log
            LOG(INFO) << "Receive log: " << log << response;
        }
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
    int start(int port,
              int election_timeout_ms,
              int snapshot_interval,
              bool disable_cli,
              const std::string &conf,
              const std::string &data_path,
              const std::string &group) {
        butil::EndPoint addr(butil::my_ip(), port);
        braft::NodeOptions node_options;
        if (node_options.initial_conf.parse_from(conf) != 0) {
            LOG(ERROR) << "Fail to parse configuration `" << conf << '\'';
            return -1;
        }
        node_options.election_timeout_ms = election_timeout_ms;
        node_options.fsm = this;
        node_options.node_owns_fsm = false;
        node_options.snapshot_interval_s = snapshot_interval;
        std::string prefix = "local://" + data_path;
        node_options.log_uri = prefix + "/log";
        node_options.raft_meta_uri = prefix + "/raft_meta";
        node_options.snapshot_uri = prefix + "/snapshot";
        node_options.disable_cli = disable_cli;
        braft::Node *node = new braft::Node(group, braft::PeerId(addr));
        if (node->init(node_options) != 0) {
            LOG(ERROR) << "Fail to init raft node";
            delete node;
            return -1;
        }
        _node = node;
        return 0;
    }

private:
    braft::Node *volatile _node;
    butil::atomic<int64_t> _leader_term;
    char *log_buffer_;  // 用来暂时存储系统运行过程中添加的日志; append
                        // log_record into log_buffer
    char *flush_buffer_;  // 用来暂时存储需要刷新到磁盘中的日志; flush the
                          // logs in flush_buffer into disk file

    std::atomic<bool> enable_flushing_;  // 是否允许刷新
    std::atomic<bool> needFlush_;        // 是否需要刷盘
    std::atomic<lsn_t> next_lsn_;  // 用于分发日志序列号; next lsn in the system
    std::atomic<lsn_t>
        persistent_lsn_;  // 已经刷新到磁盘中的最后一条日志的日志序列号;
                          // the last persistent lsn
    lsn_t flush_lsn_;  // flush_buffer_中最后一条日志的日志记录号; the last
                       // lsn in the flush_buffer

    size_t log_buffer_write_offset_ = 0;    // log_buffer_的偏移量
    size_t flush_buffer_write_offset_ = 0;  // flush_buffer_的偏移量

    std::thread *flush_thread_;  // 日志刷新线程

    std::mutex latch_{};  // 互斥锁，用于log_buffer_的互斥访问

    std::condition_variable cv_;  // 条件变量，用于flush_thread的唤醒; to
                                  // notify the flush_thread

    std::condition_variable operation_cv_;

    LogStorage *log_storage_;

    void redirect(SendRaftLogResponse *response) {
        response->set_success(false);
        if (_node) {
            braft::PeerId leader = _node->leader_id();
            if (!leader.is_empty()) {
                response->set_redirect(leader.to_string());
            }
        }
    }
};

class RaftLogServiceImpl : public RaftLogService {
public:
    explicit RaftLogServiceImpl(LogManager *log_manager)
        : _log_manager(log_manager) {}
    void SendRaftLog(google::protobuf::RpcController *controller,
                     const SendRaftLogRequest *request,
                     SendRaftLogResponse *response,
                     google::protobuf::Closure *done) {
        return _log_manager->SendRaftLog(request, response, done);
    }

private:
    LogManager *_log_manager;
};
