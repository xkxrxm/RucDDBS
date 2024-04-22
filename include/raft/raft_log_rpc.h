#include <brpc/server.h>
#include <butil/logging.h>
#include <gflags/gflags.h>

#include "raft_log.pb.h"
#include "raft_log_record.h"
#include "transaction_manager.h"

namespace raft_log {
class RaftLogImpl : public Raft_Service {
public:
    RaftLogImpl(RaftLogManager *raftlog_manager)
        : raftlog_manager_(raftlog_manager){};
    RaftLogImpl(){};
    virtual ~RaftLogImpl(){};

    virtual void SendRaftLog(::google::protobuf::RpcController *controller,
                             const ::raft_log::SendRaftLogRequest *request,
                             ::raft_log::SendRaftLogResponse *response,
                             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        LOG raftlog_manager_->RedoRaftLog(request->raft_log().c_str());
        return;
    }

private:
    RaftLogManager *raftlog_manager_;
};
}  // namespace raft_log