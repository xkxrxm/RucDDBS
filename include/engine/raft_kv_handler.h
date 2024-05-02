#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include "kv.pb.h"
#include "storage/KVStoreAPI.h"
class RaftKVHandler : public KVStoreAPI {
private:
    const std::string GROUP = "KV";
    const std::string CONF =
        "127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,";
    braft::PeerId leader;

public:
    RaftKVHandler() {
        if (braft::rtb::update_configuration(GROUP, CONF) != 0) {
            LOG(ERROR) << "Fail to register configuration " << CONF
                       << " of group " << GROUP;
        }
    };
    ~RaftKVHandler() = default;

    bool put(const std::string &key, const std::string &value);

    std::string get(const std::string &key);

    bool del(const std::string &key);

    void flush();

    void reset();

private:
    void select_leader() {
        while (braft::rtb::select_leader(GROUP, &leader) != 0) {
            LOG(INFO) << "Select_leader failed. Refreshing leader...";
            butil::Status st = braft::rtb::refresh_leader(GROUP, 1000);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to refresh_leader : " << st;
            }
        }
    }
};
