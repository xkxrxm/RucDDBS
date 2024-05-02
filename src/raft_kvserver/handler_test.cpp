#include <braft/raft.h>
#include <braft/route_table.h>
#include <braft/util.h>
#include <brpc/channel.h>
#include <brpc/controller.h>
#include <gflags/gflags.h>

#include "kv.pb.h"

DEFINE_string(conf,
              "127.0.1.1:8100:0,127.0.1.1:8101:0,127.0.1.1:8102:0,",
              "Configuration of the raft group");
DEFINE_string(group, "KV", "Id of the replication group");

int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    LOG(INFO) << "FLAGS_group: " << FLAGS_group
              << " FLAGS_conf: " << FLAGS_conf;
    if (braft::rtb::update_configuration(FLAGS_group, FLAGS_conf) != 0) {
        LOG(ERROR) << "Fail to register configuration " << FLAGS_conf
                   << " of group " << FLAGS_group;
        return -1;
    }

    braft::PeerId leader;
    while (braft::rtb::select_leader(FLAGS_group, &leader) != 0) {
        LOG(INFO) << "Select_leader failed. Refreshing leader...";
        butil::Status st = braft::rtb::refresh_leader(FLAGS_group, 1000);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to refresh_leader : " << st;
        }
    }
    LOG(INFO) << "leader.addr: " << leader.addr;
    return 0;
}