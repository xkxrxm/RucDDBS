#include <gflags/gflags.h>

#include "KVServiceImpl.h"
DEFINE_int32(KV_SERVER_PORT, 8002, "KV server address.");
DEFINE_string(DATA_DIR,
              "/home/xkx/ruc_workspace/RucDDBS2/data/",
              "Data directory.");
int main(int argc, char *argv[]) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server server;
    kv::KVServiceImpl service(FLAGS_DATA_DIR);
    if (server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }
    brpc::ServerOptions options;
    if (server.Start(FLAGS_KV_SERVER_PORT, &options) != 0) {
        LOG(ERROR) << "Fail to start KVServer";
        return -1;
    }
    server.RunUntilAskedToQuit();
    return 0;
}