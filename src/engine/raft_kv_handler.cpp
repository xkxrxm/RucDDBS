#include "engine/raft_kv_handler.h"

#include <brpc/channel.h>

#include "kv.pb.h"
bool RaftKVHandler ::put(const std::string &key, const std::string &value) {
    select_leader();
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;
    brpc::Channel channel;

    if (channel.Init(leader.addr, &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return false;
    }
    kv::KVService_Stub stub(&channel);
    brpc::Controller cntl;
    kv::PutRequest request;
    kv::PutResponse response;
    request.set_key(key);
    request.set_value(value);
    stub.Put(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
    }
    return response.ok();
}

std::string RaftKVHandler ::get(const std::string &key) {
    select_leader();
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;
    brpc::Channel channel;

    if (channel.Init(leader.addr, &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return "";
    }
    kv::KVService_Stub stub(&channel);
    brpc::Controller cntl;
    kv::GetRequest request;
    kv::GetResponse response;
    request.set_key(key);
    stub.Get(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
        return "";
    }
    return response.value();
}

bool RaftKVHandler ::del(const std::string &key) {
    select_leader();
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;
    brpc::Channel channel;

    if (channel.Init(leader.addr, &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return false;
    }
    kv::KVService_Stub stub(&channel);
    brpc::Controller cntl;
    kv::DelRequest request;
    kv::DelResponse response;
    request.set_key(key);
    stub.Del(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
    }
    return response.ok();
}

void RaftKVHandler ::flush() {
    select_leader();
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;
    brpc::Channel channel;

    if (channel.Init(leader.addr, &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }
    kv::KVService_Stub stub(&channel);
    brpc::Controller cntl;
    kv::FlushRequest request;
    kv::FlushResponse response;
    stub.Flush(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
    }
}

void RaftKVHandler ::reset() {
    select_leader();
    brpc::ChannelOptions options;
    options.timeout_ms = 10000;
    options.max_retry = 3;
    brpc::Channel channel;

    if (channel.Init(leader.addr, &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }
    kv::KVService_Stub stub(&channel);
    brpc::Controller cntl;
    kv::ResetRequest request;
    kv::ResetResponse response;
    stub.Reset(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        LOG(WARNING) << cntl.ErrorText();
    }
}