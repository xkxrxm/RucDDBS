#include <brpc/server.h>
#include <butil/logging.h>

#include <string>

#include "kv.pb.h"
#include "storage/KVStore.h"
namespace kv {

class KVServiceImpl final : public KVService {
private:
    KVStore *store_;

public:
    KVServiceImpl(const std::string &dir) : store_(new KVStore(dir)){};
    ~KVServiceImpl() = default;
    void Put(::google::protobuf::RpcController *controller,
             const ::kv::PutRequest *request,
             ::kv::PutResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        store_->put(request->key(), request->value());
        response->set_ok(true);
        LOG(INFO) << "Put key: " << request->key()
                  << " value: " << request->value();
    };
    void Get(::google::protobuf::RpcController *controller,
             const ::kv::GetRequest *request,
             ::kv::GetResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        response->set_value(store_->get(request->key()));
        LOG(INFO) << "Get key: " << request->key()
                  << " value: " << response->value();
    };
    void Del(::google::protobuf::RpcController *controller,
             const ::kv::DelRequest *request,
             ::kv::DelResponse *response,
             ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        response->set_ok(store_->del(request->key()));
        LOG(INFO) << "Del key: " << request->key();
    };
    void Reset(::google::protobuf::RpcController *controller,
               const ::kv::ResetRequest *request,
               ::kv::ResetResponse *response,
               ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        store_->reset();
        response->set_ok(true);
        LOG(INFO) << "Reset";
    };
    void Flush(::google::protobuf::RpcController *controller,
               const ::kv::FlushRequest *request,
               ::kv::FlushResponse *response,
               ::google::protobuf::Closure *done) {
        brpc::ClosureGuard done_guard(done);
        store_->flush();
        response->set_ok(true);
        LOG(INFO) << "Flush";
    };
};
}  // namespace kv
