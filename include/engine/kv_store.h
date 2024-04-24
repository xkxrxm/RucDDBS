#pragma once
// 用于做交互
#include <iostream>
#include <memory>
#include <string>

#include "op_etcd.h"
#include "record.h"
#include "storage/KVStore.h"

namespace kv_store {
bool get(string key, shared_ptr<record> &val);
bool del(string key);
bool put(string key, shared_ptr<record> &val);
bool get_par(string tab_name, int par, vector<shared_ptr<record>> &res);
}  // namespace kv_store
extern KVStore store;