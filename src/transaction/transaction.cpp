#include "transaction.h"

#include <butil/logging.h>

#include "engine/op_etcd.h"
const std::string LOCK = "lock";

bool Transaction::get(std::string key, shared_ptr<record> &val) {
    if (!check_lock(key)) {
        return false;
    }
    auto ret = kv_->get(key);
    read_set_->keys.push_back(key);
    if (ret != "")
        val->string_to_rec(ret);
    else
        return false;
    LOG(DEBUG) << "get : <" << key << ": " << ret << " >";
    return true;
}
void Transaction::del(std::string key) {
    LOG(DEBUG) << "del : " << key;
    // auto ret = etcd_del(key);
    // return ret;
    Modify *m = new Modify(Delete{key});
    write_set_->keys.push_back(key);
    writes_->push_back(*m);
}
void Transaction::commit() {
    LOG(DEBUG) << "commit";
    for (auto &m : *writes_) {
        if (m.Value() != "") {
            kv_->put(m.Key(), m.Value());
        } else {
            kv_->del(m.Key());
        }
    }
}
void Transaction::put(std::string key, std::shared_ptr<record> &val) {
    std::string str = val->rec_to_string();
    LOG(DEBUG) << "put : <" << key << ", " << str << ">";
    write_set_->keys.push_back(key);
    Modify *m = new Modify(Put{key, str});
    writes_->push_back(*m);
    return;
}
bool Transaction::get_par(std::string tab_name,
                          int par,
                          vector<std::shared_ptr<record>> &res) {
    LOG(DEBUG) << "get_par " << tab_name << "," << par;
    std::string key = "/store_data/" + tab_name + "/" + to_string(par) + "/";
    auto ret = etcd_par(key);
    for (auto iter : ret) {
        std::shared_ptr<record> rec(new record);
        rec->string_to_rec(iter.second);
        res.push_back(rec);
    }
    return 1;
}

bool Transaction::check_lock(string key) {
    auto ret = kv_->get(LOCK + key);
    LOG(DEBUG) << "check_lock :[" << key << " locked by txn " << ret << "]";
    if (ret == "") {
        return true;
    } else if (to_string(txn_id_) == ret) {
        return true;
    } else {
        return false;
    }
}
bool Transaction::try_lock(string key) {
    LOG(DEBUG) << "try_lock : " << key;
    std::string txn_id = kv_->get(LOCK + key);
    if (txn_id == "") {
        kv_->put(LOCK + key, to_string(txn_id_));
        return true;
    } else if (to_string(txn_id_) == txn_id) {
        return true;
    } else {
        LOG(DEBUG) << "try_lock failed : " << key;
        return false;
    }
}
bool Transaction::release_lock(string key) {
    LOG(DEBUG) << "release_lock : " << key;
    return kv_->del(LOCK + key);
}