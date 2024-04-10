#include "transaction.h"

#include <butil/logging.h>

#include "engine/op_etcd.h"
const std::string LOCK = "lock";

bool Transaction::get(std::string key, shared_ptr<record> &val)
{
    auto ret = etcd_get(key);
    if (ret != "")
        val->string_to_rec(ret);
    else
        return 0;
    LOG(DEBUG) << "get : " << key << " " << ret;
    return 1;
}
bool Transaction::del(std::string key)
{
    LOG(DEBUG) << "del : " << key;
    auto ret = etcd_del(key);
    return ret;
}
bool Transaction::put(std::string key, std::shared_ptr<record> &val)
{
    std::string str = val->rec_to_string();
    LOG(DEBUG) << "put : <" << key << ", " << str << ">";
    return etcd_put(key, str);
}
bool Transaction::get_par(std::string tab_name,
                          int par,
                          vector<std::shared_ptr<record>> &res)
{
    LOG(DEBUG) << "get_par " << tab_name << "," << par;
    std::string key = "/store_data/" + tab_name + "/" + to_string(par) + "/";
    auto ret = etcd_par(key);
    for (auto iter : ret)
    {
        std::shared_ptr<record> rec(new record);
        rec->string_to_rec(iter.second);
        res.push_back(rec);
    }
    return 1;
}

txn_id_t Transaction::check_lock(string key)
{
    LOG(DEBUG) << "check_lock : " << key;
    return std::stoull(etcd_get(LOCK + key));
}
bool Transaction::try_lock(string key)
{
    LOG(DEBUG) << "try_lock : " << key;
    std::string txn_id = etcd_get(LOCK + key);
    if (txn_id == "")
    {
        return etcd_put(LOCK + key, to_string(txn_id_));
    }
    else if (to_string(txn_id_) == txn_id)
    {
        return true;
    }
    else
    {
        LOG(DEBUG) << "try_lock failed : " << key;
        return false;
    }
}
bool Transaction::release_lock(string key)
{
    LOG(DEBUG) << "release_lock : " << key;
    return etcd_del(LOCK + key);
}