#pragma once

#include "txn.h"
#include "kvstore_occ.h"
#include <vector>

class foccTxn
{
private:
  KVStorage *storage;
  std::vector<Modify *> writes;
public:
  static const std::string cf_value = "value";
  static const std::string cf_lock = "lock";
public:
  foccTxn(KVStorage *storage): storage(storage) {};
  ~foccTxn(){};
  inline std::vector<Modify *> Writes() { return writes; };

  void PutValue(const std::string &key, const std::string &value){
    writes.push_back(new Put(cf_value, key, value));
  }

  void DelValue(const std::string &key){
    writes.push_back(new Delete(cf_value, key));
  }

  void PutLock(const std::string &key,uint64_t txn_id){
    writes.push_back(new Lock(cf_lock, key, std::to_string(txn_id)));
  }

  void DelLock(const std::string &key){
    writes.push_back(new Delete(cf_lock, key));
  }

  bool GetVal(const std::string &key, std::string &value, uint64_t txn_id){
    lock_id = GetLock(key);
    if(lock_id != txn_id){
      return false;
    }
    value = storage->GetCF(cf_value, key);
    return true;
  }

  uint64_t GetLock(const std::string &key){
    return std::stoull(storage->GetCF(cf_lock, key));
  }

};

foccTxn::foccTxn()
{
}

foccTxn::~foccTxn()
{
}
