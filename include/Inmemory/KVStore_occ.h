#pragma once

#include "transaction.h"
#include "KVStore_beta.h"
#include "modify.h"
#include <string>
#include <vector>

class KVStorage {
public:
    explicit KVStorage(const std::string &dir) { 
        kv_ = new KVStore_beta(dir);
    };
    
    ~KVStorage(){ kv_->flush(); };
    std::string GetCF(std::string cf, std::string key){
        return kv_->get(cf + key).second;
    }
    void Write(std::vector<Modify*> batch){
      // for (auto &m : batch){
      //   if (m->Value() == ""){
      //     kv_->del(m->Cf() + m->Key());
      //   } else {
      //     kv_->put(m->Cf() + m->Key(), m->Value());
      //   }
      // }
    }
  private:
    KVStore_beta* kv_;
};