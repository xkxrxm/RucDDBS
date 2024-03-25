#pragma once

#include <cstdint>
#include <cassert>
#include <unordered_map>
#include <string>

using table_oid_t = int32_t;
using row_id_t = int32_t;
using partition_id_t = int32_t;

// use for write set and transaction abort
enum class WType { INSERT_TUPLE = 0, DELETE_TUPLE};
class WriteRecord {
   public:
    WriteRecord() = default;
    // for insert record
    WriteRecord(const std::string& new_key, WType type):wtype_(type), key_(new_key){
        assert(type == WType::INSERT_TUPLE);
    }

    WriteRecord(const std::string& delete_key, const std::string& delete_value, WType type)
        :wtype_(type), key_(delete_key), value_(delete_value) {
        assert(type == WType::DELETE_TUPLE);
    }

    // // for update record
    // WriteRecord(char* update_key, int32_t key_size, char* new_value, int32_t value_size,
    //         char* old_value, int32_t old_value_size, WType type){
        
    //     wtype_ = type;
    //     key_size_ = key_size;
    //     value_size_ = value_size;
    //     old_value_size_ = old_value_size;

    //     key_ = new char[key_size];
    //     memcpy(key_, update_key, key_size);
    //     value_ = new char[value_size];
    //     memcpy(value_, new_value, value_size);
    //     old_value_ = new char[old_value_size];
    //     memcpy(old_value_, old_value, old_value_size);
        
    // }
    ~WriteRecord(){}
    WType getWType() const{return wtype_;}
    int32_t getKeySize() const {return key_.size();}
    std::string getKey() const {return key_;}
    int32_t getValueSize() const {return value_.size();}
    std::string getValue() const {return value_;}

   private:
    WType wtype_;
    std::string key_;
    std::string value_;
};

enum class Lock_data_type {TABLE, PARTITION, ROW};
enum class LockMode
{
    SHARED,
    EXCLUSIVE,
    INTENTION_SHARED,
    INTENTION_EXCLUSIVE,
    S_IX
};
