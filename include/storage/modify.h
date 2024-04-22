#pragma once
#include <string>
#include <variant>

// Define the Put struct
struct Put {
    std::string key;
    std::string value;
};

// Define the Delete struct
struct Delete {
    std::string key;
};


// Define the Modify class
class Modify {
private:
    // Data member to hold the variant data
    // Here, we use a union to represent the variant data
    // Enumeration to keep track of the type of data stored
    enum class DataType {
        PUT,
        DELETE,
        NONE
    } dataType;

    std::variant<Put,Delete> data;

public:
    // Constructor
    Modify() : dataType(DataType::NONE) {}

    // Copy constructor
    Modify(const Modify& other) : dataType(other.dataType),data(other.data) {}


    Modify(const Put &putData) : dataType(DataType::PUT),data(putData){
    }

    Modify(const Delete &deleteData) : dataType(DataType::DELETE),data(deleteData){
    }    

    std::string Key() const {
        switch (dataType) {
            case DataType::PUT:
                return std::get<Put>(data).key;
            case DataType::DELETE:
                return std::get<Delete>(data).key;
            default:
                return std::string();
        }
    }

    std::string Value() const {
        if (dataType == DataType::PUT) {
            return std::get<Put>(data).value;
        }
        return std::string();
    }
};
