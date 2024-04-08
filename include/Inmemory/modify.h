#pragma once
#include <vector>
#include <string>

class Modify {
public:
    virtual ~Modify() {}
    virtual std::string Key() const = 0;
    virtual std::string Value() const = 0;
    virtual const std::string& Cf() const = 0;
};

class Put : public Modify {
public:
    std::string key;
    std::string value;
    std::string cf;

    std::string Key() const override {
        return key.data();
    }

    std::string Value() const override {
        return value.data();
    }

    const std::string& Cf() const override {
        return cf;
    }

    Put(const std::string& cf, const std::string& key, const std::string& value) : 
        key(key), value(value), cf(cf) {}
};

class Delete : public Modify {
public:
    std::string key;
    std::string cf;

    Delete(const std::string& cf, const std::string& key) : 
        key(key), cf(cf) {}

    std::string Key() const override {
        return key.data();
    }

    std::string Value() const override {
        return nullptr;
    }

    const std::string& Cf() const override {
        return cf;
    }
};