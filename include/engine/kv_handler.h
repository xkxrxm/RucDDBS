#include "storage/KVStoreAPI.h"
class KVHandler : public KVStoreAPI {
private:
    const char *server_addr_ = "127.0.0.1";
    const int port_ = 8002;

public:
    KVHandler(){};
    ~KVHandler() = default;

    bool put(const std::string &key, const std::string &value);

    std::string get(const std::string &key);

    bool del(const std::string &key);

    void flush();

    void reset();
};
