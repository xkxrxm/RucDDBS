#include <iostream>
#include <string>

#include "storage/KVStore.h"

int main() {
    KVStore store = KVStore("./data");
    std::cout << "Start Putting 65536 kv pairs" << std::endl;
    for (uint64_t i = 0; i <= 65536; i++) {
        store.put(std::to_string(i), "values");
    }
    std::cout << "End Putting 65536 kv pairs" << std::endl;
    std::string result = store.get(std::to_string(1));
    std::cout << result << std::endl;
    return 0;
}