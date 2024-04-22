#include "modify.h"
#include <bits/stdc++.h>
std::vector<Modify> writes_;
void test(){
    Modify m;
    m.SetData(Put{"key", "value"});
    writes_.push_back(m);
    std::cout << m.Key() << std::endl;
}
int main(){
    test();
    return 0;
}