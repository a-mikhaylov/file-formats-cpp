#include <iostream>
#include <h5cpp/hdf5.hpp>
#include "libs/hdf5-test.h"

int main() {
    auto type = hdf5::datatype::TypeTrait<int>::create();
    std::cout<<type.get_class()<<std::endl;
}