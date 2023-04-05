#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_ArrowParquet/test.h"
// #include "_libs/_HDF5/hdf5-test.h"  // пока концентрируемся
// #include "_libs/_DuckDB/duckdb.hpp" // на parquet

int main() {
    /* std::vector<int> a = {0, 1, 2, 3};
    std::vector<int> b = {4, 5, 6, 7};
    std::vector<int> c = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    debug_set::PrintVector("a:", a);
    debug_set::PrintVector("b:", b);
    a.insert(a.end(), b.begin(), b.end());
    debug_set::PrintVector("a:", a);

    std::cerr << std::endl << std::endl;
    debug_set::PrintVector("c:", c);
    c.erase(c.begin() + 2 , c.begin() + 6);
    debug_set::PrintVector("c cleared:", c);
    return 0; */

    return test_ns::Test3_randread(
        {
            10
            /* 1000,
            10000, 
            50000,
            100000 */
        }, 
        {
            arrow::Compression::type::UNCOMPRESSED
            /* arrow::Compression::type::GZIP,
            arrow::Compression::type::ZSTD,
            arrow::Compression::type::SNAPPY */
        },
        {
            {0,     40},
            {8, 15}/* ,
            {100000, 130000} */
        }
    );
}