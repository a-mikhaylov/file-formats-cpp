#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_ArrowParquet/test.h"
// #include "_libs/_HDF5/hdf5-test.h"  // пока концентрируемся
// #include "_libs/_DuckDB/duckdb.hpp" // на parquet

int main() {
    /*std::vector<int> a = {0, 1, 2, 3};
    std::vector<int> b = {4, 5, 6, 7};
    debug_set::PrintVector("a:", a);
    debug_set::PrintVector("b:", b);

    a.insert(a.end(), b.begin(), b.end());
    debug_set::PrintVector("a:", a);

    return 0; */

    return test_ns::Test3_randread(
        {
           /*  1000,
            10000, 
            50000,  */
            100000
        }, 
        {
            arrow::Compression::type::UNCOMPRESSED
            /* arrow::Compression::type::GZIP,
            arrow::Compression::type::ZSTD,
            arrow::Compression::type::SNAPPY */
        },
        {
            {0,     5}
            /* {10000, 15000},
            {15000, 16000} */
        }
    );
    /* return test_ns::Test2_read(
        {
            1000,
            10000, 
            50000, 
            100000
        }, 
        {
            arrow::Compression::type::UNCOMPRESSED, 
            arrow::Compression::type::GZIP,
            arrow::Compression::type::ZSTD,
            arrow::Compression::type::SNAPPY
        }
    ); */
}