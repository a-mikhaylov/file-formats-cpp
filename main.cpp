#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_ArrowParquet/test.h"
// #include "_libs/_HDF5/hdf5-test.h"  // пока концентрируемся
// #include "_libs/_DuckDB/duckdb.hpp" // на parquet

int main() {

    return test_ns::Test3_randread(
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
        },
        {
            {0, 10000},
            {10000, 55000},
            {80000, 110000},  
            {170000, 210000}
        }   
    );
}