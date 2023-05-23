#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_ArrowParquet/test.h"
#include "_libs/_DuckDB/duckdb.hpp"
// #include "_libs/_HDF5/hdf5-test.h"

int main() {
    Log test_Log("../Logs/LogEncode.csv"); //debug_set::LOG_FILE

    std::vector<int> Quants = {
          1024,
          1024*8, 
          1024*8*4,
          1024*8*4*4
        };

    std::vector<arrow::Compression::type> Compression = {
        arrow::Compression::type::UNCOMPRESSED,
        arrow::Compression::type::GZIP,
        arrow::Compression::type::ZSTD,
        arrow::Compression::type::SNAPPY        
    };

    std::vector<std::pair<int, int>> Points = {   
        {100000, 100},
        {250000, 1000},
        {0, 1000},
        {1000, 25000},
        {1000, 50000}
    };

    test_ns::Test1_write(
        test_Log, 
        Quants, 
        Compression
    );

    /* test_ns::Test2_read(
        test_Log, 
        {1024*8*4*4}, 
        {arrow::Compression::type::UNCOMPRESSED}
    ); */

test_Log.Flush();
return 0;

    test_ns::Test1_write(
        test_Log, 
        Quants, 
        Compression
    );

    test_ns::Test2_read(
        test_Log, 
        Quants, 
        Compression
    );

    test_ns::Test3_randread(
        test_Log, 
        Quants, 
        Compression,
        Points
    );

    /* test_ns::Test4_shuffle(
        test_Log, 
        Quants, 
        Compression,
        { 
            // 1000,
            1000
            // 10000
        }
    ); */

    test_Log.Flush();

    return 0;
}