#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_ArrowParquet/test.h"
// #include "_libs/_HDF5/hdf5-test.h"  // пока концентрируемся
// #include "_libs/_DuckDB/duckdb.hpp" // на parquet

int main() {
    // std::string inp_fname = "../_data/big_8x60e6.bin";
    // int fsz = boost::filesystem::file_size(boost::filesystem::path(boost::filesystem::path::string_type(inp_fname)));
    // std::cerr << fsz << std::endl;
            //   << fsz / 8//
    Log test_Log(debug_set::LOG_FILE);

    test_ns::Test1_write(
        test_Log, 
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
    );

    test_ns::Test2_read(
        test_Log, 
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
    );

    test_Log.Flush();

    return 0;
}