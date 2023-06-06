#include <iostream>
//work with bin and hdr files 
#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"
//Arrow Parquet
#include "_libs/_ArrowParquet/Examples/parquet_test.h"
#include "_libs/_ArrowParquet/ArrowTest.h"
//DuckDB
#include "_libs/_DuckDB/Examples/example.h"
#include "_libs/_DuckDB/DuckDBTest.h"
//HDF5
// #include "_libs/_HDF5/hdf5-test.h"   //в последнюю очередь

int main() {
    Log test_Log("../Logs/LogDuckDB5-rand.csv"); //debug_set::LOG_FILE

    std::vector<std::string> files = {
            settings::SMALL_PATH
            // settings::BIG_PATH
        };

    std::vector<int> Quants = {
          1024,       //2^10
          1024*8,     //2^13
          1024*8*4,   //2^15
          1024*8*4*4, //2^17
          1024*8*8*8, //2^21 (524288)
          1000000     //1e6
        };

    std::vector<std::pair<int, int>> Points = {   
        {100000, 100},
        {250000, 1000},
        {0, 1000},
        {1000, 25000},
        {1000, 50000}
    };

    // duckdb_test::Test1_write(test_Log, Quants, files);
    
    // duckdb_test::Test2_read(test_Log, Quants, files);

    duckdb_test::Test3_randread(test_Log, Quants, Points, files);
    
    test_Log.Flush();

    return 0;
}