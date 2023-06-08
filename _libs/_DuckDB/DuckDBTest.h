#pragma once

#include "Tests/Test1_write.h"
#include "Tests/Test2_read.h"
#include "Tests/Test3_randread.h"
#include "Tests/duckdb_test.h"

void duckdb_test::DuckDBTest() {
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

    duckdb_test::Test1_write(test_Log, Quants, files);
    
    // duckdb_test::Test2_read(test_Log, Quants, files);

    // duckdb_test::Test3_randread(test_Log, Quants, Points, files);
    
    test_Log.Flush();

    return;
}
