#pragma once

#include "Tests/Test1_write.h"
#include "Tests/Test2_read.h"
#include "Tests/Test3_randread.h"
#include "Tests/Test4_shuffle.h"
#include "Tests/prqt_test.h"

void prqt_test::ArrowParquetTest() {
    Log test_Log("../Logs/LogEncode.csv"); //debug_set::LOG_FILE

    std::vector<std::string> files = {
        /* small_fname,  */
        settings::SMALL_PATH
        };

    std::vector<int> Quants = {
          1024,      //2^10
          1024*8,    //2^13
          1024*8*4,  //2^15
          1024*8*4*4 //2^17
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


//---------------------------------------------
    prqt_test::Test1_write(
        test_Log, 
        Quants, 
        Compression,
        files
    );

    prqt_test::Test2_read(
        test_Log, 
        Quants, 
        Compression,
        files
    );

    prqt_test::Test3_randread(
        test_Log, 
        Quants, 
        Compression,
        Points,
        files
    );

    /* prqt_test::Test4_shuffle(
        test_Log, 
        Quants, 
        Compression,
        { 
            // 1000,
            1000
            // 10000
        },
        files
    ); */

    test_Log.Flush();

}
