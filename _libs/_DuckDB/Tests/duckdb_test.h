#pragma once

// #include "../parquet_test.h"
#include "../../Log/Log.h"

namespace duckdb_test {
     const std::string TEST1_DATA_DIR = "/../$Test1_data";
     const std::string TESTLOG_DATA_DIR = "/../$TestLog_data";
     const std::string ENCODE_DATA_DIR = "/../$EncodeDisableDict";

     const std::string RUN_DATA_DIR = ENCODE_DATA_DIR;
    //тест файлов в формате базы данных через duckdb:
    //проверка различных файлов (std::vector<std::string),  //пока нету
    //проверка различных квантов записи (std::vector<int>),
    
    //Запись из .bin в .duckdb
    //Засекается: время записи, время записи одного кванта, 
    //объём получившихся файлов 
    int Test1_write(
        Log& test_Log, 
        std::vector<int> quants
        );
    
    //Чтение из .duckdb
    //Засекается: время чтение всего файла, время чтения одного кванта
    int Test2_read(
        Log& test_Log, 
        std::vector<int> quants
        );
}