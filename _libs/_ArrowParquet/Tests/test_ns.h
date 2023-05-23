#pragma once

#include "../parquet_test.h"
#include "../../Log/Log.h"

namespace test_ns {
     const std::string TEST1_DATA_DIR = "/../$Test1_data";
     const std::string TESTLOG_DATA_DIR = "/../$TestLog_data";
     const std::string ENCODE_DATA_DIR = "/../$EncodeDELTA_LR_BIN_PACK_data";

     const std::string RUN_DATA_DIR = ENCODE_DATA_DIR;
    //тест записи файлов в формат parquet:
    //проверка различных файлов (std::vector<std::string),
    //проверка различных квантов записи (std::vector<int>),
    //проверка различных типов сжатия (std::vector<arrow::Compression::type>),
    //
    //Сохраняем в лог:
    //Объём файла + время (только!) записи + кол-во блоков записи
    int Test1_write(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions
        );
    
    int Test2_read(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions
        );

    int Test3_randread(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions, 
        std::vector<std::pair<int, int>> toRead
        );

    int Test4_shuffle(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions, 
        std::vector<int> shuffle_parts
        );
}