#pragma once

// #include "../parquet_test.h"
#include "../../Log/Log.h"

namespace duckdb_test {
    // const std::string RUN_DATA_DIR = "../";

    //тест файлов в формате базы данных через duckdb:
    //проверка различных файлов (std::vector<std::string),
    //проверка различных квантов записи (std::vector<int>),
    
    //Комментрии: 
    //При одновременном тесте чтения и записи, и
    //при раздельном тесте показания разнятся  

    //Запись из .bin в .duckdb
    //Засекается: время записи, время записи одного кванта, 
    //объём получившихся файлов 
    int Test1_write(
        Log& test_Log, 
        std::vector<int> quants,
        std::vector<std::string> files
        );
    
    //Чтение из .duckdb
    //Засекается: время чтение всего файла, время чтения одного кванта
    int Test2_read(
        Log& test_Log, 
        std::vector<int> quants,
        std::vector<std::string> files
        );

    //Чтение случайных мест в файле
    //toRead - вектор пар формата {точка начала, количество точек}
    //Засекается: время чтения интервала
    int Test3_randread(
        Log& test_Log, 
        std::vector<int> quants,
        std::vector<std::pair<int, int>> toRead,
        std::vector<std::string> files
        );

    void DuckDBTest();
}