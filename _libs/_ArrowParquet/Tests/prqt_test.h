#pragma once

// #include "../parquet_test.h"
#include "../../Log/Log.h"

namespace prqt_test {
     const std::string TEST1_DATA_DIR = "/../$Test1_data";
     const std::string TESTLOG_DATA_DIR = "/../$TestLog_data";
     const std::string ENCODE_DATA_DIR = "/../$EncodeDisableDict";

     const std::string RUN_DATA_DIR = ENCODE_DATA_DIR;
    //тест файлов в формате parquet:
    //проверка различных файлов (std::vector<std::string),  //пока нету
    //проверка различных квантов записи (std::vector<int>),
    //проверка различных типов сжатия (std::vector<arrow::Compression::type>),
    //
    
    //Запись из .bin в .parquet
    //Засекается: время записи, время записи одного кванта, 
    //объём получившихся файлов 
    int Test1_write(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions
        );
    
    //Чтение из .parquet
    //Засекается: время чтение всего файла, время чтения одного кванта
    int Test2_read(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions
        );

    //Чтение случайных мест в файле
    //toRead - вектор пар формата {точка начала, количество точек}
    //Засекается: время чтения интервала
    int Test3_randread(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions, 
        std::vector<std::pair<int, int>> toRead
        );

    //Чтение всего файла, но в рандомном порядке
    //shuffle_parts - размер куска, на которые разбиваем файл
    int Test4_shuffle(
        Log& test_Log, 
        std::vector<int> quants, 
        std::vector<arrow::Compression::type> compressions, 
        std::vector<int> shuffle_parts
        );

    //большой тест, включающий прогон каждого из предыдущих
    void ArrowParquetTest();
}