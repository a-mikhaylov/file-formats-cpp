#pragma once

#include "../../_hdr-bin/bin.h"
#include "../ArrowDataReader.h"
#include "../ArrowDataWriter.h"
#include "test_ns.h"
#include <sys/stat.h>
#include <sys/types.h>

namespace debug_set {
    const std::string BIG_FILE   = "/_data/big_8x60e6";
    const std::string SMALL_FILE = "/_data/small_8x1e6";
}

std::string GetFileName(std::string title, int quant, arrow::Compression::type compress) {
    std::string comp_str = "NONE";

    switch(compress) {
        case arrow::Compression::type::UNCOMPRESSED:
            comp_str = "UNCOMP";
            break;
        case arrow::Compression::type::GZIP:
            comp_str = "GZIP";
            break;
        case arrow::Compression::type::ZSTD:
            comp_str = "ZSTD";
            break;
        case arrow::Compression::type::SNAPPY:
            comp_str = "SNAPPY";
            break;
    }
    
    return "/" + title + "-" + std::to_string(quant) + "-" + comp_str + ".parquet";
}

int test_ns::Test1_write(std::vector<int> quants, std::vector<arrow::Compression::type> compressions)
{
    std::string cur_path(boost::filesystem::current_path().c_str());
    std::cerr << "[TEST]: Test_ns::Test1_Write() - STARTED: "
              << cur_path << std::endl << std::endl;
    
    const std::string data_dir    = cur_path + test_ns::TEST1_DATA_DIR; //директория с выходными файлами
    const std::string big_fname   = cur_path + debug_set::BIG_FILE;   //текущее расположение
    const std::string small_fname = cur_path + debug_set::SMALL_FILE; //бинарных исходников
    //в перспективе - будет подаваться на вход
    const std::vector<std::string> files = {small_fname, big_fname};
    //если это первый запуск - создаем нужную директорию
    mkdir((data_dir).c_str(), 0700);



    return 0;
}

