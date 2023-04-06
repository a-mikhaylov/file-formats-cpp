#pragma once

#include "../../_hdr-bin/bin.h"
#include "../ArrowDataReader.h"
#include "../ArrowDataWriter.h"
#include "test_ns.h"
#include "../debug_func.h"
#include <sys/stat.h>
#include <sys/types.h>

int test_ns::Test3_randread(std::vector<int> quants, 
                            std::vector<arrow::Compression::type> compressions,
                            std::vector<std::pair<int, int>> toRead /* { (x0; len), ... } */)
{
    std::string cur_path(boost::filesystem::current_path().c_str());
    std::cerr << "[TEST]: Test_ns::Test3_randread() - STARTED: "
              << cur_path << std::endl << std::endl;
    
    const std::string data_dir    = cur_path + test_ns::TEST1_DATA_DIR; //директория для вывода
    const std::string big_fname   = cur_path + debug_set::BIG_FILE;   //текущее расположение
    const std::string small_fname = cur_path + debug_set::SMALL_FILE; //бинарных исходников
    
    //в перспективе - будет подаваться на вход
    const std::vector<std::string> files = {/* small_fname,  */big_fname};
    
    //если это первый запуск - создаем нужную директорию
    mkdir((data_dir).c_str(), 0700);
    //схема записи столбцов в итоговый файл (может быть изменена)
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {
            arrow::field("LR",  arrow::int32()),
            arrow::field("FR",  arrow::int32()),
            arrow::field("C1R", arrow::int32()),
            arrow::field("C2L", arrow::int32()),
            arrow::field("C3F", arrow::int32()),
            arrow::field("C4R", arrow::int32()),
            arrow::field("C5L", arrow::int32()),
            arrow::field("C6F", arrow::int32()),
        });
    std::vector<std::vector<int32_t>> dat; //[8 x QUANT]

    high_resolution_clock::time_point tmp_start;
    high_resolution_clock::time_point tmp_stop;

    // float bin_par_time = 0;
    // float par_bin_time = 0;
    std::vector<float> part_time(toRead.size());

    std::ofstream log_output("../Logs/Test3_randread-big.log", std::ios::app);

    std::string file_title;
    int which_file = -1; //определять, имя какого файл сейчас писать
    int readParts = 0;

    for (std::string file : files) {
        ++which_file;
        for (int QUANT : quants) {
            for (arrow::Compression::type compr : compressions) {
                
                if      (which_file == 0)
                    file_title = "small";
                else if (which_file == 1)
                    file_title = "big";

                std::cerr << data_dir + GenerateParquetName(file_title, QUANT, compr) << std::endl;

                {
                   ArrowDataReader ADReader{data_dir + GenerateParquetName(file_title, QUANT, compr)};
                    // BinWriter       BWriter{data_dir + GenerateBinName(file_title, QUANT, compr)};
                    bool need_go = true;
                    int i = 0;
                    for (std::pair<int, int> part : toRead) {
                        tmp_start = high_resolution_clock::now();
                           need_go = ADReader.Read(dat, part);
                        tmp_stop = high_resolution_clock::now();
                        UpdateTime(part_time[i++], tmp_start, tmp_stop);
                        
                        if (!need_go)
                            break;
                        // debug_set::PrintVector("my dat: " + std::to_string(part.first) + " - "  + 
                                                            // std::to_string(part.first + part.second), dat);

                        ++readParts;
                    }
                }
                std::cerr << "[INFO]: *.parquet --> *.bin - Complited!" << std::endl << std::endl;
 
                debug_set::LogWriteResultRandRead(log_output, QUANT, -1, 
                                                  part_time, toRead,
                                                  GenerateParquetName(file_title, QUANT, compr));

                ResetTime(part_time);
                // i = 0;
                // readParts = 0;
            }
        }
    }

    return 0;
}