#pragma once

#include "../../_hdr-bin/bin.h"
#include "../ArrowDataReader.h"
#include "../ArrowDataWriter.h"
#include "test_ns.h"
#include "../../Log/Log.h"
#include "../debug_func.h"
#include <sys/stat.h>
#include <sys/types.h>

int test_ns::Test1_write(std::vector<int> quants, std::vector<arrow::Compression::type> compressions)
{
    std::string cur_path(boost::filesystem::current_path().c_str());
    std::cerr << "[TEST]: test_ns::Test1_write() - STARTED: "
              << cur_path << std::endl << std::endl;
    
    const std::string data_dir    = cur_path + test_ns::TEST1_DATA_DIR; //директория для вывода
    const std::string big_fname   = cur_path + debug_set::BIG_FILE;   //текущее расположение
    const std::string small_fname = cur_path + debug_set::SMALL_FILE; //бинарных исходников
    
    //в перспективе - будет подаваться на вход
    const std::vector<std::string> files = {small_fname/* , big_fname */};
    
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
    std::vector<std::vector<int32_t>> dat(8); //[8 x QUANT]

    high_resolution_clock::time_point tmp_start;
    high_resolution_clock::time_point tmp_stop;

    float bin_par_time = 0;
    float par_bin_time = 0;

    std::ofstream log_output("../Logs/Test1_write.log", std::ios::app);

    std::string file_title;
    int which_file = -1; //определять, имя какого файл сейчас писать
    int writeParts = 0;  //количество кусков при записи

    for (std::string file : files) {
        ++which_file;
        for (int QUANT : quants) {
            for (arrow::Compression::type compr : compressions) { 
                
                if      (which_file == 0)
                    file_title = "small";
                else if (which_file == 1)
                    file_title = "big";

                std::cerr << GenerateParquetName(file, QUANT, compr) << std::endl;

                //Запись из *.bin в *.parquet
                { 
                    BinReader BReader{file, QUANT};
                    ArrowDataWriter ADWriter{"", data_dir, GenerateParquetName(file_title, QUANT, compr),
                                            schema, compr};
                    
                    while(BReader.Read(dat)) {
                        tmp_start = high_resolution_clock::now();
                            ADWriter.Write(dat);
                        tmp_stop = high_resolution_clock::now();
                        ++writeParts;
                        UpdateTime(bin_par_time, tmp_start, tmp_stop);
                    }
                }

                std::cerr << "[INFO]: *.bin --> *.parquet - Complited!" << std::endl;
                
                debug_set::LogWriteResultWrite(log_output , 0, QUANT, -1, 
                                            bin_par_time, writeParts,
                                            GenerateParquetName(file_title, QUANT, compr));

                ResetTime(bin_par_time, par_bin_time); 
                writeParts = 0;                        
            }
        }
    }

    return 0;
}

