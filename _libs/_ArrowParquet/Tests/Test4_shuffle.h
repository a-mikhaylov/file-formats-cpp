#pragma once

#include "../../_hdr-bin/bin.h"
#include "../ArrowDataReader.h"
#include "../ArrowDataWriter.h"
#include "prqt_test.h"
#include "../prqt_settings.h"
#include "../../settings.h"
#include <sys/stat.h>
#include <sys/types.h>

int prqt_test::Test4_shuffle(
    Log& test_Log, 
    std::vector<int> quants, 
    std::vector<arrow::Compression::type> compressions, 
    std::vector<int> shuffle_parts,
    std::vector<std::string> files
    )
{
    //для записи логов:
    FileRunInfo info;

    std::string cur_path(boost::filesystem::current_path().c_str());
    std::cerr << "[TEST]: prqt_test::Test1_write() - STARTED: "
              << cur_path << std::endl << std::endl;
    
    const std::string data_dir    = cur_path + prqt_test::RUN_DATA_DIR; //директория для вывода
    const std::string big_fname   = cur_path + settings::BIG_FILE;   //текущее расположение
    const std::string small_fname = cur_path + settings::SMALL_FILE; //бинарных исходников
    
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

    std::string file_title;
    std::string tmp_out_fname;
    std::string tmp_in_fname;
    int writeParts = 0;  //количество кусков при записи
    int readParts  = 0;
//----------------------------------------------------------------------------------
    for (std::string file : files) {
        for (int QUANT : quants) {
            for (arrow::Compression::type compr : compressions) { 
                for (size_t shuf_part : shuffle_parts) {
                
                    if      (file == small_fname)
                        file_title = "small";
                    else if (file == big_fname)
                        file_title = "big";

                    info.setFileID(prqt_settings::GenerateParquetName(file_title, QUANT, compr));
                    info.setRunSetting(compr, QUANT);

                    std::cerr << prqt_settings::GenerateParquetName(file, QUANT, compr) << std::endl;

                    {
                        ArrowDataReader ADReader{
                            data_dir + prqt_settings::GenerateParquetName(file_title, QUANT, compr)
                            };
                        ADReader.Shuffle(shuf_part);
                        bool need_go = true;
                        while(true) {
                            tmp_start = high_resolution_clock::now();
                                need_go = ADReader.Read(dat);
                            tmp_stop = high_resolution_clock::now();
                            settings::UpdateTime(par_bin_time, tmp_start, tmp_stop);
                            
                            if (!need_go)
                                break;
                            ++readParts;
                        }
                    }
                    std::cerr << "[INFO]: *.parquet --> *.bin - Complited!" 
                              << std::endl << std::endl;
    
                    info.setReadTime(par_bin_time, par_bin_time / (float)readParts);
                    settings::ResetTime(bin_par_time, par_bin_time);

                    test_Log.addInfo(info);
                    info.Reset();
                }
            }
        }
    }
}
