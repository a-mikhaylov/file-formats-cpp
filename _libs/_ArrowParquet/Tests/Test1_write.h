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

    void LogWriteResult(std::ofstream& log_out, int step_num, int quant_size, int parquet_size, 
                    float bin_parquet_time, float parquet_bin_time, std::string comment = "") 
    {
        // float b_p_time = (float)bin_parquet_time / 1000000.0f; // перевод в секунды
        // float p_b_time = (float)parquet_bin_time / 1000000.0f; // перевод в секунды

        log_out << "NODE #" << step_num << ":" << std::endl
                << "Comment: " << comment << std::endl << std::endl
                << "\tQuant     = " << quant_size << std::endl
                << "\tFile size = " << parquet_size << std::endl
                << std::endl
                << "\tbin     --> parquet time = " << bin_parquet_time << "c" << std::endl
                << "\tparquet --> bin     time = " << parquet_bin_time << "c" << std::endl << std::endl
                << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    }
}

std::string GenerateParquetName(std::string title, int quant, arrow::Compression::type compress) {
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

std::string GenerateBinName(std::string title, int quant, arrow::Compression::type compress) {
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
    
    return "/ReWr-" + title + "-" + std::to_string(quant) + "-" + comp_str + ".bin";
}

void UpdateTime(float& var, high_resolution_clock::time_point& start, high_resolution_clock::time_point& stop) {
    var = var + ((float)duration_cast<microseconds>(stop - start).count() / 1000000.0f);
}

int test_ns::Test1_write(std::vector<int> quants, std::vector<arrow::Compression::type> compressions)
{
    std::string cur_path(boost::filesystem::current_path().c_str());
    std::cerr << "[TEST]: Test_ns::Test1_Write() - STARTED: "
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

    std::ofstream log_output("SmallFile_GZIP-UNCOMP.log", std::ios::app);

    for (std::string file : files) {
        for (int QUANT : quants) {
            for (arrow::Compression::type compr : compressions) { 

                std::cerr << GenerateParquetName(file, QUANT, compr) << std::endl;

                //Запись из *.bin в *.parquet
                    { 
                        BinReader BReader{file, QUANT};
                        ArrowDataWriter ADWriter{"", data_dir, GenerateParquetName("small", QUANT, compr),
                                                schema, compr};
                        
                        while(BReader.Read(dat)) {
                            tmp_start = high_resolution_clock::now();
                                ADWriter.Write(dat, 2048);
                            tmp_stop = high_resolution_clock::now();

                            UpdateTime(bin_par_time, tmp_start, tmp_stop);
                        }
                    }

                    std::cerr << "[INFO]: *.bin --> *.parquet - Complited!" << std::endl;
                    //Запись из *.parquet в *.bin (проверка на корректность записи - совпадение хешей)
                    {
                        ArrowDataReader ADReader{data_dir + GenerateParquetName("small", QUANT, compr)};
                        BinWriter       BWriter{data_dir + GenerateBinName("small", QUANT, compr)};
                        
                        while(ADReader.Read(dat)) {
                            tmp_start = high_resolution_clock::now();
                                BWriter.Write(dat);
                            tmp_stop = high_resolution_clock::now();
                            //нужно засекать чтение!!
                            UpdateTime(par_bin_time, tmp_start, tmp_stop);
                        }
                    }
                    std::cerr << "[INFO]: *.parquet --> *.bin - Complited!" << std::endl << std::endl;

                    debug_set::LogWriteResult(log_output , 0, QUANT, -1, 
                                              bin_par_time, par_bin_time,
                                              GenerateParquetName("small", QUANT, compr));
            }
        }
    }

    return 0;
}

