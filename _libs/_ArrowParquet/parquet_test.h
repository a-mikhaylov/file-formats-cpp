#pragma once
//https://arrow.apache.org/docs/cpp/parquet.html
#include <chrono>

#include "include_base_exmpl.h"
#include "example_with_slice.h"
#include "create_write_once.h"
#include "careate_write_iterative.h"

#include "ArrowDataWriter.h"
#include "ArrowDataReader.h"

using namespace std::chrono;

void PrintVec(std::vector<std::vector<int32_t>>& vec) {
    std::cerr << "VECTOR:~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    for (int i = 0; i < vec.size(); ++i) {
        std::cerr << "\t" << i << ": { ";
        for (int j = 0; j < vec[i].size(); ++j) {
            std::cerr << vec[i][j] << ", ";
        }
        std::cerr << "\b\b }" << std::endl;
    }
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
}

void LogWriteResult(std::ofstream& log_out, int step_num, int quant_size,  
                    int parquet_size, int bin_parquet_time, int parquet_bin_time) 
{
    float b_p_time = (float)bin_parquet_time / 1000000.0f; // перевод в секунды
    float p_b_time = (float)parquet_bin_time / 1000000.0f; // перевод в секунды

    log_out << "NODE #" << step_num << ":" << std::endl
            << "\tQuant     = " << quant_size << std::endl
            << "\tFile size = " << parquet_size << std::endl
            << std::endl
            << "\tbin     --> parquet time = " << b_p_time << "c" << std::endl
            << "\tparquet --> bin     time = " << p_b_time << "c" << std::endl << std::endl
            << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
}

//Комментарии к данному преобразованию:
//
//  - Размер выходного файла обратно пропорционален размеру кванта записи (примеры в лог-файле)
//
//  - Время записи обратно пропорционально размеру кванта (примеры в лог-файле)

arrow::Status RunMain() {
    int QUANT = 50000;     //сколько точек читать-писать за раз

    std::vector<int> quant_road = { /* 1000, 10000, 50000,  */100000 };

    const std::string DATA_OUT_DIR      = "ArrowParquet_RESULT";
    const std::string LOG_FILE_NAME     = DATA_OUT_DIR + "/ParquetResults.log";

    std::ofstream log_output(LOG_FILE_NAME, std::ios::app);

    high_resolution_clock::time_point bin_par_start;
    high_resolution_clock::time_point bin_par_stop;

    high_resolution_clock::time_point par_bin_start;
    high_resolution_clock::time_point par_bin_stop;

    std::vector<std::vector<int32_t>> dat = { {}, {}, {}, {}, {}, {}, {}, {} }; //[8 x QUANT]
    for (int i = 0; i < QUANT; ++i) {
        for (int j = 0; j < dat.size(); ++j) 
            dat[j].push_back(0);
    }
    std::vector<std::vector<int32_t>> dat_2 = dat;

    int32_t** arrayPtr = nullptr; //QUANT x 8

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

    for (int i = 0; i < quant_road.size(); ++i) {
        QUANT = quant_road[i];
        const std::string DATA_OUT_NAME     = "/PX1447191017125822-uncomp-" + std::to_string(QUANT) + ".parquet";
        const std::string REWRITE_FULL_NAME = "./BIN_DATA_rewrite/PX1447191017125822-QUANT-" + std::to_string(QUANT) + ".bin";
        
        //Запись из *.bin в *.parquet
        { 
            BinReader BReader{BIN_HDR_PATH, QUANT};
            ArrowDataWriter ADWriter{"", DATA_OUT_DIR, DATA_OUT_NAME,
                                    schema, arrow::Compression::UNCOMPRESSED};
            bin_par_start = high_resolution_clock::now();
            
            while(BReader.Read(dat))
                ADWriter.Write(dat, 2048);

            bin_par_stop = high_resolution_clock::now();
        }

        std::cerr << "[INFO]: " << i << " *.bin --> *.parquet - Complited!" << std::endl;
        //Запись из *.parquet в *.bin (проверка на корректность записи - совпадение хешей)
        {
            ArrowDataReader ADReader{"./" + DATA_OUT_DIR + DATA_OUT_NAME};
            BinWriter       BWriter{REWRITE_FULL_NAME};
            
            par_bin_start = high_resolution_clock::now();

            while(ADReader.Read(dat))
                BWriter.Write(dat);

            par_bin_stop = high_resolution_clock::now();
            //хеш файла ./_data/PX<...>.bin и файла ./BIN_DATA_rewrite/PX<...>-uncomp.bin
            //полностью совпадают, то есть запись и чтение из parquet происходят корректно.
        }
        std::cerr << "[INFO]: " << i << " *.parquet --> *.bin - Complited!" << std::endl;

        LogWriteResult(log_output , i, QUANT, -1, duration_cast<microseconds>(bin_par_stop - bin_par_start).count(),
                        duration_cast<microseconds>(par_bin_stop - par_bin_start).count());
    
    } //for cycle

    return arrow::Status::OK();
}

int _parquetMain() {
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    std::cerr << "succesful ending !!" << std::endl;
    return 0;
}