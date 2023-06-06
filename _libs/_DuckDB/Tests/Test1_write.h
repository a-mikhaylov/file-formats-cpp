#pragma once

#include "../../_hdr-bin/bin.h"
#include "../DuckDBWriter.h"
#include "duckdb_test.h"
#include "../../Log/Log.h"
#include "../../settings.h"
#include "../duckdb_settings.h"

int duckdb_test::Test1_write(Log& test_Log, std::vector<int> quants) {
    //для записи логов:
    FileRunInfo info;

    std::vector<std::string> files = {settings::SMALL_PATH/* , settings::BIG_PATH */};

    std::vector<std::vector<int32_t>> dat;
    std::string title;

    high_resolution_clock::time_point tmp_start;
    high_resolution_clock::time_point tmp_stop;
    float bin_db_time = 0;
    float db_bin_time = 0;
//--------------------------------------------------------------------------
    for (std::string file :files) {
        for (int QUANT : quants) {
            dat.resize(QUANT);
            title = "none";

            if (file == settings::SMALL_PATH)
                title = "small";
            else if (file == settings::BIG_PATH)
                title = "big";
            
            info.setFileID(
                duckdb_settings::GenerateDuckDBName(title, QUANT)
                );
            info.setQuant(QUANT);

            std::cerr << "[START]: " 
                      << duckdb_settings::GenerateDuckDBName(title, QUANT) 
                      << std::endl;

            BinReader BReader(file, QUANT);
            DuckDBWriter DBWriter(
                { "LR", "FR", "C1R", "C2L", "C3F", "C4R", "C5L", "C6F"}, 
                duckdb_settings::GenerateDuckDBName(title, QUANT)
                );
            
            info.setInfo(BReader.getChannelsCount(), BReader.getPointsCount());           

            while (BReader.Read(dat)) {
                tmp_start = high_resolution_clock::now();
                    DBWriter.Write(dat);
                tmp_stop = high_resolution_clock::now();
                
                settings::UpdateTime(bin_db_time, tmp_start, tmp_stop);
            }
            std::cerr << "[INFO]: *.bin --> *.duckdb - Complited!" << std::endl;

            info.setFilesSizes(BReader.getFileName(), DBWriter.getFileName());
            info.setWriteTime(
                bin_db_time, 
                bin_db_time / (BReader.getPointsCount() / (float)QUANT)
                );

            settings::ResetTime(bin_db_time, db_bin_time); 

            test_Log.addInfo(info);
            info.Reset();
        }
    }
    
    return 0;
}