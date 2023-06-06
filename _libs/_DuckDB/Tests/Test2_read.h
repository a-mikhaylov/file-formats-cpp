#pragma once

#include "../../_hdr-bin/bin.h"
#include "../DuckDBReader.h"
#include "duckdb_test.h"
#include "../../Log/Log.h"
#include "../duckdb_settings.h"

int duckdb_test::Test2_read(Log& test_Log, std::vector<int> quants) {
    FileRunInfo info;

    std::vector<std::string> files = {settings::SMALL_PATH/* , settings::BIG_PATH */};

    std::string title;
    std::vector<std::vector<int32_t>> dat;

    high_resolution_clock::time_point tmp_start;
    high_resolution_clock::time_point tmp_stop;
    float bin_db_time = 0;
    float db_bin_time = 0;

//------------------------------------------------------------------------------
    for (std::string file :files) {
        for (int QUANT : quants) {
            dat.resize(QUANT);

            title = "none";

            if (file == settings::SMALL_PATH)
                title = "small";
            else if (file == settings::BIG_PATH)
                title = "big";

            info.setFileID(duckdb_settings::GenerateDuckDBName(title, QUANT));
            info.setQuant(QUANT);

            std::cerr << "[START]: " << duckdb_settings::GenerateBinName(title, QUANT) << std::endl; 

            BinWriter BWriter(duckdb_settings::GenerateBinName(title, QUANT));
            DuckDBReader DBReader(duckdb_settings::GenerateDuckDBName(title, QUANT), QUANT);

            bool run = true;
            int readParts = 0;
            while(true) {
                tmp_start = high_resolution_clock::now();
                    run = DBReader.Read(dat);
                tmp_stop = high_resolution_clock::now();
                
                if (!run)
                    break;
                
                BWriter.Write(dat);
                ++readParts;
                settings::UpdateTime(db_bin_time, tmp_start, tmp_stop);
            }

            std::cerr << "[INFO]: *.parquet --> *.bin - Complited!" << std::endl << std::endl;
 
            info.setReadTime(db_bin_time, db_bin_time / (float)readParts); //!!!

            settings::ResetTime(bin_db_time, db_bin_time);
            readParts = 0;

            test_Log.addInfo(info);
            info.Reset();
        }    
    }
    
    return 0;
}