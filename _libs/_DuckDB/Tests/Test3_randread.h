#pragma once

#include "../../_hdr-bin/bin.h"
#include "../DuckDBWriter.h"
#include "duckdb_test.h"
#include "../../Log/Log.h"
#include "../../settings.h"
#include "../duckdb_settings.h"

int duckdb_test::Test3_randread(
    Log& test_Log, 
    std::vector<int> quants, 
    std::vector<std::pair<int, int>> toRead,
    std::vector<std::string> files
    ) 
{
    FileRunInfo info;

    std::string title;
    std::vector<std::vector<int32_t>> dat;

    high_resolution_clock::time_point tmp_start;
    high_resolution_clock::time_point tmp_stop;
    std::vector<float> part_time(toRead.size());

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
            
            DuckDBReader DBReader(duckdb_settings::GenerateDuckDBName(title, QUANT), QUANT);

            bool run = true;
            int i = 0;

            for (std::pair<int, int> part : toRead) {
                tmp_start = high_resolution_clock::now();
                    run = DBReader.Read(dat, part);
                tmp_stop = high_resolution_clock::now();

                settings::UpdateTime(part_time[i++], tmp_start, tmp_stop);
                        
                if (!run)
                    break;
                
                info.setIntervalReading(part.second, part_time[i - 1]);
                test_Log.addInfo(info);
            }

            std::cerr << "[INFO]: *.parquet --> *.bin - Complited!" << std::endl << std::endl;

            settings::ResetTime(part_time);

            // test_Log.addInfo(info);
            info.Reset();
        }    
    }
    
    return 0;
}