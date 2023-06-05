#pragma once

#include "../../_hdr-bin/bin.h"
#include "../DuckDBReader.h"
#include "duckdb_test.h"
#include "../../Log/Log.h"
#include "../duckdb_settings.h"

int duckdb_test::Test2_read(Log& test_Log, std::vector<int> quants) {
    std::vector<std::string> files = {settings::SMALL_PATH/* , settings::BIG_PATH */};

    for (std::string file :files) {
        for (int QUANT : quants) {
            std::vector<std::vector<int32_t>> dat(QUANT);

            std::string title = "none";

            if (file == settings::SMALL_PATH)
                title = "small";
            else if (file == settings::BIG_PATH)
                title = "big";

            std::cerr << "[START]: " << duckdb_settings::GenerateBinName(title, QUANT) << std::endl; 

            BinWriter BWriter(duckdb_settings::GenerateBinName(title, QUANT));
            DuckDBReader DBReader(duckdb_settings::GenerateDuckDBName(title, QUANT), QUANT);

            while(DBReader.Read(dat))
                BWriter.Write(dat);
        }    
    }
    
    return 0;
}