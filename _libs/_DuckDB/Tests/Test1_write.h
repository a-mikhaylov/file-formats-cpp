#pragma once

#include "../../_hdr-bin/bin.h"
#include "../DuckDBWriter.h"
#include "duckdb_test.h"
#include "../../Log/Log.h"
#include "../../settings.h"
#include "../duckdb_settings.h"

int duckdb_test::Test1_write(Log& test_Log, std::vector<int> quants) {
    
    std::vector<std::string> files = {settings::SMALL_PATH/* , settings::BIG_PATH */};

    for (std::string file :files) {
        for (int QUANT : quants) {
            std::vector<std::vector<int32_t>> dat(QUANT);

            std::string title = "none";

            if (file == settings::SMALL_PATH)
                title = "small";
            else if (file == settings::BIG_PATH)
                title = "big";

            std::cerr << "[START]: " << duckdb_settings::GenerateDuckDBName(title, QUANT);

            BinReader BRead(file, QUANT);
            DuckDBWriter DBWriter({ "LR", "FR", "C1R", "C2L", 
                                "C3F", "C4R", "C5L", "C6F"}, 
                                duckdb_settings::GenerateDuckDBName(title, QUANT));
            
            while (BRead.Read(dat))
                DBWriter.Write(dat);
        }
    }
    
    return 0;
}