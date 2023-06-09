#pragma once

namespace duckdb_settings {
    const std::string DATA_DIR  ="../$DuckDBData/";

    std::string GenerateDuckDBName(std::string title, int quant) {    
        return title + std::to_string(quant);
    }

    std::string GenerateDuckDBFile(std::string title, int quant) {
        return "./" + GenerateDuckDBName(title, quant) + ".duckdb";
    }

    std::string GenerateBinName(std::string title, int quant) {    
        return DATA_DIR + "/FromDB-" + title + "-" + std::to_string(quant) + ".bin";
    }
}
