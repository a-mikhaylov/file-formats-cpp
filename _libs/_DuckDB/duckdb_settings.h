#pragma once

namespace duckdb_settings {
    
    std::string GenerateDuckDBName(std::string title, int quant) {    
        return "/" + title + "-" + std::to_string(quant) + ".duckdb";
    }

    std::string GenerateBinName(std::string title, int quant) {    
        return "/FromDB-" + title + "-" + std::to_string(quant) + ".bin";
    }
}
