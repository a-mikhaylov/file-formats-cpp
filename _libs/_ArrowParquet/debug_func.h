#pragma once

#include "include_base_exmpl.h"


namespace debug_set {
    const std::string BIG_FILE   = "/../_data/big_8x60e6";
    const std::string SMALL_FILE = "/../_data/small_8x1e6";

    void LogWriteResultWrite(std::ofstream& log_out, int step_num, int quant_size, int parquet_size, 
                            float bin_parquet_time, int write_parts, std::string comment = "") 
    {
        log_out << "NODE #" << step_num << ":" << std::endl
                << "Comment: " << comment << std::endl << std::endl
                << "\tQuant     = " << quant_size << std::endl
                << "\tFile size = " << parquet_size << std::endl
                << std::endl
                << "\tWRITE     time = " << bin_parquet_time << "c" << std::endl
                << "\tAvg_quant time =  " << bin_parquet_time / (float)write_parts << "c" << std::endl
                /* << std::endl
                << "\tparquet --> bin     time = " << parquet_bin_time << "c" << std::endl
                << "\tAvg read time =  " << parquet_bin_time / (float)read_parts << "c" << std::endl */
                << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    }

    void LogWriteResultRead(std::ofstream& log_out, int step_num, int quant_size, int parquet_size, 
                            float parquet_bin_time, int read_parts, std::string comment = "") 
    {
        log_out << "NODE #" << step_num << ":" << std::endl
                << "Comment: " << comment << std::endl << std::endl
                << "\tQuant     = " << quant_size << std::endl
                << "\tFile size = " << parquet_size << std::endl
                /* << std::endl
                << "\tWRITE     time = " << bin_parquet_time << "c" << std::endl
                << "\tAvg_quant time =  " << bin_parquet_time / (float)write_parts << "c" << std::endl*/
                << std::endl
                << "\tREAD      time = " << parquet_bin_time << "c" << std::endl
                << "\tAvg_quant time =  " << parquet_bin_time / (float)read_parts << "c" << std::endl
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

void ResetTime(float& var1, float& var2) {
    var1 = 0.0f;
    var2 = 0.0f;
}
