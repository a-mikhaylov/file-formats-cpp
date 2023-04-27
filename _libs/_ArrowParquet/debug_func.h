#pragma once

#include "include_base_exmpl.h"

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

namespace debug_set {
    const std::string LOG_FILE = "../Logs/LogTestCash.csv";

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
                << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    }

    void LogWriteResultRead(std::ofstream& log_out, int step_num, int quant_size, int parquet_size, 
                            float parquet_bin_time, int read_parts, std::string comment = "") 
    {
        log_out << "NODE #" << step_num << ":" << std::endl
                << "Comment: " << comment << std::endl << std::endl
                << "\tQuant     = " << quant_size << std::endl
                << "\tFile size = " << parquet_size << std::endl
                << std::endl
                << "\tREAD      time = " << parquet_bin_time << "c" << std::endl
                << "\tAvg_quant time =  " << parquet_bin_time / (float)read_parts << "c" << std::endl
                << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    }

    void LogWriteResultRandRead(std::ofstream& log_out, int quant_size, int parquet_size, 
                                std::vector<float> part_time, std::vector<std::pair<int, int>> parts,
                                std::string comment = "") 
    {
        log_out << "Comment: " << comment << std::endl << std::endl
                << "\tQuant = " << quant_size << std::endl 
                << std::endl;
                
        for (int i = 0; i < part_time.size(); ++i) {
            log_out << "\t" << std::to_string(parts[i].first) << " - " <<  std::to_string(parts[i].first + parts[i].second)
                      << ": " << part_time[i] << "c" << std::endl;
        }

        log_out << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    }

    void PrintVector(std::string label, std::vector<int> x) {
        std::cerr << label << std::endl << "\t{ ";
        for (int i = 0; i < x.size(); ++i) std::cerr << x[i] << ",\t";
        std::cerr << "\b\b }" << std::endl;
    }

    void PrintVector(std::string label, std::vector<std::vector<int>> x) {
        std::cerr << label << std::endl;
        for (int i = 0; i < x.size(); ++i) {
            std::cerr << "\t{  ";
            for (int j = 0; j < x[i].size(); ++j)
                std::cerr << x[i][j] << ",\t";
            std::cerr << "\b\b }" << std::endl;
        }
    }

    void DebugPartlyReading() {
        std::string cur_path(boost::filesystem::current_path().c_str());
        std::cerr << "[TEST]: test_ns::Test1_write() - STARTED: "
                << cur_path << std::endl << std::endl;
        
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
        std::vector<std::vector<int32_t>> dat(8);
        const std::string data_dir    = cur_path + test_ns::TESTLOG_DATA_DIR;
        ArrowDataReader ADReader{data_dir + GenerateParquetName("small", 10000, 
                                                                arrow::Compression::type::UNCOMPRESSED)};

        ADReader.Read(dat, {999990, 15});

        PrintVec(dat);
    }
}

void UpdateTime(float& var, high_resolution_clock::time_point& start, high_resolution_clock::time_point& stop) {
    var = var + ((float)duration_cast<microseconds>(stop - start).count() / 1000000.0f);
}

void ResetTime(float& var1, float& var2) {
    var1 = 0.0f;
    var2 = 0.0f;
}

void ResetTime(std::vector<float>& part_time) {
    for (int i = 0; i < part_time.size(); ++i )
        part_time[i] = 0;
}
