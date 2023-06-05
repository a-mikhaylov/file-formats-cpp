#pragma once

#include "Examples/include_base_exmpl.h"

namespace prqt_settings {
    const std::string LOG_FILE = "../Logs/LogTestCash.csv";

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

    void DebugPartlyReading() {
        std::string cur_path(boost::filesystem::current_path().c_str());
        std::cerr << "[TEST]: prqt_test::Test1_write() - STARTED: "
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
        const std::string data_dir    = cur_path + prqt_test::RUN_DATA_DIR;
        ArrowDataReader ADReader{data_dir + GenerateParquetName("small", 10000, 
                                                                arrow::Compression::type::UNCOMPRESSED)};

        ADReader.Read(dat, {999990, 15});

        PrintVec(dat);
    }
}
