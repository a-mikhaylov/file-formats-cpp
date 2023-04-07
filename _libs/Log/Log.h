#pragma once 

#include <iostream>
#include <boost/filesystem.hpp>
#include "../_ArrowParquet/include_base_exmpl.h"

namespace filesys = boost::filesystem;

class FileRunInfo {
    // std::string file_id;
    int ch_num = 0, all_point = 0;

    std::string compression_type = "NONE"; 
    int quant_points = 0;

    float time_write = 0.0f, time_write_quant = 0.0f;
    float time_read = 0.0f, time_read_quant = 0.0f;
    
    float file_size_mb = 0.0f, compressed_file_size_mb = 0.0f; 
    float compression_coef = 0.0f; 
    
    int read_interval_points = 0;
    float read_interval_time = 0.0f;

public:

    FileRunInfo() {}

    //параметры записи
    void setInfo(int _ch_num, int _all_point) { ch_num = _ch_num; all_point = _all_point; }

    void setRunSetting(arrow::Compression::type compr_type, int _quant_points) {
        
        switch(compr_type) {
            case arrow::Compression::type::UNCOMPRESSED:
                compression_type = "UNCOMP";
                break;
            case arrow::Compression::type::GZIP:
                compression_type = "GZIP";
                break;
            case arrow::Compression::type::ZSTD:
                compression_type = "ZSTD";
                break;
            case arrow::Compression::type::SNAPPY:
                compression_type = "SNAPPY";
                break;
        }

        quant_points = _quant_points;
    }

    void setWriteTime(float _time_write, float _time_write_quant) {
        time_write = _time_write; time_write_quant = _time_write_quant;
    }

    void setReadTime(float _time_read, float _time_read_quant) {
        time_read = _time_read; time_read_quant = _time_read_quant;
    }

    void setFilesSizes(std::string inp_fname, std::string out_fname) {
        float in_fsz = (float)filesys::file_size(filesys::path(filesys::path::string_type(inp_fname)));
        float out_fsz = (float)filesys::file_size(filesys::path(filesys::path::string_type(out_fname)));

        file_size_mb = in_fsz / 1024.0f / 1024.0f;
        compressed_file_size_mb = out_fsz / 1024.0f / 1024.0f;

        compression_coef = compressed_file_size_mb / file_size_mb;
    }

    void setIntervalReading(int _read_intervals, float _read_times) {
        read_interval_points = _read_intervals;
        read_interval_time = _read_times;
    }

    std::string ToString() {
        std::string res;
        res = std::to_string(ch_num) + "; " + std::to_string(all_point) + "; " + compression_type + "; " +
              std::to_string(quant_points) + "; " + std::to_string(time_write) + "; " + std::to_string(time_write_quant) +
              "; " + std::to_string(time_read) + "; " + std::to_string(time_read_quant) + "; " + std::to_string(file_size_mb) +
              "; " + std::to_string(compressed_file_size_mb) + "; " + std::to_string(compression_coef) + "; " +
              std::to_string(read_interval_points) + "; " + std::to_string(read_interval_time);

        return res;
    }
};

class Log {
    std::string fname;
    std::ofstream out;
    
    std::vector<FileRunInfo> info;

    int run_nr; //номер записи

public:

    void Init(std::string file_name) {
        run_nr = 0;
        fname = file_name;
        out.open(fname);

        if (!out.is_open()) {
            std::cerr << "LOG FILE NOT FOUND" << std::endl;
            return;
        }
    }

    Log(std::string file_name) { Init(file_name); }
    Log() { run_nr = 0; }

    void Flush() {
        std::string to_write;
        for (int i = 0; i < info.size(); ++i) {
            to_write = std::to_string(run_nr++) + "; " + info[i].ToString();
            out << to_write << std::endl;
        }
    }
};
