#pragma once 

#include <iostream>
#include <boost/filesystem.hpp>
#include "../_ArrowParquet/Examples/include_base_exmpl.h"

namespace filesys = boost::filesystem;

class FileRunInfo {
    std::string file_id = "";
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

    bool isEqual(FileRunInfo& other) {
        if (file_id == other.file_id)
            return true;
        return false;
    }

    bool isTest3() {
        if (read_interval_points != 0 && read_interval_time != 0.0f)
            return true;
        return false;
    }

    void Reset() {
        file_id = "";
        ch_num = 0, all_point = 0;

        compression_type = "NONE"; 
        quant_points = 0;

        time_write = 0.0f, time_write_quant = 0.0f;
        time_read = 0.0f, time_read_quant = 0.0f;
        
        file_size_mb = 0.0f, compressed_file_size_mb = 0.0f; 
        compression_coef = 0.0f; 
        
        read_interval_points = 0;
        read_interval_time = 0.0f;
    }

    //нужно, чтобы совместить информацию с разных тестов, но по 
    //одному и тому же файлу
    void Merge(FileRunInfo& other) {
        if (file_id == "")
            file_id = other.file_id;
        if (ch_num == 0)
            ch_num = other.ch_num;
        if (all_point == 0)
            all_point = other.all_point; 
        if (compression_type == "NONE")
            compression_type = other.compression_type;
        if (quant_points == 0)
            quant_points = other.quant_points; 
        
        if (time_write == 0.0f)
            time_write = other.time_write; 
        if (time_write_quant == 0.0f)
            time_write_quant = other.time_write_quant; 
        if (time_read == 0.0f)
            time_read = other.time_read; 
        if (time_read_quant == 0.0f)
            time_read_quant = other.time_read_quant; 
        
        if (file_size_mb == 0.0f)
            file_size_mb = other.file_size_mb; 
        if (compressed_file_size_mb == 0.0f)
            compressed_file_size_mb = other.compressed_file_size_mb; 
        if (compression_coef == 0.0f)
            compression_coef = other.compression_coef; 

        if (read_interval_points == 0)
            read_interval_points = other.read_interval_points; 
        if (read_interval_time == 0.0f)
            read_interval_time = other.read_interval_time; 
    }

    void setFileID(std::string fname) {
        file_id = fname;
    }

    //параметры записи
    void setInfo(int _ch_num, int _all_point) { ch_num = _ch_num; all_point = _all_point; }

    void setQuant(int _quant) { quant_points = _quant; }

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
        time_write = _time_write;  time_write_quant = _time_write_quant;
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

    void setIntervalReading(FileRunInfo& x) {
        read_interval_points = x.read_interval_points;
        read_interval_time = x.read_interval_time;
    }

    std::string ToString() {
        std::string res;
        res = file_id + "; " + std::to_string(ch_num) + "; " + std::to_string(all_point) + "; " + compression_type + "; " +
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

    void printTitle() {
        out << "run_nr; file_id; ch_num; all_points; compresion_type; " 
            << "quant_points; time_write; time_write_quant; time_read; " 
            << "time_read_quant; file_size_mb; compressed_file_size_mb; "
            << "compression_coef; read_interval_points; read_interval_time" 
            << std::endl;
    }

public:

    void Init(std::string file_name) {
        run_nr = 0;
        fname = file_name;
        out.open(fname, std::ios::app);

        if (!out.is_open()) {
            std::cerr << "LOG FILE NOT FOUND" << std::endl;
            return;
        }
    }

    Log(std::string file_name) { Init(file_name); }
    Log() { run_nr = 0; }

    void addInfo(FileRunInfo& x) { 
        for (int i = 0; i < info.size(); ++i) {
            if (x.isEqual(info[i])) {
                if (info[i].isTest3() && x.isTest3()) { //в эту ячейку уже записан один прогон кусочного чтения
                    info.insert(info.begin() + i, FileRunInfo(info[i]));
                    info[i + 1].setIntervalReading(x);
                    return;
                } if (!info[i].isTest3() && x.isTest3()) {
                    info[i].setIntervalReading(x);
                    return;
                }

                info[i].Merge(x);
                return;
            }
        }

        info.push_back(x); 
    }

    void Write(int idx) {
        if (idx < 0 || idx > info.size())
            return;
        if (run_nr == 0)
            printTitle();

        out << std::to_string(run_nr++) + "; " + info[idx].ToString() << std::endl;
        info.erase(info.begin() + idx);
    }

    void Write() { Write(0); }

    void Flush() {
        while(info.size() > 0)
            Write(0);
    }
};
