#pragma once

#include "hdr.h"

#define BIN_HDR_PATH std::string("/media/oldwizzard/kotel/_work/git/file-formats-cpp/_data/PX1447191017125822")

//чтение данных из bin файла
//TODO: оптимизировать - избавиться от векторов в работе - перейти только на указатели
class BinReader {
    int POINTS_READED = 0;

    Header      hdr;
    int         channels_count; //кол-во каналов в записи
    int         SIZE;           //кол-во считываемых байтов за один прогон (1 точка с канала)
    std::string file_path; //без расширения!
    int         read_write_quant; //кол-во считываемых значений за одиин прогон
    bool        next_stop = false; //если следующего прогона быть не должно - ставим в true
    int32_t* buf; 

    std::ifstream bin_input;
    bool PrepareData(std::vector<std::vector<int32_t>>& data) {
        // std::cerr << "::PrepareData()" << std::endl;
        int columns_cnt = channels_count;  //кол-во столбцов, как в hdr
        
        //дали меньше столбцов, чем надо => отказываемся работать, чтобы не заполнять чем попало
        if (data.size() < columns_cnt) {
            for (int i = data.size(); i < columns_cnt; ++i) data.push_back({});    
        }

        //откидываем лишние столбцы
        for (int i = data.size(); i > columns_cnt; --i) data.pop_back();

        //делаем все столбцы длиной в read_write_quant
        for (int i = 0; i < data.size(); ++i)
            data[i].resize(read_write_quant);
        
        /* std::cerr << "{" << std::endl;
        for (int i = 0; i < data.size(); ++i) {
            std::cerr << "\t{";
            for (int j = 0; j < data[i].size(); ++j)
                std::cerr << data[i][j] << ", ";
            std::cerr << "}," << std::endl; 
        }
        std::cerr << "}" << std::endl; */

        return true;
    } 

    //последняя итерация - прочитали меньше, чем квант
    void CutTail(std::vector<std::vector<int32_t>>& data, int how_much_save) {
        std::cerr << "::CutTail()" << std::endl;
        for (int i = 0; i < data.size(); ++i) 
            data[i].resize(how_much_save);
    }

public:
    void Init(std::string path, int quant) {
        std::cerr << "::Init()" << std::endl;
        read_write_quant = quant;
        file_path = path;
        
        hdr.ReadFile(file_path + ".hdr");
        std::cerr << "\t- HDR success" << std::endl;

        bin_input.open(file_path + ".bin");
        if (!bin_input.is_open()) {
            std::cerr << "[ERROR]: .bin file didn't found!!!" << std::endl;
            return;
        }
        std::cerr << "\t- bin out success" << std::endl;
        channels_count = hdr.info[0];
        SIZE = channels_count * sizeof(int32_t);
        std::cerr << "\t- Channels count = " << channels_count << std::endl;
        buf = new int32_t[channels_count];
    }

    BinReader(std::string path, int quant) { Init(path, quant); }

    ~BinReader() { 
        std::cerr << "!--Points Readed: " << POINTS_READED << std::endl;
        delete [] buf; 
    }

    bool getData(std::vector<std::vector<int32_t>>& data) {
        if (next_stop) 
            return false;

        if (!PrepareData(data))
            return false;
        
        int i;
        for (i = 0; i < read_write_quant; ++i) {
            bin_input.read((char *)buf, SIZE);
            if (bin_input.eof()){
                next_stop = true;
                break;
            }
            ++POINTS_READED;

            for (int j = 0; j < channels_count; ++j) {
                data[j][i] = buf[j];
            }
        }

        if (i != read_write_quant)
            CutTail(data, i);

        // delete [] buf;
        return true;
    }

    void _TestRun(int go_times, std::vector<std::vector<int32_t>>& data) {
        std::cerr << "::_TestRun()" << std::endl;

        for (int i = 0; i < go_times; ++i) {
            if (getData(data)) {
                printVec(data);
            }
        }
    }

    void printVec(std::vector<std::vector<int32_t>>& data) {
        std::cerr << "-----------------------" << std::endl;
        for (int i = 0; i < data.size(); ++i) {
            std::cerr << "\t{";
            for (int j = 0; j < data[i].size(); ++j) {
                std::cerr << data[i][j] << ", ";
            }
            std::cerr << "}," << std::endl;
        }
        std::cerr << "-----------------------" << std::endl;
    }
    
};

class BinWriter {
    int POINTS_WRITED = 0;

    Header      hdr;
    int         channels_count; //кол-во каналов в записи
    int         SIZE;           //кол-во считываемых байтов за один прогон (1 точка с канала)
    std::string file_path; //без расширения!
    int         read_write_quant; //кол-во считываемых значений за одиин прогон
    bool        next_stop = false; //если следующего прогона быть не должно - ставим в true
    int32_t* buf; 

    std::ifstream bin_output;
};
