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
    int32_t* buf = nullptr; 

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
        if (buf != nullptr)
            delete [] buf; 
    }

    bool Read(std::vector<std::vector<int32_t>>& data) {
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
            if (Read(data)) {
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

    int         channels_count = -1; //кол-во каналов в записи
    int         SIZE = -1;           //кол-во записываемых байтов за один прогон (1 точка с канала)
    std::string file_path;          //полный путь к файлу

    int32_t* buf = nullptr; 

    std::ofstream bin_output;

    void FirstRun(std::vector<std::vector<int32_t>>& dat) {
        channels_count   = dat.size();

        SIZE    = channels_count * sizeof(int32_t);
        buf     = new int32_t[channels_count];
    }

public:

    void Init(std::string path) {
        file_path = path;

        bin_output.open(file_path);
        if (!bin_output.is_open()) {
            std::cerr << "[ERROR]: .bin file didn't found!!!" << std::endl;
            return;
        }
    }

    BinWriter(std::string path) { Init(path); }

    ~BinWriter() {
        if (buf != nullptr) 
            delete [] buf;
    }

    bool Write(std::vector<std::vector<int32_t>>& dat) {
        //инициализируем оставшиеся значения, считая, что за всё время записи
        //dat не изменит кол-ва столбцов
        if (SIZE == -1)
            FirstRun(dat);
        
        int row_count = dat[0].size();

        for (int i = 0; i < row_count; ++i) {           // строки
            
            for (int j = 0; j < channels_count; ++j)    // столбцы
                buf[j] = dat[j][i];
            
            bin_output.write((char *)buf, SIZE);
        }
        
        return true;
    }

};
