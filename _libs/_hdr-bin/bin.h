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
    
    int32_t*  buf   = nullptr; 
    int32_t*  big_buf   = nullptr; 
    int32_t** buf2D = nullptr; //[точка][канал] = значение (read_write_quant x channels_count)

    std::ifstream bin_input;

    bool PrepareData(std::vector<std::vector<int32_t>>& data) {
        if (data.size() != channels_count)
            data.resize(channels_count);

        //делаем все столбцы длиной в read_write_quant
        for (int i = 0; i < data.size(); ++i)
            if (data[i].size() != read_write_quant)
                data[i].resize(read_write_quant);

        return true;
    } 

    //последняя итерация - прочитали меньше, чем квант
    void CutTail(std::vector<std::vector<int32_t>>& data, int how_much_save) {
        // std::cerr << "::CutTail()" << std::endl;
        for (int i = 0; i < data.size(); ++i) 
            data[i].resize(how_much_save);
    }

    void AlarmBack(int first_empty_idx) {
        std::cerr << "::AlarmBack(" << first_empty_idx << ")" << std::endl;

        for (int i = first_empty_idx; i < read_write_quant; ++i) 
            for (int j = 0;  j < channels_count; ++j) 
                buf2D[i][j] = INT32_MAX; //0x7FFFFFFF
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
        big_buf = new int32_t[channels_count * read_write_quant];
        
        buf2D = new int32_t*[read_write_quant];
        for (int i = 0; i < read_write_quant; ++i) buf2D[i] = new int32_t[channels_count];
    }

    std::string getFileName() {
        return file_path + ".bin";
    }

    BinReader(std::string path, int quant) { Init(path, quant); }

    ~BinReader() { 
        std::cerr << "!--Points Readed: " << POINTS_READED << std::endl;
        if (buf != nullptr)
            delete [] buf; 

        if (big_buf != nullptr)
            delete [] big_buf; 

        if (buf2D != nullptr) {
            for (int i = 0; i < channels_count; ++i)
                delete [] buf2D[i];
            
            delete [] buf2D; 
        }
    }

    size_t getChannelsCount() { return channels_count; }

    size_t getPointsCount() { return hdr.start_end[1] - hdr.start_end[0]; }

    //data = [канал][точка]
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

        return true;
    }

    //читаем QUANT точек в буффер
    //выигрыша по времени нет (наверное, можно оптимизировать)
    bool Read2(std::vector<std::vector<int32_t>>& data) {
        if (next_stop) 
            return false;

        if (!PrepareData(data))
            return false;
        
        bin_input.read((char *)big_buf, SIZE * read_write_quant);

        if (bin_input.eof()) {
            next_stop = true;
        }

        int points_readed_last_time = bin_input.gcount() / channels_count / sizeof(int32_t);
        POINTS_READED += points_readed_last_time;

        for (int i = 0; i < channels_count; ++i) {
            for (int j = 0; j < points_readed_last_time; ++j) {
                data[i][j] = big_buf[j * channels_count + i];
            }
        }


        if (points_readed_last_time != read_write_quant) {
            CutTail(data, points_readed_last_time);
        }

        return true;
    }

    bool Read(int32_t**& array) {
        if (next_stop) 
            return false;
        
        int i;
        for (i = 0; i < read_write_quant; ++i) {
            bin_input.read((char *)buf2D[i], SIZE);
            if (bin_input.eof()){
                next_stop = true;
                break;
            }
            ++POINTS_READED;
        }

        if (i != read_write_quant)
            AlarmBack(i);

        array = buf2D;

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

    void FirstRun(std::vector<std::vector<int32_t>>& dat, bool revers = false) {
        if (revers) {
            if (dat.size() != 0)
                channels_count = dat[0].size();
        } else 
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

    // data[канал][точка]
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
