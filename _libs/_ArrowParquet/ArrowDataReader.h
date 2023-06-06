#pragma once

#include "Examples/include_base_exmpl.h"
#include "Examples/careate_write_iterative.h"

class ArrowDataReader {
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
    std::shared_ptr<arrow::Table>                table_readed;
    
    std::vector<size_t> shuffle_idx;
    size_t shuffle_part;

    int RG_count;       //количество row_group в файле
    int Column_count;   //количество колонок в файле
    int Rows_count;     //количество строк в одной группе
    int current_group;
    int need_shuffle = -1;
    
    //https://stackoverflow.com/questions/53347028/how-to-convert-arrowarray-to-stdvector
    void ConvertData(std::vector<std::vector<int32_t>>& dat) {
        //подгоняем под нужный размер (соответствует table_readed)
        Update();
        
        // Column_count = table_readed.get()->ColumnNames().size();
        // Rows_count   = table_readed.get()->column(0).get()->length();
        // std::cerr << "::ConvertData: " << std::endl
        //           << "\tColumns: " << Column_count << std::endl
        //           << "\tRows:    " << Rows_count << std::endl;

        dat.resize(Column_count); 
        for (int i = 0; i < dat.size(); ++i) {
            dat[i].resize(Rows_count);
            //возможно стоит брать не только 0-й chunk (?)
            auto in32_array = std::static_pointer_cast<arrow::Int32Array>(table_readed.get()->column(i).get()->chunk(0));
            for (int j = 0; j < Rows_count; ++j) {
                dat[i][j] = in32_array->Value(j);
            }
        }
    }

    void Update() {
        Column_count = table_readed.get()->ColumnNames().size();
        Rows_count   = table_readed.get()->column(0).get()->length();
        
        if (need_shuffle != -1)
            Shuffle(need_shuffle);
    }

public:
    arrow::Status Init(std::string fname) {
        if (!std::ifstream(fname).is_open()) {
            std::cerr << "File doesn't exist" << std::endl;
            return arrow::Status::OK();
        }

        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(fname));
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));

        // arrow_reader->parquet_reader()->metadata()->

        RG_count = arrow_reader->num_row_groups();
        current_group = 0;
        return arrow::Status::OK();
    }

    ArrowDataReader(std::string fname) { Init(fname); }
    
    //перемешать порядок чтения (весь файл случайными кусками (размер куска на входе))
    void Shuffle(size_t part_size) {
        if (need_shuffle == -1)
            need_shuffle = part_size;
        else {
            int parts_count = RG_count * Rows_count / part_size + 1;
            shuffle_idx.resize(parts_count);
            
            for (int i = 0; i < shuffle_idx.size(); ++i)
                shuffle_idx[i] = i;

            srand(time(NULL));
            std::random_shuffle(shuffle_idx.begin(), shuffle_idx.end());
            shuffle_part = part_size;
            
            std::cerr << "\tShuffled: { ";
            for (int i = 0; i < shuffle_idx.size(); ++i)
                std::cerr << shuffle_idx[i] << ", ";
            std::cerr << "\b\b }" << std::endl;
            current_group = 0;
            need_shuffle = -1;
        } 
    }

    //чтение следующей по порядку группы строк
    bool Read(std::vector<std::vector<int32_t>>& dat) {
        if (current_group == RG_count) {
            std::cerr << "Rows Groups in input:" << RG_count << std::endl;
            return false;
        }

        if (shuffle_idx.size() != 0) {
            return Read(dat, { shuffle_idx[current_group++]*shuffle_part, shuffle_part });
        }
        
        arrow_reader->ReadRowGroup(current_group++, &table_readed);
        ConvertData(dat);

        // std::cerr << table_readed->ToString();
        return true;
    }

    //чтение произвольного места (по всем каналам)
    bool Read(std::vector<std::vector<int32_t>>& dat, std::pair<int, int> to_read) {
        if (to_read.first < 0 || to_read.second < 0)
            return false;

        dat.clear();

        int x0 = to_read.first;
        int xk = to_read.first + to_read.second - 1;
        
        //узнаем размерность таблицы
        arrow_reader->ReadRowGroup(0, &table_readed);
        Update();

        int RG0  = x0 / Rows_count;     //группа, в которой находится начало отрезка
        int row0 = x0 % Rows_count;     //строка в группе RG0, являющаяся началом входного отрезка

        int RGk  = xk / Rows_count;     //группа, в которой находится конец отрезка
        int rowk = xk % Rows_count;     //строка в группе RGk, являющаяся концом входного отрезка

        std::vector<std::vector<int32_t>> tmp;
        //читаем все группы, которые находятся в нужном интервале
        for (int group = RG0; group < RGk + 1; ++group) {
            if (group >= RG_count) {
                std::cerr << "group > RG_count" << std::endl;
                return false; 
            }

            arrow_reader->ReadRowGroup(group, &table_readed);
            ConvertData(tmp);   //tmp = [8 x 9999]
    
            if (group == RGk) {
                for (int i = 0;  i < tmp.size(); ++i) {
                    if (rowk + 1 > tmp[i].size()) {
                        return false;
                    }
                    
                    tmp[i].erase(tmp[i].begin() + rowk + 1, tmp[i].end());
                }
            }

            if (group == RG0) {
                for (int i = 0;  i < tmp.size(); ++i) {
                    if (row0 > tmp[i].size()) {
                        return false;
                    }
                    
                    tmp[i].erase(tmp[i].begin(), tmp[i].begin() + row0);
                }
            }
            
            // std::cerr << "\ttmp.size(): " << tmp.size() << std::endl;
            if (dat.size() != tmp.size())
                dat.resize(tmp.size());
            for (int i = 0; i < dat.size(); ++i)
                dat[i].insert(dat[i].end(), tmp[i].begin(), tmp[i].end());
            
            std::cerr << "\tdat.size(): " << dat.size() << std::endl;
        }

        return true;
    }
 
    //чтение произвольного места (в нужном канале)
    bool Read(std::vector<std::vector<int32_t>>& dat, std::pair<int, int> to_read, int channel) {
        if (to_read.first < 0 || to_read.second < 0 || channel < 0)
            return false;
            
        dat.clear();

        int x0 = to_read.first;
        int xk = to_read.first + to_read.second;

        int RG0  = x0 / RG_count;     //группа, в которой находится начало отрезка
        int row0 = x0 % RG_count;     //строка в группе RG0, являющаяся началом входного отрезка

        int RGk  = xk / RG_count;     //группа, в которой находится конец отрезка
        int rowk = xk % RG_count;     //строка в группе RGk, являющаяся концом входного отрезка

        std::vector<std::vector<int32_t>> tmp;
        //читаем все группы, которые находятся в нужном интервале
        /*for (int group = RG0; group < RGk + 1; ++group) {
            if (group >= RG_count) {
                std::cerr << "group > RG_count" << std::endl;
                return false; 
            }

            arrow_reader->ReadRowGroup(group, &table_readed);
            ConvertData(tmp);

            if (group == RG0) {
                for (int i = 0;  i < tmp.size(); ++i) {
                    if (row0 > tmp[i].size()) {
                        std::cerr << "row0 > tmp[" << i << "].size()" << std::endl;
                        return false;
                    }
                    tmp[i].erase(tmp[i].begin(), tmp[i].begin() + row0);
                }
            }
            
            if (group == RGk) {
                for (int i = 0;  i < tmp.size(); ++i) {
                    if (row0 > tmp[i].size()) {
                        std::cerr << "row0 > tmp[" << i << "].size()" << std::endl;
                        return false;
                    }

                    tmp[i].erase(tmp[i].begin() + rowk, tmp[i].end());
                }
            }

            dat.insert(dat.end(), tmp.begin(), tmp.end());
        }*/

        if (channel > dat.size() - 1)
            return false;

        //dat = std::vector<std::vector<int32_t>>({dat[channel]});

        return true;
    }
};