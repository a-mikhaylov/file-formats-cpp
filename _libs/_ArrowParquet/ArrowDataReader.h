#pragma once

#include "include_base_exmpl.h"
#include "careate_write_iterative.h"

class ArrowDataReader {
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
    std::shared_ptr<arrow::Table>                table_readed;
    
    int row_groups_count;
    int current_group;
    
    //https://stackoverflow.com/questions/53347028/how-to-convert-arrowarray-to-stdvector
    void ConvertData(std::vector<std::vector<int32_t>>& dat) {
        //подгоняем под нужный размер (соответствует table_readed)
        int column_count = table_readed.get()->ColumnNames().size();
        int row_count    = table_readed.get()->column(0).get()->length();

        dat.resize(column_count); 
        for (int i = 0; i < dat.size(); ++i) {
            dat[i].resize(row_count);
            //возможно стоит брать не только 0-й chunk (?)
            auto in32_array = std::static_pointer_cast<arrow::Int32Array>(table_readed.get()->column(i).get()->chunk(0));
            for (int j = 0; j < row_count; ++j) {
                dat[i][j] = in32_array->Value(j);
            }
        }
    }

public:
    arrow::Status Init(std::string fname) {
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(fname));
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));

        row_groups_count = arrow_reader->num_row_groups();
        current_group = 0;
        return arrow::Status::OK();
    }

    ArrowDataReader(std::string fname) { Init(fname); }

    bool Read(std::vector<std::vector<int32_t>>& dat) {
        if (current_group == row_groups_count) {
            std::cerr << "Rows Groups in input:" << row_groups_count << std::endl;
            return false;
        }
        
        arrow_reader->ReadRowGroup(current_group++, &table_readed);
        ConvertData(dat);

        // std::cerr << table_readed->ToString();
        return true;
    }
 
};