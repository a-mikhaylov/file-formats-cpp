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
        dat.resize(table_readed.get()->ColumnNames().size()); // продолжить
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
        if (current_group == row_groups_count)
            return false;
        
        arrow_reader->ReadRowGroup(current_group++, &table_readed);

        std::cerr << table_readed->ToString();
        return true;
    }
 
};