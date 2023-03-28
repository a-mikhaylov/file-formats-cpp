#pragma once

#include "include_base_exmpl.h"
#include "careate_write_iterative.h"

class ArrowDataReader {
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    std::unique_ptr<parquet::arrow::FileReader>  arrow_reader;
    std::shared_ptr<arrow::Table>                table_readed;
    
    int row_groups_count;
public:
    arrow::Status Init(std::string fname) {
        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(fname));
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));

        row_groups_count = arrow_reader->num_row_groups();
        return arrow::Status::OK();
    }

    ArrowDataReader(std::string fname) { Init(fname); }
};