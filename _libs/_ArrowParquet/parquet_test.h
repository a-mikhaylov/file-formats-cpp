#pragma once
//https://arrow.apache.org/docs/cpp/parquet.html
#include "include_base_exmpl.h"
#include "example_with_slice.h"
#include "create_write_once.h"
#include "careate_write_iterative.h"
#include "ArrowDataWriter.h"

arrow::Status RunMain() {
    ARROW_RETURN_NOT_OK(PrepareECGIterateEnv());

    std::cerr << "~~~~~~~~~~~~~~~~~READING~~~~~~~~~~~~~~~~~~~" << std::endl;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;

    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(DATASET_DIR + DATASET_NAME));

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    std::shared_ptr<arrow::Table> table_readed;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

    std::cerr << table_readed->ToString();

    return arrow::Status::OK();
}

int _parquetMain() {
    arrow::Status st = RunMain_Real();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    std::cerr << "succesful ending !!" << std::endl;
    return 0;
}