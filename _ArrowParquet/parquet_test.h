#pragma once

#include "include_base_exmpl.h"
#include "example_with_slice.h"

#define DATASET_DIR  std::string("./ecg_parquet_dataset")
#define DATASET_NAME std::string("/ecg.parquet")

using parquet::WriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;

arrow::Result<std::shared_ptr<arrow::Table>> CreateECGTable() {
    // This code should look familiar from the basic Arrow example, and is not the
    // focus of this example. However, we need data to work on it, and this makes that!
    auto schema =
        arrow::schema({arrow::field("LR", arrow::int32()),
                       arrow::field("FR", arrow::int32()),
                       arrow::field("C1R", arrow::int32())});
    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
    arrow::NumericBuilder<arrow::Int32Type> builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({9, 8, 7, 6, 5, 4, 3, 2, 1, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));
    return arrow::Table::Make(schema, {array_a, array_b, array_c});
}

arrow::Result<std::string> CreateECGParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const std::string& root_path) {
    // Much like CreateTable(), this is utility that gets us the dataset we'll be reading
    // from. Don't worry, we also write a dataset in the example proper.
    auto base_path = root_path + DATASET_DIR;
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    // Create an Arrow Table
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, CreateECGTable());
    // Write it into two Parquet files
    ARROW_ASSIGN_OR_RAISE(auto output,
                        filesystem->OpenOutputStream(base_path + DATASET_NAME));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table, arrow::default_memory_pool(), output, 2048));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteMetaDataFile(
        *table, arrow::default_memory_pool(), output, 2048));
    return base_path;
}

arrow::Status PrepareECGEnv() {
    // Get our environment prepared for reading, by setting up some quick writing.
    ARROW_ASSIGN_OR_RAISE(auto src_table, CreateECGTable())
    std::shared_ptr<arrow::fs::FileSystem> setup_fs;
    // Note this operates in the directory the executable is built in.
    char setup_path[256];
    char* result = getcwd(setup_path, 256);
    if (result == NULL) {
      return arrow::Status::IOError("Fetching PWD failed.");
    }

    ARROW_ASSIGN_OR_RAISE(setup_fs, arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateECGParquetDataset(setup_fs, ""));
    
    return arrow::Status::OK();
}

arrow::Status RunMain() {
    ARROW_RETURN_NOT_OK(PrepareECGEnv());

    std::cerr << "~~~~~~~~~~~~~~~~~READING~~~~~~~~~~~~~~~~~~~" << std::endl;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    // ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./" + write_options.base_dir + "/ecg_data=0/part0.parquet"));
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(DATASET_DIR + DATASET_NAME));
    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table_readed;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

    std::cerr << table_readed->ToString();

    return arrow::Status::OK();
}

int _parquetMain() {
    arrow::Status st = RunMain();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    std::cerr << "succesful ending !!" << std::endl;
    return 0;
}