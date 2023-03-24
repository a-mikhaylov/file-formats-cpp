#pragma once
//https://arrow.apache.org/docs/cpp/parquet.html
#include "include_base_exmpl.h"
#include "example_with_slice.h"
#include "create_write_once.h"

#include <parquet/metadata.h>
#include <parquet/stream_reader.h>
#include <parquet/stream_writer.h>

using parquet::WriterProperties;
using parquet::ArrowWriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;

arrow::Result<std::string> CreateAndWriteIterate(const std::shared_ptr<arrow::fs::FileSystem>& filesystem, 
                                                 const std::string& root_path) 
{
    std::cerr << "::CreateAndWriteIterate()" << std::endl;
    auto base_path = root_path + DATASET_DIR;

    //Create Table
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {
            arrow::field("LR", arrow::int32()),
            arrow::field("FR", arrow::int32()),
            arrow::field("C1R", arrow::int32())
        });

    std::shared_ptr<arrow::Array> array_a;
    std::shared_ptr<arrow::Array> array_b;
    std::shared_ptr<arrow::Array> array_c;
    arrow::NumericBuilder<arrow::Int32Type> builder;
    ARROW_RETURN_NOT_OK(builder.AppendValues({10, 11, 12, 13, 14, 15, 16, 17, 18, 19}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({90, 80, 70, 60, 50, 40, 30, 20, 10, 0}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({1, 2, 1, 2, 1, 2, 1, 2, 1, 2}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));

    std::shared_ptr<arrow::Table> table = arrow::Table::Make(schema, {array_a, array_b, array_c});
    std::cerr << "\t- Table created" << std::endl;
//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //Write Table
    // std::shared_ptr<arrow::RecordBatchReader> batch_stream;
    // ARROW_ASSIGN_OR_RAISE(batch_stream, GetRBR());
    
    std::shared_ptr<WriterProperties> props =
    WriterProperties::Builder().compression(arrow::Compression::UNCOMPRESSED)->build();

    // std::shared_ptr<ArrowWriterProperties> arrow_props =
    // ArrowWriterProperties::Builder().store_schema()->build();

    std::shared_ptr<::arrow::io::FileOutputStream> output; //base_path + DATASET_NAME
    ARROW_ASSIGN_OR_RAISE(output, arrow::io::FileOutputStream::Open(base_path + DATASET_NAME));
    std::unique_ptr<parquet::arrow::FileWriter> file_writer;
    
    ARROW_ASSIGN_OR_RAISE(file_writer, parquet::arrow::FileWriter::Open(*schema,
                                             arrow::default_memory_pool(), output,
                                             props, parquet::default_arrow_writer_properties()));

    std::cerr << "\t- FileWriter created" << std::endl;
    ARROW_RETURN_NOT_OK(file_writer->WriteTable(*table.get(), 2048));
    ARROW_RETURN_NOT_OK(file_writer->WriteTable(*table.get(), 2048));

    std::cerr << "\t- Table written" << std::endl;
    return base_path;
}

arrow::Status PrepareECGIterateEnv() {
    std::cerr << "::PrepareECGIterateEnv()" << std::endl;
    std::shared_ptr<arrow::fs::FileSystem> setup_fs;

    char setup_path[256];
    char* result = getcwd(setup_path, 256);
    if (result == NULL) {
      return arrow::Status::IOError("Fetching PWD failed.");
    }

    ARROW_ASSIGN_OR_RAISE(setup_fs, arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateAndWriteIterate(setup_fs, ""));
    
    return arrow::Status::OK();
}

arrow::Status RunMain() {
    ARROW_RETURN_NOT_OK(PrepareECGIterateEnv());

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