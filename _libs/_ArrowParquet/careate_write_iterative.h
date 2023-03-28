#pragma once

#include "include_base_exmpl.h"

arrow::Result<std::string> CreateAndWriteIterate(const std::shared_ptr<arrow::fs::FileSystem>& filesystem, 
                                                 const std::string& root_path) 
{
    std::cerr << "::CreateAndWriteIterate()" << std::endl;
    auto base_path = root_path + DATASET_DIR;
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    
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
//Change Table
    ARROW_RETURN_NOT_OK(builder.AppendValues({11, 22, 33}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_a));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({44, 55, 66}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_b));
    builder.Reset();
    ARROW_RETURN_NOT_OK(builder.AppendValues({77, 88, 99}));
    ARROW_RETURN_NOT_OK(builder.Finish(&array_c));

    table = arrow::Table::Make(schema, {array_a, array_b, array_c});
//----------    

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

arrow::Status RunMain_Iterative() {
    ARROW_RETURN_NOT_OK(PrepareECGIterateEnv());

    std::cerr << "~~~~~~~~~~~~~~~~~READING~~~~~~~~~~~~~~~~~~~" << std::endl;
    // arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;

    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open(DATASET_DIR + DATASET_NAME));

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));

    std::shared_ptr<arrow::Table> table_readed;
    //только 0 и 1 столбцы (LR, C1R)
    // ARROW_RETURN_NOT_OK(arrow_reader->ReadTable({0, 2}, &table_readed));
    
    for (int i = 0; i < arrow_reader->num_row_groups(); ++i) {
        ARROW_RETURN_NOT_OK(arrow_reader->ReadRowGroup(i, &table_readed));

        std::cerr << table_readed->ToString();
    }
    return arrow::Status::OK();
}