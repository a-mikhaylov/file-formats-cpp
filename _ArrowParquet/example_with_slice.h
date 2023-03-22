#pragma once

#include "include_base_exmpl.h"

#define NEED_TEST_READING

// Generate some data for the rest of this example.
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
    // This code should look familiar from the basic Arrow example, and is not the
    // focus of this example. However, we need data to work on it, and this makes that!
    auto schema =
        arrow::schema({arrow::field("ecg_data", arrow::int32()), arrow::field("LR", arrow::int32()),
                       arrow::field("FR", arrow::int32())});
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

// Set up a dataset by writing two Parquet files.
arrow::Result<std::string> CreateExampleParquetDataset(
    const std::shared_ptr<arrow::fs::FileSystem>& filesystem,
    const std::string& root_path) {
    // Much like CreateTable(), this is utility that gets us the dataset we'll be reading
    // from. Don't worry, we also write a dataset in the example proper.
    auto base_path = root_path + "parquet_dataset";
    ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
    // Create an Arrow Table
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, CreateTable());
    // Write it into two Parquet files
    ARROW_ASSIGN_OR_RAISE(auto output,
                        filesystem->OpenOutputStream(base_path + "/data1.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table->Slice(0, 5), arrow::default_memory_pool(), output, 2048));
    ARROW_ASSIGN_OR_RAISE(output,
                          filesystem->OpenOutputStream(base_path + "/data2.parquet"));
    ARROW_RETURN_NOT_OK(parquet::arrow::WriteTable(
        *table->Slice(5), arrow::default_memory_pool(), output, 2048));

    return base_path;
}

arrow::Status PrepareEnv() {
    // Get our environment prepared for reading, by setting up some quick writing.
    ARROW_ASSIGN_OR_RAISE(auto src_table, CreateTable())
    std::shared_ptr<arrow::fs::FileSystem> setup_fs;
    // Note this operates in the directory the executable is built in.
    char setup_path[256];
    char* result = getcwd(setup_path, 256);
    if (result == NULL) {
      return arrow::Status::IOError("Fetching PWD failed.");
    }

    ARROW_ASSIGN_OR_RAISE(setup_fs, arrow::fs::FileSystemFromUriOrPath(setup_path));
    ARROW_ASSIGN_OR_RAISE(auto dset_path, CreateExampleParquetDataset(setup_fs, ""));
    
    return arrow::Status::OK();
}

arrow::Status RunMain_WithSlicing() {
    ARROW_RETURN_NOT_OK(PrepareEnv());

    //Preparing a FileSystem Object
    std::shared_ptr<arrow::fs::FileSystem> fs;

    char init_path[256];
    char* result = getcwd(init_path, 256);
    if (result == NULL) {
        return arrow::Status::IOError("Fetching PWD failed.");
    }
    ARROW_ASSIGN_OR_RAISE(fs, arrow::fs::FileSystemFromUriOrPath(init_path));
    
    //Creating a FileSystemDatasetFactory

    // A file selector lets us actually traverse a multi-file dataset
    arrow::fs::FileSelector selector;
    selector.base_dir = "parquet_dataset";
    selector.recursive = true;

    // Making an options object lets us configure our dataset reading.
    arrow::dataset::FileSystemFactoryOptions options{};
    options.partitioning = arrow::dataset::HivePartitioning::MakeFactory();

    std::shared_ptr<arrow::dataset::ParquetFileFormat> read_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    ARROW_ASSIGN_OR_RAISE(auto factory, arrow::dataset::FileSystemDatasetFactory::Make(
                                          fs, selector, read_format, options));

    //Build Dataset using Factory
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Dataset> read_dataset, factory->Finish());
    ARROW_ASSIGN_OR_RAISE(arrow::dataset::FragmentIterator fragments, read_dataset->GetFragments());
    for (const auto& fragment : fragments) {
        std::cout << "Found fragment: " << (*fragment)->ToString() << std::endl;
        std::cout << "Partition expression: "
                << (*fragment)->partition_expression().ToString() << std::endl;
    }

    //Move Dataset into Table
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::ScannerBuilder> read_scan_builder, read_dataset->NewScan());
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Scanner> read_scanner, read_scan_builder->Finish());

    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table, read_scanner->ToTable());
    std::cerr << table->ToString();

    //Prepare Data from Table for Writing
    std::shared_ptr<arrow::TableBatchReader> write_dataset = std::make_shared<arrow::TableBatchReader>(table);
    
    //Create Scanner for Moving Table Data
    std::shared_ptr<arrow::dataset::ScannerBuilder> write_scanner_builder = arrow::dataset::ScannerBuilder::FromRecordBatchReader(write_dataset);
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::dataset::Scanner> write_scanner, write_scanner_builder->Finish());

    //Prepare Schema, Partitioning, and File Format Variables
    std::shared_ptr<arrow::Schema> partition_schema = arrow::schema({arrow::field("ecg_data", arrow::utf8())});
    std::shared_ptr<arrow::dataset::HivePartitioning> partitioning = std::make_shared<arrow::dataset::HivePartitioning>(partition_schema);
    
    std::shared_ptr<arrow::dataset::ParquetFileFormat> write_format = std::make_shared<arrow::dataset::ParquetFileFormat>();
    
    //Configure FileSystemDatasetWriteOptions
    arrow::dataset::FileSystemDatasetWriteOptions write_options;

    write_options.file_write_options     = write_format->DefaultWriteOptions();
    write_options.filesystem             = fs;
    write_options.base_dir               = "write_dataset";
    write_options.partitioning           = partitioning;
    write_options.basename_template      = "part{i}.parquet";
    write_options.existing_data_behavior = arrow::dataset::ExistingDataBehavior::kOverwriteOrIgnore;

    //Write Dataset to Disk
    ARROW_RETURN_NOT_OK(arrow::dataset::FileSystemDataset::Write(write_options, write_scanner));    

//----------READ WRITTEN INFORMATION--------------------------
#ifdef NEED_TEST_READING
    std::cerr << "~~~~~~~~~~~~~~~~~READING~~~~~~~~~~~~~~~~~~~" << std::endl;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    // ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./" + write_options.base_dir + "/ecg_data=0/part0.parquet"));
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./parquet_dataset/data1.parquet"));
    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table_readed;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

    std::cerr << table_readed->ToString();
#endif

    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    return arrow::Status::OK();
}