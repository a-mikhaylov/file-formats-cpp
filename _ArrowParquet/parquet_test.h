#pragma once

#include <arrow/io/api.h>
#include <arrow/api.h>
#include <arrow/util/type_fwd.h>

#include <arrow/dataset/api.h>
#include <arrow/dataset/dataset.h>

#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>


#include <arrow/dataset/partition.h>
#include <iostream>

using parquet::WriterProperties;
using parquet::ParquetVersion;
using parquet::ParquetDataPageVersion;
using arrow::Compression;

// Generate some data for the rest of this example.
arrow::Result<std::shared_ptr<arrow::Table>> CreateTable() {
    // This code should look familiar from the basic Arrow example, and is not the
    // focus of this example. However, we need data to work on it, and this makes that!
    auto schema =
        arrow::schema({arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()),
                       arrow::field("c", arrow::int32())});
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
    ARROW_ASSIGN_OR_RAISE(auto table, CreateTable());
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

arrow::Status RunMain_Base() {
    // (Doc section: RunMain Start)
    // (Doc section: int8builder 1 Append)
    // Builders are the main way to create Arrays in Arrow from existing values that are not
    // on-disk. In this case, we'll make a simple array, and feed that in.
    // Data types are important as ever, and there is a Builder for each compatible type;
    // in this case, int8.
    arrow::Int8Builder int8builder;
    int8_t days_raw[5] = {1, 12, 17, 23, 28};
    // AppendValues, as called, puts 5 values from days_raw into our Builder object.
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
    // (Doc section: int8builder 1 Append)

    // (Doc section: int8builder 1 Finish)
    // We only have a Builder though, not an Array -- the following code pushes out the
    // built up data into a proper Array.
    std::shared_ptr<arrow::Array> days;
    ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());
    // (Doc section: int8builder 1 Finish)

    // (Doc section: int8builder 2)
    // Builders clear their state every time they fill an Array, so if the type is the same,
    // we can re-use the builder. We do that here for month values.
    int8_t months_raw[5] = {1, 3, 5, 7, 1};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
    std::shared_ptr<arrow::Array> months;
    ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());
    // (Doc section: int8builder 2)

    // (Doc section: int16builder)
    // Now that we change to int16, we use the Builder for that data type instead.
    arrow::Int16Builder int16builder;
    int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
    std::shared_ptr<arrow::Array> years;
    ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());
    // (Doc section: int16builder)

    // (Doc section: Schema)
    // Now, we want a RecordBatch, which has columns and labels for said columns.
    // This gets us to the 2d data structures we want in Arrow.
    // These are defined by schema, which have fields -- here we get both those object types
    // ready.
    std::shared_ptr<arrow::Field> field_day, field_month, field_year;
    std::shared_ptr<arrow::Schema> schema;

    // Every field needs its name and data type.
    field_day = arrow::field("Day", arrow::int8());
    field_month = arrow::field("Month", arrow::int8());
    field_year = arrow::field("Year", arrow::int16());

    // The schema can be built from a vector of fields, and we do so here.
    schema = arrow::schema({field_day, field_month, field_year});
    // (Doc section: Schema)

    // (Doc section: RBatch)
    // With the schema and Arrays full of data, we can make our RecordBatch! Here,
    // each column is internally contiguous. This is in opposition to Tables, which we'll
    // see next.
    std::shared_ptr<arrow::RecordBatch> rbatch;
    // The RecordBatch needs the schema, length for columns, which all must match,
    // and the actual data itself.
    rbatch = arrow::RecordBatch::Make(schema, days->length(), {days, months, years});

    std::cout << rbatch->ToString();
    // (Doc section: RBatch)

    // (Doc section: More Arrays)
    // Now, let's get some new arrays! It'll be the same datatypes as above, so we re-use
    // Builders.
    int8_t days_raw2[5] = {6, 12, 3, 30, 22};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw2, 5));
    std::shared_ptr<arrow::Array> days2;
    ARROW_ASSIGN_OR_RAISE(days2, int8builder.Finish());

    int8_t months_raw2[5] = {5, 4, 11, 3, 2};
    ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw2, 5));
    std::shared_ptr<arrow::Array> months2;
    ARROW_ASSIGN_OR_RAISE(months2, int8builder.Finish());

    int16_t years_raw2[5] = {1980, 2001, 1915, 2020, 1996};
    ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw2, 5));
    std::shared_ptr<arrow::Array> years2;
    ARROW_ASSIGN_OR_RAISE(years2, int16builder.Finish());
    // (Doc section: More Arrays)

    // (Doc section: ArrayVector)
    // ChunkedArrays let us have a list of arrays, which aren't contiguous
    // with each other. First, we get a vector of arrays.
    arrow::ArrayVector day_vecs{days, days2};
    // (Doc section: ArrayVector)
    // (Doc section: ChunkedArray Day)
    // Then, we use that to initialize a ChunkedArray, which can be used with other
    // functions in Arrow! This is good, since having a normal vector of arrays wouldn't
    // get us far.
    std::shared_ptr<arrow::ChunkedArray> day_chunks =
        std::make_shared<arrow::ChunkedArray>(day_vecs);
    // (Doc section: ChunkedArray Day)

    // (Doc section: ChunkedArray Month Year)
    // Repeat for months.
    arrow::ArrayVector month_vecs{months, months2};
    std::shared_ptr<arrow::ChunkedArray> month_chunks =
        std::make_shared<arrow::ChunkedArray>(month_vecs);

    // Repeat for years.
    arrow::ArrayVector year_vecs{years, years2};
    std::shared_ptr<arrow::ChunkedArray> year_chunks =
        std::make_shared<arrow::ChunkedArray>(year_vecs);
    // (Doc section: ChunkedArray Month Year)

    // (Doc section: Table)
    // A Table is the structure we need for these non-contiguous columns, and keeps them
    // all in one place for us so we can use them as if they were normal arrays.
    std::shared_ptr<arrow::Table> table;
    table = arrow::Table::Make(schema, {day_chunks, month_chunks, year_chunks}, 10);

    std::cout << table->ToString();
    // (Doc section: Table)

    // (Doc section: Ret)

    return arrow::Status::OK();
}

arrow::Status RunMain() {
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
    std::shared_ptr<arrow::Schema> partition_schema = arrow::schema({arrow::field("a", arrow::utf8())});
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

//----------READ WRITED INFORMATION--------------------------

    std::cerr << "~~~~~~~~~~~~~~~~~READING~~~~~~~~~~~~~~~~~~~" << std::endl;
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./" + write_options.base_dir + "/a=0/part0.parquet"));

    // Open Parquet file reader
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    // Read entire file as a single Arrow table
    std::shared_ptr<arrow::Table> table_readed;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

    std::cerr << table_readed->ToString();
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
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