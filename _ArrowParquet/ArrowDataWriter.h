#pragma once

#include "include_base_exmpl.h"
#include "careate_write_iterative.h"

#define PARQUET_DATA_DIR   std::string("DataWriter_RESULT")
#define PARQUET_DATA_FNAME std::string("/Written.parquet")//std::string("/ecg.parquet")

class ArrowDataWriter {
    std::shared_ptr<arrow::fs::FileSystem>         filesystem;
    std::unique_ptr<parquet::arrow::FileWriter>    file_writer;
    std::shared_ptr<::arrow::io::FileOutputStream> output;
    
    std::shared_ptr<arrow::Schema> schema;
    
    std::string base_path;

    arrow::Status PrepareFS() {
        char setup_path[256];
        char* result = getcwd(setup_path, 256);
        
        if (result == NULL)
            return arrow::Status::IOError("Fetching PWD failed.");

        ARROW_ASSIGN_OR_RAISE(filesystem, arrow::fs::FileSystemFromUriOrPath(setup_path));
        
        return arrow::Status::OK();
    }

    //оставляет только допустимое кол-во столбцов и строк:
    //кол-во столбцов = кол-ву столбцов в схеме
    //кол-во строк должно быть попарно одинаково у каждого столбца
    bool PrepareData(std::vector<std::vector<int32_t>>& data) {
        int columns_cnt = schema.get()->field_names().size();  //кол-во столбцов, объявленное в схеме
        
        //откидываем лишние столбцы
        for (int i = data.size(); i > columns_cnt; --i) data.pop_back();

        //ищем самый короткий столбец, чтобы остальные подогнать под него
        int min_row_count = 9999999;
        for (int i = 0; i < data.size(); ++i) 
            if (data[i].size() < min_row_count)
                min_row_count = data[i].size();

        return true;
    } 

public:
    //инициализация файловой системы, настроек, потока вывода, пути вывода
    arrow::Status Init(
        const std::string& root_path, std::shared_ptr<arrow::Schema>& rule_schema,
        arrow::Compression::type CompressType) //arrow::Compression::UNCOMPRESSED
    {
        ARROW_RETURN_NOT_OK(PrepareFS());

        base_path = root_path + PARQUET_DATA_DIR;
        schema  = rule_schema;

        ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
        
        ARROW_ASSIGN_OR_RAISE(output, arrow::io::FileOutputStream::Open(base_path + PARQUET_DATA_FNAME));

        std::shared_ptr<WriterProperties> props =
            WriterProperties::Builder().compression(CompressType)->build();

        ARROW_ASSIGN_OR_RAISE(file_writer, parquet::arrow::FileWriter::Open(*schema,
                                            arrow::default_memory_pool(), output,
                                            props, parquet::default_arrow_writer_properties()));

        return arrow::Status::OK();
    }

    arrow::Status Close() {
        // filesystem.~__shared_ptr();
        // file_writer.~unique_ptr();
        // output.~__shared_ptr();
    }

    ArrowDataWriter(
        const std::string& root_path, std::shared_ptr<arrow::Schema>& rule_schema,
        arrow::Compression::type CompressType) //arrow::Compression::UNCOMPRESSED 
    {
        Init(root_path, rule_schema, CompressType);
    }

    ArrowDataWriter() {}

    //запись данных в текущий файл (сделать создание таблицы из вектора чисел)
    arrow::Status Write(const arrow::Table &table, int64_t chunk_size = 67108864L) {
        ARROW_RETURN_NOT_OK(file_writer->WriteTable(table, chunk_size));

        return arrow::Status::OK();
    }

    arrow::Status Write(std::vector<std::vector<int32_t>> data, int64_t chunk_size = 67108864L) {

        return arrow::Status::OK();
    }

    arrow::Status ReadAndShow() {
        std::shared_ptr<arrow::io::RandomAccessFile> input;

        ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./" + PARQUET_DATA_DIR + PARQUET_DATA_FNAME));

        std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
        ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader));

        std::shared_ptr<arrow::Table> table_readed;
        ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

        std::cerr << table_readed->ToString();
    }
};


arrow::Status RunMain_Real() {
{//Arrow Writer//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    std::shared_ptr<arrow::Schema> schema = arrow::schema(
        {
            arrow::field("LR", arrow::int32()),
            arrow::field("FR", arrow::int32()),
            arrow::field("C1R", arrow::int32())
        });

    ArrowDataWriter ADWriter{"", schema, arrow::Compression::UNCOMPRESSED};

    /* std::vector<std::string> v_sch = schema.get()->field_names();

    std::cerr << v_sch.size() << std::endl;
    for (int i = 0; i < v_sch.size(); ++i) {
        std::cerr << v_sch[i] << std::endl;
    } */

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
    
    ADWriter.Write(*table.get(), 2048);

    ADWriter.Close();
}//Arrow Writer//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    // ADWriter.ReadAndShow();
    std::cerr << "StartReading" << std::endl;
    //***********************************************************************
    std::shared_ptr<arrow::io::RandomAccessFile> input;

    ARROW_ASSIGN_OR_RAISE(input, arrow::io::ReadableFile::Open("./" + PARQUET_DATA_DIR + PARQUET_DATA_FNAME));
    
    std::cerr << "\t- Open done" << std::endl;
    
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(parquet::arrow::OpenFile(input, pool, &arrow_reader));

    std::cerr << "\t- Open file done" << std::endl;

    std::shared_ptr<arrow::Table> table_readed;
    ARROW_RETURN_NOT_OK(arrow_reader->ReadTable(&table_readed));

    std::cerr << "\t- Read table done" << std::endl;

    std::cerr << table_readed->ToString();
    //***********************************************************************
    return arrow::Status::OK();
}
