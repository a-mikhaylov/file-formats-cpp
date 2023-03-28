#pragma once

#include "include_base_exmpl.h"
#include "careate_write_iterative.h"

#define PARQUET_DATA_DIR   std::string("DataWriter_RESULT")
#define PARQUET_DATA_FNAME std::string("/Written.parquet")//std::string("/ecg.parquet")

//пока что работает только с int32_t (несложно шаблонизировать)
class ArrowDataWriter {
    int POINTS_WRITED = 0;

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

    //оставляет только допустимое кол-во столбцов и строк в векторе, который хотим записать:
    //кол-во столбцов = кол-ву столбцов в схеме
    //кол-во строк должно быть попарно одинаково у каждого столбца
    bool PrepareData(std::vector<std::vector<int32_t>>& data) {
        int columns_cnt = schema.get()->field_names().size();  //кол-во столбцов, объявленное в схеме
        
        //дали меньше столбцов, чем надо => отказываемся работать, чтобы не заполнять чем попало
        if (data.size() < columns_cnt) {
            std::cerr << "\t- PrepareData() failed" << std::endl;
            return false;
        }

        //откидываем лишние столбцы
        for (int i = data.size(); i > columns_cnt; --i) data.pop_back();

        //ищем самый короткий столбец, чтобы остальные подогнать под него
        //(можно искать самый большой, а остальные дополнять нулями, но так не хочется)
        int min_row_count = 9999999;
        for (int i = 0; i < data.size(); ++i) 
            if (data[i].size() < min_row_count)
                min_row_count = data[i].size();

        for (int i = 0; i < data.size(); ++i)
            data[i].resize(min_row_count);
        
        /* std::cerr << "{" << std::endl;
        for (int i = 0; i < data.size(); ++i) {
            std::cerr << "\t{";
            for (int j = 0; j < data[i].size(); ++j)
                std::cerr << data[i][j] << ", ";
            std::cerr << "}," << std::endl; 
        }
        std::cerr << "}" << std::endl; */

        return true;
    } 

    //превращает числовой вектор в табличку, которую можно будет записать
    std::shared_ptr<arrow::Table> MakeTable(std::vector<std::vector<int32_t>>& data) {
        if(!PrepareData(data))
            return nullptr;

        std::vector<std::shared_ptr<arrow::Array>> arrays(data.size());
        arrow::NumericBuilder<arrow::Int32Type> builder;

        for (int i = 0; i < arrays.size(); ++i) {
            builder.AppendValues(data[i]);
            builder.Finish(&arrays[i]);
            builder.Reset();
        }

        return arrow::Table::Make(schema, arrays);
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

    ArrowDataWriter(
        const std::string& root_path, std::shared_ptr<arrow::Schema>& rule_schema,
        arrow::Compression::type CompressType) //arrow::Compression::UNCOMPRESSED 
    {
        Init(root_path, rule_schema, CompressType);
    }

    ArrowDataWriter() {}

    ~ArrowDataWriter() {
        std::cerr << "!--Points Writed: " << POINTS_WRITED << std::endl;
    }
    //запись данных в текущий файл
    arrow::Status Write(const arrow::Table &table, int64_t chunk_size = 67108864L) {
        ARROW_RETURN_NOT_OK(file_writer->WriteTable(table, chunk_size));

        return arrow::Status::OK();
    }

    arrow::Status Write(std::vector<std::vector<int32_t>>& data, int64_t chunk_size = 67108864L) {
        ARROW_RETURN_NOT_OK(Write(*MakeTable(data).get(), chunk_size));
        POINTS_WRITED += data[0].size();
        return arrow::Status::OK();
    }
};
//-------------------------------------------------------------------------

arrow::Status RunMain_Real() {
    int QUANT = 100000;

    std::vector<std::vector<int32_t>> dat = { {}, {}, {}, {}, {}, {}, {}, {} };

    for (int i = 0; i < QUANT; ++i) {
        for (int j = 0; j < dat.size(); ++j) 
            dat[j].push_back(0);
    }

    BinReader BReader{BIN_HDR_PATH, QUANT};

    /* BReader._TestRun(5, dat);    //проверка работоспособности getData
    return arrow::Status::OK(); */

    {//Arrow Writer//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        std::shared_ptr<arrow::Schema> schema = arrow::schema(
            {
                arrow::field("LR",  arrow::int32()),
                arrow::field("FR",  arrow::int32()),
                arrow::field("C1R", arrow::int32()),
                arrow::field("C2L", arrow::int32()),
                arrow::field("C3F", arrow::int32()),
                arrow::field("C4R", arrow::int32()),
                arrow::field("C5L", arrow::int32()),
                arrow::field("C6F", arrow::int32()),
            });

        ArrowDataWriter ADWriter{"", schema, arrow::Compression::UNCOMPRESSED};
        int cnt = 0;
        while(BReader.getData(dat)/*  && cnt++ < 5 */) {
            ADWriter.Write(dat, 2048);
            /* std::cerr << dat.size() << std::endl;
            for (int i = 0; i < dat.size(); ++i) 
                std::cerr << "\td[" << i << "]: " << dat[i].size() << std::endl;
            std::cerr << "--------------------------------" << std::endl; */
        }
    }


    return arrow::Status::OK();
//Reading for debug//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    std::cerr << "StartReading" << std::endl;
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
    
    return arrow::Status::OK();
}
