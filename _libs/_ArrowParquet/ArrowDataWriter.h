#pragma once

#include "include_base_exmpl.h"
#include "careate_write_iterative.h"
#include <boost/filesystem.hpp>

#define PARQUET_DATA_DIR   std::string("DataWriter_RESULT")
#define PARQUET_DATA_FNAME std::string("/Written-ZSTD.parquet")//std::string("/ecg.parquet")

using parquet::Encoding;

//пока что работает только с int32_t (несложно шаблонизировать)
class ArrowDataWriter {
    int POINTS_WRITED = 0;

    std::shared_ptr<arrow::fs::FileSystem>         filesystem;
    std::unique_ptr<parquet::arrow::FileWriter>    file_writer;
    std::shared_ptr<::arrow::io::FileOutputStream> output;
    
    std::shared_ptr<arrow::Schema> schema;
    arrow::NumericBuilder<arrow::Int32Type> builder;
    std::vector<std::shared_ptr<arrow::Array>> arrays;

    std::string base_path;
    std::string data_fname;

    int32_t** data = nullptr;

    arrow::Status PrepareFS() {
        std::string setup_path(boost::filesystem::current_path().c_str());
        
        // if (result == NULL)
            // return arrow::Status::IOError("Fetching PWD failed.");

        ARROW_ASSIGN_OR_RAISE(filesystem, arrow::fs::FileSystemFromUriOrPath(setup_path.c_str()));
        
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

    std::vector<int32_t> Transparent(int32_t** arr, int chan_num, int points_cnt) {
        std::vector<int32_t> res(points_cnt);
        for (int i = 0; i < points_cnt; ++i) {
            if (arr[i][0] == INT32_MAX && arr[i][1] == INT32_MAX) {
                res.resize(i -1);
                return res;
            }
            
            res[i] = arr[i][chan_num];
        }
        return res;
    }

    //превращает числовой вектор в табличку, которую можно будет записать
    // data = [ кол-во_каналов x кол-во точек ]
    std::shared_ptr<arrow::Table> MakeTable(std::vector<std::vector<int32_t>>& data) {
        if(!PrepareData(data))
            return nullptr;

        // arrays.clear();
        if (arrays.size() != data.size()) 
            arrays.resize(data.size());

        for (int i = 0; i < arrays.size(); ++i) {
            builder.AppendValues(data[i]);
            builder.Finish(&arrays[i]);
            builder.Reset();
        }

        return arrow::Table::Make(schema, arrays);
    }

    // data = [ кол-во точек x кол-во_каналов ]
    std::shared_ptr<arrow::Table> MakeTable(int32_t** data, int points_count, int channels_count) {

        if (arrays.size() != channels_count) 
            arrays.resize(channels_count);

        for (int i = 0; i < channels_count; ++i) {
            builder.AppendValues(Transparent(data, i, points_count));
            builder.Finish(&arrays[i]);
            builder.Reset();
        }

        return arrow::Table::Make(schema, arrays);
    }

public:
    //инициализация файловой системы, настроек, потока вывода, пути вывода
    arrow::Status Init(
        const std::string& root_path, const std::string& DATA_DIR, 
        const std::string& DATA_FNAME, std::shared_ptr<arrow::Schema>& rule_schema,
        arrow::Compression::type CompressType) //arrow::Compression::UNCOMPRESSED
    {
        ARROW_RETURN_NOT_OK(PrepareFS());

        base_path  = root_path + DATA_DIR;
        data_fname = DATA_FNAME;
        schema  = rule_schema;

        ARROW_RETURN_NOT_OK(filesystem->CreateDir(base_path));
        
        ARROW_ASSIGN_OR_RAISE(output, arrow::io::FileOutputStream::Open(base_path + data_fname));

        std::shared_ptr<WriterProperties> props = WriterProperties::Builder()
            .version(ParquetVersion::PARQUET_2_6)
            ->data_page_version(ParquetDataPageVersion::V2)
            ->compression(CompressType)
            // ->compression_level(1)
            ->encoding(Encoding::DELTA_BINARY_PACKED)
            ->build();
        
        

        ARROW_ASSIGN_OR_RAISE(
            file_writer, 
            parquet::arrow::FileWriter::Open(*schema,
                arrow::default_memory_pool(), 
                output,
                props, 
                parquet::default_arrow_writer_properties()
                )
        );

        return arrow::Status::OK();
    }

    ArrowDataWriter(
        const std::string& root_path, const std::string& DATA_DIR, 
        const std::string& DATA_FNAME,std::shared_ptr<arrow::Schema>& rule_schema,
        arrow::Compression::type CompressType) //arrow::Compression::UNCOMPRESSED 
    {
        Init(root_path, DATA_DIR, DATA_FNAME, rule_schema, CompressType);
    }

    ArrowDataWriter() {}

    ~ArrowDataWriter() {
        std::cerr << "!--Points Writed: " << POINTS_WRITED << std::endl;
    }

    std::string getFileName() {
        return base_path + data_fname;
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

    arrow::Status Write(int32_t** data, int x_length, int y_length, int64_t chunk_size = 67108864L) {
        ARROW_RETURN_NOT_OK(Write(*MakeTable(data, x_length, y_length).get(), chunk_size));
        // POINTS_WRITED += data[0].size();
        return arrow::Status::OK();
    }
};

