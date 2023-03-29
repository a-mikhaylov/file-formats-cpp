#pragma once
//https://arrow.apache.org/docs/cpp/parquet.html
#include "include_base_exmpl.h"
#include "example_with_slice.h"
#include "create_write_once.h"
#include "careate_write_iterative.h"

#include "ArrowDataWriter.h"
#include "ArrowDataReader.h"

void PrintVec(std::vector<std::vector<int32_t>>& vec) {
    std::cerr << "VECTOR:~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
    for (int i = 0; i < vec.size(); ++i) {
        std::cerr << "\t" << i << ": { ";
        for (int j = 0; j < vec[i].size(); ++j) {
            std::cerr << vec[i][j] << ", ";
        }
        std::cerr << "\b\b }" << std::endl;
    }
    std::cerr << "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" << std::endl;
}


//Комментарии к данному преобразованию (возможно, ошибки при реализации):
//  - При записи квантами разных размеров - получаются разные по размеру итоговые 
//файлы, содержание ещё не проверено, но первые точки совпадают.
//
//  - При записи без сжатия и QUANT > 1000 (примерно) итоговый файл получается 
//меньше исходного. (676,1 МБ -> 384,1 МБ). Потерь данных при этом не происходит
//
//  - Запись маленькими квантами - очень долгая, куда выгоднее брать QUANT = 10000.
arrow::Status RunMain() {
    int QUANT = 10;     //сколько точек читать-писать за раз

    std::vector<std::vector<int32_t>> dat = { {}, {}, {}, {}, {}, {}, {}, {} };
    for (int i = 0; i < QUANT; ++i) {
        for (int j = 0; j < dat.size(); ++j) 
            dat[j].push_back(0);
    }
    std::vector<std::vector<int32_t>> dat_2 = dat;

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
/*
    //Запись из *.bin в *.parquet
    { 
        BinReader BReader{BIN_HDR_PATH, QUANT};
        ArrowDataWriter ADWriter{"", "ArrowParquet_RESULT", "/PX1447191017125822-uncomp-2.parquet",
                                schema, arrow::Compression::UNCOMPRESSED};
        // while(BReader.Read(dat) && cnt--)
            // ADWriter.Write(dat, 2048);
    }

    //Чтение из *.bin и из *.parquet с целью проверки совпадения данных
    {    
        BinReader BReader{BIN_HDR_PATH, QUANT};
        ArrowDataReader ADReader{"./ArrowParquet_RESULT/PX1447191017125822-uncomp-2.parquet"};

        BReader.Read(dat);
        ADReader.Read(dat_2);

        PrintVec(dat); 
        PrintVec(dat_2);
    }
*/
    //Запись из *.parquet в *.bin (проверка на корректность записи - совпадение хешей)
    {
        ArrowDataReader ADReader{"./ArrowParquet_RESULT/PX1447191017125822-uncomp.parquet"};
        BinWriter       BWriter{"./BIN_DATA_rewrite/PX1447191017125822-uncomp.bin"};
        
        while(ADReader.Read(dat))
            BWriter.Write(dat);

        //хеш файла ./_data/PX<...>.bin и файла ./BIN_DATA_rewrite/PX<...>-uncomp.bin
        //полностью совпадают, то есть запись и чтение из parquet происходят корректно.
    }
    

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