#pragma once
//https://arrow.apache.org/docs/cpp/parquet.html
#include "include_base_exmpl.h"
#include "example_with_slice.h"
#include "create_write_once.h"
#include "careate_write_iterative.h"

#include "ArrowDataWriter.h"
#include "ArrowDataReader.h"

arrow::Status RunMain() {
    int QUANT = 100000;     //сколько точек читать-писать за раз

    std::vector<std::vector<int32_t>> dat = { {}, {}, {}, {}, {}, {}, {}, {} };

    for (int i = 0; i < QUANT; ++i) {
        for (int j = 0; j < dat.size(); ++j) 
            dat[j].push_back(0);
    }

    BinReader BReader{BIN_HDR_PATH, QUANT};

    /* BReader._TestRun(5, dat);    //проверка работоспособности getData
    return arrow::Status::OK(); */

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

    ArrowDataWriter ADWriter{"", "ArrowParquet_RESULT", "/PX1447191017125822-snappy.parquet",
                             schema, arrow::Compression::SNAPPY};

    while(BReader.getData(dat))
        ADWriter.Write(dat, 2048);

    return arrow::Status::OK();
}

int _parquetMain() {
    arrow::Status st = RunMain_Iterative();
    if (!st.ok()) {
        std::cerr << st << std::endl;
        return 1;
    }
    std::cerr << "succesful ending !!" << std::endl;
    return 0;
}