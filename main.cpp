#include <iostream>
//work with bin and hdr files 
#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"
//Arrow Parquet
#include "_libs/_ArrowParquet/Examples/parquet_test.h"
#include "_libs/_ArrowParquet/ArrowTest.h"
//DuckDB
#include "_libs/_DuckDB/duckdb.hpp"
#include "_libs/_DuckDB/Examples/example.h"
#include "_libs/_DuckDB/DuckDBWriter.h"
#include "_libs/_DuckDB/DuckDBReader.h"
//HDF5
// #include "_libs/_HDF5/hdf5-test.h"   //в последнюю очередь

int main() {
    std::string table_name = "duckdb_test3";
    int QUANT = 1000;
    std::vector<std::vector<int32_t>> dat = 
        {
            { 1, 2, 3, 4 },
            { 3, 2, 1, 0 },
            { 100, 200, 300, 400 },
            { 100, 225, 350, 475},
            { 0, 0, 0, 0},
            { 1, 11, 111, 1111},
            { 5, 55, 55, 5}
        };
    std::vector<std::vector<int32_t>> dat_1(QUANT);
    for (int i = 0; i < dat_1.size(); ++i)
        dat_1[i].resize(dat[0].size());
    
    {
        BinReader BRead("../_data/small_8x1e6", QUANT);
        DuckDBWriter DBWr({ "LR", "FR", "C1R", "C2L", 
                            "C3F", "C4R", "C5L", "C6F"}, 
                            "test1");

        while (BRead.Read(dat_1)) {
            DBWr.Write(dat_1);
        }
    }

    dat_1.resize(QUANT);
    for (int i = 0; i < dat_1.size(); ++i)
        dat_1[i].resize(8);

    {
        BinWriter BWr("../$Databases/smalltest1.bin");
        DuckDBReader DBRe("test1");

        while(DBRe.Read(dat_1))
            BWr.Write(dat_1);
        BWr.Write(dat_1);
    }

    return 0;
}