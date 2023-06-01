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

    std::vector<std::vector<int32_t>> dat_1(2);
    for (int i = 0; i < dat_1.size(); ++i) {
        dat_1[i].resize(4);
    }

    // DebugDuckDB();
    // {
    //     DuckDBWriter DBWr({"A", "B", "C" ,"D"}, table_name);
    //     DBWr.Write(dat);
    // }

    DuckDBReader DBRe(table_name);

    while (DBRe.Read(dat_1))
        prqt_settings::PrintVector("Read", dat_1);
    prqt_settings::PrintVector("Read", dat_1);
    return 0;
}