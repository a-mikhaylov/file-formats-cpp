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
//HDF5
// #include "_libs/_HDF5/hdf5-test.h"   //в последнюю очередь

int main() {
    // DebugDuckDB();
    DuckDBWriter DBWr({"A", "B", "C" ,"D"});

    return 0;
}