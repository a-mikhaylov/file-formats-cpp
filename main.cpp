#include <iostream>

#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"

#include "_libs/_HDF5/hdf5-test.h"
#include "_libs/_ArrowParquet/parquet_test.h"
#include "_libs/_DuckDB/duckdb.hpp" //

int main() {
    std::cerr << "DuckDB test:" << std::endl << std::endl;
    
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);
    auto result = con.Query("SELECT 42");
    result->Print();
    std::cerr << std::endl << "Finish!" << std::endl;
    // return _parquetMain();//
}