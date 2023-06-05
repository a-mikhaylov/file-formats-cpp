#include <iostream>
//work with bin and hdr files 
#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"
//Arrow Parquet
#include "_libs/_ArrowParquet/Examples/parquet_test.h"
#include "_libs/_ArrowParquet/ArrowTest.h"
//DuckDB
#include "_libs/_DuckDB/Examples/example.h"
#include "_libs/_DuckDB/DuckDBWriter.h"
#include "_libs/_DuckDB/DuckDBReader.h"
#include "_libs/_DuckDB/DuckDBTest.h"
//HDF5
// #include "_libs/_HDF5/hdf5-test.h"   //в последнюю очередь

int main() {
    Log test_Log("../Logs/LogEncode.csv"); //debug_set::LOG_FILE

    std::vector<int> Quants = {
          1024,      //2^10
          1024*8,    //2^13
          1024*8*4,  //2^15
          1024*8*4*4 //2^17
        };

    // std::string table_name = "duckdb_test6";
    int QUANT = 1024;
    std::vector<std::vector<int32_t>> dat_1(QUANT);

    duckdb_test::Test1_write(test_Log, Quants);
    /* {
        BinReader BRead("../_data/small_8x1e6", QUANT);
        DuckDBWriter DBWr({ "LR", "FR", "C1R", "C2L", 
                            "C3F", "C4R", "C5L", "C6F"}, 
                            "Debug4");

        while (BRead.Read(dat_1))
            DBWr.Write(dat_1);
    
    } */

    duckdb_test::Test2_read(test_Log, Quants);//
    /* {
        BinWriter BWr("../$Databases/Debug4.bin");
        DuckDBReader DBRe("Debug4", QUANT);

        while(DBRe.Read(dat_1))
            BWr.Write(dat_1);
    } */

    return 0;
}