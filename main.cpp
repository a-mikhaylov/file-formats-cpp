#include <iostream>
// #include "_libs/for_fields_macro.h"
//work with bin and hdr files 
#include "_libs/_hdr-bin/hdr.h"
#include "_libs/_hdr-bin/bin.h"
//Arrow Parquet
#include "_libs/_ArrowParquet/Examples/parquet_test.h"
#include "_libs/_ArrowParquet/ArrowTest.h"
//DuckDB
#include "_libs/_DuckDB/Examples/example.h"
#include "_libs/_DuckDB/DuckDBTest.h"
//работа с разметкой
#include "_libs/_Markup/Markup.h"
#include "_libs/_Markup/example.h"
//HDF5
// #include "_libs/_HDF5/hdf5-test.h"   //в последнюю очередь

int main() {
   MarkupExample();
}