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
    // создание БД - на входе имя таблицы. если есть уже такой файл - открываем его
    Markup mrk{"test0"};

    // инициализация через CSV файл - имена колонок и их типы
    mrk.InitCSV("../_data/_markup/qrs-header.csv");

    // чтение готовой таблицы из .csv файла в .duckdb (и сразу можем работать с ним)
    mrk.ParseCSV("../_data/_markup/qrs_small.csv");
    
    // в 7-й записи разметки изменяем поле "timeQ" и присваиваем ему значение -888
    mrk.EditCell(7, "timeQ", -888);

    // вывод на экран текущей таблички    
    mrk.PrintCurrentDB();
}