#pragma once 

#include "../duckdb.hpp"

void PrintDB(duckdb::Connection& con, std::string tablename) {
    std::cerr << "{" << tablename << "}" << std::endl;
    std::unique_ptr<duckdb::MaterializedQueryResult> result = 
        con.Query("SELECT * FROM " + tablename);
    std::cerr << result->ToString() << std::endl << std::endl;
}

// Пример работы с БД, создание + заполнение
void DebugDuckDB() {
    std::cerr << "[DEBUG]: Start" << std::endl << std::endl;

    duckdb::DuckDB db(nullptr); //"../$Databases/example.duckdb"
    duckdb::Connection con(db);

    // create a table
    con.Query(
        std::string("CREATE TABLE example1(NUM INTEGER, LR INTEGER, FR INTEGER, C1R INTEGER,") +
        std::string(" C2L INTEGER, C3F INTEGER, C4R INTEGER, C5L INTEGER, C6F INTEGER)")
        );
    
    // insert three rows into the table
    con.Query(
        std::string("INSERT INTO example1 VALUES") +
        std::string(" (0, 0, 1, 2, 3, 4, 5, 6, 7),") +
        std::string(" (1, 7, 6, 5, 4, 3, 2, 1, 0),") +
        std::string(" (2, 1, 0, 1, 0, 1, 0, 1, 0),") +
        std::string(" (3, 3, 3, 3, 3, 3, 3, 3, 3),") +
        std::string(" (4, 5, 0, 4, 0, 3, 0, 2, 0),") +
        std::string(" (5, 6, 6, 6, 6, 6, 6, 6, 6)")
        );

    //con.GetSubstraitJSON в какой-то момент не работал без этого
    con.Query("INSTALL substrait");
    con.Query("LOAD substrait");

    /* std::cerr << con.GetSubstraitJSON("SELECT * FROM example1") << std::endl
              << std::endl << std::endl; */
    
    PrintDB(con, "example1");

    std::unique_ptr<duckdb::MaterializedQueryResult> result = 
        con.Query("SELECT * FROM example1 WHERE 4<=NUM AND NUM<8");
    std::cerr << result->ToString() << std::endl << std::endl;
    std::cerr << result->ColumnCount() << " " << result->RowCount() << std::endl;

    std::cerr << "[DEBUG]: End" << std::endl;
}
// Пример работы со связующей таблицей
void LinkTableDuckDB() {
    std::cerr << "[DEBUG]: Start" << std::endl << std::endl;

    duckdb::DuckDB db(nullptr); //"../$Databases/example.duckdb"
    duckdb::Connection con(db);
    
    std::string name_1    = "table_1";
    std::string name_2    = "table_2";
    std::string name_link = "table_link";

//---Заполнение первой таблицы----------------------------------------------------
    con.Query(
        std::string("CREATE TABLE " + name_1 + "(NUM INTEGER, PARAM1 INTEGER, ") +
        std::string("PARAM2 INTEGER, PARAM3 INTEGER, PARAM4 INTEGER, PARAM5 INTEGER)")
        );
    
    con.Query(
        std::string("INSERT INTO " + name_1 + " VALUES") +
        std::string(" (0, 0, 1, 2, 3, 4),") +
        std::string(" (1, 7, 6, 5, 4, 3),") +
        std::string(" (2, 1, 0, 1, 0, 1),") +
        std::string(" (3, 3, 3, 3, 3, 3),") +
        std::string(" (4, 5, 0, 4, 0, 3),") +
        std::string(" (5, 6, 6, 6, 6, 6)")
        );
    PrintDB(con, name_1);

//---Заполнение второй таблицы----------------------------------------------------
    con.Query(
        std::string("CREATE TABLE " + name_2 + "(NUM INTEGER, INFO1 INTEGER, ") +
        std::string("INFO2 INTEGER)")
        );
    
    con.Query(
        std::string("INSERT INTO " + name_2 + " VALUES") +
        std::string(" (0, 0, 1),") +
        std::string(" (1, 7, 6),") +
        std::string(" (2, 1, 0),") +
        std::string(" (3, 3, 3),") +
        std::string(" (4, 5, 0),") +
        std::string(" (5, 6, 6)")
        );
    PrintDB(con, name_2);

//---Заполнение связующей таблицы--------------------------------------------------

    con.Query(
        std::string("CREATE TABLE " + name_link + "(NUM INTEGER, TABLE1 INTEGER, ") +
        std::string("TABLE2 INTEGER)")
        );
    
    con.Query(
        std::string("INSERT INTO " + name_link + " VALUES") +
        std::string(" (0, 0, 5),") +
        std::string(" (1, 1, 4),") +
        std::string(" (2, 2, 3),") +
        std::string(" (3, 3, 2),") +
        std::string(" (4, 4, 1),") +
        std::string(" (5, 5, 0)")
        );
    PrintDB(con, name_link);

    std::cerr << "[DEBUG]: End" << std::endl;
}