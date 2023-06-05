#pragma once 


#include "../duckdb.hpp"

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
    
    std::unique_ptr<duckdb::MaterializedQueryResult> result = 
        con.Query("SELECT * FROM example1");
    std::cerr << result->ToString() << std::endl << std::endl;

    result = con.Query("SELECT * FROM example1 WHERE 4<=NUM AND NUM<8");
    std::cerr << result->ToString() << std::endl << std::endl;
    std::cerr << result->ColumnCount() << " " << result->RowCount() << std::endl;

    std::cerr << "[DEBUG]: End" << std::endl;
}