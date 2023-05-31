#pragma once 


#include "../duckdb.hpp"

void DebugDuckDB() {
    duckdb::DuckDB db("../$Databases/example.duckdb");
    duckdb::Connection con(db);
    return;
    // create a table
    con.Query(
        std::string("CREATE TABLE example_table(LR INTEGER, FR INTEGER, C1R INTEGER, C2L INTEGER,") +
        std::string("C3F INTEGER, C4R INTEGER, C5L INTEGER, C6F INTEGER)")
        );
    
    // insert three rows into the table
    con.Query(
        std::string("INSERT INTO example_table VALUES") +
        std::string(" (0, 1, 2, 3, 4, 5, 6, 7),") +
        std::string(" (7, 6, 5, 4, 3, 2, 1, 0),") +
        std::string(" (1, 0, 1, 0, 1, 0, 1, 0)")
        );

    //con.GetSubstraitJSON в какой-то момент не работал без этого
    con.Query("INSTALL substrait");
    con.Query("LOAD substrait");

    /* std::cerr << con.GetSubstraitJSON("SELECT * FROM example_table") << std::endl
              << std::endl << std::endl; */
    
    std::unique_ptr<duckdb::MaterializedQueryResult> result = con.Query("SELECT * FROM example_table");
    std::cerr << result->ToString() << std::endl
              << std::endl << std::endl;

    // con.Query("EXPORT DATABASE '../$Databases/' (FORMAT PARQUET)");
}