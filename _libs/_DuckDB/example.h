#pragma once 

#include "duckdb.hpp"

class DuckDBWriter {

};

class DuckDBReader {

};

void DebugDuckDB() {
    duckdb::DuckDB db(nullptr);
    duckdb::Connection con(db);

    // create a table
    con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
    
    // insert three rows into the table
    con.Query("INSERT INTO integers VALUES (3, 4), (5, 6), (7, NULL)");

    con.Query("INSTALL substrait");
    con.Query("LOAD substrait");

    std::cerr << con.GetSubstraitJSON("SELECT * FROM integers") << std::endl;

    // std::unique_ptr<duckdb::MaterializedQueryResult> result = con.Query("SELECT * FROM integers");
    // if (!result->success) {
        // std::cerr << result->error;
    // }

    // con.
}