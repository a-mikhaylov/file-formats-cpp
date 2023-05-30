#pragma once

#include "duckdb.hpp"

class DuckDBWriter {
    duckdb::DuckDB db;
    duckdb::Connection con;
    
    std::vector<std::string> col_names;
public:

    void Init(std::vector<std::string> _col_names = {}) {
        col_names = _col_names;

        std::string init_str = "CREATE TABLE integers(";
        for (int i = 0; i < col_names.size(); ++i)
            init_str = init_str + col_names[i] + " INTEGER, ";
        init_str = init_str + "\b\b)";

        con.Query(init_str);
    }

    DuckDBWriter(std::vector<std::string> _col_names = {}) : 
        db(nullptr), con(db) 
    {
        Init(_col_names);
    }

    void Write() {

    }
};