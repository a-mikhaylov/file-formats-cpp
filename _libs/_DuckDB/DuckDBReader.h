#pragma once

#include "duckdb.hpp"

class DuckDBReader {
    duckdb::DuckDB* db;
    duckdb::Connection* con;
    duckdb::Appender* appender;
    
    std::string table_name;
    std::vector<std::string> col_names;
    int col_count;
public:

    void Init(std::vector<std::string> _col_names = {}) {
        col_names = _col_names;
        col_count = col_names.size();

        std::string init_str = "CREATE TABLE " + table_name + "(";
        for (int i = 0; i < col_names.size(); ++i)
            init_str = init_str + col_names[i] + " INTEGER, ";
        init_str.erase(init_str.end() - 2); init_str.push_back(')');
        
        con->Query(init_str);
        appender = new duckdb::Appender(*con, table_name);
    }

    DuckDBReader(std::vector<std::string> _col_names = {}, std::string _table_name = "ecg")
    {
        table_name = _table_name;
        db = new duckdb::DuckDB("../$Databases/" + table_name + ".duckdb");
        con = new duckdb::Connection(*db);
        Init(_col_names);
    }

    ~DuckDBReader() {
        delete db;
        delete con;
        delete appender;
    }
};
