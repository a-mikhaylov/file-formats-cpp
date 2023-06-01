#pragma once

#include "duckdb.hpp"

class DuckDBReader {
    duckdb::DuckDB* db;
    duckdb::Connection* con;
    
    std::string table_name;
    std::vector<std::string> col_names;
    int col_count;

    int point_num = 0;      //точка, с которой начнём читать в следующий раз
    int read_quant = 0;     //пока читаем по размеру входного вектора
public:

    DuckDBReader(std::string _table_name) 
    {
        table_name = _table_name;
        db = new duckdb::DuckDB("../$Databases/" + table_name + ".duckdb");
        con = new duckdb::Connection(*db);

        PrintCurrentDB();
    }

    ~DuckDBReader() {
        delete db;
        delete con;
    }

    //dat = [кол-во строк x кол-во столбцов]
    bool Read(std::vector<std::vector<int32_t>>& dat) {
        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name +
                       " WHERE " + std::to_string(point_num) + 
                       "<=NUM AND NUM<" + std::to_string(dat.size() + point_num));
        // result->Print();

        for (int i = 0; i < dat.size(); ++i) {
            if (i >= result->RowCount()) {
                dat.erase(dat.begin() + i - 1);
                return false;
            }

            for (int j = 1; j < result->ColumnCount() && (j - 1) < dat[i].size(); ++j){
                dat[i][j - 1] = result->GetValue<int32_t>(j, i);
            }
            ++point_num;
        }

        return true;
    }

    void PrintCurrentDB() {
        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name);
        result->Print();
    }
};
