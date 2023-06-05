#pragma once

#include "duckdb.hpp"

class DuckDBReader {
    duckdb::DuckDB* db;
    duckdb::Connection* con;
    
    std::string table_name;
    // std::vector<std::string> col_names;
    int col_count;

    int point_num = 0;      //точка, с которой начнём читать в следующий раз
    int read_quant = 0;     //количество точек чтения

    void PrepareData(std::vector<std::vector<int32_t>>& dat) {
        if (dat.size() == col_count && dat[0].size() == read_quant)
            return;
        
        dat.resize(col_count);
        for (int i = 0; i < col_count; ++i)
            dat[i].resize(read_quant);
    }

public:

    DuckDBReader(std::string _table_name, int _read_quant) 
    {
        read_quant = _read_quant;
        table_name = _table_name;
        db = new duckdb::DuckDB(duckdb_settings::DATA_DIR + table_name + ".duckdb");
        con = new duckdb::Connection(*db);
        col_count = con->Query("SELECT * FROM " + table_name)->ColumnCount() - 1;
    
        std::cerr << "col_count = " << col_count << std::endl;
    }

    ~DuckDBReader() {
        delete db;
        delete con;
    }

    //dat = [канал][точка]
    bool Read(std::vector<std::vector<int32_t>>& dat) {
        bool res = true;
        PrepareData(dat);
        
        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name +
                       " WHERE " + std::to_string(point_num) + 
                       "<=NUM AND NUM<" + std::to_string(point_num + read_quant));

        if (result->RowCount() == 0)
            return false;

        for (int i = 1; i < result->ColumnCount() && (i - 1) < dat.size(); ++i) {
           for (int j = 0; j < read_quant; ++j) { 
                //выдало меньше строк, чем read_quant (конец записи) 
                if (j >= result->RowCount()) {
                    dat[i - 1].erase(dat[i - 1].begin() + j, dat[i - 1].end());
                    break;    
                }

                dat[i - 1][j] = result->GetValue<int32_t>(i, j);
                
                //считаем, что все столбцы одинакого размера =>  
                //увеличиваются на одинаковое значение
                if (i == 1) 
                    ++point_num;
            }
        }

        return res;
    }

    void PrintCurrentDB() {
        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name);
        result->Print();
    }
};
