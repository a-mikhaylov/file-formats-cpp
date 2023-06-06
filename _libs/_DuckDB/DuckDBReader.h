#pragma once

#include "duckdb.hpp"

class DuckDBReader {
    duckdb::DuckDB* db;
    duckdb::Connection* con;
    
    std::string table_name;
    std::string file_name;
    int col_count;

    int point_num = 0;      //точка, с которой начнём читать в следующий раз
    int read_quant = 0;     //количество точек чтения

    std::unique_ptr<duckdb::MaterializedQueryResult> result;

    void PrepareData(std::vector<std::vector<int32_t>>& dat, int rows = 0) {
        if (rows == 0)
            rows = read_quant;
        
        if (dat.size() == col_count && dat[0].size() == rows)
            return;
        
        dat.resize(col_count);
        for (int i = 0; i < col_count; ++i)
            dat[i].resize(rows);
    }

public:

    DuckDBReader(std::string _table_name, int _read_quant) 
    {
        read_quant = _read_quant;
        table_name = _table_name;
        file_name = duckdb_settings::DATA_DIR + table_name + ".duckdb";
        db = new duckdb::DuckDB(file_name);
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
        
        result = con->Query(
                       "SELECT * FROM " + table_name +
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

    bool Read(std::vector<std::vector<int32_t>>& dat, std::pair<int, int> toRead) {
        bool res = true;
        
        result = con->Query(
                "SELECT * FROM " + table_name +
                " WHERE " + std::to_string(toRead.first) + 
                "<=NUM AND NUM<" + 
                std::to_string(toRead.first + toRead.second)
                );

        if (result->RowCount() == 0)
            return false;

        PrepareData(dat, result->RowCount());

        for (int i = 1; i < result->ColumnCount() && (i - 1) < dat.size(); ++i) {
           for (int j = 0; j < result->RowCount(); ++j) { 
                dat[i - 1][j] = result->GetValue<int32_t>(i, j);
            }
        }

        return res;
    }

    void PrintCurrentDB() {
        result = con->Query("SELECT * FROM " + table_name);
        result->Print();
    }
};
