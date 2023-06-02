#pragma once

#include "duckdb.hpp"

class DuckDBWriter {
    duckdb::DuckDB* db;
    duckdb::Connection* con;
    duckdb::Appender* appender;
    
    std::string table_name;
    std::vector<std::string> col_names;
    int col_count;

    int point_num = 0; //точка, в которую будем писать в следующий раз

    //создаёт строку для SQL-запроса с добавлением данной строки
    std::string MakeInsertString(std::vector<int32_t>& row) {
        std::string res = " (";
        for (int i = 0; i < col_count; ++i)
            res = res + std::to_string(row[i]) + ", ";
        res.erase(res.end() - 2); res += "),";
        
        return res;
    }
public:

    void Init(std::vector<std::string> _col_names = {}) {
        col_names = _col_names;
        col_count = col_names.size();

        std::string init_str = "CREATE TABLE " + table_name + 
                               "(NUM INTEGER, ";
        for (int i = 0; i < col_names.size(); ++i)
            init_str = init_str + col_names[i] + " INTEGER, ";
        init_str.erase(init_str.end() - 2); init_str.push_back(')');
        
        con->Query(init_str);
        appender = new duckdb::Appender(*con, table_name);
    }

    DuckDBWriter(std::vector<std::string> _col_names = {}, 
                 std::string _table_name = "ecg")
    {
        table_name = _table_name;
        db = new duckdb::DuckDB("../$Databases/" + table_name + ".duckdb");
        con = new duckdb::Connection(*db);
        Init(_col_names);
    }

    ~DuckDBWriter() {
        delete db;
        delete con;
        delete appender;
    }

    //Запись с помощью встроенной команды Insert
    //Корректность: не проверена
    //Скорость: не проверена
    void WriteInsert(std::vector<std::vector<int32_t>>& data) {
        std::string insert_command = "INSERT INTO " + table_name + " VALUES";
        for (int i = 0; i < data.size(); ++i)
            insert_command += MakeInsertString(data[i]);
        insert_command.pop_back();

        con->Query(insert_command);
        con->Query("EXPORT DATABASE '../$Databases/'");

        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name);
        std::cerr << result->ToString() << std::endl
                  << std::endl << std::endl;
    }

    //Запись с помощью duckdb::Appender
    //Корректность: не проверена
    //Скорость: не проверена
    //data = [канал][точка]
    bool Write(std::vector<std::vector<int32_t>>& data) {
        if (data.size() == 0)
            return false;
        
        for (int i = 0; i < data[0].size(); ++i) {
            appender->BeginRow();
            appender->Append<int32_t>(point_num++);
            for (int j = 0; j < col_count && j < data.size(); ++j)
                appender->Append<int32_t>(data[j][i]);
            appender->EndRow();
        }
        appender->Flush();

        // PrintCurrentDB();
        return true;
    }

    void PrintCurrentDB() {
        std::unique_ptr<duckdb::MaterializedQueryResult> result = 
            con->Query("SELECT * FROM " + table_name);
        std::cerr << result->ToString() << std::endl
                  << std::endl << std::endl;
    }
};