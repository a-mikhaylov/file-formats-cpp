#pragma once

#include "../settings.h"
#include "Markup.h"

void MarkupExample() {
    // создание БД - на входе имя таблицы. если есть уже такой файл - открываем его
    Markup mrk{"test1"};
    nlohmann::json j;
    std::ifstream inp("../_data/_markup/example.json");
    inp >> j;

    // инициализация через CSV файл - имена колонок и их типы
    mrk.InitCSV("../_data/_markup/qrs-header.csv");

    // чтение готовой таблицы из .csv файла в .duckdb (и сразу можем работать с ним)
    mrk.ParseCSV("../_data/_markup/qrs_small.csv");
    
    // в 7-й записи разметки изменяем поле "timeQ" и присваиваем ему значение -888
    mrk.EditCell(7, "timeQ", -888);

    // добавляем в конец строку с данными из json-а (одиночная) 
    mrk.AddRow(j[0]);

    // добавляет сразу любое количество строк, используя json-файл
    mrk.AddJson("../_data/_markup/example.json");

    // вывод на экран текущей таблички    
    mrk.PrintCurrentDB();
}
