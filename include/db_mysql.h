#ifndef DB_MYSQL_H
#define DB_MYSQL_H

#include <string>
#include <vector>
#include <mysql/mysql.h>
#include "json.hpp"

using json = nlohmann::json;

class DBMysql
{
public:
    DBMysql();
    ~DBMysql();


    enum DataType
    {
        INT = 1,
        DOUBLE = 2,
        STRING =3
    };


public:	
    bool openDB(const json& json_connect);

    void closeDB();


    void refreshDB(const std::string& sql);

    void batchRefreshDB(const std::vector<std::string>& vect_sql);
    
    bool getData(const std::string& sql, std::map<int, DataType> col_type, json& json_data) ;
private:
    MYSQL mysql_;
};

extern DBMysql* g_db_mysql;

#endif
