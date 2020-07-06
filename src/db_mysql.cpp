#include "db_mysql.h"
#include <utility>
#include <iostream>
#include <glog/logging.h>
DBMysql* g_db_mysql = new DBMysql();

DBMysql::DBMysql()
{
}

DBMysql::~DBMysql()
{
}

bool DBMysql::openDB(const json& json_connect)
{

    if (mysql_init(&mysql_) == NULL)
    {
        return false;
    }

    std::string url = json_connect["url"].get<std::string>();
    std::string user = json_connect["user"].get<std::string>();
    std::string pass = json_connect["pass"].get<std::string>();
    int port = json_connect["port"].get<int>();
    std::string db = json_connect["db"].get<std::string>();

    if (!mysql_real_connect(&mysql_, url.c_str(), user.c_str(),
                pass.c_str(), db.c_str(),
                port, NULL, 0))
    {
        std::string error= mysql_error(&mysql_);
        LOG(ERROR) << "open DB  FAIL: " <<error;
        return false;
    }

    return true;
}

void DBMysql::refreshDB(const std::string& sql)
{

    int ret = mysql_real_query(&mysql_, sql.c_str(), strlen(sql.c_str()));
    if (ret != 0 && mysql_errno(&mysql_) != 1062)
    {
        LOG(ERROR) << "SQL fail: " << mysql_error(&mysql_);
        LOG(ERROR) << "exec sql failed" << sql ;
    }

}

bool DBMysql::getData(const std::string& sql,  std::map<int, DataType> col_type ,json& json_data) 
{

    int ret = mysql_real_query(&mysql_, sql.c_str(), strlen(sql.c_str()));
    if (ret != 0)
    {
        LOG(ERROR) << "exec sql: " << sql << " fail: " << mysql_errno(&mysql_) << " " << mysql_error(&mysql_);
        return false;
    }

    MYSQL_RES *result = mysql_store_result(&mysql_);
    size_t num_rows = mysql_num_rows(result);

    LOG(INFO) << "data size: " << num_rows;
    int col_size = col_type.size();

    for (size_t i = 0; i < num_rows; ++i)
    {
        MYSQL_ROW row = mysql_fetch_row(result);
        json row_data = json::array();

        for (size_t j = 0; j < col_size; j++)
        {
            std::string sql_data = row[j];
            DataType type  = col_type[j];	
            if ( INT == type )
            {
                int real_data = std::stoi(sql_data);
                row_data.push_back(real_data);
            }
            else if ( DOUBLE == type )
            {
                double real_data = std::stof(sql_data);
                row_data.push_back(real_data);
            }
            else if ( STRING == type )
            {
                row_data.push_back(sql_data);
            }
        }
        json_data.push_back(row_data);
    }
    mysql_free_result(result);

    return true;
}

void DBMysql::batchRefreshDB(const std::vector<std::string>& vect_sql)
{
    mysql_query(&mysql_,"START TRANSACTION");
    for(uint32_t i = 0; i < vect_sql.size(); i ++)
    {
        refreshDB(vect_sql.at(i));
    }
    mysql_query(&mysql_,"COMMIT");
}

void DBMysql::closeDB()
{
    mysql_close(&mysql_);
}
