#include "syncer.h"
#include <glog/logging.h>
#include "db_mysql.h"
#include <gmp.h>
#include <gmpxx.h>
std::map<std::string, int> s_map_address_id;
static void SetTimeout(const std::string& name, int second)
{
    struct timeval timeout ;
    timeout.tv_sec = second;
    timeout.tv_usec = 0;
    evtimer_add(Job::map_name_event_[name], &timeout);
}

static void ScanChain(int fd, short kind, void *ctx)
{
    LOG(INFO) << "scan block begin ";
    std::string sql = "SELECT address FROM account;";
    std::map<int,DBMysql::DataType> map_col_type;
    map_col_type[0] = DBMysql::STRING;

    json json_data;
    g_db_mysql->getData(sql, map_col_type, json_data);
    for(int i = 0; i < json_data.size(); i++)
    {
        s_map_address_id[json_data[i][0].get<std::string>()] = i;
    }

    Syncer::instance().scanBlockChain(); 
    SetTimeout("ScanChain", 10*60);
}

static void VerifyTransaction(int fd, short kind, void *ctx)
{
    LOG(INFO) << "verify transaction begin ";

    Syncer::instance().verifyTransaction(); 
    SetTimeout("VerifyTransaction", 10);
}

void Syncer::appendBlockToDB(const json& json_block, const uint64_t& height)
{
    //std::string timestamps = json_block["result"]["timestamp"].get<std::string>();
    json json_trans;
    json_trans = json_block["result"]["transactions"];

    json json_tran;
    mpz_class amount;
    for(int i = 0; i < json_trans.size(); i++)
    {
        json_tran = json_trans[i];
        // LOG(INFO) << json_tran.dump() ;
        std::string value = json_tran["value"].get<std::string>();
        std::string from, to, contract;
        from = json_tran["from"].get<std::string>();
        std::string txid = json_tran["hash"].get<std::string>();
        if(json_tran["to"].is_null())
        {
            continue;
        }  

        if (value  == "0x0")
        {
            contract = json_tran["to"].get<std::string>();
            if (contract != "0xdac17f958d2ee523a2206206994597c13d831ec7")
            {
                continue;
            }

            std::string input = json_tran["input"].get<std::string>();
            std::string method  = input.substr(0,10);
            if (method != "0xa9059cbb" || input.size() < 135)
            {
                continue;
            }
            to ="0x" + input.substr(34,40);

            if (s_map_address_id.find(from) == s_map_address_id.end() && s_map_address_id.find(to) == s_map_address_id.end())
            {
                continue;
            }

            value ="0x" + input.substr(input.size() - 41,40);
            amount = value;

            std::string sql = "INSERT INTO `tokentran` (`txid`, `contract`, `vin`, `vout`, `amount`,`height`) VALUES ('" + txid + "','" + contract + "','" + from + "','" + to +"','" + 
                amount.get_str() + "','" + std::to_string(height) +"');";
            vect_sql_.push_back(sql);

        }
        else
        {
            to = json_tran["to"].get<std::string>();
            if (s_map_address_id.find(from) == s_map_address_id.end() && s_map_address_id.find(to) == s_map_address_id.end())
            {
                continue;
            }

            //INSERT INTO `ethdb`.`ethtran` (`txid`, `vin`, `vout`, `value`) VALUES ('dsfasdf', 'fsdfas', 'fasdf', 'fasdf');
            amount = value;
            std::string sql = "INSERT INTO `ethtran` (`txid`, `vin`, `vout`, `amount`,`height`) VALUES ('" + txid + "','" + from + "','" + to +"','" +
                amount.get_str() +"','" + std::to_string(height) + "');";
            vect_sql_.push_back(sql);

        }

    }
}

void Syncer::refreshDB()
{
    LOG(INFO) << "refresh DB begin" ;
    LOG(INFO) << "SQL size: " << vect_sql_.size() ;
    if (vect_sql_.size() > 0)
    {
        g_db_mysql->batchRefreshDB(vect_sql_);
        vect_sql_.clear();
    }   
    LOG(INFO) << "refresh DB end" ;
}

void Syncer::verifyTransaction()
{
    std::string sql_eth = "SELECT txid FROM ethtran;";
    std::string sql_usdt = "SELECT txid FROM tokentran;";
    std::map<int,DBMysql::DataType> map_col_type;
    map_col_type[0] = DBMysql::STRING;

    json json_data;
    g_db_mysql->getData(sql_eth, map_col_type, json_data);
    std::string  txid;
    json json_transaction;
    std::string status;
    std::string sql;
    bool ret = false;
    for(int i = 0; i < json_data.size(); i++)
    {
        txid = json_data[i][0].get<std::string>(); 
        ret = rpc_.getRawTransaction(txid, json_transaction);
        if (g_node_dump)
        {
            g_node_dump = false;
            break;  
        }

        if (!ret)
        {
            continue;
        }

        status = json_transaction["result"]["status"].get<std::string>().substr(2,1);
        sql = "UPDATE ethtran set status = '" + status + "' WHERE txid = '" + txid + "';";
        vect_sql_.push_back(sql);
    }

    refreshDB();

    g_db_mysql->getData(sql_usdt, map_col_type, json_data);

    for(int i = 0; i < json_data.size(); i++)
    {
        txid = json_data[i][0].get<std::string>(); 
        ret = rpc_.getRawTransaction(txid, json_transaction);
        if (g_node_dump)
        {
            g_node_dump = false;
            break;  
        }

        if (!ret)
        {
            continue;
        }

        
        status = json_transaction["result"]["status"].get<std::string>().substr(2,1);
        sql = "UPDATE tokentran set status = '" + status + "' WHERE txid = '" + txid +"';";
        vect_sql_.push_back(sql);
    }

    refreshDB();

}
void Syncer::scanBlockChain()
{
    //check height which is needed to upate

    uint64_t pre_height  = begin_;
    if (begin_ == 0)
    {
        std::string sql = "select height from block order by height desc limit 1;";
        std::map<int,DBMysql::DataType> map_col_type;
        map_col_type[0] = DBMysql::INT;

        json json_data;
        g_db_mysql->getData(sql, map_col_type, json_data);
        if (json_data.size() > 0)
        {
            pre_height = json_data[0][0].get<uint64_t>();
        }
    }

    uint64_t cur_height  = end_;
    if (end_ == 0 )
    {
        rpc_.getBlockCount(cur_height);
    }

    json json_block;

    for (int i = pre_height + 1; i <= cur_height; i++)
    {
        json_block.clear();

        rpc_.getBlock(i, json_block);
        if (g_node_dump)
        {
            g_node_dump = false;
            begin_ = pre_height;
            break;  
        }

        appendBlockToDB(json_block, i); 
        //LOG(INFO) << "block height: " << i;
        if(i % 500 == 0 || i == cur_height)
        {

            LOG(INFO) << "block height: " << i;
            std::string timestamps = json_block["result"]["timestamp"].get<std::string>();
            std::string hash = json_block["result"]["hash"].get<std::string>();
            //INSERT INTO `xsvdb`.`block` (`height`, `timestamps`) VALUES ('23', '123123');
            std::string sql = "INSERT INTO `block` (`hash`, `height`, `timestamps`) VALUES ('" + hash +
                "','" + std::to_string(i) + "','" + timestamps + "');";
            vect_sql_.push_back(sql);
            refreshDB();
        }
    }

    if (end_ != 0)
    {
        exit(0);
    }
}


Syncer Syncer::single_;
void Syncer::registerTask(map_event_t& name_events, map_job_t& name_tasks)
{
    REFLEX_TASK(ScanChain);
    REFLEX_TASK(VerifyTransaction);
}


