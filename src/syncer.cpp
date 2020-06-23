#include "syncer.h"
#include <glog/logging.h>
#include "db_mysql.h"

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
    Syncer::instance().scanBlockChain(); 
    SetTimeout("ScanChain", 10);
}

void Syncer::appendBlockToDB(const json& json_block, const uint64_t& height)
{
	std::string timestamps = json_block["result"]["timestamp"].get<std::string>();
	json json_trans;
	json_trans = json_block["result"]["transactions"];

	json json_tran;
	for(int i = 0; i < json_trans.size(); i++)
	{
	   json_tran = json_trans[i];
	  // LOG(INFO) << json_tran.dump() ;
	   std::string value = json_tran["value"].get<std::string>();
	   std::string from, to, contract;
	   from = json_tran["from"].get<std::string>();
	   std::string txid = json_tran["hash"].get<std::string>();
	   if(txid == "0x64154406ce6f8da1e8603bd2e7cb1ae786bdfb9708bf732a212790cf854d7f2d")
	   {
		   int test =0;
		   ++test;
	   }
	   if(json_tran["to"].is_null())
	   {
	      continue;
	   }  

           if (value  == "0x0")
	   {
		contract = json_tran["to"].get<std::string>();
		std::string input = json_tran["input"].get<std::string>();
		std::string method  = input.substr(0,10);
		if (method != "0xa9059cbb" || input.size() < 140)
		{
		  continue;
		}
		to ="0x" + input.substr(35,40);
		value = input.substr(105,40);
		std::string sql = "INSERT INTO `tokentran` (`txid`, `contract`, `vin`, `vout`, `amount`) VALUES ('" + txid + "','" + contract + "','" + from + "','" + to +"','" + value +"');";
		vect_sql_.push_back(sql);

	   }
	   else
	  {
		to = json_tran["to"].get<std::string>();
		//INSERT INTO `ethdb`.`ethtran` (`txid`, `vin`, `vout`, `value`) VALUES ('dsfasdf', 'fsdfas', 'fasdf', 'fasdf');
		std::string sql = "INSERT INTO `ethtran` (`txid`, `vin`, `vout`, `amount`) VALUES ('" + txid + "','" + from + "','" + to +"','" + value +"');";
		vect_sql_.push_back(sql);

	  }

	}
	std::string hash = json_block["result"]["hash"].get<std::string>();
	//INSERT INTO `xsvdb`.`block` (`height`, `timestamps`) VALUES ('23', '123123');
	std::string sql = "INSERT INTO `block` (`hash`, `height`, `timestamps`) VALUES ('" + hash +
	   				  "','" + std::to_string(height) + "','" + timestamps + "');";

	vect_sql_.push_back(sql);
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

void Syncer::scanBlockChain()
{
	//check height which is needed to upate
	std::string sql = "select height from block order by height desc limit 1;";
	std::map<int,DBMysql::DataType> map_col_type;
	map_col_type[0] = DBMysql::INT;

	json json_data;
	g_db_mysql->getData(sql, map_col_type, json_data);
	uint64_t pre_height  = 0;
	if (json_data.size() > 0)
	{
		pre_height = json_data[0][0].get<uint64_t>();
	}
	else
	{
	    pre_height = 10000000;
	}

	uint64_t cur_height  = 0;
	rpc_.getBlockCount(cur_height);
	cur_height = 10319025;

/*	if (cur_height <= pre_height)
	{
		return ;
	}*/

	//refresh blockchain data to db
	json json_block;
	json json_tx;
	json json_tran;
	std::vector<std::string> vect_txid;

	for (int i = pre_height + 1; i <= cur_height; i++)
	{
		json_block.clear();
		vect_txid.clear();
	
		rpc_.getBlock(i, json_block);
		appendBlockToDB(json_block, i);	

		refreshDB();
	}
}


Syncer Syncer::single_;
void Syncer::registerTask(map_event_t& name_events, map_job_t& name_tasks)
{
    REFLEX_TASK(ScanChain);
}


