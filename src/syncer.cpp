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
	   std::string value = json_tran["value"].get<std::string>();
	   std::string from, to, contract;
	   from = json_tran["from"].get<std::string>();
	   std::string txid = json_tran["hash"].get<std::string>();
           if (value  == "0x0")
	   {
		contract = json_tran["to"].get<std::string>();
		std::string input = json_tran["input"].get<std::string>();
		std::string method  = input.substr(0,10);
		if (method != "0xa9059cbb")
		{
		  continue;
		}
		to = input.substr(35,40);
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

void Syncer::appendBlockSql(const json& json_block, const uint64_t& height, std::vector<std::string>& vect_txid)
{
	uint64_t timestamps = json_block["result"]["time"].get<uint64_t>();

	for(int i = 0; i < json_block["result"]["tx"].size(); i++)
	{
		vect_txid.push_back(json_block["result"]["tx"][i].get<std::string>());
	}
	std::string hash = json_block["result"]["hash"].get<std::string>();
	//INSERT INTO `xsvdb`.`block` (`height`, `timestamps`) VALUES ('23', '123123');
	std::string sql = "INSERT INTO `block` (`hash`, `height`, `timestamps`) VALUES ('" + hash +
	   				  "','" + std::to_string(height) + "','" + std::to_string(timestamps) + "');";

	vect_sql_.push_back(sql);
}

void Syncer::appendTxVinVoutSql(const json& json_tx, const std::string& txid)
{
	json json_vins = json_tx["result"]["vin"];
	json json_vouts = json_tx["result"]["vout"];
	std::string sql_vin  = "INSERT INTO `vin` (`txid`, `prevtxid`, `n`) VALUES ";
	
	bool vin = false;
	json json_vin;
	std::string prev_txid;
	int n = 0;
	for (int i = 0; i < json_vins.size(); i ++)
	{
		json_vin = json_vins[i];
		if (json_vin.find("txid") != json_vin.end())
		{
			prev_txid = json_vin["txid"].get<std::string>();
			n = json_vin["vout"].get<int>();
			vin = true;	 	
		}
		else
		{
			continue;
		}
		
		std::string sql = sql_vin + "('" + txid + "','" + prev_txid +"','" + std::to_string(n) +"');";
		std::string sql_utxo = "DELETE FROM utxo where txid = '" + prev_txid + "'AND n = '" + std::to_string(n) + "';";
		vect_sql_.push_back(sql);
		vect_sql_.push_back(sql_utxo);
	}

	std::string address;
	double value = 0.0;
	json json_vout;
	for (int i = 0; i < json_vouts.size(); i++)
	{
		json_vout = json_vouts[i];
		if (json_vout["scriptPubKey"].find("addresses") != json_vout["scriptPubKey"].end())
		{
			std::string sql_prefix = "INSERT INTO `voutaddress` (`txid`, `n`, `address`, `addresspos`, `value`) VALUES ";
			std::string sql_prefix_utxo = "INSERT INTO `utxo` (`txid`, `n`, `address`, `addresspos`, `value`) VALUES ";
			n = json_vout["n"].get<int>();
			value = json_vout["value"].get<double>();
			for(int j = 0; j < json_vout["scriptPubKey"]["addresses"].size(); j++)
			{
				std::string sql = sql_prefix + "('" + txid + "','" + std::to_string(n) + "','" + 
								  json_vout["scriptPubKey"]["addresses"][j].get<std::string>() + "','" + std::to_string(j) + "','" + std::to_string(value) + "');";
				std::string sql_utxo = sql_prefix_utxo + "('" + txid + "','" + std::to_string(n) + "','" + 
								  json_vout["scriptPubKey"]["addresses"][j].get<std::string>() + "','" + std::to_string(j) + "','" + std::to_string(value) + "');";
				vect_sql_.push_back(sql);
				vect_sql_.push_back(sql_utxo);
			}	
		}
		else
		{
			n = json_vout["n"].get<int>();
			std::string ret_data = json_vout["scriptPubKey"]["asm"].get<std::string>();
			std::string prefix = ret_data.substr(12, 400);

			std::string sql = "INSERT INTO `voutret` (`txid`, `n`, `data`) VALUES ('" + 
							  txid + "','" + std::to_string(n) + "','" + prefix  +"');";
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

	uint64_t cur_height  = 0;
	rpc_.getBlockCount(cur_height);
	
	if (cur_height <= pre_height)
	{
		return ;
	}

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
/*		appendBlockSql(json_block, i, vect_txid);

		for (uint j = 0; j < vect_txid.size(); j++)
		{
			json_tx.clear();
			std::string sql = "INSERT INTO `chaintx` (`txid`, `height`) VALUES ('" + vect_txid[j] + "','" + std::to_string(i) + "');";
			vect_sql_.push_back(sql);
			if (map_mempool_tx_.find(vect_txid[j]) != map_mempool_tx_.end())
			{
				std::string sql_mempool = "DELETE FROM mempooltx WHERE txid = '" + vect_txid[j] + "';";
				vect_sql_.push_back(sql_mempool);
				map_mempool_tx_.erase(vect_txid[j]);
			}
			else
			{
				rpc_.getRawTransaction(vect_txid[j], json_tx);
				appendTxVinVoutSql(json_tx, vect_txid[j]);
			}			
		}
		refreshDB();*/
	}
}


Syncer Syncer::single_;
void Syncer::registerTask(map_event_t& name_events, map_job_t& name_tasks)
{
    REFLEX_TASK(ScanChain);
}


