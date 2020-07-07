#ifndef SYNCER_H_
#define SYNCER_H_

#include "job/job.h"
#include "job/task.h"
#include "rpc.h"

class Syncer:public Task
{
public:
    Syncer()
    {
		init_mempool_ = false;
    }

    virtual ~Syncer()
    {

    }

    static Syncer& instance()
    {
        return single_;
    }

	void appendBlockSql(const json& json_block, const uint64_t& height, std::vector<std::string>& vect_txid);

	void appendTxVinVoutSql(const json& json_tx, const std::string& txid);
	void appendBlockToDB(const json& json_block, const uint64_t& height);
	void refreshDB();

	void scanBlockChain();
    
    void verifyTransaction();

	void scanMempool();

	void setBeginEnd(const uint64_t& begin, const uint64_t& end)
	{
		begin_ = begin;
		end_ = end;
	}

public:

    void registerTask(map_event_t& name_events, map_job_t& name_tasks);

    void setRpc(const Rpc& rpc)
	{
		rpc_ = rpc; 
	}

protected:
    static Syncer single_;
	Rpc rpc_;
	std::vector<std::string> vect_sql_;
	std::map<std::string, bool> map_mempool_tx_;
	bool init_mempool_ = false;
	uint64_t begin_ ,end_;
};

#endif
