#ifndef RPC_H_
#define RPC_H_
#include "common.h"
#include <iostream>

class Rpc
{
public:
    Rpc()
    {
    }

    ~Rpc()
    {
    }

public:
   
    bool getBlockCount(uint64_t& height);

	bool getBlock(const uint64_t& height, json& json_block);

	bool getRawTransaction(const std::string& txid, json& json_tx);

	bool getRawMempool(json& json_mempool);

public:
    bool setRpc(std::string node_url, std::string auth)
    {
        node_url_ = node_url;
        auth_ = auth;
        return true;
    }    

    bool rpcNode(const json& json_post, json& json_response);

    bool structRpc(const std::string& method, const json& json_params, json& json_post);	
protected:
    std::string node_url_;
    std::string auth_;

};
#endif // RPC_H

