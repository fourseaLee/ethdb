#include "rpc.h"
#include <glog/logging.h>
#include <iostream>
#include <chrono>
#include <fstream>

static bool CurlPost(const std::string& url, const json &json_post, const std::string& auth, json& json_response)
{
    CurlParams curl_params;
    curl_params.auth = auth;
    curl_params.url = url;
 // curl_params.content_type = "content-type:text/plain";
    curl_params.data = json_post.dump();
    std::string response;
    CurlPostParams(curl_params,response);
    //LOG(INFO) << response;
    json_response = json::parse(response);
	if (!json_response["error"].is_null())
	{
		LOG(ERROR) << response;
		LOG(ERROR) << curl_params.data;
		return false;
	}
	
	
    return true;
}

bool Rpc::structRpc(const std::string& method, const json& json_params, json& json_post)
{
    json_post["jsonrpc"] = "2.0";
    json_post["id"] = "curltest";
    json_post["method"] = method;
    json_post["params"] = json_params;
	
    return true;
}

bool Rpc::getBlockCount(uint64_t& height)
{
    json json_post;
    json json_params = json::array();
	json_params.push_back("pending");
	json_params.push_back(true);
    json_post["params"] = json_params;

    structRpc("eth_getBlockByNumber", json_params, json_post);
    json json_response;	
    if ( !rpcNode(json_post, json_response) )
	{
		return false;
	}
    std::string hex_number = json_response["result"]["number"].get<std::string>();
 	std::stringstream ss;
	ss << hex_number;

	ss >> std::hex >> height;
	
	return true;
}

bool Rpc::getBlock(const uint64_t& height, json& json_block)
{
	json json_post;
    json json_params;
	std::stringstream ss;
	ss << std::hex << height;
	std::string hex_height;
	ss >> hex_height;
	
	hex_height = "0x" + hex_height;
    json_params.push_back(hex_height);
	json_params.push_back(true);
    json_post["params"] = json_params;

    structRpc("eth_getBlockByNumber", json_params, json_post);
//    json json_response;	
    if ( !rpcNode(json_post, json_block) )
	{
		return false;
	}


		
	return true;
}

bool Rpc::getRawTransaction(const std::string& txid, json& json_tx)
{

	json json_post;
    json json_params;
    json_params.push_back(txid);
    json_params.push_back(true);	
    json_post["params"] = json_params;

    structRpc("eth_getTransactionByHash", json_params, json_post);
    if ( !rpcNode(json_post, json_tx) )
	{
		return false;
	}

	return true;
}

bool Rpc::rpcNode(const json &json_post, json& json_response)
{
    return CurlPost(node_url_,json_post,auth_, json_response);
}







