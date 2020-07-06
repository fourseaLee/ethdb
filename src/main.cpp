#include "common.h"
#include <iostream>
#include "db_mysql.h"
#include "rpc.h"
#include <fstream>
#include <glog/logging.h>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include "syncer.h"
static json s_json_conf;

static bool ParseCmd(int argc,char*argv[])
{
    using namespace boost::program_options;
    std::string conf_file ;

    boost::program_options::options_description opts_desc("All options");
    opts_desc.add_options()
        ("help,h", "help info")
        ("configure,c", value<std::string>(&conf_file)->default_value("../conf/conf.json"), "configure file");

    variables_map cmd_param_map;
    try
    {
        store(parse_command_line(argc, argv, opts_desc), cmd_param_map);
    }
    catch(boost::program_options::error_with_no_option_name &ex)
    {
        std::cerr << ex.what() << std::endl;
    }
    notify(cmd_param_map);

    if (cmd_param_map.count("help"))
    {
        std::cout << opts_desc << std::endl;
        return false;
    }
    std::ifstream jfile(conf_file);

    if (!jfile)
    {
        std::cerr << "No " <<conf_file <<" such config file!\n";
        return false;
    }

    jfile >> s_json_conf;
    if(!s_json_conf.is_object())
    {
        std::cerr << conf_file  <<   "is not json object!\n";
        jfile.close();
        return false;
    }

    jfile.close();

    return true;
}

static bool InitLog(const std::string& log_path)
{
    boost::filesystem::path path_check(log_path);

    if( !boost::filesystem::exists(path_check) )
    {
        boost::filesystem::create_directory(path_check);
    }

    FLAGS_alsologtostderr = false;
    FLAGS_colorlogtostderr = true;
    FLAGS_max_log_size = 100;
    FLAGS_stop_logging_if_full_disk  = true;
    std::string log_exec = "log_exe";
    FLAGS_logbufsecs = 0;
    google::InitGoogleLogging(log_exec.c_str());
    FLAGS_log_dir = log_path;
    std::string log_dest = log_path+"/info_";
    google::SetLogDestination(google::GLOG_INFO,log_dest.c_str());
    log_dest = log_path+"/warn_";

    google::SetLogDestination(google::GLOG_WARNING,log_dest.c_str());
    log_dest = log_path+"/error_";
    google::SetLogDestination(google::GLOG_ERROR,log_dest.c_str());
    log_dest = log_path+"/fatal_";
    google::SetLogDestination(google::GLOG_FATAL,log_dest.c_str());
    google::SetStderrLogging(google::GLOG_ERROR);

    return true;
}

static bool OpenDB()
{
    json json_connect = s_json_conf["mysql"];

    if (!g_db_mysql->openDB(json_connect))
    {
        std::cerr << "open db fail!" << std::endl;
        return false;
    }

    return true;
}

static void RunJob()
{
    Job contraller;
    Job::s_base_ = event_base_new();

    std::string node_url = s_json_conf["nodeurl"].get<std::string>();
    std::string auth = s_json_conf["auth"].get<std::string>();
    Rpc rpc;
    rpc.setRpc(node_url, auth);
    uint64_t begin = 0 ,end = 0 ;
    begin = s_json_conf["begin"].get<uint64_t>();
    end = s_json_conf["end"].get<uint64_t>();

    Syncer::instance().setRpc(rpc);
    Syncer::instance().setBeginEnd(begin, end);
    contraller.registerJob(Syncer::instance());
    contraller.run();
}


int main (int argc,char*argv[])
{
    assert(ParseCmd(argc,argv));
    std::string log_path = s_json_conf["logpath"].get<std::string>();
    assert(InitLog(log_path));
    assert(OpenDB());
    bool back_run = s_json_conf["daemon"].get<bool>();
    if (back_run)
    {
        fprintf(stdout, "Bitcoin server starting\n");

        // Daemonize
        if (daemon(1, 0)) 
        { // don't chdir (1), do close FDs (0)
            fprintf(stderr, "Error: daemon() failed: %s\n", strerror(errno));
            return 0;
        }

    }
    RunJob();

    return 0;
}
