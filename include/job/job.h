#ifndef JOB_H
#define JOB_H
#include <map>
#include <functional>
#include <event.h>
#include <evhttp.h>
#include <event2/keyvalq_struct.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/buffer.h>
#include <event2/util.h>
#include <event2/keyvalq_struct.h>

typedef std::function<void (int , short, void*)> job_t;
typedef std::map<std::string, event*>   map_event_t;
typedef std::map<std::string, job_t>    map_job_t;
class Job
{
public:
    Job()
    {

    }
    ~Job()
    {

    }

    template<class T>
    void registerJob(T& task)
    {
        task.registerTask(map_name_event_, map_name_job_);
    }

public:
    void run();

public:

     static event_base *s_base_;
     static std::map<std::string, event* > map_name_event_;

public:
     static std::map<std::string, job_t> map_name_job_;
};



#endif // JOB_H
