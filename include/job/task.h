#ifndef TASK_H__
#define TASK_H__
#include <string>
#include <vector>
#include <map>

class Task
{
public:
    Task()
    {

    }

    virtual ~Task()
    {
     
    }
public:

    void getTaskParams(const std::string& name,  std::vector<std::string>& params)
    {
        params = task_params_[name];
    }

    void setTaskParams(const std::map<std::string, std::vector<std::string> > task_params)
    {
        task_params_ = task_params;
    }

private: 
    std::map<std::string, std::vector<std::string> > task_params_;

};

#define REFLEX_TASK(task)  { \
    name_tasks[#task] = task; \
    event * job_event = evtimer_new(Job::s_base_, task, NULL); \
    name_events[#task] = job_event; \
    name_tasks[#task](-1, EV_TIMEOUT, nullptr);\
}

#endif
