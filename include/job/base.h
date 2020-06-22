#ifndef BASE_H
#define BASE_H
#include "job/job.h"
#include "job/task.h"

class BaseTask:public Task
{
public:
    BaseTask()
    {

    }

    virtual ~BaseTask()
    {

    }

    static BaseTask& instance()
    {
        return single_;
    }

public:
    void registerTask(map_event_t& name_events, map_job_t& name_tasks);

    void setContractUrlVersion(const std::string& url, const std::string& version);

protected:
    static BaseTask single_;


};

#endif
