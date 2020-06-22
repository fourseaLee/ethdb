#include "job/base.h"
#include "common.h"
#include <glog/logging.h>

static void SetTimeout(const std::string& name, int second)
{
    struct timeval timeout ;
    timeout.tv_sec = second;
    timeout.tv_usec = 0;
    evtimer_add(Job::map_name_event_[name], &timeout);
}

BaseTask BaseTask::single_;

void BaseTask::registerTask(map_event_t& name_events, map_job_t& name_tasks)
{
}

