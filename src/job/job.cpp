#include "job/job.h"
#include <glog/logging.h>
#include "db_mysql.h"
#include <iostream>

event_base *Job::s_base_ = nullptr;
map_event_t Job::map_name_event_;
map_job_t Job::map_name_job_;

void Job::run()
{
    event_base_dispatch(s_base_);
}


