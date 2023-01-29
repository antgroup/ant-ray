// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/raylet/node_manager.h"

namespace ray {

namespace raylet {

void NodeManager::AsyncScheduleAndDispatchTasks() {
  if (schedule_and_dispatch_times_++ == 0) {
    io_service_.post(
        [this] {
          cluster_task_manager_->ScheduleAndDispatchTasks();
          int optimize_times = schedule_and_dispatch_times_ - 1;
          if (optimize_times > 0) {
            RAY_LOG(DEBUG) << "Optimize " << optimize_times
                           << " calls of ScheduleAndDispatchTasks";
          }
          schedule_and_dispatch_times_ = 0;
        },
        "AsyncScheduleAndDispatchTasks");
  }
}

}  // namespace raylet

}  // namespace ray
