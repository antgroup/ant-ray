// Copyright 2020-2021 The Ray Authors.
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

#include "ray/raylet/scheduling/cluster_task_manager.h"

#include "ray/util/logging.h"

namespace ray {
namespace raylet {

void ClusterTaskManager::QueueTask(const RayTask &task, const bool grant_or_reject,
                                   rpc::RequestWorkerLeaseReply *reply,
                                   rpc::SendReplyCallback send_reply_callback) {
  // TODO(loushang.ls): invalid bundles of the placement group check.
  RAY_LOG(DEBUG) << "Queuing task " << task.GetTaskSpecification().TaskId();
  metric_tasks_queued_++;
  Work work = std::make_tuple(task, grant_or_reject, reply, [send_reply_callback] {
    send_reply_callback(Status::OK(), nullptr, nullptr);
  });
  const auto &scheduling_class = task.GetTaskSpecification().GetSchedulingClass();
  // If the scheduling class is infeasible, just add the work to the infeasible queue
  // directly.
  if (infeasible_tasks_.count(scheduling_class) > 0) {
    infeasible_tasks_[scheduling_class].push_back(work);
  } else {
    tasks_to_schedule_[scheduling_class].push_back(work);
  }
  AddToBacklogTracker(task);
}

}  // namespace raylet
}  // namespace ray
