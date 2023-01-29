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

#include "ray/raylet/actor_worker_assignment_manager.h"
#include "ray/raylet/node_manager.h"

namespace ray {
namespace raylet {

void NodeManager::HandleStopTasksAndBanNewTasks(
    const rpc::StopTasksAndBanNewTasksRequest &request,
    rpc::StopTasksAndBanNewTasksReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Stopping tasks and ban new tasks, job id is " << job_id;
  auto workers = worker_pool_.GetWorkersRunningTasksForJob(job_id);
  RAY_LOG(INFO) << "Get workers of running tasks for job, worker count is "
                << workers.size();

  // Kill all the workers. The actual cleanup for these workers is done
  // later when we receive the DisconnectClient message from them.
  for (const auto &worker : workers) {
    // NOTE: We will no longer judge whether the actor is detached, because this will
    // cause the actor worker not to be deleted after the job is offline.
    // Clean up any open ray.wait calls that the worker made.
    dependency_manager_.CancelWaitRequest(worker->WorkerId());
    // Mark the worker as killed.
    worker->MarkDead(true);
    // Then kill the worker process.
    worker_pool_.KillWorker(worker);
  }

  // Cancel all tasks associated with the specified job id that are leasing workers.
  RAY_UNUSED(actor_worker_assignment_manager_->CancelActorsForJob(job_id));
  cluster_task_manager_->CancelAndRemoveTasksForJob(
      job_id, rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED,
      /*scheduling_failure_message=*/"Tasks canceled due to job FO");

  l1_handler.HandleJobBanNewTasks(job_id, request.timestamp());
  send_reply_callback(Status::OK(), nullptr, nullptr);

  RAY_LOG(INFO) << "Finished stopping tasks and ban new tasks, job id is " << job_id;
}

void NodeManager::HandleLiftNewTasksBan(const rpc::LiftNewTasksBanRequest &request,
                                        rpc::LiftNewTasksBanReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Lifting new tasks ban, job id is " << job_id;
  l1_handler.HandleJobLiftNewTasksBan(job_id, request.timestamp());
  send_reply_callback(Status::OK(), nullptr, nullptr);
  RAY_LOG(INFO) << "Finished lifting new tasks ban, job id is " << job_id;
}

void NodeManagerL1Handler::HandleJobBanNewTasks(const JobID &job_id,
                                                const int64_t &timestamp) {
  // If there's already a ban, update timestamp.
  if (job_ban_new_tasks_flags_.contains(job_id)) {
    if (!job_ban_new_tasks_ts_.contains(job_id) ||
        job_ban_new_tasks_ts_[job_id] < timestamp) {
      job_ban_new_tasks_ts_[job_id] = timestamp;
    }
  } else {
    if (!job_ban_new_tasks_ts_.contains(job_id)) {
      // A brand new ban.
      job_ban_new_tasks_flags_[job_id] = true;
    } else if (job_ban_new_tasks_ts_[job_id] < timestamp) {
      // Ignore the ban if there's already a lift after it.
      job_ban_new_tasks_ts_.erase(job_id);
      job_ban_new_tasks_flags_[job_id] = true;
    }
  }
}

void NodeManagerL1Handler::HandleJobLiftNewTasksBan(const JobID &job_id,
                                                    const int64_t &timestamp) {
  // Store/update timestamp of early lift ban.
  if (!job_ban_new_tasks_flags_.contains(job_id)) {
    if (!job_ban_new_tasks_ts_.contains(job_id) ||
        job_ban_new_tasks_ts_[job_id] < timestamp) {
      job_ban_new_tasks_ts_[job_id] = timestamp;
    }
  } else {
    // Normal lift ban arrived, erase the flag.
    if (!job_ban_new_tasks_ts_.contains(job_id)) {
      job_ban_new_tasks_flags_.erase(job_id);
    } else if (job_ban_new_tasks_ts_[job_id] < timestamp) {
      // Ignore the lift ban if there's already a ban after it.
      job_ban_new_tasks_ts_.erase(job_id);
      job_ban_new_tasks_flags_.erase(job_id);
    }
  }
}

bool NodeManagerL1Handler::IsJobBanningNewTasks(const JobID &job_id) {
  return job_ban_new_tasks_flags_.contains(job_id);
}

}  // namespace raylet

}  // namespace ray
