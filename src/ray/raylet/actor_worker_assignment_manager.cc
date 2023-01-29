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

#include <chrono>

#include "ray/util/logging.h"

namespace ray {

namespace raylet {

ActorWorkerAssignmentManager::ActorWorkerAssignmentManager(
    boost::asio::io_context &io_context, bool gcs_task_scheduling_enabled)
    : assignment_gc_timer_(io_context),
      gcs_task_scheduling_enabled_(gcs_task_scheduling_enabled) {
  AssignmentGC();
}

bool ActorWorkerAssignmentManager::IsAssignable(const UniqueID &assignment_id) const {
  if (!gcs_task_scheduling_enabled_) {
    return true;
  }

  auto iter = assignments_.find(assignment_id);
  return iter == assignments_.end() || iter->second->is_valid_;
}

bool ActorWorkerAssignmentManager::AssignActor(const TaskID &task_id,
                                               const UniqueID &assignment_id) {
  if (!gcs_task_scheduling_enabled_) {
    return true;
  }

  if (!IsAssignable(assignment_id)) {
    RAY_LOG(INFO) << "Failed to assign actor creation task " << task_id
                  << " to the invalid assignment " << assignment_id;
    return false;
  }

  std::shared_ptr<ActorWorkerAssignment> assignment;
  auto iter = assignments_.find(assignment_id);
  if (iter == assignments_.end()) {
    assignment = std::make_shared<ActorWorkerAssignment>(task_id.JobId(), assignment_id);
    assignments_.emplace(assignment_id, assignment);
  } else {
    assignment = iter->second;
    RAY_CHECK(assignment->is_valid_);
    if (assignment->GetWorkerRegisterTimeoutNum() != 0) {
      RAY_LOG(WARNING) << "Assignment " << assignment_id
                       << " worker register timeout number is not 0, real is "
                       << assignment->GetWorkerRegisterTimeoutNum() << ", will reset it.";
      assignment->ResetWorkerRegisterTimeoutNum();
    }
  }

  RAY_LOG(INFO) << "Assigning actor creation task " << task_id << " to the assignment "
                << assignment_id << ", process = " << assignment->process_.GetId();
  RAY_CHECK(assignment->actor_creation_tasks_.emplace(task_id).second);
  RAY_CHECK(actor_to_assignment_id_.emplace(task_id, assignment_id).second);
  return true;
}

bool ActorWorkerAssignmentManager::OnActorAssigned(const TaskID &task_id) {
  if (!gcs_task_scheduling_enabled_) {
    return true;
  }

  auto assignment = RemoveActor(task_id);
  if (!assignment) {
    return false;
  }

  RAY_LOG(INFO) << "Finished assigning actor creation task " << task_id
                << " to the assignment " << assignment->assignment_id_
                << ", process = " << assignment->process_.GetId();
  return true;
}

bool ActorWorkerAssignmentManager::CancelActor(const TaskID &task_id) {
  if (!gcs_task_scheduling_enabled_) {
    return false;
  }

  if (auto assignment = RemoveActor(task_id)) {
    if (assignment->actor_creation_tasks_.empty()) {
      assignment->is_valid_ = false;
      assignment->update_time_ = time(0);
      assignment->process_ = Process();
    }
    return true;
  }
  return false;
}

std::unordered_set<TaskID> ActorWorkerAssignmentManager::CancelActorsForJob(
    const JobID &job_id) {
  if (!gcs_task_scheduling_enabled_) {
    return {};
  }

  std::unordered_set<TaskID> canceled_actor_creation_tasks;
  for (auto iter = assignments_.begin(); iter != assignments_.end(); ++iter) {
    auto assignment = iter->second;
    if (assignment->job_id_ == job_id) {
      auto tasks = CancelActors(assignment, /*mark_assignment_as_invalid=*/true);
      canceled_actor_creation_tasks.insert(tasks.begin(), tasks.end());
    }
  }
  return canceled_actor_creation_tasks;
}

std::unordered_set<TaskID> ActorWorkerAssignmentManager::CancelActorsInProcess(
    const Process &process, bool mark_assignment_as_invalid) {
  if (!gcs_task_scheduling_enabled_) {
    return {};
  }

  std::unordered_set<TaskID> canceled_actor_creation_tasks;
  auto assignment = GetAssignment(process);
  if (assignment) {
    canceled_actor_creation_tasks = CancelActors(assignment, mark_assignment_as_invalid);
  }
  return canceled_actor_creation_tasks;
}

std::unordered_set<TaskID> ActorWorkerAssignmentManager::CancelLeakedActors(
    const std::unordered_set<Process> &in_use_worker_processes) {
  if (!gcs_task_scheduling_enabled_) {
    return {};
  }

  std::unordered_set<TaskID> canceled_actor_creation_tasks;
  for (auto &pair : assignments_) {
    bool mark_assignment_as_invalid =
        (in_use_worker_processes.count(pair.second->process_) == 0);
    auto tasks = CancelActors(pair.second, mark_assignment_as_invalid);
    canceled_actor_creation_tasks.insert(tasks.begin(), tasks.end());
  }

  RAY_LOG(INFO) << "Cancelling " << canceled_actor_creation_tasks.size()
                << " leaked actors to prevent assignment leaking.";

  return canceled_actor_creation_tasks;
}

bool ActorWorkerAssignmentManager::IsValid(const Process &process) {
  if (!gcs_task_scheduling_enabled_) {
    return true;
  }

  auto assignment = GetAssignment(process);
  return assignment != nullptr && assignment->is_valid_;
}

bool ActorWorkerAssignmentManager::BindAssignmentToProcess(const UniqueID &assignment_id,
                                                           const Process &process) {
  if (!gcs_task_scheduling_enabled_) {
    return true;
  }

  if (!process.IsValid()) {
    return false;
  }

  auto assignment = GetAssignment(assignment_id);
  if (assignment == nullptr || assignment->process_.IsValid()) {
    return false;
  }

  assignment->process_ = process;
  RAY_LOG(INFO) << "Finished binding assignment " << assignment_id << " to process "
                << process.GetId();
  return true;
}

void ActorWorkerAssignmentManager::UnbindAssignmentFromProcess(const Process &process) {
  if (!gcs_task_scheduling_enabled_) {
    return;
  }

  if (auto assignment = GetAssignment(process)) {
    assignment->process_ = Process();
  }
}

std::pair<Process, UniqueID> ActorWorkerAssignmentManager::GetBindProcessAndAssignment(
    const TaskID &task_id) const {
  if (!gcs_task_scheduling_enabled_) {
    return {Process(), UniqueID::Nil()};
  }

  auto assignment = GetAssignment(task_id);
  if (assignment == nullptr) {
    return {Process(), UniqueID::Nil()};
  }
  return {assignment->process_, assignment->assignment_id_};
}

std::shared_ptr<ActorWorkerAssignmentManager::ActorWorkerAssignment>
ActorWorkerAssignmentManager::RemoveActor(const TaskID &task_id) {
  if (auto assignment = GetAssignment(task_id)) {
    RAY_CHECK(assignment->actor_creation_tasks_.erase(task_id));
    actor_to_assignment_id_.erase(task_id);
    return assignment;
  }
  return nullptr;
}

std::unordered_set<TaskID> ActorWorkerAssignmentManager::CancelActors(
    std::shared_ptr<ActorWorkerAssignment> assignment, bool mark_assignment_as_invalid) {
  RAY_CHECK(assignment);
  std::unordered_set<TaskID> canceled_tasks;
  if (assignment && assignment->is_valid_) {
    for (auto &task_id : assignment->actor_creation_tasks_) {
      RAY_LOG(INFO) << "Cancel task on worker process, task_id = " << task_id
                    << ", assignment_id = " << assignment->assignment_id_
                    << ", process = " << assignment->process_.GetId();
      RAY_CHECK(actor_to_assignment_id_.erase(task_id));
    }
    canceled_tasks = std::move(assignment->actor_creation_tasks_);
    if (mark_assignment_as_invalid) {
      assignment->is_valid_ = false;
      assignment->update_time_ = time(0);
      assignment->process_ = Process();
    }
  }
  return canceled_tasks;
}

std::shared_ptr<ActorWorkerAssignmentManager::ActorWorkerAssignment>
ActorWorkerAssignmentManager::GetAssignment(const TaskID &task_id) const {
  auto iter = actor_to_assignment_id_.find(task_id);
  if (iter != actor_to_assignment_id_.end()) {
    auto it = assignments_.find(iter->second);
    if (it != assignments_.end()) {
      return it->second;
    }
  }

  return nullptr;
}

std::shared_ptr<ActorWorkerAssignmentManager::ActorWorkerAssignment>
ActorWorkerAssignmentManager::GetAssignment(const UniqueID &assignment_id) const {
  auto iter = assignments_.find(assignment_id);
  return iter != assignments_.end() ? iter->second : nullptr;
}

std::shared_ptr<ActorWorkerAssignmentManager::ActorWorkerAssignment>
ActorWorkerAssignmentManager::GetAssignment(const Process &process) const {
  auto iter = std::find_if(
      assignments_.begin(), assignments_.end(),
      [&process](
          const std::pair<UniqueID, std::shared_ptr<ActorWorkerAssignment>> &entry) {
        return entry.second->process_.GetId() == process.GetId();
      });
  return iter != assignments_.end() ? iter->second : nullptr;
}

void ActorWorkerAssignmentManager::AssignmentGC() {
  auto interval = RayConfig::instance().actor_worker_assignment_gc_interval_s();
  auto iter = assignments_.begin();
  for (; iter != assignments_.end();) {
    auto current = iter++;
    if (!current->second->is_valid_ &&
        current->second->update_time_ + interval < time(0)) {
      RAY_CHECK(current->second->actor_creation_tasks_.empty());
      RAY_LOG(INFO) << "Worker process " << current->second->assignment_id_
                    << " is removed.";
      assignments_.erase(current);
    }
  }

  // Reset the timer.
  assignment_gc_timer_.expires_from_now(std::chrono::seconds(interval));
  assignment_gc_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::asio::error::operation_aborted) {
      return;
    }
    AssignmentGC();
  });
}

void ActorWorkerAssignmentManager::IncAssignmentWorkRegisterTimeoutNumByProc(
    const Process &process) {
  auto assignment = GetAssignment(process);
  if (assignment) {
    assignment->IncWorkerRegisterTimeoutNum();
  }
}

int64_t ActorWorkerAssignmentManager::GetAssignmentWorkRegisterTimeoutNum(
    const TaskID &task_id) const {
  auto assignment = GetAssignment(task_id);
  if (assignment) {
    return assignment->GetWorkerRegisterTimeoutNum();
  }
  return 0;
}

}  // namespace raylet
}  // namespace ray
