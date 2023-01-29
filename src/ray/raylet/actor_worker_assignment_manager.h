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

#pragma once

#include <boost/asio/steady_timer.hpp>
#include <unordered_set>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/util/process.h"

namespace ray {

namespace raylet {

/// \class ActorWorkerAssignmentManager
/// \brief Manager the relationship among task, actor worker assignment and process.
class ActorWorkerAssignmentManager {
 public:
  /// \brief ActorWorkerAssignmentManager constructor.
  ///
  /// \param gcs_task_scheduling_enabled
  /// \return Void.
  ActorWorkerAssignmentManager(boost::asio::io_context &io_context,
                               bool gcs_task_scheduling_enabled =
                                   RayConfig::instance().gcs_task_scheduling_enabled());

  /// \brief Check if the specified `assignment` is assignable.
  ///
  /// \param assignment_id ID of the specified actor worker assignment.
  /// \return Void.
  bool IsAssignable(const UniqueID &assignment_id) const;

  /// \brief Assign actor to the specified actor worker assignment.
  ///
  /// \param task_id ID of the specified actor creation task.
  /// \param assignment_id ID of the specified actor worker assignment.
  bool AssignActor(const TaskID &task_id, const UniqueID &assignment_id);

  /// \brief Handle the event of assigning actor creation task. The actor creation tasks
  /// will be removed from actor worker assignment once the actor finished leasing a
  /// worker.
  ///
  /// \param task_id ID of the specified actor creation task.
  /// \return True if the assigned actor succeed in removing from the actor worker
  /// assignment.
  bool OnActorAssigned(const TaskID &task_id);

  /// \brief Cancel the actor by the specified actor creation task.
  ///
  /// \param True if the actor is assigning, else False.
  bool CancelActor(const TaskID &task_id);

  /// \brief Cancel all actors related to the specified job id.
  ///
  /// \param task_ids IDs of the actor creation tasks to be canceled.
  std::unordered_set<TaskID> CancelActorsForJob(const JobID &job_id);

  /// \brief Cancel all actors associated with the specified worker process.
  ///
  /// \param process The specified process.
  /// \param mark_assignment_as_invalid True mark the worker process as invalid.
  /// \return Actor creation tasks canceled from this worker process.
  std::unordered_set<TaskID> CancelActorsInProcess(const Process &process,
                                                   bool mark_assignment_as_invalid);

  /// \brief Cancel all leaked actors.
  ///
  /// \param in_use_worker_processes The worker processes in use.
  /// \return All leaked actor creation tasks.
  std::unordered_set<TaskID> CancelLeakedActors(
      const std::unordered_set<Process> &in_use_worker_processes);

  /// \brief Check if the process is valid.
  ///
  /// \param process the specified task to be checked.
  virtual bool IsValid(const Process &process);

  /// \brief Bind the specified assignment to the specified process.
  ///
  /// \param assignment_id ID of the specified assignment.
  /// \param process The specified process.
  /// \return True if the assignment id is not bound to a process, else false.
  bool BindAssignmentToProcess(const UniqueID &assignment_id, const Process &process);

  /// \brief Unbind any assignment from the specifed process.
  ///
  /// \param process The specified process.
  void UnbindAssignmentFromProcess(const Process &process);

  /// \brief Get the bound process and assignment by the specified actor creation task id.
  ///
  /// \param task_id ID of the specified actor creation task id.
  /// \return The pair of bound process and assignment.
  std::pair<Process, UniqueID> GetBindProcessAndAssignment(const TaskID &task_id) const;

  void IncAssignmentWorkRegisterTimeoutNumByProc(const Process &process);

  int64_t GetAssignmentWorkRegisterTimeoutNum(const TaskID &task_id) const;

  virtual ~ActorWorkerAssignmentManager() = default;

 protected:
  /// Contains the worker process state.
  struct ActorWorkerAssignment {
    explicit ActorWorkerAssignment(const JobID &job_id, const UniqueID &assignment_id)
        : job_id_(job_id), assignment_id_(assignment_id), update_time_(time(0)) {}

    void ResetWorkerRegisterTimeoutNum() { worker_register_timeout_num_ = 0; }

    int64_t GetWorkerRegisterTimeoutNum() const { return worker_register_timeout_num_; }

    void IncWorkerRegisterTimeoutNum() { worker_register_timeout_num_++; }

    JobID job_id_;
    UniqueID assignment_id_;
    time_t update_time_;
    bool is_valid_ = true;
    std::unordered_set<TaskID> actor_creation_tasks_;
    Process process_;
    int64_t worker_register_timeout_num_ = 0;
  };

  /// \brief Get actor worker assignment by actor creation task id.
  ///
  /// \param task_id ID of the specified actor creation task id.
  /// \return The actor worker assignment.
  std::shared_ptr<ActorWorkerAssignment> GetAssignment(const TaskID &task_id) const;

  /// \brief Get actor worker assignment by the specified assignment id.
  ///
  /// \param assignment_id ID of the assignment.
  /// \return the assignemnt.
  std::shared_ptr<ActorWorkerAssignment> GetAssignment(
      const UniqueID &assignment_id) const;

  /// \brief Get actor worker assignment by process.
  ///
  /// \param process The specified process.
  /// \return the assignemnt.
  std::shared_ptr<ActorWorkerAssignment> GetAssignment(const Process &process) const;

  /// \brief Remove the actor from the assignment and return the related assignment.
  ///
  /// \param task_id ID of the actor creation task.
  /// \return The assignmant associated with this actor creation task.
  std::shared_ptr<ActorWorkerAssignment> RemoveActor(const TaskID &task_id);

  /// \brief Cancel actors from the assignment.
  ///
  /// \param assignment The specified assignment.
  /// \param mark_assignment_as_invalid True mark the process as invalid.
  /// \return Actor creation tasks that canceled from the specified assignment.
  std::unordered_set<TaskID> CancelActors(
      std::shared_ptr<ActorWorkerAssignment> assignment, bool mark_assignment_as_invalid);

  /// \brief Garbage collection about the invalid assignemnt.
  void AssignmentGC();

  FRIEND_TEST(ActorWorkerAssignmentManagerTest, TestAssignActorAndActorAssigned);
  FRIEND_TEST(ActorWorkerAssignmentManagerTest, TestBindAndCancel);

 protected:
  /// Map from actor creation task id to the assignment id.
  absl::flat_hash_map<TaskID, UniqueID> actor_to_assignment_id_;
  /// Map from assignment id to the assignment.
  absl::flat_hash_map<UniqueID, std::shared_ptr<ActorWorkerAssignment>> assignments_;
  /// The timer used to gc invalid assignment.
  boost::asio::steady_timer assignment_gc_timer_;
  bool gcs_task_scheduling_enabled_ = false;
};

}  // namespace raylet

}  // namespace ray
