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

#include "gtest/gtest.h"
#include "ray/common/task/task_util.h"

namespace ray {
namespace raylet {

class ActorWorkerAssignmentManagerTest : public ::testing::Test {};

TEST_F(ActorWorkerAssignmentManagerTest, TestAssignActorAndActorAssigned) {
  boost::asio::io_context io_context;
  ActorWorkerAssignmentManager actor_worker_assignment_manager(
      io_context, /*gcs_task_scheduling_enabled=*/true);

  auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto task_id = TaskID::ForActorCreationTask(actor_id);
  auto assignment_id = UniqueID::FromRandom();

  actor_worker_assignment_manager.AssignActor(task_id, assignment_id);
  ASSERT_TRUE(actor_worker_assignment_manager.GetAssignment(task_id) != nullptr);

  ASSERT_TRUE(actor_worker_assignment_manager.OnActorAssigned(task_id));
  ASSERT_TRUE(actor_worker_assignment_manager.GetAssignment(task_id) == nullptr);
}

TEST_F(ActorWorkerAssignmentManagerTest, TestBindAndCancel) {
  boost::asio::io_context io_context;
  ActorWorkerAssignmentManager actor_worker_assignment_manager(
      io_context, /*gcs_task_scheduling_enabled=*/true);

  auto job_id = JobID::FromInt(1);
  auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto task_id = TaskID::ForActorCreationTask(actor_id);
  auto assignment_id = UniqueID::FromRandom();

  actor_worker_assignment_manager.AssignActor(task_id, assignment_id);

  auto process = Process::FromPid(1024);
  actor_worker_assignment_manager.BindAssignmentToProcess(assignment_id, process);
  auto process_and_assignment =
      actor_worker_assignment_manager.GetBindProcessAndAssignment(task_id);
  ASSERT_EQ(process_and_assignment.first.GetId(), process.GetId());
  ASSERT_EQ(process_and_assignment.second, assignment_id);

  RAY_UNUSED(actor_worker_assignment_manager.CancelActorsInProcess(
      process, /*mark_assignment_as_invalid=*/true));
  process_and_assignment =
      actor_worker_assignment_manager.GetBindProcessAndAssignment(task_id);
  ASSERT_FALSE(process_and_assignment.first.IsValid());
  ASSERT_TRUE(process_and_assignment.second.IsNil());
}

}  // namespace raylet
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
