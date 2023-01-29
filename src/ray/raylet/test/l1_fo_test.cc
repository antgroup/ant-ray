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

#include "gtest/gtest.h"

namespace ray {
namespace raylet {

class L1FOTest : public ::testing::Test {};

TEST_F(L1FOTest, TestRightOrder) {
  NodeManagerL1Handler handler;
  JobID job_id = JobID::FromInt(1);
  handler.HandleJobBanNewTasks(job_id, 1);
  ASSERT_TRUE(handler.IsJobBanningNewTasks(job_id));
  handler.HandleJobLiftNewTasksBan(job_id, 2);
  ASSERT_TRUE(!handler.IsJobBanningNewTasks(job_id));
}

TEST_F(L1FOTest, TestSingleWrongOrder) {
  NodeManagerL1Handler handler;
  JobID job_id = JobID::FromInt(1);
  handler.HandleJobLiftNewTasksBan(job_id, 2);
  handler.HandleJobBanNewTasks(job_id, 1);
  ASSERT_TRUE(!handler.IsJobBanningNewTasks(job_id));
}

TEST_F(L1FOTest, TestTwoWrongOrder) {
  NodeManagerL1Handler handler;
  JobID job_id = JobID::FromInt(1);
  handler.HandleJobBanNewTasks(job_id, 1);
  ASSERT_TRUE(handler.IsJobBanningNewTasks(job_id));
  handler.HandleJobBanNewTasks(job_id, 3);
  ASSERT_TRUE(handler.IsJobBanningNewTasks(job_id));
  handler.HandleJobLiftNewTasksBan(job_id, 2);
  ASSERT_TRUE(handler.IsJobBanningNewTasks(job_id));
  handler.HandleJobLiftNewTasksBan(job_id, 4);
  ASSERT_TRUE(!handler.IsJobBanningNewTasks(job_id));
}

}  // namespace raylet
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
