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

#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/test/gcs_resource_scheduler_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;

class GcsResourceSchedulerTest : public GcsResourceSchedulerBaseTest {
 public:
  void CheckClusterAvailableResources(const NodeID &node_id,
                                      const std::string &resource_name,
                                      double resource_value) {
    const auto &cluster_resource = gcs_resource_manager_->GetClusterResources();
    auto iter = cluster_resource.find(node_id);
    ASSERT_TRUE(iter != cluster_resource.end());
    ASSERT_EQ(iter->second->GetAvailableResources().GetResource(resource_name).ToDouble(),
              resource_value);
  }

  void TestResourceLeaks(const gcs::SchedulingType &scheduling_type) {
    // Add node resources.
    const auto &node_id = NodeID::FromRandom();
    const std::string cpu_resource = "CPU";
    const double node_cpu_num = 6.0;
    AddNodeResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling succeeded and node resources are used up.
    std::vector<ResourceSet> required_resources_list;
    std::unordered_map<std::string, double> resource_map;
    for (int bundle_cpu_num = 1; bundle_cpu_num <= 3; ++bundle_cpu_num) {
      resource_map[cpu_resource] = bundle_cpu_num;
      required_resources_list.emplace_back(resource_map);
    }
    std::vector<const ResourceSet *> required_resources_ref_list;
    for (auto &resource_set : required_resources_list) {
      required_resources_ref_list.emplace_back(&resource_set);
    }
    auto schedule_context = std::make_unique<gcs::GcsActorScheduleContext>(job_id_);
    ASSERT_TRUE(gcs_resource_scheduler_->Schedule(
        required_resources_ref_list, scheduling_type, schedule_context.get()));
    ASSERT_EQ(schedule_context->selected_nodes.size(), 3);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);

    // Scheduling failure.
    schedule_context.reset(new gcs::GcsActorScheduleContext(job_id_));
    resource_map[cpu_resource] = 5;
    required_resources_list.emplace_back(resource_map);
    required_resources_ref_list.emplace_back(&required_resources_list.back());
    ASSERT_FALSE(gcs_resource_scheduler_->Schedule(
        required_resources_ref_list, scheduling_type, schedule_context.get()));
    ASSERT_EQ(schedule_context->selected_nodes.size(), 0);

    // Check for resource leaks.
    CheckClusterAvailableResources(node_id, cpu_resource, node_cpu_num);
  }
};

TEST_F(GcsResourceSchedulerTest, TestPackScheduleResourceLeaks) {
  TestResourceLeaks(gcs::SchedulingType::PACK);
}

TEST_F(GcsResourceSchedulerTest, TestSpreadScheduleResourceLeaks) {
  TestResourceLeaks(gcs::SchedulingType::SPREAD);
}

TEST_F(GcsResourceSchedulerTest, TestNodeFilter) {
  // Add node resources.
  const auto &node_id = NodeID::FromRandom();
  const std::string cpu_resource = "CPU";
  const double node_cpu_num = 10.0;
  AddNodeResources(node_id, cpu_resource, node_cpu_num);

  // Set node is unschedulable.
  is_node_schedulable_ = false;
  // Scheduling failure.
  auto schedule_context = std::make_unique<gcs::GcsActorScheduleContext>(job_id_);
  std::vector<ResourceSet> required_resources_list;
  std::unordered_map<std::string, double> resource_map;
  resource_map[cpu_resource] = 1;
  required_resources_list.emplace_back(resource_map);

  std::vector<const ResourceSet *> required_resources_ref_list;
  for (auto &resource_set : required_resources_list) {
    required_resources_ref_list.emplace_back(&resource_set);
  }

  ASSERT_FALSE(gcs_resource_scheduler_->Schedule(required_resources_ref_list,
                                                 gcs::SchedulingType::STRICT_SPREAD,
                                                 schedule_context.get()));
  ASSERT_EQ(schedule_context->selected_nodes.size(), 0);

  // Set node is schedulable.
  is_node_schedulable_ = true;
  // Scheduling succeeded.
  schedule_context.reset(new gcs::GcsActorScheduleContext(job_id_));
  ASSERT_TRUE(gcs_resource_scheduler_->Schedule(required_resources_ref_list,
                                                gcs::SchedulingType::STRICT_SPREAD,
                                                schedule_context.get()));
  ASSERT_EQ(schedule_context->selected_nodes.size(), 1);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
