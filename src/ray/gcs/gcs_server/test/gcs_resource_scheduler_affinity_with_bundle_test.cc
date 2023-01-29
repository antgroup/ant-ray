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

#include <fstream>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_resource_scheduler_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using ::testing::_;
class AffinityWithBundleSchedulerTest : public GcsResourceSchedulerBaseTest {
 public:
  void TestAffinityWithBundleScheduleSuccess(
      BundleID bundle_id, const std::unordered_map<std::string, double> &resources,
      NodeID except_node_id) {
    ResourceSet resource_set(
        AddPlacementGroupConstraint(resources, bundle_id.first, bundle_id.second));
    gcs::GcsActorAffinityWithBundleScheduleContext schedule_context(job_id_);
    schedule_context.SetPlacementGroupBundleID(bundle_id);
    schedule_context.SetPlacementGroupBundleLocationIndex(*pg_bundle_location_index_);
    ASSERT_TRUE(gcs_resource_scheduler_->Schedule(
        {&resource_set}, gcs::SchedulingType::AFFINITY_WITH_BUNDLE, &schedule_context));
    ASSERT_EQ(schedule_context.selected_nodes.size(), 1);
    auto selected_node_id = schedule_context.selected_nodes[0];
    if (!selected_node_id.IsNil()) {
      // Acquire the resources from the selected node.
      ASSERT_TRUE(
          gcs_resource_manager_->AcquireResources(selected_node_id, resource_set));
    }
    ASSERT_EQ(selected_node_id, except_node_id);
  }

  void TestAffinityWithBundleScheduleFailure(
      BundleID bundle_id, const std::unordered_map<std::string, double> &resources) {
    ResourceSet resource_set(
        AddPlacementGroupConstraint(resources, bundle_id.first, bundle_id.second));
    gcs::GcsActorAffinityWithBundleScheduleContext schedule_context(job_id_);
    schedule_context.SetPlacementGroupBundleID(bundle_id);
    schedule_context.SetPlacementGroupBundleLocationIndex(*pg_bundle_location_index_);
    ASSERT_FALSE(gcs_resource_scheduler_->Schedule(
        {&resource_set}, gcs::SchedulingType::AFFINITY_WITH_BUNDLE, &schedule_context));
    ASSERT_EQ(schedule_context.selected_nodes.size(), 0);
  }

 protected:
  void Init2Node1Pg2BungleEnv() {
    std::unordered_map<std::string, double> node_1_resources = {
        {"CPU", 1}, {"memory", 3250}, {"objectStoreMemory", 7}, {"MEM", 4}};
    const auto &node_1_pg_resources = AddPlacementGroupConstraint(
        {{"CPU", 1}, {"memory", 50}}, bundle_0.first, bundle_0.second);
    node_1_resources.insert(node_1_pg_resources.begin(), node_1_pg_resources.end());
    AddNodeResources(node_id_1, node_1_resources);

    std::unordered_map<std::string, double> node_2_resources = {
        {"CPU", 1}, {"memory", 3100}, {"objectStoreMemory", 7}, {"MEM", 4}};
    const auto &node_2_pg_resources = AddPlacementGroupConstraint(
        {{"CPU", 1}, {"memory", 50}}, bundle_1.first, bundle_1.second);
    node_2_resources.insert(node_2_pg_resources.begin(), node_2_pg_resources.end());
    AddNodeResources(node_id_2, node_2_resources);
  }
  NodeID node_id_1 = NodeID::FromRandom();
  NodeID node_id_2 = NodeID::FromRandom();
  PlacementGroupID pg_id_1 = PlacementGroupID::Of(JobID::FromInt(1));
  BundleID bundle_0 = std::make_pair(pg_id_1, 0);
  BundleID bundle_1 = std::make_pair(pg_id_1, 1);
};

TEST_F(AffinityWithBundleSchedulerTest, NormalTest) {
  // prepare node resources
  Init2Node1Pg2BungleEnv();

  TestAffinityWithBundleScheduleSuccess(bundle_0, {{"CPU", 1}}, node_id_1);

  TestAffinityWithBundleScheduleSuccess(bundle_1, {{"CPU", 1}}, node_id_2);

  // cpu resource not enough
  TestAffinityWithBundleScheduleFailure(bundle_1, {{"CPU", 1}});
  TestAffinityWithBundleScheduleSuccess(bundle_1, {{"memory", 50}}, node_id_2);

  BundleID bundle_id = std::make_pair(pg_id_1, -1);
  TestAffinityWithBundleScheduleFailure(bundle_id, {{"CPU", 1}});
  TestAffinityWithBundleScheduleSuccess(bundle_id, {{"memory", 50}}, node_id_1);
}

TEST_F(AffinityWithBundleSchedulerTest, FailureTest) {
  // prepare node resources
  Init2Node1Pg2BungleEnv();
  // not exist pg_id
  BundleID bundle_id_not_exist =
      std::make_pair(PlacementGroupID::Of(JobID::FromInt(1)), 0);
  TestAffinityWithBundleScheduleFailure(bundle_id_not_exist, {{"CPU", 1}});
  // not exist bundle index
  BundleID bundle_3 = std::make_pair(pg_id_1, 3);
  TestAffinityWithBundleScheduleFailure(bundle_3, {{"CPU", 1}});
  TestAffinityWithBundleScheduleFailure(bundle_0, {{"CPU", 10}});
}

TEST_F(AffinityWithBundleSchedulerTest, TestContextRequirePlacementGroup) {
  auto job_id = JobID::FromInt(1);
  gcs::GcsActorAffinityWithBundleScheduleContext context(job_id);
  context.ToString();
  ASSERT_FALSE(context.RequirePlacementGroup());
  context.SetPlacementGroupBundleID(TaskSpecification().PlacementGroupBundleId());
  ASSERT_FALSE(context.RequirePlacementGroup());
  auto bundle_id = std::make_pair(PlacementGroupID::Of(job_id), -1);
  context.SetPlacementGroupBundleID(bundle_id);
  ASSERT_TRUE(context.RequirePlacementGroup());
  context.ToString();
}

TEST_F(AffinityWithBundleSchedulerTest, TestAddPlacementGroupConstraint) {
  PlacementGroupID pg_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::string pg_id_hex = pg_id.Hex();
  auto resource_map = AddPlacementGroupConstraint({{"CPU", 1}}, pg_id, 0);
  std::unordered_map<std::string, double> except_map = {{"CPU_group_0_" + pg_id_hex, 1},
                                                        {"CPU_group_" + pg_id_hex, 1}};
  ASSERT_EQ(resource_map, except_map);

  auto resource_map_1 = AddPlacementGroupConstraint({{"CPU", 1}}, pg_id, 1);
  std::unordered_map<std::string, double> except_map_1 = {{"CPU_group_1_" + pg_id_hex, 1},
                                                          {"CPU_group_" + pg_id_hex, 1}};
  ASSERT_EQ(resource_map_1, except_map_1);

  auto resource_map_2 =
      AddPlacementGroupConstraint({{"CPU", 1}}, PlacementGroupID::Nil(), 1);
  std::unordered_map<std::string, double> except_map_2 = {{"CPU", 1}};
  ASSERT_EQ(resource_map_2, except_map_2);
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}