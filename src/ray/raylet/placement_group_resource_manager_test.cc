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

// clang-format off
#include "ray/raylet/placement_group_resource_manager.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/test/gcs_test_util.h"
// clang-format on

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"

namespace ray {

namespace gcs {

class MockNodeInfoAccessor : public BasicNodeInfoAccessor {
 public:
  using BasicNodeInfoAccessor::BasicNodeInfoAccessor;
  MOCK_CONST_METHOD2(Get, const rpc::BasicGcsNodeInfo *(const NodeID &node_id,
                                                        bool filter_dead_nodes));
};

class MockGcsClient : public ServiceBasedGcsClient {
 public:
  explicit MockGcsClient() : ServiceBasedGcsClient(GcsClientOptions()) {
    mock_node_accessor = new MockNodeInfoAccessor(this);
    this->node_accessor_.reset(mock_node_accessor);
  }

  MockNodeInfoAccessor *mock_node_accessor;
};
}  // namespace gcs

class NewPlacementGroupResourceManagerTest : public ::testing::Test {
 public:
  std::unique_ptr<raylet::NewPlacementGroupResourceManager>
      new_placement_group_resource_manager_;
  std::shared_ptr<ClusterResourceScheduler> cluster_resource_scheduler_;
  std::unique_ptr<gcs::MockGcsClient> gcs_client_;
  rpc::BasicGcsNodeInfo node_info_;
  void SetUp() {
    gcs_client_ = std::make_unique<gcs::MockGcsClient>();
    EXPECT_CALL(*gcs_client_->mock_node_accessor, Get(::testing::_, ::testing::_))
        .WillRepeatedly(::testing::Return(&node_info_));
  }
  void InitLocalAvailableResource(
      const std::unordered_map<std::string, double> &unit_resource) {
    cluster_resource_scheduler_ = std::make_shared<ClusterResourceScheduler>(
        NodeID::FromRandom().Binary(), unit_resource, *gcs_client_);
    new_placement_group_resource_manager_.reset(
        new raylet::NewPlacementGroupResourceManager(
            cluster_resource_scheduler_,
            [this](const ray::gcs::NodeResourceInfoAccessor::ResourceMap &resources) {
              update_called_ = true;
            },
            [this](const std::vector<std::string> &resource_names) {
              delete_called_ = true;
            }));
  }

  void CheckAvailableResoueceEmpty(const std::string &resource) {
    ASSERT_TRUE(cluster_resource_scheduler_->IsAvailableResourceEmpty(resource));
  }

  void CheckRemainingResourceCorrect(NodeResources &node_resources) {
    auto local_node_resource = cluster_resource_scheduler_->GetLocalNodeResources();
    ASSERT_TRUE(local_node_resource == node_resources);
  }

  bool update_called_ = false;
  bool delete_called_ = false;
};

TEST_F(NewPlacementGroupResourceManagerTest, TestNewPrepareBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  /// 4. check remaining resources is correct.
  CheckAvailableResoueceEmpty("CPU");
}

TEST_F(NewPlacementGroupResourceManagerTest,
       TestNewPrepareBundleWithInsufficientResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 2.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 1.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare bundle resource.
  ASSERT_FALSE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewCommitBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  ASSERT_FALSE(update_called_);
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  ASSERT_TRUE(update_called_);
  /// 4. check remaining resources is correct.
  std::unordered_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 1.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewReturnBundleResource) {
  // 1. create bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  InitLocalAvailableResource(unit_resource);
  /// 3. prepare and commit bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  ASSERT_FALSE(update_called_);
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  ASSERT_TRUE(update_called_);
  /// 4. return bundle resource.
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->ReturnBundle(*bundle_spec);
  ASSERT_TRUE(delete_called_);
  /// 5. check remaining resources is correct.
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", unit_resource, *gcs_client_);
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewMultipleBundlesCommitAndReturn) {
  // 1. create two bundles spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto first_bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  auto second_bundle_spec = Mocker::GenBundleCreation(group_id, 2, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> init_unit_resource;
  init_unit_resource.insert({"CPU", 2.0});
  InitLocalAvailableResource(init_unit_resource);
  /// 3. prepare and commit two bundle resource.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*first_bundle_spec));
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*second_bundle_spec));
  ASSERT_FALSE(update_called_);
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->CommitBundle(*first_bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(*second_bundle_spec);
  ASSERT_TRUE(update_called_);
  ASSERT_FALSE(delete_called_);
  /// 4. check remaining resources is correct after commit phase.
  std::unordered_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 2.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU_group_2_" + group_id.Hex(), 1.0},
      {"CPU", 2.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_2_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 2000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      init_unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();

  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 5. return second bundle.
  ASSERT_TRUE(update_called_);
  ASSERT_FALSE(delete_called_);
  new_placement_group_resource_manager_->ReturnBundle(*second_bundle_spec);
  ASSERT_TRUE(update_called_);
  ASSERT_TRUE(delete_called_);
  /// 6. check remaining resources is correct after return second bundle.
  remaining_resources = {{"CPU_group_" + group_id.Hex(), 1.0},
                         {"CPU_group_1_" + group_id.Hex(), 1.0},
                         {"CPU", 2.0},
                         {"bundle_group_1_" + group_id.Hex(), 1000},
                         {"bundle_group_" + group_id.Hex(), 1000}};
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::unordered_map<std::string, double> allocate_resource = {{"CPU", 1.0}};
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      allocate_resource, resource_instances));
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
  /// 7. return first bundle.
  new_placement_group_resource_manager_->ReturnBundle(*first_bundle_spec);
  /// 8. check remaining resources is correct after all bundle returned.
  remaining_resources = {{"CPU", 2.0}};
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  ASSERT_TRUE(update_called_);
  ASSERT_TRUE(delete_called_);
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithMultiPrepare) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle resource 10 times.
  for (int i = 0; i < 10; i++) {
    new_placement_group_resource_manager_->PrepareBundle(*bundle_spec);
  }
  /// 4. check remaining resources is correct.
  std::unordered_map<std::string, double> remaining_resources = {{"CPU", 3.0}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest, TestNewIdempotencyWithRandomOrder) {
  // 1. create one bundle spec.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  auto bundle_spec = Mocker::GenBundleCreation(group_id, 1, unit_resource);
  /// 2. init local available resource.
  std::unordered_map<std::string, double> available_resource = {
      std::make_pair("CPU", 3.0)};
  InitLocalAvailableResource(available_resource);
  /// 3. prepare bundle -> commit bundle -> prepare bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  /// 4. check remaining resources is correct.
  std::unordered_map<std::string, double> remaining_resources = {
      {"CPU_group_" + group_id.Hex(), 1.0},
      {"CPU_group_1_" + group_id.Hex(), 1.0},
      {"CPU", 3.0},
      {"bundle_group_1_" + group_id.Hex(), 1000},
      {"bundle_group_" + group_id.Hex(), 1000}};
  auto remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", remaining_resources, *gcs_client_);
  std::shared_ptr<TaskResourceInstances> resource_instances =
      std::make_shared<TaskResourceInstances>();
  ASSERT_TRUE(remaining_resource_scheduler->AllocateLocalTaskResources(
      unit_resource, resource_instances));
  auto remaining_resource_instance =
      remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
  new_placement_group_resource_manager_->ReturnBundle(*bundle_spec);
  // 5. prepare bundle -> commit bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  // 6. check remaining resources is correct.
  CheckRemainingResourceCorrect(remaining_resource_instance);
  new_placement_group_resource_manager_->ReturnBundle(*bundle_spec);
  // 7. prepare bundle -> return bundle -> commit bundle.
  ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
  new_placement_group_resource_manager_->ReturnBundle(*bundle_spec);
  new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  // 8. check remaining resources is correct.
  remaining_resource_scheduler = std::make_shared<ClusterResourceScheduler>(
      "remaining", available_resource, *gcs_client_);
  remaining_resource_instance = remaining_resource_scheduler->GetLocalNodeResources();
  CheckRemainingResourceCorrect(remaining_resource_instance);
}

TEST_F(NewPlacementGroupResourceManagerTest,
       TestReourceInstanceOrderWhenRemovingBundles) {
  // 1.Init local resources.
  std::unordered_map<std::string, double> available_resource = {
      std::make_pair("CPU", 7.0)};
  InitLocalAvailableResource(available_resource);
  // 2.Create a placement group with 7 bundles.
  auto group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::unordered_map<std::string, double> unit_resource;
  unit_resource.insert({"CPU", 1.0});
  for (int i = 0; i < 7; i++) {
    auto bundle_spec = Mocker::GenBundleCreation(group_id, i, unit_resource);
    ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
    new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  }
  RAY_LOG(INFO) << cluster_resource_scheduler_->DebugString();
  // 3.Mock to remove bundles [0,5,6].
  auto removing_bundle_spec1 = Mocker::GenBundleCreation(group_id, 0, unit_resource);
  auto removing_bundle_spec2 = Mocker::GenBundleCreation(group_id, 5, unit_resource);
  auto removing_bundle_spec3 = Mocker::GenBundleCreation(group_id, 6, unit_resource);
  new_placement_group_resource_manager_->ReturnBundle(*removing_bundle_spec1);
  new_placement_group_resource_manager_->ReturnBundle(*removing_bundle_spec2);
  new_placement_group_resource_manager_->ReturnBundle(*removing_bundle_spec3);
  RAY_LOG(INFO) << cluster_resource_scheduler_->DebugString();
  const auto &local_resource = cluster_resource_scheduler_->GetLocalNodeResources();
  // Get the pg resource name without index.
  const auto &resource_name_without_index = FormatPlacementGroupResource("CPU", group_id);
  const auto &string_id_map = cluster_resource_scheduler_->GetStringIdMap();
  const auto &resource_id = string_id_map.Get(resource_name_without_index);

  const auto &iter = local_resource.custom_resources.find(resource_id);
  ASSERT_TRUE(iter != local_resource.custom_resources.end());
  ASSERT_TRUE(iter->second.total == 4);
  // 4.Mock to add bundles [7, 8, 9].
  for (int i = 7; i < 10; i++) {
    auto bundle_spec = Mocker::GenBundleCreation(group_id, i, unit_resource);
    ASSERT_TRUE(new_placement_group_resource_manager_->PrepareBundle(*bundle_spec));
    new_placement_group_resource_manager_->CommitBundle(*bundle_spec);
  }
  const auto &new_iter = local_resource.custom_resources.find(resource_id);
  ASSERT_TRUE(new_iter != local_resource.custom_resources.end());
  ASSERT_TRUE(new_iter->second.total == 7);
  // Make sure all the resource instance is less than or equal to 1.
  const auto &local_resource_instance = cluster_resource_scheduler_->GetLocalResources();
  const auto &instance_iter = local_resource_instance.custom_resources.find(resource_id);
  ASSERT_TRUE(instance_iter != local_resource_instance.custom_resources.end());
  for (size_t i = 0; i < instance_iter->second.total.size(); i++) {
    ASSERT_TRUE(instance_iter->second.total[i] <= 1);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
