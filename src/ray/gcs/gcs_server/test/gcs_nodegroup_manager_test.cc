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

#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

class GcsNodegroupManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_nodegroup_manager_ = std::make_shared<gcs::GcsNodegroupManager>(
        gcs_table_storage_, gcs_pub_sub_,
        /*has_any_running_jobs_in_nodegroup_fn=*/
        [this](const std::string &nodegroup_id) { return has_any_running_jobs_; });
  }

  rpc::CreateOrUpdateNodegroupRequest GenCreateOrUpdateNodegroupRequest(
      const std::string &nodegroup_id, int node_count) {
    rpc::CreateOrUpdateNodegroupRequest request;
    request.set_nodegroup_id(nodegroup_id);
    rpc::NodeShapeAndCount node_shape_and_count;
    node_shape_and_count.set_node_count(node_count);
    request.add_node_shape_and_count_list()->CopyFrom(node_shape_and_count);
    return request;
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::InMemoryGcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsNodegroupManager> gcs_nodegroup_manager_;
  bool has_any_running_jobs_ = false;
};

TEST_F(GcsNodegroupManagerTest, TestEmptyNodegroup) {
  rpc::CreateOrUpdateNodegroupRequest request;
  ASSERT_TRUE(
      gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).IsInvalid());
}

TEST_F(GcsNodegroupManagerTest, TestRegiserNodeBeforeCreateOrUpdateNodegroup) {
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> all_nodes;
  int node_count = 10;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    all_nodes.emplace(NodeID::FromBinary(node->basic_gcs_node_info().node_id()), node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  const auto &node_to_namespeces = gcs_nodegroup_manager_->GetNodeToNodegroupsMap();
  for (const auto &entry : all_nodes) {
    auto iter = node_to_namespeces.find(entry.first);
    ASSERT_TRUE(iter != node_to_namespeces.end() && iter->second.size() == 1 &&
                iter->second.contains(NODEGROUP_RESOURCE_DEFAULT));
  }

  auto request = GenCreateOrUpdateNodegroupRequest("nodegroup_1", node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  for (const auto &entry : all_nodes) {
    auto iter = node_to_namespeces.find(entry.first);
    ASSERT_TRUE(iter != node_to_namespeces.end() &&
                iter->second.contains(request.nodegroup_id()));
  }
}

TEST_F(GcsNodegroupManagerTest, TestCreateOrUpdateNodegroupCompatibility) {
  int node_count = 100;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    (*node->mutable_resources_total())["CPU"] = 8.0;
    (*node->mutable_resources_total())["memory"] = 200.0;
    gcs_nodegroup_manager_->AddNode(*node);
  }

  auto request = GenCreateOrUpdateNodegroupRequest("nodegroup_1", node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());
}

TEST_F(GcsNodegroupManagerTest, CalculateDifference) {
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> all_nodes;
  size_t node_count = 100;
  for (size_t i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    (*node->mutable_resources_total())["CPU"] = 8.0;
    (*node->mutable_resources_total())["memory"] = 200.0;
    all_nodes.emplace(NodeID::FromBinary(node->basic_gcs_node_info().node_id()), node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  std::string nodegroup_id = "nodegroup_1";

  rpc::CreateOrUpdateNodegroupRequest request;
  request.set_nodegroup_id(nodegroup_id);
  rpc::NodeShapeAndCount node_shape_and_count;
  node_shape_and_count.set_node_count(node_count);
  request.add_node_shape_and_count_list()->CopyFrom(node_shape_and_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  size_t unregistered_node_count = 10;
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> unregistered_nodes;
  for (auto &entry : all_nodes) {
    gcs_nodegroup_manager_->RemoveNode(entry.second);
    unregistered_nodes.emplace(entry);
    if (unregistered_nodes.size() == unregistered_node_count) {
      break;
    }
  }

  size_t new_node_count = 90;
  node_shape_and_count.set_node_count(new_node_count);
  request.mutable_node_shape_and_count_list(0)->CopyFrom(node_shape_and_count);

  absl::flat_hash_map<NodeShape, int> to_be_added_node_shape_and_count;
  absl::flat_hash_set<std::string> to_be_removed_hosts;
  gcs_nodegroup_manager_->CalculateDifference(
      nodegroup_id, request.node_shape_and_count_list(),
      &to_be_added_node_shape_and_count, &to_be_removed_hosts);

  ASSERT_EQ(to_be_removed_hosts.size(), unregistered_node_count);
  ASSERT_EQ(to_be_added_node_shape_and_count.size(), 0);
  for (auto &entry : unregistered_nodes) {
    to_be_removed_hosts.contains(
        entry.second->basic_gcs_node_info().node_manager_hostname());
  }
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  new_node_count = 95;
  to_be_added_node_shape_and_count.clear();
  to_be_removed_hosts.clear();
  node_shape_and_count.set_node_count(new_node_count);
  request.mutable_node_shape_and_count_list(0)->CopyFrom(node_shape_and_count);
  gcs_nodegroup_manager_->CalculateDifference(
      nodegroup_id, request.node_shape_and_count_list(),
      &to_be_added_node_shape_and_count, &to_be_removed_hosts);
  ASSERT_EQ(to_be_removed_hosts.size(), 0);
  ASSERT_EQ(to_be_added_node_shape_and_count.size(), 1);
  ASSERT_EQ(to_be_added_node_shape_and_count.begin()->second, 5);
  ASSERT_FALSE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  size_t delt = 5;
  for (auto &entry : unregistered_nodes) {
    gcs_nodegroup_manager_->AddNode(*entry.second);
    if (--delt == 0) {
      break;
    }
  }

  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}