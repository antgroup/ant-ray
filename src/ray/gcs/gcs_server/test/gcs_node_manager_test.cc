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

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
class GcsNodeManagerTest : public ::testing::Test {};

TEST_F(GcsNodeManagerTest, TestManagement) {
  instrumented_io_context io_service;
  auto gcs_pub_sub = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
  auto gcs_table_storage = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service);
  gcs::GcsNodeManager node_manager(gcs_pub_sub, gcs_table_storage);
  // Test Add/Get/Remove functionality.
  auto node = Mocker::GenNodeInfo();
  auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());

  node_manager.AddNode(node);
  ASSERT_EQ(node, node_manager.GetAliveNode(node_id));

  node_manager.RemoveNode(node_id);
  ASSERT_FALSE(node_manager.GetAliveNode(node_id).has_value());
}

TEST_F(GcsNodeManagerTest, TestListener) {
  instrumented_io_context io_service;
  auto gcs_pub_sub = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
  auto gcs_table_storage = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service);
  gcs::GcsNodeManager node_manager(gcs_pub_sub, gcs_table_storage);
  // Test AddNodeAddedListener.
  int node_count = 1000;
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> added_nodes;
  node_manager.AddNodeAddedListener(
      [&added_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        added_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node_manager.AddNode(node);
  }
  ASSERT_EQ(node_count, added_nodes.size());

  // Test GetAllAliveNodes.
  auto &alive_nodes = node_manager.GetAllAliveNodes();
  ASSERT_EQ(added_nodes.size(), alive_nodes.size());
  for (const auto &node : added_nodes) {
    ASSERT_EQ(
        1, alive_nodes.count(NodeID::FromBinary(node->basic_gcs_node_info().node_id())));
  }

  // Test AddNodeRemovedListener.
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> removed_nodes;
  node_manager.AddNodeRemovedListener(
      [&removed_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        removed_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    node_manager.RemoveNode(
        NodeID::FromBinary(added_nodes[i]->basic_gcs_node_info().node_id()));
  }
  ASSERT_EQ(node_count, removed_nodes.size());
  ASSERT_TRUE(node_manager.GetAllAliveNodes().empty());
  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(added_nodes[i], removed_nodes[i]);
  }
}

TEST_F(GcsNodeManagerTest, TestGetAllNodeInfoByNodegroup) {
  instrumented_io_context io_service;
  auto gcs_pub_sub = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
  auto gcs_table_storage = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service);
  absl::flat_hash_map<NodeID, absl::flat_hash_set<std::string>> node_to_nodegroups;
  gcs::GcsNodeManager node_manager(
      gcs_pub_sub, gcs_table_storage,
      [&node_to_nodegroups](const NodeID &node_id, const std::string &nodegroup_id) {
        if (nodegroup_id.empty()) {
          return true;
        }
        auto iter = node_to_nodegroups.find(node_id);
        return iter != node_to_nodegroups.end() && iter->second.contains(nodegroup_id);
      });
  // Test AddNodeAddedListener.
  int node_count = 1000;
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> added_nodes;
  node_manager.AddNodeAddedListener(
      [&added_nodes](std::shared_ptr<rpc::GcsNodeInfo> node) {
        added_nodes.emplace_back(std::move(node));
      });
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node_manager.AddNode(node);
  }
  ASSERT_EQ(node_count, added_nodes.size());

  // Update nodegroup resources.
  const std::string nodegroup_id = "nodegroup";
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodegroup_nodes;
  for (int i = 0; i < node_count; ++i) {
    auto node_id = NodeID::FromBinary(added_nodes[i]->basic_gcs_node_info().node_id());
    node_to_nodegroups[node_id].emplace(NODEGROUP_RESOURCE_DEFAULT);
    if (i % 2 == 0) {
      node_to_nodegroups[node_id].emplace(nodegroup_id);
      nodegroup_nodes.emplace_back(added_nodes[i]);
    }
  }

  // Check specified nodegroup.
  auto nodes = node_manager.GetAllNodes(nodegroup_id);
  ASSERT_EQ(nodes.size(), nodegroup_nodes.size());
  for (auto &entry : nodes) {
    ASSERT_TRUE(std::find(nodegroup_nodes.begin(), nodegroup_nodes.end(), entry.second) !=
                nodegroup_nodes.end());
  }

  // Check empty nodegroup.
  nodes = node_manager.GetAllNodes();
  ASSERT_EQ(nodes.size(), added_nodes.size());
  for (auto &entry : nodes) {
    ASSERT_TRUE(std::find(added_nodes.begin(), added_nodes.end(), entry.second) !=
                added_nodes.end());
  }

  // Check default nodegroup.
  nodes = node_manager.GetAllNodes(NODEGROUP_RESOURCE_DEFAULT);
  ASSERT_EQ(nodes.size(), added_nodes.size());
  for (auto &entry : nodes) {
    ASSERT_TRUE(std::find(added_nodes.begin(), added_nodes.end(), entry.second) !=
                added_nodes.end());
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
