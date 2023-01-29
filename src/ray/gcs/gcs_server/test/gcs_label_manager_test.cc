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

#include "ray/gcs/gcs_server/gcs_label_manager.h"

#include <memory>
#include <string>
#include <unordered_map>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

class GcsLabelManagerTest : public ::testing::Test {
 public:
  GcsLabelManagerTest() { gcs_label_manager_ = std::make_shared<gcs::GcsLabelManager>(); }

  std::shared_ptr<gcs::GcsActor> GenGcsActor(
      const std::string nodegroup_id, const std::string &ray_namespace,
      const NodeID &node_id, const std::unordered_map<std::string, std::string> &labels) {
    rpc::Address owner_address;
    rpc::Address address;
    address.set_raylet_id(node_id.Binary());
    auto actor_creating_task_spec = Mocker::GenActorCreationTask(
        job_id_, /*max_restarts=*/1, /*detached=*/true, /*name=*/"", owner_address,
        Language::JAVA, {}, {}, {}, labels);
    auto actor = std::make_shared<gcs::GcsActor>(actor_creating_task_spec.GetMessage(),
                                                 /*ray_namespace=*/ray_namespace,
                                                 /*nodegroup_id=*/nodegroup_id);
    actor->UpdateAddress(address);
    return actor;
  }

  std::shared_ptr<gcs::GcsLabelManager> gcs_label_manager_;
  JobID job_id_ = JobID::FromInt(1);
  std::string nodegroupd_1 = "nodegroupd_1";
  std::string nodegroupd_2 = "nodegroupd_2";
  std::string default_namespace = "default_namespace";
  std::string namespace_2 = "namespace_2";
};

TEST_F(GcsLabelManagerTest, TestBasic) {
  // check query when init step.
  auto debug_string_init = gcs_label_manager_->DebugString();
  auto seleted_0_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"master"});
  ASSERT_TRUE(seleted_0_1.empty());
  auto seleted_0_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_TRUE(seleted_0_2.empty());

  auto node_1 = NodeID::FromRandom();
  auto actor_1 = GenGcsActor(nodegroupd_1, default_namespace, node_1,
                             {{"type", "master"}, {"actor_1", "1"}});
  auto node_2 = NodeID::FromRandom();
  auto actor_2 = GenGcsActor(nodegroupd_1, default_namespace, node_2,
                             {{"type", "slave"}, {"actor_2", "1"}});
  gcs_label_manager_->AddActorLabels(actor_1);
  auto debug_string_only_1 = gcs_label_manager_->DebugString();
  gcs_label_manager_->AddActorLabels(actor_2);
  auto debug_string_both_2 = gcs_label_manager_->DebugString();

  // check GetNodesByKeyAndValue
  absl::flat_hash_set<NodeID> two_node{node_1, node_2};
  auto seleted_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"master"});
  ASSERT_EQ(seleted_1, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_2 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave"});
  ASSERT_EQ(seleted_2, absl::flat_hash_set<NodeID>{node_2});
  auto seleted_3 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave", "master"});
  ASSERT_EQ(seleted_3, two_node);
  auto seleted_4 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "actor_1", {"1"});
  ASSERT_EQ(seleted_4, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_5 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"s", "m"});
  ASSERT_TRUE(seleted_5.empty());
  auto seleted_6 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "error key", {"slave"});
  ASSERT_TRUE(seleted_6.empty());
  auto seleted_other_nodegroup = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_2, default_namespace, "type", {"slave"});
  ASSERT_TRUE(seleted_other_nodegroup.empty());

  // check GetNodesByKey
  auto seleted_2_1 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_EQ(seleted_2_1, two_node);
  auto seleted_2_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "actor_1");
  ASSERT_EQ(seleted_2_2, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_2_3 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "actor_2");
  ASSERT_EQ(seleted_2_3, absl::flat_hash_set<NodeID>{node_2});
  auto seleted_2_4 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "error key");
  ASSERT_TRUE(seleted_2_4.empty());
  auto seleted_2_5 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_2, default_namespace, "actor_1");
  ASSERT_TRUE(seleted_2_5.empty());
  auto seleted_2_6 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, namespace_2, "actor_1");
  ASSERT_TRUE(seleted_2_6.empty());
  auto seleted_2_7 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_2, namespace_2, "actor_1");
  ASSERT_TRUE(seleted_2_7.empty());

  // remove actor_2
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_both_2);
  gcs_label_manager_->RemoveActorLabels(actor_2);
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_only_1);
  auto seleted_3_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"master"});
  ASSERT_EQ(seleted_3_1, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_3_2 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave"});
  ASSERT_TRUE(seleted_3_2.empty());
  auto seleted_3_3 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_EQ(seleted_3_3, absl::flat_hash_set<NodeID>{node_1});

  // remove actor_1
  gcs_label_manager_->RemoveActorLabels(actor_1);
  auto seleted_4_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"master"});
  ASSERT_TRUE(seleted_4_1.empty());
  auto seleted_4_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_TRUE(seleted_4_2.empty());
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_init);

  // readd actor.
  auto actor_3 = GenGcsActor(nodegroupd_1, default_namespace, node_1,
                             {{"type", "master"}, {"actor_1", "1"}});
  gcs_label_manager_->AddActorLabels(actor_1);
  gcs_label_manager_->AddActorLabels(actor_2);
  gcs_label_manager_->AddActorLabels(actor_3);

  auto seleted_5_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave", "master"});
  ASSERT_EQ(seleted_5_1, two_node);
  gcs_label_manager_->DebugString();
}

TEST_F(GcsLabelManagerTest, TestRepeatExec) {
  auto debug_string_init = gcs_label_manager_->DebugString();
  auto node_1 = NodeID::FromRandom();
  auto actor_1 = GenGcsActor(nodegroupd_1, default_namespace, node_1,
                             {{"type", "master"}, {"actor_1", "1"}});
  auto node_2 = NodeID::FromRandom();
  auto actor_2 = GenGcsActor(nodegroupd_1, default_namespace, node_2,
                             {{"type", "slave"}, {"actor_2", "1"}});
  gcs_label_manager_->AddActorLabels(actor_1);
  gcs_label_manager_->AddActorLabels(actor_2);
  auto debug_string_2 = gcs_label_manager_->DebugString();
  gcs_label_manager_->AddActorLabels(actor_1);
  gcs_label_manager_->AddActorLabels(actor_2);
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_2);
  gcs_label_manager_->RemoveActorLabels(actor_1);
  gcs_label_manager_->RemoveActorLabels(actor_2);
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_init);
  gcs_label_manager_->RemoveActorLabels(actor_1);
  gcs_label_manager_->RemoveActorLabels(actor_2);
  ASSERT_EQ(gcs_label_manager_->DebugString(), debug_string_init);
}

TEST_F(GcsLabelManagerTest, TestManyNodeAndActor) {
  auto debug_string_init = gcs_label_manager_->DebugString();
  absl::flat_hash_set<NodeID> all_nodes;
  absl::flat_hash_set<std::shared_ptr<gcs::GcsActor>> all_actors;
  for (int i = 0; i < 1000; i++) {
    auto node = NodeID::FromRandom();
    all_nodes.emplace(node);
    auto actor =
        GenGcsActor(nodegroupd_1, default_namespace, node,
                    {{"type", "master"}, {"key", "value"}, {"no", std::to_string(i)}});
    gcs_label_manager_->AddActorLabels(actor);
    auto actor_2 = GenGcsActor(nodegroupd_1, default_namespace, node,
                               {{"type", "slave"}, {"key", "value"}});
    gcs_label_manager_->AddActorLabels(actor_2);
    all_actors.emplace(actor);
    all_actors.emplace(actor_2);
  }
  auto select_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave", "master"});
  ASSERT_EQ(select_1, all_nodes);

  for (const auto &actor : all_actors) {
    gcs_label_manager_->RemoveActorLabels(actor);
  }
  auto select_2 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"slave", "master"});
  ASSERT_TRUE(select_2.empty());
}

TEST_F(GcsLabelManagerTest, TestEmptyString) {
  // check query when init step.
  auto seleted_0_1 =
      gcs_label_manager_->GetNodesByKeyAndValue("", "", "type", {"master"});
  ASSERT_TRUE(seleted_0_1.empty());
  auto seleted_0_2 = gcs_label_manager_->GetNodesByKey("", "", "type");
  ASSERT_TRUE(seleted_0_2.empty());

  auto node_1 = NodeID::FromRandom();
  auto actor_1 = GenGcsActor("", "", node_1, {{"type", "master"}, {"actor_1", "1"}});
  gcs_label_manager_->AddActorLabels(actor_1);
  gcs_label_manager_->DebugString();

  // check query
  auto seleted_1 = gcs_label_manager_->GetNodesByKeyAndValue("", "", "type", {"master"});
  ASSERT_EQ(seleted_1, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_2 = gcs_label_manager_->GetNodesByKey("", "", "type");
  ASSERT_EQ(seleted_2, absl::flat_hash_set<NodeID>{node_1});

  auto seleted_1_1 =
      gcs_label_manager_->GetNodesByKeyAndValue(nodegroupd_1, "", "type", {"master"});
  ASSERT_TRUE(seleted_1_1.empty());
  auto seleted_1_2 = gcs_label_manager_->GetNodesByKey(nodegroupd_1, "", "type");
  ASSERT_TRUE(seleted_1_2.empty());
  auto seleted_2_1 = gcs_label_manager_->GetNodesByKeyAndValue("", default_namespace,
                                                               "type", {"master"});
  ASSERT_TRUE(seleted_2_1.empty());
  auto seleted_2_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_TRUE(seleted_2_2.empty());
}

TEST_F(GcsLabelManagerTest, TestNodegroupAndNamespace) {
  auto node_1 = NodeID::FromRandom();
  auto actor_1 =
      GenGcsActor(nodegroupd_1, default_namespace, node_1, {{"type", "master"}});
  gcs_label_manager_->AddActorLabels(actor_1);

  auto node_2 = NodeID::FromRandom();
  auto actor_2 =
      GenGcsActor(nodegroupd_2, default_namespace, node_2, {{"type", "master"}});
  gcs_label_manager_->AddActorLabels(actor_2);

  auto node_3 = NodeID::FromRandom();
  auto actor_3 = GenGcsActor(nodegroupd_2, namespace_2, node_3, {{"type", "master"}});
  gcs_label_manager_->AddActorLabels(actor_3);

  auto node_4 = NodeID::FromRandom();
  auto actor_4 = GenGcsActor(nodegroupd_1, namespace_2, node_4, {{"type", "master"}});
  gcs_label_manager_->AddActorLabels(actor_4);
  gcs_label_manager_->DebugString();

  // check query
  auto seleted_1_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_1, default_namespace, "type", {"master"});
  ASSERT_EQ(seleted_1_1, absl::flat_hash_set<NodeID>{node_1});
  auto seleted_1_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_1, default_namespace, "type");
  ASSERT_EQ(seleted_1_1, absl::flat_hash_set<NodeID>{node_1});

  auto seleted_2_1 = gcs_label_manager_->GetNodesByKeyAndValue(
      nodegroupd_2, default_namespace, "type", {"master"});
  ASSERT_EQ(seleted_2_1, absl::flat_hash_set<NodeID>{node_2});
  auto seleted_2_2 =
      gcs_label_manager_->GetNodesByKey(nodegroupd_2, default_namespace, "type");
  ASSERT_EQ(seleted_2_1, absl::flat_hash_set<NodeID>{node_2});

  auto seleted_3_1 = gcs_label_manager_->GetNodesByKeyAndValue(nodegroupd_2, namespace_2,
                                                               "type", {"master"});
  ASSERT_EQ(seleted_3_1, absl::flat_hash_set<NodeID>{node_3});
  auto seleted_3_2 = gcs_label_manager_->GetNodesByKey(nodegroupd_2, namespace_2, "type");
  ASSERT_EQ(seleted_3_1, absl::flat_hash_set<NodeID>{node_3});

  auto seleted_4_1 = gcs_label_manager_->GetNodesByKeyAndValue(nodegroupd_1, namespace_2,
                                                               "type", {"master"});
  ASSERT_EQ(seleted_4_1, absl::flat_hash_set<NodeID>{node_4});
  auto seleted_4_2 = gcs_label_manager_->GetNodesByKey(nodegroupd_1, namespace_2, "type");
  ASSERT_EQ(seleted_4_1, absl::flat_hash_set<NodeID>{node_4});
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
