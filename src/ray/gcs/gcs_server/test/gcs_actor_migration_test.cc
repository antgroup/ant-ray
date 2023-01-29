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
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_migration_manager.h"
#include "ray/gcs/gcs_server/gcs_job_distribution_formatter.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {
class MockGcsActorMigrationManager : public GcsActorMigrationManager {
 public:
  MockGcsActorMigrationManager(instrumented_io_context &io_service)
      : GcsActorMigrationManager(io_service, nullptr, nullptr, nullptr, nullptr, nullptr,
                                 nullptr, nullptr) {}

  void PubMigrationNotification(const std::vector<NodeID> &node_id_list,
                                int64_t migration_id) override {
    published_migration_[migration_id] = node_id_list;
  }

  bool IsNodeMigrationComplete(const std::string &node_name) const override {
    return false;
  }

  void FreezeNode(const std::string &node_name) override {
    freezed_nodes_[node_name] = true;
  }

  absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetAliveNode(
      const NodeID &node_id) const override {
    auto iter = nodes_.find(node_id);
    if (iter != nodes_.end()) {
      return iter->second;
    }
    return {};
  }

  boost::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetUniqueNodeByName(
      const std::string &node_name) const override {
    for (auto &entry : nodes_) {
      if (entry.second->basic_gcs_node_info().node_name() == node_name) {
        return entry.second;
      }
    }
    return {};
  }

  bool IsPlacementGroupSchedulingInProgress() const override {
    return is_placement_group_scheduling_in_progress_;
  }

  bool HasAnyDriversInNode(const NodeID &node_id) const override {
    auto iter = node_to_drivers_.find(node_id);
    return iter != node_to_drivers_.end() && iter->second;
  }

  std::vector<std::weak_ptr<GcsActor>> GetAllActorsInNode(
      const NodeID &node_id) const override {
    std::vector<std::weak_ptr<GcsActor>> actors;
    auto iter = node_to_actors_.find(node_id);
    if (iter != node_to_actors_.end()) {
      for (auto &actor : iter->second) {
        actors.emplace_back(actor);
      }
    }
    return actors;
  }

  const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const override {
    return created_actors_;
  }

  absl::flat_hash_map<int64_t, std::vector<NodeID>> published_migration_;
  absl::flat_hash_map<std::string, bool> freezed_nodes_;
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> nodes_;
  bool is_placement_group_scheduling_in_progress_ = false;
  absl::flat_hash_map<NodeID, bool> node_to_drivers_;
  absl::flat_hash_map<NodeID, std::vector<std::shared_ptr<GcsActor>>> node_to_actors_;
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>> created_actors_;
};

class GcsActorMigrationTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_actor_migration_manager_ =
        std::make_shared<MockGcsActorMigrationManager>(io_service_);
  }

  virtual void TearDown() override {}

  void Initialize() {
    auto job_id = JobID::FromInt(1);
    // Add 10 nodes and each of them has one inflight actor and one created actor.
    for (int i = 0; i < 10; ++i) {
      std::string node_address = "127.0.0." + std::to_string(i);
      auto node = Mocker::GenNodeInfo(0, node_address);
      node->mutable_basic_gcs_node_info()->set_node_name(node_address);
      auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
      gcs_actor_migration_manager_->nodes_.emplace(node_id, node);

      rpc::Address address;
      address.set_raylet_id(node_id.Binary());

      auto request = Mocker::GenCreateActorRequest(job_id);
      auto inflight_actor = std::make_shared<gcs::GcsActor>(request.task_spec(), "");
      inflight_actor->UpdateState(rpc::ActorTableData::RESTARTING);
      inflight_actor->UpdateAddress(address);
      gcs_actor_migration_manager_->node_to_actors_[node_id].emplace_back(inflight_actor);

      auto worker_id = WorkerID::FromRandom();
      address.set_worker_id(worker_id.Binary());
      request = Mocker::GenCreateActorRequest(job_id);
      auto created_actor = std::make_shared<gcs::GcsActor>(request.task_spec(), "");
      created_actor->UpdateState(rpc::ActorTableData::ALIVE);
      created_actor->UpdateAddress(address);
      gcs_actor_migration_manager_->node_to_actors_[node_id].emplace_back(created_actor);

      gcs_actor_migration_manager_->created_actors_[node_id].emplace(
          worker_id, created_actor->GetActorID());
    }
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<MockGcsActorMigrationManager> gcs_actor_migration_manager_;
};

TEST_F(GcsActorMigrationTest, TestInvalidNode) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");
  // Add an invalid node.
  request.add_node_name_list("127.0.0.100");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 3);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());
  ASSERT_FALSE(reply.results().at("127.0.0.100").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);
}

TEST_F(GcsActorMigrationTest, TestMigrateActorsInNode) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  auto node1 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.1");
  auto node2 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.2");
  auto node_id1 = NodeID::FromBinary((*node1)->basic_gcs_node_info().node_id());
  auto node_id2 = NodeID::FromBinary((*node2)->basic_gcs_node_info().node_id());

  auto &node_id_to_migration_info =
      gcs_actor_migration_manager_->node_id_to_migration_info_;
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // inflight actor in node1 created successfully.
  auto actor = node_id_to_migration_info[node_id1].inflight_actors_.front().lock();
  auto worker_id = WorkerID::FromRandom();
  rpc::Address address;
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id1.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id1].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 1);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // inflight actor in node2 created successfully.
  actor = node_id_to_migration_info[node_id2].inflight_actors_.front().lock();
  worker_id = WorkerID::FromRandom();
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id2].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 0);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 1);
  auto &published_nodes =
      gcs_actor_migration_manager_->published_migration_.begin()->second;
  ASSERT_EQ(published_nodes.size(), 2);
}

TEST_F(GcsActorMigrationTest, TestMigrationNodeDead) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  auto node1 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.1");
  auto node2 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.2");
  auto node_id1 = NodeID::FromBinary((*node1)->basic_gcs_node_info().node_id());
  auto node_id2 = NodeID::FromBinary((*node2)->basic_gcs_node_info().node_id());

  auto &node_id_to_migration_info =
      gcs_actor_migration_manager_->node_id_to_migration_info_;
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // node1 is dead
  (*node1)->mutable_basic_gcs_node_info()->set_state(rpc::BasicGcsNodeInfo::DEAD);
  gcs_actor_migration_manager_->nodes_.erase(node_id1);
  gcs_actor_migration_manager_->node_to_drivers_.erase(node_id1);
  gcs_actor_migration_manager_->node_to_actors_.erase(node_id1);
  gcs_actor_migration_manager_->created_actors_.erase(node_id1);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // inflight actor in node2 created successfully.
  auto actor = node_id_to_migration_info[node_id2].inflight_actors_.front().lock();
  auto worker_id = WorkerID::FromRandom();
  rpc::Address address;
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id2].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 0);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 1);
  auto &published_nodes =
      gcs_actor_migration_manager_->published_migration_.begin()->second;
  ASSERT_EQ(published_nodes.size(), 1);
}

TEST_F(GcsActorMigrationTest, TestMigrationTwoNodeDead) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  auto node1 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.1");
  auto node2 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.2");
  auto node_id1 = NodeID::FromBinary((*node1)->basic_gcs_node_info().node_id());
  auto node_id2 = NodeID::FromBinary((*node2)->basic_gcs_node_info().node_id());

  auto &node_id_to_migration_info =
      gcs_actor_migration_manager_->node_id_to_migration_info_;
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // node1 is dead
  (*node1)->mutable_basic_gcs_node_info()->set_state(rpc::BasicGcsNodeInfo::DEAD);
  gcs_actor_migration_manager_->nodes_.erase(node_id1);
  gcs_actor_migration_manager_->node_to_drivers_.erase(node_id1);
  gcs_actor_migration_manager_->node_to_actors_.erase(node_id1);
  gcs_actor_migration_manager_->created_actors_.erase(node_id1);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // node2 is dead
  (*node2)->mutable_basic_gcs_node_info()->set_state(rpc::BasicGcsNodeInfo::DEAD);
  gcs_actor_migration_manager_->nodes_.erase(node_id2);
  gcs_actor_migration_manager_->node_to_drivers_.erase(node_id2);
  gcs_actor_migration_manager_->node_to_actors_.erase(node_id2);
  gcs_actor_migration_manager_->created_actors_.erase(node_id2);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 0);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);
}

TEST_F(GcsActorMigrationTest, TestMigrationSimpleDuplicateNodes) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  auto node1 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.1");
  auto node2 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.2");
  auto node3 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.3");
  auto node_id1 = NodeID::FromBinary((*node1)->basic_gcs_node_info().node_id());
  auto node_id2 = NodeID::FromBinary((*node2)->basic_gcs_node_info().node_id());
  auto node_id3 = NodeID::FromBinary((*node3)->basic_gcs_node_info().node_id());

  auto &node_id_to_migration_info =
      gcs_actor_migration_manager_->node_id_to_migration_info_;
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  int64_t migration_id_1 =
      gcs_actor_migration_manager_->node_id_to_migration_info_[node_id1].migration_id_;

  // Another migration request.
  int64_t migration_id_2 = 0;
  {
    rpc::MigrateActorsInNodeRequest request;
    request.add_node_name_list("127.0.0.1");
    request.add_node_name_list("127.0.0.2");
    request.add_node_name_list("127.0.0.3");

    rpc::MigrateActorsInNodeReply reply;
    auto send_reply_callback = [](Status status, std::function<void()> success,
                                  std::function<void()> failure) {};

    gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                            send_reply_callback);
    ASSERT_EQ(reply.results().size(), 3);
    ASSERT_FALSE(reply.results().at("127.0.0.1").success());
    ASSERT_FALSE(reply.results().at("127.0.0.2").success());
    ASSERT_TRUE(reply.results().at("127.0.0.3").success());

    ASSERT_EQ(migration_id_to_nodes_info.size(), 2);

    migration_id_2 =
        gcs_actor_migration_manager_->node_id_to_migration_info_[node_id3].migration_id_;
    ASSERT_EQ(
        migration_id_to_nodes_info[migration_id_2].number_of_nodes_with_inflight_tasks_,
        1);
  }

  // inflight actor in node1 created successfully.
  auto actor = node_id_to_migration_info[node_id1].inflight_actors_.front().lock();
  auto worker_id = WorkerID::FromRandom();
  rpc::Address address;
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id1.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id1].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id3].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(
      migration_id_to_nodes_info[migration_id_1].number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_id_to_nodes_info[migration_id_1].ready_nodes_.size(), 1);
  ASSERT_EQ(
      migration_id_to_nodes_info[migration_id_2].number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_id_to_nodes_info[migration_id_2].ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // inflight actor in node2 created successfully.
  actor = node_id_to_migration_info[node_id2].inflight_actors_.front().lock();
  worker_id = WorkerID::FromRandom();
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id2].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id3].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  ASSERT_EQ(
      migration_id_to_nodes_info[migration_id_2].number_of_nodes_with_inflight_tasks_, 1);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 1);
  // node1 & node2
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_[migration_id_1].size(), 2);

  // inflight actor in node3 created successfully.
  actor = node_id_to_migration_info[node_id3].inflight_actors_.front().lock();
  worker_id = WorkerID::FromRandom();
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id3.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id3].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 0);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 2);
  // node3
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_[migration_id_2].size(), 1);
}

TEST_F(GcsActorMigrationTest, TestMigrationDuplicateNodes) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  auto node1 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.1");
  auto node2 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.2");
  auto node3 = gcs_actor_migration_manager_->GetUniqueNodeByName("127.0.0.3");
  auto node_id1 = NodeID::FromBinary((*node1)->basic_gcs_node_info().node_id());
  auto node_id2 = NodeID::FromBinary((*node2)->basic_gcs_node_info().node_id());
  auto node_id3 = NodeID::FromBinary((*node3)->basic_gcs_node_info().node_id());

  auto &node_id_to_migration_info =
      gcs_actor_migration_manager_->node_id_to_migration_info_;
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 2);
  ASSERT_EQ(node_id_to_migration_info[node_id1].inflight_actors_.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  int64_t migration_id_1 =
      gcs_actor_migration_manager_->node_id_to_migration_info_[node_id1].migration_id_;

  // inflight actor in node1 created successfully.
  auto actor = node_id_to_migration_info[node_id1].inflight_actors_.front().lock();
  auto worker_id = WorkerID::FromRandom();
  rpc::Address address;
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id1.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id1].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  ASSERT_EQ(node_id_to_migration_info[node_id2].inflight_actors_.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 1);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 1);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 0);

  // Another migration request.
  int64_t migration_id_2 = 0;
  {
    rpc::MigrateActorsInNodeRequest request;
    request.add_node_name_list("127.0.0.1");
    request.add_node_name_list("127.0.0.2");
    request.add_node_name_list("127.0.0.3");

    rpc::MigrateActorsInNodeReply reply;
    auto send_reply_callback = [](Status status, std::function<void()> success,
                                  std::function<void()> failure) {};

    gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                            send_reply_callback);
    ASSERT_EQ(reply.results().size(), 3);
    ASSERT_TRUE(reply.results().at("127.0.0.1").success());
    ASSERT_FALSE(reply.results().at("127.0.0.2").success());
    ASSERT_TRUE(reply.results().at("127.0.0.3").success());

    ASSERT_EQ(migration_id_to_nodes_info.size(), 2);

    migration_id_2 =
        gcs_actor_migration_manager_->node_id_to_migration_info_[node_id3].migration_id_;
    ASSERT_EQ(
        migration_id_to_nodes_info[migration_id_2].number_of_nodes_with_inflight_tasks_,
        2);
  }

  // inflight actor in node2 created successfully.
  actor = node_id_to_migration_info[node_id2].inflight_actors_.front().lock();
  worker_id = WorkerID::FromRandom();
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id2.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id2].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 1);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  ASSERT_EQ(
      migration_id_to_nodes_info[migration_id_2].number_of_nodes_with_inflight_tasks_, 1);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 1);
  // node1 & node2
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_[migration_id_1].size(), 2);

  // inflight actor in node3 created successfully.
  actor = node_id_to_migration_info[node_id3].inflight_actors_.front().lock();
  worker_id = WorkerID::FromRandom();
  address.set_worker_id(worker_id.Binary());
  address.set_raylet_id(node_id3.Binary());
  actor->UpdateAddress(address);
  actor->UpdateState(rpc::ActorTableData::ALIVE);
  gcs_actor_migration_manager_->created_actors_[node_id3].emplace(worker_id,
                                                                  actor->GetActorID());
  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  // check node_id_to_migration_info.
  ASSERT_EQ(node_id_to_migration_info.size(), 0);
  // check migration_id_to_nodes_info.
  ASSERT_EQ(migration_id_to_nodes_info.size(), 0);
  // check published nodes.
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_.size(), 2);
  // node2 & node3
  ASSERT_EQ(gcs_actor_migration_manager_->published_migration_[migration_id_2].size(), 2);
}

TEST_F(GcsActorMigrationTest, TestGCInvalidMigration) {
  // Initialize 10 nodes and each of them has one inflight actor and one created actor.
  Initialize();

  rpc::MigrateActorsInNodeRequest request;
  request.add_node_name_list("127.0.0.1");
  request.add_node_name_list("127.0.0.2");

  rpc::MigrateActorsInNodeReply reply;
  auto send_reply_callback = [](Status status, std::function<void()> success,
                                std::function<void()> failure) {};

  gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                          send_reply_callback);

  ASSERT_EQ(reply.results().size(), 2);
  ASSERT_TRUE(reply.results().at("127.0.0.1").success());
  ASSERT_TRUE(reply.results().at("127.0.0.2").success());

  auto &migration_id_to_nodes_info =
      gcs_actor_migration_manager_->migration_id_to_nodes_info_;
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
  auto &migration_node_info = migration_id_to_nodes_info.begin()->second;
  ASSERT_EQ(migration_node_info.number_of_nodes_with_inflight_tasks_, 2);
  ASSERT_EQ(migration_node_info.ready_nodes_.size(), 0);

  ASSERT_EQ(gcs_actor_migration_manager_->freezed_nodes_.size(), 2);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.1"]);
  ASSERT_TRUE(gcs_actor_migration_manager_->freezed_nodes_["127.0.0.2"]);

  // Another migration request.
  {
    rpc::MigrateActorsInNodeRequest request;
    request.add_node_name_list("127.0.0.100");

    rpc::MigrateActorsInNodeReply reply;
    auto send_reply_callback = [](Status status, std::function<void()> success,
                                  std::function<void()> failure) {};

    gcs_actor_migration_manager_->HandleMigrateActorsInNode(request, &reply,
                                                            send_reply_callback);
    ASSERT_EQ(reply.results().size(), 1);
    ASSERT_FALSE(reply.results().at("127.0.0.100").success());

    ASSERT_EQ(migration_id_to_nodes_info.size(), 2);
  }

  gcs_actor_migration_manager_->PeriodicallyCheckInFlightActorsAndPG();
  ASSERT_EQ(migration_id_to_nodes_info.size(), 1);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
