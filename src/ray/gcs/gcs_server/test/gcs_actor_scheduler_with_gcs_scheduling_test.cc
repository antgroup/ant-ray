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
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {
class GcsActorSchedulerWithGcsSchedulingTest : public ::testing::Test {
 public:
  void SetUp() override {
    raylet_client_ = std::make_shared<GcsServerMocker::MockRayletClient>();
    worker_client_ = std::make_shared<GcsServerMocker::MockWorkerClient>();
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    auto gcs_pub_sub = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, gcs_pub_sub, gcs_table_storage_, /*redis_broadcast_enabled=*/true);
    gcs_node_manager_ =
        std::make_shared<gcs::GcsNodeManager>(gcs_pub_sub, gcs_table_storage_);
    store_client_ = std::make_shared<gcs::InMemoryStoreClient>(io_service_);
    gcs_actor_table_ =
        std::make_shared<GcsServerMocker::MockedGcsActorTable>(store_client_);
    gcs_actor_task_spec_table_ =
        std::make_shared<GcsServerMocker::MockedGcsActorTaskSpecTable>(store_client_);
    raylet_client_pool_ = std::make_shared<rpc::NodeManagerClientPool>(
        [this](const rpc::Address &addr) { return raylet_client_; });
    gcs_job_config_ = std::make_shared<gcs::GcsJobConfig>(
        job_id_,
        /*nodegroup_id =*/"", "",
        /*num_java_workers_per_process=*/32,
        /*java_worker_process_default_memory_units=*/32,
        /*total_memory_units=*/4 * 32);
    schedule_options_ = std::make_shared<gcs::ScheduleOptions>();
    gcs_label_manager_ = std::make_shared<GcsLabelManager>();

    auto gcs_job_distribution = std::make_shared<gcs::GcsJobDistribution>(
        /*gcs_job_scheduling_factory=*/
        [this](const JobID &job_id) {
          return std::make_shared<gcs::GcsJobSchedulingContext>(*gcs_job_config_,
                                                                schedule_options_);
        },
        gcs_resource_manager_);
    auto resource_scheduler = std::make_shared<gcs::GcsResourceScheduler>(
        *gcs_resource_manager_,
        /*is_node_schedulable_callback=*/
        [](const NodeContext &, const gcs::ResourceScheduleContext *) { return true; });
    gcs_placement_group_scheduler_ =
        std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
            io_service_, gcs_table_storage_, *gcs_node_manager_, *gcs_resource_manager_,
            raylet_client_pool_,
            [](const PlacementGroupID &placement_group) { return false; });
    gcs_placement_group_manager_ = std::make_shared<gcs::GcsPlacementGroupManager>(
        io_service_, gcs_placement_group_scheduler_, gcs_table_storage_,
        *gcs_resource_manager_, gcs_pub_sub_, [this](const JobID &job_id) { return ""; });
    auto scheduling_strategy = std::make_shared<gcs::ResourceBasedSchedulingStrategy>(
        gcs_resource_manager_, gcs_job_distribution, gcs_table_storage_,
        resource_scheduler, gcs_placement_group_manager_);
    gcs_actor_scheduler_ = std::make_shared<GcsServerMocker::MockedGcsActorScheduler>(
        io_service_, *gcs_actor_table_, *gcs_actor_task_spec_table_, *gcs_node_manager_,
        gcs_pub_sub_,
        /*schedule_failure_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor,
               rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
               const std::string &scheduling_failure_message) {
          failure_actors_.emplace_back(std::move(actor));
        },
        /*schedule_success_handler=*/
        [this](std::shared_ptr<gcs::GcsActor> actor) {
          success_actors_.emplace_back(std::move(actor));
        },
        raylet_client_pool_, scheduling_strategy,
        /*client_factory=*/
        [this](const rpc::Address &address) { return worker_client_; }, nullptr, nullptr,
        gcs_label_manager_);
  }

  std::shared_ptr<gcs::GcsActor> NewGcsActor(
      const std::unordered_map<std::string, double> &required_placement_resources =
          std::unordered_map<std::string, double>(),
      const std::string &ray_namespace = "", const std::string &nodegroup_id = "",
      const std::unordered_map<std::string, std::string> &labels = {},
      rpc::SchedulingStrategy scheduling_strategy = {}) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("127.0.0.1");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());

    std::unordered_map<std::string, double> required_resources;
    auto actor_creating_task_spec = Mocker::GenActorCreationTask(
        job_id_, /*max_restarts=*/1, /*detached=*/true, /*name=*/"", owner_address,
        Language::JAVA, {}, required_resources, required_placement_resources, labels,
        scheduling_strategy);

    return std::make_shared<gcs::GcsActor>(actor_creating_task_spec.GetMessage(),
                                           ray_namespace, nodegroup_id);
  }

  std::shared_ptr<gcs::GcsNodeInfo> NewGcsNode(
      const std::unordered_map<std::string, double> &total_resources) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_resources_total()->insert(total_resources.begin(),
                                            total_resources.end());
    return node;
  }

 protected:
  instrumented_io_context io_service_;
  JobID job_id_ = JobID::FromInt(1);
  std::shared_ptr<gcs::StoreClient> store_client_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorTable> gcs_actor_table_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorTaskSpecTable>
      gcs_actor_task_spec_table_;

  std::shared_ptr<GcsServerMocker::MockRayletClient> raylet_client_;
  std::shared_ptr<GcsServerMocker::MockWorkerClient> worker_client_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsActorScheduler> gcs_actor_scheduler_;
  std::vector<std::shared_ptr<gcs::GcsActor>> success_actors_;
  std::vector<std::shared_ptr<gcs::GcsActor>> failure_actors_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
  std::shared_ptr<gcs::GcsJobConfig> gcs_job_config_;
  std::shared_ptr<gcs::ScheduleOptions> schedule_options_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler>
      gcs_placement_group_scheduler_;
  std::shared_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::shared_ptr<gcs::GcsLabelManager> gcs_label_manager_;
};

TEST_F(GcsActorSchedulerWithGcsSchedulingTest, TestFailedToLeaseWorkerWithTop1) {
  int node_count = 10;

  // node0 has the most resources.
  auto node0 = NewGcsNode({{"memory", 64}});
  gcs_node_manager_->AddNode(node0);
  gcs_resource_manager_->OnNodeAdd(*node0);
  auto node_id_0 = NodeID::FromBinary(node0->basic_gcs_node_info().node_id());

  // node1~node9 have the same resources.
  for (int i = 1; i < node_count; ++i) {
    auto node = NewGcsNode({{"memory", 32}});
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
  }
  ASSERT_EQ(gcs_node_manager_->GetAllAliveNodes().size(), node_count);

  auto actor = NewGcsActor({{"memory", 32}});

  // set num_candidate_nodes_for_scheduling to 1.
  schedule_options_->num_candidate_nodes_for_scheduling_ = 1;

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  auto max_times_to_retry_leasing_worker_from_node =
      RayConfig::instance().max_times_to_retry_leasing_worker_from_node();

  // Mock many times IOError reply, then the lease request will retry again and the
  // selected node will always be node0.
  ASSERT_EQ(actor->GetNodeID(), node_id_0);
  for (uint64_t i = 1; i <= 100 * max_times_to_retry_leasing_worker_from_node; ++i) {
    gcs_actor_scheduler_->num_retry_leasing_count_ = 0;
    auto node = gcs_node_manager_->GetAliveNode(actor->GetNodeID()).value();
    ASSERT_TRUE(raylet_client_->GrantWorkerLease(
        node->basic_gcs_node_info().node_manager_address(),
        node->basic_gcs_node_info().node_manager_port(), WorkerID::FromRandom(),
        actor->GetNodeID(), NodeID::Nil(), Status::IOError("")));
    ASSERT_EQ(actor->GetRetryLeasingTimes(),
              i % (max_times_to_retry_leasing_worker_from_node + 1));
    ASSERT_EQ(actor->GetNodeID(), node_id_0);
  }

  // set num_candidate_nodes_for_scheduling to 3.
  schedule_options_->num_candidate_nodes_for_scheduling_ = 3;

  ASSERT_EQ(actor->GetRetryLeasingTimes(), 0);
  ASSERT_EQ(actor->GetNodeID(), node_id_0);

  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);
}

TEST_F(GcsActorSchedulerWithGcsSchedulingTest, TestSucceedInCreatingActorWithTop3) {
  int node_count = 10;

  // node0 has the most resources.
  auto node0 = NewGcsNode({{"memory", 64}});
  gcs_node_manager_->AddNode(node0);
  gcs_resource_manager_->OnNodeAdd(*node0);
  auto node_id_0 = NodeID::FromBinary(node0->basic_gcs_node_info().node_id());

  // node1~node9 have the same resources.
  for (int i = 1; i < node_count; ++i) {
    auto node = NewGcsNode({{"memory", 32}});
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
  }
  ASSERT_EQ(gcs_node_manager_->GetAllAliveNodes().size(), node_count);

  auto actor = NewGcsActor({{"memory", 32}});

  // set num_candidate_nodes_for_scheduling to 1.
  schedule_options_->num_candidate_nodes_for_scheduling_ = 1;

  // Schedule the actor with 1 available node, and the lease request should be send to the
  // node.
  gcs_actor_scheduler_->Schedule(actor);
  ASSERT_EQ(1, raylet_client_->num_workers_requested);
  ASSERT_EQ(1, raylet_client_->callbacks.size());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, gcs_actor_scheduler_->num_retry_leasing_count_);

  ASSERT_EQ(actor->GetRetryLeasingTimes(), 0);
  // Make sure the selected node is node0 as num_candidate_nodes_for_scheduling == 1.
  ASSERT_EQ(actor->GetNodeID(), node_id_0);

  // set num_candidate_nodes_for_scheduling to 3.
  schedule_options_->num_candidate_nodes_for_scheduling_ = 3;

  // Mock IOError reply for node0, and OK for other nodes, to make sure the actor will be
  // scheduled successfully.
  auto max_times_to_retry_leasing_worker_from_node =
      RayConfig::instance().max_times_to_retry_leasing_worker_from_node();
  std::unordered_set<NodeID> selected_nodes;
  for (uint64_t i = 1; i <= 100 * max_times_to_retry_leasing_worker_from_node; ++i) {
    gcs_actor_scheduler_->num_retry_leasing_count_ = 0;
    selected_nodes.emplace(actor->GetNodeID());
    auto node = gcs_node_manager_->GetAliveNode(actor->GetNodeID()).value();
    if (node != node0) {
      ASSERT_TRUE(raylet_client_->GrantWorkerLease(
          node->basic_gcs_node_info().node_manager_address(),
          node->basic_gcs_node_info().node_manager_port(), WorkerID::FromRandom(),
          actor->GetNodeID(), NodeID::Nil()));
      ASSERT_EQ(actor->GetRetryLeasingTimes(), 0);
      break;
    }

    ASSERT_TRUE(raylet_client_->GrantWorkerLease(
        node->basic_gcs_node_info().node_manager_address(),
        node->basic_gcs_node_info().node_manager_port(), WorkerID::FromRandom(),
        actor->GetNodeID(), NodeID::Nil(), Status::IOError("")));
    ASSERT_EQ(actor->GetRetryLeasingTimes(),
              i % (max_times_to_retry_leasing_worker_from_node + 1));
  }
  ASSERT_TRUE(selected_nodes.size() > 1);

  ASSERT_EQ(0, raylet_client_->callbacks.size());
  ASSERT_EQ(1, worker_client_->callbacks.size());

  // Reply the actor creation request, then the actor should be scheduled successfully.
  ASSERT_TRUE(worker_client_->ReplyPushTask());
  ASSERT_EQ(0, worker_client_->callbacks.size());
  ASSERT_EQ(0, failure_actors_.size());
  ASSERT_EQ(1, success_actors_.size());
  ASSERT_EQ(actor, success_actors_.front());
}

TEST_F(GcsActorSchedulerWithGcsSchedulingTest, TestPendingActorsSchedule) {
  RayConfig::instance().initialize(
      R"({"gcs_task_scheduling_max_time_per_round_ms": 60000})");

  std::unordered_map<std::string, size_t> nodegroup_to_nodes;
  auto pending_actors_consumer = [&nodegroup_to_nodes](std::shared_ptr<GcsActor> actor) {
    const auto &nodegroup_id = actor->GetNodegroupId();
    auto iter = nodegroup_to_nodes.find(nodegroup_id);
    if (iter != nodegroup_to_nodes.end()) {
      if (--(iter->second) == 0) {
        nodegroup_to_nodes.erase(iter);
      }
      return Status::OK();
    }
    return Status::ResourcesNotEnough("");
  };

  auto init =
      [this, &nodegroup_to_nodes](
          const std::vector<std::pair<std::string, size_t>> &nodegroup_to_actor_count) {
        for (auto &entry : nodegroup_to_actor_count) {
          nodegroup_to_nodes[entry.first] = entry.second;
        }

        size_t nodegroup_count = nodegroup_to_actor_count.size();
        std::vector<std::pair<std::string, std::vector<std::shared_ptr<GcsActor>>>>
            nodegroup_to_actors(nodegroup_count);

        size_t max_column = 0;
        for (size_t i = 0; i < nodegroup_count; ++i) {
          auto &entry = nodegroup_to_actor_count[i];
          nodegroup_to_actors[i].first = entry.first;
          for (size_t j = 0; j < entry.second; ++j) {
            auto actor = NewGcsActor({{"memory", 32}}, /*ray_namespace=*/"", entry.first);
            nodegroup_to_actors[i].second.emplace_back(actor);
          }
          if (max_column < entry.second) {
            max_column = entry.second;
          }
        }

        std::vector<std::shared_ptr<GcsActor>> actors;
        for (size_t j = 0; j < max_column; ++j) {
          for (size_t i = 0; i < nodegroup_count; ++i) {
            if (j < nodegroup_to_actors[i].second.size()) {
              actors.emplace_back(nodegroup_to_actors[i].second[j]);
            }
          }
        }
        return actors;
      };

  instrumented_io_context io_context;
  GcsActorManager::PendingActorsScheduler scheduler(io_context, pending_actors_consumer);

  std::vector<std::pair<std::string, size_t>> nodegroup_to_actor_count = {
      {"ns1", 10}, {"ns2", 20}, {"ns3", 10}};

  auto actors = init(nodegroup_to_actor_count);
  for (auto &actor : actors) {
    scheduler.Add(actor);
  }
  ASSERT_EQ(scheduler.pending_actor_ids_.size(), actors.size());
  ASSERT_EQ(scheduler.pending_actors_map_.size(), nodegroup_to_actor_count.size());

  scheduler.ScheduleAll();
  ASSERT_EQ(nodegroup_to_nodes.size(), 0);
  ASSERT_EQ(scheduler.pending_actor_ids_.size(), 0);
  ASSERT_EQ(scheduler.pending_actors_map_.size(), 0);

  // Reinit
  actors = init(nodegroup_to_actor_count);
  for (auto &actor : actors) {
    scheduler.Add(actor);
  }
  ASSERT_EQ(scheduler.pending_actor_ids_.size(), actors.size());
  ASSERT_EQ(scheduler.pending_actors_map_.size(), nodegroup_to_actor_count.size());

  // Make sure ns2 can only consume 5 actors.
  nodegroup_to_nodes["ns2"] = 5;
  scheduler.ScheduleAll();
  ASSERT_EQ(nodegroup_to_nodes.size(), 0);
  // Checking that the pending_actors_map_ is not consumed up.
  ASSERT_EQ(scheduler.pending_actors_map_.size(), 1);
  ASSERT_EQ(scheduler.pending_actors_map_.begin()->first, "ns2");
  // Checking that the skiped number of actors belongs to ns2 is (15 - 1 = 14)
  ASSERT_EQ(scheduler.pending_actor_ids_.size(), 14);
}

TEST_F(GcsActorSchedulerWithGcsSchedulingTest,
       TestPendingActorsScheduleWithActorAffinity) {
  auto pending_actors_consumer = [](std::shared_ptr<GcsActor> actor) {
    return Status::OK();
  };
  instrumented_io_context io_context;
  GcsActorManager::PendingActorsScheduler scheduler(io_context, pending_actors_consumer);

  auto actor = NewGcsActor({{"memory", 32}}, /*ray_namespace=*/"", "",
                           {{"actor_id", "1"}, {"location", "dc-1"}});
  ASSERT_FALSE(scheduler.HasActorAffinitySchedulePendingActor(actor));
  rpc::SchedulingStrategy scheduling_strategy;
  auto match_expression =
      scheduling_strategy.mutable_actor_affinity_scheduling_strategy()
          ->add_match_expressions();
  match_expression->set_key("actor_id");
  match_expression->set_actor_affinity_operator(rpc::ActorAffinityOperator::EXISTS);
  match_expression->set_soft(true);
  auto actor_2 = NewGcsActor({{"memory", 32}}, /*ray_namespace=*/"", "other_nodegroup",
                             {}, scheduling_strategy);
  scheduler.Add(actor_2);
  ASSERT_FALSE(scheduler.HasActorAffinitySchedulePendingActor(actor));

  rpc::SchedulingStrategy scheduling_strategy_2;
  auto match_expressio_2 =
      scheduling_strategy_2.mutable_actor_affinity_scheduling_strategy()
          ->add_match_expressions();
  match_expressio_2->set_key("my_actor_id");
  match_expressio_2->set_actor_affinity_operator(rpc::ActorAffinityOperator::IN);
  match_expressio_2->add_values("1");
  match_expressio_2->set_soft(true);
  auto match_expressio_3 =
      scheduling_strategy_2.mutable_actor_affinity_scheduling_strategy()
          ->add_match_expressions();
  match_expressio_3->set_key("error_key");
  match_expressio_3->set_actor_affinity_operator(rpc::ActorAffinityOperator::NOT_IN);
  match_expressio_3->add_values("1");
  match_expressio_3->set_soft(true);
  auto actor_3 = NewGcsActor({{"memory", 32}}, /*ray_namespace=*/"", "other_nodegroup",
                             {}, scheduling_strategy_2);
  scheduler.Add(actor_3);
  ASSERT_FALSE(scheduler.HasActorAffinitySchedulePendingActor(actor));

  auto actor_4 =
      NewGcsActor({{"memory", 32}}, /*ray_namespace=*/"", "", {}, scheduling_strategy);
  scheduler.Add(actor_4);
  ASSERT_TRUE(scheduler.HasActorAffinitySchedulePendingActor(actor));
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
