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

#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_job_distribution_formatter.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using gcs::GcsActor;
using gcs::GcsInitData;
using gcs::GcsJobConfig;
using gcs::GcsJobDistribution;
using gcs::GcsResourceManager;
using gcs::GcsResourceScheduler;
using gcs::ResourceBasedSchedulingStrategy;
using gcs::ResourceScheduleContext;
using rpc::JobConfig;
using rpc::JobTableData;

class GcsActorSchedulingPolicyTest : public ::testing::Test {};

class SingleJobSchedulingTest : public GcsActorSchedulingPolicyTest {
 public:
  explicit SingleJobSchedulingTest() {
    // set num_candidate_nodes_for_scheduling to 1.
    RayConfig::instance().initialize(R"({"num_candidate_nodes_for_scheduling": 1})");

    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
        io_service_, gcs_pub_sub_, gcs_table_storage_, /*redis_broadcast_enabled=*/true);
    gcs_resource_manager_->Initialize(MockGcsInitData(gcs_table_storage_));
    gcs_job_distribution_ = std::make_shared<GcsJobDistribution>(
        /*gcs_job_scheduling_factory=*/
        [this](const JobID &job_id) {
          return std::make_shared<gcs::GcsJobSchedulingContext>(*job_config_,
                                                                schedule_options_);
        },
        gcs_resource_manager_);
    resource_scheduler_ = std::make_shared<gcs::GcsResourceScheduler>(
        *gcs_resource_manager_,
        /*is_node_schedulable_callback=*/
        [](const gcs::NodeContext &, const gcs::ResourceScheduleContext *) {
          return true;
        });

    gcs_placement_group_manager_ = MockGcsPlacmentGroupManager();

    scheduling_strategy_ = std::make_shared<ResourceBasedSchedulingStrategy>(
        gcs_resource_manager_, gcs_job_distribution_, gcs_table_storage_,
        resource_scheduler_, gcs_placement_group_manager_);
    scheduling_strategy_->Initialize(MockGcsInitData(gcs_table_storage_));
  }

  std::shared_ptr<gcs::GcsPlacementGroupManager> MockGcsPlacmentGroupManager() {
    gcs_node_manager_ =
        std::make_shared<gcs::GcsNodeManager>(gcs_pub_sub_, gcs_table_storage_);
    raylet_client_pool_ =
        std::make_shared<rpc::NodeManagerClientPool>([this](const rpc::Address &addr) {
          return std::make_shared<GcsServerMocker::MockRayletClient>();
        });
    gcs_placement_group_scheduler_ =
        std::make_shared<GcsServerMocker::MockedGcsPlacementGroupScheduler>(
            io_service_, gcs_table_storage_, *gcs_node_manager_, *gcs_resource_manager_,
            raylet_client_pool_,
            [](const PlacementGroupID &placement_group) { return false; });
    return std::make_shared<gcs::GcsPlacementGroupManager>(
        io_service_, gcs_placement_group_scheduler_, gcs_table_storage_,
        *gcs_resource_manager_, gcs_pub_sub_, [this](const JobID &job_id) { return ""; });
  }

  void InitJobConfig(uint64_t java_worker_process_default_memory_units,
                     int num_java_workers_per_process, uint64_t total_memory_units) {
    job_config_ = std::make_shared<GcsJobConfig>(
        job_id_, /*nodegroup_id =*/"", "", num_java_workers_per_process,
        java_worker_process_default_memory_units, total_memory_units, 0, "", true);
    schedule_options_ = std::make_shared<gcs::ScheduleOptions>();
    job_table_data_ = Mocker::GenJobTableData(job_id_);
    job_table_data_->mutable_config()->set_java_worker_process_default_memory_units(
        java_worker_process_default_memory_units);
    job_table_data_->mutable_config()->set_num_java_workers_per_process(
        num_java_workers_per_process);
    job_table_data_->mutable_config()->set_total_memory_units(total_memory_units);
  }

  std::shared_ptr<GcsActor> NewGcsActor(
      const std::unordered_map<std::string, double> &required_placement_resources =
          std::unordered_map<std::string, double>()) {
    rpc::Address owner_address;
    owner_address.set_raylet_id(NodeID::FromRandom().Binary());
    owner_address.set_ip_address("127.0.0.1");
    owner_address.set_port(5678);
    owner_address.set_worker_id(WorkerID::FromRandom().Binary());

    std::unordered_map<std::string, double> required_resources;
    auto actor_creating_task_spec = Mocker::GenActorCreationTask(
        job_id_, /*max_restarts=*/1, /*detached=*/true, /*name=*/"", owner_address,
        Language::JAVA, {}, required_resources, required_placement_resources);
    return std::make_shared<GcsActor>(actor_creating_task_spec.GetMessage(),
                                      /*ray_namespace=*/"", /*nodegroup_id=*/"");
  }

  std::shared_ptr<rpc::GcsNodeInfo> AddNewNode(
      std::unordered_map<std::string, double> node_resources) {
    auto node_info = Mocker::GenNodeInfo();
    node_info->mutable_resources_total()->insert(node_resources.begin(),
                                                 node_resources.end());
    gcs_resource_manager_->OnNodeAdd(*node_info);
    return node_info;
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;

  const JobID job_id_ = JobID::FromInt(1);
  std::shared_ptr<rpc::JobTableData> job_table_data_;
  std::shared_ptr<GcsJobConfig> job_config_;
  std::shared_ptr<gcs::ScheduleOptions> schedule_options_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsJobDistribution> gcs_job_distribution_;
  std::shared_ptr<GcsResourceScheduler> resource_scheduler_;
  std::shared_ptr<ResourceBasedSchedulingStrategy> scheduling_strategy_;
  std::shared_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsServerMocker::MockedGcsPlacementGroupScheduler>
      gcs_placement_group_scheduler_;
  std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool_;
};

TEST_F(SingleJobSchedulingTest, TestScheduleSharedActors) {
  int num_java_worker_processes = 4;

  InitJobConfig(/*java_worker_process_default_memory_units=*/32,
                /*num_java_workers_per_process=*/32,
                /*total_memory_units=*/num_java_worker_processes * 32);

  auto actor_0 = NewGcsActor();
  auto node_id = scheduling_strategy_->Schedule(actor_0);
  // There are no available nodes to schedule the actor_0.
  RAY_LOG(INFO) << "Schedule actor_0 and ensure it will be failed.";
  ASSERT_TRUE(node_id.IsNil());

  RAY_LOG(INFO) << "Add 3 nodes, each of them has 64 units memory.";
  int node_count = 3;
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  for (int i = 0; i < node_count; ++i) {
    AddNewNode(node_resources);
  }

  RAY_LOG(INFO) << "Schedule actor_0 and ensure it will not be failed.";
  node_id = scheduling_strategy_->Schedule(actor_0);

  // Ensure this actor is bind to a worker process.
  auto worker_process_id = actor_0->GetWorkerProcessID();
  ASSERT_FALSE(worker_process_id.IsNil());

  // Ensure this actor has not assign node id as the worker process info is not flushed to
  // the storage.
  ASSERT_FALSE(node_id.IsNil());

  auto scheduling_context = gcs_job_distribution_->GetJobSchedulingContext(job_id_);
  auto worker_process = scheduling_context->GetWorkerProcessById(worker_process_id);
  ASSERT_EQ(worker_process->GetUsedSlotCount(), 1);

  RAY_LOG(INFO) << "Schedule another [max_shared_actor_count - 1] actors and ensure it "
                   "will not be failed.";
  auto max_shared_actor_count =
      num_java_worker_processes * job_config_->num_java_workers_per_process_;
  for (uint32_t i = 1; i < max_shared_actor_count; ++i) {
    auto actor = NewGcsActor();
    node_id = scheduling_strategy_->Schedule(actor);
    worker_process_id = actor->GetWorkerProcessID();
    ASSERT_FALSE(worker_process_id.IsNil());
    // Ensure the node is selected.
    ASSERT_FALSE(node_id.IsNil());
  }

  RAY_LOG(INFO) << "Check that the number of occupied worker processes equal to "
                   "num_java_worker_processes, and check that the available slot "
                   "count of each shared worker process equals to 0";
  ASSERT_EQ(scheduling_context->GetSharedWorkerProcesses().size(),
            num_java_worker_processes);
  for (const auto &entry : scheduling_context->GetSharedWorkerProcesses()) {
    const auto &shared_worker_process = entry.second;
    // Check that the available slots is 0.
    ASSERT_EQ(shared_worker_process->GetAvailableSlotCount(), 0);
  }

  RAY_LOG(INFO) << "Schedule new actor and check it will be failed as the resources "
                   "allocated is touch the limit of the job.";
  auto actor = NewGcsActor();
  node_id = scheduling_strategy_->Schedule(actor);
  ASSERT_TRUE(actor->GetWorkerProcessID().IsNil());
  ASSERT_TRUE(node_id.IsNil());
}

TEST_F(SingleJobSchedulingTest, TestScheduleUniqueActors) {
  InitJobConfig(/*java_worker_process_default_memory_units=*/32,
                /*num_java_workers_per_process=*/32,
                /*total_memory_units=*/5 * 32);

  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}};

  RAY_LOG(INFO) << "Add 3 nodes, each of them has 64 units memory.";
  int node_count = 3;
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  for (int i = 0; i < node_count; ++i) {
    AddNewNode(node_resources);
  }

  auto scheduling_context =
      gcs_job_distribution_->FindOrCreateJobSchedulingContext(job_id_);
  RAY_LOG(INFO) << "Schedule 5 unique actors and ensure it will not be failed.";
  for (int i = 0; i < 5; ++i) {
    auto actor = NewGcsActor(required_placement_resources);
    auto node_id = scheduling_strategy_->Schedule(actor);
    auto worker_process_id = actor->GetWorkerProcessID();
    ASSERT_FALSE(worker_process_id.IsNil());
    ASSERT_FALSE(node_id.IsNil());
  }

  // Unique actors are still constrained by `total_memory_units` of the job.
  RAY_LOG(INFO) << "Schedule one more unique actor and ensure it will be failed.";
  auto actor = NewGcsActor(required_placement_resources);
  auto node_id = scheduling_strategy_->Schedule(actor);
  ASSERT_TRUE(actor->GetWorkerProcessID().IsNil());
  ASSERT_TRUE(node_id.IsNil());
}

TEST_F(SingleJobSchedulingTest, TestNotEnoughClusterResourcesLog) {
  InitJobConfig(/*java_worker_process_default_memory_units=*/32,
                /*num_java_workers_per_process=*/32,
                /*total_memory_units=*/5 * 32);

  std::unordered_map<std::string, double> required_placement_resources = {
      {kMemory_ResourceLabel, 32}};

  RAY_LOG(INFO) << "Add 3 nodes, each of them has 4 units memory.";
  int node_count = 3;
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 4},
                                                            {kCPU_ResourceLabel, 1}};
  for (int i = 0; i < node_count; ++i) {
    AddNewNode(node_resources);
  }

  auto actor = NewGcsActor(required_placement_resources);
  auto node_id = scheduling_strategy_->Schedule(actor);
  auto worker_process_id = actor->GetWorkerProcessID();
  ASSERT_TRUE(worker_process_id.IsNil());
  ASSERT_TRUE(node_id.IsNil());
}

TEST_F(SingleJobSchedulingTest, TestFO) {
  MockGcsInitData gcs_init_data(gcs_table_storage_);

  int num_java_worker_processes = 4;
  InitJobConfig(/*java_worker_process_default_memory_units=*/32,
                /*num_java_workers_per_process=*/32,
                /*total_memory_units=*/num_java_worker_processes * 32);

  gcs_init_data.MutableJobs().emplace(job_id_, *job_table_data_);

  RAY_LOG(INFO) << "Add 3 nodes, each of them has 64 units memory.";
  int node_count = 3;
  std::unordered_map<std::string, double> node_resources = {{kMemory_ResourceLabel, 64},
                                                            {kCPU_ResourceLabel, 8}};
  for (int i = 0; i < node_count; ++i) {
    auto node = AddNewNode(node_resources);
    gcs_init_data.MutableNodes().emplace(
        NodeID::FromBinary(node->basic_gcs_node_info().node_id()), *node);
  }

  auto scheduling_context =
      gcs_job_distribution_->FindOrCreateJobSchedulingContext(job_id_);
  RAY_LOG(INFO) << "Schedule [max_shared_actor_count] actors and ensure it "
                   "will not be failed.";
  auto max_shared_actor_count =
      num_java_worker_processes * job_config_->num_java_workers_per_process_;
  for (uint32_t i = 0; i < max_shared_actor_count; ++i) {
    auto actor = NewGcsActor();
    auto node_id = scheduling_strategy_->Schedule(actor);
    auto worker_process_id = actor->GetWorkerProcessID();
    ASSERT_FALSE(worker_process_id.IsNil());
    // Ensure the node is selected.
    ASSERT_FALSE(node_id.IsNil());
    auto worker_process = scheduling_context->GetWorkerProcessById(worker_process_id);
    ASSERT_TRUE(worker_process != nullptr);

    // Lease success.
    rpc::Address address;
    address.set_raylet_id(node_id.Binary());
    address.set_worker_id(WorkerID::FromRandom().Binary());
    address.set_ip_address("127.0.0.1");
    address.set_port(1234);
    actor->UpdateAddress(address);

    gcs_init_data.MutableActors().emplace(actor->GetActorID(),
                                          actor->GetActorTableData());
  }

  RAY_LOG(INFO) << "Construct new gcs_resource_manager.";
  const auto &old_cluster_resources = gcs_resource_manager_->GetClusterResources();
  auto old_gcs_resource_manager = std::move(gcs_resource_manager_);
  gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
      io_service_, gcs_pub_sub_, gcs_table_storage_, /*redis_broadcast_enabled=*/true);
  for (auto &entry : old_cluster_resources) {
    gcs::ResourceMap resource_map;
    for (auto &kv : entry.second->GetTotalResources().GetResourceMap()) {
      (*resource_map.mutable_items())[kv.first].set_resource_capacity(kv.second);
    }
    gcs_init_data.MutableClusterResources().emplace(entry.first, resource_map);
  }
  gcs_resource_manager_->Initialize(gcs_init_data);

  RAY_LOG(INFO) << "Construct new gcs_job_distribution.";
  auto old_gcs_job_distribution = std::move(gcs_job_distribution_);
  gcs_job_distribution_ = std::make_shared<GcsJobDistribution>(
      /*gcs_job_scheduling_factory=*/
      [this](const JobID &job_id) {
        return std::make_shared<gcs::GcsJobSchedulingContext>(*job_config_,
                                                              schedule_options_);
      });
  RAY_LOG(INFO) << "Construct new scheduling_policy.";
  auto old_scheduling_policy = std::move(scheduling_strategy_);
  RAY_UNUSED(old_scheduling_policy);
  scheduling_strategy_ = std::make_shared<ResourceBasedSchedulingStrategy>(
      gcs_resource_manager_, gcs_job_distribution_, gcs_table_storage_,
      resource_scheduler_, gcs_placement_group_manager_);
  scheduling_strategy_->Initialize(gcs_init_data);

  RAY_LOG(INFO) << "Check that the new_cluster_resources equals to the old one.";
  const auto &new_cluster_resources = gcs_resource_manager_->GetClusterResources();
  ASSERT_EQ(new_cluster_resources.size(), old_cluster_resources.size());
  for (auto &entry : new_cluster_resources) {
    auto iter = old_cluster_resources.find(entry.first);
    ASSERT_TRUE(iter != old_cluster_resources.end());
    ASSERT_TRUE(
        entry.second->GetTotalResources().IsEqual(iter->second->GetTotalResources()));
    ASSERT_TRUE(
        entry.second->GetLoadResources().IsEqual(iter->second->GetLoadResources()));
    ASSERT_TRUE(entry.second->GetAvailableResources().IsEqual(
        iter->second->GetAvailableResources()));
  }

  RAY_LOG(INFO) << "Check that the new_gcs_job_distribution equals to the old one.";
  const auto &old_node_to_jobs = old_gcs_job_distribution->GetNodeToJobs();
  const auto &new_node_to_jobs = gcs_job_distribution_->GetNodeToJobs();
  ASSERT_EQ(new_node_to_jobs.size(), old_node_to_jobs.size());

  auto old_job_scheduling_context =
      old_gcs_job_distribution->GetJobSchedulingContext(job_id_);
  auto new_job_scheduling_context =
      gcs_job_distribution_->GetJobSchedulingContext(job_id_);
  ASSERT_TRUE(new_job_scheduling_context->GetAvailableResources().IsEqual(
      old_job_scheduling_context->GetAvailableResources()));
  ASSERT_EQ(new_job_scheduling_context->GetSharedWorkerProcesses().size(),
            old_job_scheduling_context->GetSharedWorkerProcesses().size());
  ASSERT_EQ(new_job_scheduling_context->GetSoleWorkerProcesses().size(),
            old_job_scheduling_context->GetSoleWorkerProcesses().size());

  const auto &old_node_to_worker_processes =
      old_job_scheduling_context->GetNodeToWorkerProcesses();
  const auto &new_node_to_worker_processes =
      new_job_scheduling_context->GetNodeToWorkerProcesses();
  ASSERT_EQ(new_node_to_worker_processes.size(), old_node_to_worker_processes.size());
  for (const auto &entry : new_node_to_worker_processes) {
    auto iter = old_node_to_worker_processes.find(entry.first);
    ASSERT_TRUE(iter != old_node_to_worker_processes.end());
    const auto &new_worker_processes = entry.second;
    const auto &old_worker_processes = iter->second;
    ASSERT_EQ(new_worker_processes.size(), old_worker_processes.size());
    for (const auto &kv : new_worker_processes) {
      ASSERT_TRUE(old_worker_processes.contains(kv.first));
      ASSERT_TRUE(kv.second->EqualsTo(old_worker_processes.at(kv.first)));
      ASSERT_EQ(kv.second->GetAssignedActors().size(),
                old_worker_processes.at(kv.first)->GetAssignedActors().size());
    }
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
