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

#include "ray/gcs/gcs_server/gcs_job_distribution.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using gcs::GcsJobConfig;
using gcs::GcsJobSchedulingContext;
using gcs::GcsWorkerProcess;
using gcs::ScheduleOptions;
using rpc::JobConfig;
using rpc::Language;

class GcsResourceStatsTest : public ::testing::Test {};

TEST_F(GcsResourceStatsTest, TestStats) {
  instrumented_io_context io_context;
  auto gcs_resource_manager = std::make_shared<gcs::GcsResourceManagerEx>(
      io_context, /*gcs_pub_sub=*/nullptr, /*gcs_table_storage=*/nullptr,
      /*redis_broadcast_enabled=*/false, /*nodegroup_manager=*/nullptr);

  int node_count = 3;
  std::vector<NodeID> node_ids(3);
  for (int i = 0; i < node_count; ++i) {
    node_ids[i] = NodeID::FromRandom();
    rpc::GcsNodeInfo node_info;
    node_info.mutable_basic_gcs_node_info()->set_node_id(node_ids[i].Binary());
    node_info.mutable_resources_total()->insert(
        {{"node:127.0.0." + std::to_string(i), 1.0},
         {kMemory_ResourceLabel, 100.0},
         {kCPU_ResourceLabel, 100.0}});
    gcs_resource_manager->OnNodeAdd(node_info);
  }

  JobID job_id_1 = JobID::FromInt(1);
  auto placement_group_id_1 = PlacementGroupID::Of(job_id_1);

  JobID job_id_2 = JobID::FromInt(2);
  auto placement_group_id_2 = PlacementGroupID::Of(job_id_2);

  absl::flat_hash_map<JobID, gcs::GcsJobConfig> job_config_map;
  gcs::GcsJobConfig job_config_1(
      /*job_id=*/job_id_1, /*node_group_id=*/"nodegroup_id_1", "",
      /*num_java_workers_per_process=*/1,
      /*java_worker_process_default_memory_units=*/25,
      /*total_memory_units=*/50,
      /*max_total_memory_units=*/50,
      /*job_name=*/"job_name_1");

  gcs::GcsJobConfig job_config_2(
      /*job_id=*/job_id_2, /*node_group_id=*/"nodegroup_id_2", "",
      /*num_java_workers_per_process=*/1,
      /*java_worker_process_default_memory_units=*/25,
      /*total_memory_units=*/50,
      /*max_total_memory_units=*/50,
      /*job_name=*/"job_name_2");

  job_config_map.emplace(job_id_1, job_config_1);
  job_config_map.emplace(job_id_2, job_config_2);

  gcs::GcsJobDistribution gcs_job_dist(
      /*gcs_job_scheduling_factory=*/
      [&job_config_map](const JobID &job_id) {
        auto schedule_options = std::make_shared<gcs::ScheduleOptions>();
        return std::make_shared<gcs::GcsJobSchedulingContext>(job_config_map.at(job_id),
                                                              schedule_options);
      },
      /*gcs_resource_manager=*/gcs_resource_manager);

  {
    auto job_id = job_id_1;
    auto placement_group_id = placement_group_id_1;
    std::vector<std::shared_ptr<BundleSpecification>> bundles;
    for (int i = 0; i < 2; ++i) {
      rpc::Bundle bundle;
      auto mutable_bundle_id = bundle.mutable_bundle_id();
      mutable_bundle_id->set_bundle_index(i);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
      bundle.mutable_unit_resources()->insert({{kMemory_ResourceLabel, 10.0}});
      bundles.emplace_back(std::make_shared<BundleSpecification>(bundle));
    }
    auto job_scheduling_context = gcs_job_dist.FindOrCreateJobSchedulingContext(job_id);
    job_scheduling_context->ReserveBundlesResources(bundles);

    auto &mutable_cluster_resources = gcs_resource_manager->GetMutableClusterResources();
    // node0: bundle0
    mutable_cluster_resources[node_ids[0]]->PrepareBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/0,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
    mutable_cluster_resources[node_ids[0]]->CommitBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/0,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));

    // node1: bundle1
    mutable_cluster_resources[node_ids[1]]->PrepareBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/1,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
    mutable_cluster_resources[node_ids[1]]->CommitBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/1,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
  }

  {
    auto job_id = job_id_2;
    auto placement_group_id = placement_group_id_2;
    std::vector<std::shared_ptr<BundleSpecification>> bundles;
    for (int i = 0; i < 2; ++i) {
      rpc::Bundle bundle;
      auto mutable_bundle_id = bundle.mutable_bundle_id();
      mutable_bundle_id->set_bundle_index(i);
      mutable_bundle_id->set_placement_group_id(placement_group_id.Binary());
      bundle.mutable_unit_resources()->insert({{kMemory_ResourceLabel, 10.0}});
      bundles.emplace_back(std::make_shared<BundleSpecification>(bundle));
    }
    auto job_scheduling_context = gcs_job_dist.FindOrCreateJobSchedulingContext(job_id);
    job_scheduling_context->ReserveBundlesResources(bundles);

    auto &mutable_cluster_resources = gcs_resource_manager->GetMutableClusterResources();
    // node0: bundle0
    mutable_cluster_resources[node_ids[0]]->PrepareBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/0,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
    mutable_cluster_resources[node_ids[0]]->CommitBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/0,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));

    // node2: bundle1
    mutable_cluster_resources[node_ids[2]]->PrepareBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/1,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
    mutable_cluster_resources[node_ids[2]]->CommitBundleResources(
        /*group=*/placement_group_id, /*bundle_index=*/1,
        ResourceSet({kMemory_ResourceLabel}, {10.0}));
  }

  // node0
  {  // worker0 with bundle0 in job1
    auto node_id = node_ids[0];
    ResourceSet required_resources(
        {FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_1, 0),
         FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_1, -1)},
        {10.0, 10.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, placement_group_id_1.JobId(), Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(1);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  {  // worker0 with bundle0 in job2
    auto node_id = node_ids[0];
    ResourceSet required_resources(
        {FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_2, 0),
         FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_2, -1)},
        {10.0, 10.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, placement_group_id_2.JobId(), Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(2);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  // node1
  {  // worker1 with bundle0 in job1
    auto node_id = node_ids[1];
    ResourceSet required_resources(
        {FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_1, 1),
         FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_1, -1)},
        {5.0, 5.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, placement_group_id_1.JobId(), Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(3);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  {  // worker1 in job2
    auto node_id = node_ids[1];
    ResourceSet required_resources({kMemory_ResourceLabel, kCPU_ResourceLabel},
                                   {10.0, 10.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, job_id_2, Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(4);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  // node2
  {  // worker3 in job1
    auto node_id = node_ids[2];
    ResourceSet required_resources({kMemory_ResourceLabel, kCPU_ResourceLabel},
                                   {10.0, 10.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, job_id_1, Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(5);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  {  // worker3 in job2
    auto node_id = node_ids[2];
    ResourceSet required_resources(
        {FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_2, 1),
         FormatPlacementGroupResource(kMemory_ResourceLabel, placement_group_id_2, -1)},
        {10.0, 10.0});
    ASSERT_TRUE(gcs_resource_manager->AcquireResources(node_id, required_resources));
    auto worker_process = gcs::GcsWorkerProcess::Create(
        UniqueID::FromRandom(), node_id, placement_group_id_2.JobId(), Language::PYTHON,
        /*acquired_resources=*/required_resources, /*is_shared=*/false);
    worker_process->SetPID(6);
    gcs_job_dist.AddWorkerProcess(worker_process);
  }

  // gcs_job_dist.CollectStats();

  auto worker_resources = std::make_shared<
      absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>();
  // node0, worker0
  (*worker_resources)[node_ids[0]][1].AddOrUpdateResource(kMemory_ResourceLabel, 6);
  (*worker_resources)[node_ids[0]][1].AddOrUpdateResource(kCPU_ResourceLabel, 10);
  // node0, worker1
  (*worker_resources)[node_ids[0]][2].AddOrUpdateResource(kMemory_ResourceLabel, 12);
  (*worker_resources)[node_ids[0]][2].AddOrUpdateResource(kCPU_ResourceLabel, 10);

  // node1, worker0
  (*worker_resources)[node_ids[1]][3].AddOrUpdateResource(kMemory_ResourceLabel, 8);
  (*worker_resources)[node_ids[1]][3].AddOrUpdateResource(kCPU_ResourceLabel, 10);
  // node1, worker1
  (*worker_resources)[node_ids[1]][4].AddOrUpdateResource(kMemory_ResourceLabel, 8);
  (*worker_resources)[node_ids[1]][4].AddOrUpdateResource(kCPU_ResourceLabel, 10);

  // node1, worker0
  (*worker_resources)[node_ids[2]][5].AddOrUpdateResource(kMemory_ResourceLabel, 15);
  (*worker_resources)[node_ids[2]][5].AddOrUpdateResource(kCPU_ResourceLabel, 10);
  // node2, worker1
  (*worker_resources)[node_ids[2]][6].AddOrUpdateResource(kMemory_ResourceLabel, 15);
  (*worker_resources)[node_ids[2]][6].AddOrUpdateResource(kCPU_ResourceLabel, 10);

  RAY_UNUSED(gcs_job_dist.OnRuntimeWorkerProcessResourcesUpdated(worker_resources));
  // gcs_job_dist.CollectStats();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
