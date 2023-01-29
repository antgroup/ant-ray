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
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using gcs::GcsJobConfig;
using gcs::GcsJobSchedulingContext;
using gcs::GcsWorkerProcess;
using gcs::ScheduleOptions;
using rpc::JobConfig;
using rpc::JobTableData;
using rpc::Language;

void InitSharedJavaWorkerProcess(GcsJobSchedulingContext *job_scheduling_context,
                                 int num_initial_java_worker_processes, NodeID node_id) {
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(
      kMemory_ResourceLabel,
      job_scheduling_context->GetJobConfig().java_worker_process_default_memory_units_);
  ResourceSet constraint_resources(resource_map);
  for (int i = 0; i < num_initial_java_worker_processes; ++i) {
    auto shared_worker_process = GcsWorkerProcess::Create(
        node_id, job_scheduling_context->GetJobConfig().job_id_, Language::JAVA,
        constraint_resources,
        /*is_shared=*/true,
        job_scheduling_context->GetJobConfig().num_java_workers_per_process_);
    job_scheduling_context->AddWorkerProcess(shared_worker_process);
  }
}

void InitSharedJavaWorkerProcess(gcs::GcsJobDistribution *gcs_job_distribution,
                                 JobID job_id, int num_initial_java_worker_processes,
                                 NodeID node_id) {
  auto job_scheduling_context =
      gcs_job_distribution->FindOrCreateJobSchedulingContext(job_id);
  const auto &job_config = job_scheduling_context->GetJobConfig();
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel,
                       job_config.java_worker_process_default_memory_units_);
  ResourceSet constraint_resources(resource_map);
  for (int i = 0; i < num_initial_java_worker_processes; ++i) {
    auto shared_worker_process = GcsWorkerProcess::Create(
        node_id, job_id, Language::JAVA, constraint_resources,
        /*is_shared=*/true, job_config.num_java_workers_per_process_);
    gcs_job_distribution->AddWorkerProcess(shared_worker_process);
  }
}

class GcsWorkerProcessTest : public ::testing::Test {};

TEST_F(GcsWorkerProcessTest, TestConsumeSlot) {
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel, 16);
  ResourceSet constraint_resources(resource_map);

  auto node_id = NodeID::FromRandom();
  auto job_id = JobID::FromInt(1);
  int slot_count = 32;
  auto gcs_worker_process =
      GcsWorkerProcess::Create(node_id, job_id, Language::JAVA, constraint_resources,
                               /*is_shared=*/true, slot_count);
  ASSERT_EQ(gcs_worker_process->GetJobID(), job_id);
  ASSERT_EQ(gcs_worker_process->GetNodeID(), node_id);
  ASSERT_EQ(gcs_worker_process->GetAvailableSlotCount(), slot_count);
  ASSERT_EQ(gcs_worker_process->GetRequiredResources(), constraint_resources);
  ASSERT_TRUE(gcs_worker_process->IsShared());

  for (int i = 0; i < slot_count; ++i) {
    ASSERT_TRUE(gcs_worker_process->AssignActor(ActorID::Of(job_id, TaskID::Nil(), i)));
  }
  ASSERT_FALSE(
      gcs_worker_process->AssignActor(ActorID::Of(job_id, TaskID::Nil(), slot_count)));
  ASSERT_EQ(gcs_worker_process->GetAvailableSlotCount(), 0);
}

class GcsJobSchedulingContextTest : public ::testing::Test {};

TEST_F(GcsJobSchedulingContextTest, TestAddWorkerProcess) {
  uint64_t java_worker_process_default_memory_units = 16;
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel, java_worker_process_default_memory_units);
  ResourceSet constraint_resources(resource_map);

  int num_initial_java_worker_processes = 4;
  auto node_id = NodeID::FromRandom();
  auto job_id = JobID::FromInt(1);
  GcsJobConfig job_config(job_id,
                          /*nodegroup_id =*/"", "",
                          /*num_java_workers_per_process=*/32,
                          java_worker_process_default_memory_units,
                          /*total_memory_units=*/4 * 32, 0, "", true);

  GcsJobSchedulingContext job_scheduling_context(job_config,
                                                 std::make_shared<ScheduleOptions>());
  InitSharedJavaWorkerProcess(&job_scheduling_context, num_initial_java_worker_processes,
                              node_id);
  ASSERT_EQ(job_scheduling_context.GetSharedWorkerProcesses().size(),
            num_initial_java_worker_processes);

  ASSERT_FALSE(job_scheduling_context.GetAvailableResources().IsEmpty());
  // Make sure it could still add shared worker process as the resources are enough.
  auto shared_worker_process = GcsWorkerProcess::Create(
      node_id, job_id, Language::JAVA, constraint_resources,
      /*is_shared=*/true, job_config.num_java_workers_per_process_);
  ASSERT_TRUE(job_scheduling_context.AddWorkerProcess(shared_worker_process));
  ASSERT_EQ(job_scheduling_context.GetSharedWorkerProcesses().size(),
            num_initial_java_worker_processes + 1);

  // Make sure it could still add sole worker process as the resources are enough.
  for (int i = 1; i < num_initial_java_worker_processes; ++i) {
    auto sole_worker_process =
        GcsWorkerProcess::Create(node_id, job_id, Language::JAVA, constraint_resources,
                                 /*is_shared=*/false);
    ASSERT_TRUE(job_scheduling_context.AddWorkerProcess(sole_worker_process));
  }
  ASSERT_EQ(job_scheduling_context.GetSoleWorkerProcesses().size(),
            num_initial_java_worker_processes - 1);

  ASSERT_TRUE(job_scheduling_context.GetAvailableResources().IsEmpty());
  // The resources is full, make sure it will be failed if continue to add new sole
  // worker process.
  shared_worker_process =
      GcsWorkerProcess::Create(node_id, job_id, Language::JAVA, constraint_resources,
                               /*is_shared=*/true);
  ASSERT_FALSE(job_scheduling_context.AddWorkerProcess(shared_worker_process));

  // The resources is full, make sure it will be failed if continue to add new sole
  // worker process.
  auto sole_worker_process =
      GcsWorkerProcess::Create(node_id, job_id, Language::JAVA, constraint_resources,
                               /*is_shared=*/false);
  ASSERT_FALSE(job_scheduling_context.AddWorkerProcess(sole_worker_process));
}

TEST_F(GcsJobSchedulingContextTest, TestSchedulerPlacementGroup) {
  int num_java_workers_per_process = 32;
  int java_worker_process_default_memory_units = 2;
  int num_initial_java_worker_processes = 4;
  auto node_id = NodeID::FromRandom();

  // 1. Create GcsJobSchedulingContext with insufficient memory resource.
  auto job_id = JobID::FromInt(1);
  GcsJobConfig insufficient_job_config(job_id,
                                       /*nodegroup_id =*/"", "",
                                       num_java_workers_per_process,
                                       java_worker_process_default_memory_units,
                                       /*total_memory_units=*/16, 0, "", true);

  auto job_scheduling_context = std::make_shared<GcsJobSchedulingContext>(
      insufficient_job_config, std::make_shared<ScheduleOptions>());
  InitSharedJavaWorkerProcess(job_scheduling_context.get(),
                              num_initial_java_worker_processes, node_id);

  // 2. Create bundle specs.
  auto group_id = PlacementGroupID::Of(job_id);
  std::unordered_map<std::string, double> required_resource;
  required_resource.emplace(kMemory_ResourceLabel, 5.0);
  auto first_bundle_spec = Mocker::GenBundleCreation(group_id, 1, required_resource);
  auto second_bundle_spec = Mocker::GenBundleCreation(group_id, 2, required_resource);
  std::vector<std::shared_ptr<BundleSpecification>> bundle_specs = {first_bundle_spec,
                                                                    second_bundle_spec};
  // 3. Check scheduler failed.
  ASSERT_FALSE(job_scheduling_context->ReserveBundlesResources(bundle_specs));
  // 4. Check rest resource is correct.
  std::unordered_map<std::string, double> res_resource = {{kMemory_ResourceLabel, 8.0}};
  ASSERT_TRUE(
      job_scheduling_context->GetAvailableResources().IsEqual(ResourceSet(res_resource)));
  // 5. Create GcsJobSchedulingContext with sufficient memory resource.
  GcsJobConfig sufficient_job_config(job_id,
                                     /*nodegroup_id =*/"", "",
                                     num_java_workers_per_process,
                                     java_worker_process_default_memory_units,
                                     /*total_memory_units=*/18);
  job_scheduling_context = std::make_shared<GcsJobSchedulingContext>(
      sufficient_job_config, std::make_shared<ScheduleOptions>());
  InitSharedJavaWorkerProcess(job_scheduling_context.get(),
                              num_initial_java_worker_processes, node_id);

  // 6. Check scheduler successful.
  ASSERT_TRUE(job_scheduling_context->ReserveBundlesResources(bundle_specs));
}

class GcsJobDistributionTest : public ::testing::Test {};

TEST_F(GcsJobDistributionTest, TestAddWorkerProcess) {
  int num_initial_java_worker_processes = 4;
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel, 16);
  ResourceSet constraint_resources(resource_map);

  auto node_id = NodeID::FromRandom();
  auto job_id = JobID::FromInt(1);
  GcsJobConfig job_config(job_id,
                          /*nodegroup_id =*/"", "",
                          /*num_java_workers_per_process=*/32,
                          /*java_worker_process_default_memory_units=*/32,
                          /*total_memory_units=*/4 * 32, 0, "", true);
  auto schedule_options = std::make_shared<gcs::ScheduleOptions>();

  gcs::GcsJobDistribution gcs_job_dist(
      /*gcs_job_scheduling_factory=*/
      [&job_config, schedule_options](const JobID &job_id) {
        return std::make_shared<gcs::GcsJobSchedulingContext>(job_config,
                                                              schedule_options);
      });
  InitSharedJavaWorkerProcess(&gcs_job_dist, job_id, num_initial_java_worker_processes,
                              node_id);
  auto job_scheduling_context = gcs_job_dist.GetJobSchedulingContext(job_id);
  ASSERT_TRUE(job_scheduling_context != nullptr);
  ASSERT_EQ(job_scheduling_context->GetSharedWorkerProcesses().size(),
            num_initial_java_worker_processes);

  auto shared_worker_process = GcsWorkerProcess::Create(
      node_id, job_id, Language::JAVA, constraint_resources,
      /*is_shared=*/true, job_config.num_java_workers_per_process_);
  ASSERT_FALSE(gcs_job_dist.AddWorkerProcess(shared_worker_process));
  ASSERT_EQ(job_scheduling_context->GetSharedWorkerProcesses().size(),
            num_initial_java_worker_processes);
}

TEST_F(GcsJobDistributionTest, TestWorkerDead) {
  int num_initial_java_worker_processes = 4;
  auto node_id = NodeID::FromRandom();

  auto job_id = JobID::FromInt(1);
  GcsJobConfig job_config(job_id,
                          /*nodegroup_id =*/"", "",
                          /*num_java_workers_per_process=*/32,
                          /*java_worker_process_default_memory_units=*/32,
                          /*total_memory_units=*/4 * 32);
  auto schedule_options = std::make_shared<gcs::ScheduleOptions>();

  gcs::GcsJobDistribution gcs_job_dist(
      /*gcs_job_scheduling_factory=*/
      [&job_config, schedule_options](const JobID &job_id) {
        return std::make_shared<gcs::GcsJobSchedulingContext>(job_config,
                                                              schedule_options);
      });
  InitSharedJavaWorkerProcess(&gcs_job_dist, job_id, num_initial_java_worker_processes,
                              node_id);

  auto job_scheduling_context = gcs_job_dist.GetJobSchedulingContext(job_id);
  ASSERT_TRUE(job_scheduling_context != nullptr);

  const auto &shared_worker_processes =
      job_scheduling_context->GetSharedWorkerProcesses();
  ASSERT_EQ(shared_worker_processes.size(), num_initial_java_worker_processes);

  const auto &node_to_jobs = gcs_job_dist.GetNodeToJobs();
  ASSERT_EQ(node_to_jobs.size(), 1);

  auto removed_worker_processes = gcs_job_dist.RemoveWorkerProcessesByNodeID(node_id);
  ASSERT_EQ(removed_worker_processes.size(), num_initial_java_worker_processes);
  ASSERT_EQ(node_to_jobs.size(), 0);

  InitSharedJavaWorkerProcess(&gcs_job_dist, job_id,
                              /*num_initial_java_worker_processes=*/2, node_id);
  auto iter = shared_worker_processes.begin();
  auto worker_process = iter->second;
  auto worker_process_2 = (++iter)->second;

  auto removed_worker_process = gcs_job_dist.RemoveWorkerProcessByWorkerProcessID(
      node_id, worker_process->GetWorkerProcessID(), job_id);
  ASSERT_EQ(removed_worker_process, worker_process);
  ASSERT_EQ(node_to_jobs.size(), 1);

  removed_worker_process = gcs_job_dist.RemoveWorkerProcessByWorkerProcessID(
      node_id, worker_process_2->GetWorkerProcessID(), job_id);
  ASSERT_EQ(removed_worker_process, worker_process_2);
  ASSERT_EQ(node_to_jobs.size(), 0);
}

TEST_F(GcsJobDistributionTest, TestMultiJob) {
  int num_initial_java_worker_processes = 4;
  auto node_id = NodeID::FromRandom();

  auto job_id_1 = JobID::FromInt(1);
  GcsJobConfig job_config_1(job_id_1,
                            /*nodegroup_id =*/"", "",
                            /*num_java_workers_per_process=*/32,
                            /*java_worker_process_default_memory_units=*/32,
                            /*total_memory_units=*/4 * 32);

  auto job_id_2 = JobID::FromInt(2);
  GcsJobConfig job_config_2(job_id_2,
                            /*nodegroup_id =*/"", "",
                            /*num_java_workers_per_process=*/32,
                            /*java_worker_process_default_memory_units=*/32,
                            /*total_memory_units=*/4 * 32);
  auto schedule_options = std::make_shared<gcs::ScheduleOptions>();

  gcs::GcsJobDistribution gcs_job_dist(
      /*gcs_job_scheduling_factory=*/
      [&job_config_1, &job_config_2, schedule_options](const JobID &job_id) {
        auto job_config = (job_id == job_config_1.job_id_ ? job_config_1 : job_config_2);
        return std::make_shared<gcs::GcsJobSchedulingContext>(job_config,
                                                              schedule_options);
      });

  InitSharedJavaWorkerProcess(&gcs_job_dist, job_id_1, num_initial_java_worker_processes,
                              node_id);
  InitSharedJavaWorkerProcess(&gcs_job_dist, job_id_2, num_initial_java_worker_processes,
                              node_id);

  const auto &node_to_jobs = gcs_job_dist.GetNodeToJobs();
  ASSERT_EQ(node_to_jobs.size(), 1);

  auto removed_worker_processes = gcs_job_dist.RemoveWorkerProcessesByNodeID(node_id);
  ASSERT_EQ(removed_worker_processes.size(), 2 * num_initial_java_worker_processes);
}

TEST_F(GcsJobDistributionTest, TestReportResourceExhaustedEvent) {
  int num_initial_java_worker_processes = 4;
  auto job_id = JobID::FromInt(1);
  GcsJobConfig job_config(job_id,
                          /*namespace_id =*/"", "",
                          /*num_java_workers_per_process=*/32,
                          /*java_worker_process_default_memory_units=*/32,
                          /*total_memory_units=*/4 * 32);

  auto schedule_options = std::make_shared<ScheduleOptions>();

  std::unordered_map<std::string, double> required_res_map{
      std::make_pair("cpu", 10),
      std::make_pair("gpu", 4),
      std::make_pair("memory", 10000),
  };

  std::unordered_map<std::string, double> constraint_res_map{
      std::make_pair("cpu", 6),
      std::make_pair("gpu", 1),
      std::make_pair("memory", 1000),
  };

  ResourceSet required_resources(required_res_map);
  ResourceSet constraint_resources(constraint_res_map);

  TaskSpecBuilder builder;
  auto task_id = TaskID::ForDriverTask(job_id);
  auto actor_id = ActorID::Of(job_id, task_id, 1);
  auto function_descriptor =
      FunctionDescriptorBuilder::BuildJava("com.antfin.arc", "test", "");
  std::unordered_map<std::string, double> empty_resources;
  builder.SetCommonTaskSpec(task_id, "", Language::JAVA, function_descriptor, job_id,
                            TaskID::Nil(), 0, TaskID::Nil(), rpc::Address(), 1,
                            empty_resources, empty_resources, "", 0);
  rpc::SchedulingStrategy scheduling_strategy;
  scheduling_strategy.mutable_default_scheduling_strategy();
  builder.SetActorCreationTaskSpec(actor_id, /*actor_handle=*/{}, scheduling_strategy, 1,
                                   0, {}, 1, false, "");
  auto job_scheduling_context =
      std::make_shared<GcsJobSchedulingContext>(job_config, schedule_options);
  InitSharedJavaWorkerProcess(job_scheduling_context.get(),
                              num_initial_java_worker_processes, NodeID::FromRandom());
  job_scheduling_context->OnJobResourcesInsufficiant(constraint_resources,
                                                     builder.Build());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
