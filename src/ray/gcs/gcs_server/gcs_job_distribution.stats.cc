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

#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

struct WorkerProcessContext {
  ResourceSet required_resources;
  ResourceSet acquired_resources;
  ResourceSet runtime_resources;
  BundleID bundle_id;
  bool is_for_placement_group = false;
};

using JobResourceDistributionMap =
    absl::flat_hash_map<JobID,
                        absl::flat_hash_map<NodeID, std::vector<WorkerProcessContext>>>;
using ClusterResourceMap =
    absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>;

struct JobStatsContext {
  std::string job_name;
  SchedulingResources scheduling_resources;
  ResourceSet shortterm_runtime_resources;
  ResourceSet longterm_runtime_resources;
};
using JobStatsContextMap = absl::flat_hash_map<JobID, JobStatsContext>;

static bool ParseWildcardResource(const std::string &resource_label,
                                  const std::string &origin_resource_label,
                                  JobID *job_id) {
  static const std::string suffix_pattern = "_group_" + PlacementGroupID::Nil().Hex();
  if ((resource_label.size() == origin_resource_label.size() + suffix_pattern.size()) &&
      StartsWith(resource_label, origin_resource_label)) {
    if (job_id) {
      *job_id = JobID::FromHex(
          resource_label.substr(resource_label.size() - 2 * JobID::Size()));
    }
    return true;
  }
  return false;
}

static bool ParseNodeIp(const std::string &resource_label, std::string *node_ip) {
  // node:1.1.1.1 -- size >= 12
  static const std::string pattern = "node:";
  if (resource_label.size() >= 12 && StartsWith(resource_label, pattern)) {
    if (node_ip) {
      *node_ip = resource_label.substr(pattern.size());
    }
    return true;
  }
  return false;
}

static ResourceSet GetOriMemoryAndCpuFromPGResources(
    const ResourceSet &resource_set, const std::string &placement_group_id_hex) {
  ResourceSet ret;
  auto wildcard_memory_resource_label =
      kMemory_ResourceLabel + "_group_" + placement_group_id_hex;
  auto wildcard_cpu_resource_label =
      kCPU_ResourceLabel + "_group_" + placement_group_id_hex;
  auto memory_quantity = resource_set.GetResource(wildcard_memory_resource_label);
  auto cpu_quantity = resource_set.GetResource(wildcard_cpu_resource_label);
  ret.AddOrUpdateResource(kMemory_ResourceLabel, memory_quantity);
  ret.AddOrUpdateResource(kCPU_ResourceLabel, cpu_quantity);
  return ret;
}

static void DoCollectStats(ClusterResourceMap cluster_resources,
                           JobResourceDistributionMap job_resources_distributions,
                           JobStatsContextMap job_stats_context_map) {
  struct AggregatedResources {
    ResourceSet required_resources;
    ResourceSet acquired_resources;
    ResourceSet runtime_resources;
  };

  absl::flat_hash_map<NodeID, std::string> node_to_ip_;
  absl::flat_hash_map<NodeID, absl::flat_hash_map<JobID, AggregatedResources>>
      aggregated_resources_on_each_job_per_node;

  absl::flat_hash_map<NodeID, absl::flat_hash_map<JobID, ResourceSet>>
      pg_acquired_resources_on_each_job_per_node;
  for (const auto &entry : cluster_resources) {
    const auto &node_id = entry.first;
    const SchedulingResources &node_resources = *entry.second;

    JobID job_id;
    std::string node_ip;
    for (const auto &resource_entry :
         node_resources.GetTotalResources().GetResourceAmountMap()) {
      if (ParseWildcardResource(resource_entry.first, kMemory_ResourceLabel, &job_id)) {
        auto &resource_set = pg_acquired_resources_on_each_job_per_node[node_id][job_id];
        resource_set.AddResources(
            ResourceSet({{kMemory_ResourceLabel, resource_entry.second}}));
      } else if (ParseWildcardResource(resource_entry.first, kCPU_ResourceLabel,
                                       &job_id)) {
        auto &resource_set = pg_acquired_resources_on_each_job_per_node[node_id][job_id];
        resource_set.AddResources(
            ResourceSet({{kCPU_ResourceLabel, resource_entry.second}}));
      } else if (node_ip.empty() && ParseNodeIp(resource_entry.first, &node_ip)) {
        node_to_ip_.emplace(node_id, node_ip);
      }
    }
  }

  for (auto &entry : pg_acquired_resources_on_each_job_per_node) {
    auto &job_resource_map = aggregated_resources_on_each_job_per_node[entry.first];
    for (auto &entry2 : entry.second) {
      auto &aggregated_resources = job_resource_map[entry2.first];
      aggregated_resources.required_resources = entry2.second;
      aggregated_resources.acquired_resources = entry2.second;
    }
  }

  // job_resources_distributions -- <JobID, <NodeID, [WorkerProcessContext]>>
  for (const auto &job_resources_distributions_entry : job_resources_distributions) {
    const auto &job_id = job_resources_distributions_entry.first;
    // job_resources_distributions_entry.second -- <NodeID, [WorkerProcessContext]>
    for (const auto &node_to_worker_processes_entry :
         job_resources_distributions_entry.second) {
      const auto &node_id = node_to_worker_processes_entry.first;
      auto &aggregated_resources =
          aggregated_resources_on_each_job_per_node[node_id][job_id];
      // node_to_worker_processes_entry.second -- [WorkerProcessContext]
      for (const auto &worker_process_context : node_to_worker_processes_entry.second) {
        if (!worker_process_context.is_for_placement_group) {
          {
            ResourceSet resource_set;
            auto memory_quantity = worker_process_context.acquired_resources.GetResource(
                kMemory_ResourceLabel);
            auto cpu_quantity =
                worker_process_context.acquired_resources.GetResource(kCPU_ResourceLabel);
            resource_set.AddOrUpdateResource(kCPU_ResourceLabel, cpu_quantity);
            resource_set.AddOrUpdateResource(kMemory_ResourceLabel, memory_quantity);
            aggregated_resources.acquired_resources.AddResources(resource_set);
          }
          {
            ResourceSet resource_set;
            auto memory_quantity = worker_process_context.required_resources.GetResource(
                kMemory_ResourceLabel);
            auto cpu_quantity =
                worker_process_context.required_resources.GetResource(kCPU_ResourceLabel);
            resource_set.AddOrUpdateResource(kCPU_ResourceLabel, cpu_quantity);
            resource_set.AddOrUpdateResource(kMemory_ResourceLabel, memory_quantity);
            aggregated_resources.required_resources.AddResources(resource_set);
          }
        } else {
          // Adjust acquired resources.
          // acquired_resources = Node(pg_acquired_resources) -
          // Workers(pg_required_resources) + Workers(pg_acquired_resources)
          ResourceSet acquired_resources;
          acquired_resources.AddOrUpdateResource(
              kMemory_ResourceLabel,
              worker_process_context.acquired_resources.GetResource(
                  kMemory_ResourceLabel));
          acquired_resources.AddOrUpdateResource(
              kCPU_ResourceLabel,
              worker_process_context.acquired_resources.GetResource(kCPU_ResourceLabel));
          auto placement_group_id_hex = worker_process_context.bundle_id.first.Hex();
          auto required_resources = GetOriMemoryAndCpuFromPGResources(
              worker_process_context.required_resources, placement_group_id_hex);
          aggregated_resources.acquired_resources.AddResources(acquired_resources);
          aggregated_resources.acquired_resources.SubtractResources(required_resources);
        }

        // Runtime resources.
        ResourceSet resource_set;
        auto memory_quantity =
            worker_process_context.runtime_resources.GetResource(kMemory_ResourceLabel);
        auto cpu_quantity =
            worker_process_context.runtime_resources.GetResource(kCPU_ResourceLabel);
        resource_set.AddOrUpdateResource(kCPU_ResourceLabel, cpu_quantity);
        resource_set.AddOrUpdateResource(kMemory_ResourceLabel, memory_quantity);
        aggregated_resources.runtime_resources.AddResources(resource_set);
      }
    }
  }

  absl::flat_hash_map<JobID, AggregatedResources> aggregated_resources_on_each_job;
  // pg_acquired_resources_on_each_job_per_node -- <NodeID, <JobID, AggregatedResources>>
  for (auto &entry : aggregated_resources_on_each_job_per_node) {
    const auto &node_ip = node_to_ip_[entry.first];
    // entry.second -- <JobID, AggregatedResources>
    AggregatedResources aggregated_resources;
    for (auto &entry2 : entry.second) {
      aggregated_resources.required_resources.AddResources(
          entry2.second.required_resources);
      aggregated_resources.acquired_resources.AddResources(
          entry2.second.acquired_resources);
      aggregated_resources.runtime_resources.AddResources(
          entry2.second.runtime_resources);

      auto &aggregated_resources2 = aggregated_resources_on_each_job[entry2.first];
      aggregated_resources2.required_resources.AddResources(
          entry2.second.required_resources);
      aggregated_resources2.acquired_resources.AddResources(
          entry2.second.acquired_resources);
      aggregated_resources2.runtime_resources.AddResources(
          entry2.second.runtime_resources);
    }

    const ray::stats::TagsType tags = {{ray::stats::WorkerNodeAddressKey, node_ip}};
    ray::stats::NodeRequiredCPU().Record(
        aggregated_resources.required_resources.GetResource(kCPU_ResourceLabel)
            .ToDouble(),
        tags);
    ray::stats::NodeAcquiredCPU().Record(
        aggregated_resources.acquired_resources.GetResource(kCPU_ResourceLabel)
            .ToDouble(),
        tags);
    ray::stats::NodeRuntimeCPU().Record(
        aggregated_resources.runtime_resources.GetResource(kCPU_ResourceLabel).ToDouble(),
        tags);
    ray::stats::NodeRequiredMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.required_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
    ray::stats::NodeAcquiredMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.acquired_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
    ray::stats::NodeRuntimeMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.runtime_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
  }

  for (auto &entry : aggregated_resources_on_each_job) {
    const auto &aggregated_resources = entry.second;
    const auto &job_stats_context = job_stats_context_map[entry.first];
    const auto &total_resources =
        job_stats_context.scheduling_resources.GetTotalResources();

    const ray::stats::TagsType tags = {
        {ray::stats::JobNameKey, job_stats_context.job_name}};
    ray::stats::JobRequiredCPU().Record(
        aggregated_resources.required_resources.GetResource(kCPU_ResourceLabel)
            .ToDouble(),
        tags);
    ray::stats::JobAcquiredCPU().Record(
        aggregated_resources.acquired_resources.GetResource(kCPU_ResourceLabel)
            .ToDouble(),
        tags);
    ray::stats::JobRuntimeCPU().Record(
        aggregated_resources.runtime_resources.GetResource(kCPU_ResourceLabel).ToDouble(),
        tags);
    ray::stats::JobTotalCPU().Record(
        total_resources.GetResource(kCPU_ResourceLabel).ToDouble(), tags);

    ray::stats::JobRequiredMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.required_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
    ray::stats::JobAcquiredMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.acquired_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
    ray::stats::JobRuntimeMemMB().Record(
        FromMemoryUnitsToBytes(
            aggregated_resources.runtime_resources.GetResource(kMemory_ResourceLabel)
                .ToDouble()) /
            (1024 * 1024),
        tags);
    ray::stats::JobTotalMemMB().Record(
        FromMemoryUnitsToBytes(
            total_resources.GetResource(kMemory_ResourceLabel).ToDouble()) /
            (1024 * 1024),
        tags);
  }
}

GcsJobDistribution::~GcsJobDistribution() {
  if (stats_thread_ != nullptr) {
    stats_io_context_.stop();
    stats_thread_->join();
  }
}

void GcsJobDistribution::CollectStats() {
  if (!gcs_resource_manager_) {
    return;
  }

  auto shared_cluster_resources = std::make_shared<ClusterResourceMap>();
  for (auto &entry : gcs_resource_manager_->GetClusterResources()) {
    shared_cluster_resources->emplace(
        entry.first, std::make_shared<SchedulingResources>(*entry.second));
  }

  auto shared_job_stats_context_map = std::make_shared<JobStatsContextMap>();
  auto shared_job_resource_distributions = std::make_shared<JobResourceDistributionMap>();
  for (const auto &job_scheduling_context_entry : job_scheduling_contexts_) {
    const auto &job_scheduling_context = job_scheduling_context_entry.second;
    const auto &job_id = job_scheduling_context->GetJobConfig().job_id_;
    auto &job_stats_context = (*shared_job_stats_context_map)[job_id];
    job_stats_context.job_name = job_scheduling_context->GetJobConfig().job_name_;
    job_stats_context.scheduling_resources =
        job_scheduling_context->GetSchedulingResources();
    job_stats_context.shortterm_runtime_resources =
        job_scheduling_context->GetShorttermRuntimeResources();
    job_stats_context.longterm_runtime_resources =
        job_scheduling_context->GetLongtermRuntimeResources();
    auto &worker_process_distributions = (*shared_job_resource_distributions)[job_id];
    for (const auto &node_to_worker_processes_entry :
         job_scheduling_context->GetNodeToWorkerProcesses()) {
      const auto &node_id = node_to_worker_processes_entry.first;
      auto &worker_processes = worker_process_distributions[node_id];
      for (const auto &entry : node_to_worker_processes_entry.second) {
        const auto &worker_process = entry.second;
        WorkerProcessContext context = {
            .required_resources = worker_process->GetRequiredResources(),
            .acquired_resources = worker_process->GetResources(),
            .runtime_resources = worker_process->GetRuntimeResources(),
            .bundle_id = worker_process->GetBundleID(),
            .is_for_placement_group = worker_process->IsForPlacementGroup()};
        if (context.is_for_placement_group) {
          for (const auto &resource_entry :
               worker_process->GetPGAcquiredResources().GetResourceAmountMap()) {
            context.acquired_resources.AddOrUpdateResource(resource_entry.first,
                                                           resource_entry.second);
          }
        }
        worker_processes.emplace_back(std::move(context));
      }
    }
  }

  if (stats_thread_ == nullptr) {
    stats_thread_.reset(new std::thread([this] {
      boost::asio::io_service::work work(stats_io_context_);
      stats_io_context_.run();
    }));
  }

  stats_io_context_.post([shared_cluster_resources, shared_job_resource_distributions,
                          shared_job_stats_context_map] {
    DoCollectStats(std::move(*shared_cluster_resources),
                   std::move(*shared_job_resource_distributions),
                   std::move(*shared_job_stats_context_map));
  });
}
}  // namespace gcs
}  // namespace ray