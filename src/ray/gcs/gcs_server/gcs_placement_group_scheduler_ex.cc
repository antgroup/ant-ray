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

#include "ray/gcs/gcs_server/gcs_placement_group_scheduler_ex.h"

#include "ray/gcs/gcs_server/gcs_job_distribution.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"

namespace ray {
namespace gcs {

GcsPlacementGroupSchedulerEx::GcsPlacementGroupSchedulerEx(
    instrumented_io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const GcsNodeManager &gcs_node_manager, GcsResourceManager &gcs_resource_manager,
    std::shared_ptr<GcsResourceScheduler> resource_scheduler,
    std::shared_ptr<GcsJobDistribution> job_distribution,
    std::shared_ptr<rpc::NodeManagerClientPool> lease_client_factory,
    std::function<bool(const PlacementGroupID &)> is_placement_group_lifetime_done,
    std::function<bool(const NodeID &, const std::string &)> is_node_in_nodegroup_fn)
    : GcsPlacementGroupScheduler(io_context, std::move(gcs_table_storage),
                                 gcs_node_manager, gcs_resource_manager,
                                 std::move(lease_client_factory),
                                 std::move(is_placement_group_lifetime_done)),
      resource_scheduler_(std::move(resource_scheduler)),
      job_distribution_(std::move(job_distribution)),
      is_node_in_nodegroup_fn_(std::move(is_node_in_nodegroup_fn)) {}

void GcsPlacementGroupSchedulerEx::ReleaseUnusedBundles(
    const std::unordered_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles) {
  for (auto &entry : node_to_bundles) {
    auto &mutable_cluster_resources = gcs_resource_manager_.GetMutableClusterResources();
    auto iter = mutable_cluster_resources.find(entry.first);
    if (iter != mutable_cluster_resources.end()) {
      auto &node_resources = *iter->second;
      for (auto &bundle : entry.second) {
        BundleSpecification bundle_spec(bundle);
        if (node_resources.PrepareBundleResources(bundle_spec.PlacementGroupId(),
                                                  bundle_spec.Index(),
                                                  bundle_spec.GetRequiredResources())) {
          gcs_resource_manager_.AddNodeTotalRequiredResources(
              iter->first, bundle_spec.GetRequiredResources());
        }
        node_resources.CommitBundleResources(bundle_spec.PlacementGroupId(),
                                             bundle_spec.Index(),
                                             bundle_spec.GetRequiredResources());
      }
    }
  }

  GcsPlacementGroupScheduler::ReleaseUnusedBundles(node_to_bundles);
}

ScheduleMap GcsPlacementGroupSchedulerEx::SelectNodesForUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  auto strategy = SchedulingType(placement_group->GetStrategy());
  auto placement_group_id = placement_group->GetPlacementGroupID();

  auto bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);

  auto job_id = placement_group_id.JobId();
  auto job_scheduling_context = job_distribution_->GetJobSchedulingContext(job_id);
  const auto &nodegroup_id = job_scheduling_context->GetJobConfig().nodegroup_id_;

  auto schedule_options = job_scheduling_context->GetScheduleOptions();
  PGScheduleContext context(job_id, nodegroup_id, schedule_options, bundle_locations);

  std::vector<const ResourceSet *> required_resources_list;
  required_resources_list.reserve(bundles.size());
  for (auto &bundle : bundles) {
    required_resources_list.emplace_back(&bundle->GetRequiredResources());
  }

  ScheduleMap selected_bundle_to_node;
  if (resource_scheduler_->Schedule(required_resources_list, strategy, &context)) {
    RAY_CHECK(context.selected_nodes.size() == bundles.size());
    for (size_t i = 0; i < bundles.size(); ++i) {
      selected_bundle_to_node[bundles[i]->BundleId()] = context.selected_nodes[i];
    }
  }

  // If no nodes are available, scheduling fails.
  if (selected_bundle_to_node.empty()) {
    absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
        nodegroupd_cluster_resources;
    for (const auto &entry : gcs_resource_manager_.GetClusterResources()) {
      if (is_node_in_nodegroup_fn_(entry.first, nodegroup_id)) {
        nodegroupd_cluster_resources.emplace(entry);
      }
    }
    WarnPlacementGroupSchedulingFailure(placement_group, nodegroupd_cluster_resources);
  }

  return selected_bundle_to_node;
}

}  // namespace gcs
}  // namespace ray
