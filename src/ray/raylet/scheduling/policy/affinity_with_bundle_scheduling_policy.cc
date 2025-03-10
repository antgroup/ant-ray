// Copyright 2021 The Ray Authors.
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

#include "ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.h"

namespace ray {
namespace raylet_scheduling_policy {

bool AffinityWithBundleSchedulingPolicy::IsNodeFeasibleAndAvailable(
    const scheduling::NodeID &node_id,
    const ResourceRequest &resource_request,
    bool avoid_gpu_nodes,
    const SchedulingContext *scheduling_context) {
  if (!(nodes_.contains(node_id) && is_node_alive_(node_id) &&
        is_node_schedulable_(node_id, scheduling_context) &&
        nodes_.at(node_id).GetLocalView().IsFeasible(resource_request) &&
        nodes_.at(node_id).GetLocalView().IsAvailable(resource_request))) {
    return false;
  }
  if (!avoid_gpu_nodes) {
    return true;
  }

  // Avoiding gpu nodes is only needed for requests with no bundle id specified, so we
  // only avoid the nodes with the PG's gpu wildcard resource.
  // Now combine the right prefix and suffix for the gpu wildcard resource name.
  std::string gpu_wildcard_resource_name =
      "GPU_group_" +
      GetGroupIDFromResource(resource_request.ResourceIds().begin()->Binary());

  const auto &node_total = nodes_.at(node_id).GetLocalView().total;
  return !node_total.Has(scheduling::ResourceID(gpu_wildcard_resource_name));
}

scheduling::NodeID AffinityWithBundleSchedulingPolicy::Schedule(
    const ResourceRequest &resource_request, SchedulingOptions options) {
  RAY_CHECK(options.scheduling_type == SchedulingType::AFFINITY_WITH_BUNDLE);

  auto bundle_scheduling_context =
      dynamic_cast<const AffinityWithBundleSchedulingContext *>(
          options.scheduling_context.get());
  const BundleID &bundle_id = bundle_scheduling_context->GetAffinityBundleID();
  if (bundle_id.second != -1) {
    const auto &node_id_opt = bundle_location_index_.GetBundleLocation(bundle_id);
    if (node_id_opt) {
      auto target_node_id = scheduling::NodeID(node_id_opt.value().Binary());
      if (IsNodeFeasibleAndAvailable(target_node_id,
                                     resource_request,
                                     /*avoid_gpu_nodes=*/false,
                                     options.scheduling_context.get())) {
        return target_node_id;
      }
    }
  } else {
    const PlacementGroupID &pg_id = bundle_id.first;
    const auto &bundle_locations_opt = bundle_location_index_.GetBundleLocations(pg_id);
    if (bundle_locations_opt) {
      // Find a target with gpu nodes avoided (if required).
      if (options.avoid_gpu_nodes) {
        for (const auto &iter : *(bundle_locations_opt.value())) {
          auto target_node_id = scheduling::NodeID(iter.second.first.Binary());
          if (IsNodeFeasibleAndAvailable(target_node_id,
                                         resource_request,
                                         /*avoid_gpu_nodes=*/true,
                                         options.scheduling_context.get())) {
            return target_node_id;
          }
        }
      }
      // Find a target from all nodes.
      for (const auto &iter : *(bundle_locations_opt.value())) {
        auto target_node_id = scheduling::NodeID(iter.second.first.Binary());
        if (IsNodeFeasibleAndAvailable(target_node_id,
                                       resource_request,
                                       /*avoid_gpu_nodes=*/false,
                                       options.scheduling_context.get())) {
          return target_node_id;
        }
      }
    }
  }
  return scheduling::NodeID::Nil();
}

}  // namespace raylet_scheduling_policy
}  // namespace ray
