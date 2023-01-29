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
#pragma once

#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"

namespace ray {
namespace gcs {

class GcsResourceScheduler;
class GcsJobDistribution;
class GcsPlacementGroupSchedulerEx : public GcsPlacementGroupScheduler {
 public:
  GcsPlacementGroupSchedulerEx(
      instrumented_io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      const GcsNodeManager &gcs_node_manager, GcsResourceManager &gcs_resource_manager,
      std::shared_ptr<GcsResourceScheduler> resource_scheduler,
      std::shared_ptr<GcsJobDistribution> job_distribution,
      std::shared_ptr<rpc::NodeManagerClientPool> lease_client_factory,
      std::function<bool(const PlacementGroupID &)> is_placement_group_lifetime_done,
      std::function<bool(const NodeID &, const std::string &)> is_node_in_nodegroup_fn);

  /// Notify raylets to release unused bundles.
  ///
  /// \param node_to_bundles Bundles used by each node.
  void ReleaseUnusedBundles(const std::unordered_map<NodeID, std::vector<rpc::Bundle>>
                                &node_to_bundles) override;

 protected:
  ScheduleMap SelectNodesForUnplacedBundles(
      std::shared_ptr<GcsPlacementGroup> placement_group,
      std::vector<std::shared_ptr<BundleSpecification>> &bundles) override;

 protected:
  std::shared_ptr<GcsResourceScheduler> resource_scheduler_;
  std::shared_ptr<GcsJobDistribution> job_distribution_;
  /// Check if the node is in the specified nodegroup.
  std::function<bool(const NodeID &, const std::string &)> is_node_in_nodegroup_fn_;
};

}  // namespace gcs
}  // namespace ray
