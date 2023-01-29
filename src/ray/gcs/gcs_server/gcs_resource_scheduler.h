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

#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/node_selector.h"

namespace ray {
namespace gcs {

/// 5 resource scheduling policies
enum SchedulingType {
  /// If the condition of strict packing is not satisfied,
  /// the node with higher score will be selected from the nodes that have been already
  /// scheduled.
  PACK = 0,
  /// If the condition of strict spreading is not satisfied,
  /// the node with higher score will be selected from the nodes that have not been
  /// already scheduled.
  SPREAD = 1,
  /// The required resources are scheduled to the only one node, and spreading is not
  /// allowed.
  STRICT_PACK = 2,
  /// The required resources are absolutely scheduled to some nodes, and packing is not
  /// allowed.
  STRICT_SPREAD = 3,
  // For schedule required resources which require bundle of placement group
  AFFINITY_WITH_BUNDLE = 4,
  // Actor affinity scheduling
  ACTOR_AFFINITY = 5,
  // Node affinity scheduling
  NODE_AFFINITY = 6,
  SchedulingType_MAX  // the last one tell the total enum count
};

class ResourceScheduleContext;
class DefaultNodesFilter;
class NodeSelector;
/// GcsResourceScheduler is used for scheduling a list of entities (e.g., actors and
/// placement groups) that require resources on the cluster.
class GcsResourceScheduler {
 public:
  GcsResourceScheduler(
      GcsResourceManager &gcs_resources_manager,
      std::function<bool(const NodeContext &, const ResourceScheduleContext *)>
          is_node_schedulable_callback =
              [](const NodeContext &, const ResourceScheduleContext *) { return true; });

  /// Scheduling a list of resources on the cluster. The scheduling results will be saved
  /// in the ResourceScheduleContext. Note, this only makes scheduling decisions and
  /// doesn't actually acquire resources.
  /// \param required_resources_list The resources list that the worker required.
  /// \param scheduling_type Four Schedule scheduling_type: PACK, SPREAD, STRICT_PACK,
  /// STRICT_SPREAD.
  /// \param context Resource scheduling context.
  bool Schedule(const std::vector<const ResourceSet *> &required_resources_list,
                SchedulingType scheduling_type, ResourceScheduleContext *context);

  std::string GetMatchExpressionDebugString(
      const ::ray::rpc::ActorAffinitySchedulingStrategy &scheduling_strategy) const;

 private:
  /// Schedule a list of resources with STRICT_SPREAD.
  void StrictSpreadSchedule(
      const std::vector<const ResourceSet *> &required_resources_list,
      ResourceScheduleContext *context);

  /// Schedule a list of resources with SPREAD.
  void SpreadSchedule(const std::vector<const ResourceSet *> &required_resources_list,
                      ResourceScheduleContext *context);

  /// Schedule a list of resources with STRICT_PACK.
  void StrictPackSchedule(const std::vector<const ResourceSet *> &required_resources_list,
                          ResourceScheduleContext *context);

  /// Schedule a list of resources with PACK.
  void PackSchedule(const std::vector<const ResourceSet *> &required_resources_list,
                    ResourceScheduleContext *context);

  /// Schedule the required resources which affinity with placement group bundle.
  void AffinityWithBundleSchedule(
      const std::vector<const ResourceSet *> &required_resources,
      ResourceScheduleContext *context);

  /// Schedule actor which affinity with other actors.
  void ActorAffinitySchedule(
      const std::vector<const ResourceSet *> &required_resources_list,
      ResourceScheduleContext *context);

  /// Schedule actor which affinity with node.
  void NodeAffinitySchedule(
      const std::vector<const ResourceSet *> &required_resources_list,
      ResourceScheduleContext *context);

  /// check whether the resources of this node are sufficient
  bool IsNodeFeasible(const ray::NodeID &node_id, const ray::ResourceSet &resource_set);

  /// Acquire resources from an appropriate node selected from a set of feasible nodes.
  /// \param required_resources Required resources.
  /// \param feasible_nodes A set of feasible nodes.
  /// \param context Context of the current scheduling.
  NodeID AcquireResources(const ResourceSet &required_resources,
                          std::vector<NodeContext> feasible_nodes,
                          ResourceScheduleContext *context);

  /// Acquire resources from the specified node.
  /// \param required_resources Required resources.
  /// \param node_id ID of the node from which the resources will be acquired.
  void AcquireResources(const ResourceSet &required_resources, const NodeID &node_id);

  NodeID SelectNodeByActorAffinityStrategy(
      GcsActorAffinityScheduleContext *schedule_context,
      const rpc::ActorAffinitySchedulingStrategy &actor_affinity_strategy,
      const ResourceSet &required_resources,
      const std::vector<NodeContext> &feasible_nodes);

  absl::flat_hash_set<NodeID> FilterNodeByActorAffinityExpression(
      GcsActorAffinityScheduleContext *schedule_context,
      const rpc::ActorAffinityMatchExpression &match_expression,
      const absl::flat_hash_set<NodeID> &candidate_nodes);

  void RecordSoftMatchNumber(absl::flat_hash_map<NodeID, int> &soft_match_number_map,
                             const absl::flat_hash_set<NodeID> &soft_match_nodes);

  absl::flat_hash_set<NodeID> GetMatchAllExpressionNodes(
      const absl::flat_hash_map<NodeID, int> &soft_match_number_map, int expression_size);

 private:
  GcsResourceManager &gcs_resources_manager_;
  /// Thread pool, used to speed up scheduling.
  std::shared_ptr<thread_pool> pool_;
  /// The filter to find all nodes that meet the required resources.
  std::unique_ptr<DefaultNodesFilter> nodes_filter_;
  /// Scorer to make a grade to the node.
  std::unique_ptr<NodeSelector> node_selector_;
  /// Function to check whether the node is schedulable.
  std::function<bool(const NodeContext &, const ResourceScheduleContext *)>
      is_node_schedulable_callback_;
};

}  // namespace gcs

}  // namespace ray
