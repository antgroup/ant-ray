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

#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"

#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

GcsResourceScheduler::GcsResourceScheduler(
    GcsResourceManager &gcs_resources_manager,
    std::function<bool(const NodeContext &, const ResourceScheduleContext *)>
        is_node_schedulable_callback)
    : gcs_resources_manager_(gcs_resources_manager),
      pool_(new thread_pool()),
      nodes_filter_(new DefaultNodesFilter(gcs_resources_manager, *pool_)),
      node_selector_(new NodeSelector(*pool_)),
      is_node_schedulable_callback_(std::move(is_node_schedulable_callback)) {}

bool GcsResourceScheduler::Schedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    SchedulingType scheduling_type, ResourceScheduleContext *context) {
  context->stat_.OnScheduleBegin();
  bool is_valid_scheduling_type = scheduling_type >= SchedulingType::PACK &&
                                  scheduling_type < SchedulingType::SchedulingType_MAX;
  RAY_CHECK(is_valid_scheduling_type);
  RAY_CHECK(context);

  /// Schedule and aquire resources.
  /// GcsResourceScheduler does not really aquire resources, it just provide a good
  /// resources scheduling result. So it is need to release resources when finished
  /// scheduling resources.
  switch (scheduling_type) {
  case SchedulingType::PACK:
    PackSchedule(required_resources_list, context);
    break;
  case SchedulingType::SPREAD:
    SpreadSchedule(required_resources_list, context);
    break;
  case SchedulingType::STRICT_PACK:
    StrictPackSchedule(required_resources_list, context);
    break;
  case SchedulingType::STRICT_SPREAD:
    StrictSpreadSchedule(required_resources_list, context);
    break;
  case SchedulingType::AFFINITY_WITH_BUNDLE:
    AffinityWithBundleSchedule(required_resources_list, context);
    break;
  case SchedulingType::ACTOR_AFFINITY:
    ActorAffinitySchedule(required_resources_list, context);
    break;
  case SchedulingType::NODE_AFFINITY:
    NodeAffinitySchedule(required_resources_list, context);
    break;
  default:
    break;
  }

  auto &selected_nodes = context->selected_nodes;
  /// Release acquired resources after scheduling resources.
  for (size_t i = 0; i < selected_nodes.size(); ++i) {
    gcs_resources_manager_.ReleaseResources(selected_nodes[i],
                                            *required_resources_list[i]);
    gcs_resources_manager_.SubtractNodeTotalRequiredResources(
        selected_nodes[i], *required_resources_list[i]);
  }

  {  // Record metrics.
    context->stat_.OnScheduleEnd();
    int64_t schedule_cost_us = context->stat_.GetScheduleCost();
    ray::stats::ResourceSchedulingTimeConsumption().Record(schedule_cost_us);

    ray::stats::ResourceSchedulingCount().Record(1);
  }

  if (selected_nodes.size() != required_resources_list.size()) {
    RAY_LOG(INFO) << "Failed to schedule resources, " << required_resources_list.size()
                  << " resources were expected to be scheduled, but only "
                  << selected_nodes.size()
                  << " was successful, SchedulingType = " << scheduling_type;
    selected_nodes.clear();
    ray::stats::ResourceSchedulingFailedCount().Record(1);
    return false;
  }

  ray::stats::ResourceSchedulingSuccessCount().Record(1);
  return true;
}

void GcsResourceScheduler::StrictSpreadSchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  for (const auto &required_resources : required_resources_list) {
    auto feasilble_nodes = nodes_filter_->FindFeasibleNodes(
        *required_resources,
        [this, context](const NodeContext &node) {
          if (!is_node_schedulable_callback_(node, context)) {
            return false;
          }

          return !context->IsNodeSelected(node.node_id_);
        },
        context->GetScheduleOptions());

    if (feasilble_nodes.empty()) {
      return;
    }

    context->selected_nodes.emplace_back(
        AcquireResources(*required_resources, std::move(feasilble_nodes), context));
  }
}

void GcsResourceScheduler::SpreadSchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  for (const auto &required_resources : required_resources_list) {
    context->stat_.OnFindFeasibleNodesStrictlyBegin();
    auto feasilble_nodes = nodes_filter_->FindFeasibleNodes(
        *required_resources,
        [this, context](const NodeContext &node) {
          if (!is_node_schedulable_callback_(node, context)) {
            return false;
          }

          return !context->IsNodeSelected(node.node_id_);
        },
        context->GetScheduleOptions());
    context->stat_.OnFindFeasibleNodesStrictlyEnd();

    if (feasilble_nodes.empty()) {
      context->stat_.OnFindFeasibleNodesSoftlyBegin();
      feasilble_nodes = nodes_filter_->FindFeasibleNodes(
          *required_resources,
          [this, context](const NodeContext &node) {
            if (!is_node_schedulable_callback_(node, context)) {
              return false;
            }

            return true;
          },
          context->GetScheduleOptions());

      context->stat_.OnFindFeasibleNodesSoftlyEnd();
      if (feasilble_nodes.empty()) {
        return;
      }
    }

    context->stat_.OnAcquireResourcesBegin();
    context->selected_nodes.emplace_back(
        AcquireResources(*required_resources, std::move(feasilble_nodes), context));
    context->stat_.OnAcquireResourcesEnd();
  }
}

void GcsResourceScheduler::StrictPackSchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  ResourceSet required_resources{};
  for (const auto &item : required_resources_list) {
    required_resources.AddResources(*item);
  }

  auto feasilble_nodes = nodes_filter_->FindFeasibleNodes(
      required_resources,
      [this, context](const NodeContext &node) {
        if (!is_node_schedulable_callback_(node, context)) {
          return false;
        }

        return !context->HasAnySelectedNode() || context->IsNodeSelected(node.node_id_);
      },
      context->GetScheduleOptions());

  if (feasilble_nodes.empty()) {
    return;
  }

  auto node_id =
      AcquireResources(required_resources, std::move(feasilble_nodes), context);
  if (node_id.IsNil()) {
    return;
  }

  for (size_t i = 0; i < required_resources_list.size(); ++i) {
    context->selected_nodes.emplace_back(node_id);
  }
}

void GcsResourceScheduler::PackSchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  for (const auto &required_resources : required_resources_list) {
    auto feasilble_nodes = nodes_filter_->FindFeasibleNodes(
        *required_resources,
        [this, context](const NodeContext &node) {
          if (!is_node_schedulable_callback_(node, context)) {
            return false;
          }

          return !context->HasAnySelectedNode() || context->IsNodeSelected(node.node_id_);
        },
        context->GetScheduleOptions());

    if (feasilble_nodes.empty()) {
      feasilble_nodes = nodes_filter_->FindFeasibleNodes(
          *required_resources,
          [this, context](const NodeContext &node) {
            if (!is_node_schedulable_callback_(node, context)) {
              return false;
            }

            return true;
          },
          context->GetScheduleOptions());

      if (feasilble_nodes.empty()) {
        return;
      }
    }

    context->selected_nodes.emplace_back(
        AcquireResources(*required_resources, std::move(feasilble_nodes), context));
  }
}

void GcsResourceScheduler::AffinityWithBundleSchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  auto schedule_contex =
      dynamic_cast<GcsActorAffinityWithBundleScheduleContext *>(context);
  schedule_contex->stat_.OnFindFeasibleNodesStrictlyBegin();
  RAY_CHECK(required_resources_list.size() == 1)
      << "Invalid required_resources_list_size = " << required_resources_list.size();
  RAY_CHECK(schedule_contex->RequirePlacementGroup())
      << "The required resources must contain placement group resources.";
  const ResourceSet &required_resources = *(required_resources_list[0]);
  const BundleID &origin_bundle_id = schedule_contex->GetPlacementGroupBundleID();
  const PlacementGroupID &pg_id = origin_bundle_id.first;

  const auto &pg_bundle_location_index =
      schedule_contex->GetPlacementGroupBundleLocationIndex();
  RAY_CHECK(pg_bundle_location_index.has_value())
      << "Invalid pg_bundle_location_index, placement_group_id:" << pg_id;

  const auto &bundle_locations_opt =
      (*pg_bundle_location_index).GetBundleLocations(pg_id);
  if (!bundle_locations_opt.has_value()) {
    return;
  }

  NodeID candidate_node_id = NodeID::Nil();
  std::shared_ptr<BundleLocations> bundle_locations = bundle_locations_opt.value();
  if (origin_bundle_id.second != -1) {
    auto pg_bundle_it = bundle_locations->find(origin_bundle_id);
    if (pg_bundle_it == bundle_locations->end()) {
      return;
    }

    if (!IsNodeFeasible(pg_bundle_it->second.first, required_resources)) {
      return;
    }

    candidate_node_id = pg_bundle_it->second.first;
  } else {
    for (const auto &bundle_pair : *bundle_locations) {
      const auto &node_id = bundle_pair.second.first;
      if (IsNodeFeasible(node_id, required_resources)) {
        candidate_node_id = node_id;
        break;
      }
    }
    if (candidate_node_id.IsNil()) {
      return;
    }
  }
  schedule_contex->stat_.OnFindFeasibleNodesStrictlyEnd();
  schedule_contex->stat_.OnAcquireResourcesBegin();
  AcquireResources(required_resources, candidate_node_id);
  schedule_contex->selected_nodes.emplace_back(std::move(candidate_node_id));
  schedule_contex->stat_.OnAcquireResourcesEnd();
}

bool GcsResourceScheduler::IsNodeFeasible(const NodeID &node_id,
                                          const ResourceSet &required_resources) {
  auto node_context_opt = gcs_resources_manager_.GetNodeContext(node_id);
  if (!node_context_opt.has_value()) {
    RAY_LOG(ERROR) << "The node " << node_id << " is unavailable.";
    return false;
  }

  ResourceSet new_required_resource_set;
  const ResourceSet *required_resource_set = &required_resources;
  if (!(*node_context_opt).scheduling_resources_->GetNormalTaskResources().IsEmpty()) {
    new_required_resource_set =
        (*node_context_opt).scheduling_resources_->GetNormalTaskResources();
    new_required_resource_set.AddResources(required_resources);
    required_resource_set = &new_required_resource_set;
  }

  const auto &available_resources =
      (*node_context_opt).scheduling_resources_->GetAvailableResources();
  if (!available_resources.IsSuperset(*required_resource_set)) {
    return false;
  }
  return true;
}

NodeID GcsResourceScheduler::AcquireResources(const ResourceSet &required_resources,
                                              std::vector<NodeContext> feasible_nodes,
                                              ResourceScheduleContext *context) {
  auto select_node =
      node_selector_->SelectNode(required_resources, std::move(feasible_nodes), context);
  AcquireResources(required_resources, select_node);
  return select_node;
}

void GcsResourceScheduler::AcquireResources(const ResourceSet &required_resources,
                                            const NodeID &node_id) {
  if (!node_id.IsNil()) {
    RAY_CHECK(gcs_resources_manager_.AcquireResources(node_id, required_resources));
    gcs_resources_manager_.AddNodeTotalRequiredResources(node_id, required_resources);
  }
}

}  // namespace gcs

}  // namespace ray
