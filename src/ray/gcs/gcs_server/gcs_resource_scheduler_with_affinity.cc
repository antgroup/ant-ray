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

#include <algorithm>
#include <optional>
#include <ostream>
#include <vector>
#include "ray/gcs/gcs_server/gcs_node_context_cache.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

void SetIntersection(const absl::flat_hash_set<NodeID> &A,
                     const absl::flat_hash_set<NodeID> &B,
                     absl::flat_hash_set<NodeID> &output_set) {
  for (const auto &value : A) {
    if (B.contains(value)) {
      output_set.insert(value);
    }
  }
}

void SetComplement(const absl::flat_hash_set<NodeID> &primary_set,
                   const absl::flat_hash_set<NodeID> &antiaffinity_set,
                   absl::flat_hash_set<NodeID> &ouput_set) {
  for (const auto &value : primary_set) {
    if (!(antiaffinity_set.contains(value))) {
      ouput_set.insert(value);
    }
  }
}

void GcsResourceScheduler::ActorAffinitySchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  auto schedule_context = dynamic_cast<GcsActorAffinityScheduleContext *>(context);
  RAY_CHECK(schedule_context != nullptr)
      << "The context can't conversion to GcsActorAffinityScheduleContext.";
  RAY_CHECK(
      schedule_context->GetSchedulingStrategy().has_actor_affinity_scheduling_strategy())
      << "The scheduling_strategy is not actor affinity strategy.";
  const auto &actor_affinity_strategy =
      schedule_context->GetSchedulingStrategy().actor_affinity_scheduling_strategy();
  RAY_CHECK(required_resources_list.size() == 1)
      << "The required_resources_list size must be 1.";
  const auto &required_resources = *(required_resources_list[0]);
  auto feasible_nodes = nodes_filter_->FindFeasibleNodes(
      required_resources, [this, schedule_context](const NodeContext &node) {
        if (!is_node_schedulable_callback_(node, schedule_context)) {
          return false;
        }
        return true;
      });
  if (feasible_nodes.empty() && RAY_LOG_ENABLED(DEBUG)) {
    RAY_LOG(DEBUG) << "Failed to schedule with actor affinity strategy for can't find "
                      "feasibel nodes.";
    return;
  }
  auto select_node = SelectNodeByActorAffinityStrategy(
      schedule_context, actor_affinity_strategy, required_resources, feasible_nodes);
  schedule_context->stat_.OnAcquireResourcesBegin();
  AcquireResources(required_resources, select_node);
  schedule_context->selected_nodes.emplace_back(select_node);
  schedule_context->stat_.OnAcquireResourcesEnd();
}

NodeID GcsResourceScheduler::SelectNodeByActorAffinityStrategy(
    GcsActorAffinityScheduleContext *schedule_context,
    const rpc::ActorAffinitySchedulingStrategy &actor_affinity_strategy,
    const ResourceSet &required_resources,
    const std::vector<NodeContext> &feasible_nodes) {
  auto match_expressions_size = actor_affinity_strategy.match_expressions_size();
  std::vector<ray::rpc::ActorAffinityMatchExpression> strict_expressions;
  std::vector<ray::rpc::ActorAffinityMatchExpression> soft_expressions;
  for (int i = 0; i < match_expressions_size; ++i) {
    const auto &match_expression = actor_affinity_strategy.match_expressions(i);
    if (!match_expression.soft()) {
      strict_expressions.emplace_back(match_expression);
    } else {
      soft_expressions.emplace_back(match_expression);
    }
  }
  schedule_context->stat_.OnFindFeasibleNodesStrictlyBegin();
  auto strict_candidate_nodes = GetNodeIDsByContexts(feasible_nodes);
  for (const auto &expression : strict_expressions) {
    strict_candidate_nodes = FilterNodeByActorAffinityExpression(
        schedule_context, expression, strict_candidate_nodes);
    if (strict_candidate_nodes.size() <= 0) {
      return NodeID::Nil();
    }
  }
  schedule_context->stat_.OnFindFeasibleNodesStrictlyEnd();
  if (!soft_expressions.empty()) {
    schedule_context->stat_.OnFindFeasibleNodesSoftlyBegin();
    absl::flat_hash_map<NodeID, int> soft_match_number_map;
    for (const auto &expression : soft_expressions) {
      auto soft_candidate_nodes = FilterNodeByActorAffinityExpression(
          schedule_context, expression, strict_candidate_nodes);
      RecordSoftMatchNumber(soft_match_number_map, soft_candidate_nodes);
    }
    auto match_all_expression_nodes =
        GetMatchAllExpressionNodes(soft_match_number_map, soft_expressions.size());
    if (match_all_expression_nodes.empty()) {
      auto select_node = node_selector_->SelectNodeWithSoftMatchNumberMap(
          required_resources,
          gcs_resources_manager_.GetContextsByNodeIDs(strict_candidate_nodes),
          schedule_context, soft_match_number_map, soft_expressions.size());
      schedule_context->stat_.OnFindFeasibleNodesSoftlyBegin();
      return select_node;
    } else {
      strict_candidate_nodes = match_all_expression_nodes;
    }
    schedule_context->stat_.OnFindFeasibleNodesSoftlyBegin();
  }
  auto select_node = node_selector_->SelectNode(
      required_resources,
      gcs_resources_manager_.GetContextsByNodeIDs(strict_candidate_nodes),
      schedule_context);
  return select_node;
}
absl::flat_hash_set<NodeID> GcsResourceScheduler::FilterNodeByActorAffinityExpression(
    GcsActorAffinityScheduleContext *schedule_context,
    const rpc::ActorAffinityMatchExpression &match_expression,
    const absl::flat_hash_set<NodeID> &candidate_nodes) {
  absl::flat_hash_set<NodeID> final_nodes;
  const auto &nodegroup_id = schedule_context->GetNodegroupId();
  const auto &ray_namespace = schedule_context->GetRayNamespace();
  const auto &affinity_operator = match_expression.actor_affinity_operator();
  const auto &key = match_expression.key();
  absl::flat_hash_set<std::string> values;
  for (int i = 0; i < match_expression.values_size(); i++) {
    values.emplace(match_expression.values(i));
  }
  switch (affinity_operator) {
  case rpc::ActorAffinityOperator::IN: {
    auto affinity_nodes = schedule_context->GetGcsLabelManager().GetNodesByKeyAndValue(
        nodegroup_id, ray_namespace, key, values);
    SetIntersection(candidate_nodes, affinity_nodes, final_nodes);
    break;
  }
  case rpc::ActorAffinityOperator::NOT_IN: {
    auto antiaffinity_nodes =
        schedule_context->GetGcsLabelManager().GetNodesByKeyAndValue(
            nodegroup_id, ray_namespace, key, values);
    SetComplement(candidate_nodes, antiaffinity_nodes, final_nodes);
    break;
  }
  case rpc::ActorAffinityOperator::EXISTS: {
    auto exist_key_nodes = schedule_context->GetGcsLabelManager().GetNodesByKey(
        nodegroup_id, ray_namespace, key);
    SetIntersection(candidate_nodes, exist_key_nodes, final_nodes);
    break;
  }
  case rpc::ActorAffinityOperator::DOES_NOT_EXIST: {
    auto not_exist_key_nodes = schedule_context->GetGcsLabelManager().GetNodesByKey(
        nodegroup_id, ray_namespace, key);
    SetComplement(candidate_nodes, not_exist_key_nodes, final_nodes);
    break;
  }
  default:
    RAY_LOG(FATAL) << "Failed to filter node by actor affinity expression for affinity "
                      "operator is unknow, operator:"
                   << rpc::ActorAffinityOperator_Name(affinity_operator);
  }
  return final_nodes;
}

void GcsResourceScheduler::RecordSoftMatchNumber(
    absl::flat_hash_map<NodeID, int> &soft_match_number_map,
    const absl::flat_hash_set<NodeID> &soft_match_nodes) {
  for (const auto &node : soft_match_nodes) {
    if (soft_match_number_map.contains(node)) {
      soft_match_number_map[node] += 1;
    } else {
      soft_match_number_map[node] = 1;
    }
  }
}

absl::flat_hash_set<NodeID> GcsResourceScheduler::GetMatchAllExpressionNodes(
    const absl::flat_hash_map<NodeID, int> &soft_match_number_map, int expression_size) {
  absl::flat_hash_set<NodeID> match_all_nodes;
  for (const auto &[node_id, match_number] : soft_match_number_map) {
    if (match_number >= expression_size) {
      match_all_nodes.emplace(node_id);
    }
  }
  return match_all_nodes;
}

std::string GcsResourceScheduler::GetMatchExpressionDebugString(
    const ::ray::rpc::ActorAffinitySchedulingStrategy &scheduling_strategy) const {
  std::ostringstream out;
  for (int i = 0; i < scheduling_strategy.match_expressions_size(); i++) {
    const auto &match_expression = scheduling_strategy.match_expressions(i);
    out << "Expression " << i << " : " << match_expression.key() << " "
        << rpc::ActorAffinityOperator_Name(match_expression.actor_affinity_operator())
        << " ["
        << absl::StrJoin(match_expression.values().begin(),
                         match_expression.values().end(), ",")
        << "] (" << (match_expression.soft() ? "soft" : "hard") << ")\n";
  }
  return out.str();
}

void GcsResourceScheduler::NodeAffinitySchedule(
    const std::vector<const ResourceSet *> &required_resources_list,
    ResourceScheduleContext *context) {
  auto schedule_context = dynamic_cast<GcsNodeAffinityScheduleContext *>(context);
  RAY_CHECK(schedule_context != nullptr)
      << "The context can't conversion to GcsNodeAffinityScheduleContext.";
  RAY_CHECK(required_resources_list.size() == 1)
      << "The required_resources_list size must be 1.";
  const auto &required_resources = *(required_resources_list[0]);

  schedule_context->stat_.OnFindFeasibleNodesStrictlyBegin();
  const auto &nodes = schedule_context->GetNodes();
  auto is_soft = schedule_context->IsSoft();
  auto is_anti_affinity = schedule_context->IsAntiAffinity();

  absl::flat_hash_set<NodeID> seleted_nodes;
  if (is_anti_affinity) {
    auto feasible_nodes = nodes_filter_->FindFeasibleNodes(
        required_resources, [this, schedule_context](const NodeContext &node) {
          if (!is_node_schedulable_callback_(node, schedule_context)) {
            return false;
          }
          return true;
        });
    if (feasible_nodes.empty() && RAY_LOG_ENABLED(DEBUG)) {
      RAY_LOG(DEBUG) << "Failed to schedule with actor affinity strategy for can't find "
                        "feasibel nodes.";
      return;
    }
    SetComplement(GetNodeIDsByContexts(feasible_nodes), nodes, seleted_nodes);
  } else {
    // Affinity nodes size must be 1.
    RAY_CHECK(nodes.size() == 1);
    for (const auto &node_id : nodes) {
      auto node_context_opt = gcs_resources_manager_.GetNodeContext(node_id);
      if (node_context_opt.has_value()) {
        const auto &node_context = *node_context_opt;
        if (is_node_schedulable_callback_(node_context, schedule_context)) {
          if (nodes_filter_->IsNodeFeasible(required_resources, node_context)) {
            seleted_nodes.emplace(node_id);
          }
        }
      }
    }
  }
  schedule_context->stat_.OnFindFeasibleNodesStrictlyEnd();

  if (seleted_nodes.empty() && is_soft) {
    SpreadSchedule(required_resources_list, schedule_context);
  } else {
    schedule_context->stat_.OnAcquireResourcesBegin();
    const auto select_node = AcquireResources(
        required_resources, gcs_resources_manager_.GetContextsByNodeIDs(seleted_nodes),
        schedule_context);
    schedule_context->selected_nodes.emplace_back(select_node);
    schedule_context->stat_.OnAcquireResourcesEnd();
  }
}

}  // namespace gcs

}  // namespace ray
