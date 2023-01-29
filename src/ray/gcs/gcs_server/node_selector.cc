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

#include "ray/gcs/gcs_server/node_selector.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <vector>

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/task/scheduling_resources_util.h"

namespace ray {
namespace gcs {

//////////////////////////////////// Begin of NodeScorer ////////////////////////////////
double NodeScorer::Score(const ResourceSet &required_resources,
                         const NodeContext &node_context,
                         const ScheduleOptions *schedule_options) const {
  const auto &node_resources = *node_context.scheduling_resources_;
  ResourceSet new_available_resource_set;
  const ResourceSet *available_resource_set = &node_resources.GetAvailableResources();
  if (!node_resources.GetNormalTaskResources().IsEmpty()) {
    new_available_resource_set = node_resources.GetAvailableResources();
    new_available_resource_set.SubtractResources(node_resources.GetNormalTaskResources());
    available_resource_set = &new_available_resource_set;
  }
  const auto &available_resource_amount_map =
      available_resource_set->GetResourceAmountMap();

  double node_score = 0.0;
  uint64_t weight_sum = 0;
  for (const auto &entry : required_resources.GetResourceAmountMap()) {
    auto available_resource_amount_iter = available_resource_amount_map.find(entry.first);
    RAY_CHECK(available_resource_amount_iter != available_resource_amount_map.end());
    auto weight = schedule_options->GetResourceWeight(entry.first);
    auto score = Calculate(entry.second, available_resource_amount_iter->second);
    node_score += weight * score;
    weight_sum += weight;
  }
  if (!required_resources.GetResourceAmountMap().empty()) {
    node_score /= weight_sum;
  }

  return node_score;
}

double LeastResourceScorer::Calculate(const FractionalResourceQuantity &requested,
                                      const FractionalResourceQuantity &available) const {
  if (available == 0 || requested > available) {
    return -1;
  }
  return (available - requested).ToDouble() / available.ToDouble();
}

double MostResourceScorer::Calculate(const FractionalResourceQuantity &requested,
                                     const FractionalResourceQuantity &available) const {
  if (available == 0 || requested > available) {
    return -1;
  }
  return requested.ToDouble() / available.ToDouble();
}

double RuntimeLeastResourceScorer::Score(const ResourceSet &required_resources,
                                         const NodeContext &node_context,
                                         const ScheduleOptions *schedule_options) const {
  auto runtime_resources = node_context.scheduling_resources_->GetNodeRuntimeResources();
  double node_score = 0.0;
  uint64_t weight_sum = 0;
  for (const auto &entry : required_resources.GetResourceAmountMap()) {
    auto weight = schedule_options->GetResourceWeight(entry.first);
    auto score = Calculate(entry.second, runtime_resources.GetResource(entry.first));
    node_score += weight * score;
    weight_sum += weight;
  }
  if (!required_resources.GetResourceAmountMap().empty()) {
    node_score /= weight_sum;
  }
  RAY_LOG(DEBUG) << "RuntimeLeastResourceScorer is scoring node " << node_context.node_id_
                 << ", whose runtime resources are " << runtime_resources.ToString()
                 << ". Score = " << node_score;
  return node_score;
}

double RuntimeLeastResourceScorer::Calculate(
    const FractionalResourceQuantity &requested,
    const FractionalResourceQuantity &available) const {
  if (available == 0 || requested > available) {
    /// If Runtime*ResourceScorer returns a negative score (e.g., -1), the aggregate score
    /// might be reduced below zero, which might result in a result that no node can be
    /// selected. So Runtime*ResourceScorer only returns a zero here.
    return 0;
  }
  return (available - requested).ToDouble() / available.ToDouble();
}

double InnerProductScorer::Score(const ResourceSet &required_resources,
                                 const NodeContext &node_context,
                                 const ScheduleOptions *schedule_options) const {
  const SchedulingResources &node_resources = *node_context.scheduling_resources_;
  RAY_UNUSED(schedule_options);
  double val = 0;
  ResourceSet new_available_resource_set;
  const ResourceSet *available_resource_set = &node_resources.GetAvailableResources();
  if (!node_resources.GetNormalTaskResources().IsEmpty()) {
    new_available_resource_set = node_resources.GetAvailableResources();
    new_available_resource_set.SubtractResources(node_resources.GetNormalTaskResources());
    available_resource_set = &new_available_resource_set;
  }
  const auto &available = available_resource_set->GetResourceMap();
  const auto &total = node_resources.GetTotalResources().GetResourceMap();
  const auto &required = required_resources.GetResourceMap();

  for (auto &entry : required) {
    auto available_it = available.find(entry.first);
    if (available_it == available.end()) {
      continue;
    }

    auto total_it = total.find(entry.first);
    if (total_it == total.end()) {
      continue;
    }

    val += entry.second * available_it->second;
  }

  return val;
}

//////////////////////////////////// End of NodeScorer ////////////////////////////////

//////////////////////////////////// Begin of NodesFilter /////////////////////////////
std::vector<NodeContext> DefaultNodesFilter::FindFeasibleNodes(
    const ResourceSet &required_resources,
    std::function<bool(const NodeContext &)> node_affinities,
    std::shared_ptr<ScheduleOptions> schedule_options) {
  const std::vector<NodeContext> &node_list = gcs_resource_manager_.GetNodeContextList();
  if (RayConfig::instance().enable_concurrent_resource_scheduler() &&
      node_list.size() >=
          RayConfig::instance().minimum_nodes_for_concurrent_resoruce_scheduler()) {
    return FindFeasibleNodesConcurrently(required_resources, node_list, node_affinities,
                                         schedule_options);
  }

  return FindFeasibleNodes(required_resources, node_list, 0, node_list.size(),
                           node_affinities, schedule_options);
}

std::vector<NodeContext> DefaultNodesFilter::FindFeasibleNodes(
    const ResourceSet &required_resources, const std::vector<NodeContext> &node_list,
    size_t node_begin, size_t node_end,
    std::function<bool(const NodeContext &)> node_affinities,
    std::shared_ptr<ScheduleOptions> schedule_options) {
  auto rare_resource_scheduling_enabled =
      RayConfig::instance().rare_resource_scheduling_enabled();
  bool demand_contains_rare_resources = false;
  bool contains_placement_group = false;
  if (rare_resource_scheduling_enabled) {
    demand_contains_rare_resources = required_resources.ContainsRareResources();
    contains_placement_group = required_resources.ContainsPlacementGroup();
  }

  std::vector<NodeContext> feasible_nodes;
  for (size_t i = node_begin; i < node_end; i++) {
    const auto &node = node_list[i];
    if (node_affinities && !node_affinities(node)) {
      continue;
    }

    if (rare_resource_scheduling_enabled) {
      if (!contains_placement_group && !demand_contains_rare_resources &&
          node.contains_rare_resources_) {
        continue;
      }
    }

    if (IsNodeFeasible(required_resources, node, schedule_options)) {
      feasible_nodes.emplace_back(node);
    }
  }

  return feasible_nodes;
}

bool DefaultNodesFilter::IsNodeFeasible(
    const ResourceSet &required_resources, const NodeContext &node,
    std::shared_ptr<ScheduleOptions> schedule_options) {
  // We should exclude normal task resources from available resources,
  // so adding the required_resources to normal task resources, the added resources will
  // be compared with available resources to find feasible nodes.
  ResourceSet new_required_resource_set;
  const ResourceSet *required_resource_set = &required_resources;
  if (!node.scheduling_resources_->GetNormalTaskResources().IsEmpty()) {
    new_required_resource_set = node.scheduling_resources_->GetNormalTaskResources();
    new_required_resource_set.AddResources(required_resources);
    required_resource_set = &new_required_resource_set;
  }

  const auto &available_resources = node.scheduling_resources_->GetAvailableResources();
  if (!available_resources.IsSuperset(*required_resource_set)) {
    return false;
  }

  // Check whether exceeding the node-wise overcommit ratio.
  bool overcommit_exceeded = false;
  if (schedule_options && schedule_options->runtime_resource_scheduling_enabled_) {
    for (const auto &resource_label : RUNTIME_RESOURCE_LABELS) {
      auto overcommitted_resource =
          node.scheduling_resources_->GetTotalRequiredResources().GetResource(
              resource_label);
      overcommitted_resource += required_resource_set->GetResource(resource_label);

      auto total_resource =
          node.scheduling_resources_->GetTotalResources().GetResource(resource_label);
      if (total_resource > 0 &&
          overcommitted_resource.ToDouble() / total_resource.ToDouble() >
              schedule_options->node_overcommit_ratio_) {
        overcommit_exceeded = true;
        break;
      }
    }
  }
  return !overcommit_exceeded;
}

std::vector<NodeContext> DefaultNodesFilter::FindFeasibleNodesConcurrently(
    const ResourceSet &required_resources, const std::vector<NodeContext> &node_list,
    std::function<bool(const NodeContext &)> node_affinities,
    std::shared_ptr<ScheduleOptions> schedule_options) {
  const auto threads_num = pool_.GetThreadNumber();
  std::vector<std::vector<NodeContext>> feasible_nodes_list(threads_num);
  std::vector<std::promise<std::size_t>> promise_count_list(threads_num);
  std::size_t block_size = node_list.size() / threads_num + 1;
  for (std::size_t i = 0; i < threads_num; i++) {
    std::size_t node_begin = i * block_size;
    std::size_t node_end = node_begin + block_size < node_list.size()
                               ? node_begin + block_size
                               : node_list.size();
    boost::asio::post(pool_, [this, i, node_begin, node_end, &required_resources,
                              &node_list, node_affinities, schedule_options,
                              &feasible_nodes_list, &promise_count_list]() {
      feasible_nodes_list[i] =
          FindFeasibleNodes(required_resources, node_list, node_begin, node_end,
                            node_affinities, schedule_options);
      promise_count_list[i].set_value(feasible_nodes_list[i].size());
    });
  }

  std::size_t tot_node_count = 0;
  for (std::size_t i = 0; i < threads_num; i++) {
    tot_node_count += promise_count_list[i].get_future().get();
  }
  std::vector<NodeContext> feasible_nodes;
  feasible_nodes.reserve(tot_node_count);
  for (std::size_t i = 0; i < threads_num; i++) {
    if (!feasible_nodes_list[i].empty()) {
      feasible_nodes.insert(feasible_nodes.end(), feasible_nodes_list[i].begin(),
                            feasible_nodes_list[i].end());
    }
  }

  return feasible_nodes;
}
////////////////////////////////// End of NodesFilter ////////////////////////////////

///////////////////////////////// Begin of NodeSelector///////////////////////////////
NodeSelector::NodeSelector(thread_pool &pool)
    : gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      pool_(pool),
      inner_product_scorer_(InnerProductScorer()),
      least_resource_scorer_(LeastResourceScorer()),
      most_resource_scorer_(MostResourceScorer()),
      runtime_least_resource_scorer_(RuntimeLeastResourceScorer()) {}

std::vector<NodeSelector::NodeScore> NodeSelector::GetPrioritizeNodes(
    const ResourceSet &required_resources, std::vector<NodeContext> feasible_nodes,
    ResourceScheduleContext *context) {
  bool contains_big_memory = required_resources.Contains(kBigMemory_ResourceLabel);
  bool contains_acpu = required_resources.Contains(kACPU_ResourceLabel);
  std::string gpu_name = GetGPUResourceName(required_resources);
  if (contains_big_memory || contains_acpu || gpu_name != "") {
    auto schedule_options =
        std::make_shared<ScheduleOptions>(*context->GetScheduleOptions());
    // Random select top num_candidate_nodes_for_scheduling_ nodes to scheduling with pack
    // strategy (by default).
    schedule_options->pack_step_ = schedule_options->rare_resources_schedule_pack_step_;

    if (contains_big_memory) {
      // kBigMemory_ResourceLabel is just a flag, the weight of kMemory_ResourceLabel is
      // the real resoruces to be concerned.
      schedule_options->resource_weights_[kMemory_ResourceLabel] =
          RayConfig::instance().rare_resource_weight();
    }
    if (gpu_name != "") {
      // gpu_name is just a flag, the weight of it is
      // the real resoruces to be concerned.
      schedule_options->resource_weights_[gpu_name] =
          RayConfig::instance().rare_resource_weight();
    }
    context->SetScheduleOptions(schedule_options);
  }

  context->stat_.OnPrioritizedNodesBegin();
  // Prioritize nodes.
  auto prioritized_nodes =
      PrioritizeNodes(required_resources, std::move(feasible_nodes), context);
  context->stat_.OnPrioritizedNodesEnd();
  return prioritized_nodes;
}

NodeID NodeSelector::SelectNode(const ResourceSet &required_resources,
                                std::vector<NodeContext> feasible_nodes,
                                ResourceScheduleContext *context) {
  if (feasible_nodes.empty()) {
    return NodeID::Nil();
  }

  if (feasible_nodes.size() == 1) {
    return feasible_nodes.begin()->node_id_;
  }

  auto prioritized_nodes =
      GetPrioritizeNodes(required_resources, feasible_nodes, context);

  // Select node from the prioritized node list.
  context->stat_.OnSelectNodeBegin();
  auto selected_node = SelectNode(std::move(prioritized_nodes), context);
  context->stat_.OnSelectNodeEnd();
  return selected_node;
}

bool NodeSelector::LessThanThreshold(const NodeContext &node,
                                     const ResourceSet &required_resources,
                                     bool add_required_resources, double threshold) {
  RAY_CHECK(node.scheduling_resources_);
  const auto &scheduling_resources = node.scheduling_resources_;
  const auto &available_resource_amount =
      scheduling_resources->GetAvailableResources().GetResourceAmountMap();
  const auto &total_resource_amount =
      scheduling_resources->GetTotalResources().GetResourceAmountMap();

  bool is_less_than_threshold = true;
  for (const auto &entry : required_resources.GetResourceAmountMap()) {
    if (entry.first.find("_group_") != std::string::npos) {
      // Skip the resource of placement group.
      continue;
    }

    auto used_amount =
        total_resource_amount.at(entry.first) - available_resource_amount.at(entry.first);
    if (add_required_resources) {
      used_amount += entry.second;
    }

    if (used_amount.ToDouble() >=
        total_resource_amount.at(entry.first).ToDouble() * threshold) {
      is_less_than_threshold = false;
      break;
    }
  }

  return is_less_than_threshold;
}

std::vector<NodeSelector::NodeScore> NodeSelector::PrioritizeNodes(
    const ResourceSet &required_resources, std::vector<NodeContext> feasible_nodes,
    ResourceScheduleContext *context) {
  const auto &schedule_options = context->GetScheduleOptions();
  if (schedule_options->pack_step_ <= 0.0) {
    context->stat_.OnPrioritizedNodesByScorerBegin();
    auto scored_nodes = PrioritizeNodesByScorers(
        required_resources, feasible_nodes, 0, feasible_nodes.size(),
        schedule_options.get(),
        {&least_resource_scorer_, &runtime_least_resource_scorer_});
    context->stat_.OnPrioritizedNodesByScorerEnd();
    return scored_nodes;
  }

  if (schedule_options->pack_step_ >= 1.0) {
    context->stat_.OnPrioritizedNodesByScorerBegin();
    auto scored_nodes = PrioritizeNodesByScorers(
        required_resources, feasible_nodes, 0, feasible_nodes.size(),
        schedule_options.get(), {&most_resource_scorer_});
    context->stat_.OnPrioritizedNodesByScorerEnd();
    return scored_nodes;
  }

  // STEP_PACK starts here.
  // Check whether the 'threshold_' needs a increasement.
  auto bound = feasible_nodes.begin();
  while (bound == feasible_nodes.begin() && schedule_options->threshold_ < 1.0) {
    bound = std::partition(
        feasible_nodes.begin(), feasible_nodes.end(),
        /*less_than_threshold=*/
        [this, schedule_options, &required_resources](const NodeContext &node) {
          return LessThanThreshold(node, required_resources,
                                   /*add_required_resources=*/false,
                                   schedule_options->threshold_);
        });
    if (bound == feasible_nodes.begin()) {
      schedule_options->threshold_ += schedule_options->pack_step_;
      if (schedule_options->threshold_ > 1.0) {
        schedule_options->threshold_ = 1.0;
      }
      RAY_LOG(INFO) << "The threshold of " << context->ToString()
                    << " has been increased to " << schedule_options->threshold_;
    }
  }

  if (schedule_options->threshold_ == 1.0) {
    RAY_LOG(INFO) << "The schedule policy of " << context->ToString()
                  << " now becomes PACK";
    context->stat_.OnPrioritizedNodesByScorerBegin();
    auto scored_nodes = PrioritizeNodesByScorers(
        required_resources, feasible_nodes, 0, feasible_nodes.size(),
        schedule_options.get(), {&most_resource_scorer_});
    context->stat_.OnPrioritizedNodesByScorerEnd();
    return scored_nodes;
  }

  if (bound > feasible_nodes.begin()) {
    // There are nodes under the 'threshold_'. Now further check whether
    // there are 'feasible' nodes with enough resources (under the 'threshold_')
    // for the required resources.
    bound = std::partition(
        feasible_nodes.begin(), bound,
        /*less_than_threshold_considering_required_resources=*/
        [this, schedule_options, &required_resources](const NodeContext &node) {
          return LessThanThreshold(node, required_resources,
                                   /*add_required_resources=*/true,
                                   schedule_options->threshold_);
        });
    if (bound > feasible_nodes.begin()) {
      // Feasible nodes (under the 'threshold_') are found.
      // Now PACK among these candidates.
      context->stat_.OnPrioritizedNodesByScorerBegin();
      auto scored_nodes = PrioritizeNodesByScorers(
          required_resources, feasible_nodes, 0, bound - feasible_nodes.begin(),
          schedule_options.get(), {&most_resource_scorer_});
      context->stat_.OnPrioritizedNodesByScorerEnd();
      return scored_nodes;
    }
  }
  // There is no feasible node under the 'threshold_'. Now SPREAD among all nodes.
  context->stat_.OnPrioritizedNodesByScorerBegin();
  auto scored_nodes = PrioritizeNodesByScorers(
      required_resources, feasible_nodes, bound - feasible_nodes.begin(),
      feasible_nodes.size(), schedule_options.get(), {&least_resource_scorer_});
  context->stat_.OnPrioritizedNodesByScorerEnd();
  return scored_nodes;
}

std::vector<NodeSelector::NodeScore> NodeSelector::PrioritizeNodesByScorers(
    const ResourceSet &required_resources, const std::vector<NodeContext> &feasible_nodes,
    std::size_t feasible_nodes_begin, std::size_t feasible_nodes_end,
    const ScheduleOptions *schedule_options,
    const std::vector<NodeScorer *> &node_scorers) const {
  std::vector<uint64_t> scorer_weights(node_scorers.size());
  for (size_t i = 0; i < node_scorers.size(); i++) {
    scorer_weights[i] =
        schedule_options->GetScorerWeight(node_scorers[i]->GetScorerName());
  }

  if (RayConfig::instance().enable_concurrent_resource_scheduler() &&
      feasible_nodes_end - feasible_nodes_begin >=
          RayConfig::instance().minimum_nodes_for_concurrent_resoruce_scheduler()) {
    return PrioritizeNodesByScorersConcurrently(
        required_resources, feasible_nodes, feasible_nodes_begin, feasible_nodes_end,
        schedule_options, node_scorers, scorer_weights);
  }

  return PrioritizeNodesByScorers(feasible_nodes_begin, feasible_nodes_end,
                                  required_resources, feasible_nodes, schedule_options,
                                  node_scorers, scorer_weights);
}

std::vector<NodeSelector::NodeScore> NodeSelector::PrioritizeNodesByScorers(
    std::size_t feasible_nodes_begin, std::size_t feasible_nodes_end,
    const ResourceSet &required_resources, const std::vector<NodeContext> &feasible_nodes,
    const ScheduleOptions *schedule_options,
    const std::vector<NodeScorer *> &node_scorers,
    const std::vector<uint64_t> &scorer_weights) const {
  std::vector<NodeScore> node_score_list;
  for (auto feasible_nodes_index = feasible_nodes_begin;
       feasible_nodes_index < feasible_nodes_end; ++feasible_nodes_index) {
    const auto &feasible_node = feasible_nodes[feasible_nodes_index];
    const auto &node_id = feasible_node.node_id_;
    RAY_CHECK(feasible_node.scheduling_resources_);
    double weighted_node_score = 0.0;
    RAY_CHECK(node_scorers.size() == scorer_weights.size());
    for (size_t i = 0; i < node_scorers.size(); i++) {
      double node_score =
          node_scorers[i]->Score(required_resources, feasible_node, schedule_options);
      if (node_score < 0) {
        weighted_node_score = node_score;
        break;
      }
      weighted_node_score += scorer_weights[i] * node_score;
    }
    node_score_list.emplace_back(NodeScore(node_id, weighted_node_score));
  }

  return node_score_list;
}

std::vector<NodeSelector::NodeScore> NodeSelector::PrioritizeNodesByScorersConcurrently(
    const ResourceSet &required_resources, const std::vector<NodeContext> &feasible_nodes,
    std::size_t feasible_nodes_begin, std::size_t feasible_nodes_end,
    const ScheduleOptions *schedule_options,
    const std::vector<NodeScorer *> &node_scorers,
    const std::vector<uint64_t> &scorer_weights) const {
  const auto threads_num = pool_.GetThreadNumber();
  std::vector<std::vector<NodeScore>> node_score_lists(threads_num);
  std::vector<std::promise<std::size_t>> promise_count_list(threads_num);
  std::size_t block_size = (feasible_nodes_end - feasible_nodes_begin) / threads_num + 1;
  for (std::size_t i = 0; i < threads_num; i++) {
    std::size_t node_begin = i * block_size + feasible_nodes_begin;
    std::size_t node_end =
        node_begin + block_size < feasible_nodes_end - feasible_nodes_begin
            ? node_begin + block_size
            : feasible_nodes_end - feasible_nodes_begin;
    boost::asio::post(pool_, [this, i, &feasible_nodes, node_begin, node_end,
                              &required_resources, &node_score_lists, &promise_count_list,
                              &node_scorers, &scorer_weights, &schedule_options]() {
      node_score_lists[i] = PrioritizeNodesByScorers(
          node_begin, node_end, required_resources, feasible_nodes, schedule_options,
          node_scorers, scorer_weights);
      promise_count_list[i].set_value(node_score_lists[i].size());
    });
  }

  std::size_t tot_node_count = 0;
  for (std::size_t i = 0; i < threads_num; i++) {
    tot_node_count += promise_count_list[i].get_future().get();
  }
  std::vector<NodeScore> node_score_list;
  node_score_list.reserve(tot_node_count);
  for (std::size_t i = 0; i < threads_num; i++) {
    if (!node_score_lists[i].empty()) {
      node_score_list.insert(node_score_list.end(), node_score_lists[i].begin(),
                             node_score_lists[i].end());
    }
  }

  return node_score_list;
}

NodeID NodeSelector::SelectNode(std::vector<NodeScore> prioritized_nodes,
                                ResourceScheduleContext *context) {
  RAY_CHECK(context);
  if (prioritized_nodes.empty()) {
    return NodeID::Nil();
  }

  auto num_candidate_nodes_for_scheduling =
      context->GetScheduleOptions()->num_candidate_nodes_for_scheduling_;
  num_candidate_nodes_for_scheduling =
      std::min(num_candidate_nodes_for_scheduling, uint64_t(prioritized_nodes.size()));
  RAY_CHECK(num_candidate_nodes_for_scheduling > 0);

  std::nth_element(prioritized_nodes.begin(),
                   prioritized_nodes.begin() + num_candidate_nodes_for_scheduling,
                   prioritized_nodes.end(),
                   [](const NodeScore &node1, const NodeScore &node2) {
                     return node1.score_ > node2.score_;
                   });

  // Skip nodes with negative score.
  do {
    if (prioritized_nodes[num_candidate_nodes_for_scheduling - 1].score_ >= 0) {
      break;
    }
  } while (--num_candidate_nodes_for_scheduling > 0);

  if (num_candidate_nodes_for_scheduling == 0) {
    return NodeID::Nil();
  }

  std::uniform_int_distribution<int> distrib(0, num_candidate_nodes_for_scheduling - 1);
  int index = distrib(gen_);
  return prioritized_nodes[index].node_id_;
}

NodeID NodeSelector::SelectNodeWithSoftMatchNumberMap(
    const ResourceSet &required_resources, std::vector<NodeContext> feasible_nodes,
    ResourceScheduleContext *context,
    const absl::flat_hash_map<NodeID, int> &soft_match_number_map,
    const int expression_size) {
  if (feasible_nodes.empty()) {
    return NodeID::Nil();
  }

  if (feasible_nodes.size() == 1) {
    return feasible_nodes.begin()->node_id_;
  }

  std::vector<double> weight = {0.5, 0.5};

  auto prioritized_nodes =
      GetPrioritizeNodes(required_resources, feasible_nodes, context);
  for (auto &node_score : prioritized_nodes) {
    if (soft_match_number_map.contains(node_score.node_id_)) {
      node_score.score_ = (node_score.score_ * weight[0]) +
                          ((double)soft_match_number_map.at(node_score.node_id_) /
                           expression_size * weight[1]);
    } else {
      node_score.score_ = (node_score.score_ * weight[0]);
    }
  }
  // Select node from the prioritized node list.
  context->stat_.OnSelectNodeBegin();
  auto selected_node = SelectNode(std::move(prioritized_nodes), context);
  context->stat_.OnSelectNodeEnd();
  return selected_node;
}

///////////////////////////////// End of NodeSelector ////////////////////////////////////

}  // namespace gcs
}  // namespace ray
