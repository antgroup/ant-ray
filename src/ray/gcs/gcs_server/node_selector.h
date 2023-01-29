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

#include <random>

#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/util/thread_pool.h"

namespace ray {
namespace gcs {

/// NodeScorer is a scorer to make a grade to the node, which is used for scheduling
/// decision.
class NodeScorer {
 public:
  explicit NodeScorer(){};
  virtual ~NodeScorer() = default;
  /// \brief Make a grade based on the node resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_context The node context with resources info.
  virtual double Score(const ResourceSet &required_resources,
                       const NodeContext &node_context,
                       const ScheduleOptions *schedule_options) const;

  virtual const std::string &GetScorerName() const { return scorer_name_; }

 protected:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  virtual double Calculate(const FractionalResourceQuantity &requested,
                           const FractionalResourceQuantity &available) const {
    return 0.;
  }

  std::string scorer_name_;
};

/// LeastResourceScorer is a score plugin that favors nodes with fewer allocation
/// requested resources based on requested resources.
class LeastResourceScorer : public NodeScorer {
 public:
  explicit LeastResourceScorer() { scorer_name_ = "LeastResource"; };

 protected:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  double Calculate(const FractionalResourceQuantity &requested,
                   const FractionalResourceQuantity &available) const;
};

/// MostResourceScorer is a score plugin that favors nodes with more allocation
/// requested resources based on requested resources.
class MostResourceScorer : public NodeScorer {
 public:
  explicit MostResourceScorer() { scorer_name_ = "MostResource"; };

 protected:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the available resources.
  /// \return Score of the node.
  double Calculate(const FractionalResourceQuantity &requested,
                   const FractionalResourceQuantity &available) const;
};

/// RuntimeLeastResourceScorer is a score plugin that favors nodes with
/// less runtime resource usages.
class RuntimeLeastResourceScorer : public NodeScorer {
 public:
  explicit RuntimeLeastResourceScorer() { scorer_name_ = "RuntimeLeastResource"; };

  /// \brief Make a grade based on the runtime available resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_context The node context with resources info.
  /// \return Score of the node.
  double Score(const ResourceSet &required_resources, const NodeContext &node_context,
               const ScheduleOptions *schedule_options) const override;

 protected:
  /// \brief Calculate one of the resource scores.
  ///
  /// \param requested Quantity of one of the required resources.
  /// \param available Quantity of one of the runtime available resources.
  /// \return Score of the node.
  double Calculate(const FractionalResourceQuantity &requested,
                   const FractionalResourceQuantity &available) const override;
};

/// InnerProductScorer is used to calculate inner product value between required_resources
/// and node_resources.
class InnerProductScorer : public NodeScorer {
 public:
  explicit InnerProductScorer() { scorer_name_ = "InnerProduct"; };

  /// \brief Make a grade based on the node resources, the grade value is inner product
  /// value between required_resources and node_resources.
  ///
  /// \param required_resources The required resources.
  /// \param node_context The node context with resources info.
  /// \return Score of the node.
  double Score(const ResourceSet &required_resources, const NodeContext &node_context,
               const ScheduleOptions *schedule_options) const override;
};

/// NodesFilter is a filter, it will find all nodes that meet the required resources.

/// DefaultNodesFilter is a filter, it will find all nodes that meet the required
/// resources. it uses LeastResourceScorer as the default scorer to make grades for each
/// of the feasible nodes and find the optimal one.
class DefaultNodesFilter {
 public:
  explicit DefaultNodesFilter(GcsResourceManager &gcs_resource_manager, thread_pool &pool)
      : gcs_resource_manager_(gcs_resource_manager), pool_(pool) {}

  /// \brief Find all nodes that meet the required resources.
  ///
  /// \param required_resources The required resources.
  /// \return Nodes that meet the required resources.
  std::vector<NodeContext> FindFeasibleNodes(
      const ResourceSet &required_resources,
      std::function<bool(const NodeContext &)> node_affinities = nullptr,
      std::shared_ptr<ScheduleOptions> schedule_options = nullptr);

  /// \brief Single find all nodes that meet the required resources.
  ///
  /// \param required_resources The required resources.
  /// \return Nodes that meet the required resources.
  std::vector<NodeContext> FindFeasibleNodes(
      const ResourceSet &required_resources, const std::vector<NodeContext> &node_list,
      size_t node_begin, size_t node_end,
      std::function<bool(const NodeContext &)> node_affinities = nullptr,
      std::shared_ptr<ScheduleOptions> schedule_options = nullptr);

  /// \brief Multi-threaded find all nodes that meet the required resources.
  ///
  /// \param required_resources The required resources.
  /// \return Nodes that meet the required resources.
  std::vector<NodeContext> FindFeasibleNodesConcurrently(
      const ResourceSet &required_resources, const std::vector<NodeContext> &node_list,
      std::function<bool(const NodeContext &)> node_affinities = nullptr,
      std::shared_ptr<ScheduleOptions> schedule_options = nullptr);

  bool IsNodeFeasible(const ResourceSet &required_resources, const NodeContext &node,
                      std::shared_ptr<ScheduleOptions> schedule_options = nullptr);

 private:
  GcsResourceManager &gcs_resource_manager_;
  thread_pool &pool_;
};

/// NodeSelector is used to select an optimal node to allocate the specified resources.
class NodeSelector {
 public:
  explicit NodeSelector(thread_pool &pool);

  /// Select an optimal node to allocate the specified resources.
  /// \param required_resources The resources required to allocate a new worker process.
  /// \param nodes_filter The filter to find all nodes that meet the required resources.
  /// \return The optimal node.

  NodeID SelectNode(const ResourceSet &required_resources,
                    std::vector<NodeContext> feasible_nodes,
                    ResourceScheduleContext *context);

  NodeID SelectNodeWithSoftMatchNumberMap(
      const ResourceSet &required_resources, std::vector<NodeContext> feasible_nodes,
      ResourceScheduleContext *context,
      const absl::flat_hash_map<NodeID, int> &soft_match_number_map,
      const int expression_size);

 protected:
  struct NodeScore {
    explicit NodeScore() = default;
    explicit NodeScore(const NodeID &node_id, double score)
        : node_id_(node_id), score_(score) {}
    NodeID node_id_;
    double score_ = 0.0;
  };

  /// Get a list of node scores after prioritize
  std::vector<NodeSelector::NodeScore> GetPrioritizeNodes(
      const ResourceSet &required_resources, std::vector<NodeContext> feasible_nodes,
      ResourceScheduleContext *context);

  /// Prioritizes the nodes by running the score plugins.
  std::vector<NodeScore> PrioritizeNodes(const ResourceSet &required_resources,
                                         std::vector<NodeContext> feasible_nodes,
                                         ResourceScheduleContext *context);

  /// Prioritizes the nodes by running the score plugins.
  std::vector<NodeScore> PrioritizeNodesByScorers(
      const ResourceSet &required_resources,
      const std::vector<NodeContext> &feasible_nodes, std::size_t feasible_nodes_begin,
      std::size_t feasible_nodes_end, const ScheduleOptions *schedule_options,
      const std::vector<NodeScorer *> &node_scorers) const;

  /// Single prioritizes the nodes by running the score plugins.
  std::vector<NodeScore> PrioritizeNodesByScorers(
      std::size_t feasible_nodes_begin, std::size_t feasible_nodes_end,
      const ResourceSet &required_resources,
      const std::vector<NodeContext> &feasible_nodes,
      const ScheduleOptions *schedule_options,
      const std::vector<NodeScorer *> &node_scorers,
      const std::vector<uint64_t> &scorer_weights) const;

  /// Multithreaded prioritizes the nodes by running the score plugins.
  std::vector<NodeScore> PrioritizeNodesByScorersConcurrently(
      const ResourceSet &required_resources,
      const std::vector<NodeContext> &feasible_nodes, std::size_t feasible_nodes_begin,
      std::size_t feasible_nodes_end, const ScheduleOptions *schedule_options,
      const std::vector<NodeScorer *> &node_scorers,
      const std::vector<uint64_t> &scorer_weights) const;

  /// Select node from prioritized nodes list.
  NodeID SelectNode(std::vector<NodeScore> prioritized_nodes,
                    ResourceScheduleContext *context);

  bool LessThanThreshold(const NodeContext &node_resource_context,
                         const ResourceSet &required_resources,
                         bool add_required_resources, double threshold);

 private:
  /// Internally maintained random number generator.
  std::mt19937_64 gen_;
  thread_pool &pool_;

  InnerProductScorer inner_product_scorer_;
  LeastResourceScorer least_resource_scorer_;
  MostResourceScorer most_resource_scorer_;
  RuntimeLeastResourceScorer runtime_least_resource_scorer_;
};

}  // namespace gcs
}  // namespace ray
