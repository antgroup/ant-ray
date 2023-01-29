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

#include "ray/util/util.h"

namespace ray {
namespace gcs {

/// Save the time consumed by each part of the schedule.
struct ScheduleStat {
 public:
  void OnScheduleBegin();

  void OnScheduleEnd();

  int64_t GetScheduleCost() const;

  /// Start the timing of finding feasible nodes strictly.
  void OnFindFeasibleNodesStrictlyBegin();

  /// End the timing of finding feasible nodes strictly.
  void OnFindFeasibleNodesStrictlyEnd();

  /// Get the the time of finding feasible nodes strictly.
  int64_t GetFindFeasibleNodesStrictlyCost() const;

  /// Start the timing of finding feasible nodes softly.
  void OnFindFeasibleNodesSoftlyBegin();

  /// End the timing of finding feasible nodes softly.
  void OnFindFeasibleNodesSoftlyEnd();

  /// Get the the time of finding feasible nodes softly.
  int64_t GetFindFeasibleNodesSoftlyCost() const;

  /// Start the timing of acquiring resources.
  void OnAcquireResourcesBegin();

  /// End the timing of acquiring resources.
  void OnAcquireResourcesEnd();

  /// Get the time of acquiring resources.
  int64_t GetAcquireResourcesCost() const;

  /// Start the timing of prioritizing nodes.
  void OnPrioritizedNodesBegin();

  /// End the timing of prioritizing nodes.
  void OnPrioritizedNodesEnd();

  /// Get the time of prioritizing nodes.
  int64_t GetPrioritizedNodesCost() const;

  /// Start the timing of prioritizing nodes by scorer.
  void OnPrioritizedNodesByScorerBegin();

  /// End the timing of prioritizing nodes by scorer.
  void OnPrioritizedNodesByScorerEnd();

  /// Get the time of prioritizing nodes by scorer.
  int64_t GetPrioritizedNodesByScorerCost() const;

  /// Start the timing of selecting node.
  void OnSelectNodeBegin();

  /// End the timing of selecting node.
  void OnSelectNodeEnd();

  /// Get the time of selecting node.
  int64_t GetSelectNodeCost() const;

 private:
  int64_t schedule_begin_us_ = 0;
  int64_t schedule_cost_us_ = 0;

  int64_t find_feasible_nodes_strictly_begin_us_ = 0;
  int64_t find_feasible_nodes_strictly_cost_us_ = 0;

  int64_t find_feasible_nodes_softly_begin_us_ = 0;
  int64_t find_feasible_nodes_softly_cost_us_ = 0;

  int64_t acquire_resources_begin_us_ = 0;
  int64_t acquire_resources_cost_us_ = 0;

  int64_t prioritized_nodes_begin_us_ = 0;
  int64_t prioritized_nodes_cost_us_ = 0;

  int64_t prioritized_nodes_by_scorer_begin_us_ = 0;
  int64_t prioritized_nodes_by_scorer_cost_us_ = 0;

  int64_t select_node_begin_us_ = 0;
  int64_t select_node_cost_us_ = 0;
};
}  // namespace gcs
}  // namespace ray