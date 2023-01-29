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

#include "ray/gcs/gcs_server/gcs_resource_schedule_stat.h"

namespace ray {
namespace gcs {
void ScheduleStat::OnScheduleBegin() { schedule_begin_us_ = current_sys_time_us(); }

void ScheduleStat::OnScheduleEnd() {
  schedule_cost_us_ = current_sys_time_us() - schedule_begin_us_;
}

void ScheduleStat::OnFindFeasibleNodesStrictlyBegin() {
  find_feasible_nodes_strictly_begin_us_ = current_sys_time_us();
}

int64_t ScheduleStat::GetScheduleCost() const { return schedule_cost_us_; }

void ScheduleStat::OnFindFeasibleNodesStrictlyEnd() {
  find_feasible_nodes_strictly_cost_us_ =
      current_sys_time_us() - find_feasible_nodes_strictly_begin_us_;
}

int64_t ScheduleStat::GetFindFeasibleNodesStrictlyCost() const {
  return find_feasible_nodes_strictly_cost_us_;
}

void ScheduleStat::OnFindFeasibleNodesSoftlyBegin() {
  find_feasible_nodes_softly_begin_us_ = current_sys_time_us();
}

void ScheduleStat::OnFindFeasibleNodesSoftlyEnd() {
  find_feasible_nodes_softly_cost_us_ =
      current_sys_time_us() - find_feasible_nodes_softly_begin_us_;
}

int64_t ScheduleStat::GetFindFeasibleNodesSoftlyCost() const {
  return find_feasible_nodes_softly_cost_us_;
}

void ScheduleStat::OnAcquireResourcesBegin() {
  acquire_resources_begin_us_ = current_sys_time_us();
}

void ScheduleStat::OnAcquireResourcesEnd() {
  acquire_resources_cost_us_ = current_sys_time_us() - acquire_resources_begin_us_;
}

int64_t ScheduleStat::GetAcquireResourcesCost() const {
  return acquire_resources_cost_us_;
}

void ScheduleStat::OnPrioritizedNodesBegin() {
  prioritized_nodes_begin_us_ = current_sys_time_us();
}

void ScheduleStat::OnPrioritizedNodesEnd() {
  prioritized_nodes_cost_us_ = current_sys_time_us() - prioritized_nodes_begin_us_;
}

int64_t ScheduleStat::GetPrioritizedNodesCost() const {
  return prioritized_nodes_cost_us_;
}

void ScheduleStat::OnPrioritizedNodesByScorerBegin() {
  prioritized_nodes_by_scorer_begin_us_ = current_sys_time_us();
}

void ScheduleStat::OnPrioritizedNodesByScorerEnd() {
  prioritized_nodes_by_scorer_cost_us_ =
      current_sys_time_us() - prioritized_nodes_by_scorer_begin_us_;
}

int64_t ScheduleStat::GetPrioritizedNodesByScorerCost() const {
  return prioritized_nodes_by_scorer_cost_us_;
}

void ScheduleStat::OnSelectNodeBegin() { select_node_begin_us_ = current_sys_time_us(); }

void ScheduleStat::OnSelectNodeEnd() {
  select_node_cost_us_ = current_sys_time_us() - select_node_begin_us_;
}

int64_t ScheduleStat::GetSelectNodeCost() const { return select_node_cost_us_; }

}  // namespace gcs
}  // namespace ray