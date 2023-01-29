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

#include "ray/raylet/scheduling/cluster_resource_scheduler.h"

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/scheduling_resources_util.h"

namespace ray {

ClusterResourceScheduler::ClusterResourceScheduler(
    int64_t local_node_id, const NodeResources &local_node_resources,
    gcs::GcsClient &gcs_client,
    const std::function<bool(const std::string &binary_node_id,
                             const ScheduleContext &schedule_context)>
        &is_node_schedulable_fn)
    : local_node_id_(local_node_id),
      gen_(std::chrono::high_resolution_clock::now().time_since_epoch().count()),
      gcs_client_(&gcs_client),
      is_node_schedulable_fn_(is_node_schedulable_fn) {
  scheduling_policy_ = std::make_unique<raylet_scheduling_policy::SchedulingPolicy>(
      local_node_id_, nodes_);
  AddOrUpdateNode(local_node_id_, local_node_resources);
  InitLocalResources(local_node_resources);
}

ClusterResourceScheduler::ClusterResourceScheduler(
    const std::string &local_node_id,
    const std::unordered_map<std::string, double> &local_node_resources,
    gcs::GcsClient &gcs_client, std::function<int64_t(void)> get_used_object_store_memory,
    const std::function<bool(const std::string &binary_node_id,
                             const ScheduleContext &schedule_context)>
        &is_node_schedulable_fn,
    std::function<bool()> is_overcommit_enabled_fn)
    : loadbalance_spillback_(RayConfig::instance().scheduler_loadbalance_spillback()),
      gcs_client_(&gcs_client),
      is_node_schedulable_fn_(is_node_schedulable_fn),
      is_overcommit_enabled_fn_(std::move(is_overcommit_enabled_fn)) {
  local_node_id_ = string_to_int_map_.Insert(local_node_id);
  scheduling_policy_ = std::make_unique<raylet_scheduling_policy::SchedulingPolicy>(
      local_node_id_, nodes_);
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, local_node_resources, local_node_resources);

  AddOrUpdateNode(local_node_id_, node_resources);
  InitLocalResources(node_resources);
  get_used_object_store_memory_ = get_used_object_store_memory;
}

bool ClusterResourceScheduler::NodeAlive(int64_t node_id) const {
  if (node_id == local_node_id_) {
    return true;
  }
  if (node_id == -1) {
    return false;
  }
  auto node_id_binary = string_to_int_map_.Get(node_id);
  return gcs_client_->Nodes().Get(NodeID::FromBinary(node_id_binary)) != nullptr;
}

void ClusterResourceScheduler::AddOrUpdateNode(
    const std::string &node_id,
    const std::unordered_map<std::string, double> &resources_total,
    const std::unordered_map<std::string, double> &resources_available) {
  NodeResources node_resources = ResourceMapToNodeResources(
      string_to_int_map_, resources_total, resources_available);
  AddOrUpdateNode(string_to_int_map_.Insert(node_id), node_resources);
}

void ClusterResourceScheduler::AddOrUpdateNode(int64_t node_id,
                                               const NodeResources &node_resources) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // This node is new, so add it to the map.
    nodes_.emplace(node_id, node_resources);
  } else {
    // This node exists, so update its resources.
    it->second = Node(node_resources);
  }
}

bool ClusterResourceScheduler::UpdateNode(const std::string &node_id_string,
                                          const rpc::ResourcesData &resource_data) {
  auto node_id = string_to_int_map_.Insert(node_id_string);
  if (!nodes_.contains(node_id)) {
    return false;
  }

  NodeResources local_view;
  RAY_CHECK(GetNodeResources(node_id, &local_view));

  if (resource_data.resources_total_size() > 0) {
    auto resources_total = MapFromProtobuf(resource_data.resources_total());
    auto resources_available = MapFromProtobuf(resource_data.resources_available());
    NodeResources node_resources = ResourceMapToNodeResources(
        string_to_int_map_, resources_total, resources_available);
    for (size_t i = 0; i < node_resources.predefined_resources.size(); ++i) {
      local_view.predefined_resources[i].total =
          node_resources.predefined_resources[i].total;
    }

    // Since the total resources are changed, it is necessory to delete the invalid
    // resources from local_view.
    for (auto iter = local_view.custom_resources.begin();
         iter != local_view.custom_resources.end();) {
      auto current = iter++;
      if (!node_resources.custom_resources.contains(current->first)) {
        local_view.custom_resources.erase(current);
      }
    }

    for (auto &entry : node_resources.custom_resources) {
      local_view.custom_resources[entry.first].total = entry.second.total;
    }
  }

  if (resource_data.resources_available_changed()) {
    auto available_resources = ResourceMapToTaskRequest(
        string_to_int_map_, MapFromProtobuf(resource_data.resources_available()));
    for (size_t i = 0; i < available_resources.predefined_resources.size(); ++i) {
      local_view.predefined_resources[i].available =
          available_resources.predefined_resources[i].demand;
    }
    for (auto &entry : available_resources.custom_resources) {
      local_view.custom_resources[entry.id].available = entry.demand;
    }
  }

  AddOrUpdateNode(node_id, local_view);
  return true;
}

bool ClusterResourceScheduler::RemoveNode(int64_t node_id) {
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    // Node not found.
    return false;
  } else {
    nodes_.erase(it);
    string_to_int_map_.Remove(node_id);
    return true;
  }
}

bool ClusterResourceScheduler::RemoveNode(const std::string &node_id_string) {
  auto node_id = string_to_int_map_.Get(node_id_string);
  if (node_id == -1) {
    return false;
  }

  return RemoveNode(node_id);
}

bool ClusterResourceScheduler::IsLocallyFeasible(
    const std::unordered_map<std::string, double> shape,
    const TaskSpecification &task_spec) {
  const TaskRequest task_req = ResourceMapToTaskRequest(string_to_int_map_, shape);
  RAY_CHECK(nodes_.contains(local_node_id_));
  const auto &it = nodes_.find(local_node_id_);
  RAY_CHECK(it != nodes_.end());
  return IsFeasible(task_req, it->first, it->second.GetLocalView(), task_spec);
}

bool ClusterResourceScheduler::IsFeasible(const TaskRequest &task_req, int64_t node_id,
                                          const NodeResources &resources,
                                          const TaskSpecification &task_spec) const {
  // Check whether this node is schedulable
  if (!IsNodeUsable(node_id, {task_spec, task_req, resources})) {
    return false;
  }
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (task_req.predefined_resources[i].demand >
        resources.predefined_resources[i].total) {
      return false;
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = resources.custom_resources.find(task_req_custom_resource.id);

    if (it == resources.custom_resources.end()) {
      return false;
    }
    if (task_req_custom_resource.demand > it->second.total) {
      return false;
    }
  }

  return true;
}

bool ClusterResourceScheduler::IsSchedulable(const TaskRequest &task_req, int64_t node_id,
                                             const NodeResources &resources,
                                             const TaskSpecification &task_spec) const {
  if (!IsNodeUsable(node_id, {task_spec, task_req, resources})) {
    return false;
  }

  int violations = 0;
  // First, check predefined resources.
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (task_req.predefined_resources[i].demand >
        resources.predefined_resources[i].available) {
      if (task_req.predefined_resources[i].soft) {
        // A soft constraint has been violated.
        // Just remember this as soft violations do not preclude a task
        // from being scheduled.
        violations++;
      } else {
        // A hard constraint has been violated, so we cannot schedule
        // this task request.
        return false;
      }
    }
  }

  // Now check custom resources.
  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = resources.custom_resources.find(task_req_custom_resource.id);

    if (it == resources.custom_resources.end()) {
      // Requested resource doesn't exist at this node. However, this
      // is a soft constraint, so just increment "violations" and continue.
      if (task_req_custom_resource.soft) {
        violations++;
      } else {
        // This is a hard constraint so cannot schedule this task request.
        return false;
      }
    } else {
      if (task_req_custom_resource.demand > it->second.available) {
        // Resource constraint is violated, but since it is soft
        // just increase the "violations" and continue.
        if (task_req_custom_resource.soft) {
          violations++;
        } else {
          return false;
        }
      }
    }
  }

  if (task_req.placement_hints.size() > 0) {
    auto it_p = task_req.placement_hints.find(node_id);
    if (it_p == task_req.placement_hints.end()) {
      // Node not found in the placement_hints list, so
      // record this as a soft constraint violation.
      violations++;
    }
  }

  return true;
}

int64_t ClusterResourceScheduler::GetBestSchedulableNode(
    const TaskRequest &task_req, const rpc::SchedulingStrategy &scheduling_strategy,
    bool actor_creation, bool force_spillback, int64_t *total_violations,
    bool *is_infeasible, const TaskSpecification &task_spec) {
  if (actor_creation && RayConfig::instance().gcs_task_scheduling_enabled()) {
    return local_node_id_;
  }
  // The zero cpu actor is a special case that must be handled the same way by all
  // scheduling policies.
  if (actor_creation && task_req.IsEmpty() &&
      (!task_spec.IsNodeAffinitySchedulingStrategy())) {
    int64_t best_node = -1;
    // This an actor which requires no resources.
    // Pick a random node to to avoid all scheduling all actors on the local node.
    std::vector<int64_t> schedulable_nodes;
    for (auto &entry : nodes_) {
      if (IsNodeUsable(entry.first, {task_spec, task_req, entry.second.GetLocalView()})) {
        schedulable_nodes.emplace_back(entry.first);
      }
    }
    if (schedulable_nodes.size() > 0) {
      std::uniform_int_distribution<int> distribution(0, schedulable_nodes.size() - 1);
      int idx = distribution(gen_);
      auto iter = std::next(schedulable_nodes.begin(), idx);
      for (size_t i = 0; i < schedulable_nodes.size(); ++i) {
        // TODO(iycheng): Here is there are a lot of nodes died, the
        // distribution might not be even.
        if (NodeAlive(*iter)) {
          best_node = *iter;
          break;
        }
        ++iter;
        if (iter == schedulable_nodes.end()) {
          iter = schedulable_nodes.begin();
        }
      }
    }
    RAY_LOG(DEBUG) << "GetBestSchedulableNode, best_node = " << best_node
                   << ", # nodes = " << nodes_.size()
                   << ", task_req = " << task_req.DebugString(string_to_int_map_);
    return best_node;
  }

  if (!IsNodeUsable(
          local_node_id_,
          {task_spec, task_req, nodes_.find(local_node_id_)->second.GetLocalView()})) {
    RAY_LOG(DEBUG)
        << "Local node is not schedulable, force spillback task to other nodes.";
    force_spillback = true;
  }

  auto is_node_available_fn = [this, &task_req, &task_spec](auto node_id) {
    // ANT-INTERNAL: we have to check whether a node is alive and usable (considering
    // nodegroup, node freezing, etc.) when scheduling the task.
    const auto node_it = nodes_.find(node_id);
    RAY_CHECK(node_it != nodes_.end());
    return this->NodeAlive(node_id) &&
           this->IsNodeUsable(node_id,
                              {task_spec, task_req, node_it->second.GetLocalView()});
  };

  int64_t best_node_id = -1;
  if (scheduling_strategy.scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy) {
    best_node_id = scheduling_policy_->SpreadPolicy(
        task_req, force_spillback, force_spillback, is_node_available_fn);
  } else if (scheduling_strategy.has_node_affinity_scheduling_strategy()) {
    absl::flat_hash_set<int64_t> node_ids(
        scheduling_strategy.node_affinity_scheduling_strategy().nodes_size());
    for (int i = 0;
         i < scheduling_strategy.node_affinity_scheduling_strategy().nodes_size(); i++) {
      node_ids.emplace(string_to_int_map_.Get(
          scheduling_strategy.node_affinity_scheduling_strategy().nodes(i)));
    }
    bool soft = scheduling_strategy.node_affinity_scheduling_strategy().soft();
    bool anti_affinity =
        scheduling_strategy.node_affinity_scheduling_strategy().anti_affinity();
    best_node_id = scheduling_policy_->NodeAffinityPolicy(
        task_req, RayConfig::instance().scheduler_spread_threshold(), force_spillback,
        force_spillback, is_node_available_fn, node_ids, soft, anti_affinity);
  } else {
    // TODO (Alex): Setting require_available == force_spillback is a hack in order to
    // remain bug compatible with the legacy scheduling algorithms.
    best_node_id = scheduling_policy_->HybridPolicy(
        task_req,
        scheduling_strategy.scheduling_strategy_case() ==
                rpc::SchedulingStrategy::SchedulingStrategyCase::kSpreadSchedulingStrategy
            ? 0.0
            : RayConfig::instance().scheduler_spread_threshold(),
        force_spillback, force_spillback, is_node_available_fn);
  }

  *is_infeasible = best_node_id == -1 ? true : false;
  if (!*is_infeasible) {
    // TODO (Alex): Support soft constraints if needed later.
    *total_violations = 0;
  }

  RAY_LOG(DEBUG) << "Scheduling decision. "
                 << "forcing spillback: " << force_spillback
                 << ". Best node: " << best_node_id
                 << ", is infeasible: " << *is_infeasible;
  return best_node_id;
}

std::string ClusterResourceScheduler::GetBestSchedulableNode(
    const std::unordered_map<std::string, double> &task_resources,
    const rpc::SchedulingStrategy &scheduling_strategy, bool actor_creation,
    bool force_spillback, int64_t *total_violations, bool *is_infeasible,
    const TaskSpecification &task_spec) {
  TaskRequest task_request = ResourceMapToTaskRequest(string_to_int_map_, task_resources);
  int64_t node_id =
      GetBestSchedulableNode(task_request, scheduling_strategy, actor_creation,
                             force_spillback, total_violations, is_infeasible, task_spec);

  std::string id_string;
  if (node_id == -1) {
    // This is not a schedulable node, so return empty string.
    return "";
  }
  // Return the string name of the node.
  return string_to_int_map_.Get(node_id);
}

bool ClusterResourceScheduler::SubtractRemoteNodeAvailableResources(
    int64_t node_id, const TaskRequest &task_req, const TaskSpecification &task_spec) {
  RAY_CHECK(node_id != local_node_id_);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return false;
  }
  NodeResources *resources = it->second.GetMutableLocalView();

  // Just double check this node can still schedule the task request.
  if (!IsSchedulable(task_req, node_id, *resources, task_spec)) {
    return false;
  }

  FixedPoint zero(0.);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    resources->predefined_resources[i].available =
        std::max(FixedPoint(0), resources->predefined_resources[i].available -
                                    task_req.predefined_resources[i].demand);
  }

  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = resources->custom_resources.find(task_req_custom_resource.id);
    if (it != resources->custom_resources.end()) {
      it->second.available =
          std::max(FixedPoint(0), it->second.available - task_req_custom_resource.demand);
    }
  }
  return true;
}

bool ClusterResourceScheduler::GetNodeResources(int64_t node_id,
                                                NodeResources *ret_resources) const {
  auto it = nodes_.find(node_id);
  if (it != nodes_.end()) {
    *ret_resources = it->second.GetLocalView();
    return true;
  } else {
    return false;
  }
}

const NodeResources &ClusterResourceScheduler::GetLocalNodeResources() const {
  const auto &node_it = nodes_.find(local_node_id_);
  RAY_CHECK(node_it != nodes_.end());
  return node_it->second.GetLocalView();
}

int64_t ClusterResourceScheduler::NumNodes() const { return nodes_.size(); }

const StringIdMap &ClusterResourceScheduler::GetStringIdMap() const {
  return string_to_int_map_;
}

void ClusterResourceScheduler::AddLocalResourceInstances(
    const std::string &resource_name, const std::vector<FixedPoint> &instances) {
  ResourceInstanceCapacities *node_instances;
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);
  if (kCPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[CPU];
  } else if (kGPU_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[GPU];
  } else if (kObjectStoreMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[OBJECT_STORE_MEM];
  } else if (kMemory_ResourceLabel == resource_name) {
    node_instances = &local_resources_.predefined_resources[MEM];
  } else {
    string_to_int_map_.Insert(resource_name);
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    node_instances = &local_resources_.custom_resources[resource_id];
  }

  if (node_instances->total.size() < instances.size()) {
    node_instances->total.resize(instances.size());
    node_instances->available.resize(instances.size());
  }

  for (size_t i = 0; i < instances.size(); i++) {
    node_instances->available[i] += instances[i];

    // ANT-INTERNAL
    // Updating the total value with the max of total and available may lead to
    // smaller result than the expected one, especially when removing and adding
    // PG bundles.
    // For example, there is a PG resource (e.g., "memory_group_XXX" without bundle
    // indexes) with total and available values as [500 MB]:[0 MB]. Then remove 3
    // bundles (each with 100 MB), resource would become [200 MB]:[0 MB]; then
    // sequentially add 3 bundles (each with 100 MB), resource would become
    // [200 MB]:[100 MB] --> [200 MB]:[200 MB] --> [300 MB]:[300 MB] step by step.
    // We can find the total value (300 MB) is now smaller than the right one (500
    // MB), which may introduce unexpected actor creation failures.
    // The open-source version may not run into this problem
    // because PG resizing has not been supported. Ant version replaces this updating
    // method for now.

    // node_instances->total[i] =
    //    std::max(node_instances->total[i], node_instances->available[i]);
    node_instances->total[i] += instances[i];
  }
  UpdateLocalAvailableResourcesFromResourceInstances();
}

bool ClusterResourceScheduler::IsAvailableResourceEmpty(
    const std::string &resource_name) {
  auto it = nodes_.find(local_node_id_);
  if (it == nodes_.end()) {
    RAY_LOG(WARNING) << "Can't find local node:[" << local_node_id_
                     << "] when check local available resource.";
    return true;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };

  auto local_view = it->second.GetMutableLocalView();
  if (idx != -1) {
    return local_view->predefined_resources[idx].available <= 0;
  }
  string_to_int_map_.Insert(resource_name);
  int64_t resource_id = string_to_int_map_.Get(resource_name);
  auto itr = local_view->custom_resources.find(resource_id);
  if (itr != local_view->custom_resources.end()) {
    return itr->second.available <= 0;
  } else {
    return true;
  }
}

// Ant-Internal
bool ClusterResourceScheduler::IsTotalResourceEmpty(const std::string &resource_name) {
  auto it = nodes_.find(local_node_id_);
  if (it == nodes_.end()) {
    RAY_LOG(WARNING) << "Can't find local node:[" << local_node_id_
                     << "] when check local total resource.";
    return true;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };

  auto local_view = it->second.GetMutableLocalView();
  if (idx != -1) {
    return local_view->predefined_resources[idx].total <= 0;
  }
  string_to_int_map_.Insert(resource_name);
  int64_t resource_id = string_to_int_map_.Get(resource_name);
  auto itr = local_view->custom_resources.find(resource_id);
  if (itr != local_view->custom_resources.end()) {
    return itr->second.total <= 0;
  } else {
    return true;
  }
}

void ClusterResourceScheduler::UpdateResourceCapacity(const std::string &node_id_string,
                                                      const std::string &resource_name,
                                                      double resource_total) {
  int64_t node_id = string_to_int_map_.Get(node_id_string);

  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    NodeResources node_resources;
    node_resources.predefined_resources.resize(PredefinedResources_MAX);
    node_id = string_to_int_map_.Insert(node_id_string);
    it = nodes_.emplace(node_id, node_resources).first;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };

  auto local_view = it->second.GetMutableLocalView();
  FixedPoint resource_total_fp(resource_total);
  if (idx != -1) {
    auto diff_capacity = resource_total_fp - local_view->predefined_resources[idx].total;
    local_view->predefined_resources[idx].total += diff_capacity;
    local_view->predefined_resources[idx].available += diff_capacity;
    if (local_view->predefined_resources[idx].available < 0) {
      local_view->predefined_resources[idx].available = 0;
    }
    if (local_view->predefined_resources[idx].total < 0) {
      local_view->predefined_resources[idx].total = 0;
    }
  } else {
    string_to_int_map_.Insert(resource_name);
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = local_view->custom_resources.find(resource_id);
    if (itr != local_view->custom_resources.end()) {
      auto diff_capacity = resource_total_fp - itr->second.total;
      itr->second.total += diff_capacity;
      itr->second.available += diff_capacity;
      if (itr->second.available < 0) {
        itr->second.available = 0;
      }
      if (itr->second.total < 0) {
        itr->second.total = 0;
      }
    } else {
      ResourceCapacity resource_capacity;
      resource_capacity.total = resource_capacity.available = resource_total_fp;
      local_view->custom_resources.emplace(resource_id, resource_capacity);
    }
  }
}

void ClusterResourceScheduler::DeleteLocalResource(const std::string &resource_name) {
  DeleteResource(string_to_int_map_.Get(local_node_id_), resource_name);
}

void ClusterResourceScheduler::DeleteResource(const std::string &node_id_string,
                                              const std::string &resource_name) {
  int64_t node_id = string_to_int_map_.Get(node_id_string);
  auto it = nodes_.find(node_id);
  if (it == nodes_.end()) {
    return;
  }

  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };
  auto local_view = it->second.GetMutableLocalView();
  if (idx != -1) {
    local_view->predefined_resources[idx].available = 0;
    local_view->predefined_resources[idx].total = 0;

    if (node_id == local_node_id_) {
      local_resources_.predefined_resources[idx].total.clear();
      local_resources_.predefined_resources[idx].available.clear();
    }
  } else {
    int64_t resource_id = string_to_int_map_.Get(resource_name);
    auto itr = local_view->custom_resources.find(resource_id);
    if (itr != local_view->custom_resources.end()) {
      string_to_int_map_.Remove(resource_id);
      local_view->custom_resources.erase(itr);
    }

    auto c_itr = local_resources_.custom_resources.find(resource_id);
    if (node_id == local_node_id_ && c_itr != local_resources_.custom_resources.end()) {
      local_resources_.custom_resources[resource_id].total.clear();
      local_resources_.custom_resources[resource_id].available.clear();
      local_resources_.custom_resources.erase(c_itr);
    }
  }
}

std::string ClusterResourceScheduler::SerializedTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) const {
  bool has_added_resource = false;
  std::stringstream buffer;
  buffer << "{";
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    std::vector<FixedPoint> resource = task_allocation->predefined_resources[i];
    if (resource.empty()) {
      continue;
    }
    if (has_added_resource) {
      buffer << ",";
    }
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    buffer << "\"" << resource_name << "\":";
    bool is_unit_instance = (resource.size() == 1 ? false : true);
    if (!is_unit_instance) {
      buffer << resource[0];
    } else {
      buffer << "[";
      for (size_t i = 0; i < resource.size(); i++) {
        buffer << resource[i];
        if (i < resource.size() - 1) {
          buffer << ", ";
        }
      }
      buffer << "]";
    }
    has_added_resource = true;
  }
  // TODO (chenk008): add custom_resources
  buffer << "}";
  return buffer.str();
}

std::string ClusterResourceScheduler::DebugString(void) const {
  std::stringstream buffer;
  buffer << "\nLocal id: " << NodeID::FromBinary(string_to_int_map_.Get(local_node_id_));
  buffer << " Local resources: " << local_resources_.DebugString(string_to_int_map_);
  for (auto &node : nodes_) {
    buffer << "node id: " << NodeID::FromBinary(string_to_int_map_.Get(node.first));
    buffer << node.second.GetLocalView().DebugString(string_to_int_map_);
  }
  return buffer.str();
}

void ClusterResourceScheduler::InitResourceInstances(
    FixedPoint total, bool unit_instances, ResourceInstanceCapacities *instance_list) {
  if (unit_instances) {
    size_t num_instances = static_cast<size_t>(total.Double());
    instance_list->total.resize(num_instances);
    instance_list->available.resize(num_instances);
    for (size_t i = 0; i < num_instances; i++) {
      instance_list->total[i] = instance_list->available[i] = 1.0;
    };
  } else {
    instance_list->total.resize(1);
    instance_list->available.resize(1);
    instance_list->total[0] = instance_list->available[0] = total;
  }
}

std::string ClusterResourceScheduler::GetLocalResourceViewString() const {
  const auto &node_it = nodes_.find(local_node_id_);
  RAY_CHECK(node_it != nodes_.end());
  return node_it->second.GetLocalView().DictString(string_to_int_map_);
}

void ClusterResourceScheduler::InitLocalResources(const NodeResources &node_resources) {
  local_resources_.predefined_resources.resize(PredefinedResources_MAX);

  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (node_resources.predefined_resources[i].total > 0) {
      InitResourceInstances(
          node_resources.predefined_resources[i].total,
          (UnitInstanceResources.find(i) != UnitInstanceResources.end()),
          &local_resources_.predefined_resources[i]);
    }
  }

  if (node_resources.custom_resources.size() == 0) {
    return;
  }

  for (auto it = node_resources.custom_resources.begin();
       it != node_resources.custom_resources.end(); ++it) {
    if (it->second.total > 0) {
      ResourceInstanceCapacities instance_list;
      InitResourceInstances(it->second.total, false, &instance_list);
      local_resources_.custom_resources.emplace(it->first, instance_list);
    }
  }
}

std::vector<FixedPoint> ClusterResourceScheduler::AddAvailableResourceInstances(
    std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances) {
  std::vector<FixedPoint> overflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    resource_instances->available[i] = resource_instances->available[i] + available[i];
    if (resource_instances->available[i] > resource_instances->total[i]) {
      overflow[i] = (resource_instances->available[i] - resource_instances->total[i]);
      resource_instances->available[i] = resource_instances->total[i];
    }
  }

  return overflow;
}

std::vector<FixedPoint> ClusterResourceScheduler::SubtractAvailableResourceInstances(
    std::vector<FixedPoint> available, ResourceInstanceCapacities *resource_instances,
    bool allow_going_negative) {
  RAY_CHECK(available.size() == resource_instances->available.size());

  std::vector<FixedPoint> underflow(available.size(), 0.);
  for (size_t i = 0; i < available.size(); i++) {
    if (resource_instances->available[i] < 0) {
      if (allow_going_negative) {
        resource_instances->available[i] =
            resource_instances->available[i] - available[i];
      } else {
        underflow[i] = available[i];  // No change in the value in this case.
      }
    } else {
      resource_instances->available[i] = resource_instances->available[i] - available[i];
      if (resource_instances->available[i] < 0 && !allow_going_negative) {
        underflow[i] = -resource_instances->available[i];
        resource_instances->available[i] = 0;
      }
    }
  }
  return underflow;
}

bool ClusterResourceScheduler::RemoveResourceByInstance(
    const std::string &resource_name, const std::vector<FixedPoint> &instances) {
  // Note: Only support customer resource currently.
  int64_t resource_id = string_to_int_map_.Get(resource_name);
  if (resource_id == -1) {
    return false;
  }
  ResourceInstanceCapacities *node_instances =
      &local_resources_.custom_resources[resource_id];
  if (instances.size() > node_instances->total.size()) {
    RAY_LOG(DEBUG) << "Remove resource by instance failed as the removing resource "
                      "instance size is "
                      "begger that the current total resource instance size.";
    return false;
  }

  for (size_t i = 0; i < instances.size(); i++) {
    node_instances->available[i] -= instances[i];
    if (node_instances->available[i] < 0) {
      node_instances->available[i] = 0;
    }

    node_instances->total[i] -= instances[i];
    if (node_instances->total[i] < 0) {
      node_instances->total[i] = 0;
    }
  }
  UpdateLocalAvailableResourcesFromResourceInstances();
  return true;
}

bool ClusterResourceScheduler::AllocateResourceInstances(
    FixedPoint demand, bool soft, std::vector<FixedPoint> &available,
    std::vector<FixedPoint> *allocation) {
  allocation->resize(available.size());
  FixedPoint remaining_demand = demand;

  if (available.size() == 1) {
    // This resource has just an instance.
    if (available[0] >= remaining_demand) {
      available[0] -= remaining_demand;
      (*allocation)[0] = remaining_demand;
      return true;
    } else {
      if (soft) {
        available[0] = 0;
        return true;
      }
      // Not enough capacity.
      return false;
    }
  }

  // If resources has multiple instances, each instance has total capacity of 1.
  //
  // If this resource constraint is hard, as long as remaining_demand is greater
  // than 1., allocate full unit-capacity instances until the remaining_demand becomes
  // fractional. Then try to find the best fit for the fractional remaining_resources.
  // Best fist means allocating the resource instance with the smallest available
  // capacity greater than remaining_demand
  //
  // If resource constraint is soft, allocate as many full unit-capacity resources and
  // then distribute remaining_demand across remaining instances. Note that in case we
  // can overallocate this resource.
  if (remaining_demand >= 1.) {
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] == 1.) {
        // Allocate a full unit-capacity instance.
        (*allocation)[i] = 1.;
        available[i] = 0;
        remaining_demand -= 1.;
      }
      if (remaining_demand < 1.) {
        break;
      }
    }
  }

  if (soft && remaining_demand > 0) {
    // Just get as many resources as available.
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        available[i] -= remaining_demand;
        (*allocation)[i] = remaining_demand;
        return true;
      } else {
        (*allocation)[i] += available[i];
        remaining_demand -= available[i];
        available[i] = 0;
      }
    }
    return true;
  }

  if (remaining_demand >= 1.) {
    // Cannot satisfy a demand greater than one if no unit capacity resource is
    // available.
    return false;
  }

  // Remaining demand is fractional. Find the best fit, if exists.
  if (remaining_demand > 0.) {
    int64_t idx_best_fit = -1;
    FixedPoint available_best_fit = 1.;
    for (size_t i = 0; i < available.size(); i++) {
      if (available[i] >= remaining_demand) {
        if (idx_best_fit == -1 ||
            (available[i] - remaining_demand < available_best_fit)) {
          available_best_fit = available[i] - remaining_demand;
          idx_best_fit = static_cast<int64_t>(i);
        }
      }
    }
    if (idx_best_fit == -1) {
      return false;
    } else {
      (*allocation)[idx_best_fit] = remaining_demand;
      available[idx_best_fit] -= remaining_demand;
    }
  }
  return true;
}

bool ClusterResourceScheduler::AllocateTaskResourceInstances(
    const TaskRequest &task_req, std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  if (nodes_.find(local_node_id_) == nodes_.end()) {
    return false;
  }
  task_allocation->predefined_resources.resize(PredefinedResources_MAX);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    if (task_req.predefined_resources[i].demand > 0) {
      if (!AllocateResourceInstances(task_req.predefined_resources[i].demand,
                                     task_req.predefined_resources[i].soft,
                                     local_resources_.predefined_resources[i].available,
                                     &task_allocation->predefined_resources[i])) {
        // Allocation failed. Restore node's local resources by freeing the resources
        // of the failed allocation.
        FreeTaskResourceInstances(task_allocation);
        return false;
      }
    }
  }

  for (const auto &task_req_custom_resource : task_req.custom_resources) {
    auto it = local_resources_.custom_resources.find(task_req_custom_resource.id);
    if (it != local_resources_.custom_resources.end()) {
      if (task_req_custom_resource.demand > 0) {
        std::vector<FixedPoint> allocation;
        bool success = AllocateResourceInstances(task_req_custom_resource.demand,
                                                 task_req_custom_resource.soft,
                                                 it->second.available, &allocation);
        // Even if allocation failed we need to remember partial allocations to
        // correctly free resources.
        task_allocation->custom_resources.emplace(it->first, allocation);
        if (!success) {
          // Allocation failed. Restore node's local resources by freeing the resources
          // of the failed allocation.
          FreeTaskResourceInstances(task_allocation);
          return false;
        }
      }
    } else {
      return false;
    }
  }
  return true;
}

void ClusterResourceScheduler::UpdateLocalAvailableResourcesFromResourceInstances() {
  auto it_local_node = nodes_.find(local_node_id_);
  RAY_CHECK(it_local_node != nodes_.end());

  auto local_view = it_local_node->second.GetMutableLocalView();
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    local_view->predefined_resources[i].available = 0;
    for (size_t j = 0; j < local_resources_.predefined_resources[i].available.size();
         j++) {
      local_view->predefined_resources[i].available +=
          local_resources_.predefined_resources[i].available[j];
    }
  }

  for (auto &custom_resource : local_resources_.custom_resources) {
    int64_t resource_name = custom_resource.first;
    auto &instances = custom_resource.second;

    FixedPoint available = std::accumulate(instances.available.begin(),
                                           instances.available.end(), FixedPoint());
    FixedPoint total =
        std::accumulate(instances.total.begin(), instances.total.end(), FixedPoint());

    local_view->custom_resources[resource_name].available = available;
    local_view->custom_resources[resource_name].total = total;
  }
}

void ClusterResourceScheduler::FreeTaskResourceInstances(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  for (size_t i = 0; i < PredefinedResources_MAX; i++) {
    AddAvailableResourceInstances(task_allocation->predefined_resources[i],
                                  &local_resources_.predefined_resources[i]);
  }

  for (const auto &task_allocation_custom_resource : task_allocation->custom_resources) {
    auto it =
        local_resources_.custom_resources.find(task_allocation_custom_resource.first);
    if (it != local_resources_.custom_resources.end()) {
      AddAvailableResourceInstances(task_allocation_custom_resource.second, &it->second);
    }
  }
}

std::vector<double> ClusterResourceScheduler::AddCPUResourceInstances(
    std::vector<double> &cpu_instances) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No overflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto overflow = AddAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> ClusterResourceScheduler::SubtractCPUResourceInstances(
    std::vector<double> &cpu_instances, bool allow_going_negative) {
  std::vector<FixedPoint> cpu_instances_fp =
      VectorDoubleToVectorFixedPoint(cpu_instances);

  if (cpu_instances.size() == 0) {
    return cpu_instances;  // No underflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto underflow = SubtractAvailableResourceInstances(
      cpu_instances_fp, &local_resources_.predefined_resources[CPU],
      allow_going_negative);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(underflow);
}

std::vector<double> ClusterResourceScheduler::AddGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No overflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto overflow = AddAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(overflow);
}

std::vector<double> ClusterResourceScheduler::SubtractGPUResourceInstances(
    std::vector<double> &gpu_instances) {
  std::vector<FixedPoint> gpu_instances_fp =
      VectorDoubleToVectorFixedPoint(gpu_instances);

  if (gpu_instances.size() == 0) {
    return gpu_instances;  // No underflow.
  }
  RAY_CHECK(nodes_.find(local_node_id_) != nodes_.end());

  auto underflow = SubtractAvailableResourceInstances(
      gpu_instances_fp, &local_resources_.predefined_resources[GPU]);
  UpdateLocalAvailableResourcesFromResourceInstances();

  return VectorFixedPointToVectorDouble(underflow);
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const TaskRequest &task_request,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (AllocateTaskResourceInstances(task_request, task_allocation)) {
    UpdateLocalAvailableResourcesFromResourceInstances();
    return true;
  }
  return false;
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const std::unordered_map<std::string, double> &task_resources,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  TaskRequest task_request = ResourceMapToTaskRequest(string_to_int_map_, task_resources);

  return AllocateLocalTaskResources(task_request, task_allocation);
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const TaskSpecification &task_spec,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  TaskRequest task_request = ResourceMapToTaskRequest(
      string_to_int_map_, task_spec.GetRequiredResources().GetResourceMap());
  if (RayConfig::instance().gcs_task_scheduling_enabled() &&
      task_spec.IsActorCreationTask() && is_overcommit_enabled_fn_ &&
      is_overcommit_enabled_fn_()) {
    task_request.predefined_resources[CPU].soft = true;
    task_request.predefined_resources[MEM].soft = true;
    for (auto &resource_entry : task_request.custom_resources) {
      if (GetResourceNameFromIndex(resource_entry.id).find("CPU_group") == 0 ||
          GetResourceNameFromIndex(resource_entry.id).find("memory_group") == 0) {
        resource_entry.soft = true;
      }
    }
    RAY_LOG(DEBUG) << "Allow resource overcommit for task request: "
                   << task_request.DebugString(string_to_int_map_);
  }
  return AllocateLocalTaskResources(task_request, task_allocation);
}

bool ClusterResourceScheduler::AllocateLocalTaskResources(
    const BundleSpecification &bundle_spec,
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  RAY_CHECK(task_allocation != nullptr);
  TaskRequest task_request = ResourceMapToTaskRequest(
      string_to_int_map_, bundle_spec.GetRequiredResources().GetResourceMap());
  if (RayConfig::instance().gcs_task_scheduling_enabled() && is_overcommit_enabled_fn_ &&
      is_overcommit_enabled_fn_()) {
    task_request.predefined_resources[CPU].soft = true;
    task_request.predefined_resources[MEM].soft = true;
    RAY_LOG(DEBUG) << "Allow resource overcommit for task request: "
                   << task_request.DebugString(string_to_int_map_);
  }
  return AllocateLocalTaskResources(task_request, task_allocation);
}

std::string ClusterResourceScheduler::GetResourceNameFromIndex(int64_t res_idx) {
  if (res_idx == CPU) {
    return ray::kCPU_ResourceLabel;
  } else if (res_idx == GPU) {
    return ray::kGPU_ResourceLabel;
  } else if (res_idx == OBJECT_STORE_MEM) {
    return ray::kObjectStoreMemory_ResourceLabel;
  } else if (res_idx == MEM) {
    return ray::kMemory_ResourceLabel;
  } else {
    return string_to_int_map_.Get((uint64_t)res_idx);
  }
}

bool ClusterResourceScheduler::AllocateRemoteTaskResources(
    const std::string &node_string,
    const std::unordered_map<std::string, double> &task_resources,
    const TaskSpecification &task_spec) {
  TaskRequest task_request = ResourceMapToTaskRequest(string_to_int_map_, task_resources);
  auto node_id = string_to_int_map_.Insert(node_string);
  RAY_CHECK(node_id != local_node_id_);
  return SubtractRemoteNodeAvailableResources(node_id, task_request, task_spec);
}

void ClusterResourceScheduler::ReleaseWorkerResources(
    std::shared_ptr<TaskResourceInstances> task_allocation) {
  if (task_allocation == nullptr || task_allocation->IsEmpty()) {
    return;
  }
  FreeTaskResourceInstances(task_allocation);
  UpdateLocalAvailableResourcesFromResourceInstances();
}

void ClusterResourceScheduler::UpdateLastResourceUsage(
    std::shared_ptr<SchedulingResources> gcs_resources) {
  last_report_resources_ = std::make_unique<NodeResources>(ResourceMapToNodeResources(
      string_to_int_map_, gcs_resources->GetTotalResources().GetResourceMap(),
      gcs_resources->GetAvailableResources().GetResourceMap()));
}

void ClusterResourceScheduler::FillResourceUsage(rpc::ResourcesData &resources_data,
                                                 bool need_whole_report) {
  NodeResources resources;

  RAY_CHECK(GetNodeResources(local_node_id_, &resources))
      << "Error: Populating heartbeat failed. Please file a bug report: "
         "https://github.com/ray-project/ray/issues/new.";

  // Initialize if last report resources is empty.
  if (!last_report_resources_) {
    NodeResources node_resources =
        ResourceMapToNodeResources(string_to_int_map_, {{}}, {{}});
    last_report_resources_.reset(new NodeResources(node_resources));
  }

  // Automatically report object store usage.
  // XXX: this MUTATES the resources field, which is needed since we are storing
  // it in last_report_resources_.
  if (get_used_object_store_memory_ != nullptr) {
    auto &capacity = resources.predefined_resources[OBJECT_STORE_MEM];
    double used = get_used_object_store_memory_();
    capacity.available = FixedPoint(capacity.total.Double() - used);
  }

  bool is_resources_total_changed = (resources.custom_resources.size() !=
                                     last_report_resources_->custom_resources.size());
  resources_data.set_resources_available_changed(need_whole_report);
  for (int i = 0; i < PredefinedResources_MAX; i++) {
    const auto &label = ResourceEnumToString((PredefinedResources)i);
    const auto &capacity = resources.predefined_resources[i];
    const auto &last_capacity = last_report_resources_->predefined_resources[i];
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available > 0) {
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
      if (!resources_data.resources_available_changed()) {
        resources_data.set_resources_available_changed(capacity.available !=
                                                       last_capacity.available);
      }
    }
    if (capacity.total > 0) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
      if (!is_resources_total_changed) {
        is_resources_total_changed = (capacity.total != last_capacity.total);
      }
    }
  }

  for (const auto &it : resources.custom_resources) {
    uint64_t custom_id = it.first;
    const auto &capacity = it.second;
    const auto &last_capacity = last_report_resources_->custom_resources[custom_id];
    const auto &label = string_to_int_map_.Get(custom_id);
    // Note: available may be negative, but only report positive to GCS.
    if (capacity.available > 0) {
      (*resources_data.mutable_resources_available())[label] =
          capacity.available.Double();
      if (!resources_data.resources_available_changed()) {
        resources_data.set_resources_available_changed(capacity.available !=
                                                       last_capacity.available);
      }
    }

    if (capacity.total > 0) {
      (*resources_data.mutable_resources_total())[label] = capacity.total.Double();
      if (!is_resources_total_changed) {
        is_resources_total_changed = (capacity.total != last_capacity.total);
      }
    }
  }
  if (!resources_data.resources_available_changed()) {
    resources_data.mutable_resources_available()->clear();
  }
  if (!is_resources_total_changed) {
    resources_data.mutable_resources_total()->clear();
  }

  if (resources != *last_report_resources_.get()) {
    last_report_resources_.reset(new NodeResources(resources));
  }
}

ray::gcs::NodeResourceInfoAccessor::ResourceMap
ClusterResourceScheduler::GetResourceTotals() const {
  ray::gcs::NodeResourceInfoAccessor::ResourceMap map;
  auto it = nodes_.find(local_node_id_);
  RAY_CHECK(it != nodes_.end());
  const auto &local_resources = it->second.GetLocalView();
  for (size_t i = 0; i < local_resources.predefined_resources.size(); i++) {
    std::string resource_name = ResourceEnumToString(static_cast<PredefinedResources>(i));
    double resource_total = local_resources.predefined_resources[i].total.Double();
    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }

  for (auto entry : local_resources.custom_resources) {
    std::string resource_name = string_to_int_map_.Get(entry.first);
    double resource_total = entry.second.total.Double();
    if (resource_total > 0) {
      auto data = std::make_shared<rpc::ResourceTableData>();
      data->set_resource_capacity(resource_total);
      map.emplace(resource_name, std::move(data));
    }
  }
  return map;
}
// === ANT-INTERNAL below ===

bool ClusterResourceScheduler::ContainsResource(const std::string &resource_name) const {
  int idx = -1;
  if (resource_name == ray::kCPU_ResourceLabel) {
    idx = (int)CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    idx = (int)GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    idx = (int)OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    idx = (int)MEM;
  };

  if (idx != -1) {
    return true;
  }

  int64_t resource_id = string_to_int_map_.Get(resource_name);
  const auto &node_resource = GetLocalNodeResources();
  auto it = node_resource.custom_resources.find(resource_id);
  return it != node_resource.custom_resources.end();
}

int64_t ClusterResourceScheduler::GetIndexFromResourceName(
    const std::string &resource_name) const {
  if (resource_name == ray::kCPU_ResourceLabel) {
    return CPU;
  } else if (resource_name == ray::kGPU_ResourceLabel) {
    return GPU;
  } else if (resource_name == ray::kObjectStoreMemory_ResourceLabel) {
    return OBJECT_STORE_MEM;
  } else if (resource_name == ray::kMemory_ResourceLabel) {
    return MEM;
  } else {
    return string_to_int_map_.Get(resource_name);
  }
}

bool ClusterResourceScheduler::IsLocallySchedulable(
    const std::unordered_map<std::string, double> &shape,
    const TaskSpecification &task_spec) {
  auto task_request = ResourceMapToTaskRequest(string_to_int_map_, shape);
  if (RayConfig::instance().gcs_task_scheduling_enabled() &&
      task_spec.IsActorCreationTask() && is_overcommit_enabled_fn_ &&
      is_overcommit_enabled_fn_()) {
    task_request.predefined_resources[CPU].soft = true;
    task_request.predefined_resources[MEM].soft = true;
    for (auto &resource_entry : task_request.custom_resources) {
      if (GetResourceNameFromIndex(resource_entry.id).find("CPU_group") == 0 ||
          GetResourceNameFromIndex(resource_entry.id).find("memory_group") == 0) {
        resource_entry.soft = true;
      }
    }
    RAY_LOG(DEBUG) << "Allow resource overcommit for task request: "
                   << task_request.DebugString(string_to_int_map_);
    return IsSchedulable(task_request, local_node_id_, GetLocalNodeResources(),
                         task_spec);
  }
  return IsSchedulable(task_request, local_node_id_, GetLocalNodeResources(), task_spec);
}

bool ClusterResourceScheduler::ContainsRareResources(const TaskRequest &task_req) const {
  if (task_req.predefined_resources.size() >=
      PredefinedResources::PredefinedResources_MAX) {
    if (task_req.predefined_resources[PredefinedResources::GPU].demand > 0) {
      return true;
    }
  }

  for (auto &entry : task_req.custom_resources) {
    if (IsRareResource(string_to_int_map_.Get(entry.id))) {
      return true;
    }
  }

  return false;
}

bool ClusterResourceScheduler::ContainsRareResources(
    const NodeResources &resources) const {
  if (resources.predefined_resources.size() >=
      PredefinedResources::PredefinedResources_MAX) {
    if (resources.predefined_resources[PredefinedResources::GPU].total > 0) {
      return true;
    }
  }

  for (auto &entry : resources.custom_resources) {
    if (IsRareResource(string_to_int_map_.Get(entry.first))) {
      return true;
    }
  }

  return false;
}

// Top level node filter
bool ClusterResourceScheduler::IsNodeUsable(
    int64_t node_id, const ScheduleContext &schedule_context) const {
  bool require_placement_group =
      !schedule_context.task_spec.PlacementGroupBundleId().first.IsNil();
  // Rare resource checking.
  if (RayConfig::instance().rare_resource_scheduling_enabled() &&
      !require_placement_group && !ContainsRareResources(schedule_context.task_request) &&
      ContainsRareResources(schedule_context.node_resources)) {
    return false;
  }

  // Additional filter function.
  return is_node_schedulable_fn_(string_to_int_map_.Get(node_id), schedule_context);
}

// === ANT-INTERNAL above ===

}  // namespace ray
