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

#include "ray/raylet/scheduling/old_cluster_resource_scheduler.h"

#include "ray/common/grpc_util.h"
#include "ray/common/ray_config.h"
#include "ray/util/resource_util.h"

namespace ray {
OldClusterResourceScheduler::OldClusterResourceScheduler(
    const NodeID &self_node_id, ResourceIdSet &local_available_resources,
    std::unordered_map<NodeID, SchedulingResources> &cluster_resource_map,
    std::shared_ptr<SchedulingResources> last_heartbeat_resources,
    std::function<int64_t(void)> get_used_object_store_memory)
    : self_node_id_string_(self_node_id.Binary()),
      local_available_resources_(local_available_resources),
      cluster_resource_map_(cluster_resource_map),
      last_heartbeat_resources_(last_heartbeat_resources),
      get_used_object_store_memory_(std::move(get_used_object_store_memory)) {}

bool OldClusterResourceScheduler::RemoveNode(const std::string &node_id_string) {
  return cluster_resource_map_.erase(NodeID::FromBinary(node_id_string)) != 0;
}

void OldClusterResourceScheduler::UpdateResourceCapacity(
    const std::string &node_id_string, const std::string &resource_name,
    double resource_total) {
  if (node_id_string == self_node_id_string_) {
    local_available_resources_.AddOrUpdateResource(resource_name, resource_total);
  }
  SchedulingResources &cluster_schedres =
      cluster_resource_map_[NodeID::FromBinary(node_id_string)];
  cluster_schedres.UpdateResourceCapacity(resource_name, resource_total);
}

void OldClusterResourceScheduler::DeleteResource(const std::string &node_id_string,
                                                 const std::string &resource_name) {
  if (node_id_string == self_node_id_string_) {
    local_available_resources_.DeleteResource(resource_name);
  }
  auto iter = cluster_resource_map_.find(NodeID::FromBinary(node_id_string));
  if (iter != cluster_resource_map_.end()) {
    auto &scheduling_resources = iter->second;
    scheduling_resources.DeleteResource(resource_name);
  }
}

void OldClusterResourceScheduler::FillResourceUsage(rpc::ResourcesData &resources_data,
                                                    bool need_whole_report) {
  // TODO(atumanov): modify the heartbeat table protocol to use the ResourceSet
  // directly.
  // TODO(atumanov): implement a ResourceSet const_iterator.
  // If light resource usage report enabled, we only set filed that represent resources
  // changed.
  auto local_resources = cluster_resource_map_[NodeID::FromBinary(self_node_id_string_)];
  // Automatically report object store usage.
  // XXX: this MUTATES the resources field, which is needed since we are storing
  // it in last_report_resources_.
  if (get_used_object_store_memory_ != nullptr) {
    const auto &total_resources = local_resources.GetTotalResources();
    auto total_quantity =
        total_resources.GetResource(ray::kObjectStoreMemory_ResourceLabel);
    double used = 1.0 * get_used_object_store_memory_() / MEMORY_RESOURCE_UNIT_BYTES;
    auto available = total_quantity - used;

    auto &mutable_available_resources =
        const_cast<ResourceSet &>(local_resources.GetAvailableResources());
    mutable_available_resources.AddOrUpdateResource(ray::kObjectStoreMemory_ResourceLabel,
                                                    available);
  }

  if (need_whole_report || !last_heartbeat_resources_->GetTotalResources().IsEqual(
                               local_resources.GetTotalResources())) {
    for (const auto &resource_pair :
         local_resources.GetTotalResources().GetResourceMap()) {
      (*resources_data.mutable_resources_total())[resource_pair.first] =
          resource_pair.second;
    }
  }

  if (need_whole_report || !last_heartbeat_resources_->GetAvailableResources().IsEqual(
                               local_resources.GetAvailableResources())) {
    resources_data.set_resources_available_changed(true);
    for (const auto &resource_pair :
         local_resources.GetAvailableResources().GetResourceMap()) {
      (*resources_data.mutable_resources_available())[resource_pair.first] =
          resource_pair.second;
    }
  }
}

bool OldClusterResourceScheduler::UpdateNode(const std::string &node_id_string,
                                             const rpc::ResourcesData &resource_data) {
  NodeID node_id = NodeID::FromBinary(node_id_string);
  auto iter = cluster_resource_map_.find(node_id);
  if (iter == cluster_resource_map_.end()) {
    return false;
  }

  SchedulingResources &remote_resources = iter->second;
  if (resource_data.resources_total_size() > 0) {
    ResourceSet remote_total(MapFromProtobuf(resource_data.resources_total()));
    remote_resources.SetTotalResources(std::move(remote_total));
  }
  if (resource_data.resources_available_changed()) {
    ResourceSet remote_available(MapFromProtobuf(resource_data.resources_available()));
    remote_resources.SetAvailableResources(std::move(remote_available));
  }
  if (resource_data.resource_load_changed()) {
    ResourceSet remote_load(MapFromProtobuf(resource_data.resource_load()));
    // Extract the load information and save it locally.
    remote_resources.SetLoadResources(std::move(remote_load));
  }
  return true;
}

std::string OldClusterResourceScheduler::GetLocalResourceViewString() const {
  SchedulingResources &local_resources =
      cluster_resource_map_[NodeID::FromBinary(self_node_id_string_)];
  return local_resources.GetAvailableResources().ToString();
}

// === ANT-INTERNAL below ===

bool OldClusterResourceScheduler::ContainsResource(
    const std::string &resource_name) const {
  SchedulingResources &local_resources =
      cluster_resource_map_[NodeID::FromBinary(self_node_id_string_)];
  return local_resources.GetTotalResources().Contains(resource_name);
}

// === ANT-INTERNAL above ===

}  // namespace ray
