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

#include "ray/gcs/gcs_server/gcs_resource_manager.h"

#include "ray/common/ray_config.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

GcsResourceManager::GcsResourceManager(
    instrumented_io_context &main_io_service, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage, bool redis_broadcast_enabled)
    : periodical_runner_(main_io_service),
      gcs_pub_sub_(gcs_pub_sub),
      gcs_table_storage_(gcs_table_storage),
      redis_broadcast_enabled_(redis_broadcast_enabled),
      node_context_cache_(std::make_unique<NodeContextCache>()) {
  if (redis_broadcast_enabled_) {
    periodical_runner_.RunFnPeriodically(
        [this] { SendBatchedResourceUsage(); },
        RayConfig::instance().raylet_report_resources_period_milliseconds(),
        "GcsResourceManager.deadline_timer.send_batched_resource_usage");
  }
}

void GcsResourceManager::HandleGetResources(const rpc::GetResourcesRequest &request,
                                            rpc::GetResourcesReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    const auto &resource_map = iter->second->GetTotalResources().GetResourceMap();
    rpc::ResourceTableData resource_table_data;
    for (const auto &resource : resource_map) {
      resource_table_data.set_resource_capacity(resource.second);
      (*reply->mutable_resources())[resource.first] = resource_table_data;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleUpdateResources(
    const rpc::UpdateResourcesRequest &request, rpc::UpdateResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Updating resources, node id = " << node_id;
  auto changed_resources = std::make_shared<std::unordered_map<std::string, double>>();
  for (const auto &entry : request.resources()) {
    changed_resources->emplace(entry.first, entry.second.resource_capacity());
  }

  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    SchedulingResources &scheduling_resources = *iter->second;
    for (const auto &entry : *changed_resources) {
      scheduling_resources.UpdateResourceCapacity(entry.first, entry.second);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    for (const auto &entry : scheduling_resources.GetTotalResources().GetResourceMap()) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }
    for (const auto &entry : *changed_resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }

    node_context_cache_->OnNodeTotalResourceChanged(node_id, iter->second);

    for (const auto &listener : resources_changed_listeners_) {
      listener();
    }

    auto on_done = [this, node_id, changed_resources, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      // TODO (Alex): We need to move this into ResourceBatchData. It's currently a
      // message-reodering nightmare.
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      node_resource_change.mutable_updated_resources()->insert(changed_resources->begin(),
                                                               changed_resources->end());
      RAY_CHECK_OK(gcs_pub_sub_->Publish(
          NODE_RESOURCE_CHANNEL, node_id.Hex(), node_resource_change.SerializeAsString(),
          [node_id](const ray::Status &status) {
            RAY_CHECK_OK(status);
            RAY_LOG(INFO) << "Succeeded in publishing update node resources, node id = "
                          << node_id;
          }));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      RAY_LOG(INFO) << "Finished updating resources, node id = " << node_id;
    };

    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Node is not exist."));
    RAY_LOG(ERROR) << "Failed to update resources as node " << node_id
                   << " is not registered.";
  }
  ++counts_[CountType::UPDATE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleDeleteResources(
    const rpc::DeleteResourcesRequest &request, rpc::DeleteResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Deleting node resources, node id = " << node_id;
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    // Update `cluster_scheduling_resources_`.
    for (const auto &resource_name : resource_names) {
      iter->second->DeleteResource(resource_name);
    }

    // Update gcs storage.
    rpc::ResourceMap resource_map;
    auto resources = iter->second->GetTotalResources().GetResourceMap();
    for (const auto &resource_name : resource_names) {
      resources.erase(resource_name);
    }
    for (const auto &entry : resources) {
      (*resource_map.mutable_items())[entry.first].set_resource_capacity(entry.second);
    }

    auto on_done = [this, node_id, resource_names, reply,
                    send_reply_callback](const Status &status) {
      RAY_CHECK_OK(status);
      rpc::NodeResourceChange node_resource_change;
      node_resource_change.set_node_id(node_id.Binary());
      for (const auto &resource_name : resource_names) {
        node_resource_change.add_deleted_resources(resource_name);
      }
      RAY_CHECK_OK(gcs_pub_sub_->Publish(
          NODE_RESOURCE_CHANNEL, node_id.Hex(), node_resource_change.SerializeAsString(),
          [node_id](const Status &status) {
            RAY_CHECK_OK(status);
            RAY_LOG(INFO) << "Succeeded in publishing delete node resources, node id = "
                          << node_id;
          }));

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    };
    RAY_CHECK_OK(
        gcs_table_storage_->NodeResourceTable().Put(node_id, resource_map, on_done));
  } else {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(INFO) << "Finished deleting node resources, node id = " << node_id;
  }
  ++counts_[CountType::DELETE_RESOURCES_REQUEST];
}

void GcsResourceManager::HandleGetResourceUsage(
    const rpc::GetResourceUsageRequest &request, rpc::GetResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  auto iter = node_resource_usages_.find(node_id);
  if (iter != node_resource_usages_.end()) {
    reply->mutable_resources()->CopyFrom(iter->second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::UpdateNodeRealtimeResources(
    const NodeID &node_id, const rpc::ResourcesData &heartbeat) {
  if (GetClusterResources().count(node_id) == 0 ||
      heartbeat.resources_available_changed()) {
    RAY_LOG(DEBUG) << "Refresh gcs server resource view according to heartbeat.";
    SetAvailableResources(node_id,
                          ResourceSet(MapFromProtobuf(heartbeat.resources_available())));
  }
}

void GcsResourceManager::HandleGetAllAvailableResources(
    const rpc::GetAllAvailableResourcesRequest &request,
    rpc::GetAllAvailableResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &iter : node_resource_usages_) {
    rpc::AvailableResources resource;
    resource.set_node_id(iter.first.Binary());
    resource.mutable_resources_available()->insert(
        iter.second.resources_available().begin(),
        iter.second.resources_available().end());
    reply->add_resources_list()->CopyFrom(resource);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST];
}

void GcsResourceManager::UpdateFromResourceReport(const rpc::ResourcesData &data) {
  NodeID node_id = NodeID::FromBinary(data.node_id());
  auto resources_data = std::make_shared<rpc::ResourcesData>();
  resources_data->CopyFrom(data);

  UpdateNodeResourceUsage(node_id, data);

  // Update node realtime resources.
  UpdateNodeRealtimeResources(node_id, *resources_data);
  if (resources_data->should_global_gc() || resources_data->resources_total_size() > 0 ||
      resources_data->resources_available_changed() ||
      resources_data->resource_load_changed()) {
    absl::MutexLock guard(&resource_buffer_mutex_);
    resources_buffer_[node_id] = *resources_data;
    // Clear the fields that will not be used by raylet.
    resources_buffer_[node_id].clear_resource_load();
    resources_buffer_[node_id].clear_resource_load_by_shape();
    resources_buffer_[node_id].clear_resources_normal_task();
  }
}

void GcsResourceManager::HandleReportResourceUsage(
    const rpc::ReportResourceUsageRequest &request, rpc::ReportResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  UpdateFromResourceReport(request.resources());

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::HandleGetAllResourceUsage(
    const rpc::GetAllResourceUsageRequest &request, rpc::GetAllResourceUsageReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (!node_resource_usages_.empty()) {
    auto batch = std::make_shared<rpc::ResourceUsageBatchData>();
    absl::flat_hash_map<ResourceSet, rpc::ResourceDemand> aggregate_load;
    for (const auto &usage : node_resource_usages_) {
      // Aggregate the load reported by each raylet.
      auto load = usage.second.resource_load_by_shape();
      for (const auto &demand : load.resource_demands()) {
        auto scheduling_key = ResourceSet(MapFromProtobuf(demand.shape()));
        auto &aggregate_demand = aggregate_load[scheduling_key];
        aggregate_demand.set_num_ready_requests_queued(
            aggregate_demand.num_ready_requests_queued() +
            demand.num_ready_requests_queued());
        aggregate_demand.set_num_infeasible_requests_queued(
            aggregate_demand.num_infeasible_requests_queued() +
            demand.num_infeasible_requests_queued());
        if (RayConfig::instance().report_worker_backlog()) {
          aggregate_demand.set_backlog_size(aggregate_demand.backlog_size() +
                                            demand.backlog_size());
        }
      }

      batch->add_batch()->CopyFrom(usage.second);
    }

    for (const auto &demand : aggregate_load) {
      auto demand_proto = batch->mutable_resource_load_by_shape()->add_resource_demands();
      demand_proto->CopyFrom(demand.second);
      for (const auto &resource_pair : demand.first.GetResourceMap()) {
        (*demand_proto->mutable_shape())[resource_pair.first] = resource_pair.second;
      }
    }

    // Update placement group load to heartbeat batch.
    // This is updated only one per second.
    if (placement_group_load_.has_value()) {
      auto placement_group_load = placement_group_load_.value();
      auto placement_group_load_proto = batch->mutable_placement_group_load();
      placement_group_load_proto->CopyFrom(*placement_group_load.get());
    }
    reply->mutable_resource_usage_data()->CopyFrom(*batch);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST];
}

void GcsResourceManager::HandleGetResourcesOfAllNodegroups(
    const rpc::GetResourcesOfAllNodegroupsRequest &request,
    rpc::GetResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsResourceManager::HandleGetLayeredResourcesOfAllNodegroups(
    const rpc::GetLayeredResourcesOfAllNodegroupsRequest &request,
    rpc::GetLayeredResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsResourceManager::HandleGetPendingResourcesOfAllNodegroups(
    const rpc::GetPendingResourcesOfAllNodegroupsRequest &request,
    rpc::GetPendingResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsResourceManager::HandleGetClusterResources(
    const rpc::GetClusterResourcesRequest &request, rpc::GetClusterResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &entry : cluster_scheduling_resources_) {
    auto node_resources = reply->add_node_resources();
    node_resources->set_node_id(entry.first.Binary());

    auto total_resources = entry.second->GetTotalResources().GetResourceMap();
    for (auto &resource : total_resources) {
      if (resource.first.find(kMemory_ResourceLabel) == 0) {
        resource.second = FromMemoryUnitsToBytes(resource.second);
      }
    }
    node_resources->mutable_total_resources()->insert(total_resources.begin(),
                                                      total_resources.end());

    ResourceSet available_resource_set;
    entry.second->GetAvailableResources(&available_resource_set);
    auto available_resources = available_resource_set.GetResourceMap();
    for (auto &resource : available_resources) {
      if (resource.first.find(kMemory_ResourceLabel) == 0) {
        resource.second = FromMemoryUnitsToBytes(resource.second);
      }
    }
    node_resources->mutable_available_resources()->insert(available_resources.begin(),
                                                          available_resources.end());
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_CLUSTER_RESOURCES_REQUEST];
}

void GcsResourceManager::UpdateNodeResourceUsage(const NodeID &node_id,
                                                 const rpc::ResourcesData &resources) {
  auto iter = node_resource_usages_.find(node_id);
  if (iter == node_resource_usages_.end()) {
    auto resources_data = std::make_shared<rpc::ResourcesData>();
    resources_data->CopyFrom(resources);
    node_resource_usages_[node_id] = *resources_data;
  } else {
    if (resources.resources_total_size() > 0) {
      (*iter->second.mutable_resources_total()) = resources.resources_total();
    }
    if (resources.resources_available_changed()) {
      (*iter->second.mutable_resources_available()) = resources.resources_available();
    }
    if (resources.resource_load_changed()) {
      (*iter->second.mutable_resource_load()) = resources.resource_load();
    }
    if (resources.resources_normal_task_changed()) {
      (*iter->second.mutable_resources_normal_task()) = resources.resources_normal_task();
    }
    (*iter->second.mutable_resource_load_by_shape()) = resources.resource_load_by_shape();
  }
}

void GcsResourceManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &nodes = gcs_init_data.Nodes();
  for (const auto &entry : nodes) {
    if (entry.second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::ALIVE) {
      OnNodeAdd(entry.second);
    }
  }

  const auto &cluster_resources = gcs_init_data.ClusterResources();
  for (const auto &entry : cluster_resources) {
    const auto &iter = cluster_scheduling_resources_.find(entry.first);
    if (iter != cluster_scheduling_resources_.end()) {
      for (const auto &resource : entry.second.items()) {
        iter->second->UpdateResourceCapacity(resource.first,
                                             resource.second.resource_capacity());
      }
    }
  }
}

absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
    &GcsResourceManager::GetMutableClusterResources() {
  return cluster_scheduling_resources_;
}

const absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
    &GcsResourceManager::GetClusterResources() const {
  return cluster_scheduling_resources_;
}

const absl::flat_hash_map<NodeID, rpc::ResourcesData>
    &GcsResourceManager::GetNodeResources() {
  return node_resource_usages_;
}

void GcsResourceManager::SetAvailableResources(const NodeID &node_id,
                                               const ResourceSet &resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    iter->second->SetAvailableResources(ResourceSet(resources));
  } else {
    RAY_LOG(WARNING)
        << "Skip the setting of available resources of node " << node_id
        << " as it does not exist, maybe it is not registered yet or is already dead.";
  }
}

void GcsResourceManager::DeleteResources(
    const NodeID &node_id, const std::vector<std::string> &deleted_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    for (const auto &resource_name : deleted_resources) {
      iter->second->DeleteResource(resource_name);
    }
    node_context_cache_->OnNodeTotalResourceChanged(node_id, iter->second);
  }
}

void GcsResourceManager::OnNodeAdd(const rpc::GcsNodeInfo &node) {
  auto node_id = NodeID::FromBinary(node.basic_gcs_node_info().node_id());
  if (!cluster_scheduling_resources_.contains(node_id)) {
    std::unordered_map<std::string, double> resource_mapping;
    for (auto &entry : node.resources_total()) {
      resource_mapping.emplace(entry.first, entry.second);
    }
    // Update the cluster scheduling resources as new node is added.
    ResourceSet node_resources(resource_mapping);
    auto iter =
        cluster_scheduling_resources_
            .emplace(node_id, std::make_shared<SchedulingResources>(node_resources))
            .first;
    node_context_cache_->OnNodeAdded(node_id, iter->second,
                                     node.basic_gcs_node_info().node_name());
  }
}

void GcsResourceManager::OnNodeDead(const NodeID &node_id) {
  {
    absl::MutexLock guard(&resource_buffer_mutex_);
    resources_buffer_.erase(node_id);
  }
  node_resource_usages_.erase(node_id);
  cluster_scheduling_resources_.erase(node_id);
  node_context_cache_->OnNodeRemoved(node_id);
}

bool GcsResourceManager::AcquireResources(const NodeID &node_id,
                                          const ResourceSet &required_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    if (!required_resources.IsSubset(iter->second->GetAvailableResources())) {
      return false;
    }
    iter->second->Acquire(required_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

bool GcsResourceManager::ReleaseResources(const NodeID &node_id,
                                          const ResourceSet &acquired_resources) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    iter->second->Release(acquired_resources);
  }
  // If node dead, we will not find the node. This is a normal scenario, so it returns
  // true.
  return true;
}

void GcsResourceManager::GetResourceUsageBatchForBroadcast(
    rpc::ResourceUsageBatchData &buffer) {
  absl::MutexLock guard(&resource_buffer_mutex_);
  if (!resources_buffer_.empty()) {
    GetResourceUsageBatchForBroadcast_Locked(buffer);
    resources_buffer_.clear();
  }
}

void GcsResourceManager::GetResourceUsageBatchForBroadcast_Locked(
    rpc::ResourceUsageBatchData &buffer) {
  for (auto &resources : resources_buffer_) {
    buffer.add_batch()->Swap(&resources.second);
  }
}

void GcsResourceManager::SendBatchedResourceUsage() {
  absl::MutexLock guard(&resource_buffer_mutex_);
  if (!resources_buffer_.empty()) {
    auto batch = std::make_shared<rpc::ResourceUsageBatchData>();
    GetResourceUsageBatchForBroadcast_Locked(*batch);
    stats::OutboundHeartbeatSizeKB.Record((double)(batch->ByteSizeLong() / 1024.0));
    RAY_CHECK_OK(gcs_pub_sub_->Publish(RESOURCES_BATCH_CHANNEL, "",
                                       batch->SerializeAsString(), nullptr));
    resources_buffer_.clear();
  }
}

void GcsResourceManager::UpdatePlacementGroupLoad(
    const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load) {
  placement_group_load_ = absl::make_optional(placement_group_load);
}

std::string GcsResourceManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsResourceManager: {GetResources request count: "
         << counts_[CountType::GET_RESOURCES_REQUEST]
         << ", GetAllAvailableResources request count"
         << counts_[CountType::GET_ALL_AVAILABLE_RESOURCES_REQUEST]
         << ", UpdateResources request count: "
         << counts_[CountType::UPDATE_RESOURCES_REQUEST]
         << ", GetResourceUsage request count: "
         << counts_[CountType::GET_RESOURCE_USAGE_REQUEST]
         << ", ReportResourceUsage request count: "
         << counts_[CountType::REPORT_RESOURCE_USAGE_REQUEST]
         << ", GetAllResourceUsage request count: "
         << counts_[CountType::GET_ALL_RESOURCE_USAGE_REQUEST]
         << ", DeleteResources request count: "
         << counts_[CountType::DELETE_RESOURCES_REQUEST]
         << ", GetClusterResources request count: "
         << counts_[CountType::GET_CLUSTER_RESOURCES_REQUEST] << "}";
  return stream.str();
}

void GcsResourceManager::AddResourcesChangedListener(
    const std::function<void()> &listener) {
  RAY_CHECK(listener != nullptr);
  resources_changed_listeners_.emplace_back(listener);
}

void GcsResourceManager::NotifyResourcesChanged() {
  for (const auto &listener : resources_changed_listeners_) {
    listener();
  }
}

const std::vector<NodeContext> &GcsResourceManager::GetNodeContextList() const {
  return node_context_cache_->GetNodeContextList();
}

boost::optional<const NodeContext &> GcsResourceManager::GetNodeContext(
    const NodeID &node_id) const {
  return node_context_cache_->GetNodeContext(node_id);
}

std::vector<NodeContext> GcsResourceManager::GetContextsByNodeIDs(
    const absl::flat_hash_set<NodeID> &node_ids) {
  return node_context_cache_->GetContextsByNodeIDs(node_ids);
}

void GcsResourceManager::AddResourceDelta(const NodeID &node_id /* = NodeID::Nil()*/) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    return;
  }
  if (!node_id.IsNil()) {
    auto iter = cluster_scheduling_resources_.find(node_id);
    if (iter != cluster_scheduling_resources_.end()) {
      iter->second->AddResource(RESOURCE_DELTA);
    }
    return;
  }

  for (auto &node_entry : cluster_scheduling_resources_) {
    node_entry.second->AddResource(RESOURCE_DELTA);
  }
}

void GcsResourceManager::SubtractResourceDelta(
    const NodeID &node_id /* = NodeID::Nil()*/) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    return;
  }
  if (!node_id.IsNil()) {
    auto iter = cluster_scheduling_resources_.find(node_id);
    if (iter != cluster_scheduling_resources_.end()) {
      iter->second->SubtractResource(RESOURCE_DELTA);
    }
    return;
  }

  for (auto &node_entry : cluster_scheduling_resources_) {
    node_entry.second->SubtractResource(RESOURCE_DELTA);
  }
}

void GcsResourceManager::AddNodeTotalRequiredResources(const NodeID &node_id,
                                                       const ResourceSet &resource_set) {
  auto node_iter = cluster_scheduling_resources_.find(node_id);
  if (node_iter != cluster_scheduling_resources_.end()) {
    auto &node_total_required_resources =
        node_iter->second->GetMutableTotalRequiredResources();
    node_total_required_resources.AddResources(resource_set);
  }
}

void GcsResourceManager::SubtractNodeTotalRequiredResources(
    const NodeID &node_id, const ResourceSet &resource_set) {
  auto node_iter = cluster_scheduling_resources_.find(node_id);
  if (node_iter != cluster_scheduling_resources_.end()) {
    auto &node_total_required_resources =
        node_iter->second->GetMutableTotalRequiredResources();
    node_total_required_resources.SubtractResourcesStrict(resource_set);
  }
}

absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>>
GcsResourceManager::GetUnusedBundles(const NodeID &node_id) const {
  absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> idle_bundles;
  absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> non_idle_bundles;

  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter == cluster_scheduling_resources_.end()) {
    return idle_bundles;
  }
  const SchedulingResources &scheduling_res = *iter->second;
  const auto &total_res_map = scheduling_res.GetTotalResources().GetResourceAmountMap();
  const auto &avail_res_map =
      scheduling_res.GetAvailableResources().GetResourceAmountMap();

  // Find bundles that are ide, e.g:
  // [Not Idle] cpu_group_1_xxx : 10/10, gpu_group_1_xxx : 1/10
  // [Idle] cpu_group_2_xxx : 10/10, gpu_group_2_xxx : 10/10
  for (auto &res_pair : total_res_map) {
    const std::string &res_name = res_pair.first;
    PlacementGroupID pg_id;
    int64_t bundle_index;
    if (!ParseBundleResource(res_name, &pg_id, &bundle_index)) {
      continue;
    }
    // If avail==total, it means this bundle resource is idle
    if (avail_res_map.count(res_name) && avail_res_map.at(res_name) == res_pair.second) {
      // Save this bundle index
      idle_bundles[pg_id].emplace(bundle_index);
    } else {
      non_idle_bundles[pg_id].emplace(bundle_index);
    }
  }

  // remove all non-idle bundle indexes
  for (auto &pg_id_to_set : non_idle_bundles) {
    for (auto &index : pg_id_to_set.second) {
      idle_bundles[pg_id_to_set.first].erase(index);
    }
  }
  return idle_bundles;
}

bool GcsResourceManager::IsUnusedNode(const NodeID &node_id) const {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter == cluster_scheduling_resources_.end()) {
    return true;
  }

  const auto &node_resources = iter->second;
  if (!node_resources->GetNormalTaskResources().IsEmpty()) {
    return false;
  }

  const auto &total_resources = node_resources->GetTotalResources();
  const auto &available_resources = node_resources->GetAvailableResources();
  if (available_resources.GetResourceAmountMap().size() !=
      total_resources.GetResourceAmountMap().size()) {
    return false;
  }

  if (total_resources.ContainsPlacementGroup()) {
    return false;
  }

  return total_resources.IsEqual(available_resources);
}

}  // namespace gcs
}  // namespace ray
