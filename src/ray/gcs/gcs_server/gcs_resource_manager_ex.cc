#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"

#include "absl/strings/ascii.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/pb_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsResourceManagerEx::GcsResourceManagerEx(
    instrumented_io_context &main_io_service, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage, bool redis_broadcast_enabled,
    std::shared_ptr<GcsNodegroupManagerInterface> nodegroup_manager,
    std::function<const std::string &(const JobID &job_id)> get_nodegroup_id,
    std::function<
        absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>()>
        get_pending_resource_demands,
    std::function<bool(const NodeID &node_id)> has_any_drivers_in_node)
    : GcsResourceManager(main_io_service, gcs_pub_sub, gcs_table_storage,
                         redis_broadcast_enabled),
      nodegroup_manager_(std::move(nodegroup_manager)),
      get_nodegroup_id_(std::move(get_nodegroup_id)),
      get_pending_resource_demands_(std::move(get_pending_resource_demands)),
      has_any_drivers_in_node_(std::move(has_any_drivers_in_node)) {
  latest_resources_normal_task_timestamp_ = 0;
}

void GcsResourceManagerEx::HandleUpdateResources(
    const rpc::UpdateResourcesRequest &request, rpc::UpdateResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  std::unordered_map<std::string, double> changed_resources;
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter != cluster_scheduling_resources_.end()) {
    const auto &total_resources = iter->second->GetTotalResources();
    for (auto &entry : request.resources()) {
      if (!total_resources.Contains(entry.first)) {
        changed_resources.emplace(entry.first, entry.second.resource_capacity());
      }
    }

    if (changed_resources.empty()) {
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      RAY_LOG(DEBUG) << "Changed resources is empty, return directly, node id = "
                     << node_id;
      return;
    }
  }

  rpc::ResourceTableData resource_table_data;
  rpc::UpdateResourcesRequest new_request;
  new_request.set_node_id(request.node_id());
  for (const auto &resource : changed_resources) {
    resource_table_data.set_resource_capacity(resource.second);
    (*new_request.mutable_resources())[resource.first] = resource_table_data;
  }
  GcsResourceManager::HandleUpdateResources(new_request, reply, send_reply_callback);
}

void GcsResourceManagerEx::UpdateNodeRealtimeResources(
    const NodeID &node_id, const rpc::ResourcesData &heartbeat) {
  auto iter = cluster_scheduling_resources_.find(node_id);
  if (iter == cluster_scheduling_resources_.end()) {
    return;
  }

  auto &scheduling_resoruces = *iter->second;
  ResourceSet resources_normal_task(MapFromProtobuf(heartbeat.resources_normal_task()));
  if (heartbeat.resources_normal_task_changed() &&
      heartbeat.resources_normal_task_timestamp() >
          latest_resources_normal_task_timestamp_ &&
      !resources_normal_task.IsEqual(scheduling_resoruces.GetNormalTaskResources())) {
    scheduling_resoruces.SetNormalTaskResources(resources_normal_task);
    latest_resources_normal_task_timestamp_ = heartbeat.resources_normal_task_timestamp();
    for (const auto &listener : resources_changed_listeners_) {
      listener();
    }
  }
}

void GcsResourceManagerEx::HandleGetLayeredResourcesOfAllNodegroups(
    const rpc::GetLayeredResourcesOfAllNodegroupsRequest &request,
    rpc::GetLayeredResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = GetLayeredResourcesOfAllNodegroups(request, reply);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
}

Status GcsResourceManagerEx::GetLayeredResourcesOfAllNodegroups(
    const rpc::GetLayeredResourcesOfAllNodegroupsRequest &request,
    rpc::GetLayeredResourcesOfAllNodegroupsReply *reply) {
  auto &total_unassigned_hosts = *(reply->mutable_total_unassigned_hosts());
  std::unordered_map<std::string, rpc::NodegroupLayer> nodegroup_layer_map;

  auto &node_to_nodegroups = nodegroup_manager_->GetNodeToNodegroupsMap();

  nodegroup_manager_->ForEachNodeOfDefaultNodegroup(
      [this, &total_unassigned_hosts, &nodegroup_layer_map, &node_to_nodegroups](
          const std::string &nodegroup_id, const std::string &host_name,
          const NodeID &node_id, const std::string &shape_group) {
        std::unordered_map<std::string, double> total_resources;
        std::unordered_map<std::string, double> available_resources;

        auto iter = cluster_scheduling_resources_.find(node_id);
        if (iter != cluster_scheduling_resources_.end()) {
          ResourceSet available_resource_set;
          iter->second->GetAvailableResources(&available_resource_set);
          for (const auto &entry :
               iter->second->GetTotalResources().GetResourceAmountMap()) {
            // Only export standard, rare, or user-defined resources.
            if (kStandardResourceLabels.count(entry.first) > 0 ||
                IsRareResource(entry.first) ||
                (entry.first.find("_group_") == std::string::npos &&
                 entry.first != kObjectStoreMemory_ResourceLabel &&
                 !(entry.first.size() > 12 && StartsWith(entry.first, "node:")))) {
              total_resources[entry.first] += entry.second.ToDouble();
              auto resource_iter =
                  available_resource_set.GetResourceAmountMap().find(entry.first);
              if (resource_iter != available_resource_set.GetResourceAmountMap().end()) {
                available_resources[entry.first] += resource_iter->second.ToDouble();
              } else {
                available_resources[entry.first] += 0.0;
              }
            }
          }
        }

        rpc::LabelResourceMap *label_resource_map = nullptr;
        const auto nodegroups_iter = node_to_nodegroups.find(node_id);
        if (nodegroups_iter == node_to_nodegroups.end()) {
          return;
        }
        const auto &nodegroup_set = nodegroups_iter->second;
        if (nodegroup_set.size() == 1) {
          // An unassigned host in default nodegroup.
          label_resource_map =
              &(*(total_unassigned_hosts.mutable_host_resource_map()))[host_name];
        } else if (nodegroup_set.size() == 2) {
          // An unassigned host in user-defined nodegroup.
          for (const auto &nodegroup_id : nodegroup_set) {
            if (nodegroup_id != NODEGROUP_RESOURCE_DEFAULT) {
              auto &host_resource_map =
                  *(nodegroup_layer_map[nodegroup_id].mutable_unassigned_hosts());
              label_resource_map =
                  &(*(host_resource_map.mutable_host_resource_map()))[host_name];
              break;
            }
          }
        } else {  // An host in sub_nodegroup.
          std::string nodegroup_id;
          std::string sub_nodegroup_id;
          for (const auto &value : nodegroup_set) {
            if (nodegroup_manager_->IsSubnodegroupID(value)) {
              sub_nodegroup_id = value;
            } else if (value != NODEGROUP_RESOURCE_DEFAULT) {
              nodegroup_id = value;
            }
          }
          auto &host_resource_map =
              (*(nodegroup_layer_map[nodegroup_id]
                     .mutable_nodegroup_host_resource_map()))[sub_nodegroup_id];
          label_resource_map =
              &(*(host_resource_map.mutable_host_resource_map()))[host_name];
        }

        label_resource_map->set_shape_group(shape_group);
        label_resource_map->set_has_driver(has_any_drivers_in_node_(node_id));
        for (auto &resource : total_resources) {
          auto &resource_list =
              (*(label_resource_map->mutable_resource_map()))[resource.first];
          if (resource.first == kMemory_ResourceLabel) {
            resource_list.set_total(resource_list.total() +
                                    FromMemoryUnitsToBytes(resource.second));
            resource_list.set_available(
                resource_list.available() +
                FromMemoryUnitsToBytes(available_resources[resource.first]));
          } else {
            resource_list.set_total(resource_list.total() + resource.second);
            resource_list.set_available(resource_list.available() +
                                        available_resources[resource.first]);
          }
        }
      });

  for (auto &entry : nodegroup_layer_map) {
    auto nodegroup_data = nodegroup_manager_->GetNodegroupData(entry.first);
    if (nodegroup_data->enable_sub_nodegroup_isolation_) {
      entry.second.set_enable_sub_nodegroup_isolation(true);
    } else {
      entry.second.set_enable_sub_nodegroup_isolation(false);
    }
    if (nodegroup_data->enable_revision_) {
      entry.second.set_revision(nodegroup_data->revision_);
    }
  }
  reply->mutable_nodegroup_layer_map()->insert(nodegroup_layer_map.begin(),
                                               nodegroup_layer_map.end());
  return Status::OK();
}

void GcsResourceManagerEx::HandleGetPendingResourcesOfAllNodegroups(
    const rpc::GetPendingResourcesOfAllNodegroupsRequest &request,
    rpc::GetPendingResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = GetPendingResourcesOfAllNodegroups(request, reply);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
}

Status GcsResourceManagerEx::GetPendingResourcesOfAllNodegroups(
    const rpc::GetPendingResourcesOfAllNodegroupsRequest &request,
    rpc::GetPendingResourcesOfAllNodegroupsReply *reply) {
  absl::flat_hash_map<std::string, rpc::PendingResourceDemands> nodegroup_resources_map;
  if (get_nodegroup_id_ != nullptr && get_pending_resource_demands_ != nullptr) {
    auto pending_resource_demands = get_pending_resource_demands_();
    for (const auto &nodegroup_entry : pending_resource_demands) {
      auto &resources = nodegroup_resources_map[nodegroup_entry.first];
      for (const auto &demands_entry : nodegroup_entry.second) {
        if (demands_entry.first.ContainsPlacementGroup()) {
          continue;
        }
        auto resource_demand_proto =
            resources.mutable_resource_load_by_shape()->add_resource_demands();
        for (const auto &resource_entry : demands_entry.first.GetResourceAmountMap()) {
          auto amount = (resource_entry.first == "memory")
                            ? FromMemoryUnitsToBytes(resource_entry.second.ToDouble())
                            : resource_entry.second.ToDouble();
          (*resource_demand_proto->mutable_shape())[resource_entry.first] = amount;
        }
        resource_demand_proto->set_backlog_size(demands_entry.second);
      }
    }

    if (placement_group_load_.has_value()) {
      auto placement_group_load = placement_group_load_.value();
      for (const auto &placement_group_data :
           placement_group_load->placement_group_data()) {
        const auto &nodegroup_id =
            get_nodegroup_id_(JobID::FromBinary(placement_group_data.creator_job_id()));
        auto &resources = nodegroup_resources_map[nodegroup_id];

        auto placement_group_table_data_proto =
            resources.mutable_placement_group_load()->add_placement_group_data();
        placement_group_table_data_proto->CopyFrom(placement_group_data);
        for (auto &bundle : *placement_group_table_data_proto->mutable_bundles()) {
          auto &unit_resources = *bundle.mutable_unit_resources();
          for (auto &resource_entry : unit_resources) {
            if (resource_entry.first == "memory") {
              resource_entry.second = FromMemoryUnitsToBytes(resource_entry.second);
            }
          }
        }
      }
    }
  }

  reply->mutable_pending_resources()->insert(nodegroup_resources_map.begin(),
                                             nodegroup_resources_map.end());
  return Status::OK();
}

std::string GcsResourceManagerEx::ToString() const {
  std::ostringstream ostr;
  const int indent = 0;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');
  ostr << "{\n";
  for (const auto &entry : cluster_scheduling_resources_) {
    ostr << indent_1 << entry.first << " : " << entry.second->DebugString() << ",\n";
  }
  ostr << indent_0 << "}\n";
  return ostr.str();
}

}  // namespace gcs

}  // namespace ray