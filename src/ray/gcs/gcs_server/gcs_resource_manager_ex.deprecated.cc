#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"

#include "absl/strings/ascii.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/pb_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

void GcsResourceManagerEx::HandleGetResourcesOfAllNodegroups(
    const rpc::GetResourcesOfAllNodegroupsRequest &request,
    rpc::GetResourcesOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = GetResourcesOfAllNodegroups(request, reply);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
}

Status GcsResourceManagerEx::GetResourcesOfAllNodegroups(
    const rpc::GetResourcesOfAllNodegroupsRequest &request,
    rpc::GetResourcesOfAllNodegroupsReply *reply) {
  std::unordered_map<std::string, rpc::HostResourceMap> nodegroup_host_resource_map;
  nodegroup_manager_->ForEachRegisteredNodegroupNode([this, &nodegroup_host_resource_map](
                                                         const std::string &nodegroup_id,
                                                         const std::string &host_name,
                                                         const NodeID &node_id,
                                                         const std::string &shape_group) {
    auto &host_resource_map = nodegroup_host_resource_map[nodegroup_id];
    auto &label_resource_map =
        (*(host_resource_map.mutable_host_resource_map()))[host_name];

    std::unordered_map<std::string, double> total_resources;
    std::unordered_map<std::string, double> available_resources;

    auto iter = cluster_scheduling_resources_.find(node_id);
    if (iter != cluster_scheduling_resources_.end()) {
      ResourceSet available_resource_set;
      iter->second->GetAvailableResources(&available_resource_set);
      for (const auto &entry : iter->second->GetTotalResources().GetResourceAmountMap()) {
        if (kStandardResourceLabels.count(entry.first) == 0 &&
            !IsRareResource(entry.first)) {
          continue;
        }
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

    for (auto &resource : total_resources) {
      auto &resource_list =
          (*(label_resource_map.mutable_resource_map()))[resource.first];
      if (resource.first == kMemory_ResourceLabel) {
        resource_list.set_total(FromMemoryUnitsToBytes(resource.second));
        resource_list.set_available(
            FromMemoryUnitsToBytes(available_resources[resource.first]));
      } else {
        resource_list.set_total(resource.second);
        resource_list.set_available(available_resources[resource.first]);
      }
    }
  });
  reply->mutable_nodegroup_host_resource_map()->insert(
      nodegroup_host_resource_map.begin(), nodegroup_host_resource_map.end());
  return Status::OK();
}

}  // namespace gcs

}  // namespace ray