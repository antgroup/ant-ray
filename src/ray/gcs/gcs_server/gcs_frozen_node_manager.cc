#include "ray/gcs/gcs_server/gcs_frozen_node_manager.h"

namespace ray {
namespace gcs {
void GcsFrozenNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  RAY_LOG(INFO) << "Initializing GcsFrozenNodeManager, frozen_node_set size="
                << gcs_init_data.FrozenNodes().size();
  frozen_node_set_ = gcs_init_data.FrozenNodes();
}

void GcsFrozenNodeManager::HandleFreezeNodes(const rpc::FreezeNodesRequest &request,
                                             rpc::FreezeNodesReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  std::unordered_set<std::string> node_name_set(request.node_name_set().begin(),
                                                request.node_name_set().end());
  std::string node_name_set_str = StrJoin(node_name_set.begin(), node_name_set.end(),
                                          [](auto &node) { return *node; }, ",");
  RAY_LOG(INFO) << "Freezing nodes, host set size=" << node_name_set.size()
                << ", node name list=[" << node_name_set_str << "]";

  bool old_size = frozen_node_set_.size();
  for (auto &node_name : node_name_set) {
    frozen_node_set_.emplace(node_name);
  }
  if (frozen_node_set_.size() == old_size) {
    RAY_LOG(INFO) << "All these nodes have already been added.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  // Flush before reply to ensure request won't be lost
  FlushAndPublishFrozenNodeSet([send_reply_callback, reply]() {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
}

void GcsFrozenNodeManager::HandleUnfreezeNodes(
    const rpc::UnfreezeNodesRequest &request, rpc::UnfreezeNodesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::unordered_set<std::string> node_name_set(request.node_name_set().begin(),
                                                request.node_name_set().end());
  std::string node_name_set_str = StrJoin(node_name_set.begin(), node_name_set.end(),
                                          [](auto &node) { return *node; }, ",");
  RAY_LOG(INFO) << "Unfreezing nodes, host set size=" << node_name_set.size()
                << ", node name list=[" << node_name_set_str << "]";

  std::unordered_set<std::string> unfreezed_nodes;
  for (auto &node_name : node_name_set) {
    if (frozen_node_set_.erase(node_name)) {
      unfreezed_nodes.emplace(node_name);
    }
  }
  if (unfreezed_nodes.empty()) {
    RAY_LOG(INFO) << "All these nodes are not in frozen node set.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  unfreeze_done_listener_(node_name_set);
  // Flush before reply to ensure request won't be lost
  FlushAndPublishFrozenNodeSet([send_reply_callback, reply]() {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  });
}

void GcsFrozenNodeManager::HandleReportFreezeDone(
    const rpc::ReportFreezeDoneRequest &request, rpc::ReportFreezeDoneReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  freeze_done_listener_(request.node_name(), NodeID::FromBinary(request.node_id()));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsFrozenNodeManager::FlushAndPublishFrozenNodeSet(
    EmptyCallback flush_done_callback) {
  // 1. flush node name set to storage
  // 2. publish frozen nodes to raylet
  RAY_LOG(INFO) << "Flushing frozen node data and publishing to raylet.";
  rpc::FrozenNodesNotification notification;
  for (const std::string &node_name : frozen_node_set_) {
    notification.add_frozen_node_set(node_name);
  }
  auto on_flush_done = [this, notification, flush_done_callback](const Status &status) {
    if (flush_done_callback) {
      flush_done_callback();
    }
    RAY_CHECK_OK(gcs_pub_sub_->Publish(FROZEN_HOST_CHANNEL,
                                       CONST_KEY_FROZEN_TABLE.Binary(),
                                       notification.SerializeAsString(), nullptr));
  };
  RAY_CHECK_OK(gcs_table_storage_->FrozenNodeTable().Put(CONST_KEY_FROZEN_TABLE,
                                                         notification, on_flush_done));
}

void GcsFrozenNodeManager::HandleGetAllFrozenNodes(
    const rpc::GetAllFrozenNodesRequest &request, rpc::GetAllFrozenNodesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (auto &it : frozen_node_set_) {
    reply->add_frozen_node_set(it);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsFrozenNodeManager::FreezeNode(const std::string &node_name) {
  RAY_LOG(INFO) << "Freezing node " << node_name;
  frozen_node_set_.emplace(node_name);
  FlushAndPublishFrozenNodeSet(nullptr);
}

bool GcsFrozenNodeManager::IsNodeFrozen(const std::string &node_name) {
  return frozen_node_set_.count(node_name);
}

bool GcsFrozenNodeManager::IsNodeFrozen(const NodeID &node_id) {
  auto optional_node = gcs_node_manager_->GetAliveNode(node_id);
  return optional_node &&
         IsNodeFrozen(optional_node.value()->basic_gcs_node_info().node_name());
}

// Check whether there are idle PG bundles, and reschedule them to other nodes.
void GcsFrozenNodeManager::PeriodicallyRescheduleIdlePlacementGroupBundles() {
  // In raylet scheduling, rescheduling PG is not supported.
  if (!gcs_placement_group_manager_) {
    return;
  }
  RAY_LOG(DEBUG) << "Rescheduling idle placement group bundles, finding idle bundles.";

  absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> unused_bundles;

  for (std::string node_name : frozen_node_set_) {
    if (auto optional_node = gcs_node_manager_->GetUniqueNodeByName(node_name)) {
      NodeID node_id =
          NodeID::FromBinary(optional_node.value()->basic_gcs_node_info().node_id());
      unused_bundles.merge(gcs_resource_manager_->GetUnusedBundles(node_id));
    }
  }

  // Rescheche bundles whose resources are all idle
  RAY_LOG(DEBUG) << "Rescheduling idle placement group bundles, idle PG num="
                 << unused_bundles.size();
  gcs_placement_group_manager_->RescheduleBundle(unused_bundles);
}

}  // namespace gcs
}  // namespace ray
