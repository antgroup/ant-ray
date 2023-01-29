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

#include "ray/gcs/gcs_server/gcs_node_manager.h"

#include <list>

#include "absl/strings/ascii.h"
#include "nlohmann/json.hpp"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

using nlohmann::json;
//////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                               std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                               std::function<bool(const NodeID &, const std::string &)>
                                   is_node_in_nodegroup_callback,
                               std::function<void(const NodeID &)> reject_node_fn)
    : gcs_pub_sub_(gcs_pub_sub),
      gcs_table_storage_(gcs_table_storage),
      is_node_in_nodegroup_callback_(std::move(is_node_in_nodegroup_callback)),
      reject_node_fn_(std::move(reject_node_fn)) {}

void GcsNodeManager::HandleRegisterNode(const rpc::RegisterNodeRequest &request,
                                        rpc::RegisterNodeReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id =
      NodeID::FromBinary(request.node_info().basic_gcs_node_info().node_id());
  if (failure_machines_.contains(request.node_info().machine_id())) {
    RAY_LOG(WARNING) << "Rejecting the registration of node " << node_id
                     << ", because physical machine " << request.node_info().machine_id()
                     << " has been marked failure";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                       Status::Invalid("The physical machine has been marked failure"));
  }
  RAY_LOG(INFO) << "Registering node info, node id = " << node_id << ", address = "
                << request.node_info().basic_gcs_node_info().node_manager_address()
                << ", node name = "
                << request.node_info().basic_gcs_node_info().node_name();
  auto on_done = [this, node_id, request, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    RAY_LOG(INFO) << "Finished registering node info, node id = " << node_id
                  << ", address = "
                  << request.node_info().basic_gcs_node_info().node_manager_address();
    RAY_CHECK_OK(gcs_pub_sub_->Publish(FULL_NODE_INFO_CHANNEL, node_id.Hex(),
                                       request.node_info().SerializeAsString(), nullptr));
    RAY_CHECK_OK(gcs_pub_sub_->Publish(
        BASIC_NODE_INFO_CHANNEL, node_id.Hex(),
        request.node_info().basic_gcs_node_info().SerializeAsString(), nullptr));
    AddNode(std::make_shared<rpc::GcsNodeInfo>(request.node_info()));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->NodeTable().Put(node_id, request.node_info(), on_done));
  ++counts_[CountType::REGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleUnregisterNode(const rpc::UnregisterNodeRequest &request,
                                          rpc::UnregisterNodeReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  NodeID node_id = NodeID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Unregistering node info, node id = " << node_id;
  if (auto node = RemoveNode(node_id, /* is_intended = */ true)) {
    node->set_terminate_time(current_sys_time_seconds());
    node->mutable_basic_gcs_node_info()->set_state(rpc::BasicGcsNodeInfo::DEAD);
    node->mutable_basic_gcs_node_info()->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);
    auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
    node_info_delta->mutable_basic_gcs_node_info()->set_node_id(
        node->basic_gcs_node_info().node_id());
    node_info_delta->mutable_basic_gcs_node_info()->set_state(
        node->basic_gcs_node_info().state());
    node_info_delta->mutable_basic_gcs_node_info()->set_timestamp(
        node->basic_gcs_node_info().timestamp());

    auto on_done = [this, node_id, node_info_delta, reply,
                    send_reply_callback](const Status &status) {
      auto on_done = [this, node_id, node_info_delta, reply,
                      send_reply_callback](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(FULL_NODE_INFO_CHANNEL, node_id.Hex(),
                                           node_info_delta->SerializeAsString(),
                                           nullptr));
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            BASIC_NODE_INFO_CHANNEL, node_id.Hex(),
            node_info_delta->basic_gcs_node_info().SerializeAsString(), nullptr));
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        RAY_LOG(INFO) << "Finished unregistering node info, node id = " << node_id;
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    // Update node state to DEAD instead of deleting it.
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
  ++counts_[CountType::UNREGISTER_NODE_REQUEST];
}

void GcsNodeManager::HandleGetAllBasicNodeInfo(
    const rpc::GetAllBasicNodeInfoRequest &request, rpc::GetAllBasicNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all node info.";
  const auto &nodegroup_id = request.nodegroup_id();
  auto nodegroup_nodes = GetAllNodes(nodegroup_id);
  for (const auto &entry : nodegroup_nodes) {
    reply->mutable_node_info_list()->UnsafeArenaAddAllocated(
        entry.second->mutable_basic_gcs_node_info());
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting all node info, size = " << nodegroup_nodes.size();
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

void GcsNodeManager::HandleGetAllFullNodeInfo(
    const rpc::GetAllFullNodeInfoRequest &request, rpc::GetAllFullNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all node info.";
  const auto &nodegroup_id = request.nodegroup_id();
  auto nodegroup_nodes = GetAllNodes(nodegroup_id);
  for (const auto &entry : nodegroup_nodes) {
    reply->mutable_node_info_list()->UnsafeArenaAddAllocated(entry.second.get());
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting all node info, size = " << nodegroup_nodes.size();
  ++counts_[CountType::GET_ALL_NODE_INFO_REQUEST];
}

void GcsNodeManager::HandleGetInternalConfig(const rpc::GetInternalConfigRequest &request,
                                             rpc::GetInternalConfigReply *reply,
                                             rpc::SendReplyCallback send_reply_callback) {
  auto get_system_config = [reply, send_reply_callback](
                               const ray::Status &status,
                               const boost::optional<rpc::StoredConfig> &config) {
    if (config.has_value()) {
      reply->set_config(config.get().config());
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(
      gcs_table_storage_->InternalConfigTable().Get(UniqueID::Nil(), get_system_config));
  ++counts_[CountType::GET_INTERNAL_CONFIG_REQUEST];
}

void GcsNodeManager::HandleUpdateInternalConfig(
    const rpc::UpdateInternalConfigRequest &request,
    rpc::UpdateInternalConfigReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "UpdateInternalConfig, config_json_str=" << request.config_json_str();
  std::string error_message;
  if (!RayConfig::instance().validate(request.config_json_str(), &error_message)) {
    RAY_LOG(ERROR) << "Invalid config, error_message: " << error_message;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(error_message));
    return;
  }
  RAY_LOG(INFO) << "Flushing internal config";

  auto put_and_apply = [this, send_reply_callback,
                        reply](const std::string &merged_config_str) {
    // 3. Flush new config to storage
    ray::rpc::StoredConfig config;
    config.set_config(merged_config_str);
    RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Put(
        ray::UniqueID::Nil(), config,
        [this, merged_config_str, send_reply_callback, reply](const ray::Status &status) {
          if (!status.ok()) {
            std::string error = "Put config to storage failed!";
            RAY_LOG(ERROR) << error;
            GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(error));
            return;
          }
          // 4. Apply config
          RAY_LOG(INFO) << "Put config done, applying config!";
          RayConfig::instance().initialize(merged_config_str);
          // 5. Publish
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              INTERNAL_CONFIG_CHANNEL, {}, merged_config_str, [](const Status &status) {
                RAY_LOG(INFO) << "Publish done! status=" << status;
              }));
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        }));
  };

  auto on_get_done = [request, send_reply_callback, reply, put_and_apply](
                         const ray::Status &status,
                         const boost::optional<rpc::StoredConfig> &config) {
    std::string merged_config_str;
    try {
      // 2. Merge config
      json merged = json::parse(request.config_json_str());
      if (config.has_value()) {
        json original = json::parse(config.get().config());
        merged.merge_patch(original);
      }
      merged_config_str = merged.dump();
    } catch (json::exception &ex) {
      RAY_LOG(ERROR) << "Error when merging configs: " << ex.what();
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(ex.what()));
      return;
    }
    put_and_apply(merged_config_str);
  };
  // 1. Get original config from storage
  RAY_CHECK_OK(
      gcs_table_storage_->InternalConfigTable().Get(UniqueID::Nil(), on_get_done));
}

void GcsNodeManager::HandleMarkFailureMachines(
    const rpc::MarkFailureMachinesRequest &request, rpc::MarkFailureMachinesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  absl::flat_hash_set<std::string> current_failure_machines;
  for (const auto &machine_id : request.machine_list()) {
    RAY_LOG(INFO) << "Removing machine, machine_id = " << machine_id;
    current_failure_machines.emplace(machine_id);
    if (failure_machines_.contains(machine_id)) {
      continue;
    }
    for (const auto &node_entry : alive_nodes_) {
      if (node_entry.second->machine_id() == machine_id) {
        RAY_LOG(INFO) << "Rejecting node, node_id = " << node_entry.first;
        reject_node_fn_(node_entry.first);
      }
    }
  }
  failure_machines_ = std::move(current_failure_machines);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GcsNodeManager::GetAliveNode(
    const ray::NodeID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return {};
  }

  return iter->second;
}

void GcsNodeManager::AddNode(std::shared_ptr<rpc::GcsNodeInfo> node) {
  auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    alive_nodes_.emplace(node_id, node);

    // Notify all listeners.
    for (auto &listener : node_added_listeners_) {
      listener(node);
    }
  }
}

std::shared_ptr<rpc::GcsNodeInfo> GcsNodeManager::RemoveNode(
    const ray::NodeID &node_id, bool is_intended /*= false*/) {
  RAY_LOG(INFO) << "Removing node, node id = " << node_id;
  std::shared_ptr<rpc::GcsNodeInfo> removed_node;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    removed_node = std::move(iter->second);
    // Record stats that there's a new removed node.
    stats::NodeFailureTotal.Record(1);
    // Remove from alive nodes.
    alive_nodes_.erase(iter);
    if (!is_intended) {
      // Broadcast a warning to all of the drivers indicating that the node
      // has been marked as dead.
      // TODO(rkn): Define this constant somewhere else.
      std::string type = "node_removed";
      std::ostringstream error_message;
      error_message << "The node with node id: " << node_id << " and ip: "
                    << removed_node->basic_gcs_node_info().node_manager_address()
                    << " has been marked dead. This can happen when a "
                       "raylet crashes unexpectedly, or has lagging heartbeats, or a "
                       "raylet with the same node name is registering.";
      auto error_data_ptr =
          gcs::CreateErrorTableData(type, error_message.str(), current_time_ms());
      RAY_LOG(INFO) << "Publish RemoveNode, msg=" << error_message.str();
      RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, node_id.Hex(),
                                         error_data_ptr->SerializeAsString(), nullptr));
    }

    // Notify all listeners.
    for (auto &listener : node_removed_listeners_) {
      listener(removed_node);
    }
  }
  return removed_node;
}

void GcsNodeManager::OnNodeFailure(const NodeID &node_id) {
  if (auto node = RemoveNode(node_id, /* is_intended = */ false)) {
    node->mutable_basic_gcs_node_info()->set_state(rpc::BasicGcsNodeInfo::DEAD);
    node->mutable_basic_gcs_node_info()->set_timestamp(current_sys_time_ms());
    AddDeadNodeToCache(node);
    auto node_info_delta = std::make_shared<rpc::GcsNodeInfo>();
    node_info_delta->mutable_basic_gcs_node_info()->set_node_id(
        node->basic_gcs_node_info().node_id());
    node_info_delta->mutable_basic_gcs_node_info()->set_state(
        node->basic_gcs_node_info().state());
    node_info_delta->mutable_basic_gcs_node_info()->set_timestamp(
        node->basic_gcs_node_info().timestamp());

    auto on_done = [this, node_id, node_info_delta](const Status &status) {
      auto on_done = [this, node_id, node_info_delta](const Status &status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(FULL_NODE_INFO_CHANNEL, node_id.Hex(),
                                           node_info_delta->SerializeAsString(),
                                           nullptr));
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            BASIC_NODE_INFO_CHANNEL, node_id.Hex(),
            node_info_delta->basic_gcs_node_info().SerializeAsString(), nullptr));
      };
      RAY_CHECK_OK(gcs_table_storage_->NodeResourceTable().Delete(node_id, on_done));
    };
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Put(node_id, *node, on_done));
  }
}

void GcsNodeManager::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &item : gcs_init_data.Nodes()) {
    if (item.second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::ALIVE) {
      AddNode(std::make_shared<rpc::GcsNodeInfo>(item.second));
    } else if (item.second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::DEAD) {
      dead_nodes_.emplace(item.first, std::make_shared<rpc::GcsNodeInfo>(item.second));
      sorted_dead_node_list_.emplace_back(item.first,
                                          item.second.basic_gcs_node_info().timestamp());
    }
  }
  sorted_dead_node_list_.sort(
      [](const std::pair<NodeID, int64_t> &left,
         const std::pair<NodeID, int64_t> &right) { return left.second < right.second; });
}

absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>>
GcsNodeManager::GetAllNodes(const std::string &nodegroup_id /* = ""*/) const {
  absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> nodegroup_nodes;
  for (const auto &entry : alive_nodes_) {
    if (is_node_in_nodegroup_callback_(entry.first, nodegroup_id)) {
      nodegroup_nodes.emplace(entry);
    }
  }
  for (const auto &entry : dead_nodes_) {
    if (is_node_in_nodegroup_callback_(entry.first, nodegroup_id)) {
      nodegroup_nodes.emplace(entry);
    }
  }
  return nodegroup_nodes;
}

void GcsNodeManager::AddDeadNodeToCache(std::shared_ptr<rpc::GcsNodeInfo> node) {
  if (dead_nodes_.size() >= RayConfig::instance().maximum_gcs_dead_node_cached_count()) {
    EvictOneDeadNode();
  }
  auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  dead_nodes_.emplace(node_id, node);
  sorted_dead_node_list_.emplace_back(node_id, node->basic_gcs_node_info().timestamp());
}

std::string GcsNodeManager::DebugString() const {
  std::ostringstream stream;
  stream << "GcsNodeManager: {RegisterNode request count: "
         << counts_[CountType::REGISTER_NODE_REQUEST]
         << ", UnregisterNode request count: "
         << counts_[CountType::UNREGISTER_NODE_REQUEST]
         << ", GetAllBasicNodeInfo request count: "
         << counts_[CountType::GET_ALL_NODE_INFO_REQUEST]
         << ", GetInternalConfig request count: "
         << counts_[CountType::GET_INTERNAL_CONFIG_REQUEST] << "}";
  return stream.str();
}

std::vector<std::shared_ptr<rpc::GcsNodeInfo>> GcsNodeManager::GetAliveNodesByName(
    const std::string &node_name) const {
  // (Note) This find opeartion can be optimized to O(1) using an additional map. But
  // it's not necessary now because current max node number is less than 10000.
  std::vector<std::shared_ptr<rpc::GcsNodeInfo>> nodes;
  for (auto &entry : alive_nodes_) {
    if (entry.second->basic_gcs_node_info().node_name() == node_name) {
      nodes.emplace_back(entry.second);
    }
  }
  return nodes;
}

boost::optional<std::shared_ptr<rpc::GcsNodeInfo>> GcsNodeManager::GetUniqueNodeByName(
    const std::string &node_name) const {
  auto node_list = GetAliveNodesByName(node_name);
  // Node should be unique.
  RAY_CHECK(node_list.size() <= 1);
  if (node_list.size() == 0) {
    return {};
  }
  return node_list.front();
}

void GcsNodeManager::QuickDetectNodeFailureByName(const std::string &node_name) {
  if (node_name.empty()) {
    return;
  }

  auto nodes = GetAliveNodesByName(node_name);
  if (nodes.size() <= 1) {
    return;
  }

  auto max_iter = std::max_element(nodes.begin(), nodes.end(),
                                   [](const std::shared_ptr<rpc::GcsNodeInfo> &node1,
                                      const std::shared_ptr<rpc::GcsNodeInfo> &node2) {
                                     return node1->start_time() < node2->start_time();
                                   });
  auto max_node = *max_iter;

  for (const auto &node : nodes) {
    if (node != max_node) {
      auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
      RAY_LOG(INFO) << "Marking node " << node_id
                    << " (start_time: " << node->start_time()
                    << ") as dead as a new node "
                    << NodeID::FromBinary(max_node->basic_gcs_node_info().node_id())
                    << " (start_time: " << max_node->start_time()
                    << ") with the same node name "
                    << max_node->basic_gcs_node_info().node_name() << " is registered.";
      OnNodeFailure(node_id);
    }
  }
}

void GcsNodeManager::QuickDetectNodeFailure() {
  absl::flat_hash_map<std::string, std::list<std::shared_ptr<rpc::GcsNodeInfo>>>
      name_to_alive_nodes;
  for (auto &entry : alive_nodes_) {
    const std::string &node_name = entry.second->basic_gcs_node_info().node_name();
    if (!node_name.empty()) {
      auto &named_alive_nodes = name_to_alive_nodes[node_name];
      if (named_alive_nodes.empty()) {
        named_alive_nodes.emplace_back(entry.second);
      } else {
        auto back = named_alive_nodes.back();
        if (entry.second->start_time() > back->start_time()) {
          named_alive_nodes.emplace_back(entry.second);
        } else {
          named_alive_nodes.emplace_front(entry.second);
        }
      }
    }
  }
  for (auto &entry : name_to_alive_nodes) {
    // Pop the node which with the largest `start_time`.
    auto back = entry.second.back();
    entry.second.pop_back();
    for (auto &node : entry.second) {
      auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
      RAY_LOG(INFO) << "Marking node " << node_id
                    << " (start_time: " << node->start_time()
                    << ") as dead as a new node "
                    << NodeID::FromBinary(back->basic_gcs_node_info().node_id())
                    << " (start_time: " << back->start_time()
                    << ") with the same node name "
                    << back->basic_gcs_node_info().node_name() << " is registered.";
      OnNodeFailure(node_id);
    }
  }
}

void GcsNodeManager::EvictExpiredNodes() {
  RAY_LOG(INFO) << "Try evicting expired nodes, there are "
                << sorted_dead_node_list_.size() << " dead nodes in the cache.";
  int evicted_node_number = 0;

  std::vector<NodeID> batch_ids;
  size_t batch_size = RayConfig::instance().gcs_dead_data_max_batch_delete_size();
  batch_ids.reserve(batch_size);

  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_node_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_node_data_keep_duration_ms();
  while (!sorted_dead_node_list_.empty()) {
    auto timestamp = sorted_dead_node_list_.begin()->second;
    if (timestamp + gcs_dead_node_data_keep_duration_ms > current_time_ms) {
      break;
    }

    auto iter = sorted_dead_node_list_.begin();
    const auto &worker_id = iter->first;
    batch_ids.emplace_back(worker_id);
    dead_nodes_.erase(worker_id);
    sorted_dead_node_list_.erase(iter);
    ++evicted_node_number;

    if (batch_ids.size() == batch_size) {
      RAY_CHECK_OK(gcs_table_storage_->NodeTable().BatchDelete(batch_ids, nullptr));
      batch_ids.clear();
    }
  }

  if (!batch_ids.empty()) {
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().BatchDelete(batch_ids, nullptr));
  }
  RAY_LOG(INFO) << evicted_node_number << " nodes are evicted, there are still "
                << sorted_dead_node_list_.size() << " dead nodes in the cache.";
}

void GcsNodeManager::EvictOneDeadNode() {
  if (!sorted_dead_node_list_.empty()) {
    auto iter = sorted_dead_node_list_.begin();
    const auto &node_id = iter->first;
    RAY_CHECK_OK(gcs_table_storage_->NodeTable().Delete(node_id, nullptr));
    dead_nodes_.erase(node_id);
    sorted_dead_node_list_.erase(iter);
  }
}

}  // namespace gcs

}  // namespace ray
