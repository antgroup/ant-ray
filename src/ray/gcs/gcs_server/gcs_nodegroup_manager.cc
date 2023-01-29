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

#include "absl/strings/str_join.h"

#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>

#include "ray/common/ray_config.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"

namespace ray {
namespace gcs {

static inline bool GetParentNodegroupIDAndJob(const std::string &subnodegroup_id,
                                              std::string *parent_nodegroup_id,
                                              JobID *job_id, std::string *job_name) {
  size_t pos = subnodegroup_id.find("##");
  if (pos != std::string::npos) {
    if (parent_nodegroup_id != nullptr) {
      *parent_nodegroup_id = subnodegroup_id.substr(0, pos);
    }
    std::string suffix = subnodegroup_id.substr(pos + 2);
    pos = suffix.find("_");
    RAY_CHECK(pos != std::string::npos);
    if (job_id != nullptr) {
      *job_id = JobID::FromHex(suffix.substr(0, pos));
    }
    if (job_name != nullptr) {
      *job_name = suffix.substr(pos + 1);
    }
    return true;
  }

  return false;
}

static inline std::string ToDebugString(
    const absl::flat_hash_map<NodeShape, int> &node_shape_to_count) {
  std::ostringstream ostr;
  ostr << "{";
  for (const auto &entry : node_shape_to_count) {
    ostr << entry.first << "=" << entry.second << "; ";
  }
  ostr << "}";
  return ostr.str();
}

static inline std::string ToDebugString(
    const NodeShapeAndCountList &node_shape_and_count_list) {
  std::ostringstream ostr;
  for (const auto &node_shape_and_count : node_shape_and_count_list) {
    ostr << "{node_count: " << node_shape_and_count.node_count()
         << ", node_shape: " << NodeShape::ToString(node_shape_and_count.node_shape())
         << "}, ";
  }
  return ostr.str();
}

static inline std::string ToDebugString(
    const rpc::CreateOrUpdateNodegroupRequest &request) {
  std::ostringstream ostr;
  ostr << "{enable_sub_nodegroup_isolation = "
       << request.enable_sub_nodegroup_isolation();
  ostr << ", node_shape_and_count_list = [";
  ostr << ToDebugString(request.node_shape_and_count_list());
  ostr << "]}";
  return ostr.str();
}

std::string NodegroupData::ParseParentNodegroupID(const std::string &nodegroup_id) {
  std::string parent_nodegroup_id;
  GetParentNodegroupIDAndJob(nodegroup_id, &parent_nodegroup_id, /*job_id=*/nullptr,
                             /*job_name=*/nullptr);
  return parent_nodegroup_id;
}

//////////////////////////////////////////////////////////////////////////////////////////
void GcsNodegroupManager::Initialize(const GcsInitData &gcs_init_data) {
  auto default_nodegroup_data = GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  default_nodegroup_data->schedule_options_ = std::make_shared<ScheduleOptions>();
  for (const auto &entry : gcs_init_data.Nodegroups()) {
    const auto &nodegroup_id = entry.first.id_;

    auto nodegroup_data = std::make_shared<NodegroupData>(entry.second);
    nodegroup_data_map_.emplace(nodegroup_id, nodegroup_data);
    default_nodegroup_data->host_to_node_spec_.insert(
        nodegroup_data->host_to_node_spec_.begin(),
        nodegroup_data->host_to_node_spec_.end());

    JobID job_id;
    std::string parent_nodegroup_id;
    if (GetParentNodegroupIDAndJob(nodegroup_id, &parent_nodegroup_id, &job_id,
                                   /*job_name=*/nullptr)) {
      // This nodegroup is a subnodegroup.
      auto iter = gcs_init_data.Jobs().find(job_id);
      if (iter == gcs_init_data.Jobs().end() || iter->second.is_dead()) {
        // This nodegroup should be removed to avoid node resource leak.
        rpc::RemoveNodegroupRequest request;
        request.set_nodegroup_id(nodegroup_id);
        RAY_LOG(INFO) << "Start removing subnodegroup " << nodegroup_id;
        RAY_CHECK_OK(RemoveNodegroup(request, [nodegroup_id](const Status &status) {
          RAY_CHECK_OK(status);
          RAY_LOG(INFO) << "Finished removing subnodegroup " << nodegroup_id;
        }));
      }
    }
  }

  for (const auto &entry : gcs_init_data.Nodes()) {
    if (entry.second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::ALIVE) {
      AddNode(entry.second);
    }
  }
}

void GcsNodegroupManager::HandleCreateOrUpdateNodegroup(
    const rpc::CreateOrUpdateNodegroupRequest &request,
    rpc::CreateOrUpdateNodegroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &nodegroup_id = request.nodegroup_id();
  RAY_LOG(INFO) << "Start creating or updating nodegroup " << nodegroup_id << ", "
                << ToDebugString(request);
  auto on_done = [this, nodegroup_id, reply, send_reply_callback](const Status &status) {
    auto nodegroup_data = GetNodegroupData(nodegroup_id);
    if (nodegroup_data != nullptr) {
      reply->set_revision(nodegroup_data->revision_);
    }

    if (status.ok()) {
      if (nodegroup_data != nullptr) {
        for (auto &entry : nodegroup_data->host_to_node_spec_) {
          reply->mutable_host_to_node_spec()->insert(
              {entry.first, *entry.second.ToProto()});
        }
      }
      RAY_LOG(INFO) << "Succeed in creating or updating nodegroup " << nodegroup_id;
    } else {
      RAY_LOG(WARNING) << "Failed to create or update nodegroup " << nodegroup_id
                       << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = CreateOrUpdateNodegroup(request, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsNodegroupManager::HandleRemoveNodesFromNodegroup(
    const rpc::RemoveNodesFromNodegroupRequest &request,
    rpc::RemoveNodesFromNodegroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &nodegroup_id = request.nodegroup_id();
  RAY_LOG(INFO) << "Removing nodes from nodegroup " << nodegroup_id
                << ", nodes count = " << request.host_name_list_size();
  auto on_done = [nodegroup_id, reply, send_reply_callback](
                     const Status &status,
                     const std::vector<NodeID> &to_be_removed_node_ids) {
    if (status.ok()) {
      for (auto &node_id : to_be_removed_node_ids) {
        reply->add_node_id_list(node_id.Binary());
      }
      RAY_LOG(INFO) << "Finished removing nodes from nodegroup " << nodegroup_id;
    } else {
      RAY_LOG(WARNING) << "Failed to remove nodes from " << nodegroup_id
                       << ", status = " << status.ToString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = RemoveNodesFromNodegroup(request, on_done);
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Failed to remove nodes from " << nodegroup_id
                     << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsNodegroupManager::HandleRemoveNodegroup(
    const rpc::RemoveNodegroupRequest &request, rpc::RemoveNodegroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const auto &nodegroup_id = request.nodegroup_id();
  RAY_LOG(INFO) << "Removing nodegroup " << nodegroup_id;
  if (has_any_running_jobs_in_nodegroup_fn_(nodegroup_id)) {
    std::ostringstream ostr;
    ostr << "Nodegroup " + nodegroup_id +
                " has unfinished jobs, please stop all jobs associated with this "
                "nodegroup first.";
    auto status = Status::Invalid(ostr.str());
    RAY_LOG(WARNING) << "Failed to remove nodegroup " << nodegroup_id
                     << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    return;
  }

  auto on_done = [nodegroup_id, reply, send_reply_callback](const Status &status) {
    RAY_LOG(INFO) << "Finished removing nodegroup " << nodegroup_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = RemoveNodegroup(request, on_done);
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Failed to remove nodegroup " << nodegroup_id
                     << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsNodegroupManager::HandleGetAllNodegroups(
    const rpc::GetAllNodegroupsRequest &request, rpc::GetAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto status = GetAllNodegroups(request, reply);
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
}

NodeShapeAndCountList ToNodeShapeAndCountList(
    const absl::flat_hash_map<std::string, NodeSpec> &host_to_node_spec) {
  NodeShapeAndCountList node_shape_and_count_list;
  absl::flat_hash_map<NodeShape, absl::flat_hash_set<std::string>> node_shape_to_hosts;
  for (auto &entry : host_to_node_spec) {
    node_shape_to_hosts[entry.second.GetNodeShape()].emplace(entry.first);
  }
  for (auto &entry : node_shape_to_hosts) {
    auto node_shape_and_count = node_shape_and_count_list.Add();
    node_shape_and_count->set_node_count(entry.second.size());
    node_shape_and_count->mutable_node_shape()->CopyFrom(entry.first.GetProto());
  }
  return node_shape_and_count_list;
}

void GcsNodegroupManager::HandleReleaseIdleNodes(
    const rpc::ReleaseIdleNodesRequest &request, rpc::ReleaseIdleNodesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto nodegroup_id = request.nodegroup_id();
  if (nodegroup_id.empty()) {
    nodegroup_id = NODEGROUP_RESOURCE_DEFAULT;
  }
  RAY_LOG(INFO) << "Release idle nodes from nodegroup " << nodegroup_id;

  auto on_done =
      [this, nodegroup_id, reply, send_reply_callback](
          const Status &status,
          const absl::flat_hash_map<std::string, NodeSpec> &released_host_to_node_spec) {
        if (status.ok()) {
          if (auto nodegroup_data = GetNodegroupData(nodegroup_id)) {
            (*reply->mutable_released_node_shape_and_count_list()) =
                ToNodeShapeAndCountList(released_host_to_node_spec);
            // Filter unregistered nodes.
            absl::flat_hash_map<std::string, NodeSpec> host_to_node_spec;
            for (auto &entry : nodegroup_data->host_to_node_spec_) {
              if (registered_host_to_nodes_.contains(entry.first)) {
                host_to_node_spec.emplace(entry.first, entry.second);
              }
            }

            if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
              // Filter nodes to be released.
              for (auto &entry : nodes_to_release_) {
                host_to_node_spec.erase(entry.first);
              }
              (*reply->mutable_remaining_node_shape_and_count_list()) =
                  ToNodeShapeAndCountList(host_to_node_spec);
            } else {
              (*reply->mutable_remaining_node_shape_and_count_list()) =
                  ToNodeShapeAndCountList(host_to_node_spec);
            }
          }
          RAY_LOG(INFO) << "Finished releasing idle nodes from nodegroup " << nodegroup_id
                        << ", released: "
                        << ToDebugString(reply->released_node_shape_and_count_list())
                        << ", remaining: "
                        << ToDebugString(reply->remaining_node_shape_and_count_list());
        } else {
          RAY_LOG(WARNING) << "Failed to release idle nodes from nodegroup "
                           << nodegroup_id << ", status = " << status.ToString();
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      };
  auto status = ReleaseIdleNodes(request, on_done);
  if (!status.ok()) {
    RAY_LOG(WARNING) << "Failed to release idle nodes from nodegroup " << nodegroup_id
                     << ", status = " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsNodegroupManager::HandlePinNodesForClusterScalingDown(
    const rpc::PinNodesForClusterScalingDownRequest &request,
    rpc::PinNodesForClusterScalingDownReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::set<std::string> request_hosts;
  request_hosts.insert(request.alive_node_list().begin(),
                       request.alive_node_list().end());
  std::set<std::string> local_subtract_remote;
  std::set<std::string> remote_subtract_local;
  CalcDifferenceNodes(request_hosts, &local_subtract_remote, &remote_subtract_local);
  if (!remote_subtract_local.empty() || !local_subtract_remote.empty()) {
    std::ostringstream ostr;
    ostr << "Failed to pin nodes for cluster scaling";

    if (!remote_subtract_local.empty()) {
      std::ostringstream oss;
      std::copy(remote_subtract_local.begin(), remote_subtract_local.end(),
                std::ostream_iterator<std::string>(oss, ", "));
      ostr << ", [" << oss.str() << "] are running but not registered yet";
    }

    if (!local_subtract_remote.empty()) {
      std::ostringstream oss;
      std::copy(local_subtract_remote.begin(), local_subtract_remote.end(),
                std::ostream_iterator<std::string>(oss, ", "));
      ostr << ", [" << oss.str() << "] are registered but not running";
    }

    std::string message = ostr.str();
    RAY_LOG(WARNING) << message;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(message));
    return;
  }

  // Get registered shape to nodes map.
  auto registered_shape_to_nodes =
      GetRegisteredHostToNodeSpec(NODEGROUP_RESOURCE_DEFAULT);
  // Calc idle node shape to nodes map.
  decltype(registered_shape_to_nodes) idle_shape_to_nodes;
  for (auto &entry1 : registered_shape_to_nodes) {
    for (auto &entry2 : entry1.second) {
      // `registered_host_to_nodes_` must contains host_name.
      for (auto &node_id : registered_host_to_nodes_[entry2.first]) {
        // `node_to_nodegroups_` must contains node_id.
        if (node_to_nodegroups_[node_id].size() == 1) {
          idle_shape_to_nodes[entry1.first].emplace(entry2);
        }
      }
    }
  }

  absl::flat_hash_set<std::string> pinned_node_list;
  if (PinNodesForClusterScalingDown(request.final_node_shape_and_count_list(),
                                    registered_shape_to_nodes, idle_shape_to_nodes,
                                    &pinned_node_list)) {
    reply->mutable_pinned_node_list()->Assign(pinned_node_list.begin(),
                                              pinned_node_list.end());
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  } else {
    std::ostringstream ostr;
    ostr << "Failed to pin nodes for cluster scaling, the cluster node info: ";
    for (auto &entry : registered_shape_to_nodes) {
      auto iter = idle_shape_to_nodes.find(entry.first);
      auto idle_count = iter == idle_shape_to_nodes.end() ? 0 : iter->second.size();
      ostr << entry.first.GetShapeGroup() << "(idle/registered): " << idle_count << "/"
           << entry.second.size() << ", ";
    }
    std::string message = ostr.str();
    RAY_LOG(WARNING) << message;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(message));
  }
}

void GcsNodegroupManager::HandleAddAlternateNodesForMigration(
    const rpc::AddAlternateNodesForMigrationRequest &request,
    rpc::AddAlternateNodesForMigrationReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(INFO) << "Start adding alternate nodes for migration.";
  auto on_done = [reply, send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Failed to add alternate nodes for migration, status = "
                       << status.ToString();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = AddAlternateNodesForMigration(request, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

Status GcsNodegroupManager::CreateOrUpdateNodegroup(
    const rpc::CreateOrUpdateNodegroupRequest &request,
    const ray::gcs::StatusCallback &callback) {
  const std::string &nodegroup_id = request.nodegroup_id();
  if (nodegroup_id.empty()) {
    std::ostringstream ostr;
    ostr << "Invalid request, the nodegroup id is empty.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
    std::ostringstream ostr;
    ostr << "Invalid request, the nodegroup " << nodegroup_id
         << " is a system reserved nodegroup.";
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  for (auto &entry : request.node_shape_and_count_list()) {
    if (entry.node_count() < 0) {
      std::ostringstream ostr;
      ostr << "Invalid request, node_count(" << entry.node_count()
           << ") must >= 0, nodegroup " << nodegroup_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::Invalid(message);
    }

    if (entry.node_shape().shape_group().empty() &&
        !entry.node_shape().resource_shape().empty()) {
      std::ostringstream ostr;
      ostr << "Invalid request, shape_group is empty, nodegroup " << nodegroup_id;
      auto message = ostr.str();
      RAY_LOG(ERROR) << message;
      return Status::Invalid(message);
    }
  }

  auto local_nodegroup_data = GetNodegroupData(nodegroup_id);
  if (local_nodegroup_data == nullptr && IsSubnodegroupID(request.nodegroup_id())) {
    std::ostringstream ostr;
    ostr << "Invalid request, '##' is a system reserved string for subnodegroups, "
            "nodegroup "
         << nodegroup_id;
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  absl::flat_hash_map<NodeShape, int> to_be_added;
  absl::flat_hash_set<std::string> to_be_removed;
  CalculateDifference(nodegroup_id, request.node_shape_and_count_list(), &to_be_added,
                      &to_be_removed);

  absl::flat_hash_map<std::string, NodeSpec> idle_host_to_node_spec;
  if (!to_be_added.empty()) {
    absl::flat_hash_map<NodeShape, int> free_shape_to_count;
    std::string parent_nodegroup_id = NODEGROUP_RESOURCE_DEFAULT;
    if (local_nodegroup_data != nullptr &&
        !local_nodegroup_data->parent_nodegroup_id_.empty()) {
      parent_nodegroup_id = local_nodegroup_data->parent_nodegroup_id_;
    }

    idle_host_to_node_spec = SelectIdleNodesFromNodegroup(
        parent_nodegroup_id, to_be_added, &free_shape_to_count);
    if (idle_host_to_node_spec.empty()) {
      std::ostringstream ss;
      ss << "There are no enough free nodes to assign to the nodegroup " << nodegroup_id
         << ", demand nodes: " << ToDebugString(to_be_added)
         << ", free nodes: " << ToDebugString(free_shape_to_count);
      std::string message = ss.str();
      RAY_LOG(ERROR) << message;
      return Status(StatusCode::NodegroupResourcesInsufficient, message);
    }
  }

  auto schedule_options = ScheduleOptions::FromProto(request.schedule_options());

  std::shared_ptr<NodegroupData> nodegroup_data;
  if (local_nodegroup_data != nullptr) {
    nodegroup_data = std::make_shared<NodegroupData>(*local_nodegroup_data);
    nodegroup_data->nodegroup_name_ = request.nodegroup_name();
    nodegroup_data->enable_sub_nodegroup_isolation_ =
        request.enable_sub_nodegroup_isolation();
    nodegroup_data->enable_revision_ = request.enable_revision();
    nodegroup_data->revision_ = request.revision();
    nodegroup_data->host_to_node_spec_.insert(idle_host_to_node_spec.begin(),
                                              idle_host_to_node_spec.end());
    for (auto &host : to_be_removed) {
      nodegroup_data->host_to_node_spec_.erase(host);
    }
    nodegroup_data->schedule_options_ = std::move(schedule_options);
    nodegroup_data->user_data_ = request.user_data();
    nodegroup_data->enable_job_quota_ = request.enable_job_quota();

    return UpdateNodegroup(std::move(nodegroup_data), callback);
  }

  nodegroup_data = std::make_shared<NodegroupData>(nodegroup_id);
  nodegroup_data->nodegroup_name_ = request.nodegroup_name();
  nodegroup_data->enable_sub_nodegroup_isolation_ =
      request.enable_sub_nodegroup_isolation();
  nodegroup_data->enable_revision_ = request.enable_revision();
  nodegroup_data->host_to_node_spec_ = std::move(idle_host_to_node_spec);
  nodegroup_data->schedule_options_ = std::move(schedule_options);
  nodegroup_data->user_data_ = request.user_data();
  nodegroup_data->enable_job_quota_ = request.enable_job_quota();

  return CreateNodegroup(std::move(nodegroup_data), callback);
}

Status GcsNodegroupManager::CreateNodegroup(std::shared_ptr<NodegroupData> nodegroup_data,
                                            const ray::gcs::StatusCallback &callback) {
  // Update revision.
  if (nodegroup_data->enable_revision_) {
    nodegroup_data->revision_ = current_sys_time_ns();
  }

  auto default_nodegroup_data = GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  const auto &nodegroup_id = nodegroup_data->nodegroup_id_;
  RAY_CHECK(nodegroup_data_map_.emplace(nodegroup_id, nodegroup_data).second);
  for (const auto &entry : nodegroup_data->host_to_node_spec_) {
    auto iter = registered_host_to_nodes_.find(entry.first);
    if (iter != registered_host_to_nodes_.end()) {
      for (auto &node_id : iter->second) {
        node_to_nodegroups_[node_id].emplace(nodegroup_id);
      }
    }
  }

  auto proto_nodegroup_data = nodegroup_data->ToProto();
  auto on_done = [this, proto_nodegroup_data, callback](const Status &status) {
    proto_nodegroup_data->set_nodegroup_updated_timestamp(current_sys_time_ns());
    RAY_CHECK_OK(gcs_pub_sub_->Publish(
        NODEGROUP_CHANNEL, proto_nodegroup_data->nodegroup_id(),
        proto_nodegroup_data->SerializeAsString(), [](const Status &status) {}));
    if (callback) {
      callback(status);
    }
  };

  return gcs_table_storage_->NodegroupTable().Put(NodegroupID::FromBinary(nodegroup_id),
                                                  *proto_nodegroup_data, on_done);
}

Status GcsNodegroupManager::UpdateNodegroup(
    std::shared_ptr<NodegroupData> nodegroup_data,
    const ray::gcs::StatusCallback &callback,
    bool force_remove_nodes_if_needed /*=false*/) {
  const auto &nodegroup_id = nodegroup_data->nodegroup_id_;
  RAY_CHECK(nodegroup_id != NODEGROUP_RESOURCE_DEFAULT);

  auto local_nodegroup_data = GetNodegroupData(nodegroup_id);
  RAY_CHECK(local_nodegroup_data != nullptr);

  if (!local_nodegroup_data->PropertiesMatch(*nodegroup_data)) {
    std::ostringstream ostr;
    ostr << "The properties(enable_sub_nodegroup_isolation/enableSubNamespaceIsolation, "
            "enable_revision, enable_job_quota) "
            "of nodegroup "
         << nodegroup_id << " are mismatch, local: ("
         << local_nodegroup_data->enable_sub_nodegroup_isolation_ << ","
         << local_nodegroup_data->enable_revision_ << ","
         << local_nodegroup_data->enable_job_quota_ << "), received: ("
         << nodegroup_data->enable_sub_nodegroup_isolation_ << ", "
         << nodegroup_data->enable_revision_ << ", " << nodegroup_data->enable_job_quota_
         << ").";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  if (local_nodegroup_data->enable_revision_ &&
      nodegroup_data->revision_ != local_nodegroup_data->revision_) {
    std::ostringstream ss;
    ss << "The revision (" << nodegroup_data->revision_
       << ") is expired, the latest revision of nodegroup " << nodegroup_id << " is "
       << local_nodegroup_data->revision_;
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    return Status(StatusCode::NodegroupRevisionExpired, message);
  }

  absl::flat_hash_set<std::string> to_be_removed_hosts;
  absl::flat_hash_set<std::string> to_be_saftely_removed_hosts;
  absl::flat_hash_set<std::string> not_to_be_saftely_removed_hosts;
  for (auto &entry : local_nodegroup_data->host_to_node_spec_) {
    if (!nodegroup_data->host_to_node_spec_.contains(entry.first)) {
      to_be_removed_hosts.emplace(entry.first);
      if (force_remove_nodes_if_needed ||
          !registered_host_to_nodes_.contains(entry.first) ||
          IsIdleHost(local_nodegroup_data, entry.first)) {
        to_be_saftely_removed_hosts.emplace(entry.first);
      } else {
        not_to_be_saftely_removed_hosts.emplace(entry.first);
      }
    }
  }

  absl::flat_hash_map<std::string, NodeSpec> to_be_added_hosts;
  for (const auto &entry : nodegroup_data->host_to_node_spec_) {
    if (!local_nodegroup_data->host_to_node_spec_.contains(entry.first)) {
      to_be_added_hosts.emplace(entry);
    }
  }

  if (to_be_added_hosts.empty() && to_be_removed_hosts.empty() &&
      local_nodegroup_data->revision_ == nodegroup_data->revision_ &&
      local_nodegroup_data->user_data_ == nodegroup_data->user_data_ &&
      *(local_nodegroup_data->schedule_options_) ==
          *(nodegroup_data->schedule_options_)) {
    if (callback) {
      callback(Status::OK());
    }
    return Status::OK();
  }

  // Considering the compatibility of antc, only checking the removal operation for the
  // nodegroup managed by GCS.
  if (to_be_removed_hosts.size() != to_be_saftely_removed_hosts.size()) {
    std::ostringstream ostr;
    ostr << "There are " << to_be_removed_hosts.size()
         << " nodes need to be removed from nodegroup " << nodegroup_id << ", but only "
         << to_be_saftely_removed_hosts.size() << " nodes could be safely removed.";
    ostr << "There are nodes info that can't be removed safely:"
         << "["
         << absl::StrJoin(not_to_be_saftely_removed_hosts.begin(),
                          not_to_be_saftely_removed_hosts.end(), ",")
         << "]";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  // Update revision.
  if (local_nodegroup_data->enable_revision_) {
    local_nodegroup_data->revision_ = current_sys_time_ns();
  }
  local_nodegroup_data->nodegroup_name_ = nodegroup_data->nodegroup_name_;
  if (updating_nodegroup_schedule_options_fn_) {
    updating_nodegroup_schedule_options_fn_(nodegroup_data);
  }
  *(local_nodegroup_data->schedule_options_) = *(nodegroup_data->schedule_options_);
  local_nodegroup_data->user_data_ = nodegroup_data->user_data_;

  // All the hosts to be removed are unregistered, so it is ok to remove them directly.
  for (auto &host : to_be_removed_hosts) {
    local_nodegroup_data->host_to_node_spec_.erase(host);
    auto registered_host_to_nodes_iter = registered_host_to_nodes_.find(host);
    if (registered_host_to_nodes_iter != registered_host_to_nodes_.end()) {
      for (auto &node_id : registered_host_to_nodes_iter->second) {
        auto node_to_nodegroups_iter = node_to_nodegroups_.find(node_id);
        if (node_to_nodegroups_iter != node_to_nodegroups_.end()) {
          node_to_nodegroups_iter->second.erase(nodegroup_id);
          if (node_to_nodegroups_iter->second.empty()) {
            node_to_nodegroups_.erase(node_id);
          }
        }
      }
    }
  }

  for (auto &entry : to_be_added_hosts) {
    RAY_CHECK(local_nodegroup_data->host_to_node_spec_.emplace(entry).second);
    auto iter = registered_host_to_nodes_.find(entry.first);
    if (iter != registered_host_to_nodes_.end()) {
      for (auto &node_id : iter->second) {
        node_to_nodegroups_[node_id].emplace(nodegroup_id);
      }
    }
  }

  bool need_notify = !to_be_added_hosts.empty();
  auto proto_nodegroup_data = local_nodegroup_data->ToProto();
  auto on_done = [this, proto_nodegroup_data, callback,
                  need_notify](const Status &status) {
    proto_nodegroup_data->set_nodegroup_updated_timestamp(current_sys_time_ns());
    RAY_CHECK_OK(gcs_pub_sub_->Publish(
        NODEGROUP_CHANNEL, proto_nodegroup_data->nodegroup_id(),
        proto_nodegroup_data->SerializeAsString(), [](const Status &status) {}));
    if (callback) {
      callback(status);
    }
    if (need_notify) {
      NotifyNodegroupNodesAdded(proto_nodegroup_data->nodegroup_id());
    }
  };

  return gcs_table_storage_->NodegroupTable().Put(NodegroupID::FromBinary(nodegroup_id),
                                                  *proto_nodegroup_data, on_done);
}

Status GcsNodegroupManager::RemoveNodesFromNodegroup(
    const rpc::RemoveNodesFromNodegroupRequest &request,
    const RemoveNodesFromNodegroupCallback &callback) {
  // Verify if the nodegroup is valid.
  const auto &nodegroup_id = request.nodegroup_id();
  auto local_nodegroup_data = GetNodegroupData(nodegroup_id);
  if (local_nodegroup_data == nullptr) {
    std::string message = "Nodegroup " + nodegroup_id + " does not exist.";
    RAY_LOG(ERROR) << message;
    return Status(StatusCode::NodegroupNotFound, message);
  }

  if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
    std::ostringstream ostr;
    ostr << "It is not supported to remove nodes from system nodegroup " << nodegroup_id;
    auto message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  auto to_be_removed_node_ids = std::make_shared<std::vector<NodeID>>();
  if (request.host_name_list_size() == 0) {
    callback(Status::OK(), *to_be_removed_node_ids);
    return Status::OK();
  }

  std::vector<std::string> invalid_host_name_list;
  auto nodegroup_data = std::make_shared<NodegroupData>(*local_nodegroup_data);
  for (int i = 0; i < request.host_name_list_size(); ++i) {
    const auto &to_be_removed_host = request.host_name_list(i);
    if (nodegroup_data->host_to_node_spec_.erase(to_be_removed_host) == 0) {
      invalid_host_name_list.emplace_back(to_be_removed_host);
      continue;
    }
  }
  if (!invalid_host_name_list.empty()) {
    std::ostringstream invalid_hosts_ostr;
    std::copy(invalid_host_name_list.begin(), invalid_host_name_list.end(),
              std::ostream_iterator<std::string>(invalid_hosts_ostr, ", "));
    std::ostringstream ostr;
    ostr << "[" << invalid_hosts_ostr.str() << "] are not inside " << nodegroup_id;
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  bool force_remove_nodes_if_needed =
      request.mode() == rpc::RemoveNodesFromNodegroupRequest_Mode_HARD;
  for (int i = 0; i < request.host_name_list_size(); ++i) {
    const auto &to_be_removed_host = request.host_name_list(i);
    auto iter = registered_host_to_nodes_.find(to_be_removed_host);
    if (iter != registered_host_to_nodes_.end()) {
      to_be_removed_node_ids->insert(to_be_removed_node_ids->end(), iter->second.begin(),
                                     iter->second.end());
      if (force_remove_nodes_if_needed && exit_node_fn_ &&
          has_any_running_jobs_in_node_fn_) {
        for (auto &node_id : iter->second) {
          if (has_any_running_jobs_in_node_fn_(node_id)) {
            // TODO(Shanly): To be more precise, we should ensure that the node dead
            // state is flushed to redis first, and then update the namespace.
            auto status = Status::ExitAndRestartNode("Node need restart.");
            RAY_LOG(INFO) << "Exit node " << node_id << ", status: " << status.ToString();
            exit_node_fn_(node_id, status);
          }
        }
      }
    }
  }

  auto update_node_group_func = [this, callback, to_be_removed_node_ids, nodegroup_data,
                                 force_remove_nodes_if_needed] {
    return UpdateNodegroup(std::move(nodegroup_data),
                           [callback, to_be_removed_node_ids](const Status &status) {
                             callback(status, *to_be_removed_node_ids);
                           },
                           force_remove_nodes_if_needed);
  };

  if (nodegroup_data->enable_sub_nodegroup_isolation_) {
    absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
        sub_nodegroup_data_map;
    for (auto &entry : nodegroup_data_map_) {
      if (entry.second->parent_nodegroup_id_ == nodegroup_id) {
        std::shared_ptr<NodegroupData> nodegroup_data = nullptr;
        for (int i = 0; i < request.host_name_list_size(); ++i) {
          const auto &to_be_removed_host = request.host_name_list(i);
          if (entry.second->host_to_node_spec_.contains(to_be_removed_host)) {
            if (nodegroup_data == nullptr) {
              nodegroup_data = std::make_shared<NodegroupData>(*entry.second);
              sub_nodegroup_data_map[entry.first] = nodegroup_data;
            }
            nodegroup_data->host_to_node_spec_.erase(to_be_removed_host);
          }
        }
      }
    }
    if (!sub_nodegroup_data_map.empty()) {
      auto update_sub_nodegroups_callback = [callback,
                                             update_node_group_func](Status status) {
        if (status.ok()) {
          status = update_node_group_func();
        }
        if (!status.ok()) {
          callback(status, {});
        }
      };
      return UpdateSubNodegroups(nodegroup_id, sub_nodegroup_data_map,
                                 update_sub_nodegroups_callback,
                                 force_remove_nodes_if_needed);
    }
  }

  return update_node_group_func();
}

Status GcsNodegroupManager::UpdateSubNodegroups(
    const std::string &parent_nodegroup_id,
    const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
        &sub_nodegroup_data_map,
    const ray::gcs::StatusCallback &callback,
    bool force_remove_nodes_if_needed /*= false*/) {
  RAY_CHECK(!sub_nodegroup_data_map.empty());
  absl::flat_hash_set<std::string> to_be_removed_hosts;
  absl::flat_hash_set<std::string> to_be_saftely_removed_hosts;
  for (auto &entry : sub_nodegroup_data_map) {
    auto nodegroup_data = entry.second;
    auto local_nodegroup_data = GetNodegroupData(entry.first);
    for (auto &host_to_node_spec_entry : local_nodegroup_data->host_to_node_spec_) {
      if (!nodegroup_data->host_to_node_spec_.contains(host_to_node_spec_entry.first)) {
        to_be_removed_hosts.emplace(host_to_node_spec_entry.first);
        if (force_remove_nodes_if_needed ||
            !registered_host_to_nodes_.contains(host_to_node_spec_entry.first) ||
            IsIdleHost(local_nodegroup_data, host_to_node_spec_entry.first)) {
          to_be_saftely_removed_hosts.emplace(host_to_node_spec_entry.first);
        }
      }
    }
  }
  if (to_be_removed_hosts.size() != to_be_saftely_removed_hosts.size()) {
    std::ostringstream ostr;
    ostr << "There are " << to_be_removed_hosts.size()
         << " nodes need to be removed from nodegroup " << parent_nodegroup_id
         << ", but only " << to_be_saftely_removed_hosts.size()
         << " nodes could be safely removed.";
    std::string message = ostr.str();
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  auto on_all_nodegroup_update_callbacked = [callback](const auto &status_list) {
    for (auto &status : *status_list) {
      if (!status.ok()) {
        callback(status);
        return;
      }
    }
    callback(Status::OK());
  };

  auto sub_nodegroup_count = sub_nodegroup_data_map.size();
  auto status_list = std::make_shared<std::vector<Status>>();
  for (auto &entry : sub_nodegroup_data_map) {
    const auto &nodegroup_id = entry.first;
    auto on_done = [nodegroup_id, status_list, sub_nodegroup_count,
                    on_all_nodegroup_update_callbacked](const Status &status) {
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Failed to update sub-nodegorup " << nodegroup_id
                       << ", status: " << status.ToString();
      }
      status_list->emplace_back(status);
      if (status_list->size() == sub_nodegroup_count) {
        on_all_nodegroup_update_callbacked(status_list);
      }
    };

    auto status = UpdateNodegroup(entry.second, on_done, force_remove_nodes_if_needed);
    if (!status.ok()) {
      on_done(status);
    }
  }
  return Status::OK();
}

Status GcsNodegroupManager::RemoveNodegroup(const rpc::RemoveNodegroupRequest &request,
                                            const ray::gcs::StatusCallback &callback) {
  // Verify if the nodegroup is valid.
  const auto &nodegroup_id = request.nodegroup_id();
  if (nodegroup_id.empty()) {
    std::string message = "Invalid nodegroup, the nodegroup id is empty.";
    RAY_LOG(ERROR) << message;
    return Status::Invalid(message);
  }

  auto iter = nodegroup_data_map_.find(nodegroup_id);
  if (iter == nodegroup_data_map_.end()) {
    std::string message = "Nodegroup " + nodegroup_id + " does not exist.";
    RAY_LOG(ERROR) << message;
    return Status(StatusCode::NodegroupNotFound, message);
  }

  auto nodegroup_data = std::move(iter->second);
  nodegroup_data_map_.erase(iter);

  for (auto &entry : nodegroup_data->host_to_node_spec_) {
    auto iter = registered_host_to_nodes_.find(entry.first);
    if (iter != registered_host_to_nodes_.end()) {
      for (auto &node_id : iter->second) {
        auto it = node_to_nodegroups_.find(node_id);
        if (it != node_to_nodegroups_.end()) {
          it->second.erase(nodegroup_id);
          if (it->second.empty()) {
            node_to_nodegroups_.erase(it);
          }
        }
      }
    }
  }
  nodegroup_data->host_to_node_spec_.clear();

  auto proto_nodegroup_data = nodegroup_data->ToProto();
  proto_nodegroup_data->set_is_removed(true);

  auto on_done = [this, proto_nodegroup_data, callback](const Status &status) {
    RAY_CHECK_OK(gcs_pub_sub_->Publish(
        NODEGROUP_CHANNEL, proto_nodegroup_data->nodegroup_id(),
        proto_nodegroup_data->SerializeAsString(), [](const Status &status) {}));
    if (callback) {
      callback(status);
    }
  };

  return gcs_table_storage_->NodegroupTable().Delete(
      NodegroupID::FromBinary(nodegroup_id), on_done);
}

Status GcsNodegroupManager::GetAllNodegroups(const rpc::GetAllNodegroupsRequest &request,
                                             rpc::GetAllNodegroupsReply *reply) {
  for (auto &nodegroup_entry : nodegroup_data_map_) {
    if (!request.include_sub_nodegroups() && IsSubnodegroupID(nodegroup_entry.first)) {
      continue;
    }

    auto proto_nodegroup_data = nodegroup_entry.second->ToProto();
    for (auto &entry : nodegroup_entry.second->host_to_node_spec_) {
      if (!registered_host_to_nodes_.contains(entry.first)) {
        proto_nodegroup_data->mutable_host_to_node_spec()->erase(entry.first);
        proto_nodegroup_data->mutable_host_to_node_spec()->insert(
            {entry.first + " [unregistered]", *entry.second.ToProto()});
      }
    }
    (*reply->add_nodegroup_data_list()) = std::move(*proto_nodegroup_data);
  }
  return Status::OK();
}

void GcsNodegroupManager::AddNode(const gcs::GcsNodeInfo &node) {
  const auto &host_name = node.basic_gcs_node_info().node_manager_hostname();
  auto node_id = NodeID::FromBinary(node.basic_gcs_node_info().node_id());

  RAY_LOG(INFO) << "Add node " << node_id;
  // Update registered nodes.
  registered_host_to_nodes_[host_name].emplace(node_id);
  // Add the node to the default nodegroup.
  auto default_nodegroup_data = GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  NodeSpec node_spec(node);
  default_nodegroup_data->host_to_node_spec_[host_name] = node_spec;

  for (auto &entry : nodegroup_data_map_) {
    auto &nodegroup_data = entry.second;
    auto iter = nodegroup_data->host_to_node_spec_.find(host_name);
    if (iter != nodegroup_data->host_to_node_spec_.end()) {
      // Update the node shape as the cached one maybe still empty.
      iter->second = node_spec;
      // Update the node_to_nodegroup map.
      node_to_nodegroups_[node_id].emplace(entry.first);
    }
  }
}

void GcsNodegroupManager::RemoveNode(std::shared_ptr<gcs::GcsNodeInfo> node) {
  const auto &node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  const auto &host_name = node->basic_gcs_node_info().node_manager_hostname();
  // Remove the node from registered map.
  auto iter = registered_host_to_nodes_.find(host_name);
  RAY_CHECK(iter != registered_host_to_nodes_.end() && iter->second.erase(node_id) != 0);
  if (iter->second.empty()) {
    registered_host_to_nodes_.erase(iter);
  }

  // Remove the node from `nodes_to_release_` map.
  auto it = nodes_to_release_.find(host_name);
  if (it != nodes_to_release_.end()) {
    it->second.erase(node_id);
    if (it->second.empty()) {
      nodes_to_release_.erase(it);
    }
  }

  // Remove entry associated with this node ID from the `node_to_nodegroups_`.
  node_to_nodegroups_.erase(node_id);
}

bool GcsNodegroupManager::IsNodeInNodegroup(const NodeID &node_id,
                                            const std::string &nodegroup_id) const {
  if (nodegroup_id.empty()) {
    return true;
  }

  auto iter = node_to_nodegroups_.find(node_id);
  return iter != node_to_nodegroups_.end() && iter->second.contains(nodegroup_id);
}

const absl::flat_hash_map<NodeID, absl::flat_hash_set<std::string>>
    &GcsNodegroupManager::GetNodeToNodegroupsMap() const {
  return node_to_nodegroups_;
}

std::string GcsNodegroupManager::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');
  std::string indent_2(indent + 2 * 2, ' ');
  std::string indent_3(indent + 3 * 2, ' ');

  ostr << "{\n";

  ostr << indent_1 << "nodegroup_data_map = {\n";
  for (auto &nodegroup_entry : nodegroup_data_map_) {
    ostr << indent_2 << nodegroup_entry.first << " = [\n";
    for (auto &entry : nodegroup_entry.second->host_to_node_spec_) {
      ostr << indent_3 << entry.first << " = " << entry.second << ",\n";
    }
    ostr << indent_2 << "],\n";
  }
  ostr << indent_1 << "},\n";

  ostr << indent_1 << "registered_nodes = {\n";
  for (auto &entry : registered_host_to_nodes_) {
    ostr << indent_2 << entry.first << " = [\n";
    for (auto &item : entry.second) {
      ostr << indent_3 << item << ",\n";
    }
    ostr << indent_2 << "],\n";
  }
  ostr << indent_1 << "},\n";

  ostr << indent_1 << "node_to_nodegroups = {\n";
  for (auto &entry : node_to_nodegroups_) {
    ostr << indent_2 << entry.first << " = [\n";
    for (auto &item : entry.second) {
      ostr << indent_3 << item << ",\n";
    }
    ostr << indent_2 << "],\n";
  }
  ostr << indent_1 << "},\n";

  ostr << indent_0 << "},\n";
  return ostr.str();
}

bool GcsNodegroupManager::IsIdleHost(const std::shared_ptr<NodegroupData> &nodegroup_data,
                                     const std::string &host) const {
  if (nodes_to_release_.contains(host)) {
    // Skip nodes that will be released later.
    return false;
  }

  auto registered_node_it = registered_host_to_nodes_.find(host);
  if (registered_node_it == registered_host_to_nodes_.end()) {
    // The node with the host has not yet registered.
    return false;
  }
  const auto &registered_node_ids = registered_node_it->second;

  const auto &nodegroup_id = nodegroup_data->nodegroup_id_;
  if (!nodegroup_data->enable_sub_nodegroup_isolation_ &&
      nodegroup_id != NODEGROUP_RESOURCE_DEFAULT) {
    if (!has_any_running_jobs_in_node_fn_) {
      return !has_any_running_jobs_in_nodegroup_fn_(nodegroup_id);
    }

    for (auto &node_id : registered_node_ids) {
      if (has_any_running_jobs_in_node_fn_(node_id)) {
        return false;
      }
    }
    return true;
  }

  absl::flat_hash_set<std::string> related_nodegroups;
  for (auto &node_id : registered_node_ids) {
    auto iter = node_to_nodegroups_.find(node_id);
    if (iter != node_to_nodegroups_.end()) {
      related_nodegroups.insert(iter->second.begin(), iter->second.end());
    }
  }

  if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
    return related_nodegroups.size() == 1;
  }

  return related_nodegroups.size() == 2 && related_nodegroups.contains(nodegroup_id);
}

absl::flat_hash_map<std::string, NodeSpec>
GcsNodegroupManager::SelectIdleNodesFromNodegroup(
    std::string parent_nodegroup_id,
    absl::flat_hash_map<NodeShape, int> node_shape_to_count,
    absl::flat_hash_map<NodeShape, int> *free_node_shape_to_count /*=nullptr*/) const {
  absl::flat_hash_map<std::string, NodeSpec> idle_host_to_node_spec;

  auto nodegroup_data = GetNodegroupData(parent_nodegroup_id);
  if (nodegroup_data == nullptr) {
    return idle_host_to_node_spec;
  }

  // Filter out entry whose count == 0.
  auto iter = node_shape_to_count.begin();
  for (; iter != node_shape_to_count.end();) {
    auto current = iter++;
    if (current->second == 0) {
      node_shape_to_count.erase(current);
    }
  }

  if (free_node_shape_to_count) {
    for (auto &entry : node_shape_to_count) {
      (*free_node_shape_to_count)[entry.first] = 0;
    }
  }

  NodeShape any_shape;
  for (auto &entry : nodegroup_data->host_to_node_spec_) {
    auto it = node_shape_to_count.find(entry.second.GetNodeShape());
    if (it == node_shape_to_count.end()) {
      it = node_shape_to_count.find(any_shape);
      if (it == node_shape_to_count.end()) {
        // This shape is unexpected.
        continue;
      }
    }

    if (it->second == 0) {
      node_shape_to_count.erase(it);
      continue;
    }

    if (IsIdleHost(nodegroup_data, entry.first)) {
      if (IsFrozenHostName(entry.first)) {
        // Skip frozen nodes.
        continue;
      }

      idle_host_to_node_spec.emplace(entry);
      if (free_node_shape_to_count) {
        (*free_node_shape_to_count)[entry.second.GetNodeShape()]++;
      }
      if (--(it->second) == 0) {
        node_shape_to_count.erase(it);
        if (node_shape_to_count.empty()) {
          break;
        }
      }
    }
  }

  if (!node_shape_to_count.empty()) {
    idle_host_to_node_spec.clear();
  }

  return idle_host_to_node_spec;
}

void GcsNodegroupManager::ReplenishNodegroupNode(
    std::shared_ptr<NodegroupData> nodegroup_data,
    const std::pair<std::string, NodeSpec> &old_node,
    const std::pair<std::string, NodeSpec> &new_node) {
  const auto &nodegroup_id = nodegroup_data->nodegroup_id_;
  RAY_LOG(INFO) << "Replenishing a new host (" << new_node.first << ") to nodegroup "
                << nodegroup_id << " to replace the dead one(" << old_node.first << ").";

  auto new_nodegroup_data = std::make_shared<NodegroupData>(*nodegroup_data);
  new_nodegroup_data->host_to_node_spec_.erase(old_node.first);
  new_nodegroup_data->host_to_node_spec_.emplace(new_node);

  RAY_CHECK_OK(UpdateNodegroup(std::move(new_nodegroup_data),
                               [old_node, new_node, nodegroup_id](const Status &status) {
                                 RAY_CHECK_OK(status);
                                 RAY_LOG(INFO)
                                     << "Finished replenishing a new host("
                                     << new_node.first << ") to nodegroup "
                                     << nodegroup_id << " to replace the dead one("
                                     << new_node.first << ").";
                               }));
}

void GcsNodegroupManager::StartNodegroupMonitor(boost::asio::io_context &io_context) {
  RAY_CHECK(monitor_timer_ == nullptr);
  monitor_timer_.reset(new boost::asio::steady_timer(io_context));
  auto interval =
      std::chrono::milliseconds(RayConfig::instance().nodegroups_inspect_interval_ms());
  // Reset the timer.
  monitor_timer_->expires_from_now(interval);
  monitor_timer_->async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    MonitorNodegroups();
  });
}

void GcsNodegroupManager::MonitorNodegroups() {
  auto check_and_replenish_func =
      [this](const std::vector<std::shared_ptr<NodegroupData>> &nodegroup_data_list) {
        for (const auto &nodegroup_data : nodegroup_data_list) {
          // NOTE: It is neccessary to clone a `host_to_node_spec` to avoid invalid
          // iterator when traversing as the method `UpdateNodegroup` inside
          // `ReplenishNodegroupNode` may erase element from `host_to_node_spec`.
          auto cloned_host_to_node_spec = nodegroup_data->host_to_node_spec_;
          for (const auto &entry : cloned_host_to_node_spec) {
            if (!registered_host_to_nodes_.contains(entry.first)) {
              auto idle_host_to_node_spec =
                  SelectIdleNodesFromNodegroup(nodegroup_data->parent_nodegroup_id_,
                                               {{entry.second.GetNodeShape(), 1}});
              if (!idle_host_to_node_spec.empty()) {
                ReplenishNodegroupNode(nodegroup_data, entry,
                                       *idle_host_to_node_spec.begin());
              }
            }
          }
        }
      };

  std::vector<std::shared_ptr<NodegroupData>> sub_nodegroup_data_list;
  std::vector<std::shared_ptr<NodegroupData>> non_sub_nodegroup_data_list;
  for (auto &entry : nodegroup_data_map_) {
    if (entry.first == NODEGROUP_RESOURCE_DEFAULT) {
      continue;
    }
    if (entry.second->parent_nodegroup_id_.empty()) {
      non_sub_nodegroup_data_list.emplace_back(entry.second);
    } else {
      sub_nodegroup_data_list.emplace_back(entry.second);
    }
  }

  // The non-sub-nodegroup should be checked in advance.
  // Consider this scene, nodegroup N has 3 nodes [1, 2, 3], and it has a sub-nodegroup
  // sub(N) takes all the 3 nodes [1,2,3] from N.
  // T1: node 3 is dead. the state will be:
  //        N: [1,2,3(dead)], sub(N): [1,2,3(dead)]
  // T2: checking the sub-nodegroup in advance, nothing will be changed about sub(N) as N
  // has idle alive nodes. Then checking N, suppose that there are a lot of idle alive
  // nodes inside NODEGROUP_RESOURCE_DEFAULT, the node 3 will be replenished by node 4
  // inside N, the state will be:
  //        N: [1,2,4], sub(N): [1,2,3(dead)]
  // T3: node 3 alive again, the state will be:
  //        N: [1,2,4], sub(N): [1,2,3]
  // It is unexpected.
  // If we checking the non-sub-nodegroup inadvance, it will be ok.
  check_and_replenish_func(non_sub_nodegroup_data_list);
  check_and_replenish_func(sub_nodegroup_data_list);

  auto interval =
      std::chrono::milliseconds(RayConfig::instance().nodegroups_inspect_interval_ms());
  // Reset the timer.
  monitor_timer_->expires_from_now(interval);
  monitor_timer_->async_wait([this](const boost::system::error_code &error) {
    RAY_CHECK(!error);
    MonitorNodegroups();
  });
}

void GcsNodegroupManager::OnJobFinished(std::shared_ptr<JobTableData> job_data) {
  if (job_data == nullptr) {
    return;
  }

  const auto &nodegroup_id = job_data->nodegroup_id();
  if (IsSubnodegroupID(nodegroup_id)) {
    rpc::RemoveNodegroupRequest request;
    request.set_nodegroup_id(nodegroup_id);
    RAY_LOG(INFO) << "Start removing subnodegroup " << nodegroup_id;
    RAY_UNUSED(RemoveNodegroup(request, [nodegroup_id](const Status &status) {
      RAY_CHECK_OK(status);
      RAY_LOG(INFO) << "Finished removing subnodegroup " << nodegroup_id;
    }));
  }
}

absl::flat_hash_map<NodeShape, absl::flat_hash_set<std::string>>
GcsNodegroupManager::GetShapedHostsFromNodegroup(const std::string &nodegroup_id) const {
  absl::flat_hash_map<NodeShape, absl::flat_hash_set<std::string>> node_shape_to_hosts;
  if (auto nodegroup_data = GetNodegroupData(nodegroup_id)) {
    for (auto &entry : nodegroup_data->host_to_node_spec_) {
      node_shape_to_hosts[entry.second.GetNodeShape()].emplace(entry.first);
    }
  }
  return node_shape_to_hosts;
}

void GcsNodegroupManager::CalculateDifference(
    const std::string &nodegroup_id,
    const NodeShapeAndCountList &node_shape_and_count_list,
    absl::flat_hash_map<NodeShape, int> *to_be_added,
    absl::flat_hash_set<std::string> *to_be_removed) const {
  auto node_shape_to_count = ToNodeShapeAndCountMap(node_shape_and_count_list);
  auto node_shape_to_hosts = GetShapedHostsFromNodegroup(nodegroup_id);
  if (node_shape_to_hosts.empty()) {
    (*to_be_added) = std::move(node_shape_to_count);
    return;
  }

  auto nodegroup_data = GetNodegroupData(nodegroup_id);
  RAY_CHECK(nodegroup_data != nullptr);

  for (auto &entry : node_shape_to_hosts) {
    auto it = node_shape_to_count.find(entry.first);
    if (it == node_shape_to_count.end()) {
      to_be_removed->insert(entry.second.begin(), entry.second.end());
      continue;
    }

    size_t new_count = it->second;
    size_t old_count = entry.second.size();
    if (old_count < new_count) {
      // `delt` additional nodes with current shape need to be added.
      auto delt = new_count - old_count;
      to_be_added->emplace(entry.first, delt);
    } else if (old_count > new_count) {
      // Make sure the order is: unregisterd,idle,registerd
      std::vector<std::string> sorted_hosts(entry.second.begin(), entry.second.end());
      // Split sorted_hosts to (unregisterd) | (idle, registerd)
      auto bound = std::partition(sorted_hosts.begin(), sorted_hosts.end(),
                                  /*condition=*/
                                  [this](const std::string &host) {
                                    return !registered_host_to_nodes_.contains(host);
                                  });
      if (bound != sorted_hosts.end()) {
        // Split (idle, registerd) to (idle) | (registerd)
        std::partition(bound, sorted_hosts.end(), /*condition=*/
                       [this, nodegroup_data](const std::string &host) {
                         return IsIdleHost(nodegroup_data, host);
                       });
      }

      // `delt` nodes with current shape need to be removed.
      auto delt = old_count - new_count;
      for (auto &host : sorted_hosts) {
        to_be_removed->emplace(host);
        if (--delt == 0) {
          break;
        }
      }
    }
  }

  for (auto &entry : node_shape_to_count) {
    const auto &node_shape = entry.first;
    if (node_shape.IsAny()) {
      continue;
    }

    auto node_shape_to_hosts_it = node_shape_to_hosts.find(node_shape);
    if (node_shape_to_hosts_it == node_shape_to_hosts.end()) {
      to_be_added->emplace(entry);
    }
  }

  NodeShape any_shape;
  auto node_shape_to_count_iter = node_shape_to_count.find(any_shape);
  if (node_shape_to_count_iter != node_shape_to_count.end()) {
    size_t new_count = node_shape_to_count_iter->second;

    if (new_count > 0) {
      // Make sure the order is: registered,idle,unregistered
      std::vector<std::string> sorted_hosts(to_be_removed->begin(), to_be_removed->end());
      // Split sorted_hosts to (registerd) | (idle, unregistered)
      auto bound = std::partition(sorted_hosts.begin(), sorted_hosts.end(),
                                  /*condition=*/
                                  [this](const std::string &host) {
                                    return registered_host_to_nodes_.contains(host);
                                  });
      if (bound != sorted_hosts.end()) {
        // Split (idle, unregistered) to (idle) | (unregistered)
        std::partition(bound, sorted_hosts.end(), /*condition=*/
                       [this, nodegroup_data](const std::string &host) {
                         return IsIdleHost(nodegroup_data, host);
                       });
      }

      // Cancel nodes from the list to be removed if new_count > 0.
      for (auto &host : sorted_hosts) {
        to_be_removed->erase(host);
        if (--new_count == 0) {
          break;
        }
      }
    }

    if (new_count > 0) {
      // Put the node shape and count to the list to be added.
      to_be_added->emplace(any_shape, new_count);
    }
  }
}

std::shared_ptr<NodegroupData> GcsNodegroupManager::GetNodegroupData(
    const std::string &nodegroup_id) const {
  if (nodegroup_id.empty()) {
    return GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  }
  auto iter = nodegroup_data_map_.find(nodegroup_id);
  return iter == nodegroup_data_map_.end() ? nullptr : iter->second;
}

std::shared_ptr<ScheduleOptions> GcsNodegroupManager::GetScheduleOptions(
    const std::string &nodegroup_id) const {
  auto nodegroup_data = GetNodegroupData(nodegroup_id);
  return nodegroup_data ? nodegroup_data->schedule_options_
                        : std::make_shared<ScheduleOptions>();
}

std::shared_ptr<ScheduleOptions> GcsNodegroupManager::GetScheduleOptions(
    const NodeID &node_id) const {
  auto iter = node_to_nodegroups_.find(node_id);
  if (iter != node_to_nodegroups_.end()) {
    for (const auto &nodegroup_id : iter->second) {
      if (nodegroup_id != NODEGROUP_RESOURCE_DEFAULT) {
        return GetScheduleOptions(nodegroup_id);
      }
    }
  }
  return std::make_shared<ScheduleOptions>();
}

void GcsNodegroupManager::AddNodegroupNodeAddedListener(
    const std::function<void(const std::string &)> &listener) {
  RAY_CHECK(listener != nullptr);
  nodegroup_nodes_added_listeners_.emplace_back(listener);
}

void GcsNodegroupManager::NotifyNodegroupNodesAdded(const std::string &nodegroup_id) {
  for (auto &listener : nodegroup_nodes_added_listeners_) {
    listener(nodegroup_id);
  }
}

Status GcsNodegroupManager::AddSubNodegroup(
    const std::string &nodegroup_id, const JobID &job_id, const std::string &job_name,
    const NodeShapeAndCountList &node_shape_and_count_list,
    const AddSubNodegroupCallback &callback) {
  auto node_shape_to_count = ToNodeShapeAndCountMap(node_shape_and_count_list);

  absl::flat_hash_map<NodeShape, int> free_node_shape_to_count;
  auto idle_host_to_node_spec = SelectIdleNodesFromNodegroup(
      nodegroup_id, node_shape_to_count, &free_node_shape_to_count);
  if (idle_host_to_node_spec.empty()) {
    std::ostringstream ss;
    ss << "Nodegroup " << nodegroup_id
       << " does not have enough free nodes to initialize job " << job_id
       << ". Required nodes: " << ToDebugString(node_shape_to_count)
       << ", Free nodes: " << ToDebugString(free_node_shape_to_count);
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  auto parent_nodegroup_data = GetNodegroupData(nodegroup_id);
  if (parent_nodegroup_data == nullptr) {
    std::ostringstream ss;
    ss << "Nodegroup " << nodegroup_id << " does not exist.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  auto subnodegroup_id = BuildSubnodegroupID(nodegroup_id, job_id, job_name);
  auto subnodegroup_data = std::make_shared<NodegroupData>(subnodegroup_id);
  subnodegroup_data->enable_sub_nodegroup_isolation_ = false;
  subnodegroup_data->enable_revision_ = parent_nodegroup_data->enable_revision_;
  for (const auto &entry : idle_host_to_node_spec) {
    subnodegroup_data->host_to_node_spec_.emplace(entry);
  }

  auto create_sub_nodegroup_callback = [this, subnodegroup_id,
                                        callback](const Status &status) {
    auto s = callback(subnodegroup_id);
    if (!s.ok()) {
      // remove the nodegroup to avoid resource leak.
      rpc::RemoveNodegroupRequest remove_nodegroup_request;
      remove_nodegroup_request.set_nodegroup_id(subnodegroup_id);
      RAY_LOG(INFO) << "Start removing subnodegroup " << subnodegroup_id;
      RAY_CHECK_OK(RemoveNodegroup(
          remove_nodegroup_request, [subnodegroup_id](const Status &status) {
            RAY_LOG(INFO) << "Finished removing subnodegroup " << subnodegroup_id;
          }));
    }
  };

  return CreateNodegroup(subnodegroup_data, create_sub_nodegroup_callback);
}

absl::flat_hash_set<NodeID> GcsNodegroupManager::GetRegisteredNodes(
    const std::string &nodegroup_id) const {
  absl::flat_hash_set<NodeID> nodes;
  if (auto nodegroup_data = GetNodegroupData(nodegroup_id)) {
    for (const auto &entry : nodegroup_data->host_to_node_spec_) {
      auto iter = registered_host_to_nodes_.find(entry.first);
      if (iter != registered_host_to_nodes_.end()) {
        nodes.insert(iter->second.begin(), iter->second.end());
      }
    }
  }
  return nodes;
}

void GcsNodegroupManager::ForEachRegisteredNodegroupNode(
    const NodegroupNodeIterFunc &func) {
  RAY_CHECK(func);
  for (const auto &nodegroup_entry : nodegroup_data_map_) {
    for (const auto &entry : nodegroup_entry.second->host_to_node_spec_) {
      const auto &host_name = entry.first;
      auto host_iter = registered_host_to_nodes_.find(host_name);
      if (host_iter != registered_host_to_nodes_.end()) {
        for (const auto &node_id : host_iter->second) {
          func(nodegroup_entry.first, host_name, node_id,
               entry.second.GetNodeShape().GetShapeGroup());
        }
      }
    }
  }
}

void GcsNodegroupManager::ForEachNodeOfDefaultNodegroup(
    const NodegroupNodeIterFunc &func) {
  RAY_CHECK(func);
  for (const auto &entry :
       nodegroup_data_map_[NODEGROUP_RESOURCE_DEFAULT]->host_to_node_spec_) {
    const auto &host_name = entry.first;
    auto host_iter = registered_host_to_nodes_.find(host_name);
    if (host_iter != registered_host_to_nodes_.end()) {
      for (const auto &node_id : host_iter->second) {
        func(NODEGROUP_RESOURCE_DEFAULT, host_name, node_id,
             entry.second.GetNodeShape().GetShapeGroup());
      }
    }
  }
}

Status GcsNodegroupManager::ReleaseIdleNodes(const rpc::ReleaseIdleNodesRequest &request,
                                             const ReleaseIdleNodesCallback &callback) {
  const auto &nodegroup_id = request.nodegroup_id();
  auto local_nodegroup_data = GetNodegroupData(nodegroup_id);
  if (local_nodegroup_data == nullptr) {
    return Status::Invalid("Nodegroup " + nodegroup_id + " does not exist.");
  }

  auto host_shape_to_release =
      std::make_shared<absl::flat_hash_map<std::string, NodeSpec>>();
  auto nodes_to_release = std::make_shared<decltype(nodes_to_release_)>();
  auto nodegroup_data = std::make_shared<NodegroupData>(*local_nodegroup_data);
  for (const auto &entry : local_nodegroup_data->host_to_node_spec_) {
    if (IsHostReleasable(local_nodegroup_data, entry.first)) {
      nodegroup_data->host_to_node_spec_.erase(entry.first);
      auto iter = registered_host_to_nodes_.find(entry.first);
      if (iter != registered_host_to_nodes_.end()) {
        nodes_to_release->emplace(*iter);
        host_shape_to_release->emplace(entry);
      }
    }
  }
  if (nodes_to_release->empty()) {
    callback(Status::OK(), {});
    return Status::OK();
  }

  auto on_done = [this, host_shape_to_release, nodes_to_release,
                  callback](const ray::Status &status) {
    if (!status.ok()) {
      callback(status, {});
      return;
    }

    for (auto &entry : *nodes_to_release) {
      nodes_to_release_[entry.first].insert(entry.second.begin(), entry.second.end());
      if (exit_node_fn_) {
        auto status = Status::ExitAndRestartNode("Node has been released.");
        for (auto it = entry.second.begin(); it != entry.second.end(); ++it) {
          RAY_LOG(INFO) << "Exit node " << (*it) << ", status: " << status.ToString();
          exit_node_fn_(*it, status);
        }
      }
    }

    callback(status, *host_shape_to_release);
  };

  if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
    // Default nodegroup does not need to write meta data to storage.
    on_done(Status::OK());
    return Status::OK();
  }

  return UpdateNodegroup(std::move(nodegroup_data), on_done);
}

bool GcsNodegroupManager::IsHostReleasable(
    const std::shared_ptr<NodegroupData> &nodegroup_data, const std::string &host) const {
  if (nodes_to_release_.contains(host)) {
    // Skip nodes that will be released later.
    return false;
  }

  const auto &nodegroup_id = nodegroup_data->nodegroup_id_;
  if (!nodegroup_data->enable_sub_nodegroup_isolation_ &&
      nodegroup_id != NODEGROUP_RESOURCE_DEFAULT) {
    if (!has_any_running_jobs_in_node_fn_) {
      return !has_any_running_jobs_in_nodegroup_fn_(nodegroup_id);
    }

    auto registered_node_it = registered_host_to_nodes_.find(host);
    if (registered_node_it != registered_host_to_nodes_.end()) {
      for (auto &node_id : registered_node_it->second) {
        if (has_any_running_jobs_in_node_fn_(node_id)) {
          return false;
        }
      }
    }

    return true;
  }

  auto registered_node_it = registered_host_to_nodes_.find(host);
  if (registered_node_it != registered_host_to_nodes_.end()) {
    absl::flat_hash_set<std::string> related_nodegroups;
    for (auto &node_id : registered_node_it->second) {
      auto iter = node_to_nodegroups_.find(node_id);
      if (iter != node_to_nodegroups_.end()) {
        related_nodegroups.insert(iter->second.begin(), iter->second.end());
      }
    }

    if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
      return related_nodegroups.size() == 1;
    }

    return related_nodegroups.size() == 2 && related_nodegroups.contains(nodegroup_id);
  }

  if (nodegroup_data->enable_sub_nodegroup_isolation_) {
    // The host can not be released if it is still used by any sub-nodegroup.
    for (auto &entry : nodegroup_data_map_) {
      if (entry.second->parent_nodegroup_id_ == nodegroup_data->nodegroup_id_ &&
          entry.second->host_to_node_spec_.contains(host)) {
        return false;
      }
    }
    return true;
  }

  return false;
}

absl::flat_hash_map<NodeShape, int> GcsNodegroupManager::ToNodeShapeAndCountMap(
    const NodeShapeAndCountList &node_shape_and_count_list) const {
  absl::flat_hash_map<NodeShape, int> node_shape_to_count;
  for (const auto &node_shape_and_count : node_shape_and_count_list) {
    NodeShape node_shape(node_shape_and_count.node_shape());
    auto iter = node_shape_to_count.find(node_shape);
    if (iter == node_shape_to_count.end()) {
      node_shape_to_count.emplace(node_shape, node_shape_and_count.node_count());
    } else {
      iter->second += node_shape_and_count.node_count();
    }
  }
  return node_shape_to_count;
}

void GcsNodegroupManager::CalcDifferenceNodes(
    const std::set<std::string> &request_hosts,
    std::set<std::string> *local_subtract_remote,
    std::set<std::string> *remote_subtract_local) {
  std::set<std::string> registered_hosts;
  for (auto &entry : registered_host_to_nodes_) {
    registered_hosts.emplace(entry.first);
  }

  std::set_difference(
      registered_hosts.begin(), registered_hosts.end(), request_hosts.begin(),
      request_hosts.end(),
      std::inserter(*local_subtract_remote, local_subtract_remote->begin()));

  std::set_difference(
      request_hosts.begin(), request_hosts.end(), registered_hosts.begin(),
      registered_hosts.end(),
      std::inserter(*remote_subtract_local, remote_subtract_local->begin()));
}

absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
GcsNodegroupManager::GetRegisteredHostToNodeSpec(const std::string &nodegroup_id) const {
  absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
      shape_to_nodes;
  if (auto nodegroup_data = GetNodegroupData(nodegroup_id)) {
    for (auto &entry : nodegroup_data->host_to_node_spec_) {
      if (registered_host_to_nodes_.contains(entry.first)) {
        shape_to_nodes[entry.second.GetNodeShape()].emplace(entry);
      }
    }
  }
  return shape_to_nodes;
}

bool GcsNodegroupManager::PinNodesForClusterScalingDown(
    const NodeShapeAndCountList &final_node_shape_and_count_list,
    const absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
        &registered_shape_to_nodes,
    const absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
        &idle_shape_to_nodes,
    absl::flat_hash_set<std::string> *pinned_node_list) {
  absl::flat_hash_set<std::string> to_be_removed_nodes;
  for (auto &entry : registered_shape_to_nodes) {
    auto iter = std::find_if(final_node_shape_and_count_list.begin(),
                             final_node_shape_and_count_list.end(),
                             [&entry](const auto &node_shape_and_count) {
                               return node_shape_and_count.node_shape().shape_group() ==
                                      entry.first.GetShapeGroup();
                             });
    int number_of_nodes_to_be_scaled_down = 0;
    if (iter == final_node_shape_and_count_list.end()) {
      number_of_nodes_to_be_scaled_down = entry.second.size();
    } else {
      number_of_nodes_to_be_scaled_down = int(entry.second.size()) - iter->node_count();
    }
    if (number_of_nodes_to_be_scaled_down > 0) {
      auto iter = idle_shape_to_nodes.find(entry.first);
      if (iter == idle_shape_to_nodes.end() ||
          int(iter->second.size()) < number_of_nodes_to_be_scaled_down) {
        return false;
      }

      for (auto &host_to_node_spec_entry : iter->second) {
        to_be_removed_nodes.emplace(host_to_node_spec_entry.first);
        if (--number_of_nodes_to_be_scaled_down == 0) {
          break;
        }
      }
    }
  }

  auto nodegroup_info = GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  for (auto &entry1 : registered_shape_to_nodes) {
    for (auto &entry2 : entry1.second) {
      if (!to_be_removed_nodes.contains(entry2.first)) {
        pinned_node_list->emplace(entry2.first);
      }
    }
  }
  return true;
}

Status GcsNodegroupManager::AddAlternateNodesForMigration(
    const rpc::AddAlternateNodesForMigrationRequest &request,
    const ray::gcs::StatusCallback &callback) {
  absl::flat_hash_set<std::string> host_name_set;
  int node_list_size = request.alternate_node_list_size();
  for (int i = 0; i < node_list_size; ++i) {
    const auto &alternate_node = request.alternate_node_list(i);
    if (alternate_node.alternate_host_name().empty() ||
        alternate_node.to_be_migrated_host_name().empty()) {
      std::string message =
          "Invalid request, alternateHostName and toBeMigratedHostName can not be empty.";
      return Status::Invalid(message);
    }

    if (!host_name_set.emplace(alternate_node.alternate_host_name()).second ||
        !host_name_set.emplace(alternate_node.to_be_migrated_host_name()).second) {
      std::string message =
          "Invalid request, alternateHostName and toBeMigratedHostName must be "
          "different.";
      return Status::Invalid(message);
    }
  }

  struct HostNameAndNodegroups {
    std::string host_name_;
    NodeSpec node_spec_;
    absl::flat_hash_set<std::string> nodegroups_;
  };
  struct AlternateNodeWithNodegroups {
    std::shared_ptr<HostNameAndNodegroups> alternate_hostname_and_nodegroups_;
    std::shared_ptr<HostNameAndNodegroups> to_be_migrated_hostname_and_nodegroups_;
  };
  auto get_hostname_and_nodegroups_func =
      [this](const std::string &host_name,
             const absl::flat_hash_map<std::string, NodeSpec> &host_to_node_spec,
             absl::flat_hash_set<std::string> *unregistered_hosts) {
        auto hostname_and_nodegroups = std::make_shared<HostNameAndNodegroups>();
        hostname_and_nodegroups->host_name_ = host_name;
        auto iter = registered_host_to_nodes_.find(host_name);
        if (iter == registered_host_to_nodes_.end()) {
          unregistered_hosts->emplace(host_name);
        } else {
          for (auto &node_id : iter->second) {
            auto iter = node_to_nodegroups_.find(node_id);
            if (iter != node_to_nodegroups_.end()) {
              hostname_and_nodegroups->nodegroups_.insert(iter->second.begin(),
                                                          iter->second.end());
            }
          }

          auto iter = host_to_node_spec.find(host_name);
          if (iter != host_to_node_spec.end()) {
            hostname_and_nodegroups->node_spec_ = iter->second;
          }
        }
        return hostname_and_nodegroups;
      };

  absl::flat_hash_set<std::string> unregistered_hosts;
  std::vector<AlternateNodeWithNodegroups> alternate_node_with_nodegroups(node_list_size);
  auto default_nodegroup = GetNodegroupData(NODEGROUP_RESOURCE_DEFAULT);
  for (int i = 0; i < node_list_size; ++i) {
    const auto &alternate_node = request.alternate_node_list(i);
    auto &hostname_and_nodegroups = alternate_node_with_nodegroups[i];
    hostname_and_nodegroups.alternate_hostname_and_nodegroups_ =
        get_hostname_and_nodegroups_func(alternate_node.alternate_host_name(),
                                         default_nodegroup->host_to_node_spec_,
                                         &unregistered_hosts);
    hostname_and_nodegroups.to_be_migrated_hostname_and_nodegroups_ =
        get_hostname_and_nodegroups_func(alternate_node.to_be_migrated_host_name(),
                                         default_nodegroup->host_to_node_spec_,
                                         &unregistered_hosts);
  }

  // Makesure all nodes are registered.
  if (!unregistered_hosts.empty()) {
    std::ostringstream unregistered_hosts_ostr;
    std::copy(unregistered_hosts.begin(), unregistered_hosts.end(),
              std::ostream_iterator<std::string>(unregistered_hosts_ostr, ", "));
    std::ostringstream ostr;
    ostr << "[" << unregistered_hosts_ostr.str() << "] are not registered.";
    return Status::Invalid(ostr.str());
  }

  auto is_superset = [](const absl::flat_hash_set<std::string> &groups1,
                        const absl::flat_hash_set<std::string> &groups2) {
    if (groups1.size() < groups2.size()) {
      return false;
    }
    for (auto &nodegroup_id : groups2) {
      if (!groups1.contains(nodegroup_id)) {
        return false;
      }
    }
    return true;
  };

  for (const auto &entry : alternate_node_with_nodegroups) {
    // Make sure the alternate hostname is not frozen.
    const auto &alternate_hostname = entry.alternate_hostname_and_nodegroups_->host_name_;
    const auto &to_be_migrated_hostname =
        entry.alternate_hostname_and_nodegroups_->host_name_;
    if (IsFrozenHostName(alternate_hostname)) {
      std::ostringstream ostr;
      ostr << "The alternate node " << alternate_hostname << " is in the frozen list.";
      return Status::Invalid(ostr.str());
    }

    // Make sure the group is consistent between alternateHostName and
    // toBeMigratedHostName.
    if (entry.alternate_hostname_and_nodegroups_->node_spec_.GetNodeShape() !=
        entry.to_be_migrated_hostname_and_nodegroups_->node_spec_.GetNodeShape()) {
      std::ostringstream ostr;
      ostr << "The group of alternateHostName and toBeMigratedHostName is mismatch, "
           << alternate_hostname << "("
           << entry.alternate_hostname_and_nodegroups_->node_spec_.GetNodeShape()
                  .GetShapeGroup()
           << ") vs. " << to_be_migrated_hostname << "("
           << entry.to_be_migrated_hostname_and_nodegroups_->node_spec_.GetNodeShape()
                  .GetShapeGroup()
           << ")";
      return Status::Invalid(ostr.str());
    }

    // Make sure the alternate node could replace the node to be migrated.
    if (!is_superset(entry.to_be_migrated_hostname_and_nodegroups_->nodegroups_,
                     entry.alternate_hostname_and_nodegroups_->nodegroups_)) {
      std::ostringstream ostr;
      ostr << "Node " << alternate_hostname << " cannot be used as a alternate node for "
           << to_be_migrated_hostname << " because it has been used.";
      return Status::Invalid(ostr.str());
    }
  }

  // Calc all related sub_nodegroup_data_list and non_sub_nodegroup_data_list.
  std::vector<std::shared_ptr<NodegroupData>> sub_nodegroup_data_list;
  std::vector<std::shared_ptr<NodegroupData>> non_sub_nodegroup_data_list;
  absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>> related_nodegroups;
  for (const auto &entry : alternate_node_with_nodegroups) {
    // Since entrto_be_migrated_hostname_and_nodegroups_->nodegroups_ is superset of
    // alternate_hostname_and_nodegroups_->nodegroups_, it is just need to iterate
    // to_be_migrated_hostname_and_nodegroups_->nodegroups_
    for (auto &nodegroup_id :
         entry.to_be_migrated_hostname_and_nodegroups_->nodegroups_) {
      if (nodegroup_id == NODEGROUP_RESOURCE_DEFAULT) {
        // Skip NODEGROUP_RESOURCE_DEFAULT as node will register to it directly.
        continue;
      }

      const auto &alternate_hostname =
          entry.alternate_hostname_and_nodegroups_->host_name_;
      auto iter = related_nodegroups.find(nodegroup_id);
      if (iter == related_nodegroups.end()) {
        auto nodegroup_data = GetNodegroupData(nodegroup_id);
        RAY_CHECK(nodegroup_data != nullptr);
        if (nodegroup_data->host_to_node_spec_.contains(alternate_hostname)) {
          // Skip if the `alternate_hostname` alreay in the nodegorup.
          continue;
        }
        // clone a new one to avoid the original meta data be modified.
        auto cloned_nodegroup_data = std::make_shared<NodegroupData>(*nodegroup_data);
        if (cloned_nodegroup_data->parent_nodegroup_id_.empty()) {
          non_sub_nodegroup_data_list.emplace_back(cloned_nodegroup_data);
        } else {
          sub_nodegroup_data_list.emplace_back(cloned_nodegroup_data);
        }
        iter = related_nodegroups.emplace(nodegroup_id, cloned_nodegroup_data).first;
      }
      iter->second->host_to_node_spec_.emplace(
          alternate_hostname, entry.alternate_hostname_and_nodegroups_->node_spec_);
    }
  }

  auto to_be_udpated_nodegroup_count =
      non_sub_nodegroup_data_list.size() + sub_nodegroup_data_list.size();
  if (to_be_udpated_nodegroup_count == 0) {
    callback(Status::OK());
    return Status::OK();
  }

  auto status_list = std::make_shared<std::vector<Status>>();
  auto on_done = [to_be_udpated_nodegroup_count, status_list,
                  callback](const Status &status) {
    status_list->emplace_back(status);
    if (status_list->size() == to_be_udpated_nodegroup_count) {
      for (auto &status : *status_list) {
        if (!status.ok()) {
          callback(status);
          return;
        }
      }
      callback(Status::OK());
    }
  };

  for (auto &nodegroup_data : non_sub_nodegroup_data_list) {
    auto status = UpdateNodegroup(nodegroup_data, on_done);
    if (!status.ok()) {
      on_done(status);
    }
  }

  for (auto &nodegroup_data : sub_nodegroup_data_list) {
    auto status = UpdateNodegroup(nodegroup_data, on_done);
    if (!status.ok()) {
      on_done(status);
    }
  }

  return Status::OK();
}

bool GcsNodegroupManager::IsFrozenHostName(const std::string &host_name) const {
  if (is_frozen_node_fn_) {
    auto iter = registered_host_to_nodes_.find(host_name);
    if (iter != registered_host_to_nodes_.end()) {
      for (auto &node_id : iter->second) {
        if (is_frozen_node_fn_(node_id)) {
          return true;
        }
      }
    }
  }
  return false;
}

const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
    &GcsNodegroupManager::GetNodegroupDataMap() const {
  return nodegroup_data_map_;
}

bool GcsNodegroupManager::IsJobQuotaEnabled(const std::string &nodegroup_id) const {
  auto nodegroup_data = GetNodegroupData(nodegroup_id);
  return nodegroup_data ? nodegroup_data->enable_job_quota_
                        : RayConfig::instance().enable_job_quota();
}

}  // namespace gcs
}  // namespace ray
