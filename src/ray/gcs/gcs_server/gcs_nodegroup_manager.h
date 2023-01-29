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

#include <memory>

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/node_spec.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/server_call.h"

namespace ray {
namespace gcs {

typedef ::google::protobuf::RepeatedPtrField<rpc::NodeShapeAndCount>
    NodeShapeAndCountList;

struct ScheduleOptions;
struct NodegroupData {
  NodegroupData(const std::string &nodegroup_id)
      : nodegroup_id_(nodegroup_id),
        parent_nodegroup_id_(ParseParentNodegroupID(nodegroup_id_)),
        schedule_options_(std::make_shared<ScheduleOptions>()) {}

  NodegroupData(NodegroupData &&nodegroup_data)
      : nodegroup_id_(std::move(nodegroup_data.nodegroup_id_)),
        parent_nodegroup_id_(ParseParentNodegroupID(nodegroup_id_)),
        nodegroup_name_(std::move(nodegroup_data.nodegroup_name_)),
        enable_sub_nodegroup_isolation_(nodegroup_data.enable_sub_nodegroup_isolation_),
        host_to_node_spec_(std::move(nodegroup_data.host_to_node_spec_)),
        schedule_options_(std::move(nodegroup_data.schedule_options_)),
        user_data_(std::move(nodegroup_data.user_data_)),
        enable_revision_(nodegroup_data.enable_revision_),
        revision_(nodegroup_data.revision_),
        enable_job_quota_(nodegroup_data.enable_job_quota_) {}

  NodegroupData(const NodegroupData &nodegroup_data)
      : nodegroup_id_(nodegroup_data.nodegroup_id_),
        parent_nodegroup_id_(ParseParentNodegroupID(nodegroup_id_)),
        nodegroup_name_(nodegroup_data.nodegroup_name_),
        enable_sub_nodegroup_isolation_(nodegroup_data.enable_sub_nodegroup_isolation_),
        host_to_node_spec_(nodegroup_data.host_to_node_spec_),
        schedule_options_(nodegroup_data.schedule_options_),
        user_data_(nodegroup_data.user_data_),
        enable_revision_(nodegroup_data.enable_revision_),
        revision_(nodegroup_data.revision_),
        enable_job_quota_(nodegroup_data.enable_job_quota_) {}

  NodegroupData(const rpc::NodegroupData &data)
      : nodegroup_id_(data.nodegroup_id()),
        parent_nodegroup_id_(ParseParentNodegroupID(nodegroup_id_)),
        nodegroup_name_(data.nodegroup_name()),
        enable_sub_nodegroup_isolation_(data.enable_sub_nodegroup_isolation()),
        user_data_(data.user_data()),
        enable_revision_(data.enable_revision()),
        revision_(data.revision()),
        enable_job_quota_(data.enable_job_quota()) {
    for (auto &entry : data.host_to_node_spec()) {
      host_to_node_spec_.emplace(entry.first, NodeSpec(entry.second));
    }
    schedule_options_ = ScheduleOptions::FromMapProto(data.schedule_options());
  }

  std::shared_ptr<rpc::NodegroupData> ToProto() const {
    auto nodegroup_data = std::make_shared<rpc::NodegroupData>();
    nodegroup_data->set_nodegroup_id(nodegroup_id_);
    nodegroup_data->set_nodegroup_name(nodegroup_name_);
    nodegroup_data->set_enable_sub_nodegroup_isolation(enable_sub_nodegroup_isolation_);
    nodegroup_data->set_user_data(user_data_);
    nodegroup_data->set_enable_revision(enable_revision_);
    nodegroup_data->set_revision(revision_);
    nodegroup_data->set_enable_job_quota(enable_job_quota_);
    auto mutable_host_to_node_spec = nodegroup_data->mutable_host_to_node_spec();
    for (auto &entry : host_to_node_spec_) {
      mutable_host_to_node_spec->insert({entry.first, *entry.second.ToProto()});
    }
    auto schedule_option_map = schedule_options_->ToMap();
    auto mutable_schedule_options = nodegroup_data->mutable_schedule_options();
    mutable_schedule_options->insert(schedule_option_map.begin(),
                                     schedule_option_map.end());
    return nodegroup_data;
  }

  std::string ToString() const {
    std::ostringstream ostr;
    ostr << "nodegroup_id: " << nodegroup_id_ << ", nodegroup_name: " << nodegroup_name_
         << ", enable_sub_nodegroup_isolation: " << enable_sub_nodegroup_isolation_
         << ", host_to_node_spec: [";
    for (const auto &entry : host_to_node_spec_) {
      ostr << entry.first << ": " << entry.second << ", ";
    }
    ostr << "], schedule_options: " << schedule_options_->ToString();
    ostr << ", user_data: " << user_data_;
    ostr << ", enable_revision: " << enable_revision_;
    ostr << ", revision: " << revision_;
    ostr << ", enable_job_quota: " << enable_job_quota_;
    return ostr.str();
  }

  /// Whether the properties of the current nodegroup match the specified one.
  bool PropertiesMatch(const NodegroupData &other) const {
    return enable_sub_nodegroup_isolation_ == other.enable_sub_nodegroup_isolation_ &&
           enable_revision_ == other.enable_revision_ &&
           nodegroup_id_ == other.nodegroup_id_ &&
           enable_job_quota_ == other.enable_job_quota_;
  }

  static std::string ParseParentNodegroupID(const std::string &nodegroup_id);

 public:
  /// ID of the nodegroup.
  std::string nodegroup_id_;
  /// ID of the parent nodegroup.
  /// The parent nodegroup could supplement idle nodes to its child nodegroup.
  std::string parent_nodegroup_id_;
  /// Name of the nodegroup.
  std::string nodegroup_name_;
  /// If true (default), multiple jobs submitted to this nodegroup will share all the
  /// nodes in this nodegroup. Otherwise, we will create a sub-nodegroup for each job upon
  /// submission.
  bool enable_sub_nodegroup_isolation_ = false;
  /// Map from host name to its node specification.
  absl::flat_hash_map<std::string, NodeSpec> host_to_node_spec_;

  std::shared_ptr<ScheduleOptions> schedule_options_;

  /// User data.
  std::string user_data_;
  bool enable_revision_ = false;
  /// Version of this nodegroup.
  uint64_t revision_ = 0;
  /// Whether limiting jobs' quota.
  bool enable_job_quota_ = RayConfig::instance().enable_job_quota();
};

using AddSubNodegroupCallback = std::function<Status(const std::string &)>;
using NodegroupNodeIterFunc =
    std::function<void(const std::string &nodegroup_id, const std::string &host_name,
                       const NodeID &node_id, const std::string &shape_group)>;
typedef std::function<void(const Status &,
                           const absl::flat_hash_map<std::string, NodeSpec> &)>
    ReleaseIdleNodesCallback;
typedef std::function<void(const Status &, const std::vector<NodeID> &)>
    RemoveNodesFromNodegroupCallback;
class GcsNodegroupManagerInterface {
 public:
  virtual ~GcsNodegroupManagerInterface() = default;

  virtual std::shared_ptr<NodegroupData> GetNodegroupData(
      const std::string &nodegroup_id) const = 0;

  virtual Status RemoveNodegroup(const rpc::RemoveNodegroupRequest &request,
                                 const ray::gcs::StatusCallback &callback) = 0;

  virtual Status AddSubNodegroup(const std::string &nodegroup_id, const JobID &job_id,
                                 const std::string &job_name,
                                 const NodeShapeAndCountList &node_shape_and_count_list,
                                 const AddSubNodegroupCallback &callback) = 0;

  virtual void ForEachRegisteredNodegroupNode(const NodegroupNodeIterFunc &func) = 0;

  virtual void ForEachNodeOfDefaultNodegroup(const NodegroupNodeIterFunc &func) = 0;

  virtual absl::flat_hash_set<NodeID> GetRegisteredNodes(
      const std::string &nodegroup_id) const = 0;

  static bool IsSubnodegroupID(const std::string &nodegroup_id) {
    return nodegroup_id.find("##") != std::string::npos;
  }

  virtual const absl::flat_hash_map<NodeID, absl::flat_hash_set<std::string>>
      &GetNodeToNodegroupsMap() const = 0;

  static std::string BuildSubnodegroupID(const std::string &parent_nodegroup_id,
                                         const JobID &job_id,
                                         const std::string &job_name) {
    return parent_nodegroup_id + "##" + job_id.Hex() + "_" + job_name;
  }

  virtual std::shared_ptr<ScheduleOptions> GetScheduleOptions(
      const std::string &nodegroup_id) const = 0;

  virtual const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
      &GetNodegroupDataMap() const = 0;

  virtual bool IsJobQuotaEnabled(const std::string &nodegroup_id) const = 0;
};

/// This implementation class of `NodegroupInfoHandler`.
class GcsNodegroupManager : public rpc::NodegroupInfoHandler,
                            public GcsNodegroupManagerInterface {
 public:
  explicit GcsNodegroupManager(
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::function<bool(const std::string &)> has_any_running_jobs_in_nodegroup_fn,
      std::function<void(const NodeID &, const Status &)> exit_node_fn = nullptr,
      std::function<bool(const NodeID &)> has_any_running_jobs_in_node_fn = nullptr,
      std::function<bool(const NodeID &)> is_frozen_node_fn = nullptr,
      std::function<void(std::shared_ptr<NodegroupData>)>
          updating_nodegroup_schedule_options_fn = nullptr)
      : gcs_table_storage_(std::move(gcs_table_storage)),
        gcs_pub_sub_(std::move(gcs_pub_sub)),
        has_any_running_jobs_in_nodegroup_fn_(
            std::move(has_any_running_jobs_in_nodegroup_fn)),
        exit_node_fn_(std::move(exit_node_fn)),
        has_any_running_jobs_in_node_fn_(std::move(has_any_running_jobs_in_node_fn)),
        is_frozen_node_fn_(std::move(is_frozen_node_fn)),
        updating_nodegroup_schedule_options_fn_(
            std::move(updating_nodegroup_schedule_options_fn)) {
    auto default_nodegroup_data =
        std::make_shared<NodegroupData>(NODEGROUP_RESOURCE_DEFAULT);
    nodegroup_data_map_.emplace(NODEGROUP_RESOURCE_DEFAULT, default_nodegroup_data);
  }

  ~GcsNodegroupManager() override = default;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  void StartNodegroupMonitor(boost::asio::io_context &io_context);

  const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
      &GetAllNodegroups() const {
    return nodegroup_data_map_;
  }

  Status CreateOrUpdateNodegroup(const rpc::CreateOrUpdateNodegroupRequest &request,
                                 const ray::gcs::StatusCallback &callback);

  Status CreateNodegroup(std::shared_ptr<NodegroupData> nodegroup_data,
                         const ray::gcs::StatusCallback &callback);

  Status UpdateNodegroup(std::shared_ptr<NodegroupData> nodegroup_data,
                         const ray::gcs::StatusCallback &callback,
                         bool force_remove_nodes_if_needed = false);

  Status RemoveNodesFromNodegroup(const rpc::RemoveNodesFromNodegroupRequest &request,
                                  const RemoveNodesFromNodegroupCallback &callback);

  Status UpdateSubNodegroups(
      const std::string &parent_nodegroup_id,
      const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
          &sub_nodegroup_data_map,
      const ray::gcs::StatusCallback &callback,
      bool force_remove_nodes_if_needed = false);

  Status RemoveNodegroup(const rpc::RemoveNodegroupRequest &request,
                         const ray::gcs::StatusCallback &callback) override;

  Status GetAllNodegroups(const rpc::GetAllNodegroupsRequest &request,
                          rpc::GetAllNodegroupsReply *reply);

  Status ReleaseIdleNodes(const rpc::ReleaseIdleNodesRequest &request,
                          const ReleaseIdleNodesCallback &callback);

  void ForEachRegisteredNodegroupNode(const NodegroupNodeIterFunc &func) override;

  void ForEachNodeOfDefaultNodegroup(const NodegroupNodeIterFunc &func) override;

  /// Event handling when a node is registered.
  void AddNode(const gcs::GcsNodeInfo &node);

  /// Event handling when a node is removed.
  void RemoveNode(std::shared_ptr<gcs::GcsNodeInfo> node);

  const absl::flat_hash_map<NodeID, absl::flat_hash_set<std::string>>
      &GetNodeToNodegroupsMap() const override;

  bool IsNodeInNodegroup(const NodeID &node_id, const std::string &nodegroup_id) const;

  void OnJobFinished(std::shared_ptr<JobTableData> job_data);

  void AddNodegroupNodeAddedListener(
      const std::function<void(const std::string &)> &listener);

  void NotifyNodegroupNodesAdded(const std::string &nodegroup_id);

  std::shared_ptr<ScheduleOptions> GetScheduleOptions(
      const std::string &nodegroup_id) const override;

  std::shared_ptr<ScheduleOptions> GetScheduleOptions(const NodeID &node_id) const;

  Status AddSubNodegroup(const std::string &nodegroup_id, const JobID &job_id,
                         const std::string &job_name,
                         const NodeShapeAndCountList &node_shape_and_count_list,
                         const AddSubNodegroupCallback &callback) override;

  absl::flat_hash_set<NodeID> GetRegisteredNodes(
      const std::string &nodegroup_id) const override;

  Status AddAlternateNodesForMigration(
      const rpc::AddAlternateNodesForMigrationRequest &request,
      const ray::gcs::StatusCallback &callback);

  const absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>>
      &GetNodegroupDataMap() const override;

  bool IsJobQuotaEnabled(const std::string &nodegroup_id) const override;

 protected:
  void HandleCreateOrUpdateNodegroup(const rpc::CreateOrUpdateNodegroupRequest &request,
                                     rpc::CreateOrUpdateNodegroupReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveNodesFromNodegroup(
      const rpc::RemoveNodesFromNodegroupRequest &request,
      rpc::RemoveNodesFromNodegroupReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveNodegroup(const rpc::RemoveNodegroupRequest &request,
                             rpc::RemoveNodegroupReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllNodegroups(const rpc::GetAllNodegroupsRequest &request,
                              rpc::GetAllNodegroupsReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleReleaseIdleNodes(const rpc::ReleaseIdleNodesRequest &request,
                              rpc::ReleaseIdleNodesReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandlePinNodesForClusterScalingDown(
      const rpc::PinNodesForClusterScalingDownRequest &request,
      rpc::PinNodesForClusterScalingDownReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddAlternateNodesForMigration(
      const rpc::AddAlternateNodesForMigrationRequest &request,
      rpc::AddAlternateNodesForMigrationReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  absl::flat_hash_map<std::string, NodeSpec> SelectIdleNodesFromNodegroup(
      std::string parent_namespace_id,
      absl::flat_hash_map<NodeShape, int> node_shape_to_count,
      absl::flat_hash_map<NodeShape, int> *free_node_spec_to_count = nullptr) const;

  void ReplenishNodegroupNode(std::shared_ptr<NodegroupData> nodegroup_data,
                              const std::pair<std::string, NodeSpec> &old_node,
                              const std::pair<std::string, NodeSpec> &new_node);

  /// Monitor nodegroups periodically.
  /// If a host dead, the monitor will make up an idle one to the nodegroup from its
  /// parent nodegroup.
  void MonitorNodegroups();

  std::shared_ptr<NodegroupData> GetNodegroupData(
      const std::string &nodegroup_id) const override;

  bool IsIdleHost(const std::shared_ptr<NodegroupData> &nodegroup_data,
                  const std::string &host) const;

  bool IsHostReleasable(const std::shared_ptr<NodegroupData> &namespace_data,
                        const std::string &host) const;

  absl::flat_hash_map<NodeShape, absl::flat_hash_set<std::string>>
  GetShapedHostsFromNodegroup(const std::string &nodegroup_id) const;

  void CalculateDifference(const std::string &nodegroup_id,
                           const NodeShapeAndCountList &node_shape_and_count_list,
                           absl::flat_hash_map<NodeShape, int> *to_be_added,
                           absl::flat_hash_set<std::string> *to_be_removed) const;

  void CalcDifferenceNodes(const std::set<std::string> &request_hosts,
                           std::set<std::string> *local_subtract_remote,
                           std::set<std::string> *remote_subtract_local);

  absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
  GetRegisteredHostToNodeSpec(const std::string &nodegroup_id) const;

  bool PinNodesForClusterScalingDown(
      const NodeShapeAndCountList &final_node_shape_and_count_list,
      const absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
          &registered_shape_to_nodes,
      const absl::flat_hash_map<NodeShape, absl::flat_hash_map<std::string, NodeSpec>>
          &idle_shape_to_nodes,
      absl::flat_hash_set<std::string> *pinned_node_list);

  absl::flat_hash_map<NodeShape, int> ToNodeShapeAndCountMap(
      const NodeShapeAndCountList &node_shape_and_count_list) const;

  bool IsFrozenHostName(const std::string &host_name) const;

  std::string ToString(int indent = 0) const;

 protected:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;

  std::function<bool(const std::string &)> has_any_running_jobs_in_nodegroup_fn_;

  /// <Host, [NodeID]>
  ///            |
  ///        <NodeID, [NodegroupID]>
  ///                       |
  ///                 <NodegroupID, NodegroupData>
  /// Map from host name to the IDs of nodes associated with the host.
  absl::flat_hash_map<std::string, absl::flat_hash_set<NodeID>> registered_host_to_nodes_;
  /// Map from node ID to the IDs of nodegroups.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<std::string>> node_to_nodegroups_;
  /// Map from nodegroup ID to the nodegroup data.
  absl::flat_hash_map<std::string, std::shared_ptr<NodegroupData>> nodegroup_data_map_;
  /// The timer to inspect nodegroups.
  std::unique_ptr<boost::asio::steady_timer> monitor_timer_;

  std::vector<std::function<void(const std::string &)>> nodegroup_nodes_added_listeners_;

  /// Map from host name to the IDs of nodes associated with the host.
  absl::flat_hash_map<std::string, absl::flat_hash_set<NodeID>> nodes_to_release_;
  std::function<void(const NodeID &, const Status &)> exit_node_fn_;

  std::function<bool(const NodeID &)> has_any_running_jobs_in_node_fn_;

  std::function<bool(const NodeID &)> is_frozen_node_fn_;

  std::function<void(std::shared_ptr<NodegroupData>)>
      updating_nodegroup_schedule_options_fn_;

  FRIEND_TEST(GcsNodegroupManagerTest, CalculateDifference);
  FRIEND_TEST(GcsJobManagerTest, Initialize);
};

}  // namespace gcs
}  // namespace ray
