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

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/accessor.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_node_context_cache.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/util/resource_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// Gcs resource manager interface.
/// It is responsible for handing node resource related rpc requests and it is used for
/// actor and placement group scheduling. It obtains the available resources of nodes
/// through heartbeat reporting. Non-thread safe.
class GcsResourceManager : public rpc::NodeResourceInfoHandler {
 public:
  /// Create a GcsResourceManager.
  ///
  /// \param main_io_service The main event loop.
  /// \param gcs_pub_sub GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  explicit GcsResourceManager(instrumented_io_context &main_io_service,
                              std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
                              std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
                              bool redis_broadcast_enabled);

  virtual ~GcsResourceManager() {}

  /// Handle get resource rpc request.
  void HandleGetResources(const rpc::GetResourcesRequest &request,
                          rpc::GetResourcesReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  /// Handle update resource rpc request.
  void HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                             rpc::UpdateResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle delete resource rpc request.
  void HandleDeleteResources(const rpc::DeleteResourcesRequest &request,
                             rpc::DeleteResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get resource usage rpc request.
  void HandleGetResourceUsage(const rpc::GetResourceUsageRequest &request,
                              rpc::GetResourceUsageReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get available resources of all nodes.
  void HandleGetAllAvailableResources(
      const rpc::GetAllAvailableResourcesRequest &request,
      rpc::GetAllAvailableResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Handle report resource usage rpc come from raylet.
  void HandleReportResourceUsage(const rpc::ReportResourceUsageRequest &request,
                                 rpc::ReportResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Handle get all resource usage rpc request.
  void HandleGetAllResourceUsage(const rpc::GetAllResourceUsageRequest &request,
                                 rpc::GetAllResourceUsageReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetResourcesOfAllNodegroups(
      const rpc::GetResourcesOfAllNodegroupsRequest &request,
      rpc::GetResourcesOfAllNodegroupsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetLayeredResourcesOfAllNodegroups(
      const rpc::GetLayeredResourcesOfAllNodegroupsRequest &request,
      rpc::GetLayeredResourcesOfAllNodegroupsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetPendingResourcesOfAllNodegroups(
      const rpc::GetPendingResourcesOfAllNodegroupsRequest &request,
      rpc::GetPendingResourcesOfAllNodegroupsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetClusterResources(const rpc::GetClusterResourcesRequest &request,
                                 rpc::GetClusterResourcesReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  /// Get the resources of all nodes in the cluster.
  ///
  /// \return The resources of all nodes in the cluster.
  const absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
      &GetClusterResources() const;

  /// Find bundles that are ide, e.g:
  /// [Not Idle] cpu_group_1_xxx : 10/10, gpu_group_1_xxx : 1/10
  /// [Idle] cpu_group_2_xxx : 10/10, gpu_group_2_xxx : 10/10
  absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> GetUnusedBundles(
      const NodeID &node_id) const;

  absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
      &GetMutableClusterResources();

  /// Handle a node registration.
  ///
  /// \param node The specified node to add.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Set the available resources of the specified node.
  ///
  /// \param node_id Id of a node.
  /// \param resources Available resources of a node.
  void SetAvailableResources(const NodeID &node_id, const ResourceSet &resources);

  /// Acquire resources from the specified node. It will deduct directly from the node
  /// resources.
  ///
  /// \param node_id Id of a node.
  /// \param required_resources Resources to apply for.
  /// \return True if acquire resources successfully. False otherwise.
  bool AcquireResources(const NodeID &node_id, const ResourceSet &required_resources);

  /// Release the resources of the specified node. It will be added directly to the node
  /// resources.
  ///
  /// \param node_id Id of a node.
  /// \param acquired_resources Resources to release.
  /// \return True if release resources successfully. False otherwise.
  bool ReleaseResources(const NodeID &node_id, const ResourceSet &acquired_resources);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  virtual void Initialize(const GcsInitData &gcs_init_data);

  virtual std::string ToString() const { return ""; }

  std::string DebugString() const;

  // Update node realtime resources.
  virtual void UpdateNodeRealtimeResources(const NodeID &node_id,
                                           const rpc::ResourcesData &heartbeat);

  /// Update resource usage of given node.
  ///
  /// \param node_id Node id.
  /// \param request Request containing resource usage.
  void UpdateNodeResourceUsage(const NodeID &node_id,
                               const rpc::ResourcesData &resources);

  /// Process a new resource report from a node, independent of the rpc handler it came
  /// from.
  void UpdateFromResourceReport(const rpc::ResourcesData &data);

  /// Update the placement group load information so that it will be reported through
  /// heartbeat.
  ///
  /// \param placement_group_load placement group load protobuf.
  void UpdatePlacementGroupLoad(
      const std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load);

  /// Move the lightweight heartbeat information for broadcast into the buffer. This
  /// method MOVES the information, clearing an internal buffer, so it is NOT idempotent.
  ///
  /// \param buffer return parameter
  void GetResourceUsageBatchForBroadcast(rpc::ResourceUsageBatchData &buffer)
      LOCKS_EXCLUDED(resource_buffer_mutex_);

  /// Add resources changed listener.
  void AddResourcesChangedListener(const std::function<void()> &listener);

  /// Notify that the resources are changed.
  void NotifyResourcesChanged();

  const absl::flat_hash_map<NodeID, rpc::ResourcesData> &GetNodeResources();

  const std::vector<NodeContext> &GetNodeContextList() const;

  boost::optional<const NodeContext &> GetNodeContext(const NodeID &node_id) const;

  std::vector<NodeContext> GetContextsByNodeIDs(
      const absl::flat_hash_set<NodeID> &node_ids);

  void AddResourceDelta(const NodeID &node_id = NodeID::Nil());
  void SubtractResourceDelta(const NodeID &node_id = NodeID::Nil());
  bool IsUnusedNode(const NodeID &node_id) const;

  void AddNodeTotalRequiredResources(const NodeID &node_id,
                                     const ResourceSet &resource_set);

  void SubtractNodeTotalRequiredResources(const NodeID &node_id,
                                          const ResourceSet &resource_set);

 protected:
  /// Delete the scheduling resources of the specified node.
  ///
  /// \param node_id Id of a node.
  /// \param deleted_resources Deleted resources of a node.
  void DeleteResources(const NodeID &node_id,
                       const std::vector<std::string> &deleted_resources);

 protected:
  /// Send any buffered resource usage as a single publish.
  void SendBatchedResourceUsage();

  /// Prelocked version of GetResourceUsageBatchForBroadcast. This is necessary for need
  /// the functionality as part of a larger transaction.
  void GetResourceUsageBatchForBroadcast_Locked(rpc::ResourceUsageBatchData &buffer)
      EXCLUSIVE_LOCKS_REQUIRED(resource_buffer_mutex_);

  /// The runner to run function periodically.
  PeriodicalRunner periodical_runner_;
  /// Newest resource usage of all nodes.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> node_resource_usages_;

  /// Protect the lightweight heartbeat deltas which are accessed by different threads.
  absl::Mutex resource_buffer_mutex_;
  /// A buffer containing the lightweight heartbeats since the last broadcast.
  absl::flat_hash_map<NodeID, rpc::ResourcesData> resources_buffer_
      GUARDED_BY(resource_buffer_mutex_);

  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// Storage for GCS tables.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Whether or not to broadcast resource usage via redis.
  const bool redis_broadcast_enabled_;
  /// Map from node id to the scheduling resources of the node.
  absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
      cluster_scheduling_resources_;

  /// Placement group load information that is used for autoscaler.
  absl::optional<std::shared_ptr<rpc::PlacementGroupLoad>> placement_group_load_;
  /// The resources changed listeners.
  std::vector<std::function<void()>> resources_changed_listeners_;
  /// This is a cache used to speed up the traversal of cluster nodes (multi-threaded
  /// parallel iteration).
  std::unique_ptr<NodeContextCache> node_context_cache_;

 protected:
  /// Debug info.
  enum CountType {
    GET_RESOURCES_REQUEST = 0,
    UPDATE_RESOURCES_REQUEST = 1,
    DELETE_RESOURCES_REQUEST = 2,
    GET_ALL_AVAILABLE_RESOURCES_REQUEST = 3,
    REPORT_RESOURCE_USAGE_REQUEST = 4,
    GET_ALL_RESOURCE_USAGE_REQUEST = 5,
    GET_RESOURCE_USAGE_REQUEST = 6,
    GET_CLUSTER_RESOURCES_REQUEST = 7,
    CountType_MAX = 8,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};
};

}  // namespace gcs
}  // namespace ray
