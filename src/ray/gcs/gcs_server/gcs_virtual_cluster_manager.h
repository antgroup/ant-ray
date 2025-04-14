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

#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/gcs_virtual_cluster.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {

class PeriodicalRunner;

namespace gcs {

/// This implementation class of `VirtualClusterInfoHandler`.
class GcsVirtualClusterManager : public rpc::VirtualClusterInfoHandler {
 public:
  explicit GcsVirtualClusterManager(
      instrumented_io_context &io_context,
      GcsTableStorage &gcs_table_storage,
      GcsPublisher &gcs_publisher,
      const ClusterResourceManager &cluster_resource_manager,
      std::shared_ptr<PeriodicalRunner> periodical_runner = nullptr)
      : io_context_(io_context),
        gcs_table_storage_(gcs_table_storage),
        gcs_publisher_(gcs_publisher),
        periodical_runner_(periodical_runner),
        primary_cluster_(std::make_shared<PrimaryCluster>(
            [this](auto data, auto callback) {
              return FlushAndPublish(std::move(data), std::move(callback));
            },
            cluster_resource_manager)) {}

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Handle the node added event.
  ///
  /// \param node The node that is added.
  void OnNodeAdd(const rpc::GcsNodeInfo &node);

  /// Handle the node dead event.
  ///
  /// \param node The node that is dead.
  void OnNodeDead(const rpc::GcsNodeInfo &node);

  /// Handle the job finished event.
  ///
  /// \param job_data The job that is finished.
  void OnJobFinished(const rpc::JobTableData &job_data);

  /// Get virtual cluster by virtual cluster id
  ///
  /// \param virtual_cluster_id The id of virtual cluster
  /// \return the virtual cluster
  std::shared_ptr<VirtualCluster> GetVirtualCluster(
      const std::string &virtual_cluster_id);

  /// Handle detached actor registration.
  void OnDetachedActorRegistration(const std::string &virtual_cluster_id,
                                   const ActorID &actor_id);

  /// Handle detached actor destroy.
  void OnDetachedActorDestroy(const std::string &virtual_cluster_id,
                              const ActorID &actor_id);

  /// Handle detached placement group registration.
  void OnDetachedPlacementGroupRegistration(const std::string &virtual_cluster_id,
                                            const PlacementGroupID &placement_group_id);

  /// Handle detached placement group destroy.
  void OnDetachedPlacementGroupDestroy(const std::string &virtual_cluster_id,
                                       const PlacementGroupID &placement_group_id);

 protected:
  void HandleCreateOrUpdateVirtualCluster(
      rpc::CreateOrUpdateVirtualClusterRequest request,
      rpc::CreateOrUpdateVirtualClusterReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveNodesFromVirtualCluster(
      rpc::RemoveNodesFromVirtualClusterRequest request,
      rpc::RemoveNodesFromVirtualClusterReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemoveVirtualCluster(rpc::RemoveVirtualClusterRequest request,
                                  rpc::RemoveVirtualClusterReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetVirtualClusters(rpc::GetVirtualClustersRequest request,
                                rpc::GetVirtualClustersReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  void HandleCreateJobCluster(rpc::CreateJobClusterRequest request,
                              rpc::CreateJobClusterReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllVirtualClusterInfo(
      rpc::GetAllVirtualClusterInfoRequest request,
      rpc::GetAllVirtualClusterInfoReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  Status VerifyRequest(const rpc::CreateOrUpdateVirtualClusterRequest &request);

  Status VerifyRequest(const rpc::RemoveNodesFromVirtualClusterRequest &request);

  Status VerifyRequest(const rpc::RemoveVirtualClusterRequest &request);

  Status FlushAndPublish(std::shared_ptr<rpc::VirtualClusterTableData> data,
                         CreateOrUpdateVirtualClusterCallback callback);

 private:
  /// io context. This is to ensure thread safety. Ideally, all public
  /// funciton needs to post job to this io_context.
  instrumented_io_context &io_context_;
  /// The storage of the GCS tables.
  GcsTableStorage &gcs_table_storage_;
  /// The publisher of the GCS tables.
  GcsPublisher &gcs_publisher_;

  /// The periodical runner to run `ReplenishAllClusterNodeInstances` task.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  /// The global cluster.
  std::shared_ptr<PrimaryCluster> primary_cluster_;
};

}  // namespace gcs
}  // namespace ray