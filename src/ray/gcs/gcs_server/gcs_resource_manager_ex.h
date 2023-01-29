#pragma once
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

const absl::flat_hash_set<std::string> kStandardResourceLabels{
    kCPU_ResourceLabel, kGPU_ResourceLabel, kMEM_ResourceLabel, kMemory_ResourceLabel};
class GcsNodegroupManagerInterface;
class GcsResourceManagerEx : public GcsResourceManager {
 public:
  /// \param main_io_service The main event loop.
  /// \param gcs_pub_sub GCS message publisher.
  /// \param gcs_table_storage GCS table external storage accessor.
  explicit GcsResourceManagerEx(
      instrumented_io_context &main_io_service,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      bool redis_broadcast_enabled,
      std::shared_ptr<GcsNodegroupManagerInterface> nodegroup_manager,
      std::function<const std::string &(const JobID &job_id)> get_nodegroup_id = nullptr,
      std::function<
          absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>()>
          get_pending_resource_demands = nullptr,
      std::function<bool(const NodeID &node_id)> has_any_drivers_in_node = nullptr);

  virtual ~GcsResourceManagerEx() = default;

  /// Handle update resource rpc request.
  void HandleUpdateResources(const rpc::UpdateResourcesRequest &request,
                             rpc::UpdateResourcesReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  // Deprecated.
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

  // Update node realtime resources.
  void UpdateNodeRealtimeResources(const NodeID &node_id,
                                   const rpc::ResourcesData &heartbeat) override;

  // Deprecated.
  Status GetResourcesOfAllNodegroups(
      const rpc::GetResourcesOfAllNodegroupsRequest &request,
      rpc::GetResourcesOfAllNodegroupsReply *reply);

  Status GetLayeredResourcesOfAllNodegroups(
      const rpc::GetLayeredResourcesOfAllNodegroupsRequest &request,
      rpc::GetLayeredResourcesOfAllNodegroupsReply *reply);

  Status GetPendingResourcesOfAllNodegroups(
      const rpc::GetPendingResourcesOfAllNodegroupsRequest &request,
      rpc::GetPendingResourcesOfAllNodegroupsReply *reply);

  std::string ToString() const override;

 private:
  int64_t latest_resources_normal_task_timestamp_;
  std::shared_ptr<GcsNodegroupManagerInterface> nodegroup_manager_;
  std::function<const std::string &(const JobID &job_id)> get_nodegroup_id_;
  std::function<absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>()>
      get_pending_resource_demands_;
  std::function<bool(const NodeID &node_id)> has_any_drivers_in_node_;
};

}  // namespace gcs
}  // namespace ray
