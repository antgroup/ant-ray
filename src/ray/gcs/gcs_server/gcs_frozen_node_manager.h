#ifndef __GCS_FROZEN_IP_MANAGER_H__
#define __GCS_FROZEN_IP_MANAGER_H__

#include "ray/common/asio/periodical_runner.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class GcsFrozenNodeManager : public rpc::FrozenNodeGcsServiceHandler {
 public:
  GcsFrozenNodeManager(
      instrumented_io_context &io_service,
      std::shared_ptr<GcsTableStorage> gcs_table_storage,
      std::shared_ptr<GcsPubSub> gcs_pub_sub,
      std::shared_ptr<GcsNodeManager> gcs_node_manager,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::shared_ptr<GcsPlacementGroupManagerEx> gcs_placement_group_manager,
      std::function<void(const std::string &, const NodeID &)> freeze_done_listener,
      std::function<void(const std::unordered_set<std::string> &)> unfreeze_done_listener)
      : io_service_(io_service),
        gcs_table_storage_(gcs_table_storage),
        gcs_pub_sub_(gcs_pub_sub),
        gcs_node_manager_(gcs_node_manager),
        gcs_resource_manager_(gcs_resource_manager),
        gcs_placement_group_manager_(gcs_placement_group_manager),
        freeze_done_listener_(freeze_done_listener),
        unfreeze_done_listener_(unfreeze_done_listener) {
    periodical_runner_ = std::make_unique<PeriodicalRunner>(io_service_);
    periodical_runner_->RunFnPeriodically(
        [this] { this->PeriodicallyRescheduleIdlePlacementGroupBundles(); },
        RayConfig::instance().gcs_actor_migration_reschedule_idle_bundle_period_ms(),
        "GcsFrozenNodeManager.idle_bundle_rescheduler");
  }

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  // -----------RPC methods start---------------

  void HandleFreezeNodes(const rpc::FreezeNodesRequest &request,
                         rpc::FreezeNodesReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleUnfreezeNodes(const rpc::UnfreezeNodesRequest &request,
                           rpc::UnfreezeNodesReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllFrozenNodes(const rpc::GetAllFrozenNodesRequest &request,
                               rpc::GetAllFrozenNodesReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportFreezeDone(const rpc::ReportFreezeDoneRequest &request,
                              rpc::ReportFreezeDoneReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  // -----------RPC methods end---------------

  void FreezeNode(const std::string &node_name);

  bool IsNodeFrozen(const std::string &node_name);

  bool IsNodeFrozen(const NodeID &node_id);

 private:
  void FlushAndPublishFrozenNodeSet(EmptyCallback flush_done_callback);

  // Check whether there are idle PG bundles, and reschedule them to other nodes.
  void PeriodicallyRescheduleIdlePlacementGroupBundles();

  instrumented_io_context &io_service_;
  std::unique_ptr<PeriodicalRunner> periodical_runner_;
  std::shared_ptr<GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<GcsPubSub> gcs_pub_sub_;
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsPlacementGroupManagerEx> gcs_placement_group_manager_;
  std::unordered_set<std::string> frozen_node_set_;
  std::function<void(const std::string &, const NodeID &)> freeze_done_listener_;
  std::function<void(const std::unordered_set<std::string> &)> unfreeze_done_listener_;
};

}  // namespace gcs
}  // namespace ray

#endif  // __GCS_FROZEN_IP_MANAGER_H__