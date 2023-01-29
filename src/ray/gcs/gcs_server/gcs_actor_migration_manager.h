#pragma once

#include <cstdint>

#include "gcs_table_storage.h"
#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_frozen_node_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"

namespace ray {
namespace gcs {

class GcsActorMigrationManager : public rpc::ActorMigrationGcsServiceHandler {
 public:
  GcsActorMigrationManager(
      instrumented_io_context &io_service, std::shared_ptr<GcsPubSub> gcs_pub_sub,
      std::shared_ptr<GcsFrozenNodeManager> gcs_frozen_node_manager,
      std::shared_ptr<GcsActorManager> gcs_actor_manager,
      std::shared_ptr<GcsNodeManager> gcs_node_manager,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager,
      std::shared_ptr<GcsJobManager> gcs_job_manager)
      : io_service_(io_service),
        gcs_pub_sub_(gcs_pub_sub),
        gcs_frozen_node_manager_(gcs_frozen_node_manager),
        gcs_actor_manager_(gcs_actor_manager),
        gcs_node_manager_(gcs_node_manager),
        gcs_resource_manager_(gcs_resource_manager),
        gcs_placement_group_manager_(gcs_placement_group_manager),
        gcs_job_manager_(gcs_job_manager) {
    periodical_runner_ = std::make_unique<PeriodicalRunner>(io_service_);
    periodical_runner_->RunFnPeriodically(
        [this] { this->PeriodicallyCheckInFlightActorsAndPG(); },
        RayConfig::instance().gcs_actor_migration_check_in_flight_period_ms(),
        "migration_inflight_checker");
  }

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data) {}
  // -----------RPC methods start---------------

  void HandleMigrateActorsInNode(const rpc::MigrateActorsInNodeRequest &request,
                                 rpc::MigrateActorsInNodeReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void HandleCheckIfMigrationIsComplete(
      const rpc::CheckIfMigrationIsCompleteRequest &request,
      rpc::CheckIfMigrationIsCompleteReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  // -----------RPC methods end---------------

 protected:
  Status MigrateActorsInNode(const std::string &node_name, int64_t migration_id);

  void PeriodicallyCheckInFlightActorsAndPG();

  std::vector<std::weak_ptr<GcsActor>> FilterInflightActors(
      const NodeID &node_id, const std::vector<std::weak_ptr<GcsActor>> &actors);

  void UpdateReadyNode(int64_t migration_id, const NodeID &node_id, bool is_node_dead);

  virtual void PubMigrationNotification(const std::vector<NodeID> &node_id_list,
                                        int64_t migration_id);

  virtual bool IsNodeMigrationComplete(const std::string &node_name) const;

  virtual void FreezeNode(const std::string &node_name);

  virtual absl::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetAliveNode(
      const NodeID &node_id) const;

  virtual boost::optional<std::shared_ptr<rpc::GcsNodeInfo>> GetUniqueNodeByName(
      const std::string &node_name) const;

  virtual bool IsPlacementGroupSchedulingInProgress() const;

  virtual bool HasAnyDriversInNode(const NodeID &node_id) const;

  virtual std::vector<std::weak_ptr<GcsActor>> GetAllActorsInNode(
      const NodeID &node_id) const;

  virtual const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const;

  struct MigrationInfo {
    int64_t migration_id_ = 0;
    std::vector<std::weak_ptr<GcsActor>> inflight_actors_;
  };

  struct MigrationNodesInfo {
    int number_of_nodes_with_inflight_tasks_ = 0;
    // Nodes ready to be migrated.
    // When the node has no flying task, it is considered that the node can start
    // migration.
    std::vector<NodeID> ready_nodes_;
  };

  instrumented_io_context &io_service_;
  std::shared_ptr<GcsPubSub> gcs_pub_sub_;
  std::shared_ptr<GcsFrozenNodeManager> gcs_frozen_node_manager_;
  std::shared_ptr<GcsActorManager> gcs_actor_manager_;
  absl::flat_hash_map<NodeID, MigrationInfo> node_id_to_migration_info_;
  absl::flat_hash_map<int64_t, MigrationNodesInfo> migration_id_to_nodes_info_;
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::shared_ptr<GcsJobManager> gcs_job_manager_;
  std::unique_ptr<PeriodicalRunner> periodical_runner_;

  FRIEND_TEST(GcsActorMigrationTest, TestInvalidNode);
  FRIEND_TEST(GcsActorMigrationTest, TestMigrateActorsInNode);
  FRIEND_TEST(GcsActorMigrationTest, TestMigrationNodeDead);
  FRIEND_TEST(GcsActorMigrationTest, TestMigrationTwoNodeDead);
  FRIEND_TEST(GcsActorMigrationTest, TestMigrationSimpleDuplicateNodes);
  FRIEND_TEST(GcsActorMigrationTest, TestMigrationDuplicateNodes);
  FRIEND_TEST(GcsActorMigrationTest, TestGCInvalidMigration);
};
}  // namespace gcs
}  // namespace ray