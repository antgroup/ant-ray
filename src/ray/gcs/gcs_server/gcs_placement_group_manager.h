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
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

/// GcsPlacementGroup just wraps `PlacementGroupTableData` and provides some convenient
/// interfaces to access the fields inside `PlacementGroupTableData`. This class is not
/// thread-safe.
class GcsPlacementGroup {
 public:
  /// Create a GcsPlacementGroup by placement_group_table_data.
  ///
  /// \param placement_group_table_data Data of the placement_group (see gcs.proto).
  explicit GcsPlacementGroup(rpc::PlacementGroupTableData placement_group_table_data)
      : placement_group_table_data_(std::move(placement_group_table_data)) {}

  /// Create a GcsPlacementGroup by CreatePlacementGroupRequest.
  ///
  /// \param request Contains the placement group creation task specification.
  explicit GcsPlacementGroup(const ray::rpc::CreatePlacementGroupRequest &request,
                             std::string ray_namespace) {
    const auto &placement_group_spec = request.placement_group_spec();
    placement_group_table_data_.set_placement_group_id(
        placement_group_spec.placement_group_id());
    placement_group_table_data_.set_name(placement_group_spec.name());
    placement_group_table_data_.set_state(rpc::PlacementGroupTableData::PENDING);
    placement_group_table_data_.mutable_bundles()->CopyFrom(
        placement_group_spec.bundles());
    placement_group_table_data_.set_strategy(placement_group_spec.strategy());
    placement_group_table_data_.set_creator_job_id(placement_group_spec.creator_job_id());
    placement_group_table_data_.set_creator_actor_id(
        placement_group_spec.creator_actor_id());
    placement_group_table_data_.set_creator_job_dead(
        placement_group_spec.creator_job_dead());
    placement_group_table_data_.set_creator_actor_dead(
        placement_group_spec.creator_actor_dead());
    placement_group_table_data_.set_is_detached(placement_group_spec.is_detached());
    placement_group_table_data_.set_ray_namespace(ray_namespace);
  }

  /// Get the immutable PlacementGroupTableData of this placement group.
  const rpc::PlacementGroupTableData &GetPlacementGroupTableData() const;

  /// Get the mutable bundle of this placement group.
  rpc::Bundle *GetMutableBundle(int bundle_index);

  /// Get the bundle of this placement group.
  rpc::Bundle GetBundle(int bundle_index);

  /// Update the state of this placement_group.
  void UpdateState(rpc::PlacementGroupTableData::PlacementGroupState state);

  /// Get the state of this gcs placement_group.
  rpc::PlacementGroupTableData::PlacementGroupState GetState() const;

  /// Get the id of this placement_group.
  PlacementGroupID GetPlacementGroupID() const;

  /// Get the name of this placement_group.
  std::string GetName() const;

  /// Get the name of this placement_group.
  std::string GetRayNamespace() const;

  /// Get the bundles of this placement_group (including unplaced).
  std::vector<std::shared_ptr<BundleSpecification>> GetBundles() const;

  /// Get all the exist bundle indexes of this placement_group.
  std::vector<int> GetAllBundleIndexes() const;

  /// Add a set of new bundles to this placement_group.
  void AddBundles(const ray::rpc::AddPlacementGroupBundlesRequest &request);

  /// Remove a set of old bundles from this placement_group.
  void RemoveBundles(const std::vector<int> &bundle_indexes);

  /// Remove node info associated with a set of bundles.
  /// This will happen when removing bundles.
  void RemoveNodeInfoFromBundles(const std::vector<int64_t> &bundle_indexes);

  /// Get the unplaced bundles of this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetUnplacedBundles() const;

  /// Check if there are unplaced bundles.
  bool HasUnplacedBundles() const;

  /// Get all bundles that have been removed of this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetRemovedBundles() const;

  /// Get all bundles that have been created of this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetCreatedBundles() const;

  /// Get all bundles that have been marked `valid` of this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetAllValidBundles() const;

  /// Get all bundles that have been marked `invalid` of this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetAllInvalidBundles() const;

  /// Get all bundles that are being removed from this placement group.
  std::vector<std::shared_ptr<BundleSpecification>> GetAllRemovingBundles() const;

  /// Get the Strategy
  rpc::PlacementStrategy GetStrategy() const;

  /// Get debug string for the placement group.
  std::string DebugString() const;

  /// Below fields are used for automatic cleanup of placement groups.

  /// Get the actor id that created the placement group.
  const ActorID GetCreatorActorId() const;

  /// Get the job id that created the placement group.
  const JobID GetCreatorJobId() const;

  /// Mark that the creator job of this placement group is dead.
  void MarkCreatorJobDead();

  /// Mark that the creator actor of this placement group is dead.
  void MarkCreatorActorDead();

  /// Return True if the placement group lifetime is done. False otherwise.
  bool IsPlacementGroupLifetimeDone() const;

  /// Returns whether or not this is a detached placement group.
  bool IsDetached() const;

  /// Get the creation time of current placement group.
  ///
  /// \return The creation time of `Nanosecond`.
  int64_t CreationTime() const { return creation_time_; };

  /// Mark the placement group creation start.
  const void MarkCreationStart();

  /// ANT-INTERNAL.
  /// A helper field to log the debug info when reschedule occur.
  uint64_t last_log_reschedule_debug_info_time_ms_ = 0;

  /// Get the start scheduling time(ms) of current placement group.
  uint64_t GetLastStartSchedulingTimeMs() const { return last_start_scheduling_time_ms_; }

  /// Mark the placement group start scheduling.
  void MarkSchedulingStarted();

 private:
  /// The placement_group meta data which contains the task specification as well as the
  /// state of the gcs placement_group and so on (see gcs.proto).
  rpc::PlacementGroupTableData placement_group_table_data_;
  int64_t creation_time_;

  /// ANT-INTERNAL.
  /// The start time of the latest schedule.
  uint64_t last_start_scheduling_time_ms_ = 0;
};

/// GcsPlacementGroupManager is responsible for managing the lifecycle of all placement
/// group. This class is not thread-safe.
/// The placementGroup will be added into queue and set the status as pending first and
/// use SchedulePendingPlacementGroups(). The SchedulePendingPlacementGroups() will get
/// the head of the queue and schedule it. If schedule success, using the
/// SchedulePendingPlacementGroups() Immediately. Else wait for a short time before using
/// SchedulePendingPlacementGroups() next time.
class GcsPlacementGroupManager : public rpc::PlacementGroupInfoHandler {
 public:
  /// Create a GcsPlacementGroupManager
  ///
  /// \param io_context The event loop to run the monitor on.
  /// \param scheduler Used to schedule placement group creation tasks.
  /// \param gcs_table_storage Used to flush placement group data to storage.
  /// \param gcs_resource_manager Reference of GcsResourceManager.
  explicit GcsPlacementGroupManager(
      instrumented_io_context &io_context,
      std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      GcsResourceManager &gcs_resource_manager,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::function<std::string(const JobID &)> get_ray_namespace);

  ~GcsPlacementGroupManager() = default;

  void HandleCreatePlacementGroup(const rpc::CreatePlacementGroupRequest &request,
                                  rpc::CreatePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddPlacementGroupBundles(
      const rpc::AddPlacementGroupBundlesRequest &request,
      rpc::AddPlacementGroupBundlesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemovePlacementGroupBundles(
      const rpc::RemovePlacementGroupBundlesRequest &request,
      rpc::RemovePlacementGroupBundlesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandleRemovePlacementGroup(const rpc::RemovePlacementGroupRequest &request,
                                  rpc::RemovePlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetPlacementGroup(const rpc::GetPlacementGroupRequest &request,
                               rpc::GetPlacementGroupReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedPlacementGroup(const rpc::GetNamedPlacementGroupRequest &request,
                                    rpc::GetNamedPlacementGroupReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllPlacementGroup(const rpc::GetAllPlacementGroupRequest &request,
                                  rpc::GetAllPlacementGroupReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) override;
  void HandleWaitPlacementGroupUntilReady(
      const rpc::WaitPlacementGroupUntilReadyRequest &request,
      rpc::WaitPlacementGroupUntilReadyReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Add some new bundles for the specific placement group.
  ///
  /// \param placement_group_id The placement group id whose bundles will be added.
  /// \param request The rpc request including the new bundles will be added.
  /// \param callback Will be invoked after the `bundles` field of the specific placement
  /// group has been modified successfully or be invoked immediately if the state of the
  /// placement group is `Pending` or `REMOVED`.
  void AddBundlesForPlacementGroup(
      const PlacementGroupID &placement_group_id,
      const ray::rpc::AddPlacementGroupBundlesRequest &request, StatusCallback callback);

  /// Remove exist bundles from the specific placement group.
  ///
  /// \param placement_group_id The placement group id whose bundles will be removed.
  /// \param bundle_indexes The index of the bundles to be removed.
  /// \param callback Will be invoked after the `bundles` field of the specific placement
  /// group has been modified successfully.
  void RemoveBundlesFromPlacementGroup(const PlacementGroupID &placement_group_id,
                                       const std::vector<int> &bundle_indexes,
                                       StatusCallback callback);

  /// Register a callback which will be invoked after successfully created.
  ///
  /// \param placement_group_id The placement group id which we want to listen.
  /// \param callback Will be invoked after the placement group is created successfully or
  /// be invoked if the placement group is deleted before create successfully.
  void WaitPlacementGroup(const PlacementGroupID &placement_group_id,
                          StatusCallback callback);

  /// Register placement_group asynchronously.
  ///
  /// \param placement_group The placement group to be created.
  /// \param callback Will be invoked after the placement_group is created successfully or
  /// be invoked immediately if the placement_group is already registered to
  /// `registered_placement_groups_` and its state is `CREATED`. The callback will not be
  /// called in this case.
  void RegisterPlacementGroup(const std::shared_ptr<GcsPlacementGroup> &placement_group,
                              StatusCallback callback);

  /// Schedule placement_groups in the `pending_placement_groups_` queue.
  void SchedulePendingPlacementGroups();

  /// Get the placement_group ID for the named placement_group. Returns nil if the
  /// placement_group was not found.
  /// \param name The name of the  placement_group to look up.
  /// \returns PlacementGroupID The ID of the placement_group. Nil if the
  /// placement_group was not found.
  PlacementGroupID GetPlacementGroupIDByName(const std::string &name,
                                             const std::string &ray_namespace);

  /// Handle placement_group creation task failure. This should be called when scheduling
  /// an placement_group creation task is infeasible.
  ///
  /// \param placement_group The placement_group whose creation task is infeasible.
  virtual void OnPlacementGroupCreationFailed(
      std::shared_ptr<GcsPlacementGroup> placement_group);

  /// Handle placement_group creation task success. This should be called when the
  /// placement_group creation task has been scheduled successfully.
  ///
  /// \param placement_group The placement_group that has been created.
  virtual void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<GcsPlacementGroup> &placement_group);

  /// Try to return bundle resources to gcs resource manager,
  /// it will add the removing bundle to `waiting_removed_bundles_` and
  /// wait to be returned in the future if try failed.
  void TryReturnBundleResources(const std::shared_ptr<BundleSpecification> &bundle);

  // Callback that will be invoked when receiving cluster resource changed.
  void OnClusterResourcesChanged();

  // Try to release bundle resource to gcs resource manager and job context.
  bool TryReleasingBundleResources(const std::shared_ptr<BundleSpecification> &bundle,
                                   bool release_job_resources = true);

  /// TODO-SANG Fill it up.
  void RemovePlacementGroup(const PlacementGroupID &placement_group_id,
                            StatusCallback on_placement_group_removed);

  /// Handle a node death. This will reschedule all bundles associated with the
  /// specified node id.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  /// Clean placement group that belongs to the job id if necessary.
  ///
  /// This interface is a part of automatic lifecycle management for placement groups.
  /// When a job is killed, this method should be invoked to clean up
  /// placement groups that belong to the given job.
  ///
  /// Calling this method doesn't mean placement groups that belong to the given job
  /// will be cleaned. Placement groups are cleaned only when the creator job AND actor
  /// are both dead.
  ///
  /// NOTE: This method is idempotent.
  ///
  /// \param job_id The job id where placement groups that need to be cleaned belong to.
  void CleanPlacementGroupIfNeededWhenJobDead(const JobID &job_id);

  /// ANT-INTERNAL.
  /// Check whether the placement group's lefetime is done or not.
  bool IsPlacementGroupLifetimeDone(const PlacementGroupID &placement_group_id);

  /// Collect stats from gcs placement group manager in-memory data structures.
  void CollectStats() const;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Publish all bundles information.
  /// Note: We should publish all bundles of this placement group every time to prevent
  /// publish message loss.
  ///
  /// \param placement_group.
  void PublishBundlesInfo(const std::shared_ptr<GcsPlacementGroup> &placement_group);

  /// Check if there's any placement group scheduling going on.
  bool IsSchedulingInProgress() const {
    return scheduling_in_progress_id_ != PlacementGroupID::Nil();
  }

  std::vector<std::shared_ptr<GcsPlacementGroup>> GetAllPlacementGroupsInNode(
      const NodeID &node_id);

  const BundleLocationIndex &GetCommittedBundleLocationIndex() const;

  std::string DebugString() const;

 protected:
  virtual Status ReserveJobResources(
      const PlacementGroupID &placement_group_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
    return Status::OK();
  }

  // Return bundle resource to job context.
  virtual void ReturnJobResources(
      const JobID &job_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {}

  // Subtract job's acquired resources.
  virtual void SubtractJobAcquiredResources(
      const JobID &job_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {}

  // Return bundle resource to gcs resource manager.
  virtual void ReturnBundleResources(
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {}

  const absl::flat_hash_map<PlacementGroupID, std::shared_ptr<GcsPlacementGroup>>
      &GetRegisteredPlacementGroups() const {
    return registered_placement_groups_;
  }

  /// Check if this placement group is waiting for scheduling.
  bool IsPlacmentGroupIDInPendingQueue(const PlacementGroupID &placement_group_id) const;

  /// Reschedule this placement group if it still has unplaced bundles.
  bool RescheduleIfStillHasUnplacedBundles(const PlacementGroupID &placement_group_id);

  /// All registered placement_groups (pending placement_groups are also included).
  absl::flat_hash_map<PlacementGroupID, std::shared_ptr<GcsPlacementGroup>>
      registered_placement_groups_;

  /// The pending placement_groups which will not be scheduled until there's a resource
  /// change.
  /// NOTE: When we remove placement group, we need to look for
  /// `pending_placement_groups_` and delete the specific placement group, so we can't use
  /// `std::priority_queue`.
  std::deque<std::shared_ptr<GcsPlacementGroup>> pending_placement_groups_;

 private:
  /// Try to create placement group after a short time.
  void RetryCreatingPlacementGroup();

  /// Mark the manager that there's a placement group scheduling going on.
  void MarkSchedulingStarted(const std::shared_ptr<GcsPlacementGroup> &placement_group) {
    placement_group->MarkSchedulingStarted();
    scheduling_in_progress_id_ = placement_group->GetPlacementGroupID();
  }

  /// Mark the manager that there's no more placement group scheduling going on.
  void MarkSchedulingDone() { scheduling_in_progress_id_ = PlacementGroupID::Nil(); }

  /// Check if the placement group of a given id is scheduling.
  bool IsSchedulingInProgress(const PlacementGroupID &placement_group_id) const {
    return scheduling_in_progress_id_ == placement_group_id;
  }

  // Method that is invoked every second.
  void Tick();

  // Update placement group load information so that the autoscaler can use it.
  void UpdatePlacementGroupLoad();

  // Detect whether the placement group schedules slow.
  void DetectPlacementGroupSchedulingSlow();

  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  instrumented_io_context &io_context_;

  /// Callbacks of pending `RegisterPlacementGroup` requests.
  /// Maps placement group ID to placement group registration callbacks, which is used to
  /// filter duplicated messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<PlacementGroupID, std::vector<StatusCallback>>
      placement_group_to_register_callbacks_;

  /// Callback of `WaitPlacementGroupUntilReady` requests.
  absl::flat_hash_map<PlacementGroupID, std::vector<StatusCallback>>
      placement_group_to_create_callbacks_;

 protected:
  /// The scheduler to schedule all registered placement_groups.
  std::shared_ptr<gcs::GcsPlacementGroupSchedulerInterface>
      gcs_placement_group_scheduler_;

 private:
  /// Used to update placement group information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;

  /// The placement group id that is in progress of scheduling bundles.
  /// TODO(sang): Currently, only one placement group can be scheduled at a time.
  /// We should probably support concurrent creation (or batching).
  PlacementGroupID scheduling_in_progress_id_ = PlacementGroupID::Nil();

 protected:
  /// Reference of GcsResourceManager.
  GcsResourceManager &gcs_resource_manager_;

 private:
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;

  /// Get ray namespace.
  std::function<std::string(const JobID &)> get_ray_namespace_;

  /// Maps placement group names to their placement group ID for lookups by
  /// name, first keyed by namespace.
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, PlacementGroupID>>
      named_placement_groups_;

  /// The bundles that waiting to be destroyed and release resources.
  std::list<std::shared_ptr<BundleSpecification>> waiting_removed_bundles_;

  // Debug info.
  enum CountType {
    CREATE_PLACEMENT_GROUP_REQUEST = 0,
    REMOVE_PLACEMENT_GROUP_REQUEST = 1,
    GET_PLACEMENT_GROUP_REQUEST = 2,
    GET_ALL_PLACEMENT_GROUP_REQUEST = 3,
    WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST = 4,
    GET_NAMED_PLACEMENT_GROUP_REQUEST = 5,
    ADD_PLACEMENT_GROUP_BUNDLES_REQUEST = 6,
    REMOVE_PLACEMENT_GROUP_BUNDLES_REQUEST = 7,
    CountType_MAX = 8,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  PeriodicalRunner periodical_runner_;
};

}  // namespace gcs
}  // namespace ray
