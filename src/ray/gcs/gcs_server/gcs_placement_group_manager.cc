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

#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

void GcsPlacementGroup::UpdateState(
    rpc::PlacementGroupTableData::PlacementGroupState state) {
  placement_group_table_data_.set_state(state);
}

rpc::PlacementGroupTableData::PlacementGroupState GcsPlacementGroup::GetState() const {
  return placement_group_table_data_.state();
}

PlacementGroupID GcsPlacementGroup::GetPlacementGroupID() const {
  return PlacementGroupID::FromBinary(placement_group_table_data_.placement_group_id());
}

std::string GcsPlacementGroup::GetName() const {
  return placement_group_table_data_.name();
}

std::string GcsPlacementGroup::GetRayNamespace() const {
  return placement_group_table_data_.ray_namespace();
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetBundles() const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> ret_bundles;
  for (const auto &bundle : bundles) {
    ret_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
  }
  return ret_bundles;
}

std::vector<int> GcsPlacementGroup::GetAllBundleIndexes() const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<int> ret_bundle_indexes;
  for (auto &bundle : bundles) {
    ret_bundle_indexes.push_back(bundle.bundle_id().bundle_index());
  }
  return ret_bundle_indexes;
}

void GcsPlacementGroup::AddBundles(
    const ray::rpc::AddPlacementGroupBundlesRequest &request) {
  for (int i = GetBundles().size(), j = 0; j < request.bundles_size(); i++, j++) {
    auto message_bundle = placement_group_table_data_.add_bundles();
    auto resource = MapFromProtobuf(request.bundles(j).unit_resources());
    auto new_bundle = BuildBundle(resource, i, GetPlacementGroupID());
    *message_bundle = std::move(new_bundle);
  }
}

void GcsPlacementGroup::RemoveBundles(const std::vector<int> &bundle_indexes) {
  for (auto &index : bundle_indexes) {
    auto bundle = placement_group_table_data_.mutable_bundles(index);
    RAY_CHECK(bundle);
    bundle->set_is_valid(false);
  }
}

void GcsPlacementGroup::RemoveNodeInfoFromBundles(
    const std::vector<int64_t> &bundle_indexes) {
  for (auto &index : bundle_indexes) {
    auto bundle = placement_group_table_data_.mutable_bundles(index);
    RAY_CHECK(bundle);
    bundle->clear_node_id();
  }
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetUnplacedBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> unplaced_bundles;
  for (auto &bundle : bundles) {
    if (bundle.is_valid() && NodeID::FromBinary(bundle.node_id()).IsNil()) {
      unplaced_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return unplaced_bundles;
}

bool GcsPlacementGroup::HasUnplacedBundles() const {
  return !GetUnplacedBundles().empty();
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetRemovedBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> removed_bundles;
  for (auto &bundle : bundles) {
    if (!bundle.is_valid() && NodeID::FromBinary(bundle.node_id()).IsNil()) {
      removed_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return removed_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetCreatedBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> created_bundles;
  for (auto &bundle : bundles) {
    if (bundle.is_valid() && !NodeID::FromBinary(bundle.node_id()).IsNil()) {
      created_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return created_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>> GcsPlacementGroup::GetAllValidBundles()
    const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> valid_bundles;
  for (auto &bundle : bundles) {
    if (bundle.is_valid()) {
      valid_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return valid_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>>
GcsPlacementGroup::GetAllInvalidBundles() const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> invalid_bundles;
  for (auto &bundle : bundles) {
    if (!bundle.is_valid()) {
      invalid_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return invalid_bundles;
}

std::vector<std::shared_ptr<BundleSpecification>>
GcsPlacementGroup::GetAllRemovingBundles() const {
  const auto &bundles = placement_group_table_data_.bundles();
  std::vector<std::shared_ptr<BundleSpecification>> removing_bundles;
  for (auto &bundle : bundles) {
    if (!bundle.is_valid() && !NodeID::FromBinary(bundle.node_id()).IsNil()) {
      removing_bundles.push_back(std::make_shared<BundleSpecification>(bundle));
    }
  }
  return removing_bundles;
}

rpc::PlacementStrategy GcsPlacementGroup::GetStrategy() const {
  return placement_group_table_data_.strategy();
}

const rpc::PlacementGroupTableData &GcsPlacementGroup::GetPlacementGroupTableData()
    const {
  return placement_group_table_data_;
}

std::string GcsPlacementGroup::DebugString() const {
  std::stringstream stream;
  stream << "placement group id = " << GetPlacementGroupID() << ", name = " << GetName()
         << ", strategy = " << GetStrategy();
  return stream.str();
}

rpc::Bundle *GcsPlacementGroup::GetMutableBundle(int bundle_index) {
  return placement_group_table_data_.mutable_bundles(bundle_index);
}

rpc::Bundle GcsPlacementGroup::GetBundle(int bundle_index) {
  return placement_group_table_data_.bundles(bundle_index);
}

const ActorID GcsPlacementGroup::GetCreatorActorId() const {
  return ActorID::FromBinary(placement_group_table_data_.creator_actor_id());
}

const JobID GcsPlacementGroup::GetCreatorJobId() const {
  return JobID::FromBinary(placement_group_table_data_.creator_job_id());
}

void GcsPlacementGroup::MarkCreatorJobDead() {
  placement_group_table_data_.set_creator_job_dead(true);
}

void GcsPlacementGroup::MarkCreatorActorDead() {
  placement_group_table_data_.set_creator_actor_dead(true);
}

bool GcsPlacementGroup::IsPlacementGroupLifetimeDone() const {
  // ANT-INTERNAL.
  // NOTE: The semantics of the `Detached` is that the placement group
  // will not be destroyed when its owner actor dead but will be destroyed
  // when the job is dead.
  // TODO(loushang.ls): We only support the `Detached` placement group in Ant internal.
  return placement_group_table_data_.creator_job_dead();
}

const void GcsPlacementGroup::MarkCreationStart() {
  auto creation_time = absl::GetCurrentTimeNanos() / 1000;
  creation_time_ = creation_time;
}

bool GcsPlacementGroup::IsDetached() const {
  return placement_group_table_data_.is_detached();
}

void GcsPlacementGroup::MarkSchedulingStarted() {
  last_start_scheduling_time_ms_ = current_time_ms();
}

/////////////////////////////////////////////////////////////////////////////////////////

GcsPlacementGroupManager::GcsPlacementGroupManager(
    instrumented_io_context &io_context,
    std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    GcsResourceManager &gcs_resource_manager, std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
    std::function<std::string(const JobID &)> get_ray_namespace)
    : io_context_(io_context),
      gcs_placement_group_scheduler_(std::move(scheduler)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_resource_manager_(gcs_resource_manager),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      get_ray_namespace_(get_ray_namespace),
      periodical_runner_(io_context) {
  Tick();
  periodical_runner_.RunFnPeriodically(
      [this] { DetectPlacementGroupSchedulingSlow(); },
      RayConfig::instance().placement_group_detect_scheduling_slow_interval_time_s() *
          1000);
}

void GcsPlacementGroupManager::RegisterPlacementGroup(
    const std::shared_ptr<GcsPlacementGroup> &placement_group, StatusCallback callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to register placement group
  // successfully.
  RAY_CHECK(callback);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  auto iter = registered_placement_groups_.find(placement_group_id);
  if (iter != registered_placement_groups_.end()) {
    auto pending_register_iter =
        placement_group_to_register_callbacks_.find(placement_group_id);
    if (pending_register_iter != placement_group_to_register_callbacks_.end()) {
      // 1. The GCS client sends the `RegisterPlacementGroup` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterPlacementGroup` request to the GCS server.
      pending_register_iter->second.emplace_back(std::move(callback));
    } else {
      // 1. The GCS client sends the `RegisterPlacementGroup` request to the GCS server.
      // 2. The GCS server flushes the placement group to the storage and restarts before
      // replying to the GCS client.
      // 3. The GCS client resends the `RegisterPlacementGroup` request to the GCS server.
      RAY_LOG(INFO) << "Placement group " << placement_group_id
                    << " is already registered.";
      callback(Status::OK());
    }
    return;
  }

  const auto &reserve_resource_res = ReserveJobResources(
      placement_group->GetPlacementGroupID(), placement_group->GetUnplacedBundles());
  if (!reserve_resource_res.ok()) {
    callback(reserve_resource_res);
    return;
  }

  if (!placement_group->GetName().empty()) {
    auto &pgs_in_namespace = named_placement_groups_[placement_group->GetRayNamespace()];
    auto it = pgs_in_namespace.find(placement_group->GetName());
    if (it == pgs_in_namespace.end()) {
      pgs_in_namespace.emplace(placement_group->GetName(),
                               placement_group->GetPlacementGroupID());
    } else {
      std::stringstream stream;
      stream << "Failed to create placement group '"
             << placement_group->GetPlacementGroupID() << "' because name '"
             << placement_group->GetName() << "' already exists.";
      RAY_LOG(WARNING) << stream.str();
      callback(Status::Invalid(stream.str()));
      return;
    }
  }

  placement_group_to_register_callbacks_[placement_group->GetPlacementGroupID()]
      .emplace_back(std::move(callback));
  registered_placement_groups_.emplace(placement_group->GetPlacementGroupID(),
                                       placement_group);
  pending_placement_groups_.emplace_back(placement_group);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id, placement_group](Status status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        if (registered_placement_groups_.contains(placement_group_id)) {
          auto iter = placement_group_to_register_callbacks_.find(placement_group_id);
          auto callbacks = std::move(iter->second);
          placement_group_to_register_callbacks_.erase(iter);
          for (const auto &callback : callbacks) {
            callback(status);
          }
          SchedulePendingPlacementGroups();
        } else {
          // The placement group registration is synchronous, so if we found the placement
          // group was deleted here, it must be triggered by the abnormal exit of job,
          // we will return directly in this case.
          RAY_CHECK(placement_group_to_register_callbacks_.count(placement_group_id) == 0)
              << "The placement group has been removed unexpectedly with an unknown "
                 "error. Please file a bug report on here: "
                 "https://github.com/ray-project/ray/issues";
          RAY_LOG(WARNING) << "Failed to create placement group '"
                           << placement_group->GetPlacementGroupID()
                           << "', because the placement group has been removed by GCS.";
          return;
        }
      }));
}

PlacementGroupID GcsPlacementGroupManager::GetPlacementGroupIDByName(
    const std::string &name, const std::string &ray_namespace) {
  PlacementGroupID placement_group_id = PlacementGroupID::Nil();
  auto namespace_it = named_placement_groups_.find(ray_namespace);
  if (namespace_it != named_placement_groups_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      placement_group_id = it->second;
    }
  }
  return placement_group_id;
}

void GcsPlacementGroupManager::OnPlacementGroupCreationFailed(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  // We will log detailed failed info in `Scheduler::WarnPlacementGroupSchedulingFailure`.
  RAY_LOG(DEBUG) << "Failed to create placement group " << placement_group->GetName()
                 << ", id: " << placement_group->GetPlacementGroupID() << ", try again.";
  // We will attempt to schedule this placement_group once an eligible node is
  // registered.
  auto state = placement_group->GetState();
  RAY_CHECK(state == rpc::PlacementGroupTableData::RESCHEDULING ||
            state == rpc::PlacementGroupTableData::PENDING ||
            state == rpc::PlacementGroupTableData::REMOVED ||
            state == rpc::PlacementGroupTableData::UPDATING)
      << "State: " << state;
  if (state == rpc::PlacementGroupTableData::RESCHEDULING) {
    // NOTE: If a node is dead, the placement group scheduler should try to recover the
    // group by rescheduling the bundles of the dead node. This should have higher
    // priority than trying to place other placement groups.
    pending_placement_groups_.emplace_front(std::move(placement_group));
  } else {
    pending_placement_groups_.emplace_back(std::move(placement_group));
  }

  MarkSchedulingDone();
  RetryCreatingPlacementGroup();
}

void GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  std::ostringstream success_message;
  success_message << "Successfully schedule placement group "
                  << placement_group->GetName()
                  << ", id: " << placement_group->GetPlacementGroupID();
  RAY_LOG(INFO) << success_message.str();
  RAY_EVENT(INFO, EVENT_LABEL_PLACEMENT_GROUP_SCHEDULE_SUCCESS)
          .WithField("job_id", placement_group->GetCreatorJobId().Hex())
      << success_message.str();

  {  // Recore the metric.
    auto end_time = absl::GetCurrentTimeNanos() / 1000;
    const ray::stats::TagsType pg_id_tag = {
        {ray::stats::PlacementGroupIdKey, placement_group->GetPlacementGroupID().Hex()}};
    ray::stats::PGCreationElapsedTime().Record(end_time - placement_group->CreationTime(),
                                               pg_id_tag);
    ray::stats::PGCreationCount().Record(1);
  }
  // Update state.
  placement_group->UpdateState(rpc::PlacementGroupTableData::CREATED);

  if (!placement_group->GetAllRemovingBundles().empty()) {
    // Firstly, remove bundles from placement group.
    for (const auto &bundle : placement_group->GetAllRemovingBundles()) {
      // Remove node info from this bundle so that we can skip this bundle next
      // time(idempotence).
      placement_group->RemoveNodeInfoFromBundles({bundle->Index()});

      // Then, try to release bundle resource to gcs resource manager and job
      // context(idempotence).
      TryReturnBundleResources(bundle);
    }
  }

  auto placement_group_id = placement_group->GetPlacementGroupID();
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id](Status status) {
        RAY_CHECK_OK(status);

        MarkSchedulingDone();

        if (RescheduleIfStillHasUnplacedBundles(placement_group_id)) {
          return;
        }
        SchedulePendingPlacementGroups();

        // Invoke all callbacks for all `WaitPlacementGroupUntilReady` requests of this
        // placement group and remove all of them from
        // placement_group_to_create_callbacks_.
        auto pg_to_create_iter =
            placement_group_to_create_callbacks_.find(placement_group_id);
        if (pg_to_create_iter != placement_group_to_create_callbacks_.end()) {
          for (auto &callback : pg_to_create_iter->second) {
            callback(status);
          }
          placement_group_to_create_callbacks_.erase(pg_to_create_iter);
        }
      }));
}

void GcsPlacementGroupManager::TryReturnBundleResources(
    const std::shared_ptr<BundleSpecification> &bundle) {
  if (!TryReleasingBundleResources(bundle)) {
    // Put it into a container and try to release resource when receiving resource
    // changed event.
    RAY_LOG(INFO) << "We can't release the bundle: " << bundle->Index()
                  << " in placement group: " << bundle->BundleId().first
                  << " immediately since the worker corresponding to this bundle has "
                     "not dead, will try it later.";
    waiting_removed_bundles_.push_back(bundle);
  }
}

void GcsPlacementGroupManager::OnClusterResourcesChanged() {
  // Firstly, try to release the bundle that waitting to be destroyed.
  RAY_LOG(DEBUG) << "Receive a cluster resource changed event and will try to release "
                    "the bundle resources firstly.";
  for (auto iter = waiting_removed_bundles_.begin();
       iter != waiting_removed_bundles_.end();) {
    auto current = iter++;
    auto bundle = *current;
    if (TryReleasingBundleResources(bundle)) {
      // Release bundle successfully.
      waiting_removed_bundles_.erase(current);
    }
  }
  // Then, scheduling the pending placement group.
  SchedulePendingPlacementGroups();
}

bool GcsPlacementGroupManager::TryReleasingBundleResources(
    const std::shared_ptr<BundleSpecification> &bundle, bool release_job_resources) {
  auto &cluster_resources = gcs_resource_manager_.GetMutableClusterResources();
  auto iter = cluster_resources.find(bundle->NodeId());
  if (iter != cluster_resources.end() &&
      IsBundleResourceReleasable(bundle, *iter->second)) {
    RAY_LOG(INFO) << "Start to release the bundle: " << bundle->Index()
                  << " in placement group: " << bundle->PlacementGroupId()
                  << " that waitting to be destroyed";

    // Return bundle resource to gcs resource manager.
    if (iter->second->ReturnBundleResources(bundle->PlacementGroupId(),
                                            bundle->Index())) {
      if (RayConfig::instance().gcs_task_scheduling_enabled()) {
        gcs_resource_manager_.SubtractNodeTotalRequiredResources(
            bundle->NodeId(), bundle->GetRequiredResources());
      }
      // When job quota is disabled, it is hard to subtract job's acquired resources
      // idempotently. So we only do it here to make sure the acquired resources
      // are subtracted only once when returning bundles.
      SubtractJobAcquiredResources(bundle->PlacementGroupId().JobId(), {bundle});
    }

    // If we can successfully release bundle resource to gcs resource manager, then we
    // can directly release bundle resource to job context, as it will coinstantaneous
    // release resource to gcs resource manager and job distribution when a worker
    // dead.

    // Return removed bundle resource to job context if exist.
    // Note: This operation may be invoked many times for one placement group, so it
    // should always keep idempotence!
    if (release_job_resources) {
      ReturnJobResources(bundle->PlacementGroupId().JobId(), {bundle});
    }
    return true;
  }
  return false;
}

void GcsPlacementGroupManager::SchedulePendingPlacementGroups() {
  // Update the placement group load to report load information to the autoscaler.
  if (pending_placement_groups_.empty() || IsSchedulingInProgress()) {
    return;
  }
  const auto placement_group = pending_placement_groups_.front();
  pending_placement_groups_.pop_front();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  RAY_LOG(DEBUG) << "Start to schedule placement group: " << placement_group_id
                 << " pending queue size:" << pending_placement_groups_.size();
  // Do not reschedule if the placement group has removed already.
  if (registered_placement_groups_.contains(placement_group_id)) {
    MarkSchedulingStarted(placement_group);
    gcs_placement_group_scheduler_->ScheduleUnplacedBundles(
        placement_group,
        [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
          OnPlacementGroupCreationFailed(std::move(placement_group));
        },
        [this](std::shared_ptr<GcsPlacementGroup> placement_group) {
          OnPlacementGroupCreationSuccess(std::move(placement_group));
        });
  } else {
    RAY_LOG(WARNING)
        << "The placement group " << placement_group_id
        << " is already removed, and schedule pending placement groups again.";
    io_context_.post([this] { SchedulePendingPlacementGroups(); },
                     "GcsPlacementGroupManager.SchedulePendingPlacementGroups");
  }
}

void GcsPlacementGroupManager::HandleCreatePlacementGroup(
    const ray::rpc::CreatePlacementGroupRequest &request,
    ray::rpc::CreatePlacementGroupReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  const JobID &job_id =
      JobID::FromBinary(request.placement_group_spec().creator_job_id());
  auto placement_group =
      std::make_shared<GcsPlacementGroup>(request, get_ray_namespace_(job_id));
  placement_group->MarkCreationStart();
  RAY_LOG(INFO) << "Registering placement group, " << placement_group->DebugString();
  RegisterPlacementGroup(placement_group, [reply, send_reply_callback,
                                           placement_group](Status status) {
    if (status.ok()) {
      RAY_LOG(DEBUG) << "Finished registering placement group, "
                     << placement_group->DebugString();
    } else {
      RAY_LOG(INFO) << "Failed to register placement group, "
                    << placement_group->DebugString() << ", cause: " << status.message();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });
  ++counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleRemovePlacementGroupBundles(
    const ray::rpc::RemovePlacementGroupBundlesRequest &request,
    ray::rpc::RemovePlacementGroupBundlesReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  const auto &placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());

  auto removing_bundle_indexes = VectorFromProtobuf(request.bundle_indexes());
  std::ostringstream removed_bundle_indexes_str;
  std::copy(removing_bundle_indexes.begin(), removing_bundle_indexes.end(),
            std::ostream_iterator<int>(removed_bundle_indexes_str, ","));
  RAY_LOG(INFO) << "Registering remove bundles: [" << removed_bundle_indexes_str.str()
                << "] request from the placement group: " << placement_group_id;
  RemoveBundlesFromPlacementGroup(
      placement_group_id, removing_bundle_indexes,
      [reply, send_reply_callback, placement_group_id](Status status) {
        if (status.ok()) {
          RAY_LOG(INFO)
              << "Finished register remove bundles request from the placemnet group: "
              << placement_group_id;
        } else {
          RAY_LOG(ERROR)
              << "Failed to register remove bundles request from the placement group: "
              << placement_group_id << ", cause: " << status.message();
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });
  ++counts_[CountType::REMOVE_PLACEMENT_GROUP_BUNDLES_REQUEST];
}

void GcsPlacementGroupManager::HandleAddPlacementGroupBundles(
    const ray::rpc::AddPlacementGroupBundlesRequest &request,
    ray::rpc::AddPlacementGroupBundlesReply *reply,
    ray::rpc::SendReplyCallback send_reply_callback) {
  const auto &placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  std::ostringstream debug_info;
  debug_info << "Registering add bundles request for placement group: "
             << placement_group_id << ", detailed new bundles info: ";
  for (int index = 0; index < request.bundles_size(); index++) {
    debug_info << "{" << index << ": ";
    const std::unordered_map<std::string, double> resources =
        MapFromProtobuf(request.bundles(index).unit_resources());
    for (const auto &resource : resources) {
      debug_info << "{" << resource.first << ":" << resource.second << "},";
    }
    debug_info << "}, ";
  }
  RAY_LOG(INFO) << debug_info.str();

  AddBundlesForPlacementGroup(
      placement_group_id, request,
      [reply, send_reply_callback, placement_group_id](Status status) {
        if (status.ok()) {
          RAY_LOG(INFO)
              << "Finished register add bundles request for the placemnet group: "
              << placement_group_id;
        } else {
          RAY_LOG(ERROR)
              << "Failed to register add bundles request for the placement group: "
              << placement_group_id << ", cause: " << status.message();
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      });
  ++counts_[CountType::ADD_PLACEMENT_GROUP_BUNDLES_REQUEST];
}

void GcsPlacementGroupManager::RemoveBundlesFromPlacementGroup(
    const PlacementGroupID &placement_group_id, const std::vector<int> &bundle_indexes,
    StatusCallback callback) {
  RAY_CHECK(callback);
  // Check whether the placement group is exist or not.
  const auto placement_group_it = registered_placement_groups_.find(placement_group_id);
  if (placement_group_it == registered_placement_groups_.end()) {
    // Return failed directly if the placement group has been removed.
    std::ostringstream error_message;
    error_message << "Register remove bundles request from placement group: "
                  << placement_group_id << " failed, cause it has been removed.";
    RAY_LOG(WARNING) << error_message.str();
    callback(Status::NotFound(error_message.str()));
    return;
  }
  auto placement_group = placement_group_it->second;
  // Check whether the removing bundle indexes are all exist or not.
  auto all_bundle_indexes = placement_group->GetAllBundleIndexes();
  for (const auto &removing_index : bundle_indexes) {
    const auto removing_it =
        std::find(all_bundle_indexes.begin(), all_bundle_indexes.end(), removing_index);
    if (removing_it == all_bundle_indexes.end()) {
      std::ostringstream exist_bundle_indexes_str;
      std::copy(all_bundle_indexes.begin(), all_bundle_indexes.end(),
                std::ostream_iterator<int>(exist_bundle_indexes_str, ","));
      std::ostringstream error_message;
      error_message << "Register remove bundles request from placement group: "
                    << placement_group_id
                    << " failed, cause the bundle index: " << removing_index
                    << " isn't exist, current bundle indexes view: ["
                    << exist_bundle_indexes_str.str() << "].";
      RAY_LOG(WARNING) << error_message.str();
      callback(Status::NotFound(error_message.str()));
      return;
    }
  }
  // Return failed directly if the placement group is not `CREATED`.
  if (placement_group->GetState() != rpc::PlacementGroupTableData::CREATED) {
    std::ostringstream error_message;
    error_message
        << "Register remove bundles request for placement group: " << placement_group_id
        << " failed, cause the placement group is in scheduling now. You should always "
           "invoke `Pg.wait(timeout)` before resizing the bundle size to make sure the "
           "placement group has already created successfully.";
    RAY_LOG(WARNING) << error_message.str();
    callback(Status::Invalid(error_message.str()));
    return;
  }

  // Update the metadata information of the placement group regardless of it's state.
  placement_group->RemoveBundles(bundle_indexes);
  //  Put it into the pending queue so that we can reschedule it next time.
  pending_placement_groups_.emplace_back(placement_group);
  placement_group->UpdateState(rpc::PlacementGroupTableData::UPDATING);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id, placement_group, bundle_indexes,
       callback](Status status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        // The placement group can't be removed, because the registration of remove
        // bundles is synchronous.
        if (registered_placement_groups_.contains(placement_group_id)) {
          // Publish bundles changed event to workers that has been registered.
          PublishBundlesInfo(placement_group);
        } else {
          RAY_LOG(WARNING) << "The placement group: " << placement_group_id
                           << " has been removed when removing bundles.";
        }

        callback(status);
        SchedulePendingPlacementGroups();
      }));
};

void GcsPlacementGroupManager::AddBundlesForPlacementGroup(
    const PlacementGroupID &placement_group_id,
    const ray::rpc::AddPlacementGroupBundlesRequest &request, StatusCallback callback) {
  RAY_CHECK(callback);
  // TODO(loushang.ls): support reconnection when occurring network error.

  // Check whether the placement group is exist or not.
  const auto placement_group_it = registered_placement_groups_.find(placement_group_id);
  if (placement_group_it == registered_placement_groups_.end()) {
    // Return failed directly if the placement group has been removed.
    std::ostringstream error_message;
    error_message << "Register add bundles request for placement group: "
                  << placement_group_id << " failed, cause it has been removed.";
    RAY_LOG(WARNING) << error_message.str();
    callback(Status::NotFound(error_message.str()));
    return;
  }
  auto placement_group = placement_group_it->second;
  // Return failed directly if the placement group is not `CREATED`.
  if (placement_group->GetState() != rpc::PlacementGroupTableData::CREATED) {
    std::ostringstream error_message;
    error_message
        << "Register add bundles request for placement group: " << placement_group_id
        << " failed, cause the placement group is in scheduling now. You should always "
           "invoke `Pg.wait(timeout)` before resizing the bundle size to make sure the "
           "placement group has already created successfully.";
    RAY_LOG(WARNING) << error_message.str();
    callback(Status::Invalid(error_message.str()));
    return;
  }

  placement_group->AddBundles(request);
  placement_group->UpdateState(rpc::PlacementGroupTableData::UPDATING);

  // ANT-INTERNAL.
  const auto &reserve_resource_res = ReserveJobResources(
      placement_group->GetPlacementGroupID(), placement_group->GetUnplacedBundles());
  if (!reserve_resource_res.ok()) {
    callback(reserve_resource_res);
    return;
  }

  // Put it into the pending queue so that we can reschedule it next time.
  pending_placement_groups_.emplace_back(placement_group);

  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group_id, placement_group->GetPlacementGroupTableData(),
      [this, placement_group_id, placement_group, callback, request](Status status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        // The placement group can't be removed, because the registration of add bundles
        // is synchronous.
        if (registered_placement_groups_.contains(placement_group_id)) {
          // Publish bundles changed event to workers that has been registered.
          PublishBundlesInfo(placement_group);
        } else {
          RAY_LOG(WARNING) << "The placement group: " << placement_group_id
                           << " has been removed when adding bundles.";
        }

        callback(status);
        SchedulePendingPlacementGroups();
      }));
}

void GcsPlacementGroupManager::HandleRemovePlacementGroup(
    const rpc::RemovePlacementGroupRequest &request,
    rpc::RemovePlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const auto placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());

  RemovePlacementGroup(placement_group_id, [send_reply_callback, reply,
                                            placement_group_id](Status status) {
    if (status.ok()) {
      RAY_LOG(INFO) << "Placement group of an id, " << placement_group_id
                    << " is removed successfully.";
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });
  ++counts_[CountType::REMOVE_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::RemovePlacementGroup(
    const PlacementGroupID &placement_group_id,
    StatusCallback on_placement_group_removed) {
  RAY_CHECK(on_placement_group_removed);
  RAY_LOG(INFO) << "Removing placement group " << placement_group_id;

  // If the placement group has been already removed, don't do anything.
  auto placement_group_it = registered_placement_groups_.find(placement_group_id);
  if (placement_group_it == registered_placement_groups_.end()) {
    on_placement_group_removed(Status::OK());
    return;
  }
  // Destroy all bundles firstly since it will check the creator's state from
  // `registered_placement_groups_`.
  gcs_placement_group_scheduler_->DestroyPlacementGroupBundleResourcesIfExists(
      placement_group_id);

  auto placement_group = std::move(placement_group_it->second);
  registered_placement_groups_.erase(placement_group_it);
  placement_group_to_register_callbacks_.erase(placement_group_id);

  // Remove placement group from `named_placement_groups_` if its name is not empty.
  if (!placement_group->GetName().empty()) {
    auto namespace_it = named_placement_groups_.find(placement_group->GetRayNamespace());
    if (namespace_it != named_placement_groups_.end()) {
      auto it = namespace_it->second.find(placement_group->GetName());
      if (it != namespace_it->second.end() &&
          it->second == placement_group->GetPlacementGroupID()) {
        namespace_it->second.erase(it);
      }
      if (namespace_it->second.empty()) {
        named_placement_groups_.erase(namespace_it);
      }
    }
  }

  // Cancel the scheduling request if necessary.
  if (IsSchedulingInProgress(placement_group_id)) {
    // If the placement group is scheduling.
    gcs_placement_group_scheduler_->MarkScheduleCancelled(placement_group_id);
  }

  // Remove a placement group from a pending list if exists.
  auto pending_it = std::find_if(
      pending_placement_groups_.begin(), pending_placement_groups_.end(),
      [placement_group_id](const std::shared_ptr<GcsPlacementGroup> &placement_group) {
        return placement_group->GetPlacementGroupID() == placement_group_id;
      });
  if (pending_it != pending_placement_groups_.end()) {
    // The placement group was pending scheduling, remove it from the queue.
    pending_placement_groups_.erase(pending_it);
  }

  // Return job resources.
  ReturnJobResources(placement_group->GetPlacementGroupID().JobId(),
                     placement_group->GetBundles());

  // Return bundle resources to gcs resource manager.
  // NOTE: In GCS scheduler, Instead of return bundle resource to gcs
  // resource manager in the pg scheduler, we need to return bundle resource until
  // the tasks that running at that bundle has done.
  ReturnBundleResources(placement_group->GetBundles());

  // Flush the status and respond to workers.
  placement_group->UpdateState(rpc::PlacementGroupTableData::REMOVED);
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupTable().Put(
      placement_group->GetPlacementGroupID(),
      placement_group->GetPlacementGroupTableData(),
      [this, on_placement_group_removed, placement_group_id](Status status) {
        RAY_CHECK_OK(status);
        // If there is a driver waiting for the creation done, then send a message that
        // the placement group has been removed.
        auto it = placement_group_to_create_callbacks_.find(placement_group_id);
        if (it != placement_group_to_create_callbacks_.end()) {
          for (auto &callback : it->second) {
            callback(
                Status::NotFound("Placement group is removed before it is created."));
          }
          placement_group_to_create_callbacks_.erase(it);
        }
        on_placement_group_removed(status);
      }));
}

void GcsPlacementGroupManager::HandleGetPlacementGroup(
    const rpc::GetPlacementGroupRequest &request, rpc::GetPlacementGroupReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PlacementGroupID placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  RAY_LOG(DEBUG) << "Getting placement group info, placement group id = "
                 << placement_group_id;

  auto on_done = [placement_group_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<PlacementGroupTableData> &result) {
    if (result) {
      reply->mutable_placement_group_table_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting placement group info, placement group id = "
                   << placement_group_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status =
      gcs_table_storage_->PlacementGroupTable().Get(placement_group_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
  ++counts_[CountType::GET_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleGetNamedPlacementGroup(
    const rpc::GetNamedPlacementGroupRequest &request,
    rpc::GetNamedPlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  RAY_LOG(DEBUG) << "Getting named placement group info, name = " << name;

  // Try to look up the placement Group ID for the named placement group.
  auto placement_group_id = GetPlacementGroupIDByName(name, request.ray_namespace());

  if (placement_group_id.IsNil()) {
    // The placement group was not found.
    RAY_LOG(DEBUG) << "Placement Group with name '" << name << "' was not found";
  } else {
    const auto &iter = registered_placement_groups_.find(placement_group_id);
    if (iter != registered_placement_groups_.end()) {
      reply->mutable_placement_group_table_data()->CopyFrom(
          iter->second->GetPlacementGroupTableData());
      RAY_LOG(DEBUG) << "Finished get named placement group info, placement group id = "
                     << placement_group_id;
    }
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_NAMED_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleGetAllPlacementGroup(
    const rpc::GetAllPlacementGroupRequest &request,
    rpc::GetAllPlacementGroupReply *reply, rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all placement group info.";
  auto on_done =
      [reply, send_reply_callback](
          const std::unordered_map<PlacementGroupID, PlacementGroupTableData> &result) {
        for (auto &data : result) {
          reply->add_placement_group_table_data()->CopyFrom(data.second);
        }
        RAY_LOG(DEBUG) << "Finished getting all placement group info.";
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      };
  Status status = gcs_table_storage_->PlacementGroupTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<PlacementGroupID, PlacementGroupTableData>());
  }
  ++counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST];
}

void GcsPlacementGroupManager::HandleWaitPlacementGroupUntilReady(
    const rpc::WaitPlacementGroupUntilReadyRequest &request,
    rpc::WaitPlacementGroupUntilReadyReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  PlacementGroupID placement_group_id =
      PlacementGroupID::FromBinary(request.placement_group_id());
  RAY_LOG(DEBUG) << "Waiting for placement group until ready, placement group id = "
                 << placement_group_id;

  WaitPlacementGroup(placement_group_id, [reply, send_reply_callback,
                                          placement_group_id](Status status) {
    if (status.ok()) {
      RAY_LOG(DEBUG)
          << "Finished waiting for placement group until ready, placement group id = "
          << placement_group_id;
    } else {
      RAY_LOG(WARNING)
          << "Failed to waiting for placement group until ready, placement group id = "
          << placement_group_id << ", cause: " << status.message();
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  });

  ++counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST];
}

void GcsPlacementGroupManager::WaitPlacementGroup(
    const PlacementGroupID &placement_group_id, StatusCallback callback) {
  // If the placement group does not exist or it has been successfully created, return
  // directly.
  const auto &iter = registered_placement_groups_.find(placement_group_id);
  if (iter == registered_placement_groups_.end()) {
    // Check whether the placement group does not exist or is removed.
    auto on_done = [this, placement_group_id, callback](
                       const Status &status,
                       const boost::optional<PlacementGroupTableData> &result) {
      if (result) {
        RAY_LOG(DEBUG) << "Placement group is removed, placement group id = "
                       << placement_group_id;
        callback(Status::NotFound("Placement group is removed."));
      } else {
        // `wait` is a method of placement group object. Placement group object is
        // obtained by create placement group api, so it can guarantee the existence of
        // placement group.
        // GCS client does not guarantee the order of placement group creation and
        // wait, so GCS may call wait placement group first and then create placement
        // group.
        placement_group_to_create_callbacks_[placement_group_id].emplace_back(
            std::move(callback));
      }
    };

    Status status =
        gcs_table_storage_->PlacementGroupTable().Get(placement_group_id, on_done);
    if (!status.ok()) {
      on_done(status, boost::none);
    }
  } else if (iter->second->GetState() == rpc::PlacementGroupTableData::CREATED) {
    RAY_LOG(DEBUG) << "Placement group schedule successful, placement group id = "
                   << placement_group_id;
    callback(Status::OK());
  } else {
    placement_group_to_create_callbacks_[placement_group_id].emplace_back(
        std::move(callback));
  }
}

void GcsPlacementGroupManager::RetryCreatingPlacementGroup() {
  execute_after(io_context_, [this] { SchedulePendingPlacementGroups(); },
                RayConfig::instance().gcs_create_placement_group_retry_interval_ms());
}

void GcsPlacementGroupManager::OnNodeDead(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id
                << " failed, rescheduling the placement groups on the dead node.";
  auto bundles = gcs_placement_group_scheduler_->GetBundlesOnNode(node_id);
  for (const auto &bundle : bundles) {
    auto iter = registered_placement_groups_.find(bundle.first);
    if (iter != registered_placement_groups_.end()) {
      // Whether exist valid bundles of this placement group in this node.
      bool existing_valid_bundles = false;
      for (const auto &bundle_index : bundle.second) {
        iter->second->GetMutableBundle(bundle_index)->clear_node_id();
        if (iter->second->GetMutableBundle(bundle_index)->is_valid()) {
          existing_valid_bundles = true;
          RAY_LOG(INFO) << "Changed the state of the bundle to unplaced to "
                           "reschedule, placement group id:"
                        << iter->second->GetPlacementGroupID()
                        << " bundle index:" << bundle_index;
        } else {
          // Try to remove the bundle from `waiting_removed_bundles_` in case memory leak.
          const auto removing_bundle = std::make_shared<BundleSpecification>(
              iter->second->GetBundle(bundle_index));
          std::remove_if(waiting_removed_bundles_.begin(), waiting_removed_bundles_.end(),
                         [removing_bundle](std::shared_ptr<BundleSpecification> bundle) {
                           return removing_bundle->BundleId() == bundle->BundleId();
                         });
        }
      }
      // TODO(ffbin): If we have a placement group bundle that requires a unique resource
      // (for example gpu resource when there’s only one gpu node), this can postpone
      // creating until a node with the resources is added. we will solve it in next pr.
      if (existing_valid_bundles &&
          iter->second->GetState() != rpc::PlacementGroupTableData::RESCHEDULING) {
        iter->second->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
        pending_placement_groups_.emplace_front(iter->second);
      }
    }
  }

  SchedulePendingPlacementGroups();
}

void GcsPlacementGroupManager::CleanPlacementGroupIfNeededWhenJobDead(
    const JobID &job_id) {
  std::vector<PlacementGroupID> groups_to_remove;

  for (const auto &it : registered_placement_groups_) {
    auto &placement_group = it.second;
    if (placement_group->GetCreatorJobId() != job_id) {
      continue;
    }
    placement_group->MarkCreatorJobDead();
    if (placement_group->IsPlacementGroupLifetimeDone()) {
      groups_to_remove.push_back(placement_group->GetPlacementGroupID());
    }
  }

  for (const auto &group : groups_to_remove) {
    RAY_LOG(INFO) << "Job " << job_id
                  << " is finished, we will remove the placement group " << group;
    RemovePlacementGroup(group, [](Status status) {});
  }
}

bool GcsPlacementGroupManager::IsPlacementGroupLifetimeDone(
    const PlacementGroupID &placement_group_id) {
  for (const auto &it : registered_placement_groups_) {
    auto &current_placement_group = it.second;
    if (current_placement_group->GetPlacementGroupID() == placement_group_id) {
      return current_placement_group->IsPlacementGroupLifetimeDone();
    }
  }
  return false;
}

void GcsPlacementGroupManager::CollectStats() const {
  stats::PendingPlacementGroups.Record(pending_placement_groups_.size());
}

void GcsPlacementGroupManager::Tick() {
  UpdatePlacementGroupLoad();
  execute_after(io_context_, [this] { Tick(); }, 1000 /* milliseconds */);
}

void GcsPlacementGroupManager::UpdatePlacementGroupLoad() {
  std::shared_ptr<rpc::PlacementGroupLoad> placement_group_load =
      std::make_shared<rpc::PlacementGroupLoad>();
  int total_cnt = 0;
  for (const auto &pending_pg_spec : pending_placement_groups_) {
    auto placement_group_data = placement_group_load->add_placement_group_data();
    auto placement_group_table_data = pending_pg_spec->GetPlacementGroupTableData();
    placement_group_data->Swap(&placement_group_table_data);
    total_cnt += 1;
    if (total_cnt >= RayConfig::instance().max_placement_group_load_report_size()) {
      break;
    }
  }
  gcs_resource_manager_.UpdatePlacementGroupLoad(move(placement_group_load));
}

void GcsPlacementGroupManager::PublishBundlesInfo(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  auto notification = std::make_shared<rpc::PlacementGroupBundlesChangedNotification>();
  notification->set_placement_group_id(placement_group->GetPlacementGroupID().Binary());

  auto all_bundles = placement_group->GetBundles();
  for (size_t index = 0; index < all_bundles.size(); index++) {
    notification->add_bundles(all_bundles[index]->IsValid());
  }

  RAY_LOG(DEBUG) << "Register resizing bundles request for placement group: "
                 << placement_group->GetPlacementGroupID()
                 << " successful, will publish bundles info to workers.";
  RAY_UNUSED(gcs_pub_sub_->Publish(PLACEMENT_GROUP_BUNDELS_CHANGED_CHANNEL,
                                   placement_group->GetPlacementGroupID().Hex(),
                                   notification->SerializeAsString(),
                                   [](const Status &status) {}));
}

std::vector<std::shared_ptr<GcsPlacementGroup>>
GcsPlacementGroupManager::GetAllPlacementGroupsInNode(const NodeID &node_id) {
  std::vector<std::shared_ptr<GcsPlacementGroup>> res;
  for (auto &pair : registered_placement_groups_) {
    auto &pg = pair.second;
    if (pg->GetState() != rpc::PlacementGroupTableData::CREATED) {
      continue;
    }
    for (auto &bundle : pg->GetCreatedBundles()) {
      if (bundle->NodeId() == node_id) {
        res.emplace_back(pg);
        break;
      }
    }
  }
  return res;
}

const BundleLocationIndex &GcsPlacementGroupManager::GetCommittedBundleLocationIndex()
    const {
  return gcs_placement_group_scheduler_->GetCommittedBundleLocationIndex();
}

void GcsPlacementGroupManager::Initialize(const GcsInitData &gcs_init_data) {
  std::unordered_map<NodeID, std::vector<rpc::Bundle>> node_to_bundles;
  std::unordered_map<PlacementGroupID, std::vector<std::shared_ptr<BundleSpecification>>>
      group_to_bundles;
  for (auto &item : gcs_init_data.PlacementGroups()) {
    auto placement_group = std::make_shared<GcsPlacementGroup>(item.second);
    if (item.second.state() != rpc::PlacementGroupTableData::REMOVED) {
      registered_placement_groups_.emplace(item.first, placement_group);
      if (!placement_group->GetName().empty()) {
        named_placement_groups_[placement_group->GetRayNamespace()].emplace(
            placement_group->GetName(), placement_group->GetPlacementGroupID());
      }

      if (item.second.state() == rpc::PlacementGroupTableData::PENDING ||
          item.second.state() == rpc::PlacementGroupTableData::UPDATING ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        pending_placement_groups_.emplace_back(placement_group);
      }

      if (item.second.state() == rpc::PlacementGroupTableData::CREATED ||
          // Note: we need to put the bundles that has created before into
          // `node_to_bundles` when it's state is `UPDATING`.
          item.second.state() == rpc::PlacementGroupTableData::UPDATING ||
          item.second.state() == rpc::PlacementGroupTableData::RESCHEDULING) {
        const auto &bundles = item.second.bundles();
        for (const auto &bundle : bundles) {
          // We don't need to remove the bundles that has been marked `invalid`.
          if (!NodeID::FromBinary(bundle.node_id()).IsNil() && bundle.is_valid()) {
            node_to_bundles[NodeID::FromBinary(bundle.node_id())].emplace_back(bundle);
            group_to_bundles[PlacementGroupID::FromBinary(
                                 bundle.bundle_id().placement_group_id())]
                .emplace_back(std::make_shared<BundleSpecification>(bundle));
          }
        }
      }

      // ANT-INTERNAL
      // Ignore the return value directly as we have guaranteed the resource is
      // sufficient before.
      // Note: we should reserve all the bundles that have been created before as this is
      // a fo case!
      RAY_UNUSED(ReserveJobResources(placement_group->GetPlacementGroupID(),
                                     placement_group->GetAllValidBundles()));
    }
  }

  // Notify raylets to release unused bundles.
  gcs_placement_group_scheduler_->ReleaseUnusedBundles(node_to_bundles);
  gcs_placement_group_scheduler_->Initialize(group_to_bundles);

  SchedulePendingPlacementGroups();
}

std::string GcsPlacementGroupManager::DebugString() const {
  uint64_t num_pgs = 0;
  for (auto it : named_placement_groups_) {
    num_pgs += it.second.size();
  }
  std::ostringstream stream;
  stream << "GcsPlacementGroupManager: {CreatePlacementGroup request count: "
         << counts_[CountType::CREATE_PLACEMENT_GROUP_REQUEST]
         << ", RemovePlacementGroup request count: "
         << counts_[CountType::REMOVE_PLACEMENT_GROUP_REQUEST]
         << ", GetPlacementGroup request count: "
         << counts_[CountType::GET_PLACEMENT_GROUP_REQUEST]
         << ", GetAllPlacementGroup request count: "
         << counts_[CountType::GET_ALL_PLACEMENT_GROUP_REQUEST]
         << ", WaitPlacementGroupUntilReady request count: "
         << counts_[CountType::WAIT_PLACEMENT_GROUP_UNTIL_READY_REQUEST]
         << ", GetNamedPlacementGroup request count: "
         << counts_[CountType::GET_NAMED_PLACEMENT_GROUP_REQUEST]
         << ", AddPlacementGroupBundles request count: "
         << counts_[CountType::ADD_PLACEMENT_GROUP_BUNDLES_REQUEST]
         << ", RemovePlacementGroupBundles request count: "
         << counts_[CountType::REMOVE_PLACEMENT_GROUP_BUNDLES_REQUEST]
         << ", Registered placement groups count: " << registered_placement_groups_.size()
         << ", Named placement group count: " << num_pgs
         << ", Pending placement groups count: " << pending_placement_groups_.size()
         << "}";
  return stream.str();
}

bool GcsPlacementGroupManager::IsPlacmentGroupIDInPendingQueue(
    const PlacementGroupID &placement_group_id) const {
  auto pending_it = std::find_if(
      pending_placement_groups_.begin(), pending_placement_groups_.end(),
      [placement_group_id](const std::shared_ptr<GcsPlacementGroup> &placement_group) {
        return placement_group->GetPlacementGroupID() == placement_group_id;
      });
  return pending_it != pending_placement_groups_.end();
}

bool GcsPlacementGroupManager::RescheduleIfStillHasUnplacedBundles(
    const PlacementGroupID &placement_group_id) {
  auto iter = registered_placement_groups_.find(placement_group_id);
  if (iter != registered_placement_groups_.end()) {
    auto &placement_group = iter->second;
    if (placement_group->HasUnplacedBundles()) {
      if ((!IsPlacmentGroupIDInPendingQueue(placement_group->GetPlacementGroupID())) &&
          placement_group->GetState() != rpc::PlacementGroupTableData::REMOVED) {
        RAY_LOG(INFO) << "The placement group still has unplaced bundles, so put "
                         "it to pending queue again, id:"
                      << placement_group->GetPlacementGroupID();
        placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
        pending_placement_groups_.emplace_front(placement_group);
        SchedulePendingPlacementGroups();
        return true;
      }
    }
  }
  return false;
}

void GcsPlacementGroupManager::DetectPlacementGroupSchedulingSlow() {
  if (!scheduling_in_progress_id_.IsNil()) {
    const auto &iter = registered_placement_groups_.find(scheduling_in_progress_id_);
    if (iter != registered_placement_groups_.end()) {
      const auto &placement_group = iter->second;
      auto scheduling_time_s =
          (current_time_ms() - placement_group->GetLastStartSchedulingTimeMs()) / 1000;
      if (scheduling_time_s >
          RayConfig::instance().placement_group_scheduling_slow_threshold_time_s()) {
        std::ostringstream stream;
        stream << "The placement group " << placement_group->GetPlacementGroupID()
               << " has been scheduled for " << scheduling_time_s << " (> "
               << RayConfig::instance().placement_group_scheduling_slow_threshold_time_s()
               << ") seconds. This blocks " << pending_placement_groups_.size()
               << " placement groups scheduling.";
        RAY_EVENT(ERROR, EVENT_LABEL_SLOW_PLACEMENT_GROUP_SCHEDULING)
                .WithField("job_id", placement_group->GetCreatorJobId().Hex())
            << stream.str();
        RAY_LOG(ERROR) << stream.str();
      }
    } else {
      RAY_LOG(ERROR) << "The placement group " << scheduling_in_progress_id_
                     << " is already removed from registered list, but it is still in "
                        "scheduling process.";
    }
  }
}
}  // namespace gcs
}  // namespace ray
