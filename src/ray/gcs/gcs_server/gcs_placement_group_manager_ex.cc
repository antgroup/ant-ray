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

#include "ray/gcs/gcs_server/gcs_placement_group_manager_ex.h"

#include "ray/common/asio/asio_util.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/pb_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

void GcsPlacementGroupManagerEx::OnPlacementGroupCreationFailed(
    std::shared_ptr<GcsPlacementGroup> placement_group) {
  GcsPlacementGroupManager::OnPlacementGroupCreationFailed(placement_group);
}

void GcsPlacementGroupManagerEx::OnPlacementGroupCreationSuccess(
    const std::shared_ptr<GcsPlacementGroup> &placement_group) {
  GcsPlacementGroupManager::OnPlacementGroupCreationSuccess(placement_group);
  placement_group_create_success_callback_(placement_group->GetPlacementGroupID());
}

Status GcsPlacementGroupManagerEx::ReserveJobResources(
    const PlacementGroupID &placement_group_id,
    const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  const JobID &job_id = placement_group_id.JobId();
  auto job_scheduling_context =
      job_distribution_->FindOrCreateJobSchedulingContext(job_id);
  auto reserve_res = job_scheduling_context->ReserveBundlesResources(bundles);
  if (!reserve_res) {
    std::ostringstream stream;
    stream << "Failed to register placment group: " << placement_group_id
           << " or add new bundles to placment group: " << placement_group_id
           << " as the job resource requirements are not enough, please check if the job "
              "resource requirements are set correctly.\njob resource requirements: "
           << job_scheduling_context->GetSchedulingResources().DebugString()
           << "\nrequest resources: \n";
    for (const auto &resource : GetBundleConstraintResources(bundles)) {
      stream << "{bundle index: " << resource.first
             << ", resource request:" << resource.second << " }\n";
    }
    return Status::Invalid(stream.str());
  }
  return Status::OK();
}

void GcsPlacementGroupManagerEx::ReturnJobResources(
    const JobID &job_id,
    const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  if (auto job_scheduling_context = job_distribution_->GetJobSchedulingContext(job_id)) {
    job_scheduling_context->ReturnBundlesResources(bundles);
  }
}

void GcsPlacementGroupManagerEx::SubtractJobAcquiredResources(
    const JobID &job_id,
    const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  if (auto job_scheduling_context = job_distribution_->GetJobSchedulingContext(job_id)) {
    job_scheduling_context->SubtractJobAcquiredResources(bundles);
  }
}

void GcsPlacementGroupManagerEx::ReturnBundleResources(
    const std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  for (const auto &bundle : bundles) {
    TryReturnBundleResources(bundle);
  }
}

bool GcsPlacementGroupManagerEx::IsBundleResourceReserved(
    const BundleID &bundle_id) const {
  if (bundle_id.second < 0) {
    return false;
  }

  const auto &registered_placement_groups = GetRegisteredPlacementGroups();
  auto iter = registered_placement_groups.find(bundle_id.first);
  if (iter != registered_placement_groups.end()) {
    const auto &placement_group_table_data = iter->second->GetPlacementGroupTableData();
    const auto &bundle = placement_group_table_data.bundles(bundle_id.second);
    if (!bundle.is_valid()) {
      return false;
    }
    if (iter->second->GetState() == rpc::PlacementGroupTableData::CREATED) {
      return true;
    }
    if (iter->second->GetState() == rpc::PlacementGroupTableData::UPDATING) {
      return !NodeID::FromBinary(bundle.node_id()).IsNil();
    }
  }

  return false;
}

void GcsPlacementGroupManagerEx::RescheduleBundle(
    const absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> &bundles) {
  for (auto &pair : bundles) {
    const PlacementGroupID &placement_group_id = pair.first;
    const absl::flat_hash_set<int64_t> &bundle_indexes = pair.second;
    // Find this placement group.
    const auto placement_group_it = registered_placement_groups_.find(placement_group_id);
    if (placement_group_it == registered_placement_groups_.end()) {
      return;
    }
    auto placement_group = placement_group_it->second;
    auto count_down = std::make_shared<int>(bundle_indexes.size());
    for (int64_t index : bundle_indexes) {
      auto mutable_bundle = placement_group->GetMutableBundle(index);
      auto original_node_id = mutable_bundle->node_id();
      // Clear resources in raylet
      gcs_placement_group_scheduler_->CancelBundleResourcesFromRaylet(
          *mutable_bundle,
          /*on_bundle_canceled*/
          [this, placement_group, count_down, mutable_bundle,
           original_node_id = std::move(original_node_id)](const Status &status) {
            (*count_down)--;
            RAY_LOG(DEBUG) << "Bundle canceled, PG ID="
                           << placement_group->GetPlacementGroupID()
                           << ", bundle index = "
                           << mutable_bundle->bundle_id().bundle_index()
                           << ", count down left=" << (*count_down);
            // If node_id changed, it means this bundle is rescheduled elsewhere, we don't
            // need to schedule it again.
            if (original_node_id != mutable_bundle->node_id()) {
              return;
            }
            RAY_LOG(DEBUG) << "Removing bundle resources from GCS.";
            TryReleasingBundleResources(
                std::make_shared<BundleSpecification>(*mutable_bundle),
                /*release_job_resources=*/false);
            // Clear node info from bundle.
            mutable_bundle->clear_node_id();
            if ((*count_down) == 0) {
              // Add this placement group to pending queue
              pending_placement_groups_.emplace_back(placement_group);
              placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
              // Reschedule these placement group bundles after all resources are
              // canceled.
              RAY_LOG(DEBUG) << "All bundles canceled, start to reschedule them.";
              SchedulePendingPlacementGroups();
            }
          });
    }
  }
}

}  // namespace gcs
}  // namespace ray
