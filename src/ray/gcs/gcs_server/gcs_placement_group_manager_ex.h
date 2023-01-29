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

#include "ray/gcs/gcs_server/gcs_job_distribution.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"

namespace ray {
namespace gcs {

class GcsPlacementGroupManagerEx : public GcsPlacementGroupManager {
 public:
  /// Create a GcsPlacementGroupManager
  ///
  /// \param io_context The event loop to run the monitor on.
  /// \param scheduler Used to schedule placement group creation tasks.
  /// \param gcs_table_storage Used to flush placement group data to storage.
  explicit GcsPlacementGroupManagerEx(
      instrumented_io_context &io_context,
      std::shared_ptr<GcsPlacementGroupSchedulerInterface> scheduler,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      GcsResourceManager &gcs_resource_manager,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub,
      std::function<std::string(const JobID &)> get_ray_namespace,
      std::shared_ptr<GcsJobDistribution> job_distribution,
      std::function<void(const PlacementGroupID &)>
          placement_group_create_success_callback)
      : GcsPlacementGroupManager(io_context, std::move(scheduler),
                                 std::move(gcs_table_storage), gcs_resource_manager,
                                 gcs_pub_sub, get_ray_namespace),
        job_distribution_(std::move(job_distribution)),
        placement_group_create_success_callback_(
            std::move(placement_group_create_success_callback)) {
    RAY_CHECK(placement_group_create_success_callback_);
  }

  ~GcsPlacementGroupManagerEx() = default;

  bool IsBundleResourceReserved(const BundleID &bundle_id) const;

  void RescheduleBundle(
      const absl::flat_hash_map<PlacementGroupID, absl::flat_hash_set<int64_t>> &bundles);

 protected:
  void OnPlacementGroupCreationFailed(
      std::shared_ptr<GcsPlacementGroup> placement_group) override;

  void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<GcsPlacementGroup> &placement_group) override;

  Status ReserveJobResources(
      const PlacementGroupID &placement_group_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) override;

  void ReturnJobResources(
      const JobID &job_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) override;

  void SubtractJobAcquiredResources(
      const JobID &job_id,
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) override;

  void ReturnBundleResources(
      const std::vector<std::shared_ptr<BundleSpecification>> &bundles) override;

 private:
  std::shared_ptr<GcsJobDistribution> job_distribution_;
  std::function<void(const PlacementGroupID &)> placement_group_create_success_callback_;
};

}  // namespace gcs
}  // namespace ray
