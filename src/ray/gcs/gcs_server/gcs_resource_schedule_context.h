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
#include <string>
#include "absl/container/flat_hash_map.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_label_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_stat.h"
#include "src/ray/protobuf/gcs_service.pb.h"

namespace ray {
namespace gcs {

struct ScheduleOptions {
  static std::shared_ptr<ScheduleOptions> FromProto(
      const rpc::ScheduleOptions &proto_options);

  static std::shared_ptr<ScheduleOptions> FromMapProto(
      const ::google::protobuf::Map<std::string, std::string> &schedule_option_proto);

  std::unordered_map<std::string, std::string> ToMap() const;

  std::string ToString() const;

  // The 'threshold_' seeks the balance between PACK and SPREAD. If there
  // are feasible nodes under the 'threshold_', PACK policy is used. Otherwise,
  // use SPREAD policy instead.
  // Note: The 'threshold' is not flushed to storage. It can be re-calculated
  // inside node_selector after GCS failover.
  double threshold_ = 0.0;

  // The 'pack_step_' defines how much (by percentage) the 'threshold_' is increased
  // each time when needed.
  double pack_step_ = RayConfig::instance().gcs_schedule_pack_step();

  double rare_resources_schedule_pack_step_ =
      RayConfig::instance().rare_resources_schedule_pack_step();

  // The scheduler randomly selects one node from 'num_candidate_nodes_for_scheduling'
  // high-score candidates, which avoids quick dumping actors to a single node or
  // pointless retrying at a node without a prepared environment.
  uint64_t num_candidate_nodes_for_scheduling_ =
      RayConfig::instance().num_candidate_nodes_for_scheduling();

  // The weight of each resource.
  absl::flat_hash_map<std::string, uint64_t> resource_weights_;

  absl::flat_hash_map<std::string, uint64_t> scorer_weights_;

  bool runtime_resource_scheduling_enabled_ =
      RayConfig::instance().runtime_resource_scheduling_enabled();

  uint32_t runtime_resources_calculation_interval_s_ =
      RayConfig::instance().runtime_resources_calculation_interval_s();

  float runtime_memory_tail_percentile_ =
      RayConfig::instance().runtime_memory_tail_percentile();

  float runtime_cpu_tail_percentile_ =
      RayConfig::instance().runtime_cpu_tail_percentile();

  uint32_t runtime_resources_history_window_len_s_ =
      RayConfig::instance().runtime_resources_history_window_len_s();

  float runtime_memory_history_window_tail_ =
      RayConfig::instance().runtime_memory_history_window_tail();

  float runtime_cpu_history_window_tail_ =
      RayConfig::instance().runtime_cpu_history_window_tail();

  float overcommit_ratio_ = RayConfig::instance().overcommit_ratio();

  float node_overcommit_ratio_ = RayConfig::instance().node_overcommit_ratio();

  float job_resource_requirements_max_min_ratio_limit_ =
      RayConfig::instance().job_resource_requirements_max_min_ratio_limit();

  bool operator==(const ScheduleOptions &other) const {
    if (pack_step_ != other.pack_step_ ||
        rare_resources_schedule_pack_step_ != other.rare_resources_schedule_pack_step_ ||
        num_candidate_nodes_for_scheduling_ !=
            other.num_candidate_nodes_for_scheduling_ ||
        resource_weights_.size() != other.resource_weights_.size() ||
        scorer_weights_.size() != other.scorer_weights_.size() ||
        runtime_resource_scheduling_enabled_ !=
            other.runtime_resource_scheduling_enabled_ ||
        runtime_resources_calculation_interval_s_ !=
            other.runtime_resources_calculation_interval_s_ ||
        runtime_memory_tail_percentile_ != other.runtime_memory_tail_percentile_ ||
        runtime_cpu_tail_percentile_ != other.runtime_cpu_tail_percentile_ ||
        runtime_resources_history_window_len_s_ !=
            other.runtime_resources_history_window_len_s_ ||
        runtime_memory_history_window_tail_ !=
            other.runtime_memory_history_window_tail_ ||
        runtime_cpu_history_window_tail_ != other.runtime_cpu_history_window_tail_ ||
        overcommit_ratio_ != other.overcommit_ratio_ ||
        node_overcommit_ratio_ != other.node_overcommit_ratio_ ||
        job_resource_requirements_max_min_ratio_limit_ !=
            other.job_resource_requirements_max_min_ratio_limit_) {
      return false;
    }

    for (auto &entry : other.resource_weights_) {
      auto iter = resource_weights_.find(entry.first);
      if (iter == resource_weights_.end() || iter->second != entry.second) {
        return false;
      }
    }

    for (auto &entry : other.scorer_weights_) {
      auto iter = scorer_weights_.find(entry.first);
      if (iter == scorer_weights_.end() || iter->second != entry.second) {
        return false;
      }
    }
    return true;
  }

  bool operator!=(const ScheduleOptions &other) const { return !operator==(other); }

  uint64_t GetResourceWeight(const std::string &resource_name) const {
    auto iter = resource_weights_.find(resource_name);
    return iter != resource_weights_.end() ? iter->second : 1;
  }

  uint64_t GetScorerWeight(const std::string &scorer_name) const {
    // TODO: RuntimeLeastResourceScorer is temporarily disabled. Enable it later.
    if (scorer_name == "RuntimeLeastResource") {
      return 0;
    }
    auto iter = scorer_weights_.find(scorer_name);
    return iter != scorer_weights_.end() ? iter->second : 1;
  }
};

/// Abstract class ResourceScheduleContext used by GcsResourceScheduler, it is used to
/// unify resource scheduling. It is used to hide the context differences between
/// ResourceBasedSchedulingStrategy and GcsPlacementGroupScheduler.
class ResourceScheduleContext {
 public:
  ResourceScheduleContext(const JobID &job_id, const std::string &nodegroup_id = "",
                          std::shared_ptr<ScheduleOptions> schedule_options =
                              std::make_shared<ScheduleOptions>())
      : job_id_(job_id),
        nodegroup_id_(nodegroup_id),
        schedule_options_(std::move(schedule_options)) {}

  /// \brief Check if the node has been already selected or not.
  /// \param node The node id.
  /// \return the node has selected or not.
  virtual bool IsNodeSelected(const NodeID &node) const {
    return std::find(selected_nodes.begin(), selected_nodes.end(), node) !=
           selected_nodes.end();
  }

  /// \brief If the context has any selected nodes.
  /// \return True if there are any selected node, else False.
  virtual bool HasAnySelectedNode() const { return !selected_nodes.empty(); }

  /// \brief Get the job ID.
  /// \return ID of the job associated with the context.
  const JobID &GetJobId() const { return job_id_; }

  /// \brief Get the nodegorup ID.
  /// \return ID of the nodegroup associated with the context.
  const std::string &GetNodegroupId() const { return nodegroup_id_; }

  /// \brief Get the schedule options.
  /// \return the Schedule options.
  std::shared_ptr<ScheduleOptions> GetScheduleOptions() const {
    return schedule_options_;
  }

  /// \brief Set the schedule options.
  /// \return the Schedule options.
  void SetScheduleOptions(std::shared_ptr<ScheduleOptions> schedule_options) {
    RAY_CHECK(schedule_options);
    schedule_options_ = schedule_options;
  }

  /// \brief Whether this schedule context need a placement group.
  bool RequirePlacementGroup() const { return !placement_group_bundle_id_.first.IsNil(); }

  const BundleID &GetPlacementGroupBundleID() const { return placement_group_bundle_id_; }

  void SetPlacementGroupBundleID(const BundleID &bundle_id) {
    placement_group_bundle_id_ = bundle_id;
  }

  virtual std::string ToString() const {
    std::ostringstream ostr;
    ostr << "job_id: " << job_id_ << ", nodegroup_id: " << nodegroup_id_
         << ", selected_node_count: " << selected_nodes.size()
         << ", placement_group_id: " << placement_group_bundle_id_.first
         << ", bundle_index: " << placement_group_bundle_id_.second;
    return ostr.str();
  }

  virtual ~ResourceScheduleContext() {}

  /// Save already scheduled nodes when scheduling
  std::vector<NodeID> selected_nodes;

  /// Save the time consumed by each part of the schedule
  ScheduleStat stat_;

 protected:
  JobID job_id_;
  std::string nodegroup_id_;
  std::shared_ptr<ScheduleOptions> schedule_options_;
  BundleID placement_group_bundle_id_;
};

/// Inherited from ResourceScheduleContext, used by ResourceBasedSchedulingStrategy for
/// gcs actor scheduling
class GcsActorScheduleContext : public ResourceScheduleContext {
 public:
  using ResourceScheduleContext::ResourceScheduleContext;
  virtual ~GcsActorScheduleContext() {}
};

class GcsActorAffinityWithBundleScheduleContext : public ResourceScheduleContext {
 public:
  using ResourceScheduleContext::ResourceScheduleContext;
  virtual ~GcsActorAffinityWithBundleScheduleContext() {}

  const boost::optional<const BundleLocationIndex &>
      &GetPlacementGroupBundleLocationIndex() const {
    return placement_group_bundle_location_index_;
  }

  void SetPlacementGroupBundleLocationIndex(
      const BundleLocationIndex &bundle_location_index) {
    placement_group_bundle_location_index_ = bundle_location_index;
  }

 private:
  boost::optional<const BundleLocationIndex &> placement_group_bundle_location_index_;
};

class GcsActorAffinityScheduleContext : public ResourceScheduleContext {
 public:
  using ResourceScheduleContext::ResourceScheduleContext;

  GcsActorAffinityScheduleContext(const JobID &job_id, const std::string &nodegroup_id,
                                  const std::string &ray_namespace,
                                  std::shared_ptr<ScheduleOptions> schedule_options,
                                  const GcsLabelManager &gcs_label_manager,
                                  const rpc::SchedulingStrategy &scheduling_strategy)
      : ResourceScheduleContext(job_id, nodegroup_id, std::move(schedule_options)),
        gcs_label_manager_(gcs_label_manager),
        scheduling_strategy_(scheduling_strategy),
        ray_namespace_(ray_namespace) {}

  virtual ~GcsActorAffinityScheduleContext() {}

  const rpc::SchedulingStrategy &GetSchedulingStrategy() const {
    return scheduling_strategy_;
  }

  const GcsLabelManager &GetGcsLabelManager() const { return gcs_label_manager_; }

  std::string GetRayNamespace() const { return ray_namespace_; }

 private:
  const GcsLabelManager &gcs_label_manager_;

  const rpc::SchedulingStrategy &scheduling_strategy_;

  std::string ray_namespace_;
};

class GcsNodeAffinityScheduleContext : public ResourceScheduleContext {
 public:
  using ResourceScheduleContext::ResourceScheduleContext;
  GcsNodeAffinityScheduleContext(
      const JobID &job_id, const std::string &nodegroup_id,
      const std::string &ray_namespace, std::shared_ptr<ScheduleOptions> schedule_options,
      const ray::rpc::NodeAffinitySchedulingStrategy &node_affinity_strategy)
      : ResourceScheduleContext(job_id, nodegroup_id, std::move(schedule_options)) {
    nodes_.reserve(node_affinity_strategy.nodes_size());
    for (int i = 0; i < node_affinity_strategy.nodes_size(); i++) {
      nodes_.emplace(NodeID::FromBinary(node_affinity_strategy.nodes(i)));
    }
    soft_ = node_affinity_strategy.soft();
    anti_affinity_ = node_affinity_strategy.anti_affinity();
  }

  virtual ~GcsNodeAffinityScheduleContext() {}

  const absl::flat_hash_set<NodeID> &GetNodes() const { return nodes_; }

  bool IsSoft() const { return soft_; }

  bool IsAntiAffinity() const { return anti_affinity_; }

 private:
  absl::flat_hash_set<NodeID> nodes_;

  bool soft_;

  bool anti_affinity_;
};

/// Inherited from ResourceScheduleContext, used by GcsPlacementGroupScheduler for
/// placement group scheduling
class PGScheduleContext : public ResourceScheduleContext {
 public:
  PGScheduleContext(
      const JobID &job_id, const std::string &nodegroup_id,
      std::shared_ptr<ScheduleOptions> schedule_options,
      const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations)
      : ResourceScheduleContext(job_id, nodegroup_id, std::move(schedule_options)),
        bundle_locations_(bundle_locations) {}

  bool IsNodeSelected(const NodeID &node) const override;

  bool HasAnySelectedNode() const override;

  virtual ~PGScheduleContext() {}

 private:
  const absl::optional<std::shared_ptr<BundleLocations>> bundle_locations_;
};
}  // namespace gcs
}  // namespace ray
