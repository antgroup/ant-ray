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
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_job_distribution.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class GcsActor;
class GcsResourceScheduler;
class GcsLabelManager;

/// \class GcsActorScheduleStrategyInterface
///
/// Used for different kinds of actor scheduling strategy.
class GcsActorScheduleStrategyInterface {
 public:
  virtual ~GcsActorScheduleStrategyInterface() = default;

  /// Select a node to schedule the actor.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node. If the scheduling fails, nullptr is returned.
  virtual NodeID Schedule(std::shared_ptr<GcsActor> actor, Status *status = nullptr) = 0;

  /// Set process id for an actor.
  ///
  /// \param actor to be set.
  /// \param pid in which actor runs.
  virtual void SetPIDForWorkerProcess(std::shared_ptr<GcsActor> actor, int32_t pid) = 0;

  /// Cancel actor scheduling.
  ///
  /// \param actor_id ID of the actor to be canceled.
  /// \param worker_process_Id ID of the worker process on which the actor will be
  /// scheduled.
  virtual void CancelOnWorkerProcess(const ActorID &actor_id,
                                     const UniqueID &worker_process_id) = 0;

  /// \brief Handle the removal event of a node.
  ///
  /// \param node The removed node.
  /// \return The jobs deployed on the specified node.
  virtual absl::flat_hash_set<JobID> OnNodeRemoved(const NodeID &node_id) = 0;

  /// Handle the event of worker process dead.
  ///
  /// It will remove the gcs worker process from job scheduling context and return
  /// the resources allocated by the worker process back to the cluster resource pool.
  ///
  /// \param node_id ID of the node that the dead worker process was forked on.
  /// \param worker_process_id ID of the gcs worker process which is an instance of
  /// equivalence with the dead worker process.
  /// \param job_id ID of the job with which the
  /// worker process associated.
  virtual void OnWorkerProcessDead(const NodeID &node_id, const WorkerID &worker_id,
                                   const UniqueID &worker_process_id,
                                   const JobID &job_id) = 0;

  /// Check if the worker process is dead.
  ///
  /// \param worker_process_id ID of the gcs worker process which is an instance of
  /// equivalence with the dead worker process.
  /// \param job_id ID of the job with which the
  /// worker process associated.
  virtual bool IsWorkerProcessDead(const UniqueID &worker_process_id,
                                   const JobID &job_id) = 0;

  /// Handle the finished event of a job.
  ///
  /// \param job_id ID of the finished job.
  virtual void OnJobFinished(const JobID &job_id) = 0;

  virtual std::vector<rpc::NodeInfo> GetJobDistribution() const = 0;

  /// A helper function to format the policy states to a pretty string.
  virtual std::string ToString() const = 0;

  virtual void Initialize(const GcsInitData &gcs_init_data) = 0;
};

/// \class GcsRandomActorScheduleStrategy
///
/// This strategy will select node randomly from the node pool to schedule the actor.
class GcsRandomActorScheduleStrategy : public GcsActorScheduleStrategyInterface {
 public:
  /// Create a GcsRandomActorScheduleStrategy
  ///
  /// \param gcs_node_manager Node management of the cluster, which provides interfaces
  /// to access the node information.
  explicit GcsRandomActorScheduleStrategy(
      std::shared_ptr<GcsNodeManager> gcs_node_manager)
      : gcs_node_manager_(std::move(gcs_node_manager)) {}

  virtual ~GcsRandomActorScheduleStrategy() = default;

  /// Select a node to schedule the actor.
  ///
  /// \param actor The actor to be scheduled.
  /// \return The selected node. If the scheduling fails, nullptr is returned.
  NodeID Schedule(std::shared_ptr<GcsActor> actor, Status *status = nullptr) override;

  /// Set process id for an actor.
  ///
  /// \param actor to be set.
  /// \param pid in which actor runs.
  void SetPIDForWorkerProcess(std::shared_ptr<GcsActor> actor, int32_t pid) override {}

  /// Cancel actor scheduling.
  ///
  /// \param actor_id ID of the actor to be canceled.
  /// \param worker_process_Id ID of the worker process on which the actor will be
  /// scheduled.
  void CancelOnWorkerProcess(const ActorID &actor_id,
                             const UniqueID &worker_process_id) override {}

  /// \brief Handle the addition event of a node.
  ///
  /// \param node The removed node.
  /// \return The jobs deployed on the specified node.
  absl::flat_hash_set<JobID> OnNodeRemoved(const NodeID &node_id) override {
    return absl::flat_hash_set<JobID>();
  }

  /// Check if the worker process is dead.
  bool IsWorkerProcessDead(const UniqueID &worker_process_id,
                           const JobID &job_id) override {
    return false;
  }

  /// Handle the event of worker process dead.
  void OnWorkerProcessDead(const NodeID &node_id, const WorkerID &worker_id,
                           const UniqueID &worker_process_id,
                           const JobID &job_id) override {}

  /// Handle the finished event of a job.
  ///
  /// \param job_id ID of the finished job.
  void OnJobFinished(const JobID &job_id) override {}

  std::vector<rpc::NodeInfo> GetJobDistribution() const override {
    return std::vector<rpc::NodeInfo>();
  }

  /// A helper function to format the policy states to a pretty string.
  std::string ToString() const override { return ""; }

  void Initialize(const GcsInitData &gcs_init_data) override {}

 private:
  /// Select a node from alive nodes randomly.
  ///
  /// \return The selected node. If the scheduling fails, `nullptr` is returned.
  std::shared_ptr<rpc::GcsNodeInfo> SelectNodeRandomly() const;

  /// The node manager.
  std::shared_ptr<GcsNodeManager> gcs_node_manager_;
};

/// \class ResourceBasedSchedulingStrategy
///
/// This policy will select node based on the actual usages of resources.
class ResourceBasedSchedulingStrategy : public GcsActorScheduleStrategyInterface {
 public:
  /// Create a ResourceBasedSchedulingStrategy
  ///
  /// \param gcs_job_distribution Instance of the `GcsJobDistribution` which records
  /// scheduling information of jobs running on each node.
  explicit ResourceBasedSchedulingStrategy(
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::shared_ptr<GcsJobDistribution> gcs_job_distribution,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<GcsResourceScheduler> resource_scheduler,
      std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager,
      std::function<bool(const BundleID &)> is_bundle_resource_reserved =
          [](const BundleID &) { return true; },
      std::function<bool(const NodeID &, const std::string &)>
          is_node_in_nodegroup_callback =
              [](const NodeID &, const std::string &) { return true; },
      std::shared_ptr<GcsLabelManager> gcs_label_manager = nullptr);

  /// Select a node to schedule the actor.
  ///
  /// \param actor to be scheduled.
  /// \return ID of the selected node.
  NodeID Schedule(std::shared_ptr<GcsActor> actor, Status *status = nullptr) override;

  /// Set process id for an actor.
  ///
  /// \param actor to be set.
  /// \param pid in which actor runs.
  void SetPIDForWorkerProcess(std::shared_ptr<GcsActor> actor, int32_t pid) override;

  /// Cancel actor scheduling.
  ///
  /// \param actor_id ID of the actor to be canceled.
  /// \param worker_process_Id ID of the worker process on which the actor will be
  /// scheduled.
  void CancelOnWorkerProcess(const ActorID &actor_id,
                             const UniqueID &worker_process_id) override;

  /// Handle the event of node dead.
  ///
  /// \param node_id ID of the dead node.
  /// \return The jobs deployed on the specified node.
  absl::flat_hash_set<JobID> OnNodeRemoved(const NodeID &node_id) override;

  /// Check if the worker process is dead.
  ///
  /// \param worker_process_id ID of the gcs worker process which is an instance of
  /// equivalence with the dead worker process.
  /// \param job_id ID of the job with which the
  /// worker process associated.
  bool IsWorkerProcessDead(const UniqueID &worker_process_id,
                           const JobID &job_id) override;

  /// Handle the event of worker process dead.
  ///
  /// It will remove the gcs worker process from job scheduling context and return
  /// the resources allocated by the worker process back to the cluster resource pool.
  ///
  /// \param node_id ID of the node that the dead worker process was forked on.
  /// \param worker_process_id ID of the gcs worker process which is an instance of
  /// equivalence with the dead worker process.
  /// \param job_id ID of the job with which the worker process associated.
  void OnWorkerProcessDead(const NodeID &node_id, const WorkerID &worker_id,
                           const UniqueID &worker_process_id,
                           const JobID &job_id) override;

  /// Handle the finished event of a job.
  ///
  /// 1) It will remove the resource tag associated with the specified job.
  /// 2) Remove the scheduling context of the specified to prevent memory from leaking.
  /// 3) Return all resources allocated by the worker process associated with the
  /// specified job back to the cluster resource pool.
  ///
  /// \param job_id ID of the finished job.
  void OnJobFinished(const JobID &job_id) override;

  Status UpdateJobResourceRequirements(const rpc::JobTableData &job_table_data);

  std::vector<rpc::NodeInfo> GetJobDistribution() const override;

  /// Add cluster resources changed event handler.
  void AddClusterResourcesChangedListener(std::function<void()> listener);

  /// A helper function to format the policy states to a pretty string.
  std::string ToString() const override;

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data) override;

 private:
  /// Select an exist or allocate a new worker process to schedule the actor.
  std::shared_ptr<GcsWorkerProcess> SelectOrAllocateWorkerProcess(
      std::shared_ptr<GcsActor> actor, bool need_sole_worker_process,
      Status *status = nullptr);

  /// Allocate a new worker from the cluster.
  ///
  /// \param job_scheduling_context Scheduling context of the job.
  /// \param required_resources The resources that the worker required.
  /// \param is_shared If the worker is shared by multiple actors or not.
  /// \param task_spec The specification of the task.
  std::shared_ptr<GcsWorkerProcess> AllocateNewWorkerProcess(
      std::shared_ptr<ray::gcs::GcsJobSchedulingContext> job_scheduling_context,
      const ResourceSet &required_resources, bool is_shared,
      const TaskSpecification &task_spec, Status *status = nullptr);

  /// Allocate resources for the specified job.
  ///
  /// \param job_id ID of the specified job.
  /// \param required_resources The resources to be allocated.
  /// \return ID of the node from which the resources are allocated.
  NodeID AllocateResources(
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
      const ResourceSet &required_resources, const TaskSpecification &task_spec);

  /// Notify that the cluster resources are changed.
  void NotifyClusterResourcesChanged();

  NodeContext GetHighestScoreNodeResource(
      const ResourceSet &required_resources,
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context) const;

  void WarnResourceAllocationFailure(
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
      const TaskSpecification &task_spec, const ResourceSet &required_resources) const;

 protected:
  /// The gcs resources manager.
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  /// Instance of the `GcsJobDistribution` which records scheduling information of jobs
  /// running on each node.
  std::shared_ptr<GcsJobDistribution> gcs_job_distribution_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::function<bool(const BundleID &)> is_bundle_resource_reserved_;
  /// The callback to check if the node is in the nodegroup.
  std::function<bool(const NodeID &, const std::string &)> is_node_in_nodegroup_callback_;
  /// The resource changed listeners.
  std::vector<std::function<void()>> resource_changed_listeners_;
  /// Gcs resource scheduler
  std::shared_ptr<GcsResourceScheduler> resource_scheduler_;
  /// Gcs placement group manager
  std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager_;
  /// Gcs actor label manager
  std::shared_ptr<GcsLabelManager> gcs_label_manager_;
};

}  // namespace gcs
}  // namespace ray
