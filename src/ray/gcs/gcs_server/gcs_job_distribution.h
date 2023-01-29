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
#include <memory>
#include <queue>
#include <thread>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"
#include "ray/common/id_util.h"
#include "ray/common/status.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/util/resource_util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
using rpc::Language;
namespace gcs {

using WorkerProcessResourceChangedListener =
    std::function<void(const NodeID &node_id, const ResourceSet &old_resources,
                       const ResourceSet &new_resources)>;

struct GcsJobConfig {
  explicit GcsJobConfig(const JobID &job_id, const std::string &nodegroup_id,
                        const std::string &ray_namespace,
                        uint32_t num_java_workers_per_process,
                        uint64_t java_worker_process_default_memory_units,
                        uint64_t total_memory_units, uint64_t max_total_memory_units = 0,
                        const std::string &job_name = "",
                        bool enable_job_quota = RayConfig::instance().enable_job_quota())
      : job_id_(job_id),
        job_name_(job_name),
        nodegroup_id_(nodegroup_id),
        ray_namespace_(ray_namespace),
        enable_job_quota_(enable_job_quota) {
    if (num_java_workers_per_process != 0) {
      num_java_workers_per_process_ = num_java_workers_per_process;
    }

    if (java_worker_process_default_memory_units > 0) {
      java_worker_process_default_memory_units_ =
          java_worker_process_default_memory_units;
    }

    if (total_memory_units > 0) {
      total_memory_units_ = total_memory_units;
    }

    if (max_total_memory_units == 0) {
      max_total_memory_units_ = total_memory_units_;
    } else {
      max_total_memory_units_ = max_total_memory_units;
    }
  }

  std::string ToString() const {
    std::ostringstream ostr;
    ostr << "{ job_id: " << job_id_ << ", nodegroup_id: " << nodegroup_id_
         << ", num_java_workers_per_process: " << num_java_workers_per_process_
         << ", java_worker_process_default_memory_gb: "
         << FromMemoryUnitsToGiB(java_worker_process_default_memory_units_)
         << ", python_worker_process_default_memory_gb: "
         << FromMemoryUnitsToGiB(python_worker_process_default_memory_units_)
         << ", total_memory_gb: " << FromMemoryUnitsToGiB(total_memory_units_)
         << ", max_total_memory_gb: " << FromMemoryUnitsToGiB(max_total_memory_units_)
         << " }";
    return ostr.str();
  }

  JobID job_id_;
  std::string job_name_;
  std::string nodegroup_id_;
  std::string ray_namespace_;
  // The number of workers per worker process.
  uint32_t num_java_workers_per_process_ = 1;
  // The default memory of java worker process in memory units.
  uint64_t java_worker_process_default_memory_units_ = 250ULL * 1024 * 1024;
  // The default memory of python worker process in units.
  uint64_t python_worker_process_default_memory_units_ = 50ULL * 1024 * 1024;
  // The total memory units that the job can use.
  uint64_t total_memory_units_ =
      RayConfig::instance().default_job_total_memory_mb() * 1024 * 1024;
  // The max of total memory units that the job can use.
  uint64_t max_total_memory_units_ = total_memory_units_;
  bool enable_job_quota_ = RayConfig::instance().enable_job_quota();
};

/// `GcsWorkerProcess` is an abstract of real worker process forked on the raylet.
/// It contains multiple slots, and each of them can bind to an actor.
class GcsWorkerProcess : public std::enable_shared_from_this<GcsWorkerProcess> {
  enum WorkerProcessStatus {
    IN_MEMORY,
    FLUSHING,
    FLUSHED,
  };

  /// Create a GcsWorkerProcess
  ///
  /// \param worker_process_id ID uniquely identify this gcs worker process.
  /// \param node_id ID of node on which this gcs worker process is allocated.
  /// \param job_id ID of job that this gcs worker process belongs to.
  /// \param acquired_resources Resources owned by this gcs worker process.
  /// \param is_shared A flag to represent that whether this worker process is a shared
  /// worker process.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  explicit GcsWorkerProcess(const UniqueID &worker_process_id, const NodeID &node_id,
                            const JobID &job_id, Language language,
                            const ResourceSet &acquired_resources, bool is_shared,
                            size_t slot_capacity = 1,
                            bool runtime_resource_scheduling_enabled = true)
      : worker_process_id_(worker_process_id),
        node_id_(node_id),
        job_id_(job_id),
        language_(language),
        acquired_resources_(acquired_resources),
        required_resources_(acquired_resources),
        is_shared_(is_shared),
        slot_capacity_(slot_capacity),
        is_for_placement_group_(acquired_resources.ContainsPlacementGroup()),
        bundle_id_(std::make_pair(PlacementGroupID::Nil(), -1)) {
    if (!is_shared) {
      RAY_CHECK(slot_capacity == 1);
    }
    original_required_runtime_resoruces_ =
        CalcOriginalRequiredRuntimeResources(&bundle_id_);
    runtime_resources_ = original_required_runtime_resoruces_;

    if (is_for_placement_group_ && runtime_resource_scheduling_enabled) {
      InitializePGAcquiredResources();
    }
  }

 public:
  /// Create a GcsWorkerProcess
  ///
  /// \param worker_process_id ID uniquely identify this gcs worker process.
  /// \param node_id ID of node on which this gcs worker process is allocated.
  /// \param job_id ID of job that this gcs worker process belongs to.
  /// \param acquired_resources Resources owned by this gcs worker process.
  /// \param is_shared A flag to represent that whether this worker process is a shared
  /// worker process.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  static std::shared_ptr<GcsWorkerProcess> Create(
      const UniqueID &worker_process_id, const NodeID &node_id, const JobID &job_id,
      Language language, const ResourceSet &acquired_resources, bool is_shared,
      size_t slot_capacity = 1, bool is_flushed = false,
      bool runtime_resource_scheduling_enabled = true) {
    return std::shared_ptr<GcsWorkerProcess>(new GcsWorkerProcess(
        worker_process_id, node_id, job_id, language, acquired_resources, is_shared,
        slot_capacity, runtime_resource_scheduling_enabled));
  }

  /// Create a GcsWorkerProcess with a random worker process id.
  ///
  /// \param node_id ID of node on which this gcs worker process is allocated.
  /// \param job_id ID of job that this gcs worker process belongs to.
  /// \param acquired_resources Resources owned by this gcs worker process.
  /// \param is_shared A flag to represent that whether this worker process is a shared
  /// worker process.
  /// \param slot_capacity The capacity of slots inside this worker
  /// process.
  static std::shared_ptr<GcsWorkerProcess> Create(
      const NodeID &node_id, const JobID &job_id, Language language,
      const ResourceSet &acquired_resources, bool is_shared, size_t slot_capacity = 1,
      bool is_flushed = false, bool runtime_resource_scheduling_enabled = true) {
    auto worker_process_id = ray::GenerateActorWorkerAssignmentId(job_id);
    return Create(worker_process_id, node_id, job_id, language, acquired_resources,
                  is_shared, slot_capacity, is_flushed,
                  runtime_resource_scheduling_enabled);
  }

  const UniqueID &GetWorkerProcessID() const;

  const int32_t &GetPID() const;

  void SetPID(const int32_t &pid);

  const NodeID &GetNodeID() const;

  void SetNodeID(const NodeID &node_id);

  const JobID &GetJobID() const;

  const Language &GetLanguage() const;

  size_t GetAvailableSlotCount() const;

  size_t GetUsedSlotCount() const;

  bool IsShared() const;

  size_t GetSlotCapacity() const;

  bool AssignActor(const ActorID &actor_id);

  const absl::flat_hash_set<ActorID> &GetAssignedActors() const;

  bool RemoveActor(const ActorID &actor_id);

  const ResourceSet &GetResources() const;

  ResourceSet &GetMutableResources();

  const ResourceSet &GetRuntimeResources() const;

  void UpdateRuntimeResources(const ResourceSet &resources);

  const ResourceSet &GetRequiredResources() const;

  ResourceSet GetConstraintResources();

  void AddResourceChangedListener(WorkerProcessResourceChangedListener listener);

  std::string ToString(int indent = 0) const;

  bool EqualsTo(std::shared_ptr<GcsWorkerProcess> other);

  void AsyncFlushRuntimeResources(
      const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage,
      std::function<void(const ray::Status &)> callback);

  /// Build worker process runtime resource data.
  std::shared_ptr<rpc::GcsWorkerProcessRuntimeResourceTableData>
  BuildWorkerProcessRuntimeResourceData() const;

  bool IsForPlacementGroup() const;
  const BundleID &GetBundleID() const;

  const ResourceSet &GetOriginalRequiredRuntimeResources() const;

  ResourceSet CalcOriginalRequiredRuntimeResources(BundleID *bundle_id) const;

  // Initialize the acquired resources specialized for a PG-dependent worker process.
  // If a PG-dependent worker process's `acquired_resources_` are:
  // memory_group_0_id: 10, memory_group_id: 10, CPU_group_0_id: 4, CPU_group_id: 4;
  // then the `pg_acquired_resources_` becomes:
  // memory: 10, CPU: 4
  void InitializePGAcquiredResources();

  const ResourceSet &GetPGAcquiredResources() const;

  ResourceSet &GetMutablePGAcquiredResources();

 private:
  /// ID uniquely identify this gcs worker process.
  UniqueID worker_process_id_;
  /// Process id of the worker.
  int32_t pid_ = 0;
  /// ID of node on which this gcs worker process is allocated.
  NodeID node_id_;
  /// ID of job that this gcs worker process belongs to.
  JobID job_id_;
  /// Language of the worker process.
  Language language_;
  /// Resources owned by this gcs worker process at runtime. Note, this
  /// variable is used for releasing and acquiring to/from a node,
  /// so it is limited by the node's resources.
  ResourceSet acquired_resources_;
  /// Resources owned by this gcs worker process at runtime (without limitation).
  /// The memory & CPU will be updated periodically.
  ResourceSet runtime_resources_;
  ResourceSet original_required_runtime_resoruces_;
  /// The initial required resources.
  const ResourceSet required_resources_;
  /// Resources owned by a PG-dependent worker process at runtime, only containing
  /// memory and CPU. This variable is also limited by the node's resources.
  ResourceSet pg_acquired_resources_;
  /// A flag to represent that whether this worker process is a shared worker process.
  bool is_shared_ = true;
  /// The capacity of slots inside this worker process.
  size_t slot_capacity_ = 1;

  /// IDs of actors that the gcs worker process acceptted.
  absl::flat_hash_set<ActorID> actor_ids_;
  /// Listeners to watch the change of resources of this worker process.
  std::vector<WorkerProcessResourceChangedListener> listeners_;

  bool is_for_placement_group_ = false;
  BundleID bundle_id_;
};

using WorkerProcessMap = absl::flat_hash_map<UniqueID, std::shared_ptr<GcsWorkerProcess>>;
using NodeToWorkerProcessesMap = absl::flat_hash_map<NodeID, WorkerProcessMap>;
struct ScheduleOptions;
/// `GcsJobSchedulingContext` represents scheduling status of a job.
/// It contains the job configuration, resources claimed when submiting, shared or sole
/// worker process of the job as well as the worker process distribution on the cluster
/// nodes.
class GcsResourceManager;
class GcsJobSchedulingContext {
 public:
  /// Create a `GcsJobSchedulingContext`
  ///
  /// \param job_config Configuration of the job.
  explicit GcsJobSchedulingContext(const GcsJobConfig &job_config,
                                   std::shared_ptr<ScheduleOptions> schedule_options);

  /// Add a worker process.
  ///
  /// \param worker_process to be added.
  /// \return true if the job available resources are enough, else false.
  bool AddWorkerProcess(std::shared_ptr<GcsWorkerProcess> worker_process);

  /// Get available resources of the job.
  const ResourceSet &GetAvailableResources() const;

  /// Get runtime resources of the job.
  /// NOTE: The job runtime resource requirements must between job's
  /// min_resource_requirements and max_resource_requirements. The job's
  /// min_resource_requirements is `min_resource_requirements_` while the max resource
  /// requirements are the total resources of `scheduling_resources_` without bundle
  /// resources.
  ResourceSet GetRuntimeResourceRequirements() const;

  /// Get scheduling resources of the job.
  const SchedulingResources &GetSchedulingResources() const;

  /// Get configuration of the job.
  const GcsJobConfig &GetJobConfig() const;

  /// Get mutable configuration of the job.
  GcsJobConfig *GetMutableJobConfig();

  /// Get schedule options of the job.
  std::shared_ptr<ScheduleOptions> GetScheduleOptions() const;

  /// Update job resource requirements.
  Status UpdateJobResourceRequirements(const rpc::JobTableData &job_table_data);

  /// Get shared worker processes.
  const WorkerProcessMap &GetSharedWorkerProcesses() const;

  /// Get sole worker processes.
  const WorkerProcessMap &GetSoleWorkerProcesses() const;

  /// Get node to worker processes distribution.
  const NodeToWorkerProcessesMap &GetNodeToWorkerProcesses() const;

  /// Remove worker processes by the specified node id.
  ///
  /// The resources of removed worker processes will be released to the job scheduling
  /// resources.
  ///
  /// \param node_id ID of the specified node.
  /// \return Worker processes associated with the specified node.
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> RemoveWorkerProcessesByNodeID(
      const NodeID &node_id);

  /// Remove worker process by the specified node id and worker process id.
  ///
  /// The resources of removed worker process will be released to the job scheduling
  /// resources.
  ///
  /// \param node_id ID of the specified node.
  /// \param worker_process_id ID of the gcs worker process to be removed.
  /// \return Worker process associated with the specified node id and worker process id.
  std::shared_ptr<GcsWorkerProcess> RemoveWorkerProcessByWorkerProcessID(
      const NodeID &node_id, const UniqueID &worker_process_id);

  /// Get worker process by the specified node id and worker process id.
  ///
  /// \param node_id ID of the specified node.
  /// \param worker_process_id ID of the gcs worker process to be get.
  /// \return Worker process associated with the specified node id and worker process id.
  std::shared_ptr<GcsWorkerProcess> GetWorkerProcess(
      const NodeID &node_id, const UniqueID &worker_process_id) const;

  /// A helper function to format the memory status to a pretty string.
  std::string ToString(int indent = 0) const;

  std::shared_ptr<GcsWorkerProcess> GetWorkerProcessById(
      const UniqueID &worker_process_id) const;

  bool ReserveBundlesResources(std::vector<std::shared_ptr<BundleSpecification>> bundles);

  // Subtract job's acquired resources when returning bundles.
  void SubtractJobAcquiredResources(
      std::vector<std::shared_ptr<BundleSpecification>> bundles);

  // Return bundle resources back to job's scheduling resources.
  void ReturnBundlesResources(std::vector<std::shared_ptr<BundleSpecification>> bundles);

  void OnJobResourcesInsufficiant(const ResourceSet &constraint_resources,
                                  const TaskSpecification &task_spec);

  const ResourceSet &GetShorttermRuntimeResources() const;

  void UpdateShorttermRuntimeResources(const ResourceSet &prev_resources,
                                       const ResourceSet &cur_resources);

  ResourceSet GetLongtermRuntimeResources() const;

  void UpdateLongtermRuntimeResources(const ResourceSet &resource_set);

  const ResourceSet &GetAcquiredResources() const;

 private:
  /// Configuration of the job.
  GcsJobConfig job_config_;
  /// Schedule options.
  std::shared_ptr<ScheduleOptions> schedule_options_;
  /// Shared worker processes.
  WorkerProcessMap shared_worker_processes_;
  /// Sole worker processes.
  WorkerProcessMap sole_worker_processes_;
  /// Node to worker distribution.
  NodeToWorkerProcessesMap node_to_worker_processes_;
  /// The total resources of the `scheduling_resources` is the max of the job's
  /// resource requirements.
  /// NOTE: `scheduling_resources` only contains constraint resoruces and the contraint
  /// bundle resources. Job constraint resource currently contains only memory.
  SchedulingResources scheduling_resources_;
  /// The min resource requirements that the job claimed when submitting.
  /// The runtime resources is max{min_resource_requirements_, used_resources}
  /// NOTE: It only contains constraint resoruces which currently contains only memory.
  ResourceSet min_resource_requirements_;

  /// The job's acquired resources.
  ResourceSet acquired_resources_;
  /// The job's long-term runtime resources.
  ResourceSet longterm_runtime_resources_;
  uint64_t last_report_time_ms_ = 0;

  /// The job's short-term runtime resources. It only serves as a runtime metric.
  ResourceSet shortterm_runtime_resources_;
};

class GcsWorkerProcessRuntimeResourceFlusher {
 public:
  void Flush(std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage);

  void Add(std::shared_ptr<GcsWorkerProcess> gcs_worker_process);

 private:
  std::queue<std::vector<std::weak_ptr<GcsWorkerProcess>>> queue_;
};

/// `GcsJobDistribution` represents job distribution on the cluster nodes.
/// It is responsible for the lifetime of all the `GcsJobSchedulingContext` and records
/// the distribution of jobs on each node.
class GcsJobDistribution {
 public:
  /// Create a `GcsJobDistribution`
  ///
  /// \param gcs_job_scheduling_factory Factory to create GcsJobSchedulingContext.
  explicit GcsJobDistribution(
      std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
          gcs_job_scheduling_factory,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager = nullptr,
      std::shared_ptr<GcsWorkerProcessRuntimeResourceFlusher> runtime_resource_flusher =
          nullptr);

  virtual ~GcsJobDistribution();

  /// Add a worker process.
  ///
  /// \param worker_process to be added.
  bool AddWorkerProcess(std::shared_ptr<GcsWorkerProcess> worker_process);

  /// Get job scheduling context by the specified job id.
  std::shared_ptr<GcsJobSchedulingContext> GetJobSchedulingContext(
      const JobID &job_id) const;

  /// Remove job scheduling context by the specified job id.
  ///
  /// \param ID of the specified job.
  /// \return The removed job scheduling context.
  std::shared_ptr<GcsJobSchedulingContext> RemoveJobSchedulingContext(
      const JobID &job_id);

  /// Find or create a job scheduling context if not exist.
  ///
  /// \param job_id ID of the specified job.
  /// \return job scheduling context found or created.
  std::shared_ptr<GcsJobSchedulingContext> FindOrCreateJobSchedulingContext(
      const JobID &job_id);

  /// Remove worker processes by node id and update the distribution of jobs on the node
  /// if needed.
  ///
  /// \param node_id The specified node id.
  /// \return The removed worker process.
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> RemoveWorkerProcessesByNodeID(
      const NodeID &node_id);

  /// Remove worker processes by the tuple(node_id, worker_process_id, job_id) and update
  /// the distribution of jobs on the node if needed.
  ///
  /// \param node_id ID of the specified node.
  /// \param worker_process_id ID of the gcs worker process to be removed.
  /// \param job_id ID of the job related with the worker process to be removed.
  /// \return Worker process associated with the specified tuple(node_id,
  /// worker_process_id,job_id).
  std::shared_ptr<GcsWorkerProcess> RemoveWorkerProcessByWorkerProcessID(
      const NodeID &node_id, const UniqueID &worker_process_id, const JobID &job_id);

  std::vector<rpc::NodeInfo> GetJobDistribution() const;

  /// Get map of node to jobs.
  const absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>> &GetNodeToJobs() const;

  /// Get all job scheduling contexts.
  const absl::flat_hash_map<JobID, std::shared_ptr<GcsJobSchedulingContext>>
      &GetAllJobSchedulingContexts() const;

  /// Gets jobs deployed on the specified node.
  ///
  /// \param node_id The specified node id.
  /// \return The jobs deployed on the specified node.
  absl::flat_hash_set<JobID> GetJobsByNodeID(const NodeID &node_id) const;

  std::shared_ptr<GcsWorkerProcess> GetWorkerProcessById(
      const JobID &job_id, const UniqueID &worker_process_id) const;

  absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
  OnRuntimeWorkerProcessResourcesUpdated(
      const std::shared_ptr<
          absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>
          worker_resources);

  void UpdateWorkerProcessAcquiredResources(
      std::shared_ptr<GcsWorkerProcess> worker_process,
      const ResourceSet &runtime_resources);

  // Return the delta between logical and runtime resources for a
  // PG-dependent worker process.
  void AdjustPGResources(std::shared_ptr<GcsWorkerProcess> worker_process);

  void OnRuntimeResourceSchedulingConfigChanged(
      const std::string &nodegroup_id, std::shared_ptr<ScheduleOptions> schedule_options);

  void CollectStats();

 private:
  void RemoveJobFromNode(const JobID &job_id, const NodeID &node_id);
  /// Initialize the worker process of the job scheduling context.
  void InitializeWorkerProcesses(
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context);

  void UpdateJobAndWorkerProcessShorttermRuntimeResources(
      std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
      std::shared_ptr<GcsWorkerProcess> worker_process,
      const ResourceSet &runtime_resources);

 private:
  /// The distribution of jobs on each node.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>> node_to_jobs_;
  /// Map from job id to job scheduling context.
  absl::flat_hash_map<JobID, std::shared_ptr<GcsJobSchedulingContext>>
      job_scheduling_contexts_;
  /// Factory to create GcsJobSchedulingContext.
  std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
      gcs_job_scheduling_factory_;
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;

  std::unique_ptr<std::thread> stats_thread_;
  instrumented_io_context stats_io_context_;

  std::shared_ptr<GcsWorkerProcessRuntimeResourceFlusher> runtime_resource_flusher_;
};
}  // namespace gcs
}  // namespace ray
