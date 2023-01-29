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
#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/common/task/task_execution_spec.h"
#include "ray/common/task/task_spec.h"
#include "ray/gcs/gcs_server/actor_backoff_context.h"
#include "ray/gcs/gcs_server/gcs_actor_scheduler.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "src/ray/protobuf/gcs_service.pb.h"

class rdtsc_timer;

namespace ray {
namespace gcs {
struct PendingActorClassDescriptor {
 public:
  explicit PendingActorClassDescriptor(ResourceSet rs,
                                       rpc::SchedulingStrategy scheduling_strategy)
      : resource_set(std::move(rs)),
        scheduling_strategy(std::move(scheduling_strategy)) {}
  ResourceSet resource_set;
  rpc::SchedulingStrategy scheduling_strategy;

  bool operator==(const PendingActorClassDescriptor &other) const {
    return resource_set == other.resource_set &&
           scheduling_strategy == other.scheduling_strategy;
  }
};
}  // namespace gcs
}  // namespace ray

namespace std {
template <>
struct hash<ray::gcs::PendingActorClassDescriptor> {
  size_t operator()(
      const ray::gcs::PendingActorClassDescriptor &pending_actor_cls) const {
    size_t hash = std::hash<ray::ResourceSet>()(pending_actor_cls.resource_set);
    hash ^=
        std::hash<ray::rpc::SchedulingStrategy>()(pending_actor_cls.scheduling_strategy);
    return hash;
  }
};
}  // namespace std

namespace ray {
namespace gcs {

using NodeClientFactoryFn =
    std::function<std::shared_ptr<TaskControlInterface>(const rpc::Address &address)>;

/// GcsActor just wraps `ActorTableData` and provides some convenient interfaces to access
/// the fields inside `ActorTableData`.
/// This class is not thread-safe.
class GcsActor {
 public:
  /// Create a GcsActor by actor_table_data.
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  explicit GcsActor(rpc::ActorTableData actor_table_data,
                    const std::string &nodegroup_id = "")
      : actor_table_data_(std::move(actor_table_data)),
        nodegroup_id_(nodegroup_id),
        actor_id_(ActorID::FromBinary(actor_table_data_.actor_id())) {}

  /// Create a GcsActor by actor_table_data.
  ///
  /// \param actor_table_data Data of the actor (see gcs.proto).
  /// \param task_spec Contains the actor creation task specification.
  explicit GcsActor(rpc::ActorTableData actor_table_data, ray::rpc::TaskSpec task_spec,
                    const std::string &nodegroup_id = "")
      : actor_table_data_(std::move(actor_table_data)),
        task_spec_(std::unique_ptr<rpc::TaskSpec>(new rpc::TaskSpec(task_spec))),
        nodegroup_id_(nodegroup_id),
        actor_id_(ActorID::FromBinary(actor_table_data_.actor_id())),
        labels_(task_spec_->actor_creation_task_spec().labels().begin(),
                task_spec_->actor_creation_task_spec().labels().end()) {}

  /// Create a GcsActor by TaskSpec.
  ///
  /// \param task_spec Contains the actor creation task specification.
  explicit GcsActor(const ray::rpc::TaskSpec &task_spec, std::string ray_namespace,
                    const std::string &nodegroup_id = "") {
    RAY_CHECK(task_spec.type() == TaskType::ACTOR_CREATION_TASK);
    const auto &actor_creation_task_spec = task_spec.actor_creation_task_spec();
    actor_table_data_.set_actor_id(actor_creation_task_spec.actor_id());
    actor_table_data_.set_job_id(task_spec.job_id());
    actor_table_data_.set_max_restarts(actor_creation_task_spec.max_actor_restarts());
    actor_table_data_.set_num_restarts(0);

    auto dummy_object = TaskSpecification(task_spec).ActorDummyObject().Binary();
    actor_table_data_.set_actor_creation_dummy_object_id(dummy_object);

    actor_table_data_.set_language(task_spec.language());
    actor_table_data_.set_extension_data(
        task_spec.actor_creation_task_spec().extension_data());
    actor_table_data_.set_max_task_retries(
        task_spec.actor_creation_task_spec().max_task_retries());
    actor_table_data_.mutable_function_descriptor()->CopyFrom(
        task_spec.function_descriptor());

    actor_table_data_.set_is_detached(actor_creation_task_spec.is_detached());
    actor_table_data_.set_name(actor_creation_task_spec.name());
    actor_table_data_.mutable_owner_address()->CopyFrom(task_spec.caller_address());

    actor_table_data_.set_state(rpc::ActorTableData::DEPENDENCIES_UNREADY);

    actor_table_data_.mutable_address()->set_raylet_id(NodeID::Nil().Binary());
    actor_table_data_.mutable_address()->set_worker_id(WorkerID::Nil().Binary());

    task_spec_ = std::unique_ptr<rpc::TaskSpec>(new rpc::TaskSpec(task_spec));

    // Set required resources.
    auto resource_map =
        GetCreationTaskSpecification().GetRequiredPlacementResources().GetResourceMap();
    actor_table_data_.mutable_required_resources()->insert(resource_map.begin(),
                                                           resource_map.end());
    nodegroup_id_ = nodegroup_id;
    actor_table_data_.set_ray_namespace(ray_namespace);

    const auto &function_descriptor = task_spec.function_descriptor();
    switch (function_descriptor.function_descriptor_case()) {
    case rpc::FunctionDescriptor::FunctionDescriptorCase::kJavaFunctionDescriptor:
      actor_table_data_.set_class_name(
          function_descriptor.java_function_descriptor().class_name());
      break;
    case rpc::FunctionDescriptor::FunctionDescriptorCase::kPythonFunctionDescriptor:
      actor_table_data_.set_class_name(
          function_descriptor.python_function_descriptor().class_name());
      break;
    default:
      // TODO (Alex): Handle the C++ case, which we currently don't have an
      // easy equivalent to class_name for.
      break;
    }

    actor_table_data_.set_serialized_runtime_env(task_spec.serialized_runtime_env());

    const auto &extended_properties_map =
        MapFromProtobuf(actor_creation_task_spec.extended_properties());
    actor_table_data_.mutable_extended_properties()->insert(
        extended_properties_map.begin(), extended_properties_map.end());

    actor_id_ = ActorID::FromBinary(actor_table_data_.actor_id());
    for (const auto &entry : actor_creation_task_spec.labels()) {
      labels_.emplace(entry.first, entry.second);
    }
  }

  /// Get the node id on which this actor is created.
  NodeID GetNodeID() const;
  /// Get the id of the worker on which this actor is created.
  WorkerID GetWorkerID() const;
  /// Get the actor's owner ID.
  WorkerID GetOwnerID() const;
  /// Get the node ID of the actor's owner.
  NodeID GetOwnerNodeID() const;
  /// Get the address of the actor's owner.
  const rpc::Address &GetOwnerAddress() const;

  /// Update the `Address` of this actor (see gcs.proto).
  void UpdateAddress(const rpc::Address &address);
  /// Get the `Address` of this actor.
  const rpc::Address &GetAddress() const;

  /// Update the state of this actor.
  void UpdateState(rpc::ActorTableData::ActorState state);
  /// Get the state of this gcs actor.
  rpc::ActorTableData::ActorState GetState() const;

  /// Get the id of this actor.
  const ActorID &GetActorID() const;
  /// Returns whether or not this is a detached actor.
  bool IsDetached() const;
  /// Get the name of this actor.
  const std::string &GetName() const;
  /// Get the namespace of this actor.
  std::string GetRayNamespace() const;
  /// Get the task specification of this actor.
  TaskSpecification GetCreationTaskSpecification() const;
  /// Release task spec.
  void ResetTaskSpec();

  /// Is there a task spec.
  bool HasTaskSpec() const;

  /// Get the immutable ActorTableData of this actor.
  const rpc::ActorTableData &GetActorTableData() const;
  /// Get the mutable ActorTableData of this actor.
  rpc::ActorTableData *GetMutableActorTableData();

  UniqueID GetWorkerProcessID();

  void SetWorkerProcessID(const UniqueID &worker_process_id);

  /// Get the creation time of current actor.
  ///
  /// \return The creation time of `Mircosecond`.
  int64_t CreationTime() const { return creation_time_; };

  /// Mark the actor creation start.
  void MarkCreationStart();

  /// Set actor creation time.
  void SetCreationTime(int64_t creation_time);

  /// Mark the actor creation done.
  void MarkAsRestart();

  bool IsRestart() { return is_restart_; };

  uint64_t GetRetryLeasingTimes() const { return retry_leasing_times_; }

  void UpdateRetryLeasingTimes(uint64_t retry_leasing_times) {
    retry_leasing_times_ = retry_leasing_times;
  }

  const std::string &GetNodegroupId() const { return nodegroup_id_; }

  void SetJobName(const std::string &job_name) { job_name_ = job_name; }
  const std::string &GetJobName() { return job_name_; }

  void SetBackoffContext(std::unique_ptr<ActorBackoffContext> backoff_context) {
    backoff_context_ = std::move(backoff_context);
  }
  ActorBackoffContext *GetBackoffContext() const { return backoff_context_.get(); }

  void SetLastSlowSchedulingEventSendTimeMs(int64_t time_ms) {
    last_slow_scheduling_event_send_time_ms_ = time_ms;
  }
  int64_t LastSlowSchedulingEventSendTimeMs() const {
    return last_slow_scheduling_event_send_time_ms_;
  }

  const absl::flat_hash_map<std::string, std::string> &GetLabels() const;

  const rpc::SchedulingStrategy &GetSchedulingStrategy() const;

  SchedulingClass GetSchedulingClass() const;

  void SetSchedulingClass(SchedulingClass sched_id);

 private:
  /// The actor meta data which contains the task specification as well as the state of
  /// the gcs actor and so on (see gcs.proto).
  rpc::ActorTableData actor_table_data_;
  std::unique_ptr<rpc::TaskSpec> task_spec_;
  // ID of nodegroup that the actor belongs to.
  std::string nodegroup_id_;
  // ID of the actor.
  ActorID actor_id_;
  int64_t creation_time_ = 0;
  bool is_restart_ = false;
  uint64_t retry_leasing_times_ = 0;
  std::string job_name_;
  std::unique_ptr<ActorBackoffContext> backoff_context_;

  int64_t last_slow_scheduling_event_send_time_ms_ = 0;
  absl::flat_hash_map<std::string, std::string> labels_;
  /// Cached scheduling class of this actor. This is only used by pending actor scheduler.
  SchedulingClass sched_id_ = 0;
};

class GcsActorManagerL1Handler;

using RegisterActorCallback = std::function<void(std::shared_ptr<GcsActor>)>;
using CreateActorCallback =
    std::function<void(std::shared_ptr<GcsActor>, bool creation_cancelled)>;

/// GcsActorManager is responsible for managing the lifecycle of all actors.
/// This class is not thread-safe.
/// Actor State Transition Diagram:
///                                                        3
///  0                       1                   2        --->
/// --->DEPENDENCIES_UNREADY--->PENDING_CREATION--->ALIVE      RESTARTING
///             |                      |              |   <---      |
///           8 |                    7 |            6 |     4       | 5
///             |                      v              |             |
///              ------------------> DEAD <-------------------------
///
/// 0: When GCS receives a `RegisterActor` request from core worker, it will add an actor
/// to `registered_actors_` and `unresolved_actors_`.
/// 1: When GCS receives a `CreateActor` request from core worker, it will remove the
/// actor from `unresolved_actors_` and schedule the actor.
/// 2: GCS selects a node to lease worker. If the worker is successfully leased,
/// GCS will push actor creation task to the core worker, else GCS will select another
/// node to lease worker. If the actor is created successfully, GCS will add the actor to
/// `created_actors_`.
/// 3: When GCS detects that the worker/node of an actor is dead, it
/// will get actor from `registered_actors_` by actor id. If the actor's remaining
/// restarts number is greater than 0, it will reconstruct the actor.
/// 4: When the actor is successfully reconstructed, GCS will update its state to `ALIVE`.
/// 5: If the actor is restarting, GCS detects that its worker or node is dead and its
/// remaining restarts number is 0, it will update its state to `DEAD`. If the actor is
/// detached, GCS will remove it from `registered_actors_` and `created_actors_`. If the
/// actor is non-detached, when GCS detects that its owner is dead, GCS will remove it
/// from `registered_actors_`.
/// 6: When GCS detected that an actor is dead, GCS will
/// reconstruct it. If its remaining restarts number is 0, it will update its state to
/// `DEAD`. If the actor is detached, GCS will remove it from `registered_actors_` and
/// `created_actors_`. If the actor is non-detached, when GCS detects that its owner is
/// dead, it will destroy the actor and remove it from `registered_actors_` and
/// `created_actors_`.
/// 7: If the actor is non-detached, when GCS detects that its owner is
/// dead, it will destroy the actor and remove it from `registered_actors_` and
/// `created_actors_`.
/// 8: For both detached and non-detached actors, when GCS detects that
/// an actor's creator is dead, it will update its state to `DEAD` and remove it from
/// `registered_actors_` and `created_actors_`. Because in this case, the actor can never
/// be created. If the actor is non-detached, when GCS detects that its owner is dead, it
/// will update its state to `DEAD` and remove it from `registered_actors_` and
/// `created_actors_`.
class GcsActorManager : public rpc::ActorInfoHandler {
 public:
  /// Create a GcsActorManager
  ///
  /// \param scheduler Used to schedule actor creation tasks.
  /// \param gcs_table_storage Used to flush actor data to storage.
  /// \param gcs_pub_sub Used to publish gcs message.
  GcsActorManager(
      instrumented_io_context &io_context,
      std::shared_ptr<GcsActorSchedulerInterface> scheduler,
      std::shared_ptr<GcsJobManager> job_manager,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub, RuntimeEnvManager &runtime_env_manager,
      std::function<std::string(const JobID &)> get_ray_namespace,
      std::function<void(std::function<void(void)>, boost::posix_time::milliseconds)>
          run_delayed,
      const rpc::ClientFactoryFn &worker_client_factory = nullptr);

  ~GcsActorManager() = default;

  void HandleRegisterActor(const rpc::RegisterActorRequest &request,
                           rpc::RegisterActorReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleCreateActor(const rpc::CreateActorRequest &request,
                         rpc::CreateActorReply *reply,
                         rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetActorInfo(const rpc::GetActorInfoRequest &request,
                          rpc::GetActorInfoReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNamedActorInfo(const rpc::GetNamedActorInfoRequest &request,
                               rpc::GetNamedActorInfoReply *reply,
                               rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllActorInfo(const rpc::GetAllActorInfoRequest &request,
                             rpc::GetAllActorInfoReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleKillActorViaGcs(const rpc::KillActorViaGcsRequest &request,
                             rpc::KillActorViaGcsReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetJobDistribution(const rpc::GetJobDistributionRequest &request,
                                rpc::GetJobDistributionReply *reply,
                                rpc::SendReplyCallback send_reply_callback) override;

  /// Register actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param success_callback Will be invoked after the actor is created successfully or
  /// be invoked immediately if the actor is already registered to `registered_actors_`
  /// and its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an
  /// actor with the specified name already exists. The callback will not be called in
  /// this case.
  Status RegisterActor(const rpc::RegisterActorRequest &request,
                       RegisterActorCallback success_callback);

  /// Create actor asynchronously.
  ///
  /// \param request Contains the meta info to create the actor.
  /// \param callback Will be invoked after the actor is created successfully or if the
  /// actor creation is cancelled (e.g. due to the actor going out-of-scope or being
  /// killed before actor creation has been completed), or will be invoked immediately if
  /// the actor is already registered to `registered_actors_` and its state is `ALIVE`.
  /// \return Status::Invalid if this is a named actor and an actor with the specified
  /// name already exists. The callback will not be called in this case.
  Status CreateActor(const rpc::CreateActorRequest &request,
                     CreateActorCallback callback);

  /// Get the actor ID for the named actor. Returns nil if the actor was not found.
  /// \param name The name of the detached actor to look up.
  /// \returns ActorID The ID of the actor. Nil if the actor was not found.
  ActorID GetActorIDByName(const std::string &name,
                           const std::string &ray_namespace) const;

  /// Schedule actors in the `pending_actors_map_` map.
  /// This method should be called when new nodes are registered or resources
  /// change.
  void SchedulePendingActors();

  /// Schedule the specified pending actors.
  void SchedulePendingActors(
      const absl::flat_hash_set<ActorID> &to_be_scheduled_actor_ids);

  /// Handle a node death. This will restart all actors associated with the
  /// specified node id, including actors which are scheduled or have been
  /// created on this node. Actors whose owners have died (possibly due to this
  /// node being removed) will not be restarted. If any workers on this node
  /// owned an actor, those actors will be destroyed.
  ///
  /// \param node_id The specified node id.
  /// \param node_ip_address The ip address of the dead node.
  void OnNodeDead(const NodeID &node_id, const std::string &node_ip_address);

  /// Handle a worker failure. This will restart the associated actor, if any,
  /// which may be pending or already created. If the worker owned other
  /// actors, those actors will be destroyed.
  ///
  /// \param node_id ID of the node where the dead worker was located.
  /// \param worker_id ID of the dead worker.
  /// \param exit_type exit reason of the dead worker.
  /// \param creation_task_exception if this arg is set, this worker is died because of an
  /// exception thrown in actor's creation task.
  void OnWorkerDead(
      const NodeID &node_id, const WorkerID &worker_id, const std::string &worker_ip,
      const rpc::WorkerExitType disconnect_type,
      const std::shared_ptr<rpc::RayException> &creation_task_exception = nullptr);

  void OnWorkerDead(const NodeID &node_id, const WorkerID &worker_id);

  /// Handle actor creation task failure. This should be called
  /// - when scheduling an actor creation task is infeasible.
  /// - when actor cannot be created to the cluster (e.g., runtime environment ops
  /// failed).
  ///
  /// \param actor The actor whose creation task is infeasible.
  /// \param failure_type Scheduling failure type.
  /// \param scheduling_failure_message The scheduling failure error message.
  void OnActorSchedulingFailed(
      std::shared_ptr<GcsActor> actor,
      rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
      const std::string &scheduling_failure_message);

  /// Handle actor creation task success. This should be called when the actor
  /// creation task has been scheduled successfully.
  ///
  /// \param actor The actor that has been created.
  void OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  /// Delete non-detached actor information from durable storage once the associated job
  /// finishes.
  ///
  /// \param job_id The id of finished job.
  void OnJobFinished(const JobID &job_id);

  std::vector<std::weak_ptr<GcsActor>> GetAllActorsInNode(const NodeID &node_id) const;

  /// Get the created actors.
  ///
  /// \return The created actors.
  const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
      &GetCreatedActors() const;

  const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &GetRegisteredActors()
      const;

  const absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      &GetActorRegisterCallbacks() const;

  std::shared_ptr<GcsActor> GetRegisteredActor(const ActorID &actor_id) const;

  void SetL1Handler(std::shared_ptr<GcsActorManagerL1Handler> gcs_l1_handler);

  /// Collect stats from gcs actor manager in-memory data structures.
  void CollectStats() const;

  void SuspendSchedulingForJob(const JobID &job_id);

  void ResumeSchedulingForJob(const JobID &job_id);

  absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>
  GetPendingResourceDemands() const;

  /// Evict all actors which ttl is expired.
  void EvictExpiredActors();

  std::string DebugString() const;

 private:
  /// A data structure representing an actor's owner.
  struct Owner {
    Owner(std::shared_ptr<rpc::CoreWorkerClientInterface> client)
        : client(std::move(client)) {}
    /// A client that can be used to contact the owner.
    std::shared_ptr<rpc::CoreWorkerClientInterface> client;
    /// The IDs of actors owned by this worker.
    absl::flat_hash_set<ActorID> children_actor_ids;
  };

  /// Poll an actor's owner so that we will receive a notification when the
  /// actor has gone out of scope, or the owner has died. This should not be
  /// called for detached actors.
  void PollOwnerForActorOutOfScope(const std::shared_ptr<GcsActor> &actor);

  /// Destroy an actor that has gone out of scope. This cleans up all local
  /// state associated with the actor and marks the actor as dead. For owned
  /// actors, this should be called when all actor handles have gone out of
  /// scope or the owner has died.
  /// NOTE: This method can be called multiple times in out-of-order and should be
  /// idempotent.
  ///
  /// \param[in] actor_id The actor id to destroy.
  /// \param[in] death_cause The reason why actor is destroyed.
  /// \param[in] force_kill Whether destory the actor forcelly.
  void DestroyActor(const ActorID &actor_id, const rpc::ActorDeathCause &death_cause,
                    bool force_kill = true);

  /// Get unresolved actors that were submitted from the specified node.
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>
  GetUnresolvedActorsByOwnerNode(const NodeID &node_id) const;

  /// Get unresolved actors that were submitted from the specified worker.
  absl::flat_hash_set<ActorID> GetUnresolvedActorsByOwnerWorker(
      const NodeID &node_id, const WorkerID &worker_id) const;

  /// Reconstruct the specified actor.
  ///
  /// \param actor The target actor to be reconstructed.
  /// \param need_reschedule Whether to reschedule the actor creation task, sometimes
  /// users want to kill an actor intentionally and don't want it to be reconstructed
  /// again.
  /// \param death_cause Context about why this actor is dead. Should only be set when
  /// need_reschedule=false.
  void ReconstructActor(const ActorID &actor_id, bool need_reschedule,
                        const rpc::ActorDeathCause &death_cause,
                        bool need_backoff = false);

  /// Remove the specified actor from `unresolved_actors_`.
  ///
  /// \param actor The actor to be removed.
  void RemoveUnresolvedActor(const std::shared_ptr<GcsActor> &actor);

  /// Remove the specified actor from owner.
  ///
  /// \param actor The actor to be removed.
  void RemoveActorFromOwner(const std::shared_ptr<GcsActor> &actor);

  /// Kill the specified actor.
  ///
  /// \param actor_id ID of the actor to kill.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Notify CoreWorker to kill the specified actor.
  ///
  /// \param actor The actor to be killed.
  /// \param force_kill Whether to force kill an actor by killing the worker.
  /// \param no_restart If set to true, the killed actor will not be restarted anymore.
  void NotifyCoreWorkerToKillActor(const std::shared_ptr<GcsActor> &actor,
                                   bool force_kill = true, bool no_restart = true);

  /// Add the destroyed actor to the cache. If the cache is full, one actor is randomly
  /// evicted.
  ///
  /// \param actor The actor to be killed.
  void AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor);

  /// Evict one destoyed actor from sorted_destroyed_actor_list_ as well as
  /// destroyed_actors_.
  void EvictOneDestroyedActor();

  std::shared_ptr<rpc::ActorTableData> GenActorDataOnlyWithStates(
      const rpc::ActorTableData &actor) {
    auto actor_delta = std::make_shared<rpc::ActorTableData>();
    actor_delta->set_state(actor.state());
    actor_delta->mutable_death_cause()->CopyFrom(actor.death_cause());
    actor_delta->mutable_address()->CopyFrom(actor.address());
    actor_delta->set_num_restarts(actor.num_restarts());
    actor_delta->set_timestamp(actor.timestamp());
    actor_delta->set_pid(actor.pid());
    if (!actor.name().empty()) {
      actor_delta->set_name(actor.name());
    }
    return actor_delta;
  }

  /// Cancel actor which is either being scheduled or is pending scheduling.
  ///
  /// \param actor The actor to be cancelled.
  /// \param task_id The id of actor creation task to be cancelled.
  void CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor,
                               const TaskID &task_id);

  /// Get the alive or dead actor of the actor id.
  /// NOTE: The return value is not meant to be passed to other scope.
  /// This return value should be used only for a short-time usage.
  ///
  /// \param actor_id The id of the actor.
  /// \return Actor instance. The nullptr if the actor doesn't exist.
  ///
  const GcsActor *GetActor(const ActorID &actor_id) const;

  /// Get the number of created actors.
  int GetNumberOfCreatedActors();

  void ReportSlowActorScheduling() const;

  friend class GcsActorManagerL1Handler;

  /// Use time sharing style to schedule pending actors.
  /// All actors to be scheduled would be put into `pending_actors_`, while those which
  /// are suspended `suspended_job_to_actors_`. Actors in `pending_actors_` or
  /// `suspended_job_to_actors_` are indexed by `pending_actor_ids_`, for accessing
  /// easily. NOTE: This class is not thread-safe.
  class PendingActorsScheduler {
   public:
    explicit PendingActorsScheduler(
        instrumented_io_context &io_context,
        std::function<Status(std::shared_ptr<GcsActor>)> pending_actors_consumer);

    void ScheduleAll();

    void Add(std::shared_ptr<GcsActor> actor);

    bool Remove(const ActorID &actor_id);

    void SuspendJob(const JobID &job_id);

    void ResumeJob(const JobID &job_id);

    void OnJobFinished(const JobID &job_id);

    size_t GetPendingActorsCount() const;

    absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>
    GetPendingResourceDemands() const;

    bool HasActorAffinitySchedulePendingActor(
        const std::shared_ptr<GcsActor> &depend_actor);

   private:
    // TODO: To improve the efficiency of actor placement (similar to a bin-packing
    // problem), pending actors are currently sorted in decreasing order of memory
    // requirements. We will take multi-dimentional resources (CPU, memory, etc.) into
    // account in the future.
    struct ComparatorByMemory {
      double ExtractValueOrDefault(const PendingActorClassDescriptor &cls,
                                   const std::string &resourceName) const {
        return cls.resource_set.GetResourceMap().count(resourceName) == 0
                   ? 0.0
                   : cls.resource_set.GetResourceMap().at(resourceName);
      };
      bool operator()(const SchedulingClass &id_a, const SchedulingClass &id_b) const {
        if (id_a == id_b) {
          return false;
        }
        const auto &cls_a = PendingActorsScheduler::GetPendingActorClassDescriptor(id_a);
        const auto &cls_b = PendingActorsScheduler::GetPendingActorClassDescriptor(id_b);
        auto val_a = ExtractValueOrDefault(cls_a, kMemory_ResourceLabel);
        auto val_b = ExtractValueOrDefault(cls_b, kMemory_ResourceLabel);
        if (val_a > val_b) {
          return true;
        } else if (val_a < val_b) {
          return false;
        }

        // Memory resources are equal, next compare CPU and GPU (as
        // tie-breakers).
        val_a = ExtractValueOrDefault(cls_a, kCPU_ResourceLabel);
        val_b = ExtractValueOrDefault(cls_b, kCPU_ResourceLabel);
        if (val_a > val_b) {
          return true;
        } else if (val_a < val_b) {
          return false;
        }

        val_a = ExtractValueOrDefault(cls_a, kGPU_ResourceLabel);
        val_b = ExtractValueOrDefault(cls_b, kGPU_ResourceLabel);
        if (val_a > val_b) {
          return true;
        } else if (val_a < val_b) {
          return false;
        }

        // There are different customized resources (including PG) or scheduling
        // strategies between these two actors, so just use the scheduling class to sort.
        return id_a > id_b;
      }
    };
    typedef std::map<SchedulingClass, std::vector<std::shared_ptr<GcsActor>>,
                     ComparatorByMemory>
        ClassifiedActorsMap;
    typedef absl::flat_hash_map<std::string, ClassifiedActorsMap>
        NodegroupedClasssifiedActorsMap;
    void TimeSharingSchedule(
        std::shared_ptr<NodegroupedClasssifiedActorsMap> remaining_actors);

    void MovePendingActorsToRemainingQueue(
        std::shared_ptr<NodegroupedClasssifiedActorsMap> remaining_actors);

    Status ScheduleClassifiedActorsMap(ClassifiedActorsMap *remaining_actors,
                                       const rdtsc_timer *timer);

    void AddToPendingActorsMap(std::shared_ptr<GcsActor> actor);

    static const PendingActorClassDescriptor &GetPendingActorClassDescriptor(
        SchedulingClass id) {
      auto it = sched_id_to_pending_actor_cls_.find(id);
      RAY_CHECK(it != sched_id_to_pending_actor_cls_.end()) << "invalid id: " << id;
      return it->second;
    }

   private:
    instrumented_io_context &io_context_;
    PeriodicalRunner periodical_runner_;

    std::function<Status(std::shared_ptr<GcsActor>)> pending_actors_consumer_;
    /// Indicate whether it is scheduling.
    bool scheduling_ = false;
    /// Whether to pick up pending actors in current scheduling round.
    bool pick_up_pending_actors_ = true;
    absl::flat_hash_set<ActorID> pending_actor_ids_;
    NodegroupedClasssifiedActorsMap pending_actors_map_;
    absl::flat_hash_map<JobID, std::vector<std::shared_ptr<GcsActor>>>
        suspended_job_to_actors_;

    /// The three members below are only accessed by the main thread, so they
    /// are thread-safe.
    absl::flat_hash_map<PendingActorClassDescriptor, SchedulingClass>
        pending_actor_cls_to_sched_id_;
    static absl::flat_hash_map<SchedulingClass, PendingActorClassDescriptor>
        sched_id_to_pending_actor_cls_;
    int next_sched_id_ = 0;

    void RecordMetrics() const;
    FRIEND_TEST(GcsActorSchedulerWithGcsSchedulingTest, TestPendingActorsSchedule);
    FRIEND_TEST(GcsActorSchedulerWithGcsSchedulingTest,
                TestPendingActorsScheduleWithActorAffinity);
  };

  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  instrumented_io_context &io_context_;
  PeriodicalRunner periodical_runner_;

  /// Callbacks of pending `RegisterActor` requests.
  /// Maps actor ID to actor registration callbacks, which is used to filter duplicated
  /// messages from a driver/worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
      actor_to_register_callbacks_;
  /// Callbacks of actor creation requests.
  /// Maps actor ID to actor creation callbacks, which is used to filter duplicated
  /// messages come from a Driver/Worker caused by some network problems.
  absl::flat_hash_map<ActorID, std::vector<CreateActorCallback>>
      actor_to_create_callbacks_;
  /// All registered actors (unresolved and pending actors are also included).
  /// TODO(swang): Use unique_ptr instead of shared_ptr.
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> registered_actors_;
  /// All destroyed actors.
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> destroyed_actors_;
  /// The actors are sorted according to the timestamp, and the oldest is at the head of
  /// the list.
  std::list<std::pair<ActorID, int64_t>> sorted_destroyed_actor_list_;
  /// Maps actor names to their actor ID for lookups by name, first keyed by their
  /// namespace.
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, ActorID>>
      named_actors_;
  /// The actors which dependencies have not been resolved.
  /// Maps from worker ID to a client and the IDs of the actors owned by that worker.
  /// The actor whose dependencies are not resolved should be destroyed once it creator
  /// dies.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>>
      unresolved_actors_;
  /// The pending actors which will not be scheduled until there's a resource change.
  std::unique_ptr<PendingActorsScheduler> pending_actors_scheduler_;
  /// Map contains the relationship of node and created actors. Each node ID
  /// maps to a map from worker ID to the actor created on that worker.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>> created_actors_;
  /// Map from worker ID to a client and the IDs of the actors owned by that
  /// worker. An owned actor should be destroyed once it has gone out of scope,
  /// according to its owner, or the owner dies.
  absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, Owner>> owners_;

  /// The scheduler to schedule all registered actors.
  std::shared_ptr<gcs::GcsActorSchedulerInterface> gcs_actor_scheduler_;
  /// Job manager to get job info.
  std::shared_ptr<gcs::GcsJobManager> gcs_job_manager_;
  /// Used to update actor information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// A publisher for publishing gcs messages.
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  /// Factory to produce clients to workers. This is used to communicate with
  /// actors and their owners.
  rpc::ClientFactoryFn worker_client_factory_;
  /// The l1 fault tolerance handler.
  /// Only used when L1 fault tolerance is enabled.
  std::shared_ptr<GcsActorManagerL1Handler> gcs_l1_handler_;
  /// A callback to get the namespace an actor belongs to based on its job id. This is
  /// necessary for actor creation.
  std::function<std::string(const JobID &)> get_ray_namespace_;
  RuntimeEnvManager &runtime_env_manager_;
  /// Run a function on a delay. This is useful for guaranteeing data will be
  /// accessible for a minimum amount of time.
  std::function<void(std::function<void(void)>, boost::posix_time::milliseconds)>
      run_delayed_;
  const boost::posix_time::milliseconds actor_gc_delay_;

  // Debug info.
  enum CountType {
    REGISTER_ACTOR_REQUEST = 0,
    CREATE_ACTOR_REQUEST = 1,
    GET_ACTOR_INFO_REQUEST = 2,
    GET_NAMED_ACTOR_INFO_REQUEST = 3,
    GET_ALL_ACTOR_INFO_REQUEST = 4,
    KILL_ACTOR_REQUEST = 5,
    GET_JOB_DISTRIBUTION_REQUEST = 6,
    CountType_MAX = 7,
  };
  uint64_t counts_[CountType::CountType_MAX] = {0};

  FRIEND_TEST(GcsActorSchedulerWithGcsSchedulingTest, TestPendingActorsSchedule);
  FRIEND_TEST(GcsActorSchedulerWithGcsSchedulingTest,
              TestPendingActorsScheduleWithActorAffinity);
  FRIEND_TEST(GcsActorManagerTest, TestEvictDestoryedActors);
};

/// GcsActorManagerL1Handler is responsible for managing the fault tolerance of actors
/// when L1 fault tolerance is enabled. This class is not thread-safe.
class GcsActorManagerL1Handler {
 public:
  /// Create a GcsActorManagerL1Handler
  GcsActorManagerL1Handler(
      std::shared_ptr<GcsJobManager> gcs_job_manager,
      std::shared_ptr<GcsActorManager> gcs_actor_manager,
      instrumented_io_context &l1fo_backoff_io_service,
      const NodeClientFactoryFn &node_client_factory = nullptr,
      std::function<std::shared_ptr<absl::flat_hash_set<NodeID>>(const JobID &job_id)>
          get_job_nodes = nullptr,
      std::function<
          absl::optional<std::shared_ptr<rpc::GcsNodeInfo>>(const ray::NodeID &node_id)>
          get_node_info = nullptr,
      std::function<void()> trigger_pending_actors_scheduling = nullptr);

  ~GcsActorManagerL1Handler() = default;

  /// If the job enable level1 fault tolerance and its worker/node is dead before GCS
  /// restarts, we will restart all actors of the job when GCS is restarted.
  ///
  /// \param actors The information of all actors.
  void Initialize(const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &actors);

  /// Restart the specified actor.
  ///
  /// \param actor_id The id of the actor to be restarted.
  void HandleReconstructActor(const ActorID &actor_id);

  /// Handle actor restart success. This should be called when the actor
  /// is successfully restarted.
  ///
  /// \param actor_id The id of the actor which is successfully restarted.
  void OnActorRestartSucceeded(const ActorID &actor_id);

  /// Handle a node death.
  ///
  /// \param node_id The specified node id.
  void OnNodeDead(const NodeID &node_id);

  void OnJobFinished(const JobID &job_id);

 private:
  /// Kill all actors of the given job.
  ///
  /// \param job_id The id of the job.
  void KillAllActorsOfJob(const JobID &job_id);

  /// Restart all actors for the given job.
  ///
  /// \param job_id The id of the job.
  void RestartActorsForJob(const JobID &job_id,
                           const std::shared_ptr<JobTableData> &job_info);

  /// Get an existing node client or connect a new one.
  ///
  /// \param node_id The id of the node.
  std::shared_ptr<TaskControlInterface> GetOrConnectNodeClient(
      const ray::NodeID &node_id);

  /// Stop tasks and ban new tasks of the specified job.
  ///
  /// \param job_id The id of the job.
  void BanNewTasks(const JobID &job_id);

  /// Lift new tasks ban of the specified job.
  ///
  /// \param job_id The id of the job.
  /// \param job_info The information of the job.
  /// \param gcs_starts If this is triggered when gcs is restarting.
  void LiftNewTasksBan(const JobID &job_id, const std::shared_ptr<JobTableData> &job_info,
                       bool is_gcs_restarting);

  void LiftNewTasksBanL1fo(const JobID &job_id,
                           const std::shared_ptr<JobTableData> &job_info);

  /// Check whether the actor needs to be restarted.
  ///
  /// \param actor The information of the actor.
  /// \param job_info The information of the job.
  bool IsActorNeedToRestart(const std::shared_ptr<GcsActor> &actor,
                            const std::shared_ptr<rpc::JobTableData> &job_info);

  /// Try to do l1fo with backoff mechanism.
  void TryToDoL1foWithBackoff(const JobID &job_id);

  /// Return the timestamp in milliseconds of the next time to trigger l1fo.
  int64_t Backoff(int64_t last_l1fo_timestamp_ms, uint32_t current_round);

  /// A private helper that get current timeout in milliseconds.
  int64_t GetCurrentTimeMs() { return absl::GetCurrentTimeNanos() / (1000 * 1000); }

  /// Job manager to get job info.
  std::shared_ptr<gcs::GcsJobManager> gcs_job_manager_;
  /// Actor manager.
  std::shared_ptr<GcsActorManager> gcs_actor_manager_;

  /// The ioservice that is used to drive the timers for l1fo backoff events.
  instrumented_io_context &l1fo_backoff_io_service_;

  /// The cached node clients which are used to communicate with raylet to stop tasks.
  /// Only used when L1 fault tolerance is enabled.
  absl::flat_hash_map<NodeID, std::shared_ptr<TaskControlInterface>> remote_node_clients_;
  /// Factory for producing new clients to stop tasks from remote nodes.
  NodeClientFactoryFn node_client_factory_;
  /// Get nodes of the specified job.
  std::function<std::shared_ptr<absl::flat_hash_set<NodeID>>(const JobID &job_id)>
      get_job_nodes_;
  /// Get node info function.
  std::function<absl::optional<std::shared_ptr<rpc::GcsNodeInfo>>(
      const ray::NodeID &node_id)>
      get_node_info_;
  /// Actors to restart.
  absl::flat_hash_map<JobID, absl::flat_hash_set<ActorID>> actors_to_restart_;
  /// Actors restarting.
  absl::flat_hash_map<JobID, absl::flat_hash_set<ActorID>> actors_restarting_;
  /// Jobs restarting.
  absl::flat_hash_set<JobID> jobs_restarting_;
  /// Callback to Trigger pending actor to start scheduling.
  std::function<void()> trigger_pending_actors_scheduling_;

  /**
   * L1FO Backoff Mechanism
   *
   * The interval between 2 rounds l1fo will increase exponentially by 2.
   * And the initial interval coefficient is 1 second.
   *
   * For example, the intervals between every rounds of l1fo are:
   * 1s  2s  4s  8s 16s etc.
   *
   * If current interval between this l1fo and last one > max_l1fo_interval_minutes,
   * we treat the last l1fo successed. And this is the 1st interval of this mechanism.
   */
  struct L1foInfo {
    int64_t last_l1fo_timestamp_ms = 0;
    uint32_t current_round = 0;
    std::shared_ptr<boost::asio::steady_timer> timer = nullptr;
  };

  absl::flat_hash_map<JobID, L1foInfo> all_l1fo_info_;
};

}  // namespace gcs
}  // namespace ray
