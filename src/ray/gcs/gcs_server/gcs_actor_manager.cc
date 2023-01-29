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

#include "ray/gcs/gcs_server/gcs_actor_manager.h"

#include <boost/regex.hpp>
#include <string>
#include <utility>

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/task_spec.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/rdtsc_timer.h"

namespace {
/// The error message constructed from below methods is user-facing, so please avoid
/// including too much implementation detail or internal information.
void AddActorInfo(const ray::gcs::GcsActor *actor,
                  ray::rpc::ActorDiedErrorContext *mutable_actor_died_error_ctx) {
  if (actor == nullptr) {
    return;
  }
  RAY_CHECK(mutable_actor_died_error_ctx != nullptr);
  mutable_actor_died_error_ctx->set_owner_id(actor->GetOwnerID().Binary());
  mutable_actor_died_error_ctx->set_owner_ip_address(
      actor->GetOwnerAddress().ip_address());
  mutable_actor_died_error_ctx->set_node_ip_address(actor->GetAddress().ip_address());
  mutable_actor_died_error_ctx->set_pid(actor->GetActorTableData().pid());
  mutable_actor_died_error_ctx->set_name(actor->GetName());
  mutable_actor_died_error_ctx->set_ray_namespace(actor->GetRayNamespace());
  mutable_actor_died_error_ctx->set_class_name(actor->GetActorTableData().class_name());
  mutable_actor_died_error_ctx->set_actor_id(actor->GetActorID().Binary());
  const auto actor_state = actor->GetState();
  mutable_actor_died_error_ctx->set_never_started(
      actor_state == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY ||
      actor_state == ray::rpc::ActorTableData::PENDING_CREATION);
}

const ray::rpc::ActorDeathCause GenNodeDiedCause(const ray::gcs::GcsActor *actor,
                                                 const std::string ip_address,
                                                 const NodeID &node_id) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(absl::StrCat(
      "The actor is dead because its node has died. Node Id: ", node_id.Hex()));
  return death_cause;
}

const ray::rpc::ActorDeathCause GenWorkerDiedCause(
    const ray::gcs::GcsActor *actor, const std::string &ip_address,
    const ray::rpc::WorkerExitType &disconnect_type,
    const std::string &disconnect_detail) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(absl::StrCat(
      "The actor is dead because its worker process has died. Worker exit type: ",
      ray::rpc::WorkerExitType_Name(disconnect_type),
      " Worker exit detail: ", disconnect_detail));
  return death_cause;
}
const ray::rpc::ActorDeathCause GenOwnerDiedCause(
    const ray::gcs::GcsActor *actor, const WorkerID &owner_id,
    const ray::rpc::WorkerExitType disconnect_type, const std::string &disconnect_detail,
    const std::string owner_ip_address) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(absl::StrCat(
      "The actor is dead because its owner has died. Owner Id: ", owner_id.Hex(),
      " Owner Ip address: ", owner_ip_address,
      " Owner worker exit type: ", ray::rpc::WorkerExitType_Name(disconnect_type),
      " Worker exit detail: ", disconnect_detail));
  return death_cause;
}

const ray::rpc::ActorDeathCause GenKilledByApplicationCause(
    const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because it was killed by `ray.kill`.");
  return death_cause;
}

const ray::rpc::ActorDeathCause GenActorOutOfScopeCause(const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because because all references to the actor were removed.");
  return death_cause;
}

const ray::rpc::ActorDeathCause GenJobDeadCause(const ray::gcs::GcsActor *actor) {
  ray::rpc::ActorDeathCause death_cause;
  auto actor_died_error_ctx = death_cause.mutable_actor_died_error_context();
  AddActorInfo(actor, actor_died_error_ctx);
  actor_died_error_ctx->set_error_message(
      "The actor is dead because because its job has dead.");
  return death_cause;
}
}  // namespace

namespace ray {
namespace gcs {

bool is_uuid(const std::string &str) {
  static const boost::regex e(
      "[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89aAbB][a-f0-9]{3}-[a-f0-9]{12}");
  return regex_match(str, e);  // note: case sensitive now
}

NodeID GcsActor::GetNodeID() const {
  const auto &raylet_id_binary = actor_table_data_.address().raylet_id();
  if (raylet_id_binary.empty()) {
    return NodeID::Nil();
  }
  return NodeID::FromBinary(raylet_id_binary);
}

void GcsActor::UpdateAddress(const rpc::Address &address) {
  actor_table_data_.mutable_address()->CopyFrom(address);
}

const rpc::Address &GcsActor::GetAddress() const { return actor_table_data_.address(); }

WorkerID GcsActor::GetWorkerID() const {
  const auto &address = actor_table_data_.address();
  if (address.worker_id().empty()) {
    return WorkerID::Nil();
  }
  return WorkerID::FromBinary(address.worker_id());
}

WorkerID GcsActor::GetOwnerID() const {
  return WorkerID::FromBinary(GetOwnerAddress().worker_id());
}

NodeID GcsActor::GetOwnerNodeID() const {
  return NodeID::FromBinary(GetOwnerAddress().raylet_id());
}

const rpc::Address &GcsActor::GetOwnerAddress() const {
  return actor_table_data_.owner_address();
}

void GcsActor::UpdateState(rpc::ActorTableData::ActorState state) {
  actor_table_data_.set_state(state);
}

rpc::ActorTableData::ActorState GcsActor::GetState() const {
  return actor_table_data_.state();
}

const ActorID &GcsActor::GetActorID() const { return actor_id_; }

bool GcsActor::IsDetached() const { return actor_table_data_.is_detached(); }

const std::string &GcsActor::GetName() const { return actor_table_data_.name(); }

std::string GcsActor::GetRayNamespace() const {
  return actor_table_data_.ray_namespace();
}

TaskSpecification GcsActor::GetCreationTaskSpecification() const {
  RAY_CHECK(task_spec_) << "actor_id = " << actor_id_;
  return TaskSpecification(*task_spec_);
}

void GcsActor::ResetTaskSpec() { task_spec_.reset(); }

bool GcsActor::HasTaskSpec() const { return (bool)task_spec_; }

const rpc::ActorTableData &GcsActor::GetActorTableData() const {
  return actor_table_data_;
}

rpc::ActorTableData *GcsActor::GetMutableActorTableData() { return &actor_table_data_; }

UniqueID GcsActor::GetWorkerProcessID() {
  return UniqueID::FromBinary(actor_table_data_.worker_process_id());
}

void GcsActor::SetWorkerProcessID(const UniqueID &worker_process_id) {
  actor_table_data_.set_worker_process_id(worker_process_id.Binary());
}

void GcsActor::MarkCreationStart() {
  auto creation_time = absl::GetCurrentTimeNanos() / 1000;
  creation_time_ = creation_time;
}

void GcsActor::SetCreationTime(int64_t creation_time) { creation_time_ = creation_time; }

void GcsActor::MarkAsRestart() { is_restart_ = true; }

const absl::flat_hash_map<std::string, std::string> &GcsActor::GetLabels() const {
  return labels_;
}

const rpc::SchedulingStrategy &GcsActor::GetSchedulingStrategy() const {
  RAY_CHECK(task_spec_);
  return task_spec_->scheduling_strategy();
}

SchedulingClass GcsActor::GetSchedulingClass() const { return sched_id_; }

void GcsActor::SetSchedulingClass(SchedulingClass sched_id) { sched_id_ = sched_id; }

/////////////////////////////////////////////////////////////////////////////////////////
GcsActorManager::PendingActorsScheduler::PendingActorsScheduler(
    instrumented_io_context &io_context,
    std::function<Status(std::shared_ptr<GcsActor>)> pending_actors_consumer)
    : io_context_(io_context),
      periodical_runner_(io_context),
      pending_actors_consumer_(std::move(pending_actors_consumer)) {
  RAY_CHECK(pending_actors_consumer_ != nullptr);
  periodical_runner_.RunFnPeriodically(
      [this] { RecordMetrics(); }, RayConfig::instance().metrics_report_interval_ms());
}

absl::flat_hash_map<SchedulingClass, PendingActorClassDescriptor>
    GcsActorManager::PendingActorsScheduler::sched_id_to_pending_actor_cls_;

void GcsActorManager::PendingActorsScheduler::ScheduleAll() {
  if (!pending_actors_map_.empty()) {
    pick_up_pending_actors_ = true;
    if (!scheduling_) {
      RAY_LOG(INFO) << "Schedule " << pending_actor_ids_.size() << " pending actors";
      scheduling_ = true;
      TimeSharingSchedule(std::make_shared<NodegroupedClasssifiedActorsMap>(
          std::move(pending_actors_map_)));
    }
  }
}

void GcsActorManager::PendingActorsScheduler::TimeSharingSchedule(
    std::shared_ptr<NodegroupedClasssifiedActorsMap> remaining_actors) {
  if (pick_up_pending_actors_) {
    MovePendingActorsToRemainingQueue(remaining_actors);
    pick_up_pending_actors_ = false;
  }

  std::unique_ptr<rdtsc_timer> timer = absl::make_unique<rdtsc_timer>(
      RayConfig::instance().gcs_task_scheduling_max_time_per_round_ms());
  bool timeout = false;
  for (auto it = remaining_actors->begin(); !timeout && it != remaining_actors->end();) {
    auto current = it++;
    timeout = ScheduleClassifiedActorsMap(&current->second, timer.get()).IsTimedOut();
    if (current->second.empty()) {
      remaining_actors->erase(current);
    }
  }
  if (timeout) {
    std::ostringstream ostr;
    ostr << "Timedout, " << pending_actor_ids_.size()
         << " actors are left to be scheduled. Remaining actors: {";
    for (auto &entry : *remaining_actors) {
      ostr << entry.first << ": " << entry.second.size() << ", ";
    }
    ostr << "}";
    RAY_LOG(INFO) << ostr.str();
    io_context_.post(
        [this, remaining_actors] { TimeSharingSchedule(std::move(remaining_actors)); });
  } else {
    if (!remaining_actors->empty()) {
      std::ostringstream ostr;
      ostr << "Re-scheduling skipped, " << pending_actor_ids_.size()
           << " actors are left to be scheduled. Remaining actors: {";
      for (auto &entry : *remaining_actors) {
        ostr << entry.first << ": " << entry.second.size() << ", ";
      }
      ostr << "}";
      RAY_LOG(INFO) << ostr.str();
      // Return back all the unscheduled actors to pending_actors_map_.
      for (auto &entry : *remaining_actors) {
        auto &classified_actors_map = pending_actors_map_[entry.first];
        for (auto &it : entry.second) {
          auto &vec = classified_actors_map[it.first];
          for (auto &actor : it.second) {
            if (pending_actor_ids_.contains(actor->GetActorID())) {
              vec.emplace_back(actor);
            }
          }
        }
      }
      remaining_actors->clear();
    } else {
      // All actors of this round are scheduled successfully.
      RAY_LOG(INFO) << "Actors of this round are re-scheduled successfully. "
                    << pending_actor_ids_.size() << " actors are left to be scheduled.";
    }
  }
  scheduling_ = false;
}

void GcsActorManager::PendingActorsScheduler::MovePendingActorsToRemainingQueue(
    std::shared_ptr<NodegroupedClasssifiedActorsMap> remaining_actors) {
  for (auto &entry : pending_actors_map_) {
    auto &classified_actors_map = (*remaining_actors)[entry.first];
    for (auto &it : entry.second) {
      auto &vec = classified_actors_map[it.first];
      vec.insert(vec.end(), it.second.begin(), it.second.end());
    }
  }
  pending_actors_map_.clear();
}

Status GcsActorManager::PendingActorsScheduler::ScheduleClassifiedActorsMap(
    ClassifiedActorsMap *remaining_actors, const rdtsc_timer *timer) {
  for (auto it = remaining_actors->begin(); it != remaining_actors->end();) {
    bool skip = false;
    auto &classified_remaining_actors = it->second;
    auto iter = classified_remaining_actors.begin();
    for (; iter != classified_remaining_actors.end(); iter++) {
      if (!timer->timedout()) {
        auto actor = *iter;
        if (pending_actor_ids_.erase(actor->GetActorID())) {
          auto status = pending_actors_consumer_(actor);
          if (status.IsResourcesNotEnough()) {
            // If Actor affinity scheduling fails, it also return the state of
            // ResourcesNotEnough. So here to exclude the actor which is actor affinity
            // strategy.
            if (!(actor->HasTaskSpec() &&
                  actor->GetSchedulingStrategy()
                      .has_actor_affinity_scheduling_strategy())) {
              // No node can meet the resource requirements of this shape, just skip the
              // following actors.
              skip = true;
              break;
            }
          } else if (status.IsJobQuotaNotEnough()) {
            // TODO(Shanly): Multiple jobs share one queue, optimize later.
            // Just move on to next for the time being.
          } else {
            // status.ok()
            // status.IsInvalidJob()
            // status.IsJobSuspended()
            // status.IsActorInBackoff()
            // Just move on to next one.
          }
        }
      } else {
        // Erase the scheduled ones from classified_remaining_actors.
        // Note: the `iter` is not scheduled, so we just erase the range of
        // [classified_remaining_actors.begin(), iter)
        classified_remaining_actors.erase(classified_remaining_actors.begin(), iter);
        std::string message =
            "Time exceeded while scheduling pending actors, break and wait another "
            "round.";
        RAY_LOG(DEBUG) << message;
        return Status::TimedOut(message);
      }
    }

    if (skip) {
      // Erase the scheduled ones from classified_remaining_actors.
      classified_remaining_actors.erase(classified_remaining_actors.begin(), ++iter);
      // Move on to the next shape.
      it++;
    } else {
      // All actors in this shape have been scheduled, so just completely erase them.
      it = remaining_actors->erase(it);
    }
  }
  return Status::OK();
}

void GcsActorManager::PendingActorsScheduler::AddToPendingActorsMap(
    std::shared_ptr<GcsActor> actor) {
  auto sched_id = actor->GetSchedulingClass();
  if (sched_id == 0) {
    PendingActorClassDescriptor pending_actor_cls(
        actor->GetCreationTaskSpecification().GetRequiredResources(),
        actor->GetCreationTaskSpecification().GetSchedulingStrategy());
    auto it = pending_actor_cls_to_sched_id_.find(pending_actor_cls);
    if (it == pending_actor_cls_to_sched_id_.end()) {
      sched_id = ++next_sched_id_;
      // TODO(Chong-Li) we might want to try cleaning up actor types in these cases
      if (sched_id >= 1000 && sched_id % 100 == 0) {
        RAY_LOG(INFO) << "More than " << sched_id
                      << " types of pending actors, this may reduce performance.";
      }
      pending_actor_cls_to_sched_id_[pending_actor_cls] = sched_id;
      sched_id_to_pending_actor_cls_.emplace(sched_id, std::move(pending_actor_cls));
    } else {
      sched_id = it->second;
    }
    actor->SetSchedulingClass(sched_id);
  }
  auto &classified_actors_map = pending_actors_map_[actor->GetNodegroupId()];
  classified_actors_map[sched_id].emplace_back(std::move(actor));
}

void GcsActorManager::PendingActorsScheduler::Add(std::shared_ptr<GcsActor> actor) {
  if (pending_actor_ids_.emplace(actor->GetActorID()).second) {
    if (suspended_job_to_actors_.count(actor->GetActorID().JobId())) {
      suspended_job_to_actors_[actor->GetActorID().JobId()].emplace_back(
          std::move(actor));
    } else {
      AddToPendingActorsMap(std::move(actor));
    }
  }
}

bool GcsActorManager::PendingActorsScheduler::Remove(const ActorID &actor_id) {
  if (pending_actor_ids_.erase(actor_id) != 0) {
    for (auto &classified_actors_map : pending_actors_map_) {
      for (auto &actor_queue : classified_actors_map.second) {
        for (auto iter = actor_queue.second.begin(); iter != actor_queue.second.end();
             iter++) {
          if (iter->get()->GetActorID() == actor_id) {
            actor_queue.second.erase(iter);
            if (actor_queue.second.empty()) {
              classified_actors_map.second.erase(actor_queue.first);
              if (classified_actors_map.second.empty()) {
                pending_actors_map_.erase(classified_actors_map.first);
              }
            }
            return true;
          }
        }
      }
    }
    auto job_iter = suspended_job_to_actors_.find(actor_id.JobId());
    if (job_iter != suspended_job_to_actors_.end()) {
      for (auto iter = job_iter->second.begin(); iter != job_iter->second.end(); iter++) {
        if (iter->get()->GetActorID() == actor_id) {
          job_iter->second.erase(iter);
          break;
        }
      }
    }
    return true;
  }
  return false;
}

void GcsActorManager::PendingActorsScheduler::SuspendJob(const JobID &job_id) {
  RAY_LOG(INFO) << "Suspend job " << job_id << " for scheduling.";
  suspended_job_to_actors_[job_id] = std::vector<std::shared_ptr<GcsActor>>();
}

void GcsActorManager::PendingActorsScheduler::ResumeJob(const JobID &job_id) {
  RAY_LOG(INFO) << "Resume job " << job_id << " from scheduling.";
  for (auto &actor : suspended_job_to_actors_[job_id]) {
    AddToPendingActorsMap(actor);
  }
  suspended_job_to_actors_.erase(job_id);
}

void GcsActorManager::PendingActorsScheduler::OnJobFinished(const JobID &job_id) {
  suspended_job_to_actors_.erase(job_id);
}

size_t GcsActorManager::PendingActorsScheduler::GetPendingActorsCount() const {
  return pending_actor_ids_.size();
}

absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>
GcsActorManager::PendingActorsScheduler::GetPendingResourceDemands() const {
  absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>> pending_demands;
  for (const auto &entry : pending_actors_map_) {
    auto &nodegroup_demands = pending_demands[entry.first];
    for (const auto &classified_entry : entry.second) {
      for (const auto &actor_entry : classified_entry.second) {
        if (pending_actor_ids_.contains(actor_entry->GetActorID())) {
          nodegroup_demands[actor_entry->GetCreationTaskSpecification()
                                .GetRequiredPlacementResources()] += 1;
        }
      }
    }
  }
  return pending_demands;
}

void GcsActorManager::PendingActorsScheduler::RecordMetrics() const {
  ray::stats::GcsPendingActorCount().Record(pending_actor_ids_.size());
}

bool GcsActorManager::PendingActorsScheduler::HasActorAffinitySchedulePendingActor(
    const std::shared_ptr<GcsActor> &depend_actor) {
  const auto &classified_actors_map = pending_actors_map_[depend_actor->GetNodegroupId()];
  const auto &labels = depend_actor->GetLabels();
  for (const auto &[key, actors] : classified_actors_map) {
    for (const auto &actor : actors) {
      if (pending_actor_ids_.contains(actor->GetActorID()) && actor->HasTaskSpec() &&
          actor->GetSchedulingStrategy().has_actor_affinity_scheduling_strategy()) {
        const auto &scheduling_strategy =
            actor->GetSchedulingStrategy().actor_affinity_scheduling_strategy();
        for (int i = 0; i < scheduling_strategy.match_expressions_size(); i++) {
          const auto &match_expression = scheduling_strategy.match_expressions(i);
          if (labels.contains(match_expression.key())) {
            return true;
          }
        }
      }
    }
  }
  return false;
}

/////////////////////////////////////////////////////////////////////////////////////////
GcsActorManager::GcsActorManager(
    instrumented_io_context &io_context,
    std::shared_ptr<GcsActorSchedulerInterface> scheduler,
    std::shared_ptr<GcsJobManager> job_manager,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub, RuntimeEnvManager &runtime_env_manager,
    std::function<std::string(const JobID &)> get_ray_namespace,
    std::function<void(std::function<void(void)>, boost::posix_time::milliseconds)>
        run_delayed,
    const rpc::ClientFactoryFn &worker_client_factory)
    : io_context_(io_context),
      periodical_runner_(io_context),
      gcs_actor_scheduler_(std::move(scheduler)),
      gcs_job_manager_(std::move(job_manager)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_pub_sub_(std::move(gcs_pub_sub)),
      worker_client_factory_(worker_client_factory),
      get_ray_namespace_(get_ray_namespace),
      runtime_env_manager_(runtime_env_manager),
      run_delayed_(run_delayed),
      actor_gc_delay_(RayConfig::instance().gcs_actor_table_min_duration_ms()) {
  RAY_CHECK(worker_client_factory_);
  pending_actors_scheduler_.reset(new PendingActorsScheduler(
      io_context_,
      /*pending_actors_consumer = */
      [this](std::shared_ptr<GcsActor> actor) {
        Status status = Status::OK();
        gcs_actor_scheduler_->Schedule(std::move(actor), &status);
        return status;
      }));
  periodical_runner_.RunFnPeriodically(
      [this] { ReportSlowActorScheduling(); },
      RayConfig::instance().slow_actor_scheduling_check_interval_ms());
}

void GcsActorManager::HandleRegisterActor(const rpc::RegisterActorRequest &request,
                                          rpc::RegisterActorReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Registering actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id
                << ", task_spec_byte_size = " << request.task_spec().ByteSizeLong();
  Status status =
      RegisterActor(request, [reply, send_reply_callback,
                              actor_id](const std::shared_ptr<gcs::GcsActor> &actor) {
        std::ostringstream ss;
        ss << "Registered actor, job id = " << actor_id.JobId()
           << ", actor id = " << actor_id;
        RAY_EVENT(INFO, EVENT_LABEL_ACTOR_REGISTRATION_FINISHED)
                .WithField("job_id", actor_id.JobId().Hex())
                .WithField("actor_id", actor_id.Hex())
            << ss.str();
        RAY_LOG(INFO) << ss.str();
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      });
  if (!status.ok()) {
    std::ostringstream ss;
    ss << "Failed to register actor: " << status.ToString()
       << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;
    RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_REGISTRATION_FINISHED)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << ss.str();
    RAY_LOG(ERROR) << ss.str();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::REGISTER_ACTOR_REQUEST];
}

void GcsActorManager::HandleCreateActor(const rpc::CreateActorRequest &request,
                                        rpc::CreateActorReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(request.task_spec().type() == TaskType::ACTOR_CREATION_TASK);
  auto actor_id =
      ActorID::FromBinary(request.task_spec().actor_creation_task_spec().actor_id());

  RAY_LOG(INFO) << "Creating actor, job id = " << actor_id.JobId()
                << ", actor id = " << actor_id
                << ", task_spec_byte_size = " << request.task_spec().ByteSizeLong();
  Status status = CreateActor(request, [reply, send_reply_callback, actor_id](
                                           const std::shared_ptr<gcs::GcsActor> &actor,
                                           bool creation_cancelled) {
    if (creation_cancelled) {
      // Actor creation is cancelled.
      RAY_LOG(INFO) << "Actor creation was cancelled, job id = " << actor_id.JobId()
                    << ", actor id = " << actor_id;
      reply->mutable_death_cause()->CopyFrom(actor->GetActorTableData().death_cause());
      GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                         Status::SchedulingCancelled("Actor creation cancelled."));
    } else {
      RAY_LOG(INFO) << "Finished creating actor, job id = " << actor_id.JobId()
                    << ", actor id = " << actor_id;

      {  // Record metric.
        auto end_time = absl::GetCurrentTimeNanos() / 1000;

        const ray::stats::TagsType extra_tag = {
            {ray::stats::JobNameKey, actor->GetJobName()}};
        ray::stats::ActorCreationElapsedTime().Record(end_time - actor->CreationTime(),
                                                      extra_tag);

        const ray::stats::TagsType job_name_tag = {
            {ray::stats::JobNameKey, actor->GetJobName()}};
        ray::stats::ActorCreationCount().Record(1, job_name_tag);
      }

      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    }
  });
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to create actor, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id << ", status: " << status.ToString();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
  ++counts_[CountType::CREATE_ACTOR_REQUEST];
}

void GcsActorManager::HandleGetActorInfo(const rpc::GetActorInfoRequest &request,
                                         rpc::GetActorInfoReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  ActorID actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Getting actor info"
                 << ", job id = " << actor_id.JobId() << ", actor id = " << actor_id;

  const auto &registered_actor_iter = registered_actors_.find(actor_id);
  if (registered_actor_iter != registered_actors_.end()) {
    reply->mutable_actor_table_data()->CopyFrom(
        registered_actor_iter->second->GetActorTableData());
  } else {
    const auto &destroyed_actor_iter = destroyed_actors_.find(actor_id);
    if (destroyed_actor_iter != destroyed_actors_.end()) {
      reply->mutable_actor_table_data()->CopyFrom(
          destroyed_actor_iter->second->GetActorTableData());
    }
  }

  RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id;
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleGetAllActorInfo(const rpc::GetAllActorInfoRequest &request,
                                            rpc::GetAllActorInfoReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all actor info.";
  ++counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST];
  if (request.show_dead_jobs() == false) {
    for (const auto &iter : registered_actors_) {
      reply->add_actor_table_data()->CopyFrom(iter.second->GetActorTableData());
    }
    for (const auto &iter : destroyed_actors_) {
      reply->add_actor_table_data()->CopyFrom(iter.second->GetActorTableData());
    }
    RAY_LOG(DEBUG) << "Finished getting all actor info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  RAY_CHECK(request.show_dead_jobs());
  // We don't maintain an in-memory cache of all actors which belong to dead
  // jobs, so fetch it from redis.
  Status status = gcs_table_storage_->ActorTable().GetAll(
      [reply, send_reply_callback](
          const std::unordered_map<ActorID, rpc::ActorTableData> &result) {
        for (const auto &pair : result) {
          reply->add_actor_table_data()->CopyFrom(pair.second);
        }
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        RAY_LOG(DEBUG) << "Finished getting all actor info.";
      });
  if (!status.ok()) {
    // Send the response to unblock the sender and free the request.
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsActorManager::HandleGetNamedActorInfo(
    const rpc::GetNamedActorInfoRequest &request, rpc::GetNamedActorInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const std::string &name = request.name();
  const std::string &ray_namespace = request.ray_namespace();
  RAY_LOG(DEBUG) << "Getting actor info, name = " << name
                 << " , namespace=" << ray_namespace;

  // Try to look up the actor ID for the named actor.
  ActorID actor_id = GetActorIDByName(name, ray_namespace);

  Status status = Status::OK();
  auto iter = registered_actors_.find(actor_id);
  if (actor_id.IsNil() || iter == registered_actors_.end()) {
    // The named actor was not found or the actor is already removed.
    std::stringstream stream;
    stream << "Actor with name '" << name << "' was not found.";
    RAY_LOG(WARNING) << stream.str();
    status = Status::NotFound(stream.str());
  } else {
    reply->mutable_actor_table_data()->CopyFrom(iter->second->GetActorTableData());
    RAY_LOG(DEBUG) << "Finished getting actor info, job id = " << actor_id.JobId()
                   << ", actor id = " << actor_id;
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  ++counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST];
}

void GcsActorManager::HandleKillActorViaGcs(const rpc::KillActorViaGcsRequest &request,
                                            rpc::KillActorViaGcsReply *reply,
                                            rpc::SendReplyCallback send_reply_callback) {
  const auto &actor_id = ActorID::FromBinary(request.actor_id());
  bool force_kill = request.force_kill();
  bool no_restart = request.no_restart();
  if (no_restart) {
    DestroyActor(actor_id, GenKilledByApplicationCause(GetActor(actor_id)));
  } else {
    KillActor(actor_id, force_kill, no_restart);
  }

  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished killing actor, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", force_kill = " << force_kill
                 << ", no_restart = " << no_restart;
  ++counts_[CountType::KILL_ACTOR_REQUEST];
}

void GcsActorManager::HandleGetJobDistribution(
    const rpc::GetJobDistributionRequest &request, rpc::GetJobDistributionReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  for (const auto &node_info : gcs_actor_scheduler_->GetJobDistribution()) {
    reply->add_node_info_list()->CopyFrom(node_info);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  ++counts_[CountType::GET_JOB_DISTRIBUTION_REQUEST];
}

Status GcsActorManager::RegisterActor(const ray::rpc::RegisterActorRequest &request,
                                      RegisterActorCallback success_callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to register actor
  // successfully.
  RAY_CHECK(success_callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter != registered_actors_.end()) {
    auto pending_register_iter = actor_to_register_callbacks_.find(actor_id);
    if (pending_register_iter != actor_to_register_callbacks_.end()) {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS client receives some network errors.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      pending_register_iter->second.emplace_back(std::move(success_callback));
    } else {
      // 1. The GCS client sends the `RegisterActor` request to the GCS server.
      // 2. The GCS server flushes the actor to the storage and restarts before replying
      // to the GCS client.
      // 3. The GCS client resends the `RegisterActor` request to the GCS server.
      success_callback(iter->second);
    }
    return Status::OK();
  }
  const auto job_id = JobID::FromBinary(request.task_spec().job_id());
  auto nodegroup_id = gcs_job_manager_->GetJobNodegroupId(job_id);
  // Use the namespace in task options by default. Otherwise use the
  // namespace from the job.
  std::string ray_namespace = actor_creation_task_spec.ray_namespace();
  if (ray_namespace.empty()) {
    ray_namespace = get_ray_namespace_(job_id);
  }
  auto actor =
      std::make_shared<GcsActor>(request.task_spec(), ray_namespace, nodegroup_id);
  actor->SetJobName(gcs_job_manager_->GetJobName(actor_id.JobId()));
  if (!actor->GetName().empty()) {
    auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
    auto it = actors_in_namespace.find(actor->GetName());
    if (it == actors_in_namespace.end()) {
      if (is_uuid(actor->GetRayNamespace()) && actor->IsDetached()) {
        std::ostringstream stream;
        stream
            << "It looks like you're creating a detached actor in an anonymous "
               "namespace. In order to access this actor in the future, you will need to "
               "explicitly connect to this namespace with ray.init(namespace=\""
            << actor->GetRayNamespace() << "\", ...)";

        auto error_data_ptr =
            gcs::CreateErrorTableData("detached_actor_anonymous_namespace", stream.str(),
                                      absl::GetCurrentTimeNanos(), job_id);

        RAY_LOG(WARNING) << error_data_ptr->SerializeAsString();
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                           error_data_ptr->SerializeAsString(), nullptr));
      }

      actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
    } else {
      std::stringstream stream;
      stream << "Actor with name '" << actor->GetName() << "' already exists.";
      return Status::Invalid(stream.str());
    }
  }

  actor->MarkCreationStart();

  actor_to_register_callbacks_[actor_id].emplace_back(std::move(success_callback));
  registered_actors_.emplace(actor->GetActorID(), actor);

  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  RAY_CHECK(unresolved_actors_[node_id][worker_id].emplace(actor->GetActorID()).second)
      << "node id = " << node_id << ", worker id = " << worker_id
      << "actor id = " << actor->GetActorID();

  if (!actor->IsDetached()) {
    // This actor is owned. Send a long polling request to the actor's
    // owner to determine when the actor should be removed.
    PollOwnerForActorOutOfScope(actor);
  } else {
    // If it's a detached actor, we need to register the runtime env it used to GC
    auto job_id = JobID::FromBinary(request.task_spec().job_id());
    const auto &uris = runtime_env_manager_.GetReferences(job_id.Hex());
    auto actor_id_hex = actor->GetActorID().Hex();
    for (const auto &uri : uris) {
      runtime_env_manager_.AddURIReference(actor_id_hex, uri);
    }
  }

  // The backend storage is supposed to be reliable, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTaskSpecTable().Put(actor_id, request.task_spec(),
                                                            [](const Status &status) {}));
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(), *actor->GetMutableActorTableData(),
      [this, actor](const Status &status) {
        // The backend storage is supposed to be reliable, so the status must be ok.
        RAY_CHECK_OK(status);
        // If a creator dies before this callback is called, the actor could have been
        // already destroyed. It is okay not to invoke a callback because we don't need
        // to reply to the creator as it is already dead.
        auto registered_actor_it = registered_actors_.find(actor->GetActorID());
        if (registered_actor_it == registered_actors_.end()) {
          // NOTE(sang): This logic assumes that the ordering of backend call is
          // guaranteed. It is currently true because we use a single TCP socket to call
          // the default Redis backend. If ordering is not guaranteed, we should overwrite
          // the actor state to DEAD to avoid race condition.
          return;
        }
        RAY_CHECK_OK(gcs_pub_sub_->Publish(ACTOR_CHANNEL, actor->GetActorID().Hex(),
                                           actor->GetActorTableData().SerializeAsString(),
                                           nullptr));
        // Invoke all callbacks for all registration requests of this actor
        // (duplicated requests are included) and remove all of them from
        // actor_to_register_callbacks_.
        // Reply to the owner to indicate that the actor has been registered.
        auto iter = actor_to_register_callbacks_.find(actor->GetActorID());
        RAY_CHECK(iter != actor_to_register_callbacks_.end() && !iter->second.empty())
            << "actor id = " << actor->GetActorID();
        auto callbacks = std::move(iter->second);
        actor_to_register_callbacks_.erase(iter);
        for (auto &callback : callbacks) {
          callback(actor);
        }
      }));
  return Status::OK();
}

Status GcsActorManager::CreateActor(const ray::rpc::CreateActorRequest &request,
                                    CreateActorCallback callback) {
  // NOTE: After the abnormal recovery of the network between GCS client and GCS server or
  // the GCS server is restarted, it is required to continue to create actor
  // successfully.
  RAY_CHECK(callback);
  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());

  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(WARNING) << "Actor " << actor_id
                     << " may be already destroyed, job id = " << actor_id.JobId();
    return Status::Invalid("Actor may be already destroyed.");
  }

  if (iter->second->GetState() == rpc::ActorTableData::ALIVE) {
    // In case of temporary network failures, workers will re-send multiple duplicate
    // requests to GCS server.
    // In this case, we can just reply.
    callback(iter->second, false);
    return Status::OK();
  }

  auto actor_creation_iter = actor_to_create_callbacks_.find(actor_id);
  if (actor_creation_iter != actor_to_create_callbacks_.end()) {
    // It is a duplicate message, just mark the callback as pending and invoke it after
    // the actor has been successfully created.
    actor_creation_iter->second.emplace_back(std::move(callback));
    return Status::OK();
  }
  // Mark the callback as pending and invoke it after the actor has been successfully
  // created or if the creation has been cancelled (e.g. via ray.kill() or the actor
  // handle going out-of-scope).
  actor_to_create_callbacks_[actor_id].emplace_back(std::move(callback));

  // If GCS restarts while processing `CreateActor` request, GCS client will resend the
  // `CreateActor` request.
  // After GCS restarts, the state of the actor may not be `DEPENDENCIES_UNREADY`.
  if (iter->second->GetState() != rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    RAY_LOG(INFO) << "Actor " << actor_id
                  << " is already in the process of creation. Skip it directly, job id = "
                  << actor_id.JobId();
    return Status::OK();
  }

  // Remove the actor from the unresolved actor map.
  const auto job_id = JobID::FromBinary(request.task_spec().job_id());
  auto actor = std::make_shared<GcsActor>(request.task_spec(), get_ray_namespace_(job_id),
                                          iter->second->GetNodegroupId());
  actor->SetJobName(iter->second->GetJobName());
  actor->GetMutableActorTableData()->set_state(rpc::ActorTableData::PENDING_CREATION);
  actor->GetMutableActorTableData()->set_timestamp(current_sys_time_ms());
  actor->SetCreationTime(iter->second->CreationTime());
  const auto &actor_table_data = actor->GetActorTableData();
  // Pub this state for dashboard showing.
  RAY_CHECK_OK(gcs_pub_sub_->Publish(
      ACTOR_CHANNEL, actor_id.Hex(),
      GenActorDataOnlyWithStates(actor_table_data)->SerializeAsString(), nullptr));
  RemoveUnresolvedActor(actor);

  // Update the registered actor as its creation task specification may have changed due
  // to resolved dependencies.
  registered_actors_[actor_id] = actor;

  // Schedule the actor.
  gcs_actor_scheduler_->Schedule(actor);
  return Status::OK();
}

ActorID GcsActorManager::GetActorIDByName(const std::string &name,
                                          const std::string &ray_namespace) const {
  ActorID actor_id = ActorID::Nil();
  auto namespace_it = named_actors_.find(ray_namespace);
  if (namespace_it != named_actors_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      actor_id = it->second;
    }
  }
  return actor_id;
}

void GcsActorManager::PollOwnerForActorOutOfScope(
    const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_node_id = actor->GetOwnerNodeID();
  const auto &owner_id = actor->GetOwnerID();
  auto &workers = owners_[owner_node_id];
  auto it = workers.find(owner_id);
  if (it == workers.end()) {
    RAY_LOG(DEBUG) << "Adding owner " << owner_id << " of actor " << actor_id
                   << ", job id = " << actor_id.JobId();
    std::shared_ptr<rpc::CoreWorkerClientInterface> client =
        worker_client_factory_(actor->GetOwnerAddress());
    it = workers.emplace(owner_id, Owner(std::move(client))).first;
  }
  it->second.children_actor_ids.insert(actor_id);

  rpc::WaitForActorOutOfScopeRequest wait_request;
  wait_request.set_intended_worker_id(owner_id.Binary());
  wait_request.set_actor_id(actor_id.Binary());
  it->second.client->WaitForActorOutOfScope(
      wait_request, [this, owner_node_id, owner_id, actor_id](
                        Status status, const rpc::WaitForActorOutOfScopeReply &reply) {
        if (!status.ok()) {
          RAY_LOG(INFO) << "Worker " << owner_id
                        << " failed, destroying actor child, job id = "
                        << actor_id.JobId();
        } else {
          RAY_LOG(INFO) << "Actor " << actor_id
                        << " is out of scope, destroying actor, job id = "
                        << actor_id.JobId();
        }

        auto node_it = owners_.find(owner_node_id);
        if (node_it != owners_.end() && node_it->second.count(owner_id)) {
          // Only destroy the actor if its owner is still alive. The actor may
          // have already been destroyed if the owner died.
          auto job = gcs_job_manager_->GetJob(actor_id.JobId());
          // For multiple actors in one process, if one actor is out of scope,
          // We shouldn't force kill the actor because other actors in the process
          // are still alive.
          auto force_kill =
              job == nullptr || job->config().num_java_workers_per_process() <= 1;
          DestroyActor(actor_id, GenActorOutOfScopeCause(GetActor(actor_id)), force_kill);
        }
      });
}

void GcsActorManager::DestroyActor(const ActorID &actor_id,
                                   const rpc::ActorDeathCause &death_cause,
                                   bool force_kill) {
  RAY_LOG(INFO) << "Destroying actor, actor id = " << actor_id
                << ", job id = " << actor_id.JobId();
  actor_to_register_callbacks_.erase(actor_id);
  auto callback_it = actor_to_create_callbacks_.find(actor_id);
  const auto creation_callbacks = callback_it != actor_to_create_callbacks_.end()
                                      ? std::move(callback_it->second)
                                      : std::vector<CreateActorCallback>{};
  if (callback_it != actor_to_create_callbacks_.end()) {
    actor_to_create_callbacks_.erase(callback_it);
  }
  auto it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO) << "Tried to destroy actor that does not exist " << actor_id;
    return;
  }
  const auto &task_id = it->second->GetCreationTaskSpecification().TaskId();
  it->second->GetMutableActorTableData()->set_timestamp(current_sys_time_ms());
  AddDestroyedActorToCache(it->second);
  const auto actor = std::move(it->second);

  registered_actors_.erase(it);

  // Clean up the client to the actor's owner, if necessary.
  if (!actor->IsDetached()) {
    RemoveActorFromOwner(actor);
  } else {
    runtime_env_manager_.RemoveURIReference(actor->GetActorID().Hex());
  }

  // Remove actor from `named_actors_` if its name is not empty.
  if (!actor->GetName().empty()) {
    auto namespace_it = named_actors_.find(actor->GetRayNamespace());
    if (namespace_it != named_actors_.end()) {
      auto it = namespace_it->second.find(actor->GetName());
      if (it != namespace_it->second.end()) {
        namespace_it->second.erase(it);
      }
      // If we just removed the last actor in the namespace, remove the map.
      if (namespace_it->second.empty()) {
        named_actors_.erase(namespace_it);
      }
    }
  }

  // The actor is already dead, most likely due to process or node failure.
  if (actor->GetState() == rpc::ActorTableData::DEAD) {
    // Inform all creation callbacks that the actor is dead and that actor creation is
    // therefore cancelled.
    for (auto &callback : creation_callbacks) {
      callback(actor, true);
    }
    return;
  }

  // Remove actor labels.
  gcs_actor_scheduler_->RemoveActorLabels(actor);

  if (actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    // The actor creation task still has unresolved dependencies. Remove from the
    // unresolved actors map.
    RemoveUnresolvedActor(actor);
  } else {
    // The actor is still alive or pending creation. Clean up all remaining
    // state.
    const auto &node_id = actor->GetNodeID();
    const auto &worker_id = actor->GetWorkerID();
    auto node_it = created_actors_.find(node_id);
    if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
      // The actor has already been created. Destroy the process by force-killing
      // it.
      NotifyCoreWorkerToKillActor(actor, force_kill);
      RAY_CHECK(node_it->second.erase(actor->GetWorkerID()))
          << "actor id = " << actor_id << ", worker id = " << actor->GetWorkerID();
      if (node_it->second.empty()) {
        created_actors_.erase(node_it);
      }
    } else {
      if (!worker_id.IsNil()) {
        // The actor is in phase of creating, so we need to notify the core
        // worker exit to avoid process and resource leak.
        NotifyCoreWorkerToKillActor(actor, force_kill);
      }
      CancelActorInScheduling(actor, task_id);
    }
  }

  // Update the actor to DEAD in case any callers are still alive. This can
  // happen if the owner of the actor dies while there are still callers.
  // TODO(swang): We can skip this step and delete the actor table entry
  // entirely if the callers check directly whether the owner is still alive.
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
  auto time = current_sys_time_ms();
  mutable_actor_table_data->set_end_time(time);
  mutable_actor_table_data->set_timestamp(time);
  mutable_actor_table_data->mutable_death_cause()->CopyFrom(death_cause);

  auto actor_table_data =
      std::make_shared<rpc::ActorTableData>(*mutable_actor_table_data);
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor->GetActorID(), *actor_table_data,
      [this, actor, actor_table_data](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            ACTOR_CHANNEL, actor->GetActorID().Hex(),
            GenActorDataOnlyWithStates(*actor_table_data)->SerializeAsString(), nullptr));
        RAY_CHECK_OK(gcs_table_storage_->ActorTaskSpecTable().Delete(
            actor->GetActorID(), [actor](const Status &status) {
              if (!status.ok()) {
                RAY_LOG(ERROR) << "Failed to delete actor creation task spec "
                               << actor->GetActorID();
              }
              // Reset task spec to avoid memory leak.
              actor->ResetTaskSpec();
            }));
      }));

  // Inform all creation callbacks that the actor was cancelled, not created.
  for (auto &callback : creation_callbacks) {
    callback(actor, true);
  }
}

absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>>
GcsActorManager::GetUnresolvedActorsByOwnerNode(const NodeID &node_id) const {
  absl::flat_hash_map<WorkerID, absl::flat_hash_set<ActorID>> actor_ids_map;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    for (const auto &entry : iter->second) {
      const auto &owner_id = entry.first;
      auto &actor_ids = actor_ids_map[owner_id];
      actor_ids.insert(entry.second.begin(), entry.second.end());
    }
  }
  return actor_ids_map;
}

absl::flat_hash_set<ActorID> GcsActorManager::GetUnresolvedActorsByOwnerWorker(
    const NodeID &node_id, const WorkerID &worker_id) const {
  absl::flat_hash_set<ActorID> actor_ids;
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    if (it != iter->second.end()) {
      actor_ids.insert(it->second.begin(), it->second.end());
    }
  }
  return actor_ids;
}

void GcsActorManager::CollectStats() const {
  stats::PendingActors.Record(pending_actors_scheduler_->GetPendingActorsCount());
  /// TODO:(paer) The current version (ray-1.4-beta) is behind the "master" branch,
  /// and this func will be replaced by the "master"'s `GcsActorManager::RecordMetrics`.
  /// Move below codes into `GcsActorManager::RecordMetrics` if you find the func
  /// is defined.

  // <ActorStateKey, <JobName, Count>>
  absl::flat_hash_map<rpc::ActorTableData::ActorState,
                      absl::flat_hash_map<std::string, int>>
      state_jobname_records;
  // iterate all undead actors, and count actors of each STATE

  for (auto &entry : registered_actors_) {
    // get actor state and retrive the inner <job_name, count> map
    const std::shared_ptr<GcsActor> &actor = entry.second;
    if (state_jobname_records.find(actor->GetState()) == state_jobname_records.end()) {
      state_jobname_records[actor->GetState()] = {};
    }
    absl::flat_hash_map<std::string, int> &jobname_count_map =
        state_jobname_records[actor->GetState()];

    // get the count of the certain job, if no records, intialize to zero.
    std::string job_name = actor->GetJobName();
    if (jobname_count_map.find(job_name) == jobname_count_map.end()) {
      jobname_count_map[job_name] = 0;
    }
    jobname_count_map[job_name] += 1;
  }

  for (auto &state_entry : state_jobname_records) {
    auto &jobname_count_map = state_entry.second;
    for (auto &job_count_entry : jobname_count_map) {
      // create corresponding tag.
      const ray::stats::TagsType tag = {
          {ray::stats::ActorStateKey,
           rpc::ActorTableData_ActorState_Name(state_entry.first)},
          {ray::stats::JobNameKey, job_count_entry.first}};
      ray::stats::ActorStateGauge().Record(job_count_entry.second, tag);
    }
  }
}

void GcsActorManager::SuspendSchedulingForJob(const JobID &job_id) {
  pending_actors_scheduler_->SuspendJob(job_id);
}

void GcsActorManager::ResumeSchedulingForJob(const JobID &job_id) {
  pending_actors_scheduler_->ResumeJob(job_id);
}

absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>>
GcsActorManager::GetPendingResourceDemands() const {
  return pending_actors_scheduler_->GetPendingResourceDemands();
}

void GcsActorManager::OnWorkerDead(const ray::NodeID &node_id,
                                   const ray::WorkerID &worker_id) {
  OnWorkerDead(node_id, worker_id, "", rpc::WorkerExitType::SYSTEM_ERROR_EXIT);
}

void GcsActorManager::OnWorkerDead(
    const ray::NodeID &node_id, const ray::WorkerID &worker_id,
    const std::string &worker_ip, const rpc::WorkerExitType disconnect_type,
    const std::shared_ptr<rpc::RayException> &creation_task_exception) {
  std::string message = absl::StrCat(
      "Worker ", worker_id.Hex(), " on node ", node_id.Hex(),
      " exits, type=", rpc::WorkerExitType_Name(disconnect_type),
      ", has creation_task_exception = ", (creation_task_exception != nullptr));

  RAY_LOG(INFO) << message;
  if (creation_task_exception != nullptr) {
    RAY_LOG(INFO) << "Formatted creation task exception: "
                  << creation_task_exception->formatted_exception_string();
  }

  bool need_reconstruct = disconnect_type != rpc::WorkerExitType::INTENDED_EXIT;
  // Destroy all actors that are owned by this worker.
  const auto it = owners_.find(node_id);
  if (it != owners_.end() && it->second.count(worker_id)) {
    auto owner = it->second.find(worker_id);
    // Make a copy of the children actor IDs since we will delete from the
    // list.
    const auto children_ids = owner->second.children_actor_ids;
    for (const auto &child_id : children_ids) {
      DestroyActor(child_id,
                   GenOwnerDiedCause(GetActor(child_id), worker_id, disconnect_type,
                                     "Owner's worker process has crashed.", worker_ip));
    }
  }

  // The creator worker of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerWorker(node_id, worker_id);
  for (auto &actor_id : unresolved_actors) {
    RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_EXITED)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << "Unresolved actor is destroyed with exiting worker, worker id " << worker_id;
    DestroyActor(actor_id,
                 GenOwnerDiedCause(GetActor(actor_id), worker_id, disconnect_type,
                                   "Owner's worker process has crashed.", worker_ip));
  }

  // Find if actor is already created or in the creation process (lease request is
  // granted)
  ActorID actor_id;
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end() && iter->second.count(worker_id)) {
    actor_id = iter->second[worker_id];
    iter->second.erase(worker_id);
    if (iter->second.empty()) {
      created_actors_.erase(iter);
    }
  } else {
    actor_id = gcs_actor_scheduler_->CancelOnWorker(node_id, worker_id);
    if (actor_id.IsNil()) {
      return;
    }
  }

  bool need_backoff = false;
  if (disconnect_type == rpc::WorkerExitType::INTENDED_EXIT) {
    RAY_EVENT(INFO, EVENT_LABEL_ACTOR_EXITED)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << "Actor exit with intentional exiting worker, worker id " << worker_id;
  } else {
    RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_EXITED)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << "Actor exit with abnormal exiting worker, worker id " << worker_id;
    if (RayConfig::instance().gcs_actor_restart_backoff_enabled()) {
      need_backoff = true;
    }
  }

  rpc::ActorDeathCause death_cause;
  if (creation_task_exception != nullptr) {
    absl::StrAppend(&message, ": ",
                    creation_task_exception->formatted_exception_string());

    death_cause.mutable_creation_task_failure_context()->CopyFrom(
        *creation_task_exception);
  } else {
    death_cause = GenWorkerDiedCause(GetActor(actor_id), worker_ip, disconnect_type,
                                     /*disconnect_detail=*/"");
  }

  // Otherwise, try to reconstruct the actor that was already created or in the creation
  // process.
  ReconstructActor(actor_id, /*need_reschedule=*/need_reconstruct, death_cause,
                   need_backoff);
}

void GcsActorManager::OnNodeDead(const NodeID &node_id,
                                 const std::string &node_ip_address) {
  RAY_LOG(INFO) << "Node " << node_id << " failed, reconstructing actors.";
  const auto it = owners_.find(node_id);
  if (it != owners_.end()) {
    absl::flat_hash_map<WorkerID, ActorID> children_ids;
    // Make a copy of all the actor IDs owned by workers on the dead node.
    for (const auto &owner : it->second) {
      for (const auto &child_id : owner.second.children_actor_ids) {
        children_ids.emplace(owner.first, child_id);
      }
    }
    for (const auto &[owner_id, child_id] : children_ids) {
      RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_EXITED)
              .WithField("job_id", child_id.JobId().Hex())
              .WithField("actor_id", child_id.Hex())
          << "Actor exit with dead node, node id " << node_id;
      DestroyActor(child_id,
                   GenOwnerDiedCause(GetActor(child_id), owner_id,
                                     rpc::WorkerExitType::SYSTEM_ERROR_EXIT,
                                     "Owner's node has crashed.", node_ip_address));
    }
  }

  // Cancel the scheduling of all related actors.
  auto scheduling_actor_ids = gcs_actor_scheduler_->CancelOnNode(node_id);
  for (auto &actor_id : scheduling_actor_ids) {
    // Reconstruct the canceled actor.
    ReconstructActor(actor_id, /*need_reschedule=*/true,
                     GenNodeDiedCause(GetActor(actor_id), node_ip_address, node_id));
  }

  // Find all actors that were created on this node.
  auto iter = created_actors_.find(node_id);
  if (iter != created_actors_.end()) {
    auto created_actors = std::move(iter->second);
    // Remove all created actors from node_to_created_actors_.
    created_actors_.erase(iter);
    for (auto &entry : created_actors) {
      // Reconstruct the removed actor.
      ReconstructActor(
          entry.second, /*need_reschedule=*/true,
          GenNodeDiedCause(GetActor(entry.second), node_ip_address, node_id));
    }
  }

  // The creator node of these actors died before resolving their dependencies. In this
  // case, these actors will never be created successfully. So we need to destroy them,
  // to prevent actor tasks hang forever.
  auto unresolved_actors = GetUnresolvedActorsByOwnerNode(node_id);
  for (auto &[owner_id, actor_ids] : unresolved_actors) {
    for (const auto &actor_id : actor_ids) {
      if (registered_actors_.count(actor_id)) {
        RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_EXITED)
                .WithField("job_id", actor_id.JobId().Hex())
                .WithField("actor_id", actor_id.Hex())
            << "Unresolved actor is destroyed with dead node, node id " << node_id;
        DestroyActor(actor_id,
                     GenOwnerDiedCause(GetActor(actor_id), owner_id,
                                       rpc::WorkerExitType::SYSTEM_ERROR_EXIT,
                                       "Owner's node has crashed.", node_ip_address));
      }
    }
  }

  if (gcs_l1_handler_ != nullptr) {
    gcs_l1_handler_->OnNodeDead(node_id);
  }
}

void GcsActorManager::ReconstructActor(const ActorID &actor_id, bool need_reschedule,
                                       const rpc::ActorDeathCause &death_cause,
                                       bool need_backoff /*=false*/) {
  // If the owner and this actor is dead at the same time, the actor
  // could've been destroyed and dereigstered before reconstruction.
  auto iter = registered_actors_.find(actor_id);
  if (iter == registered_actors_.end()) {
    RAY_LOG(DEBUG) << "Actor is destroyed before reconstruction, actor id = " << actor_id
                   << ", job id = " << actor_id.JobId();
    return;
  }

  if (gcs_l1_handler_ != nullptr) {
    gcs_l1_handler_->HandleReconstructActor(actor_id);
  }

  auto &actor = iter->second;

  // Remove actor labels.
  gcs_actor_scheduler_->RemoveActorLabels(actor);

  // If it's the first time for this actor to execute its creation task, and the creation
  // task had an error, we should make it DEAD immediately, indicating that this actor was
  // failed to create.
  if (death_cause.has_creation_task_failure_context() && !actor->IsRestart()) {
    RAY_LOG(INFO)
        << "Actor " << actor_id
        << " failed to execute its creation task at the very begining. Marking it DEAD.";
    need_reschedule = false;
  }

  auto job = gcs_job_manager_->GetJob(actor_id.JobId());
  auto is_job_dead = job == nullptr || job->is_dead();
  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  // If the need_reschedule is set to false, then set the `remaining_restarts` to 0
  // so that the actor will never be rescheduled.
  int64_t max_restarts = mutable_actor_table_data->max_restarts();
  uint64_t num_restarts = mutable_actor_table_data->num_restarts();
  int64_t remaining_restarts;
  if (!need_reschedule || is_job_dead) {
    remaining_restarts = 0;
  } else if (max_restarts == -1) {
    remaining_restarts = -1;
  } else {
    int64_t remaining = max_restarts - num_restarts;
    remaining_restarts = std::max(remaining, static_cast<int64_t>(0));
  }
  std::ostringstream ss;
  ss << "Actor is failed " << actor_id << " on worker " << actor->GetWorkerID()
     << " at node " << actor->GetNodeID() << ", need_reschedule = " << need_reschedule
     << ", death context type = " << GetActorDeathCauseString(death_cause)
     << ", remaining_restarts = " << remaining_restarts
     << ", job id = " << actor_id.JobId();
  RAY_LOG(WARNING) << ss.str();
  if (remaining_restarts != 0) {
    RAY_EVENT(INFO, EVENT_LABEL_ACTOR_RECONSTRUCTION_START)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << ss.str();
    // num_restarts must be set before updating GCS, or num_restarts will be inconsistent
    // between memory cache and storage.
    mutable_actor_table_data->set_num_restarts(num_restarts + 1);
    mutable_actor_table_data->set_state(rpc::ActorTableData::RESTARTING);
    mutable_actor_table_data->set_timestamp(current_sys_time_ms());
    mutable_actor_table_data->set_pid(0);
    // Make sure to reset the address before flushing to GCS. Otherwise,
    // GCS will mistakenly consider this lease request succeeds when restarting.
    actor->UpdateAddress(rpc::Address());
    actor->SetWorkerProcessID(UniqueID::Nil());
    mutable_actor_table_data->clear_resource_mapping();

    {
      const ray::stats::TagsType job_name_tag = {
          {ray::stats::JobNameKey, actor->GetJobName()}};
      ray::stats::ActorFailoverCount().Record(1, job_name_tag);
      ray::stats::ActorFailoverSum().Record(1, job_name_tag);

      // Mark actor failover start.
      if (!actor->IsRestart()) {
        actor->MarkAsRestart();
      }

      actor->MarkCreationStart();
    }

    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor_id, mutable_actor_table_data](Status status) {
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              ACTOR_CHANNEL, actor_id.Hex(),
              GenActorDataOnlyWithStates(*mutable_actor_table_data)->SerializeAsString(),
              nullptr));
        }));

    if (need_backoff && !job->config().enable_l1_fault_tolerance()) {
      auto backoff_context = actor->GetBackoffContext();
      if (!backoff_context) {
        // Create a new backoff context.
        actor->SetBackoffContext(std::make_unique<ActorBackoffContext>());
        backoff_context = actor->GetBackoffContext();
      }

      pending_actors_scheduler_->Add(actor);
      backoff_context->ScheduleActorBackoff(io_context_, actor,
                                            /*backoff_schedule_fn=*/[this, actor] {
                                              // The actor may still be in the pending
                                              // queue.
                                              SchedulePendingActors();
                                            });
    } else {
      // Reset backoff context to nullptr.
      actor->SetBackoffContext(nullptr);
      gcs_actor_scheduler_->Schedule(actor);
    }
  } else {
    // Remove actor from `named_actors_` if its name is not empty.
    if (!actor->GetName().empty()) {
      auto namespace_it = named_actors_.find(actor->GetRayNamespace());
      if (namespace_it != named_actors_.end()) {
        auto it = namespace_it->second.find(actor->GetName());
        if (it != namespace_it->second.end()) {
          namespace_it->second.erase(it);
        }
        // If we just removed the last actor in the namespace, remove the map.
        if (namespace_it->second.empty()) {
          named_actors_.erase(namespace_it);
        }
      }
    }

    mutable_actor_table_data->set_state(rpc::ActorTableData::DEAD);
    mutable_actor_table_data->mutable_death_cause()->CopyFrom(death_cause);
    auto time = current_sys_time_ms();
    mutable_actor_table_data->set_end_time(time);
    mutable_actor_table_data->set_timestamp(time);
    // The backend storage is reliable in the future, so the status must be ok.
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
        actor_id, *mutable_actor_table_data,
        [this, actor, actor_id, mutable_actor_table_data, death_cause](Status status) {
          // If actor was an detached actor, make sure to destroy it.
          // We need to do this because detached actors are not destroyed
          // when its owners are dead because it doesn't have owners.
          if (actor->IsDetached()) {
            DestroyActor(actor_id, death_cause);
          }
          RAY_CHECK_OK(gcs_pub_sub_->Publish(
              ACTOR_CHANNEL, actor_id.Hex(),
              GenActorDataOnlyWithStates(*mutable_actor_table_data)->SerializeAsString(),
              nullptr));
          RAY_CHECK_OK(
              gcs_table_storage_->ActorTaskSpecTable().Delete(actor_id, nullptr));
        }));
    // The actor is dead, but we should not remove the entry from the
    // registered actors yet. If the actor is owned, we will destroy the actor
    // once the owner fails or notifies us that the actor's handle has gone out
    // of scope.
  }
}

void GcsActorManager::OnActorSchedulingFailed(
    std::shared_ptr<GcsActor> actor,
    rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
    const std::string &scheduling_failure_message) {
  if (failure_type == rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_INTENDED) {
    // Return directly if the actor was canceled actively as we've already done the
    // recreate and destroy operation when we killed the actor.
    return;
  }

  pending_actors_scheduler_->Add(actor);

  if (failure_type == rpc::RequestWorkerLeaseReply::SCHEDULING_FAILED) {
    // We will attempt to schedule this actor once an eligible node is
    // registered.
    return;
  }

  if (actor->IsRestart() &&
      failure_type ==
          rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED) {
    RAY_LOG(INFO) << "The actor " << actor->GetActorID()
                  << " is restart, so the scheduling will not be canceled when runtime "
                     "env setup failed occurs.";
    return;
  }

  std::string error_msg;
  ray::rpc::ActorDeathCause death_cause;
  switch (failure_type) {
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_BUNDLE_REMOVED:
    death_cause.mutable_actor_unschedulable_context()->set_error_message(
        scheduling_failure_message);
    break;
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_PLACEMENT_GROUP_REMOVED:
    error_msg =
        "Could not create the actor because its associated placement group was removed.";
    death_cause.mutable_actor_unschedulable_context()->set_error_message(error_msg);
    break;
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_RUNTIME_ENV_SETUP_FAILED:
    error_msg = absl::StrCat(
        "Could not create the actor because its associated runtime env failed to be "
        "created.\n",
        scheduling_failure_message);
    death_cause.mutable_runtime_env_failed_context()->set_error_message(error_msg);
    break;
  case rpc::RequestWorkerLeaseReply::SCHEDULING_CANCELLED_UNSCHEDULABLE:
    error_msg = absl::StrCat("Could not create the actor ", actor->GetActorID().Hex(),
                             ", reason: ", scheduling_failure_message);
    death_cause.mutable_actor_unschedulable_context()->set_error_message(error_msg);
    break;
  default:
    RAY_LOG(FATAL) << "Unknown error, failure type "
                   << rpc::RequestWorkerLeaseReply::SchedulingFailureType_Name(
                          failure_type);
    break;
  }

  DestroyActor(actor->GetActorID(), death_cause);
}

void GcsActorManager::OnActorCreationSuccess(const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  std::ostringstream ss;
  // NOTE: If an actor is deleted immediately after the user creates the actor, reference
  // counter may return a reply to the request of WaitForActorOutOfScope to GCS server,
  // and GCS server will destroy the actor. The actor creation is asynchronous, it may be
  // destroyed before the actor creation is completed.
  if (registered_actors_.count(actor_id) == 0) {
    ss << "Actor is destroyed before the creation is completed, actor id = " << actor_id
       << ", job id = " << actor_id.JobId();
    RAY_EVENT(ERROR, EVENT_LABEL_ACTOR_CREATION_FINISHED)
            .WithField("job_id", actor_id.JobId().Hex())
            .WithField("actor_id", actor_id.Hex())
        << ss.str();
    RAY_LOG(WARNING) << ss.str();
    return;
  }
  ss << "Actor created successfully, actor id = " << actor_id
     << ", job id = " << actor_id.JobId();
  RAY_EVENT(INFO, EVENT_LABEL_ACTOR_CREATION_FINISHED)
          .WithField("job_id", actor_id.JobId().Hex())
          .WithField("actor_id", actor_id.Hex())
          .WithField("worker_id", actor->GetWorkerID().Hex())
          .WithField("node_id", actor->GetNodeID().Hex())
      << ss.str();
  RAY_LOG(INFO) << ss.str();

  {  // Record metric.
    if (actor->IsRestart()) {
      // Record metric for restarted actor.
      auto end_time = absl::GetCurrentTimeNanos() / 1000;
      const ray::stats::TagsType extra_tag = {
          {ray::stats::JobNameKey, actor->GetJobName()}};
      ray::stats::ActorFailoverElapsedTime().Record(end_time - actor->CreationTime(),
                                                    extra_tag);

      const ray::stats::TagsType job_name_tag = {
          {ray::stats::JobNameKey, actor->GetJobName()}};
      ray::stats::ActorFailoverSuccessCount().Record(1, job_name_tag);

      ray::stats::ActorFailoverSuccessSum().Record(1, job_name_tag);
    }
  }

  auto mutable_actor_table_data = actor->GetMutableActorTableData();
  auto time = current_sys_time_ms();
  mutable_actor_table_data->set_timestamp(time);
  if (actor->GetState() != rpc::ActorTableData::RESTARTING) {
    mutable_actor_table_data->set_start_time(time);
  }
  actor->UpdateState(rpc::ActorTableData::ALIVE);

  // We should register the entry to the in-memory index before flushing them to
  // GCS because otherwise, there could be timing problems due to asynchronous Put.
  auto worker_id = actor->GetWorkerID();
  auto node_id = actor->GetNodeID();
  RAY_CHECK(!worker_id.IsNil());
  RAY_CHECK(!node_id.IsNil());
  RAY_CHECK(created_actors_[node_id].emplace(worker_id, actor_id).second)
      << "actor id = " << actor_id << ", worker id = " << worker_id;

  auto actor_table_data = *mutable_actor_table_data;
  // The backend storage is reliable in the future, so the status must be ok.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().Put(
      actor_id, actor_table_data,
      [this, actor_id, actor_table_data, actor](Status status) {
        RAY_CHECK_OK(gcs_pub_sub_->Publish(
            ACTOR_CHANNEL, actor_id.Hex(),
            GenActorDataOnlyWithStates(actor_table_data)->SerializeAsString(), nullptr));
        // Invoke all callbacks for all registration requests of this actor (duplicated
        // requests are included) and remove all of them from
        // actor_to_create_callbacks_.
        auto iter = actor_to_create_callbacks_.find(actor_id);
        if (iter != actor_to_create_callbacks_.end()) {
          for (auto &callback : iter->second) {
            callback(actor, false);
          }
          actor_to_create_callbacks_.erase(iter);
        }
        if (gcs_l1_handler_ != nullptr) {
          gcs_l1_handler_->OnActorRestartSucceeded(actor_id);
        }
        if (pending_actors_scheduler_->HasActorAffinitySchedulePendingActor(actor)) {
          RAY_LOG(INFO) << "These are pending actors which scheduling depend on this "
                           "actor, will scheduling pending actors.";
          io_context_.post([this] { SchedulePendingActors(); });
        }
      }));
}

void GcsActorManager::SchedulePendingActors() {
  pending_actors_scheduler_->ScheduleAll();
}

void GcsActorManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &jobs = gcs_init_data.Jobs();
  auto &actor_task_specs = gcs_init_data.ActorTaskSpecs();
  std::unordered_map<NodeID, std::vector<WorkerID>> node_to_workers;
  for (const auto &entry : gcs_init_data.Actors()) {
    std::string nodegroup_id;
    std::string job_name;
    auto job_iter = jobs.find(entry.first.JobId());
    if (job_iter != jobs.end()) {
      nodegroup_id = job_iter->second.nodegroup_id();
      job_name = job_iter->second.job_name();
    }
    auto is_job_dead = (job_iter == jobs.end() || job_iter->second.is_dead());
    if (entry.second.state() != ray::rpc::ActorTableData::DEAD && !is_job_dead) {
      const auto &iter = actor_task_specs.find(entry.first);
      RAY_CHECK(iter != actor_task_specs.end());
      auto actor = std::make_shared<GcsActor>(entry.second, iter->second, nodegroup_id);
      actor->SetJobName(job_name);
      registered_actors_.emplace(entry.first, actor);

      if (!actor->GetName().empty()) {
        auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
        actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
      }

      if (entry.second.state() == ray::rpc::ActorTableData::DEPENDENCIES_UNREADY) {
        const auto &owner = actor->GetOwnerAddress();
        const auto &owner_node = NodeID::FromBinary(owner.raylet_id());
        const auto &owner_worker = WorkerID::FromBinary(owner.worker_id());
        RAY_CHECK(unresolved_actors_[owner_node][owner_worker]
                      .emplace(actor->GetActorID())
                      .second);
      } else if (entry.second.state() == ray::rpc::ActorTableData::ALIVE) {
        created_actors_[actor->GetNodeID()].emplace(actor->GetWorkerID(),
                                                    actor->GetActorID());
      }

      if (!actor->IsDetached()) {
        // This actor is owned. Send a long polling request to the actor's
        // owner to determine when the actor should be removed.
        PollOwnerForActorOutOfScope(actor);
      }

      if (!actor->GetWorkerID().IsNil()) {
        RAY_CHECK(!actor->GetNodeID().IsNil());
        node_to_workers[actor->GetNodeID()].emplace_back(actor->GetWorkerID());
      }
    } else {
      auto actor = std::make_shared<GcsActor>(entry.second, nodegroup_id);
      actor->SetJobName(job_name);
      destroyed_actors_.emplace(entry.first, actor);
      sorted_destroyed_actor_list_.emplace_back(entry.first,
                                                (int64_t)entry.second.timestamp());
    }
  }
  sorted_destroyed_actor_list_.sort([](const std::pair<ActorID, int64_t> &left,
                                       const std::pair<ActorID, int64_t> &right) {
    return left.second < right.second;
  });

  // Notify raylets to release unused workers.
  gcs_actor_scheduler_->ReleaseUnusedWorkers(node_to_workers);

  if (gcs_l1_handler_ != nullptr) {
    gcs_l1_handler_->Initialize(registered_actors_);
  }

  RAY_LOG(DEBUG) << "The number of registered actors is " << registered_actors_.size()
                 << ", and the number of created actors is "
                 << GetNumberOfCreatedActors();
  for (auto &item : registered_actors_) {
    auto &actor = item.second;
    if (actor->GetState() == ray::rpc::ActorTableData::PENDING_CREATION ||
        actor->GetState() == ray::rpc::ActorTableData::RESTARTING) {
      // We should not reschedule actors in state of `ALIVE`.
      // We could not reschedule actors in state of `DEPENDENCIES_UNREADY` because the
      // dependencies of them may not have been resolved yet.
      RAY_LOG(INFO) << "Rescheduling a non-alive actor, actor id = "
                    << actor->GetActorID() << ", state = " << actor->GetState()
                    << ", job id = " << actor->GetActorID().JobId();
      if (!actor->GetWorkerID().IsNil()) {
        gcs_actor_scheduler_->Reschedule(actor);
      } else {
        pending_actors_scheduler_->Add(actor);
      }
    }
  }
  SchedulePendingActors();
}

void GcsActorManager::OnJobFinished(const JobID &job_id) {
  pending_actors_scheduler_->OnJobFinished(job_id);
  if (gcs_l1_handler_ != nullptr) {
    gcs_l1_handler_->OnJobFinished(job_id);
  }
  absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> actors_to_destroy;
  for (auto item : registered_actors_) {
    if (item.first.JobId() == job_id) {
      actors_to_destroy.emplace(item);
    }
  }
  for (const auto &[actor_id, actor] : actors_to_destroy) {
    DestroyActor(actor_id, GenJobDeadCause(actor.get()));
  }
  auto on_done = [this,
                  job_id](const std::unordered_map<ActorID, ActorTableData> &result) {
    if (!result.empty()) {
      auto non_detached_actors = std::make_shared<std::vector<ActorID>>();
      for (auto &item : result) {
        if (!item.second.is_detached()) {
          non_detached_actors->emplace_back(item.first);
        }
      }

      run_delayed_(
          [this, non_detached_actors = std::move(non_detached_actors)]() {
            RAY_CHECK_OK(gcs_table_storage_->ActorTable().BatchDelete(
                *non_detached_actors, [this, non_detached_actors](const Status &status) {
                  RAY_CHECK_OK(gcs_table_storage_->ActorTaskSpecTable().BatchDelete(
                      *non_detached_actors, nullptr));
                }));
          },
          actor_gc_delay_);

      for (auto iter = destroyed_actors_.begin(); iter != destroyed_actors_.end();) {
        if (iter->first.JobId() == job_id && !iter->second->IsDetached()) {
          destroyed_actors_.erase(iter++);
        } else {
          iter++;
        }
      }
    }
  };

  // Only non-detached actors should be deleted. We get all actors of this job and to the
  // filtering.
  RAY_CHECK_OK(gcs_table_storage_->ActorTable().GetByJobId(job_id, on_done));
}

const absl::flat_hash_map<NodeID, absl::flat_hash_map<WorkerID, ActorID>>
    &GcsActorManager::GetCreatedActors() const {
  return created_actors_;
}

const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>>
    &GcsActorManager::GetRegisteredActors() const {
  return registered_actors_;
}

const absl::flat_hash_map<ActorID, std::vector<RegisterActorCallback>>
    &GcsActorManager::GetActorRegisterCallbacks() const {
  return actor_to_register_callbacks_;
}

std::shared_ptr<GcsActor> GcsActorManager::GetRegisteredActor(
    const ActorID &actor_id) const {
  auto iter = registered_actors_.find(actor_id);
  return iter == registered_actors_.end() ? nullptr : iter->second;
}

void GcsActorManager::RemoveUnresolvedActor(const std::shared_ptr<GcsActor> &actor) {
  const auto &owner_address = actor->GetOwnerAddress();
  auto node_id = NodeID::FromBinary(owner_address.raylet_id());
  auto worker_id = WorkerID::FromBinary(owner_address.worker_id());
  auto iter = unresolved_actors_.find(node_id);
  if (iter != unresolved_actors_.end()) {
    auto it = iter->second.find(worker_id);
    RAY_CHECK(it != iter->second.end())
        << "actor id = " << actor->GetActorID() << ", worker id = " << worker_id;
    RAY_CHECK(it->second.erase(actor->GetActorID()) != 0)
        << "actor id = " << actor->GetActorID() << ", worker id = " << worker_id;
    if (it->second.empty()) {
      iter->second.erase(it);
      if (iter->second.empty()) {
        unresolved_actors_.erase(iter);
      }
    }
  }
}

void GcsActorManager::RemoveActorFromOwner(const std::shared_ptr<GcsActor> &actor) {
  const auto &actor_id = actor->GetActorID();
  const auto &owner_id = actor->GetOwnerID();
  RAY_LOG(DEBUG) << "Erasing actor " << actor_id << " owned by " << owner_id
                 << ", job id = " << actor_id.JobId();

  const auto &owner_node_id = actor->GetOwnerNodeID();
  auto &node = owners_[owner_node_id];
  auto worker_it = node.find(owner_id);
  RAY_CHECK(worker_it != node.end())
      << "actor id = " << actor_id << ", owner id = " << owner_id;
  auto &owner = worker_it->second;
  RAY_CHECK(owner.children_actor_ids.erase(actor_id))
      << "actor id = " << actor_id << ", owner id = " << owner_id;
  if (owner.children_actor_ids.empty()) {
    node.erase(worker_it);
    if (node.empty()) {
      owners_.erase(owner_node_id);
    }
  }
}

void GcsActorManager::NotifyCoreWorkerToKillActor(const std::shared_ptr<GcsActor> &actor,
                                                  bool force_kill, bool no_restart) {
  auto actor_client = worker_client_factory_(actor->GetAddress());
  rpc::KillActorRequest request;
  request.set_intended_actor_id(actor->GetActorID().Binary());
  request.set_force_kill(force_kill);
  request.set_no_restart(no_restart);
  RAY_UNUSED(actor_client->KillActor(
      request, [actor_client](const Status &status, const rpc::KillActorReply &reply) {
        // ANT-INTERNAL: BRPC only. Capture the actor_client to make sure the request is
        // sent.
      }));
}

void GcsActorManager::KillActor(const ActorID &actor_id, bool force_kill,
                                bool no_restart) {
  RAY_LOG(DEBUG) << "Killing actor, job id = " << actor_id.JobId()
                 << ", actor id = " << actor_id << ", force_kill = " << force_kill;
  const auto &it = registered_actors_.find(actor_id);
  if (it == registered_actors_.end()) {
    RAY_LOG(INFO) << "Tried to kill actor that does not exist " << actor_id;
    return;
  }

  const auto &actor = it->second;
  if (actor->GetState() == rpc::ActorTableData::DEAD ||
      actor->GetState() == rpc::ActorTableData::DEPENDENCIES_UNREADY) {
    return;
  }

  // The actor is still alive or pending creation.
  const auto &node_id = actor->GetNodeID();
  const auto &worker_id = actor->GetWorkerID();
  auto node_it = created_actors_.find(node_id);
  if (node_it != created_actors_.end() && node_it->second.count(worker_id)) {
    // The actor has already been created. Destroy the process by force-killing
    // it.
    NotifyCoreWorkerToKillActor(actor, force_kill, no_restart);
  } else {
    if (!worker_id.IsNil()) {
      // The actor is in phase of creating, so we need to notify the core
      // worker exit to avoid process and resource leak.
      NotifyCoreWorkerToKillActor(actor, force_kill, no_restart);
    }
    const auto &task_id = actor->GetCreationTaskSpecification().TaskId();
    CancelActorInScheduling(actor, task_id);
    ReconstructActor(actor_id, /*need_reschedule=*/true,
                     GenKilledByApplicationCause(GetActor(actor_id)));
  }
}

void GcsActorManager::SetL1Handler(
    std::shared_ptr<GcsActorManagerL1Handler> gcs_l1_handler) {
  gcs_l1_handler_ = std::move(gcs_l1_handler);
}

void GcsActorManager::AddDestroyedActorToCache(const std::shared_ptr<GcsActor> &actor) {
  if (destroyed_actors_.size() >=
      RayConfig::instance().maximum_gcs_destroyed_actor_cached_count()) {
    EvictOneDestroyedActor();
  }

  if (destroyed_actors_.emplace(actor->GetActorID(), actor).second) {
    sorted_destroyed_actor_list_.emplace_back(
        actor->GetActorID(), (int64_t)actor->GetActorTableData().timestamp());
  }
}

void GcsActorManager::CancelActorInScheduling(const std::shared_ptr<GcsActor> &actor,
                                              const TaskID &task_id) {
  const auto &actor_id = actor->GetActorID();
  const auto &node_id = actor->GetNodeID();
  // The actor has not been created yet. It is either being scheduled or is
  // pending scheduling.
  auto canceled_actor_id =
      gcs_actor_scheduler_->CancelOnWorker(actor->GetNodeID(), actor->GetWorkerID());
  if (!canceled_actor_id.IsNil()) {
    // The actor was being scheduled and has now been canceled.
    RAY_CHECK(canceled_actor_id == actor_id)
        << "actor id = " << actor_id << ", canceled_actor_id = " << canceled_actor_id;
  } else {
    if (!pending_actors_scheduler_->Remove(actor_id)) {
      // When actor creation request of this actor id is pending in raylet,
      // it doesn't responds, and the actor should be still in leasing state.
      // NOTE: Raylet will cancel the lease request once it receives the
      // actor state notification. So this method doesn't have to cancel
      // outstanding lease request by calling raylet_client->CancelWorkerLease
      gcs_actor_scheduler_->CancelOnLeasing(node_id, actor_id, task_id);
      gcs_actor_scheduler_->CancelActorFromWorkerAssignment(actor);
    }
  }
}

const GcsActor *GcsActorManager::GetActor(const ActorID &actor_id) const {
  auto it = registered_actors_.find(actor_id);
  if (it != registered_actors_.end()) {
    return it->second.get();
  }

  it = destroyed_actors_.find(actor_id);
  if (it != destroyed_actors_.end()) {
    return it->second.get();
  }

  return nullptr;
}

int GcsActorManager::GetNumberOfCreatedActors() {
  int count = 0;
  for (const auto &iter : created_actors_) {
    count += iter.second.size();
  }
  return count;
}

std::string GcsActorManager::DebugString() const {
  uint64_t num_named_actors = 0;
  for (const auto &pair : named_actors_) {
    num_named_actors += pair.second.size();
  }
  std::ostringstream stream;
  stream << "GcsActorManager: {RegisterActor request count: "
         << counts_[CountType::REGISTER_ACTOR_REQUEST]
         << ", CreateActor request count: " << counts_[CountType::CREATE_ACTOR_REQUEST]
         << ", GetActorInfo request count: " << counts_[CountType::GET_ACTOR_INFO_REQUEST]
         << ", GetNamedActorInfo request count: "
         << counts_[CountType::GET_NAMED_ACTOR_INFO_REQUEST]
         << ", GetAllActorInfo request count: "
         << counts_[CountType::GET_ALL_ACTOR_INFO_REQUEST]
         << ", KillActor request count: " << counts_[CountType::KILL_ACTOR_REQUEST]
         << ", GetJobDistribution request count: "
         << counts_[CountType::GET_JOB_DISTRIBUTION_REQUEST]
         << ", Registered actors count: " << registered_actors_.size()
         << ", Destroyed actors count: " << destroyed_actors_.size()
         << ", Named actors count: " << num_named_actors
         << ", Unresolved actors count: " << unresolved_actors_.size()
         << ", Pending actors count: "
         << pending_actors_scheduler_->GetPendingActorsCount()
         << ", Created actors count: " << created_actors_.size() << "}";
  return stream.str();
}

void GcsActorManager::EvictExpiredActors() {
  RAY_LOG(INFO) << "Try evicting expired actors, there are "
                << sorted_destroyed_actor_list_.size()
                << " destroyed actors in the cache.";
  int evicted_actor_number = 0;

  std::vector<ActorID> batch_ids;
  size_t batch_size = RayConfig::instance().gcs_dead_data_max_batch_delete_size();
  batch_ids.reserve(batch_size);

  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_actor_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_actor_data_keep_duration_ms();
  while (!sorted_destroyed_actor_list_.empty()) {
    auto timestamp = sorted_destroyed_actor_list_.begin()->second;
    if (timestamp + gcs_dead_actor_data_keep_duration_ms > current_time_ms) {
      break;
    }

    auto iter = sorted_destroyed_actor_list_.begin();
    const auto &actor_id = iter->first;
    if (destroyed_actors_.erase(actor_id) == 0) {
      // The actor may be already erased when job dead.
      // See GcsActorManager::OnJobFinished.
      sorted_destroyed_actor_list_.erase(iter);
      continue;
    }
    batch_ids.emplace_back(actor_id);
    sorted_destroyed_actor_list_.erase(iter);
    ++evicted_actor_number;

    if (batch_ids.size() == batch_size) {
      RAY_CHECK_OK(gcs_table_storage_->ActorTable().BatchDelete(batch_ids, nullptr));
      batch_ids.clear();
    }
  }

  if (!batch_ids.empty()) {
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().BatchDelete(batch_ids, nullptr));
  }
  RAY_LOG(INFO) << evicted_actor_number << " actors are evicted, there are still "
                << sorted_destroyed_actor_list_.size()
                << " destroyed actors in the cache.";
}

void GcsActorManager::EvictOneDestroyedActor() {
  if (!sorted_destroyed_actor_list_.empty()) {
    const auto &actor_id = sorted_destroyed_actor_list_.front().first;
    RAY_CHECK_OK(gcs_table_storage_->ActorTable().Delete(actor_id, nullptr));
    destroyed_actors_.erase(actor_id);
    sorted_destroyed_actor_list_.pop_front();
  }
}

std::vector<std::weak_ptr<GcsActor>> GcsActorManager::GetAllActorsInNode(
    const NodeID &node_id) const {
  std::vector<std::weak_ptr<GcsActor>> res;
  for (auto &pair : registered_actors_) {
    if (pair.second->GetNodeID() == node_id) {
      res.push_back(pair.second);
    }
  }
  return res;
}

void GcsActorManager::ReportSlowActorScheduling() const {
  int64_t current_time_ms = current_sys_time_ms();
  int64_t send_event_interval_ms =
      1000 * RayConfig::instance().slow_actor_scheduling_send_event_interval_s();
  int64_t slow_actor_scheduling_duration_ms =
      1000 * RayConfig::instance().slow_actor_scheduling_duration_s();
  for (auto &entry : registered_actors_) {
    const std::shared_ptr<GcsActor> &actor = entry.second;
    if (actor->GetState() != ray::rpc::ActorTableData::ALIVE) {
      auto latest_creation_time_ms = actor->CreationTime() / 1000;
      auto last_event_send_time_ms = actor->LastSlowSchedulingEventSendTimeMs();
      if ((current_time_ms >
           latest_creation_time_ms + slow_actor_scheduling_duration_ms) &&
          (current_time_ms > last_event_send_time_ms + send_event_interval_ms)) {
        const auto &actor_id = entry.first;
        std::ostringstream ss;
        ss << "Actor " << actor_id << " has been scheduled for "
           << (current_time_ms - latest_creation_time_ms) << "ms, the current state is: "
           << rpc::ActorTableData_ActorState_Name(actor->GetState());
        std::string message = ss.str();
        RAY_EVENT(ERROR, EVENT_LABEL_SLOW_ACTOR_SCHEDULING)
                .WithField("job_id", actor_id.JobId().Hex())
            << message;
        RAY_LOG(INFO) << message;
        actor->SetLastSlowSchedulingEventSendTimeMs(current_time_ms);
      }
    }
  }
}

//////////////////////////////////////////////////////////////////////////////////////////
void GcsActorManagerL1Handler::OnNodeDead(const NodeID &node_id) {
  remote_node_clients_.erase(node_id);
}

GcsActorManagerL1Handler::GcsActorManagerL1Handler(
    std::shared_ptr<GcsJobManager> gcs_job_manager,
    std::shared_ptr<GcsActorManager> gcs_actor_manager,
    instrumented_io_context &l1fo_backoff_io_service,
    const NodeClientFactoryFn &node_client_factory,
    std::function<std::shared_ptr<absl::flat_hash_set<NodeID>>(const JobID &job_id)>
        get_job_nodes,
    std::function<
        absl::optional<std::shared_ptr<rpc::GcsNodeInfo>>(const ray::NodeID &node_id)>
        get_node_info,
    std::function<void()> trigger_pending_actors_scheduling)
    : gcs_job_manager_(std::move(gcs_job_manager)),
      gcs_actor_manager_(std::move(gcs_actor_manager)),
      l1fo_backoff_io_service_(l1fo_backoff_io_service),
      node_client_factory_(node_client_factory),
      get_job_nodes_(std::move(get_job_nodes)),
      get_node_info_(std::move(get_node_info)),
      trigger_pending_actors_scheduling_(trigger_pending_actors_scheduling) {}

void GcsActorManagerL1Handler::KillAllActorsOfJob(const JobID &job_id) {
  RAY_LOG(INFO) << "Killing all actors of job " << job_id;

  // When gcs notify raylets to ban the new tasks for the job, we will receive the worker
  // failure message corresponding to the actor if the worker id of the actor is not nil.
  // So filter actors whose worker id is not nil here.
  actors_to_restart_[job_id].clear();
  actors_restarting_[job_id].clear();
  for (const auto &iter : gcs_actor_manager_->registered_actors_) {
    const auto &actor_id = iter.first;
    if (actor_id.JobId() != job_id) {
      continue;
    }
    const auto &actor = iter.second;
    if (!actor->GetWorkerID().IsNil()) {
      actors_to_restart_[job_id].insert(actor_id);
    }
  }
  jobs_restarting_.insert(job_id);
  RAY_LOG(INFO) << actors_to_restart_[job_id].size()
                << " actors need to be restarted for job " << job_id;

  // Find the nodes running the job and notify them to ban the new tasks for the job.
  BanNewTasks(job_id);
}

std::shared_ptr<TaskControlInterface> GcsActorManagerL1Handler::GetOrConnectNodeClient(
    const ray::NodeID &node_id) {
  auto iter = remote_node_clients_.find(node_id);
  if (iter == remote_node_clients_.end()) {
    auto node_info = get_node_info_(node_id);
    if (node_info.has_value()) {
      auto node = node_info.value();
      rpc::Address address;
      address.set_raylet_id(node->basic_gcs_node_info().node_id());
      address.set_ip_address(node->basic_gcs_node_info().node_manager_address());
      address.set_port(node->basic_gcs_node_info().node_manager_port());

      auto node_client = node_client_factory_(address);
      iter = remote_node_clients_.emplace(node_id, std::move(node_client)).first;
    }
  }
  return iter->second;
}

void GcsActorManagerL1Handler::OnJobFinished(const JobID &job_id) {
  RAY_LOG(DEBUG) << "[L1FO] Job " << job_id << "is dead. Cleanup the l1fo infos.";
  all_l1fo_info_.erase(job_id);
}

void GcsActorManagerL1Handler::HandleReconstructActor(const ActorID &actor_id) {
  // Check if l1 fault tolerance is enabled.
  auto job_id = actor_id.JobId();
  const auto &job_info = gcs_job_manager_->GetJob(job_id);
  if (nullptr == job_info || !job_info->config().enable_l1_fault_tolerance()) {
    return;
  }

  // TODO(qwang): Rename is_waiting_for_actors_to_stop to `is_in_l1fo_failover_progress`.
  if (!job_info->is_waiting_for_actors_to_stop()) {
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_L1_FO_START)
            .WithField("job_id", actor_id.JobId().Hex())
        << "L1 FO of job " << actor_id.JobId() << " starts";
    RAY_LOG(INFO) << "First actor " << actor_id << " of job " << actor_id.JobId()
                  << " is died, starting the L1 fault tolerance process and killing "
                     "all alive actors.";
    job_info->set_is_waiting_for_actors_to_stop(true);
    /// First actor dead, let's kill all actors.
    KillAllActorsOfJob(job_id);
    // Flush to gcs storage.
    gcs_job_manager_->FlushJobTableDataToStorage(actor_id.JobId());
  }

  if (job_info->is_waiting_for_actors_to_stop()) {
    // Wait for all actors to stop.
    RAY_LOG(INFO) << "Actor " << actor_id << " is died, job " << actor_id.JobId()
                  << " is still in the L1 fault tolerance process.";
    actors_restarting_[actor_id.JobId()].insert(actor_id);
    actors_to_restart_[actor_id.JobId()].erase(actor_id);
    RAY_LOG(INFO) << "The number of actors remaining to be killed is "
                  << actors_to_restart_[actor_id.JobId()].size()
                  << ", job id = " << actor_id.JobId();

    if (actors_to_restart_[actor_id.JobId()].empty()) {
      RAY_LOG(INFO) << "All actors in job " << actor_id.JobId()
                    << " are died, restarting all actors.";
      LiftNewTasksBan(actor_id.JobId(), job_info, /*is_gcs_restarting=*/false);
    }
  }
}

void GcsActorManagerL1Handler::TryToDoL1foWithBackoff(const JobID &job_id) {
  auto do_l1fo_func = [this, job_id]() {
    const auto &job_info = gcs_job_manager_->GetJob(job_id);
    if (nullptr == job_info || !job_info->config().enable_l1_fault_tolerance()) {
      RAY_LOG(INFO) << "[L1FO] The job " << job_id << " is offlined or l1fo is disabled. "
                    << "enable_l1_fault_tolerance="
                    << job_info->config().enable_l1_fault_tolerance();
      return;
    }
    auto it = all_l1fo_info_.find(job_id);
    if (it != all_l1fo_info_.end()) {
      auto &l1fo_info = it->second;
      l1fo_info.last_l1fo_timestamp_ms = GetCurrentTimeMs();
      ++l1fo_info.current_round;
      RAY_LOG(INFO) << "[L1FO] Do l1fo for job " << job_id << " , Current round is "
                    << it->second.current_round;
      RestartActorsForJob(job_id, job_info);
    }
  };

  auto it = all_l1fo_info_.find(job_id);
  if (it == all_l1fo_info_.end()) {
    /// First time to trigger l1fo for this job.
    RAY_LOG(DEBUG) << "[L1FO] First time to be triggerring l1fo for job " << job_id;
    all_l1fo_info_[job_id] = {
        /*initial_timestamp=*/0, /*initial_round=*/0,
        /*timer=*/std::make_shared<boost::asio::steady_timer>(l1fo_backoff_io_service_)};
    do_l1fo_func();
  } else {
    const int64_t current_timestamp_ms = GetCurrentTimeMs();
    auto &l1fo_info = it->second;
    /// Reset the round number to 0 if this is not continuous l1fo and do l1fo
    /// immediately.
    if (current_timestamp_ms - l1fo_info.last_l1fo_timestamp_ms >
        RayConfig::instance().max_l1fo_interval_minutes() * 60 * 1000) {
      RAY_LOG(INFO) << "[L1FO] Reset the round number to 1 since this is not "
                    << "continous l1fo progress for job" << job_id;
      l1fo_info.last_l1fo_timestamp_ms = 0;
      l1fo_info.current_round = 0;
      do_l1fo_func();
      return;
    }

    const int64_t timestamp_to_trigger_next_l1fo =
        Backoff(l1fo_info.last_l1fo_timestamp_ms, l1fo_info.current_round);
    if (timestamp_to_trigger_next_l1fo <= current_timestamp_ms) {
      RAY_LOG(DEBUG)
          << "[L1FO] It's the time to do l1fo. timestamp_to_trigger_next_l1fo = "
          << timestamp_to_trigger_next_l1fo
          << " , current_timestamp_ms = " << current_timestamp_ms;
      do_l1fo_func();
    } else {
      /// The time of the next l1fo round not reached. Defer to do l1fo.
      auto timer_ptr = l1fo_info.timer;
      RAY_CHECK(timer_ptr != nullptr);
      const auto duration = timestamp_to_trigger_next_l1fo - current_timestamp_ms;
      RAY_LOG(DEBUG)
          << "[L1FO] The time of the next l1fo round not reached. Defer to do l1fo."
          << " timestamp_to_trigger_next_l1fo = " << timestamp_to_trigger_next_l1fo
          << " , current_timestamp_ms = " << current_timestamp_ms
          << " , duration = " << duration;
      timer_ptr->expires_from_now(std::chrono::milliseconds(duration));
      timer_ptr->async_wait([this, do_l1fo_func,
                             job_id](const boost::system::error_code &ec) {
        RAY_LOG(DEBUG) << "[L1FO] L1fo backoff timer is being triggered for job "
                       << job_id;
        if (ec.value() == boost::asio::error::operation_aborted) {
          RAY_LOG(INFO) << "[L1FO] Backoff timer event was canceled for job " << job_id;
          return;
        }

        auto l1fo_info_it = all_l1fo_info_.find(job_id);
        if (l1fo_info_it == all_l1fo_info_.end()) {
          RAY_LOG(INFO) << "[L1FO] Couldn't find l1fo info for the job " << job_id
                        << " . It might the job is offlined.";
          return;
        }
        do_l1fo_func();
      });
    }
  }
}

int64_t GcsActorManagerL1Handler::Backoff(int64_t last_l1fo_timestamp_ms,
                                          uint32_t current_round) {
  const static uint32_t k_max_num_rounds = 10;  // 512s
  RAY_CHECK(current_round > 0);
  current_round = std::min(current_round, k_max_num_rounds);
  const int64_t interval_seconds = 1 << (current_round - 1);
  return last_l1fo_timestamp_ms + (interval_seconds * 1000);
}

void GcsActorManagerL1Handler::OnActorRestartSucceeded(const ActorID &actor_id) {
  // Check if l1 fault tolerance is enabled.
  const auto &job_info = gcs_job_manager_->GetJob(actor_id.JobId());
  if (nullptr == job_info || !job_info->config().enable_l1_fault_tolerance()) {
    return;
  }

  if (jobs_restarting_.contains(actor_id.JobId())) {
    actors_restarting_[actor_id.JobId()].erase(actor_id);
    // If a new actor fo occurs during actor restart, we will clear `actors_restarting_`
    // and reset `actors_to_restart_`. So we need to judge that `actors_to_restart_` is
    // empty.
    if (actors_restarting_[actor_id.JobId()].empty() &&
        actors_to_restart_[actor_id.JobId()].empty()) {
      jobs_restarting_.erase(actor_id.JobId());
      RAY_LOG(INFO) << "All actors of job " << actor_id.JobId()
                    << " are restarted successfully.";
      RAY_EVENT(INFO, EVENT_LABEL_JOB_L1_FO_END)
              .WithField("job_id", actor_id.JobId().Hex())
          << "L1 FO of job " << actor_id.JobId() << " ends";
    }
  }
}

void GcsActorManagerL1Handler::Initialize(
    const absl::flat_hash_map<ActorID, std::shared_ptr<GcsActor>> &actors) {
  // Find the jobs to be restarted.
  absl::flat_hash_set<std::shared_ptr<rpc::JobTableData>> jobs_to_restart;
  for (const auto &item : actors) {
    const auto &job_info = gcs_job_manager_->GetJob(item.first.JobId());

    // Check if l1 fault tolerance is enabled.
    if (!job_info->config().enable_l1_fault_tolerance()) {
      continue;
    }

    if (IsActorNeedToRestart(item.second, job_info)) {
      actors_restarting_[item.first.JobId()].insert(item.first);
      jobs_to_restart.insert(job_info);
    }
  }

  for (const auto &job_info : jobs_to_restart) {
    const auto &job_id = JobID::FromBinary(job_info->job_id());
    // All the actors of the job have been killed once and GCS has notified raylet to lift
    // new tasks ban of the specified job, so we only need to set `actors_restarting_`.
    if (!job_info->is_waiting_for_actors_to_stop()) {
      jobs_restarting_.insert(job_id);
      RAY_LOG(INFO) << "GCS restarts during job level1 fo, restarts job again, job id = "
                    << job_id;
    } else {
      KillAllActorsOfJob(job_id);
      // All the actors of the job have been killed once but GCS did not notify raylet to
      // lift new tasks ban of the specified job.
      if (actors_to_restart_[job_id].empty()) {
        RAY_LOG(INFO) << "All actors in job " << job_id
                      << " are died, restarting all actors.";
        LiftNewTasksBan(job_id, job_info, /*is_gcs_restarting=*/true);
      }
    }
  }
}

void GcsActorManagerL1Handler::BanNewTasks(const JobID &job_id) {
  gcs_actor_manager_->SuspendSchedulingForJob(job_id);
  auto nodes = get_job_nodes_(job_id);
  RAY_LOG(INFO) << "Requesting " << nodes->size()
                << " nodes to stop tasks and ban new tasks."
                << ", job id = " << job_id;
  int node_count = nodes->size();
  auto finished_count = std::make_shared<int>(0);
  for (const auto &node : *nodes) {
    auto node_client = GetOrConnectNodeClient(node);
    RAY_UNUSED(node_client->StopTasksAndBanNewTasks(
        job_id,
        [job_id, node_count, finished_count, nodes](
            const Status &status, const rpc::StopTasksAndBanNewTasksReply &reply) {
          ++(*finished_count);
          if (*finished_count == node_count) {
            RAY_LOG(INFO)
                << "Finished sending all StopTasksAndBanNewTasks requests for job "
                << job_id;
          }
        }));
  }
}

void GcsActorManagerL1Handler::RestartActorsForJob(
    const JobID &job_id, const std::shared_ptr<JobTableData> &job_info) {
  ray::stats::L1FOCount().Record(1, {{ray::stats::JobNameKey, job_info->job_name()}});

  job_info->set_is_waiting_for_actors_to_stop(false);
  // Flush to gcs storage.
  gcs_job_manager_->FlushJobTableDataToStorage(job_id);
  // Resume scheduling for this job.
  gcs_actor_manager_->ResumeSchedulingForJob(job_id);
  // Trigger pending actors to start scheduling.
  trigger_pending_actors_scheduling_();
}

void GcsActorManagerL1Handler::LiftNewTasksBan(
    const JobID &job_id, const std::shared_ptr<JobTableData> &job_info,
    bool is_gcs_restarting) {
  auto callback = [this, is_gcs_restarting, job_id, job_info]() {
    if (is_gcs_restarting || RayConfig::instance().disable_l1fo_backoff()) {
      RestartActorsForJob(job_id, job_info);
    } else {
      TryToDoL1foWithBackoff(job_id);
    }
  };
  auto nodes = get_job_nodes_(job_id);
  if (nodes->empty()) {
    RAY_LOG(WARNING) << "All nodes are dead.";
    callback();
    return;
  }

  int node_count = nodes->size();
  auto finished_count = std::make_shared<int>(0);
  for (const auto &node : *nodes) {
    auto node_client = GetOrConnectNodeClient(node);
    node_client->LiftNewTasksBan(
        job_id, [callback, job_id, node_count, finished_count](
                    const Status &status, const rpc::LiftNewTasksBanReply &reply) {
          ++(*finished_count);
          if (*finished_count == node_count) {
            RAY_LOG(INFO) << "Finished sending all LiftNewTasksBan requests for job "
                          << job_id;
            callback();
          }
        });
  }
}

bool GcsActorManagerL1Handler::IsActorNeedToRestart(
    const std::shared_ptr<GcsActor> &actor,
    const std::shared_ptr<rpc::JobTableData> &job_info) {
  if (actor->GetState() == ray::rpc::ActorTableData::PENDING_CREATION ||
      actor->GetState() == ray::rpc::ActorTableData::RESTARTING) {
    return true;
  } else if (actor->GetState() == ray::rpc::ActorTableData::ALIVE &&
             job_info->is_waiting_for_actors_to_stop()) {
    // If we write `job_info->is_waiting_for_actors_to_stop = true` to storage and GCS
    // restarts immediately, the status of all actors obtained after GCS restart is
    // alive, which will cause level1 fault tolerance not to be triggered. So we need
    // to restart it.
    return true;
  }

  return false;
}

}  // namespace gcs
}  // namespace ray
