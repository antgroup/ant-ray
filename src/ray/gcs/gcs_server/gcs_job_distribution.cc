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

#include "ray/gcs/gcs_server/gcs_job_distribution.h"

#include "ray/common/task/scheduling_resources_util.h"
#include "ray/event/event.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/util/resource_util.h"

namespace ray {

namespace gcs {

const UniqueID &GcsWorkerProcess::GetWorkerProcessID() const {
  return worker_process_id_;
}

const int32_t &GcsWorkerProcess::GetPID() const { return pid_; }

void GcsWorkerProcess::SetPID(const int32_t &pid) { pid_ = pid; }

const NodeID &GcsWorkerProcess::GetNodeID() const { return node_id_; }

void GcsWorkerProcess::SetNodeID(const NodeID &node_id) { node_id_ = node_id; }

const JobID &GcsWorkerProcess::GetJobID() const { return job_id_; }

const Language &GcsWorkerProcess::GetLanguage() const { return language_; }

size_t GcsWorkerProcess::GetAvailableSlotCount() const {
  return slot_capacity_ - actor_ids_.size();
}

size_t GcsWorkerProcess::GetUsedSlotCount() const { return actor_ids_.size(); }

bool GcsWorkerProcess::IsShared() const { return is_shared_; }

size_t GcsWorkerProcess::GetSlotCapacity() const { return slot_capacity_; }

bool GcsWorkerProcess::AssignActor(const ActorID &actor_id) {
  return actor_ids_.size() < slot_capacity_ && actor_ids_.emplace(actor_id).second;
}

const absl::flat_hash_set<ActorID> &GcsWorkerProcess::GetAssignedActors() const {
  return actor_ids_;
}

bool GcsWorkerProcess::RemoveActor(const ActorID &actor_id) {
  return actor_ids_.erase(actor_id) != 0;
}

const ResourceSet &GcsWorkerProcess::GetRequiredResources() const {
  return required_resources_;
}

const ResourceSet &GcsWorkerProcess::GetResources() const { return acquired_resources_; }

ResourceSet &GcsWorkerProcess::GetMutableResources() { return acquired_resources_; }

const ResourceSet &GcsWorkerProcess::GetRuntimeResources() const {
  return runtime_resources_;
}

void GcsWorkerProcess::UpdateRuntimeResources(const ResourceSet &resources) {
  // `resources` represents the worker's runtime resources, which currently
  // only contains memory and CPU.
  for (auto &entry : resources.GetResourceAmountMap()) {
    runtime_resources_.AddOrUpdateResource(entry.first, entry.second);
  }
}

ResourceSet GcsWorkerProcess::GetConstraintResources() {
  // The constraint resources should always depends on the const required_resources_.
  return ray::GetConstraintResources(required_resources_);
}

void GcsWorkerProcess::AddResourceChangedListener(
    WorkerProcessResourceChangedListener listener) {
  RAY_CHECK(listener != nullptr);
  listeners_.emplace_back(std::move(listener));
}

std::string GcsWorkerProcess::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');

  ostr << "{\n";
  ostr << indent_1 << "worker_process_id = " << worker_process_id_ << ",\n";
  ostr << indent_1 << "node_id = " << node_id_ << ",\n";
  ostr << indent_1 << "job_id = " << job_id_ << ",\n";
  ostr << indent_1 << "language = " << rpc::Language_Name(language_) << ",\n";
  ostr << indent_1 << "is_shared = " << is_shared_ << ",\n";
  ostr << indent_1 << "slot_capacity = " << slot_capacity_ << ",\n";
  ostr << indent_1 << "available_slot_count = " << GetAvailableSlotCount() << ",\n";
  ostr << indent_1 << "required_resources = " << required_resources_.ToString() << "\n";
  ostr << indent_1 << "acquired_resources = " << acquired_resources_.ToString() << "\n";
  ostr << indent_1 << "runtime_resources = " << runtime_resources_.ToString() << "\n";
  ostr << indent_0 << "},";
  return ostr.str();
}

void GcsWorkerProcess::AsyncFlushRuntimeResources(
    const std::shared_ptr<gcs::GcsTableStorage> &gcs_table_storage,
    std::function<void(const ray::Status &)> callback) {
  auto data = BuildWorkerProcessRuntimeResourceData();
  RAY_UNUSED(gcs_table_storage->WorkerProcessRuntimeResourceTable().Put(
      worker_process_id_, *data, [](const Status &status) {}));
}

bool GcsWorkerProcess::EqualsTo(std::shared_ptr<GcsWorkerProcess> other) {
  if (other == nullptr) {
    return false;
  }

  return worker_process_id_ == other->worker_process_id_ && node_id_ == other->node_id_ &&
         job_id_ == other->job_id_ && is_shared_ == other->is_shared_ &&
         slot_capacity_ == other->slot_capacity_ &&
         required_resources_.IsEqual(other->required_resources_);
}

std::shared_ptr<rpc::GcsWorkerProcessRuntimeResourceTableData>
GcsWorkerProcess::BuildWorkerProcessRuntimeResourceData() const {
  auto data = std::make_shared<rpc::GcsWorkerProcessRuntimeResourceTableData>();
  data->set_worker_process_id(worker_process_id_.Binary());

  auto mutable_runtime_resources = data->mutable_runtime_resources();
  for (auto &pair : runtime_resources_.GetResourceAmountMap()) {
    (*mutable_runtime_resources)[pair.first] = pair.second.ToDouble();
  }
  return data;
}

bool GcsWorkerProcess::IsForPlacementGroup() const { return is_for_placement_group_; }

const BundleID &GcsWorkerProcess::GetBundleID() const { return bundle_id_; }

const ResourceSet &GcsWorkerProcess::GetOriginalRequiredRuntimeResources() const {
  return original_required_runtime_resoruces_;
}

ResourceSet GcsWorkerProcess::CalcOriginalRequiredRuntimeResources(
    BundleID *bundle_id) const {
  ResourceSet resources;
  for (const auto &resoruce_label : RUNTIME_RESOURCE_LABELS) {
    if (IsForPlacementGroup()) {
      ResourceSet original_resource_set;
      RAY_CHECK(ConvertBundleResourcesToOriginalResources(
          required_resources_, &original_resource_set, bundle_id));
      resources.AddOrUpdateResource(resoruce_label,
                                    original_resource_set.GetResource(resoruce_label));
    } else {
      resources.AddOrUpdateResource(resoruce_label,
                                    required_resources_.GetResource(resoruce_label));
    }
  }
  return resources;
}

const ResourceSet &GcsWorkerProcess::GetPGAcquiredResources() const {
  return pg_acquired_resources_;
}

ResourceSet &GcsWorkerProcess::GetMutablePGAcquiredResources() {
  return pg_acquired_resources_;
}

void GcsWorkerProcess::InitializePGAcquiredResources() {
  for (const auto &resource_label : RUNTIME_RESOURCE_LABELS) {
    if (original_required_runtime_resoruces_.Contains(resource_label)) {
      pg_acquired_resources_.AddOrUpdateResource(
          resource_label,
          original_required_runtime_resoruces_.GetResource(resource_label));
    }
  }
}

//////////////////////////////////////////////////////////////////////
GcsJobSchedulingContext::GcsJobSchedulingContext(
    const GcsJobConfig &job_config, std::shared_ptr<ScheduleOptions> schedule_options)
    : job_config_(job_config),
      schedule_options_(std::move(schedule_options)),
      scheduling_resources_(ResourceSet(std::unordered_map<std::string, double>{
          {kMemory_ResourceLabel, job_config_.max_total_memory_units_}})),
      min_resource_requirements_(std::unordered_map<std::string, double>{
          {kMemory_ResourceLabel, job_config_.total_memory_units_}}) {}

bool GcsJobSchedulingContext::AddWorkerProcess(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  RAY_CHECK(worker_process != nullptr);

  const auto &constraint_resources = worker_process->GetConstraintResources();
  if (job_config_.enable_job_quota_ &&
      !constraint_resources.IsSubset(scheduling_resources_.GetAvailableResources())) {
    // Return false if the job available resources are not satisfied with the worker
    // process.
    return false;
  }

  if (worker_process->IsShared()) {
    shared_worker_processes_.emplace(worker_process->GetWorkerProcessID(),
                                     worker_process);
  } else {
    sole_worker_processes_.emplace(worker_process->GetWorkerProcessID(), worker_process);
  }

  // Update the distribution of worker process on each node.
  node_to_worker_processes_[worker_process->GetNodeID()].emplace(
      worker_process->GetWorkerProcessID(), worker_process);

  // Acquire resources from job scheduling resources.
  // The resources should be return back to the `scheduling_resources_` when the worker
  // process is removed.
  if (job_config_.enable_job_quota_) {
    scheduling_resources_.Acquire(constraint_resources);
  }
  acquired_resources_.AddResources(worker_process->GetRequiredResources());
  shortterm_runtime_resources_.AddResources(worker_process->GetRuntimeResources());
  return true;
}

const ResourceSet &GcsJobSchedulingContext::GetAvailableResources() const {
  return scheduling_resources_.GetAvailableResources();
}

const SchedulingResources &GcsJobSchedulingContext::GetSchedulingResources() const {
  return scheduling_resources_;
}

const GcsJobConfig &GcsJobSchedulingContext::GetJobConfig() const { return job_config_; }

GcsJobConfig *GcsJobSchedulingContext::GetMutableJobConfig() { return &job_config_; }

std::shared_ptr<ScheduleOptions> GcsJobSchedulingContext::GetScheduleOptions() const {
  return schedule_options_;
}

Status GcsJobSchedulingContext::UpdateJobResourceRequirements(
    const rpc::JobTableData &job_table_data) {
  std::unordered_map<std::string, double> resource_map;
  resource_map.emplace(kMemory_ResourceLabel,
                       job_table_data.config().max_total_memory_units());
  // resource_map.emplace(kCPU_ResourceLabel, job_table_data.config().max_total_cpus());
  // resource_map.emplace(kGPU_ResourceLabel, job_table_data.config().max_total_gpus());
  ResourceSet new_max_job_resource_requirements(resource_map);

  auto total_resources = scheduling_resources_.GetTotalResources();
  auto available_resources = scheduling_resources_.GetAvailableResources();

  ResourceSet used_resources = total_resources;
  used_resources.SubtractResources(available_resources);

  ResourceSet total_memory = total_resources.GetMemory();
  ResourceSet available_memory = available_resources.GetMemory();
  ResourceSet current_used_memory = used_resources.GetMemory();

  RAY_LOG(INFO) << "UpdateJobResourceRequirements:"
                << "\n -- total_memory: " << total_memory.ToString()
                << "\n -- available_memory: " << available_memory.ToString()
                << "\n -- used_memory: " << current_used_memory.ToString();

  if (!current_used_memory.IsSubset(new_max_job_resource_requirements)) {
    std::ostringstream ss;
    ss << "Failed to udpate job resource requirements as the expected max job resource "
          "requirements are smaller than the job's currently-used."
       << "\nJobID: " << JobID::FromBinary(job_table_data.job_id())
       << "\nExpected job resource requirements: "
       << new_max_job_resource_requirements.ToString()
       << "\nCurrently-used resources: " << current_used_memory.ToString();
    std::string message = ss.str();
    RAY_LOG(WARNING) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
    return Status::Invalid(message);
  }

  // The new_total_memory will always be greater than 0.
  // Update max resource requirements.
  total_resources.AddOrUpdateResource(
      kMemory_ResourceLabel,
      new_max_job_resource_requirements.GetResource(kMemory_ResourceLabel));
  scheduling_resources_.SetTotalResources(std::move(total_resources));
  // Update the min resource requirements.
  min_resource_requirements_.AddOrUpdateResource(
      kMemory_ResourceLabel, job_table_data.config().total_memory_units());

  ResourceSet new_available_memory(new_max_job_resource_requirements);
  // Should substract used memory, the current_used_memory contains both PG used memory
  // and Non-PG used memory.
  new_available_memory.SubtractResources(current_used_memory);
  auto new_available_memory_quantity =
      new_available_memory.GetResource(kMemory_ResourceLabel);
  if (new_available_memory_quantity > 0) {
    available_resources.AddOrUpdateResource(kMemory_ResourceLabel,
                                            new_available_memory_quantity);
  } else {
    available_resources.DeleteResource(kMemory_ResourceLabel);
  }
  scheduling_resources_.SetAvailableResources(std::move(available_resources));

  job_config_.total_memory_units_ = job_table_data.config().total_memory_units();
  job_config_.max_total_memory_units_ = job_table_data.config().max_total_memory_units();
  return Status::OK();
}

const WorkerProcessMap &GcsJobSchedulingContext::GetSharedWorkerProcesses() const {
  return shared_worker_processes_;
}

const WorkerProcessMap &GcsJobSchedulingContext::GetSoleWorkerProcesses() const {
  return sole_worker_processes_;
}

const NodeToWorkerProcessesMap &GcsJobSchedulingContext::GetNodeToWorkerProcesses()
    const {
  return node_to_worker_processes_;
}

absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>>
GcsJobSchedulingContext::RemoveWorkerProcessesByNodeID(const NodeID &node_id) {
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> removed_worker_processes;
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto worker_processes = std::move(iter->second);
    for (auto &entry : worker_processes) {
      removed_worker_processes.emplace(entry.second);
      // Remove worker process associated with the specified node.
      if (entry.second->IsShared()) {
        RAY_CHECK(shared_worker_processes_.erase(entry.second->GetWorkerProcessID()));
      } else {
        RAY_CHECK(sole_worker_processes_.erase(entry.second->GetWorkerProcessID()));
      }

      // Return the resources of the worker process back to the job scheduling resources.
      auto constraint_resources = entry.second->GetConstraintResources();
      if (job_config_.enable_job_quota_) {
        scheduling_resources_.Release(constraint_resources);
      }
      acquired_resources_.SubtractResources(entry.second->GetRequiredResources());
      shortterm_runtime_resources_.SubtractResources(entry.second->GetRuntimeResources());

      RAY_LOG(DEBUG) << "Removed worker process " << entry.first
                     << ", Worker constraint resources: "
                     << constraint_resources.ToString() << ", Job constraint resources: "
                     << scheduling_resources_.GetAvailableResources().ToString();
    }
    // Remove the entry from `node_to_worker_processes_`.
    node_to_worker_processes_.erase(iter);
  }
  return removed_worker_processes;
}

std::shared_ptr<GcsWorkerProcess>
GcsJobSchedulingContext::RemoveWorkerProcessByWorkerProcessID(
    const NodeID &node_id, const UniqueID &worker_process_id) {
  std::shared_ptr<GcsWorkerProcess> removed_worker_process;
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto &worker_processes = iter->second;
    auto worker_process_iter = worker_processes.find(worker_process_id);
    if (worker_process_iter != worker_processes.end()) {
      removed_worker_process = worker_process_iter->second;
      // Remove worker process associated with the specified node.
      if (removed_worker_process->IsShared()) {
        RAY_CHECK(
            shared_worker_processes_.erase(removed_worker_process->GetWorkerProcessID()));
      } else {
        RAY_CHECK(
            sole_worker_processes_.erase(removed_worker_process->GetWorkerProcessID()));
      }
      // Remove worker process from `worker_processes` to update the worker process
      // distribution on the specified node.
      worker_processes.erase(worker_process_iter);
      if (worker_processes.empty()) {
        // Remove entry from `node_to_worker_processes_` as `worker_processes` is
        // empty.
        node_to_worker_processes_.erase(iter);
      }
      // Return the resources of the removed worker process back to the job scheduling
      // resources.
      auto constraint_resources = removed_worker_process->GetConstraintResources();
      if (job_config_.enable_job_quota_) {
        scheduling_resources_.Release(constraint_resources);
      }
      acquired_resources_.SubtractResources(
          removed_worker_process->GetRequiredResources());
      shortterm_runtime_resources_.SubtractResources(
          removed_worker_process->GetRuntimeResources());

      RAY_LOG(DEBUG) << "Removed worker process "
                     << removed_worker_process->GetWorkerProcessID()
                     << ", Worker constraint resources: "
                     << constraint_resources.ToString() << ", Job constraint resources: "
                     << scheduling_resources_.GetAvailableResources().ToString();
    }
  }
  return removed_worker_process;
}

std::shared_ptr<GcsWorkerProcess> GcsJobSchedulingContext::GetWorkerProcess(
    const NodeID &node_id, const UniqueID &worker_process_id) const {
  auto iter = node_to_worker_processes_.find(node_id);
  if (iter != node_to_worker_processes_.end()) {
    auto &worker_processes = iter->second;
    auto worker_process_iter = worker_processes.find(worker_process_id);
    if (worker_process_iter != worker_processes.end()) {
      return worker_process_iter->second;
    }
  }
  return nullptr;
}

bool GcsJobSchedulingContext::ReserveBundlesResources(
    std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  if (!job_config_.enable_job_quota_) {
    for (auto &bundle : bundles) {
      acquired_resources_.AddResources(bundle->GetRequiredResources());
    }
    return true;
  }

  std::vector<std::shared_ptr<BundleSpecification>> prepared_bundles;
  prepared_bundles.reserve(bundles.size());

  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    auto required_resources = ray::GetConstraintResources(bundle->GetRequiredResources());
    auto prepare_res = scheduling_resources_.PrepareBundleResources(
        bundle_id.first, bundle_id.second, required_resources);

    if (prepare_res) {
      // If prepare resource successful, we must first commit bundle or we can't return
      // resource when prepare failed.
      scheduling_resources_.CommitBundleResources(bundle_id.first, bundle_id.second,
                                                  required_resources);
      acquired_resources_.AddResources(bundle->GetRequiredResources());

      prepared_bundles.emplace_back(bundle);
    } else {
      // If prepare resource failed, we will release the previous prepared bundle.
      ReturnBundlesResources(prepared_bundles);
      SubtractJobAcquiredResources(prepared_bundles);
      return false;
    }
  }

  return true;
}

void GcsJobSchedulingContext::SubtractJobAcquiredResources(
    std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  for (auto &bundle : bundles) {
    acquired_resources_.SubtractResources(bundle->GetRequiredResources());
  }
}

void GcsJobSchedulingContext::ReturnBundlesResources(
    std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    auto required_resources = ray::GetConstraintResources(bundle->GetRequiredResources());
    if (scheduling_resources_.ReturnBundleResources(bundle_id.first, bundle_id.second)) {
      RAY_LOG(DEBUG) << "Return resources of bundle: " << bundle->Index()
                     << ", placement group: " << bundle->BundleId().first
                     << " to job context successfully.";
    }
  }
}

void GcsJobSchedulingContext::OnJobResourcesInsufficiant(
    const ResourceSet &constraint_resources, const TaskSpecification &task_spec) {
  auto get_current_actor_num = [this] {
    int shared_actor_num = std::accumulate(
        std::begin(shared_worker_processes_), std::end(shared_worker_processes_), 0,
        [](int value, const WorkerProcessMap::value_type &p) {
          return value + p.second->GetAssignedActors().size();
        });

    int solo_actor_num = std::accumulate(
        std::begin(sole_worker_processes_), std::end(sole_worker_processes_), 0,
        [](int value, const WorkerProcessMap::value_type &p) {
          return value + p.second->GetAssignedActors().size();
        });

    return shared_actor_num + solo_actor_num;
  };

  std::ostringstream ostr;
  ostr << "Failed to schedule the actor " << task_spec.ActorCreationId()
       << " as the declared total resources of the job are too few."
       << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
       << "\nJob id: " << job_config_.job_id_
       << "\nAssigned actor count: " << get_current_actor_num()
       << "\nRequired resources: " << constraint_resources.ToString()
       << "\nJob available resources: "
       << scheduling_resources_.GetAvailableResources().ToString()
       << "\nJob total resources: "
       << scheduling_resources_.GetTotalResources().ToString();
  std::string message = ostr.str();
  RAY_LOG(WARNING) << message;

  uint64_t now = current_time_ms();
  uint64_t report_interval_ms =
      RayConfig::instance().job_resources_exhausted_report_interval_ms();
  if (last_report_time_ms_ + report_interval_ms < now) {
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_RESOURCES_ARE_EXHAUSTED)
            .WithField("job_id", job_config_.job_id_.Hex())
        << message;
    last_report_time_ms_ = now;
  }
}

const ResourceSet &GcsJobSchedulingContext::GetShorttermRuntimeResources() const {
  return shortterm_runtime_resources_;
}

void GcsJobSchedulingContext::UpdateShorttermRuntimeResources(
    const ResourceSet &prev_resources, const ResourceSet &cur_resources) {
  ResourceSet condensed_prev_resources;
  for (const auto &resource_entry : cur_resources.GetResourceAmountMap()) {
    if (prev_resources.Contains(resource_entry.first)) {
      condensed_prev_resources.AddOrUpdateResource(
          resource_entry.first, prev_resources.GetResource(resource_entry.first));
    }
  }

  shortterm_runtime_resources_.SubtractResources(condensed_prev_resources);
  shortterm_runtime_resources_.AddResources(cur_resources);
}

ResourceSet GcsJobSchedulingContext::GetLongtermRuntimeResources() const {
  if (longterm_runtime_resources_.IsEmpty()) {
    // The runtime resources have not been updated yet.
    return GetRuntimeResourceRequirements();
  }
  return longterm_runtime_resources_;
}

void GcsJobSchedulingContext::UpdateLongtermRuntimeResources(
    const ResourceSet &resource_set) {
  longterm_runtime_resources_ = resource_set;
}

const ResourceSet &GcsJobSchedulingContext::GetAcquiredResources() const {
  return acquired_resources_;
}

std::string GcsJobSchedulingContext::ToString(int indent /* = 0*/) const {
  std::ostringstream ostr;
  std::string indent_0(indent + 0 * 2, ' ');
  std::string indent_1(indent + 1 * 2, ' ');
  std::string indent_2(indent + 2 * 2, ' ');

  ostr << "{\n";
  ostr << indent_1 << "job_config = {\n";
  ostr << indent_2 << "job_id = " << job_config_.job_id_ << ",\n";
  ostr << indent_2
       << "num_java_workers_per_process = " << job_config_.num_java_workers_per_process_
       << ",\n";
  ostr << indent_2 << "java_worker_process_default_memory_gb = "
       << FromMemoryUnitsToGiB(job_config_.java_worker_process_default_memory_units_)
       << ",\n";
  ostr << indent_2 << "python_worker_process_default_memory_gb = "
       << FromMemoryUnitsToGiB(job_config_.python_worker_process_default_memory_units_)
       << ",\n";
  ostr << indent_2
       << "total_memory_gb = " << FromMemoryUnitsToGiB(job_config_.total_memory_units_)
       << ",\n";
  ostr << indent_2 << "max_total_memory_gb = "
       << FromMemoryUnitsToGiB(job_config_.max_total_memory_units_) << ",\n";
  ostr << indent_1 << "},\n";

  ostr << indent_1 << "shared_worker_processes = [";
  for (const auto &entry : shared_worker_processes_) {
    ostr << entry.second->ToString(4);
  }
  ostr << indent_1 << "],\n";

  ostr << indent_1 << "sole_worker_processes = [";
  for (const auto &entry : sole_worker_processes_) {
    ostr << entry.second->ToString(4);
  }
  ostr << "\n" << indent_1 << "],\n";

  ostr << indent_0 << "},\n";
  return ostr.str();
}

std::shared_ptr<GcsWorkerProcess> GcsJobSchedulingContext::GetWorkerProcessById(
    const UniqueID &worker_process_id) const {
  auto iter = shared_worker_processes_.find(worker_process_id);
  if (iter != shared_worker_processes_.end()) {
    return iter->second;
  }

  iter = sole_worker_processes_.find(worker_process_id);
  if (iter != sole_worker_processes_.end()) {
    return iter->second;
  }

  return nullptr;
}

ResourceSet GcsJobSchedulingContext::GetRuntimeResourceRequirements() const {
  auto runtime_job_resource_requirements = min_resource_requirements_;

  // Calc used resources.
  auto used_resource_quota = scheduling_resources_.GetTotalResources();
  used_resource_quota.SubtractResources(scheduling_resources_.GetAvailableResources());

  // The used_resoruces contains bundle resources which need to be ignored.
  for (auto &entry : runtime_job_resource_requirements.GetResourceAmountMap()) {
    auto iter = used_resource_quota.GetResourceAmountMap().find(entry.first);
    if (iter != used_resource_quota.GetResourceAmountMap().end()) {
      if (iter->second > entry.second) {
        runtime_job_resource_requirements.AddOrUpdateResource(entry.first, iter->second);
      }
    }
  }

  return runtime_job_resource_requirements;
}

//////////////////////////////////////////////////////////////////////
GcsJobDistribution::GcsJobDistribution(
    std::function<std::shared_ptr<GcsJobSchedulingContext>(const JobID &)>
        gcs_job_scheduling_factory,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager /* = nulltpr*/,
    std::shared_ptr<GcsWorkerProcessRuntimeResourceFlusher>
        runtime_resource_flusher /*=nullptr*/)
    : gcs_job_scheduling_factory_(std::move(gcs_job_scheduling_factory)),
      gcs_resource_manager_(std::move(gcs_resource_manager)),
      runtime_resource_flusher_(std::move(runtime_resource_flusher)) {
  RAY_CHECK(gcs_job_scheduling_factory_ != nullptr);
}

bool GcsJobDistribution::AddWorkerProcess(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  RAY_CHECK(worker_process != nullptr);
  auto job_id = worker_process->GetJobID();
  auto job_scheduling_context = GetJobSchedulingContext(job_id);
  if (job_scheduling_context == nullptr ||
      !job_scheduling_context->AddWorkerProcess(worker_process)) {
    return false;
  }
  if (!worker_process->IsForPlacementGroup() && gcs_resource_manager_) {
    gcs_resource_manager_->AddNodeTotalRequiredResources(
        worker_process->GetNodeID(),
        worker_process->GetOriginalRequiredRuntimeResources());
  }
  // Update the worker distribution on the related node.
  node_to_jobs_[worker_process->GetNodeID()].emplace(job_id);
  return true;
}

std::shared_ptr<GcsJobSchedulingContext> GcsJobDistribution::GetJobSchedulingContext(
    const JobID &job_id) const {
  auto iter = job_scheduling_contexts_.find(job_id);
  return iter == job_scheduling_contexts_.end() ? nullptr : iter->second;
}

std::shared_ptr<GcsJobSchedulingContext> GcsJobDistribution::RemoveJobSchedulingContext(
    const JobID &job_id) {
  std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context;
  // Remove the job scheduling context to prevent memory from leaking.
  auto iter = job_scheduling_contexts_.find(job_id);
  if (iter != job_scheduling_contexts_.end()) {
    job_scheduling_context = std::move(iter->second);
    job_scheduling_contexts_.erase(iter);
  }

  if (job_scheduling_context != nullptr) {
    // Update the job distribution on each node.
    const auto &node_to_worker_processes =
        job_scheduling_context->GetNodeToWorkerProcesses();
    for (auto &entry : node_to_worker_processes) {
      RemoveJobFromNode(job_id, entry.first);
    }
  }
  return job_scheduling_context;
}

std::shared_ptr<GcsJobSchedulingContext>
GcsJobDistribution::FindOrCreateJobSchedulingContext(const JobID &job_id) {
  auto iter = job_scheduling_contexts_.find(job_id);
  if (iter == job_scheduling_contexts_.end()) {
    auto job_scheduling_context = gcs_job_scheduling_factory_(job_id);
    RAY_LOG(INFO) << "Create a new job scheduling context: "
                  << job_scheduling_context->GetJobConfig().ToString();
    iter = job_scheduling_contexts_.emplace(job_id, job_scheduling_context).first;
  }
  return iter->second;
}

absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>>
GcsJobDistribution::RemoveWorkerProcessesByNodeID(const NodeID &node_id) {
  absl::flat_hash_set<std::shared_ptr<GcsWorkerProcess>> removed_worker_processes;
  auto node_to_jobs_iter = node_to_jobs_.find(node_id);
  if (node_to_jobs_iter != node_to_jobs_.end()) {
    // Update the job distribution on the specified node.
    for (const auto &job_id : node_to_jobs_iter->second) {
      auto job_scheduling_context = GetJobSchedulingContext(job_id);
      RAY_CHECK(job_scheduling_context != nullptr);
      // Remove worker processes associated with this node id.
      auto worker_processes =
          job_scheduling_context->RemoveWorkerProcessesByNodeID(node_id);
      removed_worker_processes.insert(worker_processes.begin(), worker_processes.end());
    }
    node_to_jobs_.erase(node_to_jobs_iter);
  }
  return removed_worker_processes;
}

std::shared_ptr<GcsWorkerProcess>
GcsJobDistribution::RemoveWorkerProcessByWorkerProcessID(
    const NodeID &node_id, const UniqueID &worker_process_id, const JobID &job_id) {
  std::shared_ptr<GcsWorkerProcess> removed_worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    if (job_scheduling_context->GetScheduleOptions()
            ->runtime_resource_scheduling_enabled_) {
      if (auto worker_process =
              job_scheduling_context->GetWorkerProcessById(worker_process_id)) {
        if (worker_process->GetNodeID() == node_id &&
            worker_process->IsForPlacementGroup()) {
          AdjustPGResources(worker_process);
        }
      }
    }
    // Remove worker process associated with this node id and worker process id.
    removed_worker_process = job_scheduling_context->RemoveWorkerProcessByWorkerProcessID(
        node_id, worker_process_id);
    if (removed_worker_process != nullptr) {
      // Update the job distribution on each node.
      auto &node_to_worker_processes = job_scheduling_context->GetNodeToWorkerProcesses();
      if (!node_to_worker_processes.contains(node_id)) {
        RemoveJobFromNode(job_id, node_id);
      }
    }
  }
  if (removed_worker_process && !removed_worker_process->IsForPlacementGroup() &&
      gcs_resource_manager_) {
    gcs_resource_manager_->SubtractNodeTotalRequiredResources(
        removed_worker_process->GetNodeID(),
        removed_worker_process->GetOriginalRequiredRuntimeResources());
  }
  return removed_worker_process;
}

std::vector<rpc::NodeInfo> GcsJobDistribution::GetJobDistribution() const {
  std::unordered_map<NodeID, rpc::NodeInfo> node_info_map;

  auto fill_resource_func = [](const ResourceSet &resource_set,
                               google::protobuf::Map<std::string, double> *result) {
    for (const auto &r : resource_set.GetResourceAmountMap()) {
      double value = 0;
      if (StartsWith(r.first, kMemory_ResourceLabel)) {
        value = FromMemoryUnitsToBytes(r.second.ToDouble());
      } else {
        value = r.second.ToDouble();
      }
      (*result)[r.first] = value;
    }
  };

  for (const auto &it : node_to_jobs_) {
    rpc::NodeInfo n;
    n.set_node_id(it.first.Binary());
    node_info_map[it.first] = n;
  }
  for (const auto &jsc : job_scheduling_contexts_) {
    WorkerProcessMap worker_processes;
    worker_processes.insert(jsc.second->GetSharedWorkerProcesses().begin(),
                            jsc.second->GetSharedWorkerProcesses().end());
    worker_processes.insert(jsc.second->GetSoleWorkerProcesses().begin(),
                            jsc.second->GetSoleWorkerProcesses().end());
    std::unordered_map<NodeID, rpc::JobInfo> node_to_job_info;
    for (const auto &entry : worker_processes) {
      const auto &worker_process = entry.second;
      rpc::WorkerInfo worker_info;
      auto node_id = worker_process->GetNodeID();
      auto job_id = worker_process->GetJobID();
      worker_info.set_worker_process_id(worker_process->GetWorkerProcessID().Binary());
      worker_info.set_is_shared(worker_process->IsShared());
      worker_info.set_slot_capacity(worker_process->GetSlotCapacity());
      worker_info.set_pid(worker_process->GetPID());
      worker_info.set_language(worker_process->GetLanguage());

      fill_resource_func(worker_process->GetRequiredResources(),
                         worker_info.mutable_required_resources());
      fill_resource_func(worker_process->GetResources(),
                         worker_info.mutable_acquired_resources());
      fill_resource_func(worker_process->GetRuntimeResources(),
                         worker_info.mutable_runtime_resources());

      auto it = node_to_job_info.find(node_id);
      if (it == node_to_job_info.end()) {
        rpc::JobInfo job_info;
        job_info.set_job_id(job_id.Binary());
        it = node_to_job_info.emplace(node_id, std::move(job_info)).first;
      }
      it->second.add_worker_info_list()->CopyFrom(worker_info);
    }

    for (const auto &it : node_to_job_info) {
      node_info_map[it.first].add_job_info_list()->CopyFrom(it.second);
    }
  }

  std::vector<rpc::NodeInfo> node_info_list;
  for (const auto &it : node_info_map) {
    node_info_list.push_back(it.second);
  }
  return node_info_list;
}

const absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>>
    &GcsJobDistribution::GetNodeToJobs() const {
  return node_to_jobs_;
}

const absl::flat_hash_map<JobID, std::shared_ptr<GcsJobSchedulingContext>>
    &GcsJobDistribution::GetAllJobSchedulingContexts() const {
  return job_scheduling_contexts_;
}

void GcsJobDistribution::RemoveJobFromNode(const JobID &job_id, const NodeID &node_id) {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    if (iter->second.erase(job_id) && iter->second.empty()) {
      node_to_jobs_.erase(iter);
    }
  }
}

absl::flat_hash_set<JobID> GcsJobDistribution::GetJobsByNodeID(
    const NodeID &node_id) const {
  auto iter = node_to_jobs_.find(node_id);
  if (iter != node_to_jobs_.end()) {
    return iter->second;
  }
  return absl::flat_hash_set<JobID>();
}

std::shared_ptr<GcsWorkerProcess> GcsJobDistribution::GetWorkerProcessById(
    const JobID &job_id, const UniqueID &worker_process_id) const {
  std::shared_ptr<GcsWorkerProcess> worker_process = nullptr;
  if (auto job_scheduling_context = GetJobSchedulingContext(job_id)) {
    worker_process = job_scheduling_context->GetWorkerProcessById(worker_process_id);
  }
  return worker_process;
}

absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
GcsJobDistribution::OnRuntimeWorkerProcessResourcesUpdated(
    const std::shared_ptr<
        absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>
        worker_resources) {
  if (!gcs_resource_manager_) {
    return absl::flat_hash_map<
        JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>();
  }

  absl::flat_hash_set<JobID> updated_jobs;
  bool need_notify_resources_changed = false;
  for (auto &node_entry : *worker_resources.get()) {
    auto node_iter = node_to_jobs_.find(node_entry.first);
    if (node_iter == node_to_jobs_.end()) {
      continue;
    }

    auto &jobs = node_iter->second;
    for (const auto &job_id : jobs) {
      auto job_scheduling_context = GetJobSchedulingContext(job_id);
      if (job_scheduling_context == nullptr) {
        continue;
      }

      auto iter =
          job_scheduling_context->GetNodeToWorkerProcesses().find(node_entry.first);
      if (iter != job_scheduling_context->GetNodeToWorkerProcesses().end()) {
        for (auto &worker_entry : iter->second) {
          auto worker_process = worker_entry.second;
          if (node_entry.second.contains(worker_process->GetPID())) {
            const auto &runtime_resourcs = node_entry.second[worker_process->GetPID()];
            if (job_scheduling_context->GetScheduleOptions()
                    ->runtime_resource_scheduling_enabled_) {
              UpdateWorkerProcessAcquiredResources(worker_process, runtime_resourcs);
              need_notify_resources_changed = true;
            }
            UpdateJobAndWorkerProcessShorttermRuntimeResources(
                job_scheduling_context, worker_process, runtime_resourcs);
            if (runtime_resource_flusher_) {
              runtime_resource_flusher_->Add(worker_process);
            }
            updated_jobs.insert(job_scheduling_context->GetJobConfig().job_id_);
          }
        }
      }
    }
  }

  if (need_notify_resources_changed) {
    gcs_resource_manager_->NotifyResourcesChanged();
  }

  absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
      job_shortterm_runtime_resources;
  for (const auto &job_id : updated_jobs) {
    auto job_scheduling_context = GetJobSchedulingContext(job_id);
    job_shortterm_runtime_resources.emplace(
        job_id, std::make_pair(job_scheduling_context->GetShorttermRuntimeResources(),
                               job_scheduling_context->GetScheduleOptions()));
  }
  return job_shortterm_runtime_resources;
}

void GcsJobDistribution::UpdateWorkerProcessAcquiredResources(
    std::shared_ptr<GcsWorkerProcess> worker_process,
    const ResourceSet &runtime_resources) {
  const auto &cluster_resources = gcs_resource_manager_->GetClusterResources();
  auto resources_iter = cluster_resources.find(worker_process->GetNodeID());
  if (resources_iter == cluster_resources.end()) {
    return;
  }

  const auto &node_resources = *resources_iter->second;
  ResourceSet *mutable_acquired_resources;
  if (worker_process->IsForPlacementGroup()) {
    mutable_acquired_resources = &worker_process->GetMutablePGAcquiredResources();
  } else {
    mutable_acquired_resources = &worker_process->GetMutableResources();
  }

  RAY_LOG(DEBUG) << "Before release : ------------------------"
                 << "\n> pid = " << worker_process->GetPID()
                 << "\n> runtime_resources = " << runtime_resources.ToString()
                 << "\n> node_resources = " << node_resources.DebugString()
                 << "\n> acquired_resources = "
                 << worker_process->GetResources().ToString();

  // Release original worker process resources first.
  if (!gcs_resource_manager_->ReleaseResources(worker_process->GetNodeID(),
                                               *mutable_acquired_resources)) {
    return;
  }

  for (const auto &entry : runtime_resources.GetResourceAmountMap()) {
    // A worker process can not acquire resources that exceed the
    // node's available resources.
    auto node_available_resource_quality =
        node_resources.GetAvailableResources().GetResource(entry.first);
    if (entry.second > node_available_resource_quality) {
      mutable_acquired_resources->AddOrUpdateResource(entry.first,
                                                      node_available_resource_quality);
    } else {
      mutable_acquired_resources->AddOrUpdateResource(entry.first, entry.second);
    }
  }
  // Acquire changed worker process resources again.
  gcs_resource_manager_->AcquireResources(worker_process->GetNodeID(),
                                          *mutable_acquired_resources);

  RAY_LOG(DEBUG) << "After acquire: ------------------------"
                 << "\n> pid = " << worker_process->GetPID()
                 << "\n> runtime_resources = " << runtime_resources.ToString()
                 << "\n> node_resources = " << node_resources.DebugString()
                 << "\n> acquired_resources = "
                 << worker_process->GetResources().ToString();
}

void GcsJobDistribution::UpdateJobAndWorkerProcessShorttermRuntimeResources(
    std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
    std::shared_ptr<GcsWorkerProcess> worker_process,
    const ResourceSet &runtime_resources) {
  RAY_LOG(DEBUG) << "Before short-term update: ------------------------"
                 << "\n> pid = " << worker_process->GetPID()
                 << "\n> runtime_resources = " << runtime_resources.ToString()
                 << "\n> worker_runtime_resources = "
                 << worker_process->GetRuntimeResources().ToString()
                 << "\n> job_shortterm_runtime_resoruces = "
                 << job_scheduling_context->GetShorttermRuntimeResources().ToString();
  job_scheduling_context->UpdateShorttermRuntimeResources(
      worker_process->GetRuntimeResources(), runtime_resources);
  worker_process->UpdateRuntimeResources(runtime_resources);
  RAY_LOG(DEBUG) << "After short-term update: ------------------------"
                 << "\n> pid = " << worker_process->GetPID()
                 << "\n> runtime_resources = " << runtime_resources.ToString()
                 << "\n> worker_runtime_resources = "
                 << worker_process->GetRuntimeResources().ToString()
                 << "\n> job_shortterm_runtime_resoruces = "
                 << job_scheduling_context->GetShorttermRuntimeResources().ToString();
}

void GcsJobDistribution::AdjustPGResources(
    std::shared_ptr<GcsWorkerProcess> worker_process) {
  // Return the delta between logical and runtime resources to the node.
  const auto &pg_acquired_resources = worker_process->GetPGAcquiredResources();
  const auto &required_resources = worker_process->GetOriginalRequiredRuntimeResources();
  for (const auto &resource_label : RUNTIME_RESOURCE_LABELS) {
    auto acquired_quantity = pg_acquired_resources.GetResource(resource_label);
    auto required_quantity = required_resources.GetResource(resource_label);
    auto delta = acquired_quantity - required_quantity;
    if (delta < 0) {
      gcs_resource_manager_->AcquireResources(
          worker_process->GetNodeID(),
          ResourceSet({{resource_label, FractionalResourceQuantity(0) - delta}}));
    } else if (delta > 0) {
      gcs_resource_manager_->ReleaseResources(worker_process->GetNodeID(),
                                              ResourceSet({{resource_label, delta}}));
    }
  }
  worker_process->InitializePGAcquiredResources();
}

void GcsJobDistribution::OnRuntimeResourceSchedulingConfigChanged(
    const std::string &nodegroup_id, std::shared_ptr<ScheduleOptions> schedule_options) {
  for (const auto &job_entry : job_scheduling_contexts_) {
    auto job_scheduling_context = job_entry.second;
    if (job_scheduling_context->GetJobConfig().nodegroup_id_ == nodegroup_id) {
      const auto &node_to_worker_processes =
          job_scheduling_context->GetNodeToWorkerProcesses();
      for (const auto &node_entry : node_to_worker_processes) {
        for (const auto &worker_entry : node_entry.second) {
          auto worker_process = worker_entry.second;
          if (!schedule_options->runtime_resource_scheduling_enabled_) {
            // We are about to disable the runtime resource scheduling.
            if (worker_process->IsForPlacementGroup()) {
              // Return the delta between logical and runtime resources.
              AdjustPGResources(worker_process);
            } else {
              // Rollback the runtime resources by the logical ones.
              UpdateWorkerProcessAcquiredResources(
                  worker_process, worker_process->GetRequiredResources());
            }
          } else {
            // We are about to enable the runtime resource scheduling.
            if (worker_process->IsForPlacementGroup()) {
              // Initialize the acquired resources specialized for PG.
              worker_process->InitializePGAcquiredResources();
            }
          }
        }
      }
    }
  }
}

void GcsWorkerProcessRuntimeResourceFlusher::Flush(
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage) {
  if (queue_.empty()) {
    return;
  }

  for (auto &weak_worker_process : queue_.front()) {
    if (auto worker_process = weak_worker_process.lock()) {
      worker_process->AsyncFlushRuntimeResources(gcs_table_storage, nullptr);
    }
  }
  queue_.pop();
}

void GcsWorkerProcessRuntimeResourceFlusher::Add(
    std::shared_ptr<GcsWorkerProcess> gcs_worker_process) {
  size_t batch_size =
      RayConfig::instance().gcs_runtime_resource_flush_batch_size_each_period();
  if (queue_.empty() || queue_.back().size() == batch_size) {
    std::vector<std::weak_ptr<GcsWorkerProcess>> slot;
    slot.reserve(batch_size);
    queue_.push(std::move(slot));
  }
  queue_.back().emplace_back(std::weak_ptr<GcsWorkerProcess>(gcs_worker_process));
}

}  // namespace gcs
}  // namespace ray