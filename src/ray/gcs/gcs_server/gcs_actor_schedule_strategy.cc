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

#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"
#include <memory>

#include "absl/strings/ascii.h"
#include "ray/common/constants.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/event/event.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_job_distribution_formatter.h"
#include "ray/gcs/gcs_server/gcs_label_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

NodeID GcsRandomActorScheduleStrategy::Schedule(std::shared_ptr<GcsActor> actor,
                                                Status *status /*=nullptr*/) {
  // Select a node to lease worker for the actor.
  std::shared_ptr<rpc::GcsNodeInfo> node;

  // If an actor has resource requirements, we will try to schedule it on the same node as
  // the owner if possible.
  const auto &task_spec = actor->GetCreationTaskSpecification();
  if (!task_spec.GetRequiredResources().IsEmpty()) {
    auto maybe_node = gcs_node_manager_->GetAliveNode(actor->GetOwnerNodeID());
    node = maybe_node.has_value() ? maybe_node.value() : SelectNodeRandomly();
  } else {
    node = SelectNodeRandomly();
  }

  if (node == nullptr) {
    if (status) {
      std::string message =
          "Resources of nodegroup " + actor->GetNodegroupId() + " are not enough.";
      *status = Status::ResourcesNotEnough(message);
    }
    return NodeID::Nil();
  }

  return NodeID::FromBinary(node->basic_gcs_node_info().node_id());
}

std::shared_ptr<rpc::GcsNodeInfo> GcsRandomActorScheduleStrategy::SelectNodeRandomly()
    const {
  auto &alive_nodes = gcs_node_manager_->GetAllAliveNodes();
  if (alive_nodes.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, alive_nodes.size() - 1);
  int key_index = distribution(gen_);
  int index = 0;
  auto iter = alive_nodes.begin();
  for (; index != key_index && iter != alive_nodes.end(); ++index, ++iter)
    ;
  return iter->second;
}

/////////////////////////Begin of ResourceBasedSchedulingStrategy/////////////////////////
ResourceBasedSchedulingStrategy::ResourceBasedSchedulingStrategy(
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<GcsJobDistribution> gcs_job_distribution,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<GcsResourceScheduler> resource_scheduler,
    std::shared_ptr<GcsPlacementGroupManager> gcs_placement_group_manager,
    std::function<bool(const BundleID &)> is_bundle_resource_reserved,
    std::function<bool(const NodeID &, const std::string &)>
        is_node_in_nodegroup_callback,
    std::shared_ptr<GcsLabelManager> gcs_label_manager)
    : gcs_resource_manager_(std::move(gcs_resource_manager)),
      gcs_job_distribution_(std::move(gcs_job_distribution)),
      gcs_table_storage_(std::move(gcs_table_storage)),
      is_bundle_resource_reserved_(std::move(is_bundle_resource_reserved)),
      is_node_in_nodegroup_callback_(std::move(is_node_in_nodegroup_callback)),
      resource_scheduler_(std::move(resource_scheduler)),
      gcs_placement_group_manager_(std::move(gcs_placement_group_manager)),
      gcs_label_manager_(std::move(gcs_label_manager)) {
  RAY_CHECK(gcs_resource_manager_ != nullptr);
  RAY_CHECK(gcs_job_distribution_ != nullptr);
  RAY_CHECK(gcs_table_storage_ != nullptr);
  RAY_CHECK(gcs_placement_group_manager_ != nullptr);
}

void ResourceBasedSchedulingStrategy::Initialize(const GcsInitData &gcs_init_data) {
  for (const auto &entry : gcs_init_data.Actors()) {
    auto job_id = entry.first.JobId();
    auto job_iter = gcs_init_data.Jobs().find(job_id);
    auto is_job_dead =
        (job_iter == gcs_init_data.Jobs().end() || job_iter->second.is_dead());
    if (is_job_dead || entry.second.state() == ray::rpc::ActorTableData::DEAD) {
      continue;
    }

    auto worker_process_id = UniqueID::FromBinary(entry.second.worker_process_id());
    if (worker_process_id.IsNil()) {
      continue;
    }

    auto node_id = NodeID::FromBinary(entry.second.address().raylet_id());
    auto node_iter = gcs_init_data.Nodes().find(node_id);
    if (node_iter == gcs_init_data.Nodes().end() ||
        node_iter->second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::DEAD) {
      RAY_LOG(INFO) << "Ignore the worker process " << worker_process_id
                    << " as the node " << node_id
                    << " is already dead. actor_id = " << entry.first;
      continue;
    }

    auto job_scheduling_context =
        gcs_job_distribution_->FindOrCreateJobSchedulingContext(job_id);

    auto worker_process =
        job_scheduling_context->GetWorkerProcess(node_id, worker_process_id);
    if (worker_process == nullptr) {
      std::unordered_map<std::string, double> resources;
      for (auto it = entry.second.required_resources().begin();
           it != entry.second.required_resources().end(); ++it) {
        resources.emplace(it->first, it->second);
      }
      ResourceSet required_resources(resources);

      auto pid = entry.second.pid();
      const auto &language = entry.second.language();
      bool is_shared = !ray::NeedSoleWorkerProcess(language, resources);
      if (is_shared) {
        // If the resources required do not contain `kMemory_ResourceLabel` then add one
        // with the value of `worker_process_default_memory_units`
        const auto &job_config = job_scheduling_context->GetJobConfig();
        required_resources.AddOrUpdateResource(
            kMemory_ResourceLabel, job_config.java_worker_process_default_memory_units_);
      }
      int num_workers_per_process = 1;
      if (language == rpc::Language::JAVA) {
        const auto &job_config = job_scheduling_context->GetJobConfig();
        num_workers_per_process = job_config.num_java_workers_per_process_;
      }
      auto slot_capacity = is_shared ? num_workers_per_process : 1;

      auto schedule_options = job_scheduling_context->GetScheduleOptions();
      worker_process = GcsWorkerProcess::Create(
          worker_process_id, node_id, job_id, entry.second.language(), required_resources,
          is_shared, slot_capacity,
          schedule_options->runtime_resource_scheduling_enabled_);
      worker_process->SetPID(pid);
      RAY_CHECK(gcs_resource_manager_->AcquireResources(node_id,
                                                        worker_process->GetResources()));
      gcs_job_distribution_->AddWorkerProcess(worker_process);
    }
    worker_process->AssignActor(entry.first);
  }

  RAY_LOG(INFO) << "gcs_job_distribution: "
                << JobDistributionFormatter::Format(gcs_job_distribution_);
}

NodeID ResourceBasedSchedulingStrategy::Schedule(std::shared_ptr<GcsActor> actor,
                                                 Status *status /*=nullptr*/) {
  auto bundle_id = actor->GetCreationTaskSpecification().PlacementGroupBundleId();
  if (bundle_id.second >= 0 && !is_bundle_resource_reserved_(bundle_id)) {
    RAY_LOG(DEBUG) << "Failed to schedule actor " << actor->GetActorID()
                   << " for the time being as the bundle is not ready yet.";
    if (status) {
      std::string message =
          "Resources of nodegroup " + actor->GetNodegroupId() + " are not enough.";
      *status = Status::ResourcesNotEnough(message);
    }
    return NodeID::Nil();
  }

  RAY_CHECK(actor->GetWorkerProcessID().IsNil());
  bool need_sole_worker_process =
      ray::NeedSoleWorkerProcess(actor->GetCreationTaskSpecification());
  if (auto selected_worker_process =
          SelectOrAllocateWorkerProcess(actor, need_sole_worker_process, status)) {
    // If succeed in selecting an available worker process then just assign the actor, it
    // will cosume a slot inside the worker process.
    RAY_CHECK(selected_worker_process->AssignActor(actor->GetActorID()))
        << ", actor id = " << actor->GetActorID()
        << ", worker_process = " << selected_worker_process->ToString();
    // Bind the worker process id to the physical worker process.
    actor->SetWorkerProcessID(selected_worker_process->GetWorkerProcessID());

    RAY_LOG(INFO) << "Finished selecting node " << selected_worker_process->GetNodeID()
                  << " to schedule actor " << actor->GetActorID()
                  << " with worker_process_id = "
                  << selected_worker_process->GetWorkerProcessID();
    return selected_worker_process->GetNodeID();
  }

  // If failed to select an available worker process then reset the worker process id of
  // the actor and just return a nil node id.
  RAY_LOG(WARNING) << "There are no available resources to schedule the actor "
                   << actor->GetActorID()
                   << ", need sole worker process = " << need_sole_worker_process;
  return NodeID::Nil();
}

void ResourceBasedSchedulingStrategy::SetPIDForWorkerProcess(
    std::shared_ptr<GcsActor> actor, int32_t pid) {
  auto worker_process_id = actor->GetWorkerProcessID();
  RAY_CHECK(!worker_process_id.IsNil());
  if (auto job_scheduling_context =
          gcs_job_distribution_->GetJobSchedulingContext(actor->GetActorID().JobId())) {
    if (auto worker_process =
            job_scheduling_context->GetWorkerProcessById(worker_process_id)) {
      worker_process->SetPID(pid);
    }
  }
}

std::shared_ptr<GcsWorkerProcess>
ResourceBasedSchedulingStrategy::SelectOrAllocateWorkerProcess(
    std::shared_ptr<GcsActor> actor, bool need_sole_worker_process,
    Status *status /*=nullptr*/) {
  auto job_id = actor->GetActorID().JobId();
  auto job_scheduling_context =
      gcs_job_distribution_->FindOrCreateJobSchedulingContext(job_id);

  const auto &task_spec = actor->GetCreationTaskSpecification();
  auto required_resources = task_spec.GetRequiredPlacementResources();

  if (need_sole_worker_process) {
    // If the task needs a sole worker process then allocate a new one.
    return AllocateNewWorkerProcess(job_scheduling_context, required_resources,
                                    /*is_shared=*/false, task_spec, status);
  }

  std::shared_ptr<GcsWorkerProcess> selected_worker_process;

  if (!task_spec.GetSchedulingStrategy().has_actor_affinity_scheduling_strategy() &&
      !task_spec.GetSchedulingStrategy().has_node_affinity_scheduling_strategy()) {
    // Otherwise, the task needs a shared worker process.
    // If there are unused slots in the allocated shared worker process, select the one
    // with the largest number of slots.
    const auto &shared_worker_processes =
        job_scheduling_context->GetSharedWorkerProcesses();
    // Select a worker process with the largest number of available slots.
    size_t max_available_slot_count = 0;
    for (const auto &entry : shared_worker_processes) {
      const auto &shared_worker_process = entry.second;
      if (max_available_slot_count < shared_worker_process->GetAvailableSlotCount()) {
        max_available_slot_count = shared_worker_process->GetAvailableSlotCount();
        selected_worker_process = shared_worker_process;
      }
    }
  }

  // If there are no existing shared worker process then allocate a new one.
  if (selected_worker_process == nullptr) {
    // If the resources required do not contain `kMemory_ResourceLabel` then add one
    // with the value of `worker_process_default_memory_units`
    const auto &job_config = job_scheduling_context->GetJobConfig();
    required_resources.AddOrUpdateResource(
        kMemory_ResourceLabel, job_config.java_worker_process_default_memory_units_);
    selected_worker_process =
        AllocateNewWorkerProcess(job_scheduling_context, required_resources,
                                 /*is_shared=*/true, task_spec, status);
  }

  return selected_worker_process;
}

std::shared_ptr<GcsWorkerProcess>
ResourceBasedSchedulingStrategy::AllocateNewWorkerProcess(
    std::shared_ptr<ray::gcs::GcsJobSchedulingContext> job_scheduling_context,
    const ResourceSet &required_resources, bool is_shared,
    const TaskSpecification &task_spec, Status *status /*=nullptr*/) {
  const auto &job_config = job_scheduling_context->GetJobConfig();
  // Figure out the `num_workers_per_process` and `slot_capacity`.
  int num_workers_per_process = 1;
  const auto &language = task_spec.GetLanguage();
  if (language == rpc::Language::JAVA) {
    num_workers_per_process = job_config.num_java_workers_per_process_;
  }
  auto slot_capacity = is_shared ? num_workers_per_process : 1;

  RAY_LOG(INFO) << "Allocating new worker process for job " << job_config.job_id_
                << ", language = " << rpc::Language_Name(language)
                << ", is_shared = " << is_shared << ", slot_capacity = " << slot_capacity
                << "\nrequired_resources = " << required_resources.ToString();
  RAY_LOG(DEBUG) << "Current cluster resources = " << gcs_resource_manager_->ToString();
  // The `required_resources` may contain multipule types of resources, but it only rely
  // on memory resources to calculate the constraints of real resources. So extract the
  // memory resource and check if the job claimed resources are satisfied with it.
  ResourceSet constraint_resources = ray::GetConstraintResources(required_resources);
  if (job_config.enable_job_quota_ &&
      !constraint_resources.IsSubset(job_scheduling_context->GetAvailableResources())) {
    // Return nullptr if the job's available resources are not enough.
    job_scheduling_context->OnJobResourcesInsufficiant(constraint_resources, task_spec);
    if (status) {
      std::string message =
          "The quota of job " + job_config.job_id_.Hex() + " is not enough.";
      *status = Status::JobQuotaNotEnough(message);
    }
    return nullptr;
  }

  // Allocate resources from cluster.
  auto selected_node_id =
      AllocateResources(job_scheduling_context, required_resources, task_spec);
  if (selected_node_id.IsNil()) {
    ray::stats::AllocateResourceFailedCount().Record(1);
    WarnResourceAllocationFailure(job_scheduling_context, task_spec, required_resources);
    if (status) {
      if (task_spec.GetSchedulingStrategy().has_node_affinity_scheduling_strategy() &&
          !task_spec.GetSchedulingStrategy().node_affinity_scheduling_strategy().soft()) {
        std::string message =
            "The nodes specified via NodeAffinitySchedulingStrategy doesn't exist any "
            "more or is inavailable, strategy: " +
            task_spec.GetSchedulingStrategy()
                .node_affinity_scheduling_strategy()
                .DebugString();
        *status = Status::Unschedulable(message);
      } else {
        std::string message =
            "Resources of nodegroup " + job_config.nodegroup_id_ + " are not enough.";
        *status = Status::ResourcesNotEnough(message);
      }
    }
    return nullptr;
  }

  // Create a new gcs worker process.
  auto schedule_options = job_scheduling_context->GetScheduleOptions();
  auto gcs_worker_process = GcsWorkerProcess::Create(
      selected_node_id, job_config.job_id_, language, required_resources, is_shared,
      slot_capacity, schedule_options->runtime_resource_scheduling_enabled_);

  // Add resources changed listener.
  // The resources of the worker process will be updated as real resources are collected
  // periodically.
  gcs_worker_process->AddResourceChangedListener([](const NodeID &node_id,
                                                    const ResourceSet &old_resources,
                                                    const ResourceSet &new_resources) {});

  // Add the gcs worker process to the job scheduling context which manager the lifetime
  // of the worker process.
  RAY_CHECK(gcs_job_distribution_->AddWorkerProcess(gcs_worker_process));
  RAY_LOG(INFO) << "Succeed in allocating new worker process for job "
                << job_config.job_id_ << " from node " << selected_node_id
                << ", worker_process = " << gcs_worker_process->ToString();

  return gcs_worker_process;
}

NodeContext ResourceBasedSchedulingStrategy::GetHighestScoreNodeResource(
    const ResourceSet &required_resources,
    std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context) const {
  const auto &node_context_list = gcs_resource_manager_->GetNodeContextList();
  const auto &nodegroup_id = job_scheduling_context->GetJobConfig().nodegroup_id_;
  auto schedule_options = job_scheduling_context->GetScheduleOptions();

  /// Get the highest score node
  InnerProductScorer scorer;

  auto rare_resource_scheduling_enabled =
      RayConfig::instance().rare_resource_scheduling_enabled();
  bool demand_contains_rare_resources = false;
  bool contains_placement_group = false;
  if (rare_resource_scheduling_enabled) {
    demand_contains_rare_resources = required_resources.ContainsRareResources();
    contains_placement_group = required_resources.ContainsPlacementGroup();
  }

  double highest_score = -1;
  NodeContext highest_score_node = {NodeID::Nil(), "", nullptr, false};
  for (const auto &node_context : node_context_list) {
    if (!is_node_in_nodegroup_callback_(node_context.node_id_, nodegroup_id)) {
      continue;
    }

    if (rare_resource_scheduling_enabled) {
      if (!contains_placement_group && !demand_contains_rare_resources &&
          node_context.contains_rare_resources_) {
        // Skip node with rare resources.
        continue;
      }
    }

    double inner_product_val =
        scorer.Score(required_resources, node_context, schedule_options.get());
    if (inner_product_val > highest_score) {
      highest_score = inner_product_val;
      highest_score_node = node_context;
    }
  }

  return highest_score_node;
}

NodeID ResourceBasedSchedulingStrategy::AllocateResources(
    std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
    const ResourceSet &required_resources, const TaskSpecification &task_spec) {
  const auto &job_config = job_scheduling_context->GetJobConfig();
  const auto &scheduling_strategy = task_spec.GetSchedulingStrategy();
  bool require_placement_group = !task_spec.PlacementGroupBundleId().first.IsNil();

  SchedulingType scheduling_type;
  std::shared_ptr<ResourceScheduleContext> schedule_context;
  if (require_placement_group) {
    auto context = std::make_shared<GcsActorAffinityWithBundleScheduleContext>(
        job_config.job_id_, job_config.nodegroup_id_,
        job_scheduling_context->GetScheduleOptions());
    context->SetPlacementGroupBundleLocationIndex(
        gcs_placement_group_manager_->GetCommittedBundleLocationIndex());
    context->SetPlacementGroupBundleID(task_spec.PlacementGroupBundleId());
    scheduling_type = SchedulingType::AFFINITY_WITH_BUNDLE;
    schedule_context = context;
  } else if (scheduling_strategy.has_actor_affinity_scheduling_strategy()) {
    RAY_CHECK(gcs_label_manager_ != nullptr) << "Gcs label manager is not initialized..";
    schedule_context = std::make_shared<GcsActorAffinityScheduleContext>(
        job_config.job_id_, job_config.nodegroup_id_, job_config.ray_namespace_,
        job_scheduling_context->GetScheduleOptions(), *gcs_label_manager_,
        scheduling_strategy);
    scheduling_type = SchedulingType::ACTOR_AFFINITY;
  } else if (scheduling_strategy.has_node_affinity_scheduling_strategy()) {
    schedule_context = std::make_shared<GcsNodeAffinityScheduleContext>(
        job_config.job_id_, job_config.nodegroup_id_, job_config.ray_namespace_,
        job_scheduling_context->GetScheduleOptions(),
        scheduling_strategy.node_affinity_scheduling_strategy());
    scheduling_type = SchedulingType::NODE_AFFINITY;
  } else {
    scheduling_type = SchedulingType::SPREAD;
    schedule_context = std::make_shared<GcsActorScheduleContext>(
        job_config.job_id_, job_config.nodegroup_id_,
        job_scheduling_context->GetScheduleOptions());
  }
  bool ok = resource_scheduler_->Schedule({&required_resources}, scheduling_type,
                                          schedule_context.get());
  if (!ok) {
    RAY_LOG(INFO)
        << "Scheduling resources failed, schedule type = SchedulingType::SPREAD";
    return NodeID::Nil();
  }

  RAY_CHECK(schedule_context->selected_nodes.size() == 1);

  auto selected_node_id = schedule_context->selected_nodes[0];
  if (!selected_node_id.IsNil()) {
    // Acquire the resources from the selected node.
    RAY_CHECK(
        gcs_resource_manager_->AcquireResources(selected_node_id, required_resources));
  }

  return selected_node_id;
}

void ResourceBasedSchedulingStrategy::CancelOnWorkerProcess(
    const ActorID &actor_id, const UniqueID &worker_process_id) {
  RAY_LOG(INFO) << "Removing actor " << actor_id << " from worker process "
                << worker_process_id << ", job id = " << actor_id.JobId();
  if (auto worker_process = gcs_job_distribution_->GetWorkerProcessById(
          actor_id.JobId(), worker_process_id)) {
    if (worker_process->RemoveActor(actor_id)) {
      RAY_LOG(INFO) << "Finished removing actor " << actor_id << " from worker process "
                    << worker_process_id << ", job id = " << actor_id.JobId();
      if (worker_process->GetUsedSlotCount() == 0) {
        auto node_id = worker_process->GetNodeID();
        RAY_LOG(INFO) << "Remove worker process " << worker_process_id << " from node "
                      << node_id << " as there are no more actors bind to it.";
        // Recycle this worker process.
        auto removed_worker_process =
            gcs_job_distribution_->RemoveWorkerProcessByWorkerProcessID(
                worker_process->GetNodeID(), worker_process_id, actor_id.JobId());
        RAY_CHECK(removed_worker_process == worker_process);
        if (gcs_resource_manager_->ReleaseResources(
                node_id, removed_worker_process->GetResources())) {
          NotifyClusterResourcesChanged();
        }
      }
    } else {
      RAY_LOG(WARNING) << "Failed to remove actor " << actor_id << " from worker process "
                       << worker_process_id
                       << " as the actor is already removed from this worker process.";
    }
  } else {
    RAY_LOG(WARNING) << "Failed to remove actor " << actor_id << " from worker process "
                     << worker_process_id << " as the worker process is not exist.";
  }
}

absl::flat_hash_set<JobID> ResourceBasedSchedulingStrategy::OnNodeRemoved(
    const NodeID &node_id) {
  RAY_LOG(INFO) << "Node " << node_id << " is removed.";
  auto affected_jobs = gcs_job_distribution_->GetJobsByNodeID(node_id);
  // Remove all worker processes associated with this node.
  auto removed_worker_processes =
      gcs_job_distribution_->RemoveWorkerProcessesByNodeID(node_id);
  if (!removed_worker_processes.empty()) {
    std::ostringstream ostr;
    for (auto &removed_worker_process : removed_worker_processes) {
      ostr << removed_worker_process->GetWorkerProcessID() << "  ";
    }
    RAY_LOG(INFO) << "Related worker processes are removed: " << ostr.str()
                  << ", as node " << node_id << " is removed.";
  }
  return affected_jobs;
}

bool ResourceBasedSchedulingStrategy::IsWorkerProcessDead(
    const UniqueID &worker_process_id, const JobID &job_id) {
  auto worker_process =
      gcs_job_distribution_->GetWorkerProcessById(job_id, worker_process_id);
  return worker_process == nullptr;
}

void ResourceBasedSchedulingStrategy::OnWorkerProcessDead(
    const NodeID &node_id, const WorkerID &worker_id, const UniqueID &worker_process_id,
    const JobID &job_id) {
  // Remove the worker process associated with the specified
  // tuple(node_id,worker_process_id,job_id).
  RAY_LOG(INFO) << "Worker process " << worker_process_id
                << " is dead, node_id = " << node_id << ", job_id = " << job_id;
  auto removed_worker_process =
      gcs_job_distribution_->RemoveWorkerProcessByWorkerProcessID(
          node_id, worker_process_id, job_id);
  if (removed_worker_process) {
    RAY_LOG(WARNING) << "Related worker process is removed: " << worker_process_id
                     << ", as worker " << worker_id << " is dead, node_id = " << node_id
                     << " job_id = " << job_id;
    if (gcs_resource_manager_->ReleaseResources(node_id,
                                                removed_worker_process->GetResources())) {
      NotifyClusterResourcesChanged();
    }
  }
}

std::vector<rpc::NodeInfo> ResourceBasedSchedulingStrategy::GetJobDistribution() const {
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

  const auto &cluster_resources = gcs_resource_manager_->GetClusterResources();
  auto node_info_list = gcs_job_distribution_->GetJobDistribution();
  for (auto &node_info : node_info_list) {
    auto node_id = NodeID::FromBinary(node_info.node_id());
    auto iter = cluster_resources.find(node_id);
    if (iter != cluster_resources.end()) {
      const auto &scheduling_resources = *iter->second;
      fill_resource_func(scheduling_resources.GetTotalResources(),
                         node_info.mutable_total_resources());
      fill_resource_func(scheduling_resources.GetAvailableResources(),
                         node_info.mutable_available_resources());
    }
  }
  return node_info_list;
}

void ResourceBasedSchedulingStrategy::OnJobFinished(const JobID &job_id) {
  // Remove job scheduling context to prevent memory from leaking.
  auto scheduling_context = gcs_job_distribution_->RemoveJobSchedulingContext(job_id);
  if (scheduling_context != nullptr) {
    bool is_cluster_resource_changed = false;
    // Return all resources allocated by worker processes of the job back to the cluster
    // resources pool.
    const auto &node_to_worker_processes = scheduling_context->GetNodeToWorkerProcesses();
    for (auto &node_to_worker_processes_entry : node_to_worker_processes) {
      for (auto &worker_process_entry : node_to_worker_processes_entry.second) {
        auto worker_process = worker_process_entry.second;
        if (worker_process->IsForPlacementGroup() &&
            scheduling_context->GetScheduleOptions()
                ->runtime_resource_scheduling_enabled_) {
          gcs_job_distribution_->AdjustPGResources(worker_process);
        }
        gcs_resource_manager_->ReleaseResources(node_to_worker_processes_entry.first,
                                                worker_process->GetResources());
        if (!worker_process->IsForPlacementGroup()) {
          gcs_resource_manager_->SubtractNodeTotalRequiredResources(
              node_to_worker_processes_entry.first,
              worker_process->GetOriginalRequiredRuntimeResources());
        }
        is_cluster_resource_changed = true;
      }
    }

    if (is_cluster_resource_changed) {
      NotifyClusterResourcesChanged();
    }
  }
}

Status ResourceBasedSchedulingStrategy::UpdateJobResourceRequirements(
    const rpc::JobTableData &job_table_data) {
  auto job_id = JobID::FromBinary(job_table_data.job_id());
  auto scheduling_context = gcs_job_distribution_->GetJobSchedulingContext(job_id);
  if (scheduling_context) {
    auto status = scheduling_context->UpdateJobResourceRequirements(job_table_data);
    if (!status.ok()) {
      return status;
    }
  }

  NotifyClusterResourcesChanged();
  return Status::OK();
}

void ResourceBasedSchedulingStrategy::AddClusterResourcesChangedListener(
    std::function<void()> listener) {
  RAY_CHECK(listener != nullptr);
  resource_changed_listeners_.emplace_back(listener);
}

void ResourceBasedSchedulingStrategy::NotifyClusterResourcesChanged() {
  for (auto &listener : resource_changed_listeners_) {
    listener();
  }
}

std::string ResourceBasedSchedulingStrategy::ToString() const {
  return JobDistributionFormatter::Format(gcs_job_distribution_);
}

void ResourceBasedSchedulingStrategy::WarnResourceAllocationFailure(
    std::shared_ptr<GcsJobSchedulingContext> job_scheduling_context,
    const TaskSpecification &task_spec, const ResourceSet &required_resources) const {
  const auto &job_config = job_scheduling_context->GetJobConfig();
  auto scheduling_node_resource =
      GetHighestScoreNodeResource(required_resources, job_scheduling_context);
  auto scheduling_resource = scheduling_node_resource.scheduling_resources_;
  const std::string &scheduling_resource_str =
      scheduling_resource ? scheduling_resource->DebugString() : "None";
  std::ostringstream ostr;
  ostr << "No enough resources for creating actor " << task_spec.ActorCreationId()
       << "\nActor class: " << task_spec.FunctionDescriptor()->ToString()
       << "\nJob id: " << job_config.job_id_
       << "\nRequired resources: " << required_resources.ToString();

  if (task_spec.GetSchedulingStrategy().has_node_affinity_scheduling_strategy()) {
    const auto &node_affinity_strategy =
        task_spec.GetSchedulingStrategy().node_affinity_scheduling_strategy();
    ostr << "\nNode affinity strategy: " << node_affinity_strategy.DebugString();
    ostr << "\n{";
    for (int i = 0; i < node_affinity_strategy.nodes_size(); i++) {
      auto node_id = NodeID::FromBinary(node_affinity_strategy.nodes(i));
      const auto node_context_opt = gcs_resource_manager_->GetNodeContext(node_id);
      if (node_context_opt) {
        ostr << "node id: " << node_id << ", resources: "
             << ((*node_context_opt).scheduling_resources_
                     ? (*node_context_opt).scheduling_resources_->DebugString()
                     : "None.");
      }
    }
    ostr << "\n}";
  } else if (task_spec.GetSchedulingStrategy().has_actor_affinity_scheduling_strategy()) {
    ostr << "\nActor affinity require ray namespace: " << job_config.ray_namespace_
         << ", scheduling expressions: "
         << resource_scheduler_->GetMatchExpressionDebugString(
                task_spec.GetSchedulingStrategy().actor_affinity_scheduling_strategy())
         << "\nactor label info: " << gcs_label_manager_->DebugString();
  }

  if (scheduling_node_resource.node_id_.IsNil()) {
    ostr << "\nThere are no available nodes in the nodegroup "
         << job_config.nodegroup_id_;
  } else {
    ostr << "\nThe node with the most resources is:"
         << "\n   Node id: " << scheduling_node_resource.node_id_
         << "\n   Node resources: " << scheduling_resource_str;
  }
  std::string message = ostr.str();

  RAY_LOG(WARNING) << message;
  RAY_LOG(DEBUG) << "Cluster resources: " << gcs_resource_manager_->ToString();

  RAY_EVENT(ERROR, EVENT_LABEL_JOB_FAILED_TO_ALLOCATE_RESOURCE)
          .WithField("job_id", job_config.job_id_.Hex())
      << message;
}

/////////////////////////End of ResourceBasedSchedulingStrategy/////////////////////////

}  // namespace gcs
}  // namespace ray
