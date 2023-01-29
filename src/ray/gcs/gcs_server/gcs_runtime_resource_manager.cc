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

#include "ray/gcs/gcs_server/gcs_runtime_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_job_distribution.h"
#include "ray/util/resource_util.h"

namespace ray {
namespace gcs {

GcsRuntimeResourceManager::GcsRuntimeResourceManager(
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    std::shared_ptr<GcsResourceManager> gcs_resource_manager,
    std::shared_ptr<GcsJobDistribution> gcs_job_distribution,
    std::function<void(std::shared_ptr<absl::flat_hash_map<
                           NodeID, absl::flat_hash_map<int, ResourceSet>>>)>
        shortterm_runtime_resources_updated_callback,
    std::function<void(const JobID, const ResourceSet)>
        job_longterm_runtime_resources_updated_callback)
    : gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_resource_manager_(std::move(gcs_resource_manager)),
      gcs_job_distribution_(std::move(gcs_job_distribution)),
      shortterm_runtime_resources_updated_callback_(
          std::move(shortterm_runtime_resources_updated_callback)),
      job_longterm_runtime_resources_updated_callback_(
          std::move(job_longterm_runtime_resources_updated_callback)) {}

GcsRuntimeResourceManager::~GcsRuntimeResourceManager() { Stop(); }

void GcsRuntimeResourceManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &worker_process_runtime_resources =
      gcs_init_data.WorkerProcessRuntimeResources();

  std::vector<UniqueID> dead_worker_process_ids;
  auto worker_resources = std::make_shared<
      absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>();
  auto all_worker_resources = std::make_shared<
      absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>();
  for (const auto &job_scheduling_context_entry :
       gcs_job_distribution_->GetAllJobSchedulingContexts()) {
    for (const auto &node_to_worker_process_entry :
         job_scheduling_context_entry.second->GetNodeToWorkerProcesses()) {
      for (const auto &worker_process_entry : node_to_worker_process_entry.second) {
        auto iter = worker_process_runtime_resources.find(worker_process_entry.first);
        if (iter != worker_process_runtime_resources.end()) {
          std::unordered_map<std::string, double> runtime_resources(
              iter->second.runtime_resources().begin(),
              iter->second.runtime_resources().end());
          (*worker_resources)[node_to_worker_process_entry.first].emplace(
              worker_process_entry.second->GetPID(), ResourceSet(runtime_resources));
          (*all_worker_resources)[node_to_worker_process_entry.first].emplace(
              worker_process_entry.second->GetPID(), ResourceSet(runtime_resources));
        } else {
          dead_worker_process_ids.emplace_back(worker_process_entry.first);
          (*all_worker_resources)[node_to_worker_process_entry.first].emplace(
              worker_process_entry.second->GetPID(),
              worker_process_entry.second->GetRuntimeResources());
        }
      }
    }
  }

  RAY_CHECK_OK(gcs_table_storage_->WorkerProcessRuntimeResourceTable().BatchDelete(
      dead_worker_process_ids, [](const Status &status) {}));

  auto job_shortterm_runtime_resources =
      gcs_job_distribution_->OnRuntimeWorkerProcessResourcesUpdated(worker_resources);

  // If the node available resources(CPU/memory) is less than delta, then adjust.
  auto adjusting_worker_resources = std::make_shared<
      absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>();
  FractionalResourceQuantity cpu_delt_quantity =
      RESOURCE_DELTA.GetResource(kCPU_ResourceLabel);
  FractionalResourceQuantity minimum_worker_cpu(0.01);
  FractionalResourceQuantity memory_delt_quantity =
      RESOURCE_DELTA.GetResource(kMemory_ResourceLabel);
  FractionalResourceQuantity minimum_worker_memory(
      ToMemoryUnits(MEMORY_RESOURCE_UNIT_BYTES));
  for (const auto &node_entry : gcs_resource_manager_->GetClusterResources()) {
    const auto &node_available_resources = node_entry.second->GetAvailableResources();
    if (node_available_resources.GetResource(kMemory_ResourceLabel) <
        memory_delt_quantity) {
      auto overloaded_memory =
          memory_delt_quantity -
          node_available_resources.GetResource(kMemory_ResourceLabel);
      for (auto &worker_entry : (*all_worker_resources)[node_entry.first]) {
        auto worker_memory = worker_entry.second.GetResource(kMemory_ResourceLabel);
        // TODO(jinwei): the epsilon to be subtracted has to be re-defined.
        FractionalResourceQuantity memory_to_be_removed(0);
        if (overloaded_memory < worker_memory) {
          memory_to_be_removed = overloaded_memory;
        } else if (worker_memory > minimum_worker_memory) {
          memory_to_be_removed = worker_memory - minimum_worker_memory;
        } else {
          // skip this worker.
          continue;
        }
        (*adjusting_worker_resources)[node_entry.first][worker_entry.first]
            .AddOrUpdateResource(kMemory_ResourceLabel,
                                 worker_memory - memory_to_be_removed);
        overloaded_memory -= memory_to_be_removed;
        if (overloaded_memory <= 0) {
          break;
        }
      }
    }

    if (node_available_resources.GetResource(kCPU_ResourceLabel) < cpu_delt_quantity) {
      auto overloaded_cpu =
          cpu_delt_quantity - node_available_resources.GetResource(kCPU_ResourceLabel);
      for (auto &worker_entry : (*all_worker_resources)[node_entry.first]) {
        auto worker_cpu = worker_entry.second.GetResource(kCPU_ResourceLabel);
        FractionalResourceQuantity cpu_to_be_removed(0);
        if (overloaded_cpu < worker_cpu) {
          cpu_to_be_removed = overloaded_cpu;
        } else if (worker_cpu > minimum_worker_cpu) {
          cpu_to_be_removed = worker_cpu - minimum_worker_cpu;
        } else {
          // skip this worker.
          continue;
        }
        (*adjusting_worker_resources)[node_entry.first][worker_entry.first]
            .AddOrUpdateResource(kCPU_ResourceLabel, worker_cpu - cpu_to_be_removed);
        overloaded_cpu -= cpu_to_be_removed;
        if (overloaded_cpu <= 0) {
          break;
        }
      }
    }
  }

  if (!adjusting_worker_resources->empty()) {
    auto adjusted_job_shortterm_runtime_resources =
        gcs_job_distribution_->OnRuntimeWorkerProcessResourcesUpdated(
            adjusting_worker_resources);
    for (auto &entry : adjusted_job_shortterm_runtime_resources) {
      job_shortterm_runtime_resources[entry.first] = entry.second;
    }
  }
  // Record job shortterm runtime resources.
  auto current_time_ms = current_sys_time_ms();
  for (const auto &job_entry : job_shortterm_runtime_resources) {
    const auto &job_id = job_entry.first;
    const auto &schedule_options = job_entry.second.second;
    if (schedule_options->runtime_resource_scheduling_enabled_) {
      absl::MutexLock lock(&job_window_map_mutex_);
      auto &window_context = job_window_map_[job_id];
      window_context.schedule_options =
          std::make_shared<ScheduleOptions>(*schedule_options);
      window_context.resources_window.emplace_back(job_entry.second.first);
      int64_t interval_ms =
          schedule_options->runtime_resources_history_window_len_s_ * 1000;
      window_context.next_calc_time_ms = current_time_ms + interval_ms;
    }
  }

  record_thread_.reset(new std::thread([this] {
    SetThreadName("res_record");
    /// The asio work to keep record_service_ alive.
    boost::asio::io_service::work io_service_work(record_service_);
    record_service_.run();
  }));

  calc_thread_.reset(new std::thread([this] {
    SetThreadName("res_calc");
    periodical_runner_.reset(new PeriodicalRunner(calc_service_));
    periodical_runner_->RunFnPeriodically(
        [this] { CalcJobLongtermRuntimeResources(); },
        RayConfig::instance().job_runtime_resources_calc_interval_s() * 1000);
    /// The asio work to keep record_service_ alive.
    boost::asio::io_service::work io_service_work(calc_service_);
    calc_service_.run();
  }));
}

void GcsRuntimeResourceManager::Stop() {
  record_service_.stop();
  if (record_thread_->joinable()) {
    record_thread_->join();
  }

  calc_service_.stop();
  if (calc_thread_->joinable()) {
    calc_thread_->join();
  }
}

void GcsRuntimeResourceManager::HandleReportClusterRuntimeResources(
    const rpc::ReportClusterRuntimeResourcesRequest &request,
    rpc::ReportClusterRuntimeResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto node_runtime_resources_map =
      std::make_shared<absl::flat_hash_map<NodeID, rpc::NodeRuntimeResources>>();
  for (const auto &resources_entry : request.node_runtime_resources_list()) {
    if (resources_entry.worker_stat_list_size() > 0) {
      NodeID node_id = NodeID::FromBinary(resources_entry.node_id());
      auto &node_entry = (*node_runtime_resources_map)[node_id];
      node_entry = resources_entry;
    }
  }
  record_service_.post([this, node_runtime_resources_map] {
    RecordRuntimeResources(node_runtime_resources_map);
  });
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

void GcsRuntimeResourceManager::RecordRuntimeResources(
    std::shared_ptr<absl::flat_hash_map<NodeID, rpc::NodeRuntimeResources>>
        node_runtime_resources_map) {
  if (node_runtime_resources_map->empty()) {
    return;
  }
  auto worker_resources_to_update = std::make_shared<
      absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>();
  for (const auto &entry : *node_runtime_resources_map) {
    const auto &node_id = entry.first;
    const auto &resources_entry = entry.second;

    for (const auto &worker : resources_entry.worker_stat_list()) {
      ResourceSet worker_resource_set;
      worker_resource_set.AddOrUpdateResource(
          kMemory_ResourceLabel,
          FractionalResourceQuantity(ToMemoryUnits(double(worker.memory_tail()))));
      worker_resource_set.AddOrUpdateResource(
          kCPU_ResourceLabel,
          FractionalResourceQuantity(double(worker.cpu_tail() / 100.0)));

      (*worker_resources_to_update.get())[node_id][worker.pid()] = worker_resource_set;
      RAY_LOG(DEBUG) << "  |- Worker " << worker.pid()
                     << ": short-term runtime resources are "
                     << worker_resource_set.ToString();
    }
  }
  if (!worker_resources_to_update->empty()) {
    // This callback will be executed by another thread.
    shortterm_runtime_resources_updated_callback_(std::move(worker_resources_to_update));
  }
}

void GcsRuntimeResourceManager::RecordJobShorttermRuntimeResources(
    absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
        job_shortterm_runtime_resources) {
  for (auto &entry : job_shortterm_runtime_resources) {
    auto schedule_options = entry.second.second;
    // Clone a new one to avoid thread sync.
    entry.second.second = std::make_shared<ScheduleOptions>(*schedule_options);
  }
  record_service_.post([this, captured_job_shortterm_runtime_resources =
                                  std::move(job_shortterm_runtime_resources)] {
    PutShorttermRuntimeResourcesToJobResourceWindow(
        std::move(captured_job_shortterm_runtime_resources));
  });
}

void GcsRuntimeResourceManager::CalcJobLongtermRuntimeResources() {
  auto window_context_map = TakeJobResourcesWindow();
  for (auto &entry : window_context_map) {
    const auto &job_id = entry.first;
    const auto &window_context = entry.second;
    const auto &resources_window = window_context.resources_window;
    RAY_LOG(INFO) << "Calc job longterm runtime resources, job_id = " << job_id
                  << ", window_size = " << resources_window.size();
    auto schedule_options = window_context.schedule_options;
    auto memory_tail = 1.0 - schedule_options->runtime_memory_history_window_tail_;
    auto cpu_tail = 1.0 - schedule_options->runtime_cpu_history_window_tail_;
    uint64_t memory_pq_size = (resources_window.size() * memory_tail < 1)
                                  ? 1
                                  : resources_window.size() * memory_tail;
    std::priority_queue<double, std::vector<double>, std::greater<double>> memory_pq;
    uint64_t cpu_pq_size =
        (resources_window.size() * cpu_tail < 1) ? 1 : resources_window.size() * cpu_tail;
    std::priority_queue<double, std::vector<double>, std::greater<double>> cpu_pq;

    for (const auto &resources : resources_window) {
      auto memory_quantity = resources.GetResource(kMemory_ResourceLabel).ToDouble();
      if (memory_pq.size() < memory_pq_size) {
        memory_pq.push(memory_quantity);
      } else {
        if (memory_quantity > memory_pq.top()) {
          memory_pq.pop();
          memory_pq.push(memory_quantity);
        }
      }

      auto cpu_quantity = resources.GetResource(kCPU_ResourceLabel).ToDouble();
      if (cpu_pq.size() < cpu_pq_size) {
        cpu_pq.push(cpu_quantity);
      } else {
        if (cpu_quantity > cpu_pq.top()) {
          cpu_pq.pop();
          cpu_pq.push(cpu_quantity);
        }
      }
    }

    ResourceSet runtime_resources;
    runtime_resources.AddOrUpdateResource(kMemory_ResourceLabel,
                                          FractionalResourceQuantity(memory_pq.top()));
    runtime_resources.AddOrUpdateResource(kCPU_ResourceLabel,
                                          FractionalResourceQuantity(cpu_pq.top()));
    RAY_LOG(INFO) << "Job " << job_id << ": long-term runtime resources are "
                  << runtime_resources.ToString();

    // This callback will be executed by another thread.
    job_longterm_runtime_resources_updated_callback_(job_id,
                                                     std::move(runtime_resources));
  }
}

absl::flat_hash_map<JobID, WindowContext>
GcsRuntimeResourceManager::TakeJobResourcesWindow() {
  absl::flat_hash_map<JobID, WindowContext> window_context_map;
  int64_t current_tims_ms = current_sys_time_ms();
  absl::MutexLock lock(&job_window_map_mutex_);
  for (auto iter = job_window_map_.begin(); iter != job_window_map_.end();) {
    auto current = iter++;
    auto &job_id = current->first;
    auto &current_window_context = current->second;
    if (current_window_context.next_calc_time_ms <= current_tims_ms) {
      if (current_window_context.resources_window.empty()) {
        RAY_LOG(INFO)
            << "Remove resources window as there are no records inside it, job_id = "
            << job_id;
        job_window_map_.erase(current);
        continue;
      }

      auto &window_context = window_context_map[job_id];
      window_context.resources_window =
          std::move(current_window_context.resources_window);
      window_context.schedule_options = current_window_context.schedule_options;
      int64_t interval_ms = current_window_context.schedule_options
                                ->runtime_resources_history_window_len_s_ *
                            1000;
      current_window_context.next_calc_time_ms = current_tims_ms + interval_ms;
    }
  }
  return window_context_map;
}

void GcsRuntimeResourceManager::PutShorttermRuntimeResourcesToJobResourceWindow(
    absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
        job_shortterm_runtime_resources_map) {
  int64_t current_tims_ms = current_sys_time_ms();
  for (const auto &job_entry : job_shortterm_runtime_resources_map) {
    absl::MutexLock lock(&job_window_map_mutex_);
    auto job_id = job_entry.first;
    auto schedule_options = job_entry.second.second;
    if (!schedule_options->runtime_resource_scheduling_enabled_) {
      if (job_window_map_.erase(job_id)) {
        RAY_LOG(INFO) << "Remove resources window as the runtime resource scheduling is "
                         "disabled, job_id = "
                      << job_id;
      }
      continue;
    }

    auto iter = job_window_map_.find(job_id);
    if (iter == job_window_map_.end()) {
      iter = job_window_map_.emplace(job_id, WindowContext()).first;
      int64_t interval_ms =
          schedule_options->runtime_resources_history_window_len_s_ * 1000;
      iter->second.next_calc_time_ms = current_tims_ms + interval_ms;
    }
    iter->second.resources_window.emplace_back(job_entry.second.first);
    iter->second.schedule_options = schedule_options;
  }
}

}  // namespace gcs
}  // namespace ray
