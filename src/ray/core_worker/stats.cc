#include "ray/core_worker/stats.h"

#include "ray/event/event.h"
#include "ray/rpc/brpc/client/stream_client.h"
#include "ray/rpc/brpc/server/stream_service.h"
#include "ray/stats/stats.h"

namespace ray {

bool IsValidKMonitorCharacter(char c) {
  // KMonitor has limitations on `metrics` and `tag` names,
  // only the following characters are allowed, and anything
  // else could be filtered.
  return ((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
          c == '.' || c == '_' || c == '-');
}

CoreWorkerStats::CoreWorkerStats(int metrics_interval_ms)
    : io_work_(io_service_),
      metrics_timer_interval_(metrics_interval_ms),
      running_task_stats_(std::make_shared<RunningTaskStatistics>()) {
  // Setup the timer to report metrics.
  metrics_timer_func_ = [this](const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted) {
      UpdateMetrics();

      metrics_timer_->expires_from_now(metrics_timer_interval_);
      metrics_timer_->async_wait(metrics_timer_func_);
    } else {
      RAY_LOG(INFO) << "worker metrics timer aborted";
    }
  };

  metrics_timer_.reset(new boost::asio::deadline_timer(io_service_));
  metrics_timer_->expires_from_now(metrics_timer_interval_);
  metrics_timer_->async_wait(metrics_timer_func_);

  io_thread_ = std::thread(&CoreWorkerStats::StartIOService, this);
}

CoreWorkerStats::~CoreWorkerStats() {
  io_service_.stop();
  io_thread_.join();
}

void CoreWorkerStats::StartIOService() {
  SetThreadName("worker.stats");
  io_service_.run();
}

void CoreWorkerStats::TaskStatistics::AddTaskSubmitted(const RayFunction &function) {
  auto hash = function.GetFunctionDescriptor()->Hash();

  std::unique_lock<std::mutex> lock(mutex_);
  tasks_submitted++;
  auto iter = tasks_submitted_per_func.find(hash);
  if (iter == tasks_submitted_per_func.end()) {
    tasks_submitted_per_func[hash] = std::make_pair(
        BuildFunctionTagString(function.GetFunctionDescriptor()->CallSiteString()), 1);
  } else {
    iter->second.second++;
  }
}

void CoreWorkerStats::TaskStatistics::AddTaskReceived(const RayFunction &function) {
  auto hash = function.GetFunctionDescriptor()->Hash();

  std::unique_lock<std::mutex> lock(mutex_);
  tasks_received++;
  auto iter = tasks_received_per_func.find(hash);
  if (iter == tasks_received_per_func.end()) {
    tasks_received_per_func[hash] = std::make_pair(
        BuildFunctionTagString(function.GetFunctionDescriptor()->CallSiteString()), 1);
  } else {
    iter->second.second++;
  }
}

void CoreWorkerStats::TaskStatistics::UpdateTaskQueueLength(int64_t task_queue_length) {
  this->task_queue_length = task_queue_length;
}

void CoreWorkerStats::TaskStatistics::UpdateExecutedTasksCount(
    int64_t num_executed_tasks) {
  this->num_executed_tasks = num_executed_tasks;
}

void CoreWorkerStats::TaskStatistics::OnSubmittedDirectActorCallPosting() {
  std::unique_lock<std::mutex> lock(mutex_);
  submitted_direct_actor_calls_posting++;
}

void CoreWorkerStats::TaskStatistics::OnSubmittedDirectActorCallPosted() {
  std::unique_lock<std::mutex> lock(mutex_);
  submitted_direct_actor_calls_posting--;
}

void CoreWorkerStats::TaskStatistics::OnReceivedDirectActorCallPosting() {
  std::unique_lock<std::mutex> lock(mutex_);
  received_direct_actor_calls_posting++;
}

void CoreWorkerStats::TaskStatistics::OnReceivedDirectActorCallPosted() {
  std::unique_lock<std::mutex> lock(mutex_);
  received_direct_actor_calls_posting--;
}

std::string CoreWorkerStats::TaskStatistics::BuildFunctionTagString(
    std::string descriptor_string) {
  // io.ray.api.test.ActorPerformanceTest$Counter-incrementAndGet-()I
  // Replace invalid characters with '.' otherwise the results cannot be shown.
  for (size_t i = 0; i < descriptor_string.size(); i++) {
    if (!IsValidKMonitorCharacter(descriptor_string[i])) {
      descriptor_string[i] = '.';
    }
  }

  return descriptor_string;
}

void CoreWorkerStats::TaskStatistics::OnBackPressured() { back_pressure_count++; }

void CoreWorkerStats::RunningTaskStatistics::TaskStarted(
    const TaskSpecification &task_spec) {
  RunningTaskContext running_task_context(task_spec);

  std::unique_lock<std::mutex> lock(mutex_);
  running_task_contexts_.emplace(task_spec.TaskId(), std::move(running_task_context));
}

void CoreWorkerStats::RunningTaskStatistics::TaskFinished(
    const TaskSpecification &task_spec) {
  std::unique_lock<std::mutex> lock(mutex_);
  running_task_contexts_.erase(task_spec.TaskId());
}

void CoreWorkerStats::UpdateMetrics() {
  std::unordered_map<WorkerID, std::shared_ptr<TaskStatistics>> tmp_task_stats;
  {
    std::unique_lock<std::mutex> lock(task_stats_mutex_);
    for (const auto &entry : task_stats_) {
      tmp_task_stats.insert(entry);
    }
  }

  for (auto &task_stat : tmp_task_stats) {
    // Take the lock for the specific task stat.
    std::unique_lock<std::mutex> lock(task_stat.second->mutex_);
    const auto worker_id_str = task_stat.first.Hex();
    const auto &stat = task_stat.second;
    const ray::stats::TagsType tags = {{ray::stats::WorkerIdKey, worker_id_str}};
    ray::stats::SubmittedTasksCount().Record(stat->tasks_submitted, tags);
    ray::stats::ReceivedTasksCount().Record(stat->tasks_received, tags);
    ray::stats::TaskQueueLength().Record(stat->task_queue_length, tags);
    ray::stats::ExecutedTasksCount().Record(stat->num_executed_tasks, tags);

    for (const auto &entry : stat->tasks_submitted_per_func) {
      ray::stats::TagsType submiited_tasks_tags = tags;
      submiited_tasks_tags.emplace_back(ray::stats::FunctionKey, entry.second.first);
      ray::stats::SubmittedTasksPerFuncCount().Record(entry.second.second,
                                                      submiited_tasks_tags);
    }

    for (const auto &entry : stat->tasks_received_per_func) {
      ray::stats::TagsType received_tasks_tags = tags;
      received_tasks_tags.emplace_back(ray::stats::FunctionKey, entry.second.first);
      ray::stats::ReceivedTasksPerFuncCount().Record(entry.second.second,
                                                     received_tasks_tags);
    }

    ray::stats::SubmittedDirectActorCallsPostingCount().Record(
        stat->submitted_direct_actor_calls_posting, tags);
    ray::stats::ReceivedDirectActorCallsPostingCount().Record(
        stat->received_direct_actor_calls_posting, tags);

    ray::stats::BackPressureCount().Record(stat->back_pressure_count, tags);
  }

  std::unordered_map<WorkerID, std::shared_ptr<MemoryStoreStatistics>>
      tmp_memory_store_stats;
  {
    std::unique_lock<std::mutex> lock(memory_store_stats_mutex_);
    for (const auto &entry : memory_store_stats_) {
      tmp_memory_store_stats.insert(entry);
    }
  }

  for (const auto &memory_stat : tmp_memory_store_stats) {
    std::unique_lock<std::mutex> lock(memory_stat.second->mutex_);
    const auto worker_id_str = memory_stat.first.Hex();
    const auto &stat = memory_stat.second;
    const ray::stats::TagsType tags = {{ray::stats::WorkerIdKey, worker_id_str}};
    ray::stats::ObjectInStoreCount().Record(stat->num_objects_in_memory_store, tags);
    ray::stats::ObjectInStoreBytes().Record(stat->total_object_size_in_memory_store,
                                            tags);
    ray::stats::MemoryStoreHitRate().Record(stat->memory_store_hit_rate, tags);
    ray::stats::LocalObjectHitRate().Record(stat->local_object_hit_rate, tags);
  }

  {
    // We allow users to set job-level environment variables to overwrite event sending
    // intervals in case the default intervals are not suitable.
    // NOTE: Because RayConfig is not initialized yet when constructing CoreWorkerStats,
    // we cannot compute the send event interval in the constructor of CoreWorkerStats.
    auto slow_actor_creation_task_send_event_interval_s =
        RayConfig::instance().slow_actor_creation_task_send_event_interval_s();
    char *actor_creation_task_env_str =
        getenv("RAY_SLOW_ACTOR_CREATION_TASK_SEND_EVENT_INTERVAL_SECONDS");
    if (actor_creation_task_env_str) {
      slow_actor_creation_task_send_event_interval_s =
          static_cast<uint32_t>(std::atol(actor_creation_task_env_str));
    }

    auto slow_non_actor_creation_task_send_event_interval_s =
        RayConfig::instance().slow_non_actor_creation_task_send_event_interval_s();
    char *non_actor_creation_task_env_str =
        getenv("RAY_SLOW_NON_ACTOR_CREATION_TASK_SEND_EVENT_INTERVAL_SECONDS");
    if (non_actor_creation_task_env_str) {
      slow_non_actor_creation_task_send_event_interval_s =
          static_cast<uint32_t>(std::atol(non_actor_creation_task_env_str));
    }

    auto current_time = time(nullptr);
    std::unique_lock<std::mutex> lock(running_task_stats_->mutex_);
    for (auto &entry : running_task_stats_->running_task_contexts_) {
      const auto &task_id = entry.first;
      auto job_id = task_id.JobId();
      auto &context = entry.second;
      auto duration_sec = difftime(current_time, context.start_time_);
      auto message_formatter = [](double duration_sec, const TaskID &task_id,
                                  const JobID &job_id, const ActorID &actor_id,
                                  const std::string &function_descriptor_string) {
        std::ostringstream oss;
        oss << "Task " << task_id << " has been executed for " << duration_sec
            << " seconds. job = " << job_id << ", actor = " << actor_id
            << ", function = " << function_descriptor_string;
        return oss.str();
      };
      if (difftime(current_time, context.last_log_print_time_) >=
          RayConfig::instance().slow_task_print_log_interval_s()) {
        context.last_log_print_time_ = current_time;
        RAY_LOG(WARNING) << message_formatter(duration_sec, task_id, task_id.JobId(),
                                              context.actor_id_,
                                              context.function_descriptor_string_);
      }
      auto slow_task_send_event_interval_s =
          context.is_actor_creation_task_
              ? slow_actor_creation_task_send_event_interval_s
              : slow_non_actor_creation_task_send_event_interval_s;
      if (difftime(current_time, context.last_event_send_time_) >=
          slow_task_send_event_interval_s) {
        context.last_event_send_time_ = current_time;
        RAY_EVENT(WARNING, EVENT_LABEL_SLOW_TASK)
                .WithField("job_id", job_id.Hex())
                .WithField("actor_id", context.actor_id_.Hex())
                .WithField("task_id", task_id.Hex())
            << message_formatter(duration_sec, task_id, task_id.JobId(),
                                 context.actor_id_, context.function_descriptor_string_);
      }
    }
  }
}

std::shared_ptr<CoreWorkerStats::MemoryStoreStatistics>
CoreWorkerStats::AddMemoryStoreStats(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "AddMemoryStoreStats: creating memory store stat for worker ID: "
                 << worker_id;
  std::unique_lock<std::mutex> lock(memory_store_stats_mutex_);
  auto result =
      memory_store_stats_.emplace(worker_id, std::make_shared<MemoryStoreStatistics>());
  RAY_CHECK(result.second);
  return result.first->second;
}

void CoreWorkerStats::RemoveMemoryStoreStats(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "RemoveMemoryStoreStats: removing memory store stat for worker ID: "
                 << worker_id;
  std::unique_lock<std::mutex> lock(memory_store_stats_mutex_);
  RAY_UNUSED(memory_store_stats_.erase(worker_id));
}

std::shared_ptr<CoreWorkerStats::TaskStatistics> CoreWorkerStats::AddTaskStats(
    const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "AddTaskStats: creating task stat for worker ID: " << worker_id;
  std::unique_lock<std::mutex> lock(task_stats_mutex_);

  auto result = task_stats_.emplace(worker_id, std::make_shared<TaskStatistics>());
  RAY_CHECK(result.second);

  return result.first->second;
}

void CoreWorkerStats::RemoveTaskStats(const WorkerID &worker_id) {
  RAY_LOG(DEBUG) << "RemoveTaskStats: removing task stat for worker ID: " << worker_id;
  std::unique_lock<std::mutex> lock(task_stats_mutex_);
  // In some corner cases, the remove operation may be performed multiple times. We don't
  // check the result here.
  RAY_UNUSED(task_stats_.erase(worker_id));
}

std::shared_ptr<CoreWorkerStats::RunningTaskStatistics>
CoreWorkerStats::GetRunningTaskStats() {
  return running_task_stats_;
}

}  // namespace ray
