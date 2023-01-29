#ifndef RAY_CORE_WORKER_CORE_WORKER_STATS_H
#define RAY_CORE_WORKER_CORE_WORKER_STATS_H

#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_service.hpp>
#include "ray/core_worker/common.h"

namespace ray {

/// TODO(zhijunfu): optimize the locking.
class CoreWorkerStats {
 public:
  CoreWorkerStats(int metrics_interval_ms);
  ~CoreWorkerStats();

  struct TaskStatistics {
    void AddTaskSubmitted(const RayFunction &function);
    void AddTaskReceived(const RayFunction &function);
    void UpdateTaskQueueLength(int64_t task_queue_length);
    void UpdateExecutedTasksCount(int64_t num_executed_tasks);
    void OnSubmittedDirectActorCallPosting();
    void OnSubmittedDirectActorCallPosted();
    void OnReceivedDirectActorCallPosting();
    void OnReceivedDirectActorCallPosted();
    void OnBackPressured();

    /// Protects the fields in this struct. This is to reduce the scope
    /// of the mutex in `CoreWorkerStats`.
    std::mutex mutex_;
    /// Number of tasks submitted per second by this worker.
    uint64_t tasks_submitted = 0;
    /// Number of tasks received per second by this worker.
    uint64_t tasks_received = 0;
    /// Number of tasks that have been pushed to the actor but not executed.
    std::atomic<int64_t> task_queue_length;
    /// Number of executed tasks.
    std::atomic<int64_t> num_executed_tasks;
    /// Number of submitted direct actor calls waiting to be posted to the IO thread.
    uint64_t submitted_direct_actor_calls_posting = 0;
    /// Number of received direct actor calls waiting to be posted to the worker execution
    /// thread.
    uint64_t received_direct_actor_calls_posting = 0;

    /// Number of tasks submitted per second by this worker for each function.
    /// This maps from function descriptor to number of tasks submitted.
    std::unordered_map<size_t, std::pair<std::string, uint64_t>> tasks_submitted_per_func;
    /// Number of tasks received per second by this worker for each function.
    /// This maps from function descriptor to number of tasks received.
    std::unordered_map<size_t, std::pair<std::string, uint64_t>> tasks_received_per_func;

    /// Number of times back pressure occurs.
    /// NOTE: Continuous submitted failures are treated as the same backpressure.
    uint64_t back_pressure_count = 0;

   private:
    std::string BuildFunctionTagString(std::string descriptor_string);
  };

  struct MemoryStoreStatistics {
    /// Protects the fields in this struct. This is to reduce the scope
    /// of the mutex in `CoreWorkerStats`.
    std::mutex mutex_;
    /// Number of objects in memory store.
    uint64_t num_objects_in_memory_store = 0;
    /// Size of total objects in memory store.
    uint64_t total_object_size_in_memory_store = 0;

    void AddMemoryStoreObject(uint64_t object_size) {
      std::unique_lock<std::mutex> lock(mutex_);
      num_objects_in_memory_store++;
      total_object_size_in_memory_store += object_size;
    }

    void RemoveMemoryStoreObject(uint64_t object_size) {
      std::unique_lock<std::mutex> lock(mutex_);
      num_objects_in_memory_store--;
      total_object_size_in_memory_store -= object_size;
    }

    void UpdateMemoryStoreHitRate(uint64_t in_memory_store, uint64_t total_count) {
      total_hit_memory_store += in_memory_store;
      total_get_object_count += total_count;
      if (total_get_object_count == 0) {
        memory_store_hit_rate = 0;
      } else {
        memory_store_hit_rate = (double)total_hit_memory_store / total_get_object_count;
      }
    }

    void UpdateLocalObjectHitRate(uint64_t local, uint64_t total_count) {
      total_fetched_locally_count += local;
      total_fetched_count += total_count;
      if (total_fetched_count == 0) {
        local_object_hit_rate = 0;
      } else {
        local_object_hit_rate = (double)total_fetched_locally_count / total_fetched_count;
      }
    }

    uint64_t total_hit_memory_store = 0;
    uint64_t total_get_object_count = 0;
    double memory_store_hit_rate = 0.0;
    /// TODO: maybe move to `ObjectStoreStatistics`
    uint64_t total_fetched_locally_count = 0;
    uint64_t total_fetched_count = 0;
    double local_object_hit_rate = 0.0;
  };

  struct RunningTaskStatistics {
    struct RunningTaskContext {
      explicit RunningTaskContext(const TaskSpecification &task_spec)
          : start_time_(time(nullptr)),
            last_log_print_time_(start_time_),
            last_event_send_time_(start_time_) {
        task_id_ = task_spec.TaskId();
        if (task_spec.IsActorCreationTask()) {
          actor_id_ = task_spec.ActorCreationId();
        } else if (task_spec.IsActorTask()) {
          actor_id_ = task_spec.ActorId();
        }
        function_descriptor_string_ = task_spec.FunctionDescriptor()->ToString();
        is_actor_creation_task_ = task_spec.IsActorCreationTask();
      }
      TaskID task_id_;
      ActorID actor_id_;
      std::string function_descriptor_string_;
      bool is_actor_creation_task_;
      time_t start_time_;
      time_t last_log_print_time_;
      time_t last_event_send_time_;
    };
    /// Protects the fields in this struct. This is to reduce the scope
    /// of the mutex in `CoreWorkerStats`.
    std::mutex mutex_;
    std::unordered_map<TaskID, RunningTaskContext> running_task_contexts_;

    void TaskStarted(const TaskSpecification &task_spec);
    void TaskFinished(const TaskSpecification &task_spec);
  };

  std::shared_ptr<CoreWorkerStats::TaskStatistics> AddTaskStats(
      const WorkerID &worker_id);
  void RemoveTaskStats(const WorkerID &worker_id);
  std::shared_ptr<CoreWorkerStats::RunningTaskStatistics> GetRunningTaskStats();

  std::shared_ptr<CoreWorkerStats::MemoryStoreStatistics> AddMemoryStoreStats(
      const WorkerID &worker_id);
  void RemoveMemoryStoreStats(const WorkerID &worker_id);

 private:
  void StartIOService();
  void UpdateMetrics();

  /// event loop where the timer is executed to report metrics.
  /// This is necessary since currently each worker has its own IO thread,
  /// thus a separate thread for the worker process is required to execute the timer.
  boost::asio::io_service io_service_;

  // keeps io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// The thread to report metrics.
  std::thread io_thread_;

  /// Interval for reporting metrics.
  boost::posix_time::milliseconds metrics_timer_interval_;

  // Timer for reporting metrics.
  std::unique_ptr<boost::asio::deadline_timer> metrics_timer_;

  /// Callback function to be invoked in timer to report metrics.
  std::function<void(const boost::system::error_code &error)> metrics_timer_func_;

  /// Protects the fields below.
  std::mutex task_stats_mutex_;

  std::mutex memory_store_stats_mutex_;

  std::unordered_map<WorkerID, std::shared_ptr<TaskStatistics>> task_stats_;
  std::shared_ptr<RunningTaskStatistics> running_task_stats_;

  std::unordered_map<WorkerID, std::shared_ptr<MemoryStoreStatistics>>
      memory_store_stats_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_STATS_H
