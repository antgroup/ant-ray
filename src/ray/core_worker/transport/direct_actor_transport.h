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

#include <boost/asio/thread_pool.hpp>
#include <boost/thread.hpp>
#include <list>
#include <queue>
#include <set>
#include <utility>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/id.h"
#include "ray/common/ray_object.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/fiber.h"
#include "ray/core_worker/store_provider/memory_store/memory_store.h"
#include "ray/core_worker/task_manager.h"
#include "ray/core_worker/transport/dependency_resolver.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/worker/core_worker_client.h"

namespace {}  // namespace

namespace ray {

/// The max time to wait for out-of-order tasks.
const int kMaxReorderWaitSeconds = 30;

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

// Interface for testing.
class CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  virtual void AddActorQueueIfNotExists(const ActorID &actor_id) = 0;
  virtual void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                            int64_t num_restarts) = 0;
  virtual void DisconnectActor(const ActorID &actor_id, int64_t num_restarts, bool dead,
                               const rpc::ActorDeathCause &death_cause) = 0;
  virtual void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) = 0;

  virtual void CheckTimeoutTasks() = 0;

  virtual void Shutdown() = 0;

  virtual ~CoreWorkerDirectActorTaskSubmitterInterface() {}
};

// This class is thread-safe.
class CoreWorkerDirectActorTaskSubmitter
    : public CoreWorkerDirectActorTaskSubmitterInterface {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool,
      std::shared_ptr<CoreWorkerMemoryStore> store,
      std::shared_ptr<TaskFinisherInterface> task_finisher, WorkerContext &worker_context,
      bool back_pressure_enabled, int32_t max_pending_calls = 1024)
      : core_worker_client_pool_(core_worker_client_pool),
        resolver_(store, task_finisher),
        task_finisher_(task_finisher),
        worker_context_(worker_context),
        actor_task_io_work_(actor_task_io_service_),
        actor_task_back_pressure_enabled_(back_pressure_enabled),
        max_pending_calls_(max_pending_calls),
        closed_(false) {
    actor_task_io_thread_ =
        std::thread(&CoreWorkerDirectActorTaskSubmitter::RunActorTaskIOService, this);
  }

  ~CoreWorkerDirectActorTaskSubmitter() {
    absl::MutexLock lock(&mu_);
    for (auto &queue : client_queues_) {
      if (queue.second->rpc_client) {
        queue.second->rpc_client->Close();
      }
    }
  }

  /// Add an actor queue. This should be called whenever a reference to an
  /// actor is created in the language frontend.
  /// TODO(swang): Remove the actor queue once it is sure that this worker will
  /// not receive another reference to the same actor.
  ///
  /// \param[in] actor_id The actor for whom to add a queue.
  void AddActorQueueIfNotExists(const ActorID &actor_id);

  /// Push a task to corresponding client queue by the actor id.
  ///
  /// \param[in] task The task spec to push.
  /// \param[in] callback  The callback function to be called after the task spec be sent
  /// out. Only used in test code. \return void.
  void AppendTask(const TaskSpecification &task_spec,
                  const std::function<void(const ActorID &)> &callback = nullptr);

  /// Tell this actor to exit immediately.
  ///
  /// \param[in] actor_id The actor_id of the actor to kill.
  /// \param[in] force_kill Whether to force kill the actor, or let the actor
  /// try a clean exit.
  /// \param[in] no_restart If set to true, the killed actor will not be
  /// restarted anymore.
  void KillActor(const ActorID &actor_id, bool force_kill, bool no_restart);

  /// Create connection to actor and send all pending tasks.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] address The new address of the actor.
  /// \param[in] num_restarts How many times this actor has been restarted
  /// before. If we've already seen a later incarnation of the actor, we will
  /// ignore the command to connect.
  void ConnectActor(const ActorID &actor_id, const rpc::Address &address,
                    int64_t num_restarts);

  /// Disconnect from a failed actor.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] num_restarts How many times this actor has been restarted
  /// before. If we've already seen a later incarnation of the actor, we will
  /// ignore the command to connect.
  /// \param[in] dead Whether the actor is permanently dead. In this case, all
  /// pending tasks for the actor should be failed.
  /// \param[in] death_cause Context about why this actor is dead.
  void DisconnectActor(const ActorID &actor_id, int64_t num_restarts, bool dead,
                       const rpc::ActorDeathCause &death_cause);

  /// Set the timerstamp for the caller.
  void SetCallerCreationTimestamp(int64_t timestamp);

  /// Check timeout tasks that are waiting for Death info.
  void CheckTimeoutTasks();

  /// Subscribe the actor reconnected event.
  ///
  /// \param[in] callback The callback to be called when the actor is reconnected except
  /// the actor was reconstructed. \return Void
  void SubscribeActorReconnected(const std::function<void(const ActorID &)> &callback);

  /// Block the actor_task_io_thread_ until the submitter is shutdown.
  void WaitForShutdown();

  /// Close rpc client when destroyed.
  void Shutdown();

  /// If the the number of tasks in requests is greater than or equal to
  /// max_pending_calls.
  /// \param[in] actor_id Actor id.
  /// \return Whether the corresponding client queue is full or not.
  bool IncPendingTaskNumsIfNotFull(const ActorID &actor_id);

 private:
  struct ClientQueue {
    /// The current state of the actor. If this is ALIVE, then we should have
    /// an RPC client to the actor. If this is DEAD, then all tasks in the
    /// queue will be marked failed and all other ClientQueue state is ignored.
    rpc::ActorTableData::ActorState state = rpc::ActorTableData::DEPENDENCIES_UNREADY;
    /// The reason why this actor is dead.
    /// If the context is not set, it means the actor is not dead.
    rpc::ActorDeathCause death_cause;
    /// How many times this actor has been restarted before. Starts at -1 to
    /// indicate that the actor is not yet created. This is used to drop stale
    /// messages from the GCS.
    int64_t num_restarts = -1;
    /// The RPC client. We use shared_ptr to enable shared_from_this for
    /// pending client callbacks.
    std::shared_ptr<rpc::CoreWorkerClientInterface> rpc_client = nullptr;
    /// The intended worker ID of the actor.
    std::string worker_id = "";

    /// Mutex to protect the TaskSpecification in requests.
    mutable absl::Mutex mu_;
    /// The actor's pending requests, ordered by the task number (see below
    /// diagram) in the request. The bool indicates whether the dependencies
    /// for that task have been resolved yet. A task will be sent after its
    /// dependencies have been resolved and its task number matches
    /// next_send_position.
    std::map<uint64_t, std::pair<TaskSpecification, bool>> requests;

    /// The max number limit of task capacity used for backpressure.
    /// If the number of tasks in requests >= max_pending_calls, it can't continue to
    /// push task to ClientQueue.
    int32_t max_pending_calls;

    /// The current task number in this client queue.
    int32_t current_pending_calls = 0;

    /// Diagram of the sequence numbers assigned to actor tasks during actor
    /// crash and restart:
    ///
    /// The actor starts, and 10 tasks are submitted. We have sent 6 tasks
    /// (0-5) so far, and have received a successful reply for 4 tasks (0-3).
    /// 0 1 2 3 4 5 6 7 8 9
    ///             ^ next_send_position
    ///         ^ next_task_reply_position
    /// ^ caller_starts_at
    ///
    /// Suppose the actor crashes and recovers. Then, caller_starts_at is reset
    /// to the current next_task_reply_position. caller_starts_at is then subtracted
    /// from each task's counter, so the recovered actor will receive the
    /// sequence numbers 0, 1, 2 (and so on) for tasks 4, 5, 6, respectively.
    /// Therefore, the recovered actor will restart execution from task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///             ^ next_send_position
    ///         ^ next_task_reply_position
    ///         ^ caller_starts_at
    ///
    /// New actor tasks will continue to be sent even while tasks are being
    /// resubmitted, but the receiving worker will only execute them after the
    /// resent tasks. For example, this diagram shows what happens if task 6 is
    /// sent for the first time, tasks 4 and 5 have been resent, and we have
    /// received a successful reply for task 4.
    /// 0 1 2 3 4 5 6 7 8 9
    ///               ^ next_send_position
    ///           ^ next_task_reply_position
    ///         ^ caller_starts_at
    ///
    /// The send position of the next task to send to this actor. This sequence
    /// number increases monotonically.
    uint64_t next_send_position = 0;
    /// The offset at which the the actor should start its counter for this
    /// caller. This is used for actors that can be restarted, so that the new
    /// instance of the actor knows from which task to start executing.
    uint64_t caller_starts_at = 0;
    /// Out of the tasks sent by this worker to the actor, the number of tasks
    /// that we will never send to the actor again. This is used to reset
    /// caller_starts_at if the actor dies and is restarted. We only include
    /// tasks that will not be sent again, to support automatic task retry on
    /// actor failure. This value only tracks consecutive tasks that are completed.
    /// Tasks completed out of order will be cached in out_of_completed_tasks first.
    uint64_t next_task_reply_position = 0;

    /// The temporary container for tasks completed out of order. It can happen in
    /// async or threaded actor mode. This map is used to store the seqno and task
    /// spec for (1) increment next_task_reply_position later when the in order tasks are
    /// returned (2) resend the tasks to restarted actor so retried tasks can maintain
    /// ordering.
    // NOTE(simon): consider absl::btree_set for performance, but it requires updating
    // abseil.
    std::map<uint64_t, std::shared_ptr<TaskSpecification>> out_of_order_completed_tasks;

    /// Tasks that can't be sent because 1) the callee actor is dead. 2) network error.
    /// For 1) the task will wait for the DEAD state notification, then mark task as
    /// failed using the death_info in notification. For 2) we'll never receive a DEAD
    /// notification, in this case we'll wait for a fixed timeout value and then mark it
    /// as failed.
    /// pair key: timestamp in ms when this task should be considered as timeout.
    /// pair value: task specification
    std::deque<std::pair<int64_t, std::shared_ptr<TaskSpecification>>>
        wait_for_death_info_tasks;

    /// A force-kill request that should be sent to the actor once an RPC
    /// client to the actor is available.
    absl::optional<rpc::KillActorRequest> pending_force_kill;

    // ======= ANT-INTERNAL =======

    /// Whether there's an connection error on this actor.
    bool has_conn_error = false;

    /// The task ID of the last not-completed has-reply task.
    TaskID last_not_completed_has_reply_task_id;

    /// Stores the subsequent no-reply task count of every has-reply task. It's used to
    /// increment `next_task_reply_position` when the has-reply task is completed.
    std::unordered_map<TaskID, uint64_t> no_reply_task_count_after_a_has_reply_task;

    /// Stores all callbacks of inflight tasks. Note that this doesn't include tasks
    /// without replies.
    std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
        inflight_task_callbacks;
  };

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] queue Which ClientQueue the task is in.
  /// \param[in] task_spec The task spec to submit.
  ///
  /// \return Status::Invalid if the task is not yet supported.
  Status SubmitTask(ClientQueue &queue, TaskSpecification task_spec);

  /// Push a task to a remote actor via the given client.
  /// Note, this function doesn't return any error status code. If an error occurs while
  /// sending the request, this task will be treated as failed.
  ///
  /// \param[in] queue The actor queue. Contains the RPC client state.
  /// \param[in] task_spec The task to send.
  /// \param[in] skip_queue Whether to skip the task queue. This will send the
  /// task for execution immediately.
  /// \return Void.
  void PushActorTask(ClientQueue &queue, std::shared_ptr<TaskSpecification> &task_spec,
                     bool skip_queue);

  /// Send all pending tasks for an actor.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] queue The actor queue. Contains the RPC client state.
  /// \return void.
  void SendPendingTasks(const ActorID &actor_id, ClientQueue &queue);

  /// Resend all previously-received, out-of-order, received tasks for an actor.
  /// When sending these tasks, the tasks will have the flag skip_execution=true.
  ///
  /// \param[in] actor_id Actor ID.
  /// \param[in] queue The actor queue. Contains the RPC client state.
  /// \return Void.
  void ResendOutOfOrderTasks(const ActorID &actor_id, ClientQueue &queue);

  /// Disconnect the RPC client for an actor.
  void DisconnectRpcClient(ClientQueue &queue);

  /// Fail all in-flight tasks.
  void FailInflightTasks(
      const std::unordered_map<TaskID, rpc::ClientCallback<rpc::PushTaskReply>>
          &inflight_task_callbacks) LOCKS_EXCLUDED(mu_);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id) const;

  /// Callback function to handle task replies.
  /// NOTE: (wanxing.wwx) This member function used to be a lambda function,
  /// we put the codes in an independent function to reuse when task fast fail enabled.
  void ReplyCallback(std::shared_ptr<TaskSpecification> task_spec,
                     bool increment_completed_tasks, Status status,
                     const rpc::PushTaskReply &reply);

  /// Callback function to handle task finish.
  /// NOTE: (wanxing.wwx) This member function used to be a lambda function,
  /// we put the codes in an independent function to reuse when task fast fail enabled.
  bool TaskFinishCallback(std::shared_ptr<TaskSpecification> task_spec,
                          const rpc::Address &addr, Status status,
                          const rpc::PushTaskReply &reply);

  /// Run the actor_task_io_service_ event loop.
  void RunActorTaskIOService();

  bool TaskFinishErrorCallback(ClientQueue &queue,
                               std::shared_ptr<TaskSpecification> task_spec,
                               const rpc::Address &addr, Status status,
                               const rpc::PushTaskReply &reply);

  /// Pool for producing new core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> core_worker_client_pool_;

  /// Mutex to protect the various maps below.
  mutable absl::Mutex mu_;

  absl::flat_hash_map<ActorID, std::shared_ptr<ClientQueue>> client_queues_
      GUARDED_BY(mu_);

  /// The callback to be called when the actor is reconnected except the actor was
  /// reconstructed.
  std::function<void(const ActorID &)> actor_reconnected_callback_;

  /// Resolve direct call object dependencies.
  LocalDependencyResolver resolver_;

  /// Used to complete tasks.
  std::shared_ptr<TaskFinisherInterface> task_finisher_;

  friend class CoreWorkerTest;

  WorkerContext &worker_context_;

  /// The event loop where the actor task events are handled.
  instrumented_io_context actor_task_io_service_;

  /// Keeps the actor_task_io_service_ alive.
  boost::asio::io_service::work actor_task_io_work_;

  /// The thread that runs actor_task_io_service_;
  std::thread actor_task_io_thread_;

  bool actor_task_back_pressure_enabled_;

  int32_t max_pending_calls_;
  /* Below fields are only available for ant-version ray. */
  bool closed_;
};

class BoundedExecutor;

/// A manager that manages a set of thread pool. which will perform
/// the methods defined in one concurrent group.
class PoolManager final {
 public:
  explicit PoolManager(const std::vector<ConcurrentGroup> &concurrent_groups = {},
                       const int32_t default_group_max_concurrency = 1) {
    for (auto &group : concurrent_groups) {
      const auto name = group.name;
      const auto max_concurrency = group.max_concurrency;
      auto pool = std::make_shared<BoundedExecutor>(max_concurrency);
      auto &fds = group.function_descriptors;
      for (auto fd : fds) {
        functions_to_thread_pool_index_[fd->ToString()] = pool;
      }
    }

    // If max concurrency of default group is 1, the tasks of default group
    // will be performed in main thread instead of any executor pool.
    if (default_group_max_concurrency > 1 || !concurrent_groups.empty()) {
      /// The concurrency group is enabled.
      default_thread_pool_ =
          std::make_shared<BoundedExecutor>(default_group_max_concurrency);
    }
  }

  std::shared_ptr<BoundedExecutor> GetPool(ray::FunctionDescriptor fd) {
    if (functions_to_thread_pool_index_.find(fd->ToString()) !=
        functions_to_thread_pool_index_.end()) {
      return functions_to_thread_pool_index_[fd->ToString()];
    }
    return default_thread_pool_;
  }

 private:
  // Map from the FunctionDescriptors to their corresponding thread pools.
  std::unordered_map<std::string, std::shared_ptr<BoundedExecutor>>
      functions_to_thread_pool_index_;

  // The thread pool for default concurrent group. It's nullptr if its max concurrency
  // is 1.
  std::shared_ptr<BoundedExecutor> default_thread_pool_ = nullptr;
};

/// Object dependency and RPC state of an inbound request.
class InboundRequest {
 public:
  InboundRequest(){};
  InboundRequest(std::function<void(rpc::SendReplyCallback)> accept_callback,
                 std::function<void(rpc::SendReplyCallback)> reject_callback,
                 rpc::SendReplyCallback send_reply_callback, TaskID task_id,
                 bool has_dependencies,
                 const ray::FunctionDescriptor &function_descriptor)
      : accept_callback_(std::move(accept_callback)),
        reject_callback_(std::move(reject_callback)),
        send_reply_callback_(std::move(send_reply_callback)),
        task_id_(task_id),
        function_descriptor_(function_descriptor),
        has_pending_dependencies_(has_dependencies) {}

  void Accept() { accept_callback_(std::move(send_reply_callback_)); }
  void Cancel() { reject_callback_(std::move(send_reply_callback_)); }
  bool CanExecute() const { return !has_pending_dependencies_; }
  ray::TaskID TaskID() const { return task_id_; }
  const ray::FunctionDescriptor &FunctionDescriptor() const {
    return function_descriptor_;
  }
  void MarkDependenciesSatisfied() { has_pending_dependencies_ = false; }

 private:
  std::function<void(rpc::SendReplyCallback)> accept_callback_;
  std::function<void(rpc::SendReplyCallback)> reject_callback_;
  rpc::SendReplyCallback send_reply_callback_;
  ray::TaskID task_id_;
  ray::FunctionDescriptor function_descriptor_;
  bool has_pending_dependencies_;
};

/// Waits for an object dependency to become available. Abstract for testing.
class DependencyWaiter {
 public:
  /// Calls `callback` once the specified objects become available.
  virtual void Wait(const std::vector<rpc::ObjectReference> &dependencies,
                    std::function<void()> on_dependencies_available) = 0;

  virtual ~DependencyWaiter(){};
};

class DependencyWaiterImpl : public DependencyWaiter {
 public:
  DependencyWaiterImpl(DependencyWaiterInterface &dependency_client)
      : dependency_client_(dependency_client) {}

  void Wait(const std::vector<rpc::ObjectReference> &dependencies,
            std::function<void()> on_dependencies_available) override {
    auto tag = next_request_id_++;
    requests_[tag] = on_dependencies_available;
    RAY_CHECK_OK(dependency_client_.WaitForDirectActorCallArgs(dependencies, tag));
  }

  /// Fulfills the callback stored by Wait().
  void OnWaitComplete(int64_t tag) {
    auto it = requests_.find(tag);
    RAY_CHECK(it != requests_.end());
    it->second();
    requests_.erase(it);
  }

 private:
  int64_t next_request_id_ = 0;
  std::unordered_map<int64_t, std::function<void()>> requests_;
  DependencyWaiterInterface &dependency_client_;
};

/// Wraps a thread-pool to block posts until the pool has free slots. This is used
/// by the SchedulingQueue to provide backpressure to clients.
class BoundedExecutor {
 public:
  BoundedExecutor(int max_concurrency) : pool_(max_concurrency){};

  /// Posts work to the pool
  void Post(std::function<void()> fn) { boost::asio::post(pool_, std::move(fn)); }

 private:
  /// The underlying thread pool for running tasks.
  boost::asio::thread_pool pool_;
};

/// Used to implement task queueing at the worker. Abstraction to provide a common
/// interface for actor tasks as well as normal ones.
class SchedulingQueue {
 public:
  virtual void Add(int64_t seq_no, int64_t client_processed_up_to,
                   std::function<void(rpc::SendReplyCallback)> accept_request,
                   std::function<void(rpc::SendReplyCallback)> reject_request,
                   const ray::FunctionDescriptor &function_descriptor,
                   rpc::SendReplyCallback send_reply_callback = nullptr,
                   TaskID task_id = TaskID::Nil(),
                   const std::vector<rpc::ObjectReference> &dependencies = {}) = 0;
  virtual void ScheduleRequests() = 0;
  virtual bool TaskQueueEmpty() const = 0;
  virtual bool CancelTaskIfFound(TaskID task_id) = 0;
  virtual ~SchedulingQueue(){};
};

/// Map task_group_name -> scheduling_queue
using SchedulingQueueGroup =
    std::unordered_map<std::string, std::unique_ptr<SchedulingQueue>>;

/// Used to ensure serial order of task execution per actor handle.
/// See direct_actor.proto for a description of the ordering protocol.
class ActorSchedulingQueue : public SchedulingQueue {
 public:
  ActorSchedulingQueue(
      instrumented_io_context &main_io_service, DependencyWaiter &waiter,
      std::shared_ptr<PoolManager> pool_manager = std::make_shared<PoolManager>(),
      bool is_asyncio = false, int max_concurrency = 1,
      int64_t reorder_wait_seconds = kMaxReorderWaitSeconds)
      : reorder_wait_seconds_(reorder_wait_seconds),
        wait_timer_(main_io_service),
        main_thread_id_(boost::this_thread::get_id()),
        waiter_(waiter),
        pool_manager_(pool_manager),
        is_asyncio_(is_asyncio) {
    if (is_asyncio_) {
      RAY_LOG(INFO) << "Setting actor as async with max_concurrency=" << max_concurrency
                    << ", creating new fiber thread.";
      fiber_state_ = std::make_unique<FiberState>(max_concurrency);
    }
  }

  bool TaskQueueEmpty() const { return pending_actor_tasks_.empty(); }

  /// Add a new actor task's callbacks to the worker queue.
  void Add(int64_t seq_no, int64_t client_processed_up_to,
           std::function<void(rpc::SendReplyCallback)> accept_request,
           std::function<void(rpc::SendReplyCallback)> reject_request,
           const ray::FunctionDescriptor &function_descriptor,
           rpc::SendReplyCallback send_reply_callback = nullptr,
           TaskID task_id = TaskID::Nil(),
           const std::vector<rpc::ObjectReference> &dependencies = {}) {
    // A seq_no of -1 means no ordering constraint. Actor tasks must be executed in order.
    RAY_CHECK(seq_no != -1);

    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
    if (client_processed_up_to >= next_seq_no_) {
      RAY_LOG(ERROR) << "client skipping requests " << next_seq_no_ << " to "
                     << client_processed_up_to;
      next_seq_no_ = client_processed_up_to + 1;
    }
    RAY_LOG(DEBUG) << "Enqueue " << seq_no << " cur seqno " << next_seq_no_;
    pending_actor_tasks_[seq_no] =
        InboundRequest(std::move(accept_request), std::move(reject_request),
                       std::move(send_reply_callback), task_id, dependencies.size() > 0,
                       function_descriptor);
    if (dependencies.size() > 0) {
      waiter_.Wait(dependencies, [seq_no, this]() {
        RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);
        auto it = pending_actor_tasks_.find(seq_no);
        if (it != pending_actor_tasks_.end()) {
          it->second.MarkDependenciesSatisfied();
          ScheduleRequests();
        }
      });
    }
    ScheduleRequests();
  }

  // We don't allow the cancellation of actor tasks, so invoking CancelTaskIfFound results
  // in a fatal error.
  bool CancelTaskIfFound(TaskID task_id) {
    RAY_CHECK(false) << "Cannot cancel actor tasks";
    // The return instruction will never be executed, but we need to include it
    // nonetheless because this is a non-void function.
    return false;
  }

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() {
    // Cancel any stale requests that the client doesn't need any longer.
    while (!pending_actor_tasks_.empty() &&
           pending_actor_tasks_.begin()->first < next_seq_no_) {
      auto head = pending_actor_tasks_.begin();
      RAY_LOG(ERROR) << "Cancelling stale RPC with seqno "
                     << pending_actor_tasks_.begin()->first << " < " << next_seq_no_;
      head->second.Cancel();
      pending_actor_tasks_.erase(head);
    }

    // Process as many in-order requests as we can.
    while (!pending_actor_tasks_.empty() &&
           (pending_actor_tasks_.begin()->first == next_seq_no_ ||
            !RayConfig::instance().task_reorder()) &&
           pending_actor_tasks_.begin()->second.CanExecute()) {
      auto head = pending_actor_tasks_.begin();
      auto &request = head->second;

      if (is_asyncio_) {
        // Process async actor task.
        fiber_state_->EnqueueFiber([request]() mutable { request.Accept(); });
      } else {
        // Process actor tasks.
        RAY_CHECK(pool_manager_ != nullptr);
        auto pool = pool_manager_->GetPool(request.FunctionDescriptor());
        if (pool == nullptr) {
          request.Accept();
        } else {
          pool->Post([request]() mutable { request.Accept(); });
        }
      }
      pending_actor_tasks_.erase(head);
      next_seq_no_++;
    }

    if (pending_actor_tasks_.empty() ||
        !pending_actor_tasks_.begin()->second.CanExecute()) {
      // No timeout for object dependency waits.
      wait_timer_.cancel();
    } else {
      // Set a timeout on the queued tasks to avoid an infinite wait on failure.
      wait_timer_.expires_from_now(boost::posix_time::seconds(reorder_wait_seconds_));
      RAY_LOG(DEBUG) << "waiting for " << next_seq_no_ << " queue size "
                     << pending_actor_tasks_.size();
      wait_timer_.async_wait([this](const boost::system::error_code &error) {
        if (error == boost::asio::error::operation_aborted) {
          return;  // time deadline was adjusted
        }
        OnSequencingWaitTimeout();
      });
    }
  }

 private:
  /// Called when we time out waiting for an earlier task to show up.
  void OnSequencingWaitTimeout() {
    RAY_CHECK(boost::this_thread::get_id() == main_thread_id_);

    // ANT-INTERNAL: In order to support RPC reconnection, we cannot cancel queued tasks
    // on timeout. That will lose tasks sent after reconnection. Instead, we update
    // `next_seq_no_` to the seq no of the first task in queue, so we can continue
    // executing queued tasks.
    if (!pending_actor_tasks_.empty()) {
      auto head = pending_actor_tasks_.begin();
      int64_t new_next_seq_no = head->first;
      RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                     << ", resetting `next_seq_no_` to " << new_next_seq_no;
      next_seq_no_ = new_next_seq_no;
      ScheduleRequests();
    }
    // Skip the community logic.
    return;

    RAY_LOG(ERROR) << "timed out waiting for " << next_seq_no_
                   << ", cancelling all queued tasks";
    while (!pending_actor_tasks_.empty()) {
      auto head = pending_actor_tasks_.begin();
      head->second.Cancel();
      next_seq_no_ = std::max(next_seq_no_, head->first + 1);
      pending_actor_tasks_.erase(head);
    }
  }

  /// Max time in seconds to wait for dependencies to show up.
  const int64_t reorder_wait_seconds_ = 0;
  /// Sorted map of (accept, rej) task callbacks keyed by their sequence number.
  std::map<int64_t, InboundRequest> pending_actor_tasks_;
  /// The next sequence number we are waiting for to arrive.
  int64_t next_seq_no_ = 0;
  /// Timer for waiting on dependencies. Note that this is set on the task main
  /// io service, which is fine since it only ever fires if no tasks are running.
  boost::asio::deadline_timer wait_timer_;
  /// The id of the thread that constructed this scheduling queue.
  boost::thread::id main_thread_id_;
  /// Reference to the waiter owned by the task receiver.
  DependencyWaiter &waiter_;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<PoolManager> pool_manager_;
  /// Whether we should enqueue requests into asyncio pool. Setting this to true
  /// will instantiate all tasks as fibers that can be yielded.
  bool is_asyncio_ = false;
  /// If is_asyncio_ is true, fiber_state_ contains the running state required
  /// to enable continuation and work together with python asyncio.
  std::shared_ptr<FiberState> fiber_state_;
  friend class SchedulingQueueTest;
};

/// Used to implement the non-actor task queue. These tasks do not have ordering
/// constraints.
class NormalSchedulingQueue : public SchedulingQueue {
 public:
  NormalSchedulingQueue(){};

  bool TaskQueueEmpty() const {
    absl::MutexLock lock(&mu_);
    return pending_normal_tasks_.empty();
  }

  /// Add a new task's callbacks to the worker queue.
  void Add(
      int64_t seq_no, int64_t client_processed_up_to,
      std::function<void(rpc::SendReplyCallback)> accept_request,
      std::function<void(rpc::SendReplyCallback)> reject_request,
      const FunctionDescriptor &function_descriptor = FunctionDescriptorBuilder::Empty(),
      rpc::SendReplyCallback send_reply_callback = nullptr,
      TaskID task_id = TaskID::Nil(),
      const std::vector<rpc::ObjectReference> &dependencies = {}) {
    absl::MutexLock lock(&mu_);
    // Normal tasks should not have ordering constraints.
    RAY_CHECK(seq_no == -1);
    // Create a InboundRequest object for the new task, and add it to the queue.
    pending_normal_tasks_.push_back(
        InboundRequest(std::move(accept_request), std::move(reject_request),
                       std::move(send_reply_callback), task_id, dependencies.size() > 0,
                       function_descriptor));
  }

  // Search for an InboundRequest associated with the task that we are trying to cancel.
  // If found, remove the InboundRequest from the queue and return true. Otherwise, return
  // false.
  bool CancelTaskIfFound(TaskID task_id) {
    absl::MutexLock lock(&mu_);
    for (std::deque<InboundRequest>::reverse_iterator it = pending_normal_tasks_.rbegin();
         it != pending_normal_tasks_.rend(); ++it) {
      if (it->TaskID() == task_id) {
        pending_normal_tasks_.erase(std::next(it).base());
        return true;
      }
    }
    return false;
  }

  /// Schedules as many requests as possible in sequence.
  void ScheduleRequests() {
    while (true) {
      InboundRequest head;
      {
        absl::MutexLock lock(&mu_);
        if (!pending_normal_tasks_.empty()) {
          head = pending_normal_tasks_.front();
          pending_normal_tasks_.pop_front();
        } else {
          return;
        }
      }
      head.Accept();
    }
  }

 private:
  /// Protects access to the dequeue below.
  mutable absl::Mutex mu_;
  /// Queue with (accept, rej) callbacks for non-actor tasks
  std::deque<InboundRequest> pending_normal_tasks_ GUARDED_BY(mu_);
  friend class SchedulingQueueTest;
};

class CoreWorkerDirectTaskReceiver {
 public:
  using TaskHandler =
      std::function<Status(const TaskSpecification &task_spec,
                           const std::shared_ptr<ResourceMappingType> resource_ids,
                           std::vector<std::shared_ptr<RayObject>> *return_objects,
                           ReferenceCounter::ReferenceTableProto *borrower_refs,
                           bool *is_application_level_error)>;

  using OnTaskDone = std::function<ray::Status()>;

  CoreWorkerDirectTaskReceiver(WorkerContext &worker_context,
                               instrumented_io_context &main_io_service,
                               const TaskHandler &task_handler,
                               const OnTaskDone &task_done)
      : worker_context_(worker_context),
        task_handler_(task_handler),
        task_main_io_service_(main_io_service),
        task_done_(task_done),
        pool_manager_(std::make_shared<PoolManager>()) {}

  /// Initialize this receiver. This must be called prior to use.
  void Init(std::shared_ptr<rpc::CoreWorkerClientPool>, rpc::Address rpc_address,
            std::shared_ptr<DependencyWaiter> dependency_waiter,
            boost::asio::io_service &task_execution_service);

  /// Handle a `PushTask` request. If it's an actor request, this function will enqueue
  /// the task and then start scheduling the requests to begin the execution. If it's a
  /// non-actor request, this function will just enqueue the task.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleTask(const rpc::PushTaskRequest &request, rpc::PushTaskReply *reply,
                  rpc::SendReplyCallback send_reply_callback);

  /// Pop tasks from the queue and execute them sequentially
  void RunNormalTasksFromQueue();

  // === ANT-INTERNAL below ===

  bool CancelQueuedNormalTask(TaskID task_id);

 private:
  // Worker context.
  WorkerContext &worker_context_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The IO event loop for running tasks on.
  instrumented_io_context &task_main_io_service_;
  /// The callback function to be invoked when finishing a task.
  OnTaskDone task_done_;
  /// Shared pool for producing new core worker clients.
  std::shared_ptr<rpc::CoreWorkerClientPool> client_pool_;
  /// Address of our RPC server.
  rpc::Address rpc_address_;
  /// Shared waiter for dependencies required by incoming tasks.
  std::shared_ptr<DependencyWaiter> waiter_;
  /// Queue of pending requests per actor handle.
  /// TODO(ekl) GC these queues once the handle is no longer active.
  std::unordered_map<WorkerID, std::unique_ptr<SchedulingQueue>> actor_scheduling_queues_;

  // Queue of pending normal (non-actor) tasks.
  std::unique_ptr<SchedulingQueue> normal_scheduling_queue_ =
      std::unique_ptr<SchedulingQueue>(new NormalSchedulingQueue());
  /// The max number of concurrent calls to allow.
  /// 0 indicates that the value is not set yet.
  int max_concurrency_ = 0;
  /// If concurrent calls are allowed, holds the pools for executing these tasks.
  std::shared_ptr<PoolManager> pool_manager_;
  /// Whether this actor use asyncio for concurrency.
  bool is_asyncio_ = false;

  /// Set the max concurrency of an actor.
  /// This should be called once for the actor creation task.
  void SetMaxActorConcurrency(bool is_asyncio, int max_concurrency);
};

}  // namespace ray
