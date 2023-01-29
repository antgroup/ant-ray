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

#ifdef __clang__
// TODO(mehrdadn): Remove this when the warnings are addressed
#pragma clang diagnostic push
#pragma clang diagnostic warning "-Wunused-result"
#pragma clang diagnostic warning "-Wunused-lambda-capture"
#endif

#include <grpcpp/grpcpp.h>

#include <deque>
#include <memory>
#include <mutex>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/hash/hash.h"
#include "ray/common/status.h"
#include "ray/rpc/brpc/client/stream_client.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {
namespace rpc {

#define CORE_WORKER_RPC_METHODS                          \
  CORE_WORKER_RPC_METHOD(DirectActorCallArgWaitComplete) \
  CORE_WORKER_RPC_METHOD(GetObjectStatus)                \
  CORE_WORKER_RPC_METHOD(WaitForActorOutOfScope)         \
  CORE_WORKER_RPC_METHOD(KillActor)                      \
  CORE_WORKER_RPC_METHOD(CancelTask)                     \
  CORE_WORKER_RPC_METHOD(RemoteCancelTask)               \
  CORE_WORKER_RPC_METHOD(SubscribeForObjectEviction)     \
  CORE_WORKER_RPC_METHOD(PubsubLongPolling)              \
  CORE_WORKER_RPC_METHOD(AddObjectLocationOwner)         \
  CORE_WORKER_RPC_METHOD(RemoveObjectLocationOwner)      \
  CORE_WORKER_RPC_METHOD(GetObjectLocationsOwner)        \
  CORE_WORKER_RPC_METHOD(GetCoreWorkerStats)             \
  CORE_WORKER_RPC_METHOD(LocalGC)                        \
  CORE_WORKER_RPC_METHOD(WaitForRefRemoved)              \
  CORE_WORKER_RPC_METHOD(SpillObjects)                   \
  CORE_WORKER_RPC_METHOD(RestoreSpilledObjects)          \
  CORE_WORKER_RPC_METHOD(DumpObjects)                    \
  CORE_WORKER_RPC_METHOD(LoadDumpedObjects)              \
  CORE_WORKER_RPC_METHOD(DeleteSpilledObjects)           \
  CORE_WORKER_RPC_METHOD(AddSpilledUrl)                  \
  CORE_WORKER_RPC_METHOD(AddDumpedUrl)                   \
  CORE_WORKER_RPC_METHOD(RunOnUtilWorker)                \
  CORE_WORKER_RPC_METHOD(PlasmaObjectReady)              \
  CORE_WORKER_RPC_METHOD(Exit)                           \
  CORE_WORKER_RPC_METHOD(BatchAssignObjectOwner)

#if CORE_WORKER_USE_GRPC
#define CORE_WORKER_VOID_RPC_CLIENT_METHOD VOID_RPC_CLIENT_METHOD
#define INVOKE_RPC_CALL INVOKE_GRPC_CALL
#else
#define VOID_RPC_CLIENT_METHOD_PARTIAL(INTERFACE_METHOD, METHOD, rpc_client)         \
  std::shared_ptr<BrpcStreamClient> underlying_rpc_client = nullptr;                 \
  if (!is_pending_request) {                                                         \
    /*Lock here to keep exclusive with rpc disconnect, ensure the pending all        \
     the requests when reconnecting.*/                                               \
    absl::MutexLock connection_lock(&rpc_client->connection_mutex_);                 \
    absl::ReleasableMutexLock pending_request_lock(&pending_request_mutex_);         \
    underlying_rpc_client = rpc_client->GetRpcClient();                              \
    /* The underlying_rpc_client is null means the client is in initial state or     \
     during connecting or the connection has failed.                                 \
    */                                                                               \
    if (!underlying_rpc_client) {                                                    \
      if (rpc_client->IsClosed()) {                                                  \
        auto status = Status::IOError("The RPC client to " +                         \
                                      rpc_client->RemoteLocation() + " is closed."); \
        reply_service_.post([callback = std::move(callback), status]() {             \
          METHOD##Reply reply;                                                       \
          callback(status, reply);                                                   \
        });                                                                          \
      } else {                                                                       \
        pending_##INTERFACE_METHOD##_requests_.emplace_back(std::move(request),      \
                                                            std::move(callback));    \
      }                                                                              \
      return;                                                                        \
    }                                                                                \
    pending_request_lock.Release();                                                  \
    rpc_client->ok_to_send_requests_future_.wait();                                  \
  } else {                                                                           \
    underlying_rpc_client = rpc_client->GetRpcClient();                              \
    /*The underlying rpc client should not be nullptr when send pending requests.*/  \
    RAY_CHECK(underlying_rpc_client);                                                \
  }
#define CORE_WORKER_VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, rpc_client, SPECS)          \
  void METHOD(const METHOD##Request &request,                                           \
              const ClientCallback<METHOD##Reply> &callback, bool is_pending_request)   \
      SPECS                                                                             \
      LOCKS_EXCLUDED(pending_request_mutex_) {                                          \
    VOID_RPC_CLIENT_METHOD_PARTIAL(METHOD, METHOD, rpc_client);                         \
    (const_cast<METHOD##Request &>(request)).set_intended_worker_id(addr_.worker_id()); \
    /* TODO (kfstorm): Support worker task stats about asio queue length. */            \
    underlying_rpc_client                                                               \
        ->template CallMethod<METHOD##Request, METHOD##Reply, SERVICE##MessageType>(    \
            SERVICE##MessageType::METHOD##RequestMessage,                               \
            SERVICE##MessageType::METHOD##ReplyMessage, request,                        \
            const_cast<ClientCallback<METHOD##Reply> &>(callback), #METHOD);            \
  }
#endif

/// The base size in bytes per request.
const int64_t kBaseRequestSize = 1024;

/// Get the estimated size in bytes of the given task.
const static int64_t RequestSizeInBytes(const PushTaskRequest &request) {
  int64_t size = kBaseRequestSize;
  for (auto &arg : request.task_spec().args()) {
    size += arg.data().size();
  }
  return size;
}

// Shared between direct actor and task submitters.
class CoreWorkerClientInterface;

// TODO(swang): Remove and replace with rpc::Address.
class WorkerAddress {
 public:
  WorkerAddress(const rpc::Address &address)
      : ip_address(address.ip_address()),
        port(address.port()),
        worker_id(WorkerID::FromBinary(address.worker_id())),
        raylet_id(NodeID::FromBinary(address.raylet_id())) {}
  template <typename H>
  friend H AbslHashValue(H h, const WorkerAddress &w) {
    return H::combine(std::move(h), w.ip_address, w.port, w.worker_id, w.raylet_id);
  }

  bool operator==(const WorkerAddress &other) const {
    return other.ip_address == ip_address && other.port == port &&
           other.worker_id == worker_id && other.raylet_id == raylet_id;
  }

  rpc::Address ToProto() const {
    rpc::Address addr;
    addr.set_raylet_id(raylet_id.Binary());
    addr.set_ip_address(ip_address);
    addr.set_port(port);
    addr.set_worker_id(worker_id.Binary());
    return addr;
  }

  /// The ip address of the worker.
  const std::string ip_address;
  /// The local port of the worker.
  const int port;
  /// The unique id of the worker.
  const WorkerID worker_id;
  /// The unique id of the worker raylet.
  const NodeID raylet_id;
};

typedef std::function<std::shared_ptr<CoreWorkerClientInterface>(const rpc::Address &)>
    ClientFactoryFn;

/// Abstract client interface for testing.
class CoreWorkerClientInterface {
 public:
  virtual const rpc::Address &Addr() const {
    static const rpc::Address empty_addr_;
    return empty_addr_;
  }

  virtual void SetOnConnectedCallback(const std::function<void()> &on_connected) {}

  virtual void SetOnDisconnectedCallback(const std::function<void()> &on_disconnected) {}

  virtual void Close() {}

  /// Push an actor task directly from worker to worker.
  ///
  /// \param[in] request The request message.
  /// \param[in] skip_queue Whether to skip the task queue. This will send the
  /// task for execution immediately.
  /// \param[in] callback The callback function that handles reply.
  /// \return void.
  virtual void PushActorTask(std::unique_ptr<PushTaskRequest> request, bool skip_queue,
                             const ClientCallback<PushTaskReply> &callback) {}

  /// Similar to PushActorTask, but sets no ordering constraint. This is used to
  /// push non-actor tasks directly to a worker.
  virtual void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                              const ClientCallback<PushTaskReply> &callback) {}

  /// Notify a wait has completed for direct actor call arguments.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual void DirectActorCallArgWaitComplete(
      const DirectActorCallArgWaitCompleteRequest &request,
      const ClientCallback<DirectActorCallArgWaitCompleteReply> &callback) {}

  /// Ask the owner of an object about the object's current status.
  virtual void GetObjectStatus(const GetObjectStatusRequest &request,
                               const ClientCallback<GetObjectStatusReply> &callback) {}

  /// Ask the actor's owner to reply when the actor has gone out of scope.
  virtual void WaitForActorOutOfScope(
      const WaitForActorOutOfScopeRequest &request,
      const ClientCallback<WaitForActorOutOfScopeReply> &callback) {}

  /// Notify the owner of an object that the object has been pinned.
  virtual void SubscribeForObjectEviction(
      const SubscribeForObjectEvictionRequest &request,
      const ClientCallback<SubscribeForObjectEvictionReply> &callback) {}

  /// Send a long polling request to a core worker for pubsub operations.
  virtual void PubsubLongPolling(const PubsubLongPollingRequest &request,
                                 const ClientCallback<PubsubLongPollingReply> &callback) {
  }

  virtual void AddObjectLocationOwner(
      const AddObjectLocationOwnerRequest &request,
      const ClientCallback<AddObjectLocationOwnerReply> &callback) {}

  virtual void RemoveObjectLocationOwner(
      const RemoveObjectLocationOwnerRequest &request,
      const ClientCallback<RemoveObjectLocationOwnerReply> &callback) {}

  virtual void GetObjectLocationsOwner(
      const GetObjectLocationsOwnerRequest &request,
      const ClientCallback<GetObjectLocationsOwnerReply> &callback) {}

  /// Tell this actor to exit immediately.
  virtual void KillActor(const KillActorRequest &request,
                         const ClientCallback<KillActorReply> &callback) {}

  virtual void CancelTask(const CancelTaskRequest &request,
                          const ClientCallback<CancelTaskReply> &callback) {}

  virtual void RemoteCancelTask(const RemoteCancelTaskRequest &request,
                                const ClientCallback<RemoteCancelTaskReply> &callback) {}

  virtual void GetCoreWorkerStats(
      const GetCoreWorkerStatsRequest &request,
      const ClientCallback<GetCoreWorkerStatsReply> &callback) {}

  virtual void LocalGC(const LocalGCRequest &request,
                       const ClientCallback<LocalGCReply> &callback) {}

  virtual void WaitForRefRemoved(const WaitForRefRemovedRequest &request,

                                 const ClientCallback<WaitForRefRemovedReply> &callback) {
  }

  virtual void SpillObjects(const SpillObjectsRequest &request,
                            const ClientCallback<SpillObjectsReply> &callback) {}

  virtual void DumpObjects(const DumpObjectsRequest &request,
                           const ClientCallback<DumpObjectsReply> &callback) {}

  virtual void RestoreSpilledObjects(
      const RestoreSpilledObjectsRequest &request,
      const ClientCallback<RestoreSpilledObjectsReply> &callback) {}

  virtual void LoadDumpedObjects(const LoadDumpedObjectsRequest &request,
                                 const ClientCallback<LoadDumpedObjectsReply> &callback) {
  }

  virtual void DeleteSpilledObjects(
      const DeleteSpilledObjectsRequest &request,
      const ClientCallback<DeleteSpilledObjectsReply> &callback) {}

  virtual void AddSpilledUrl(const AddSpilledUrlRequest &request,
                             const ClientCallback<AddSpilledUrlReply> &callback) {}

  virtual void AddDumpedUrl(const AddDumpedUrlRequest &request,
                            const ClientCallback<AddDumpedUrlReply> &callback) {}

  virtual void RunOnUtilWorker(const RunOnUtilWorkerRequest &request,
                               const ClientCallback<RunOnUtilWorkerReply> &callback) {}

  virtual void PlasmaObjectReady(const PlasmaObjectReadyRequest &request,
                                 const ClientCallback<PlasmaObjectReadyReply> &callback) {
  }

  virtual void Exit(const ExitRequest &request,
                    const ClientCallback<ExitReply> &callback) {}

  virtual void BatchAssignObjectOwner(
      const BatchAssignObjectOwnerRequest &request,
      const ClientCallback<BatchAssignObjectOwnerReply> &callback) {}

  virtual ~CoreWorkerClientInterface(){};
};

class CoreWorkerClientBase : public std::enable_shared_from_this<CoreWorkerClientBase>,
                             CoreWorkerClientInterface {
 public:
  CoreWorkerClientBase() {}

  void PushActorTask(std::unique_ptr<PushTaskRequest> request, bool skip_queue,
                     const ClientCallback<PushTaskReply> &callback) override {
    if (skip_queue) {
      // Set this value so that the actor does not skip any tasks when
      // processing this request. We could also set it to max_finished_seq_no_,
      // but we just set it to the default of -1 to avoid taking the lock.
      request->set_client_processed_up_to(-1);
      SendTaskImmediately(std::move(request), callback);
      return;
    }
    SendRequest(std::move(request), callback);
  }

 protected:
  virtual void SendTaskImmediately(std::unique_ptr<PushTaskRequest> request,
                                   const ClientCallback<PushTaskReply> &callback) = 0;

 private:
  void SendRequest(std::unique_ptr<PushTaskRequest> request,
                   const ClientCallback<PushTaskReply> &callback) {
    absl::ReleasableMutexLock lock(&mutex_);
    auto this_ptr = this->shared_from_this();

    RAY_LOG(DEBUG) << "SendRequest rpc_bytes_in_flight_: " << rpc_bytes_in_flight_;

    int64_t task_size = RequestSizeInBytes(*request);
    int64_t seq_no = request->sequence_number();
    request->set_client_processed_up_to(max_finished_seq_no_);
    rpc_bytes_in_flight_ += task_size;

    auto rpc_callback = [this, this_ptr, seq_no, task_size, callback](
                            Status status, const rpc::PushTaskReply &reply) {
      {
        absl::MutexLock lock(&mutex_);
        if (seq_no > max_finished_seq_no_) {
          max_finished_seq_no_ = seq_no;
        }
        rpc_bytes_in_flight_ -= task_size;
        RAY_LOG(DEBUG) << "rpc_callback rpc_bytes_in_flight_: " << rpc_bytes_in_flight_;
        RAY_CHECK(rpc_bytes_in_flight_ >= 0);
      }
      // ANT-INTERNAL: Currently we have setIgnoreReturn option, in this case, callback
      // is null.
      if (callback) {
        callback(status, reply);
      }
    };

    /// If current connection is failed, the rpc_callback willbe triggered immediately.
    /// So the lock should be release to avoid deadlock.
    lock.Release();
    SendTaskImmediately(std::move(request), rpc_callback);
  }

 private:
  /// Protects against unsafe concurrent access from the callback thread.
  absl::Mutex mutex_;

  /// The number of bytes currently in flight.
  int64_t rpc_bytes_in_flight_ GUARDED_BY(mutex_) = 0;

  /// The max sequence number we have processed responses for.
  int64_t max_finished_seq_no_ GUARDED_BY(mutex_) = -1;
};

#if CORE_WORKER_USE_GRPC
/// Client used for communicating with a remote worker server.
class CoreWorkerClient : public CoreWorkerClientBase {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the worker server.
  /// \param[in] port Port of the worker server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  CoreWorkerClient(const rpc::Address &address, ClientCallManager &client_call_manager)
      : addr_(address) {
    grpc_client_ =
        std::shared_ptr<GrpcClient<CoreWorkerService>>(new GrpcClient<CoreWorkerService>(
            addr_.ip_address(), addr_.port(), client_call_manager));
  };

  const rpc::Address &Addr() const override { return addr_; }

#define CORE_WORKER_RPC_METHOD(METHOD) \
  CORE_WORKER_VOID_RPC_CLIENT_METHOD(CoreWorkerService, METHOD, grpc_client_, override)
  CORE_WORKER_RPC_METHODS
#undef CORE_WORKER_RPC_METHOD

  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override {
    request->set_sequence_number(-1);
    request->set_client_processed_up_to(-1);
    INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, callback, grpc_client_);
  }

  void SendTaskImmediately(std::unique_ptr<PushTaskRequest> request,
                           const ClientCallback<PushTaskReply> &callback) override {
    INVOKE_RPC_CALL(CoreWorkerService, PushTask, *request, callback, grpc_client_);
  }

 private:
  /// Address of the remote worker.
  rpc::Address addr_;

  /// The RPC client.
  std::shared_ptr<GrpcClient<CoreWorkerService>> grpc_client_;
};

#undef INVOKE_RPC_CALL

#else

template <typename T>
using AsyncRpcClientCreater =
    std::function<void(const std::string &, const int,
                       std::function<void(const ray::Status &, std::shared_ptr<T>)>)>;

/// A wrapper client which handles connection and reconnection
template <typename T>
class ReconnectableRpcClient
    : public std::enable_shared_from_this<ReconnectableRpcClient<T>> {
 public:
  using std::enable_shared_from_this<ReconnectableRpcClient<T>>::shared_from_this;

  ReconnectableRpcClient(const std::string &address, const int port,
                         AsyncRpcClientCreater<T> async_rpc_client_creater,
                         boost::asio::io_service &io_service, bool infinite_reconnect)
      : address_(address),
        port_(port),
        async_rpc_client_creater_(async_rpc_client_creater),
        io_service_(io_service),
        reconnect_timer_(io_service),
        infinite_reconnect_(infinite_reconnect),
        is_closed_(false),
        is_reconnecting_(false) {
    absl::MutexLock lock(&future_mutex_);
    ok_to_send_requests_future_ = ok_to_send_requests_.get_future().share();
  }

  ~ReconnectableRpcClient() {
    if (infinite_reconnect_) {
      RAY_CHECK(is_closed_) << "This is an RPC client for actor tasks. It must "
                               "be closed explicitly.";
    }
  }

  std::string RemoteLocation() { return address_ + ":" + std::to_string(port_); }

  /// Start this client.
  void Start() {
    std::weak_ptr<ReconnectableRpcClient<T>> weak_this(shared_from_this());
    io_service_.dispatch([weak_this]() {
      auto shared_this = weak_this.lock();
      if (!shared_this || shared_this->is_closed_) {
        return;
      }
      shared_this->AsyncConnect();
    });
  }

  void SetOnConnectedCallback(const std::function<void()> &on_connected) {
    on_connected_ = on_connected;
  }

  void SetOnDisconnectedCallback(const std::function<void()> &on_disconnected) {
    on_disconnected_ = on_disconnected;
  }

  void SetOnCloseCallback(const std::function<void()> &on_close) { on_close_ = on_close; }

  void Close() {
    if (is_closed_) {
      return;
    }
    {
      absl::MutexLock lock(&future_mutex_);
      try {
        ok_to_send_requests_.set_value();
      } catch (const std::future_error &e) {
        RAY_CHECK(e.code() == std::future_errc::promise_already_satisfied) << e.what();
      }
    }
    is_closed_ = true;
    is_reconnecting_ = false;
    if (on_close_) {
      on_close_();
    }
    RAY_LOG(INFO) << "The RPC client to " << RemoteLocation() << " is now closed.";
  }

  /// Get the underlying RPC client, if available.
  std::shared_ptr<T> GetRpcClient() {
    absl::MutexLock lock(&rpc_client_mutex_);
    return rpc_client_;
  }

  void SetRpcClient(std::shared_ptr<T> client) {
    absl::MutexLock lock(&rpc_client_mutex_);
    rpc_client_ = client;
  }

  /// Only used in tests.
  bool IsReconnecting() { return is_reconnecting_; }

  /// Whether the connection of the client object is failed or not.
  bool IsClosed() { return is_closed_; }

 private:
  /// Connect to the remote location asynchronously.
  void AsyncConnect() {
    std::weak_ptr<ReconnectableRpcClient<T>> weak_this(shared_from_this());
    RAY_LOG(DEBUG) << "Connecting to " << RemoteLocation();

    async_rpc_client_creater_(
        address_, port_,
        [weak_this](const Status &status, std::shared_ptr<T> rpc_client) {
          auto shared_this = weak_this.lock();
          if (!shared_this || shared_this->is_closed_) {
            return;
          }
          if (rpc_client != nullptr) {
            rpc_client->SetDisconnectCallback([weak_this]() {
              auto shared_this = weak_this.lock();
              if (!shared_this || shared_this->is_closed_) {
                return;
              }
              {
                // Set rpc_client_ to nullptr, so all the requests pending after
                // disconnect.
                absl::MutexLock lock(&shared_this->connection_mutex_);
                shared_this->SetRpcClient(nullptr);

                {
                  absl::MutexLock lock(&shared_this->future_mutex_);
                  /// Reset the promise object to make the user therad waiting for resend.
                  shared_this->ok_to_send_requests_ = std::promise<void>();
                  shared_this->ok_to_send_requests_future_ =
                      shared_this->ok_to_send_requests_.get_future().share();
                }
              }
              if (shared_this->on_disconnected_) {
                shared_this->on_disconnected_();
              }
              if (shared_this->infinite_reconnect_) {
                shared_this->AsyncConnect();
              } else {
                shared_this->Close();
              }
            });
          }
          shared_this->ConnectCallback(status, rpc_client);
        });
  }

  /// The callback to be called when the remote location is successfully connected.
  void ConnectCallback(const Status &status, std::shared_ptr<T> rpc_client) {
    if (status.ok()) {
      // Successfully connected to the remote location.
      RAY_CHECK(rpc_client);
      RAY_LOG(INFO) << "Connected to " << RemoteLocation();
      current_try_connect_times_ = 0;
      next_retry_interval_sec_ = 0;
      SetRpcClient(rpc_client);
      if (on_connected_) {
        on_connected_();
      }
      is_reconnecting_ = false;
    } else {
      // Failed to connect to the remote location.
      ++current_try_connect_times_;
      is_reconnecting_ = true;
      std::stringstream ss;
      ss << "Failed to connect to " << RemoteLocation() << ". Tried "
         << current_try_connect_times_ << " times.";
      if (!is_closed_ &&
          (infinite_reconnect_ || current_try_connect_times_ < max_try_connect_times_)) {
        // Reconnect if it doesn't reach the limit.
        next_retry_interval_sec_ *= 2;
        if (next_retry_interval_sec_ < reconnect_min_interval_sec_) {
          next_retry_interval_sec_ = reconnect_min_interval_sec_;
        }
        if (next_retry_interval_sec_ > reconnect_max_interval_sec_) {
          next_retry_interval_sec_ = reconnect_max_interval_sec_;
        }
        ss << " Will retry in " << next_retry_interval_sec_ << " seconds";
        RAY_LOG(WARNING) << ss.str();

        std::weak_ptr<ReconnectableRpcClient<T>> weak_this(shared_from_this());
        reconnect_timer_.expires_from_now(
            boost::posix_time::seconds(next_retry_interval_sec_));
        reconnect_timer_.async_wait([weak_this](const boost::system::error_code &error) {
          if (error != boost::asio::error::operation_aborted) {
            auto shared_this = weak_this.lock();
            if (!shared_this || shared_this->is_closed_) {
              return;
            }
            // Reconnect on failing to establish the connection. (not on disconnected)
            shared_this->AsyncConnect();
          }
        });
      } else {
        // We have reached the limit for reconnection.
        ss << ". Abort retrying.";
        auto error_message = ss.str();
        // Maybe the state of the remote actor has changed. Ignore this failure
        // and wait for the next actor state update (to establish a new connection
        // if the actor came back to alive).
        RAY_LOG(ERROR) << error_message;

        Close();
      }
    }
  }

  std::shared_ptr<T> rpc_client_ GUARDED_BY(rpc_client_mutex_);

  const std::string address_;

  const int port_;

  const AsyncRpcClientCreater<T> async_rpc_client_creater_;

  boost::asio::io_service &io_service_;

  std::function<void()> on_connected_;

  std::function<void()> on_disconnected_;

  std::function<void()> on_close_;

  uint32_t current_try_connect_times_ = 0;

  uint32_t next_retry_interval_sec_ = 0;

  boost::asio::deadline_timer reconnect_timer_;

  bool infinite_reconnect_;

  std::atomic<bool> is_closed_;

  /// This is for RPC clients with infinite_reconnect off only.
  const uint32_t max_try_connect_times_ =
      ::RayConfig::instance().rpc_reconnect_max_try_connect_times();

  const uint32_t reconnect_min_interval_sec_ =
      ::RayConfig::instance().rpc_reconnect_min_interval_sec();

  const uint32_t reconnect_max_interval_sec_ =
      ::RayConfig::instance().rpc_reconnect_max_interval_sec();

  /// Only for test
  std::atomic<bool> is_reconnecting_;

  absl::Mutex rpc_client_mutex_;

 public:
  absl::Mutex connection_mutex_;
  std::promise<void> ok_to_send_requests_ GUARDED_BY(future_mutex_);
  std::shared_future<void> ok_to_send_requests_future_;
  absl::Mutex future_mutex_;
};

#define CORE_WORKER_SEND_PENDING_RPC_REQUESTS_BEGIN(INTERFACE_METHOD)       \
  {                                                                         \
    absl::ReleasableMutexLock lock(&pending_request_mutex_);                \
    auto requests_copy = std::move(pending_##INTERFACE_METHOD##_requests_); \
    lock.Release();                                                         \
    for (auto &request : requests_copy) {
#define CORE_WORKER_SEND_PENDING_RPC_REQUESTS_END(INTERFACE_METHOD) \
  }                                                                 \
  }

#define CORE_WORKER_SEND_PENDING_PUSH_TASK_RPC_REQUESTS(INTERFACE_METHOD) \
  CORE_WORKER_SEND_PENDING_RPC_REQUESTS_BEGIN(INTERFACE_METHOD)           \
  INTERFACE_METHOD(std::move(request.first), request.second,              \
                   /*is_pending_request=*/true);                          \
  CORE_WORKER_SEND_PENDING_RPC_REQUESTS_END(INTERFACE_METHOD)

#define CORE_WORKER_SEND_PENDING_RPC_REQUESTS(INTERFACE_METHOD)                 \
  CORE_WORKER_SEND_PENDING_RPC_REQUESTS_BEGIN(INTERFACE_METHOD)                 \
  INTERFACE_METHOD(request.first, request.second, /*is_pending_request=*/true); \
  CORE_WORKER_SEND_PENDING_RPC_REQUESTS_END(INTERFACE_METHOD)

#define CORE_WORKER_FAIL_PENDING_RPC_REQUESTS(INTERFACE_METHOD, METHOD)               \
  {                                                                                   \
    absl::MutexLock lock(&pending_request_mutex_);                                    \
    for (auto &request : pending_##INTERFACE_METHOD##_requests_) {                    \
      const auto &callback = request.second;                                          \
      if (callback) {                                                                 \
        auto status = Status::IOError("The RPC client to " +                          \
                                      rpc_client_->RemoteLocation() + " is closed."); \
        /* Delay the invocation of reply callback to avoid dead lock. */              \
        reply_service_.post([callback = std::move(callback), status]() {              \
          METHOD##Reply reply;                                                        \
          callback(status, reply);                                                    \
        });                                                                           \
      }                                                                               \
    }                                                                                 \
    pending_##INTERFACE_METHOD##_requests_.clear();                                   \
  }

/// Client used for communicating with a remote worker server.
class CoreWorkerBrpcClient : public CoreWorkerClientBase {
 public:
  /// Constructor.
  ///
  CoreWorkerBrpcClient(const rpc::Address &address,
                       AsyncRpcClientCreater<BrpcStreamClient> async_rpc_client_creater,
                       ClientCallManager &client_call_manager, bool infinite_reconnect)
      // NOTE: to improve rpc performance, the RPC reqeusts and replies are handled in
      // different threads.
      : addr_(address),
        request_service_(client_call_manager.RequestService()),
        reply_service_(client_call_manager.ReplyService()) {
    rpc_client_ = std::make_shared<ReconnectableRpcClient<BrpcStreamClient>>(
        addr_.ip_address(), addr_.port(), async_rpc_client_creater,
        client_call_manager.IOService(), infinite_reconnect);
  }

  void Start() {
    std::weak_ptr<CoreWorkerClientBase> weak_this(shared_from_this());
    rpc_client_->SetOnConnectedCallback([this, weak_this]() {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      RAY_CHECK(rpc_client_->GetRpcClient());

#define CORE_WORKER_RPC_METHOD(METHOD) CORE_WORKER_SEND_PENDING_RPC_REQUESTS(METHOD)

      CORE_WORKER_SEND_PENDING_PUSH_TASK_RPC_REQUESTS(PushActorTask)
      CORE_WORKER_SEND_PENDING_PUSH_TASK_RPC_REQUESTS(PushNormalTask)
      CORE_WORKER_RPC_METHODS

#undef CORE_WORKER_RPC_METHOD

      {
        absl::MutexLock lock(&rpc_client_->future_mutex_);
        try {
          rpc_client_->ok_to_send_requests_.set_value();
        } catch (const std::future_error &e) {
          RAY_CHECK(e.code() == std::future_errc::promise_already_satisfied) << e.what();
        }
      }
      if (on_connected_) {
        on_connected_();
      }
    });

    rpc_client_->SetOnCloseCallback([this, weak_this]() {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      ClearFailRequests();
    });
    rpc_client_->Start();
  }

  ~CoreWorkerBrpcClient() { ClearFailRequests(); }

  void ClearFailRequests() {
    if (reply_service_.stopped()) {
      return;
    }
#define CORE_WORKER_RPC_METHOD(METHOD) \
  CORE_WORKER_FAIL_PENDING_RPC_REQUESTS(METHOD, METHOD)

    CORE_WORKER_FAIL_PENDING_RPC_REQUESTS(PushActorTask, PushTask)
    CORE_WORKER_FAIL_PENDING_RPC_REQUESTS(PushNormalTask, PushTask)
    CORE_WORKER_RPC_METHODS

#undef CORE_WORKER_RPC_METHOD
  }

  void SetOnConnectedCallback(const std::function<void()> &on_connected) override {
    on_connected_ = on_connected;
  }

  void SetOnDisconnectedCallback(const std::function<void()> &on_disconnected) override {
    std::weak_ptr<CoreWorkerClientBase> weak_this(shared_from_this());
    auto wrapped_callback = [weak_this, on_disconnected]() {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      on_disconnected();
    };
    rpc_client_->SetOnDisconnectedCallback(wrapped_callback);
  }

  void Close() override {
    ClearFailRequests();
    rpc_client_->Close();
  }

  const rpc::Address &Addr() const override { return addr_; }

 private:
#define CORE_WORKER_RPC_METHOD(METHOD) \
  CORE_WORKER_VOID_RPC_CLIENT_METHOD(CoreWorkerService, METHOD, rpc_client_, )

  CORE_WORKER_RPC_METHODS

#undef CORE_WORKER_RPC_METHOD

 public:
  bool IsPendingPushActorTaskRequestsEmpty() {
    absl::ReleasableMutexLock lock(&pending_request_mutex_);
    return pending_PushActorTask_requests_.empty();
  }

#define CORE_WORKER_RPC_METHOD(METHOD)                                  \
  void METHOD(const METHOD##Request &request,                           \
              const ClientCallback<METHOD##Reply> &callback) override { \
    METHOD(request, callback, /*is_pending_request=*/false);            \
  }

  CORE_WORKER_RPC_METHODS

#undef CORE_WORKER_RPC_METHOD

  void PushActorTask(std::unique_ptr<PushTaskRequest> request, bool skip_queue,
                     const ClientCallback<PushTaskReply> &callback) override {
    CoreWorkerClientBase::PushActorTask(std::move(request), skip_queue,
                                        std::move(callback));
  }

  void SendTaskImmediately(std::unique_ptr<PushTaskRequest> request,
                           const ClientCallback<PushTaskReply> &callback) override {
    PushActorTask(std::move(request), callback);
  }

  /// Only used in tests.
  bool IsReconnecting() { return rpc_client_->IsReconnecting(); }

 private:
  void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                     const ClientCallback<PushTaskReply> &callback) {
    PushActorTask(std::move(request), callback, /*is_pending_request=*/false);
  }

  void PushActorTask(std::unique_ptr<PushTaskRequest> request,
                     const ClientCallback<PushTaskReply> &callback,
                     bool is_pending_request) LOCKS_EXCLUDED(pending_request_mutex_) {
    VOID_RPC_CLIENT_METHOD_PARTIAL(PushActorTask, PushTask, rpc_client_);
    request->set_intended_worker_id(addr_.worker_id());
    // NOTE: we don't do back-pressure for now. Just set the members so that scheduling
    // queue in direct actor transport can work.
    request->set_client_processed_up_to(-1);

    if (is_pending_request) {
      // Serialize and send request in current thread.
      underlying_rpc_client->template CallMethod<PushTaskRequest, PushTaskReply,
                                                 CoreWorkerServiceMessageType>(
          CoreWorkerServiceMessageType::PushTaskRequestMessage,
          CoreWorkerServiceMessageType::PushTaskReplyMessage, *request,
          const_cast<ClientCallback<PushTaskReply> &>(callback), "PushTask");
    } else {
      // Move the request serialization to io thread to speed up.
      auto shared_request =
          std::make_shared<std::unique_ptr<PushTaskRequest>>(std::move(request));
      // NOTE(kfstorm): use `post` instead of `dispatch` here to ensure the ordering
      // regardless of whether current thread is the IO thread or not.
      request_service_.post(std::bind(
          [underlying_rpc_client,
           shared_request](ClientCallback<PushTaskReply> &callback) {
            underlying_rpc_client->template CallMethod<PushTaskRequest, PushTaskReply,
                                                       CoreWorkerServiceMessageType>(
                CoreWorkerServiceMessageType::PushTaskRequestMessage,
                CoreWorkerServiceMessageType::PushTaskReplyMessage, **shared_request,
                callback, "PushTask");
          },
          std::move(const_cast<ClientCallback<PushTaskReply> &>(callback))));
    }
  }

 public:
  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override {
    PushNormalTask(std::move(request), callback, /*is_pending_request=*/false);
  }

 private:
  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback,
                      bool is_pending_request) LOCKS_EXCLUDED(pending_request_mutex_) {
    VOID_RPC_CLIENT_METHOD_PARTIAL(PushNormalTask, PushTask, rpc_client_);
    request->set_intended_worker_id(addr_.worker_id());
    request->set_sequence_number(-1);
    request->set_client_processed_up_to(-1);
    underlying_rpc_client->template CallMethod<PushTaskRequest, PushTaskReply,
                                               CoreWorkerServiceMessageType>(
        CoreWorkerServiceMessageType::PushTaskRequestMessage,
        CoreWorkerServiceMessageType::PushTaskReplyMessage, *request,
        const_cast<ClientCallback<PushTaskReply> &>(callback), "PushTask");
  }

 private:
  /// Address of the remote worker.
  rpc::Address addr_;

  /// The RPC client.
  std::shared_ptr<ReconnectableRpcClient<BrpcStreamClient>> rpc_client_;

  std::function<void()> on_connected_;

  absl::Mutex pending_request_mutex_;

  std::vector<std::pair<std::unique_ptr<PushTaskRequest>, ClientCallback<PushTaskReply>>>
      pending_PushActorTask_requests_ GUARDED_BY(pending_request_mutex_);
  std::vector<std::pair<std::unique_ptr<PushTaskRequest>, ClientCallback<PushTaskReply>>>
      pending_PushNormalTask_requests_ GUARDED_BY(pending_request_mutex_);

#define CORE_WORKER_RPC_METHOD(METHOD)                                   \
  std::vector<std::pair<METHOD##Request, ClientCallback<METHOD##Reply>>> \
      pending_##METHOD##_requests_ GUARDED_BY(pending_request_mutex_);

  CORE_WORKER_RPC_METHODS

#undef CORE_WORKER_RPC_METHOD

  instrumented_io_context &request_service_;

  instrumented_io_context &reply_service_;

  friend struct CoreWorkerClientTest;
};

#undef CORE_WORKER_SEND_PENDING_RPC_REQUESTS
#undef CORE_WORKER_FAIL_PENDING_RPC_REQUESTS

// A wrapper client of CoreWorkerBrpcClient to allow using `new CoreWorkerClient(...)`
// without the needing of calling `Start()` manually. This can reduce code differences
// between Ant and community.
class CoreWorkerClient : public std::enable_shared_from_this<CoreWorkerClient>,
                         public CoreWorkerClientInterface {
 public:
  /// Constructor.
  ///
  CoreWorkerClient(const rpc::Address &addr, ClientCallManager &client_call_manager,
                   bool infinite_reconnect = false) {
    // Make sure the worker_id field of Address is correctly filled.
    RAY_CHECK(!WorkerID::FromBinary(addr.worker_id()).IsNil());

    // NOTE: ideally we want to use brpc stream for worker tasks and unary for everything
    // else, there's an issue with testPuttingAndGettingManyObjects in java's StressTest,
    // where raylet sends many (100K) WaitForObjectEviction RPCs to worker, which won't
    // return util the objects are evicted (or the worker dies), this exceeds brpc unary's
    // internal limits and thus some of the RPCs return EAGAIN, in that case raylet treats
    // it as the worker death, and thus unpins the object in plasma store. For now we
    // force all worker communications to still use brpc stream to work around this issue.
    auto async_rpc_client_creater =
        std::bind(rpc::BrpcStreamClient::AsyncCreateRpcClient,
                  brpcpb::StreamServiceType::CoreWorkerServiceType, std::placeholders::_1,
                  std::placeholders::_2, std::ref(client_call_manager.ReplyService()),
                  std::placeholders::_3);
    wrapped_client_ = std::make_shared<CoreWorkerBrpcClient>(
        addr, async_rpc_client_creater, client_call_manager, infinite_reconnect);
    wrapped_client_->Start();
  }

  const rpc::Address &Addr() const override { return wrapped_client_->Addr(); }

  void SetOnConnectedCallback(const std::function<void()> &on_connected) override {
    wrapped_client_->SetOnConnectedCallback(on_connected);
  }

  void SetOnDisconnectedCallback(const std::function<void()> &on_disconnected) override {
    wrapped_client_->SetOnDisconnectedCallback(on_disconnected);
  }

  void Close() override { wrapped_client_->Close(); }

#define WRAP_CALLBACK_CODE_SNIPPET(REPLY_TYPE)                                           \
  ClientCallback<REPLY_TYPE> wrapped_callback = nullptr;                                 \
  if (callback) {                                                                        \
    /* NOTE(kfstorm): This is to avoid request not been sent when an RPC client is       \
     * out-of-scope right after invoking an RPC method. */                               \
    wrapped_callback =                                                                   \
        [shared_this = shared_from_this(), callback = std::move(callback)](              \
            const Status &status, const REPLY_TYPE &reply) { callback(status, reply); }; \
  }

  void PushActorTask(std::unique_ptr<PushTaskRequest> request, bool skip_queue,
                     const ClientCallback<PushTaskReply> &callback) override {
    WRAP_CALLBACK_CODE_SNIPPET(PushTaskReply)
    wrapped_client_->PushActorTask(std::move(request), skip_queue,
                                   std::move(wrapped_callback));
  }

  void PushNormalTask(std::unique_ptr<PushTaskRequest> request,
                      const ClientCallback<PushTaskReply> &callback) override {
    WRAP_CALLBACK_CODE_SNIPPET(PushTaskReply)
    wrapped_client_->PushNormalTask(std::move(request), std::move(wrapped_callback));
  }

  bool IsPendingPushActorTaskRequestsEmpty() {
    return wrapped_client_->IsPendingPushActorTaskRequestsEmpty();
  }

  /// Whether the reconnectable client is reconnecting or not.
  /// NOTE:(wanxing.wwx) Only used in tests.
  bool IsReconnecting() { return wrapped_client_->IsReconnecting(); }

#define CORE_WORKER_RPC_METHOD(METHOD)                                  \
  void METHOD(const METHOD##Request &request,                           \
              const ClientCallback<METHOD##Reply> &callback) override { \
    WRAP_CALLBACK_CODE_SNIPPET(METHOD##Reply)                           \
    wrapped_client_->METHOD(request, std::move(wrapped_callback));      \
  }

  CORE_WORKER_RPC_METHODS
#undef CORE_WORKER_RPC_METHOD

 private:
  /// The BRPC client.
  std::shared_ptr<CoreWorkerBrpcClient> wrapped_client_;
};

#endif

#undef VOID_RPC_CLIENT_METHOD_PARTIAL

}  // namespace rpc
}  // namespace ray

#ifdef __clang__
#pragma clang diagnostic pop
#endif
