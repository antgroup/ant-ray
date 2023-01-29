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

#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "absl/synchronization/mutex.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/util/util.h"

namespace ray {
namespace rpc {

/// Represents an outgoing gRPC request.
///
/// NOTE(hchen): Compared to `ClientCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
class ClientCall {
 public:
  /// The callback to be called by `ClientCallManager` when the reply of this request is
  /// received.
  virtual void OnReplyReceived() = 0;
  /// Return status.
  virtual ray::Status GetStatus() = 0;
  /// Set return status.
  virtual void SetReturnStatus() = 0;
  /// Get human-readable name for this RPC.
  virtual std::string GetName() = 0;

  virtual ~ClientCall() = default;
};

class ClientCallManager;

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

/// Implementation of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientCallImpl : public ClientCall {
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientCallImpl(
      const std::function<void(const Status &status, std::shared_ptr<Reply>)> &callback,
      std::string call_name)
      : reply_(new Reply), callback_(callback), call_name_(std::move(call_name)) {}

  Status GetStatus() override {
    absl::MutexLock lock(&mutex_);
    return return_status_;
  }

  void SetReturnStatus() override {
    absl::MutexLock lock(&mutex_);
    return_status_ = GrpcStatusToRayStatus(status_);
  }

  void OnReplyReceived() override {
    ray::Status status;
    {
      absl::MutexLock lock(&mutex_);
      status = return_status_;
    }
    if (callback_ != nullptr) {
      callback_(status, std::move(reply_));
    }
  }

  std::string GetName() override { return call_name_; }

 private:
  /// The reply message.
  std::shared_ptr<Reply> reply_;

  /// The callback function to handle the reply.
  std::function<void(const Status &status, std::shared_ptr<Reply>)> callback_;

  /// The human-readable name of the RPC.
  std::string call_name_;

  /// The response reader.
  std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Mutex to protect the return_status_ field.
  absl::Mutex mutex_;

  /// This is the status to be returned from GetStatus(). It is safe
  /// to read from other threads while they hold mutex_. We have
  /// return_status_ = GrpcStatusToRayStatus(status_) but need
  /// a separate variable because status_ is set internally by
  /// GRPC and we cannot control it holding the lock.
  ray::Status return_status_ GUARDED_BY(mutex_);

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class ClientCallManager;
};

/// This class wraps a `ClientCall`, and is used as the `tag` of gRPC's `CompletionQueue`.
///
/// The lifecycle of a `ClientCallTag` is as follows.
///
/// When a client submits a new gRPC request, a new `ClientCallTag` object will be created
/// by `ClientCallMangager::CreateCall`. Then the object will be used as the tag of
/// `CompletionQueue`.
///
/// When the reply is received, `ClientCallMangager` will get the address of this object
/// via `CompletionQueue`'s tag. And the manager should call
/// `GetCall()->OnReplyReceived()` and then delete this object.
class ClientCallTag {
 public:
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call) : call_(std::move(call)) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

 private:
  std::shared_ptr<ClientCall> call_;
};

/// Represents the generic signature of a `FooService::Stub::PrepareAsyncBar`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using PrepareAsyncFunction =
    std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> (GrpcService::Stub::*)(
        grpc::ClientContext *context, const Request &request, grpc::CompletionQueue *cq);

/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of
/// `ClientCall` objects.
///
/// It maintains a thread that keeps polling events from `CompletionQueue`, and post
/// the callback function to the main event loop when a reply is received.
///
/// Multiple clients can share one `ClientCallManager`.
class ClientCallManager {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service An all in one event loop, both RPC reqeusts and replies are
  /// handled.
  /// \param[in] num_threads Number of threads to handle `PollEventsFromCompletionQueue`
  /// concurrently.
  explicit ClientCallManager(instrumented_io_context &main_service, int num_threads = 1,
                             bool request_in_current_thread = false)
      : ClientCallManager(main_service, main_service, main_service, num_threads,
                          request_in_current_thread) {}

  /// Constructor.
  ///
  /// \param[in] request_service The event loop, used to handle rpc request.
  /// \param[in] reply_service The event loop, to which the reply callback functions
  /// will be posted.
  /// \param[in] num_threads Number of threads to handle `PollEventsFromCompletionQueue`
  /// concurrently.
  explicit ClientCallManager(instrumented_io_context &request_service,
                             instrumented_io_context &reply_service,
                             instrumented_io_context &io_service, int num_threads = 1,
                             bool request_in_current_thread = false)
      : request_service_(request_service),
        reply_service_(reply_service),
        io_service_(io_service),
        num_threads_(num_threads),
        shutdown_(false),
        request_in_current_thread_(request_in_current_thread) {
    rr_index_ = rand() % num_threads_;
    // Start the polling threads.
    cqs_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
      cqs_.emplace_back();
      polling_threads_.emplace_back(&ClientCallManager::PollEventsFromCompletionQueue,
                                    this, i);
    }
  }

  ~ClientCallManager() {
    shutdown_ = true;
    for (auto &cq : cqs_) {
      cq.Shutdown();
    }
    for (auto &polling_thread : polling_threads_) {
      polling_thread.join();
    }
  }

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return void
  template <class GrpcService, class Request, class Reply>
  void CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback,
      std::string call_name) {
    auto action = std::bind(
        [this, call_name](
            typename GrpcService::Stub &stub,
            PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
            Request request, ClientCallback<Reply> callback) {
          auto call = std::make_shared<ClientCallImpl<Reply>>(
              [this, callback, call_name](const Status &status,
                                          std::shared_ptr<Reply> reply) {
                if (callback && !reply_service_.stopped() && !shutdown_) {
                  reply_service_.post(
                      [status, reply, callback] { callback(status, *reply); },
                      call_name + ".reply");
                }
              },
              call_name);
          // Send request.
          // Find the next completion queue to wait for response.
          call->response_reader_ = (stub.*prepare_async_function)(
              &call->context_, request, &cqs_[rr_index_++ % num_threads_]);
          call->response_reader_->StartCall();
          // Create a new tag object. This object will eventually be deleted in the
          // `ClientCallManager::PollEventsFromCompletionQueue` when reply is received.
          //
          // NOTE(chen): Unlike `ServerCall`, we can't directly use `ClientCall` as the
          // tag. Because this function must return a `shared_ptr` to make sure the
          // returned `ClientCall` is safe to use. But `response_reader_->Finish` only
          // accepts a raw pointer.
          auto tag = new ClientCallTag(call);
          call->response_reader_->Finish(call->reply_.get(), &call->status_, (void *)tag);
        },
        stub, prepare_async_function, request, callback);
    if (request_in_current_thread_) {
      action();
    } else {
      request_service_.dispatch(action, call_name);
    }
  }

  /// ANT-INTERNAL: Get the main service.
  instrumented_io_context &RequestService() { return request_service_; }
  instrumented_io_context &ReplyService() { return reply_service_; }
  instrumented_io_context &IOService() { return io_service_; }

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue(int index) {
    SetThreadName("client.poll" + std::to_string(index));
    void *got_tag;
    bool ok = false;
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
    // synchronous cq_.Next blocks indefinitely in the case that the process
    // received a SIGTERM.
    while (true) {
      auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                   gpr_time_from_millis(250, GPR_TIMESPAN));
      auto status = cqs_[index].AsyncNext(&got_tag, &ok, deadline);
      if (status == grpc::CompletionQueue::SHUTDOWN) {
        break;
      } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
        // If we timed out and shutdown, then exit immediately. This should not
        // be needed, but gRPC seems to not return SHUTDOWN correctly in these
        // cases (e.g., test_wait will hang on shutdown without this check).
        break;
      } else if (status != grpc::CompletionQueue::TIMEOUT) {
        auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
        tag->GetCall()->SetReturnStatus();
        if (ok) {
          tag->GetCall()->OnReplyReceived();
        }
        delete tag;
      }
    }
  }

  /// The event loop used to send rpc reqeusts.
  instrumented_io_context &request_service_;
  /// The event loop used to handle rpc reply callback.
  instrumented_io_context &reply_service_;
  instrumented_io_context &io_service_;

  /// The number of polling threads.
  int num_threads_;

  /// Whether the client has shutdown.
  std::atomic<bool> shutdown_;

  /// The index to send RPCs in a round-robin fashion
  std::atomic<unsigned int> rr_index_;

  /// The gRPC `CompletionQueue` object used to poll events.
  std::vector<grpc::CompletionQueue> cqs_;

  /// Polling threads to check the completion queue.
  std::vector<std::thread> polling_threads_;

  /// Whether to send request in current thread.
  // In raylet we want to send request in current thread, so that heartbeats can be sent
  // in heartbeat thread instead main_service. In CoreWorker, we want to send requests
  // in separate request_service to optimize performance.
  bool request_in_current_thread_ = false;
};

}  // namespace rpc
}  // namespace ray
