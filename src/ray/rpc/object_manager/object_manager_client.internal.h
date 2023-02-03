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
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>

#include <thread>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/rpc/brpc/client/stream_client.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

#define OBJECT_MANAGER_RPC_METHODS       \
  OBJECT_MANAGER_RPC_METHOD(Push)        \
  OBJECT_MANAGER_RPC_METHOD(Pull)        \
  OBJECT_MANAGER_RPC_METHOD(FreeObjects)

#define VOID_RPC_CLIENT_METHOD_PARTIAL(INTERFACE_METHOD, METHOD)                     \
  std::shared_ptr<BrpcStreamClient> underlying_rpc_client = nullptr;                 \
  if (!is_pending_request) {                                                         \
    /*Lock here to keep exclusive with rpc disconnect, ensure the pending all          \
      the requests when reconnecting.*/                                                \
    absl::MutexLock connection_lock(&rpc_client_->connection_mutex_);                  \
    absl::ReleasableMutexLock pending_request_lock(&pending_request_mutex_);           \
    underlying_rpc_client = rpc_client_->GetRpcClient();                               \
    /* The underlying_rpc_client is null means the client is in initial state or       \
      during connecting or the connection has failed.                                  \
    */                                                                                 \
    if (!underlying_rpc_client) {                                                      \
      if (rpc_client_->IsClosed()) {                                                   \
        auto status = Status::IOError("The RPC client to " +                           \
                                      rpc_client_->RemoteLocation() + " is closed.");  \
        reply_service_.post([callback = std::move(callback), status]() {               \
          METHOD##Reply reply;                                                         \
          callback(status, reply);                                                     \
        });                                                                            \
      } else {                                                                         \
        pending_##INTERFACE_METHOD##_requests_.emplace_back(std::move(request),        \
                                                            std::move(callback));      \
      }                                                                                \
      return;                                                                          \
    }                                                                                  \
    pending_request_lock.Release();                                                    \
    rpc_client_->ok_to_send_requests_future_.wait();                                   \
  } else {                                                                             \
    underlying_rpc_client = rpc_client_->GetRpcClient();                                \
    /*The underlying rpc client should not be nullptr when send pending requests.*/    \
    RAY_CHECK(underlying_rpc_client);                                                  \
  }
#define OBJECT_MANAGER_VOID_RPC_CLIENT_METHOD(SERVICE, METHOD, SPECS)                   \
  void METHOD(const METHOD##Request &request,                                           \
              const ClientCallback<METHOD##Reply> &callback, bool is_pending_request)   \
      SPECS                                                                             \
      LOCKS_EXCLUDED(pending_request_mutex_) {                                          \
    VOID_RPC_CLIENT_METHOD_PARTIAL(METHOD, METHOD);                                     \
    /* TODO (kfstorm): Support worker task stats about asio queue length. */            \
    underlying_rpc_client                                                               \
        ->template CallMethod<METHOD##Request, METHOD##Reply, SERVICE##MessageType>(    \
            SERVICE##MessageType::METHOD##RequestMessage,                               \
            SERVICE##MessageType::METHOD##ReplyMessage, request,                        \
            const_cast<ClientCallback<METHOD##Reply> &>(callback), #METHOD);            \
  }
#define OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS_BEGIN(INTERFACE_METHOD)    \
  {                                                                         \
    absl::ReleasableMutexLock lock(&pending_request_mutex_);                \
    auto requests_copy = std::move(pending_##INTERFACE_METHOD##_requests_); \
    lock.Release();                                                         \
    for (auto &request : requests_copy) {
#define OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS_END(INTERFACE_METHOD) \
  }                                                                    \
  }
#define OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS(INTERFACE_METHOD)                 \
  OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS_BEGIN(INTERFACE_METHOD)                 \
  INTERFACE_METHOD(request.first, request.second, /*is_pending_request=*/true);    \
  OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS_END(INTERFACE_METHOD)
#define OBJECT_MANAGER_FAIL_PENDING_RPC_REQUESTS(INTERFACE_METHOD, METHOD)            \
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

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class ObjectManagerBrpcClient : public std::enable_shared_from_this<ObjectManagerBrpcClient> {
 public:
  ObjectManagerBrpcClient(const std::string &ip_address, const int port,
                          AsyncRpcClientCreater<BrpcStreamClient> async_rpc_client_creater,
                          ClientCallManager &client_call_manager, bool infinite_reconnect)
    : request_service_(client_call_manager.RequestService()),
      reply_service_(client_call_manager.ReplyService()) {
      rpc_client_ = std::make_shared<ReconnectableRpcClient<BrpcStreamClient>>(
        ip_address, port, async_rpc_client_creater,
        client_call_manager.IOService(), infinite_reconnect);
  }

#define OBJECT_MANAGER_RPC_METHOD(METHOD)                      \
  void METHOD(const METHOD##Request &request,                  \
              const ClientCallback<METHOD##Reply> &callback) { \
    METHOD(request, callback, /*is_pending_request=*/false);   \
  }

  OBJECT_MANAGER_RPC_METHODS

#undef OBJECT_MANAGER_RPC_METHOD

  void Start() {
    std::weak_ptr<ObjectManagerBrpcClient> weak_this(shared_from_this());
    rpc_client_->SetOnConnectedCallback([this, weak_this]() {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      RAY_CHECK(rpc_client_->GetRpcClient());

#define OBJECT_MANAGER_RPC_METHOD(METHOD) OBJECT_MANAGER_SEND_PENDING_RPC_REQUESTS(METHOD)

      OBJECT_MANAGER_RPC_METHODS

#undef OBJECT_MANAGER_RPC_METHOD

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

  void ClearFailRequests() {
    if (reply_service_.stopped()) {
      return;
    }
#define OBJECT_MANAGER_RPC_METHOD(METHOD) \
  OBJECT_MANAGER_FAIL_PENDING_RPC_REQUESTS(METHOD, METHOD)

    OBJECT_MANAGER_RPC_METHODS

#undef OBJECT_MANAGER_RPC_METHOD
  }

  void Close() {
    ClearFailRequests();
    rpc_client_->Close();
  }

  ~ObjectManagerBrpcClient() { Close(); }

 private:

#define OBJECT_MANAGER_RPC_METHOD(METHOD)                               \
  OBJECT_MANAGER_VOID_RPC_CLIENT_METHOD(ObjectManagerService, METHOD, )

  OBJECT_MANAGER_RPC_METHODS

#undef OBJECT_MANAGER_RPC_METHOD

  instrumented_io_context &request_service_;

  instrumented_io_context &reply_service_;

  std::shared_ptr<ReconnectableRpcClient<BrpcStreamClient>> rpc_client_;

  std::function<void()> on_connected_;

  absl::Mutex pending_request_mutex_;

#define OBJECT_MANAGER_RPC_METHOD(METHOD)                                \
  std::vector<std::pair<METHOD##Request, ClientCallback<METHOD##Reply>>> \
      pending_##METHOD##_requests_ GUARDED_BY(pending_request_mutex_);

  OBJECT_MANAGER_RPC_METHODS
#undef OBJECT_MANAGER_RPC_METHOD

};

/// Client used for communicating with a remote node manager server.
class ObjectManagerBrpcClients : public std::enable_shared_from_this<ObjectManagerBrpcClients> {
 public:
  /// Constructor.
  ///
  /// \param[in] ip_address Address of the node manager server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  ObjectManagerBrpcClients(const std::string &ip_address, const int port,
                      ClientCallManager &client_call_manager, int num_connections = 4)
      : num_connections_(num_connections) {
#define OBJECT_MANAGER_RPC_METHOD(METHOD)           \
  METHOD##_rr_index_ = rand() % num_connections_;

  OBJECT_MANAGER_RPC_METHODS
#undef OBJECT_MANAGER_RPC_METHOD
    rpc_clients_.reserve(num_connections_);
    auto async_rpc_client_creater =
        std::bind(rpc::BrpcStreamClient::AsyncCreateRpcClient,
                  brpcpb::StreamServiceType::ObjectManagerServiceType, std::placeholders::_1,
                  std::placeholders::_2, std::ref(client_call_manager.ReplyService()),
                  std::placeholders::_3);
    for (int i = 0; i < num_connections_; i++) {
      rpc_clients_.emplace_back(
        std::make_shared<ObjectManagerBrpcClient>(
          ip_address, port,
          async_rpc_client_creater, client_call_manager,
          /*infinite_reconnect=*/false
      ));
      rpc_clients_[i]->Start();
    }
    RAY_LOG(INFO) << "create clients, with connections number: " << num_connections_
                  << ", address:" << ip_address << ":" << port;
  };

#define OBJECT_MANAGER_WRAP_CALLBACK_CODE_SNIPPET(REPLY_TYPE)                            \
  ClientCallback<REPLY_TYPE> wrapped_callback = nullptr;                                 \
  if (callback) {                                                                        \
    /* NOTE(kfstorm): This is to avoid request not been sent when an RPC client is       \
     * out-of-scope right after invoking an RPC method. */                               \
    wrapped_callback =                                                                   \
        [shared_this = shared_from_this(), callback = std::move(callback)](              \
            const Status &status, const REPLY_TYPE &reply) { callback(status, reply); }; \
  }

#define OBJECT_MANAGER_RPC_METHOD(METHOD)                                                                \
  void METHOD(const METHOD##Request &request,                                                            \
              const ClientCallback<METHOD##Reply> &callback) {                                           \
    OBJECT_MANAGER_WRAP_CALLBACK_CODE_SNIPPET(METHOD##Reply)                                             \
    rpc_clients_[METHOD##_rr_index_++ % num_connections_]->METHOD(request, std::move(wrapped_callback)); \
  }

  OBJECT_MANAGER_RPC_METHODS

#undef OBJECT_MANAGER_RPC_METHOD

 private:
  /// To optimize object manager performance we create multiple concurrent
  /// GRPC connections, and use these connections in a round-robin way.
  int num_connections_;

#define OBJECT_MANAGER_RPC_METHOD(METHOD)         \
  std::atomic<unsigned int> METHOD##_rr_index_;

  OBJECT_MANAGER_RPC_METHODS
#undef OBJECT_MANAGER_RPC_METHOD

  /// The RPC clients.
  std::vector<std::shared_ptr<ObjectManagerBrpcClient>> rpc_clients_;
};

}  // namespace rpc
}  // namespace ray
