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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/rpc/brpc/server/server.h"
#include "ray/rpc/brpc/server/stream_service.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

namespace ray {

class CoreWorker;

namespace rpc {

#define DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD DECLARE_VOID_GRPC_SERVICE_HANDLER_METHOD

/// NOTE: See src/ray/core_worker/core_worker.h on how to add a new grpc handler.

#define RAY_CORE_WORKER_RPC_HANDLERS                                      \
  BRPC_SERVICE_HANDLER(CoreWorkerService, PushTask)                       \
  BRPC_SERVICE_HANDLER(CoreWorkerService, DirectActorCallArgWaitComplete) \
  BRPC_SERVICE_HANDLER(CoreWorkerService, GetObjectStatus)                \
  BRPC_SERVICE_HANDLER(CoreWorkerService, WaitForActorOutOfScope)         \
  BRPC_SERVICE_HANDLER(CoreWorkerService, SubscribeForObjectEviction)     \
  BRPC_SERVICE_HANDLER(CoreWorkerService, PubsubLongPolling)              \
  BRPC_SERVICE_HANDLER(CoreWorkerService, WaitForRefRemoved)              \
  BRPC_SERVICE_HANDLER(CoreWorkerService, AddObjectLocationOwner)         \
  BRPC_SERVICE_HANDLER(CoreWorkerService, RemoveObjectLocationOwner)      \
  BRPC_SERVICE_HANDLER(CoreWorkerService, GetObjectLocationsOwner)        \
  BRPC_SERVICE_HANDLER(CoreWorkerService, KillActor)                      \
  BRPC_SERVICE_HANDLER(CoreWorkerService, CancelTask)                     \
  BRPC_SERVICE_HANDLER(CoreWorkerService, RemoteCancelTask)               \
  BRPC_SERVICE_HANDLER(CoreWorkerService, GetCoreWorkerStats)             \
  BRPC_SERVICE_HANDLER(CoreWorkerService, LocalGC)                        \
  BRPC_SERVICE_HANDLER(CoreWorkerService, SpillObjects)                   \
  BRPC_SERVICE_HANDLER(CoreWorkerService, RestoreSpilledObjects)          \
  BRPC_SERVICE_HANDLER(CoreWorkerService, DeleteSpilledObjects)           \
  BRPC_SERVICE_HANDLER(CoreWorkerService, AddSpilledUrl)                  \
  BRPC_SERVICE_HANDLER(CoreWorkerService, PlasmaObjectReady)              \
  BRPC_SERVICE_HANDLER(CoreWorkerService, RunOnUtilWorker)                \
  BRPC_SERVICE_HANDLER(CoreWorkerService, Exit)                           \
  BRPC_SERVICE_HANDLER(CoreWorkerService, BatchAssignObjectOwner)

#define RAY_CORE_WORKER_DECLARE_RPC_HANDLERS                              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PushTask)                       \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DirectActorCallArgWaitComplete) \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectStatus)                \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(WaitForActorOutOfScope)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(SubscribeForObjectEviction)     \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PubsubLongPolling)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(WaitForRefRemoved)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddObjectLocationOwner)         \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RemoveObjectLocationOwner)      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetObjectLocationsOwner)        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(KillActor)                      \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(CancelTask)                     \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RemoteCancelTask)               \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(GetCoreWorkerStats)             \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(LocalGC)                        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(SpillObjects)                   \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RestoreSpilledObjects)          \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(DeleteSpilledObjects)           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(AddSpilledUrl)                  \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(PlasmaObjectReady)              \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(RunOnUtilWorker)                \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(Exit)                           \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(BatchAssignObjectOwner)

/// Interface of the `CoreWorkerServiceHandler`, see `src/ray/protobuf/core_worker.proto`.
class CoreWorkerServiceHandler {
 public:
  virtual ~CoreWorkerServiceHandler() {}
  /// Handlers. For all of the following handlers, the implementations can
  /// handle the request asynchronously. When handling is done, the
  /// `send_reply_callback` should be called. See
  /// src/ray/rpc/node_manager/node_manager_client.h and
  /// src/ray/protobuf/node_manager.proto for a description of the
  /// functionality of each handler. ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  RAY_CORE_WORKER_DECLARE_RPC_HANDLERS
};

#if CORE_WORKER_USE_GRPC
/// The `GrpcServer` for `CoreWorkerService`.
class CoreWorkerGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  CoreWorkerGrpcService(instrumented_io_context &main_service,
                        CoreWorkerServiceHandler &service_handler)
      : GrpcService(main_service), service_handler_(service_handler) {}

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::unique_ptr<ServerCallFactory>> *server_call_factories) override {
    RAY_CORE_WORKER_RPC_HANDLERS
  }

 private:
  /// The grpc async service object.
  CoreWorkerService::AsyncService service_;

  /// The service handler that actually handles the requests.
  CoreWorkerServiceHandler &service_handler_;
};
#else
/// The `BrpcStreamService` for `CoreWorkerService`.
class CoreWorkerBrpcStreamService : public BrpcStreamService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  CoreWorkerBrpcStreamService(boost::asio::io_service &io_service,
                              CoreWorkerServiceHandler &service_handler)
      : BrpcStreamService(brpcpb::StreamServiceType::CoreWorkerServiceType),
        io_service_(io_service),
        service_handler_(service_handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<StreamServiceMethod>> *server_call_methods) override {
    RAY_CORE_WORKER_RPC_HANDLERS
  }

 private:
  boost::asio::io_service &io_service_;

  /// The service handler that actually handle the requests.
  CoreWorkerServiceHandler &service_handler_;
};

#endif

#undef DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD

}  // namespace rpc
}  // namespace ray
