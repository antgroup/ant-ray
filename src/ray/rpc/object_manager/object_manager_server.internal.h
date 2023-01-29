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
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"
#include "ray/rpc/brpc/server/server.h"
#include "ray/rpc/brpc/server/stream_service.h"
#include "src/ray/protobuf/object_manager.grpc.pb.h"
#include "src/ray/protobuf/object_manager.pb.h"

namespace ray {
namespace rpc {

#define DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD DECLARE_VOID_GRPC_SERVICE_HANDLER_METHOD

#define RAY_OBJECT_MANAGER_BRPC_HANDLERS                   \
  BRPC_SERVICE_HANDLER(ObjectManagerService, Push)        \
  BRPC_SERVICE_HANDLER(ObjectManagerService, Pull)        \
  BRPC_SERVICE_HANDLER(ObjectManagerService, FreeObjects)

#define RAY_OBJECT_MANAGER_DECLARE_RPC_HANDLERS        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(Push)        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(Pull)        \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(FreeObjects)

/// Implementations of the `ObjectManagerBrpcStreamService`, check interface in
/// `src/ray/protobuf/object_manager.proto`.
class ObjectManagerServiceHandler {
 public:
  RAY_OBJECT_MANAGER_DECLARE_RPC_HANDLERS
};

/// The `BrpcService` for `ObjectManagerBrpcStreamService`.
class ObjectManagerBrpcStreamService : public BrpcStreamService {
 public:
  /// Construct a `ObjectManagerBrpcStreamService`.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  ObjectManagerBrpcStreamService(instrumented_io_context &io_service,
                                 ObjectManagerServiceHandler &service_handler)
      : BrpcStreamService(brpcpb::StreamServiceType::ObjectManagerServiceType),
        io_service_(io_service),
        service_handler_(service_handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<StreamServiceMethod>> *server_call_methods) override {
    RAY_OBJECT_MANAGER_BRPC_HANDLERS
  }

 private:
  boost::asio::io_service &io_service_;
  /// The grpc async service object.
  // ObjectManagerService::AsyncService service_;
  /// The service handler that actually handle the requests.
  ObjectManagerServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray
