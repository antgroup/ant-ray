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

#ifndef RAY_EVENT_CLIENT_H
#define RAY_EVENT_CLIENT_H

#include "ray/rpc/client_call.h"
#include "ray/rpc/grpc_client.h"
#include "src/ray/protobuf/event.grpc.pb.h"

namespace ray {
namespace rpc {

/// Abstract client interface for testing.
class EventClientInterface {
 public:
  virtual void CheckJavaHsErrLog(
      const CheckJavaHsErrLogRequest &request,
      const ClientCallback<CheckJavaHsErrLogReply> &callback){};
  virtual ~EventClientInterface(){};
};

/// Client used for communicating with a remote event server(in agent).
class EventClient : public EventClientInterface {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the event server.
  /// \param[in] port Port of the event server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EventClient(const std::string &address, const int port,
              ClientCallManager &client_call_manager)
      : client_call_manager_(client_call_manager) {
    grpc_client_.reset(new GrpcClient<EventService>(address, port, client_call_manager_));
  };

  /// Check Java hs err log.
  ///
  /// \param request The request message
  /// \param callback  The callback function that handles reply
  VOID_RPC_CLIENT_METHOD(EventService, CheckJavaHsErrLog, grpc_client_, )

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<EventService>> grpc_client_;

  /// The `ClientCallManager` used for managing requests.
  ClientCallManager &client_call_manager_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_EVENT_CLIENT_H
