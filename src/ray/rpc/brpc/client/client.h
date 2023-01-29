#ifndef RAY_RPC_BRPC_CLIENT_CLIENT_H
#define RAY_RPC_BRPC_CLIENT_CLIENT_H

#include <boost/asio.hpp>

#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"
#include "brpc/channel.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

using SharedBrpcChannel = std::pair<absl::Mutex, absl::optional<::brpc::Channel>>;

/// Base class that represents a BRPC client.
class BrpcClient {
 protected:
  BrpcClient(const std::string &address, const int port,
             instrumented_io_context &io_service);

  virtual ~BrpcClient();

  Status ConnectChannel(::brpc::Channel **channel);

  /// IP address of the server.
  const std::string address_;
  /// Port of the server.
  int port_;
  /// IO event loop to handle RPC replies.
  instrumented_io_context &reply_service_;

 private:
  static absl::Mutex cached_channels_mutex_;
  /// Map from ip:port to brpc channel. This is needed for connection sharing. All clients
  /// of the same remote address can share a single TCP connection. `weak_ptr` here is to
  /// make sure the shared channel can be destructed if no client needs it anymore.
  static std::unordered_map<std::string, std::weak_ptr<SharedBrpcChannel>>
      cached_channels_;

  std::shared_ptr<SharedBrpcChannel> channel_;
};

// NOTE(kfstorm): We put the definition here because there's no `bprc_common_lib`, only
// `brpc_client_lib` and `brpc_server_lib`. Currently all processes which use brpc depend
// on `brpc_client_lib`, so we put it in `brpc_client_lib`.
void ConfigureBrpcLogging();

}  // namespace rpc
}  // namespace ray

#endif
