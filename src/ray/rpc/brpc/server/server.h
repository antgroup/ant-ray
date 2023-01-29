#ifndef RAY_RPC_BRPC_SERVER_SERVER_H
#define RAY_RPC_BRPC_SERVER_SERVER_H

#include <boost/optional/optional.hpp>
#include "absl/synchronization/mutex.h"

#include "ray/rpc/brpc/util.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/brpc_stream.pb.h"

// brpc related headers
#include <brpc/server.h>
#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "butil/logging.h"

#include "ray/rpc/brpc/server/stream_service.h"

namespace ray {
namespace rpc {

#define BRPC_SERVICE_HANDLER(SERVICE, HANDLER)                                        \
  std::shared_ptr<StreamServiceMethod> HANDLER##_call_method(                         \
      new StreamServiceMethodImpl<SERVICE##Handler, HANDLER##Request, HANDLER##Reply, \
                                  SERVICE##MessageType>(                              \
          service_type_, SERVICE##MessageType::HANDLER##RequestMessage,               \
          SERVICE##MessageType::HANDLER##ReplyMessage, service_handler_,              \
          &SERVICE##Handler::Handle##HANDLER, io_service_));                          \
  server_call_methods->emplace_back(std::move(HANDLER##_call_method));

/// Class that represents a stream creation service.
///
/// There's only one `StreamCreationService` in a `BrpcServer`, it provides a method
/// `CreateStream` which can create a brpc stream after receiving a `CreateStreamRequest`.
/// Multiple brpc stream services can register their perspective types and handlers to
/// this service.
class BrpcStreamCreationService
    : public brpcpb::StreamCreationService,
      public std::enable_shared_from_this<BrpcStreamCreationService> {
 public:
  void RegisterStreamService(BrpcStreamService &service);

  virtual void CreateStream(google::protobuf::RpcController *controller,
                            const brpcpb::CreateStreamRequest *request,
                            brpcpb::CreateStreamReply *response,
                            google::protobuf::Closure *done);

 private:
  /// Map from the rpc service type to the handler function for the requests from
  /// this service. This is immutable after brpc server starts.
  EnumUnorderedMap<brpcpb::StreamServiceType, BrpcStreamServiceHandler> service_handlers_;
};

/// Class that represents a brpc server.
///
/// A BrpcServer listens on a specific port, and supports multiple brpc services.
/// Stream rpc services are registered via `RegisterStreamService`.
class BrpcServer {
 public:
  /// Construct a rpc server that listens on a TCP port.
  ///
  /// \param[in] name Name of this server, used for logging and debugging purpose.
  /// \param[in] port The port to bind this server to. If it's 0, a random available port
  ///  will be chosen.
  /// \param[in] io_service The io service to process requests.
  BrpcServer(std::string name, const uint32_t port)
      : name_(std::move(name)), port_(port), is_closed_(false) {
    gflags::SetCommandLineOption(
        "max_body_size",
        std::to_string(RayConfig::instance().brpc_max_body_size()).data());
  }

  BrpcServer(std::string name, const uint32_t port, int num_threads)
      : name_(std::move(name)), port_(port), is_closed_(false) {
    (void)num_threads;
    gflags::SetCommandLineOption(
        "max_body_size",
        std::to_string(RayConfig::instance().brpc_max_body_size()).data());
  }

  /// Destruct this brpc server.
  ~BrpcServer() { Shutdown(); }

  /// Initialize and run this server.
  void Run();

  /// Register a brpc stream service. Multiple services can be registered to the same
  /// server.
  ///
  /// \param[in] service An `BrpcStreamService` to register to this server.
  void RegisterStreamService(BrpcStreamService &service);

  /// Shutdown this server
  void Shutdown();

  /// Get the port of this RPC server.
  int GetPort() const { return port_; }

 protected:
  /// Name of this server, used for logging and debugging purpose.
  const std::string name_;
  /// Port of this server.
  int port_;
  /// Indicates whether this server has been closed.
  std::atomic<bool> is_closed_;

  ::brpc::Server server_;

  std::shared_ptr<BrpcStreamCreationService> stream_server_;
};

/// Implementation for request handling for BRPC stream.
class BrpcStreamRequestHandler : public ::brpc::StreamInputHandler {
 public:
  BrpcStreamRequestHandler(
      BrpcStreamServiceHandler on_request_received,
      std::shared_ptr<BrpcStreamCreationService> stream_creation_service)
      : on_request_received_(on_request_received),
        stream_creation_service_(stream_creation_service) {}

  virtual ~BrpcStreamRequestHandler() {
    RAY_LOG(INFO) << "BrpcStreamRequestHandler for stream " << stream_id_
                  << " is destructed";
  }

  virtual int on_received_messages(::brpc::StreamId id, butil::IOBuf *const messages[],
                                   size_t size) {
    RAY_CHECK(id == stream_id_);
    RAY_LOG(DEBUG) << "received " << size << " messages from stream " << id;
    for (size_t i = 0; i < size; ++i) {
      // NOTE(zhijunfu): it's ok to pass the pointer of `IOBuf`
      // as we don't post to other IO services fo async processing.
      // If later we do, this needs to be changed to passing `IObuf`
      // instead which requires a copy, that's also fine since
      // `IOBuf` supports reference counting, and thus this doesn't
      // copy the actual data.
      //
      // TODO(zhijunfu): IOBuf has a limitation that it takes a 8K
      // block, thus its lifetime should be short, aka we shouldn't
      // cache too many `IOBuf` structs. We should revisit this and
      // possibly do the actual data copy for small objects.
      // https://github.com/apache/incubator-brpc/blob/master/docs/cn/iobuf.md
      on_request_received_(id, *messages[i]);
    }

    return 0;
  }

  virtual void on_idle_timeout(::brpc::StreamId id) {
    RAY_CHECK(id == stream_id_);
    RAY_LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
  }

  virtual void on_closed(::brpc::StreamId id) {
    RAY_CHECK(id == stream_id_);
    RAY_LOG(INFO) << "Stream=" << id << " is closed";
  }

  void Init(::brpc::StreamId stream_id) { stream_id_ = stream_id; }

 private:
  ::brpc::StreamId stream_id_ = ::brpc::INVALID_STREAM_ID;

  BrpcStreamServiceHandler on_request_received_;
  /// Hold the reference of BrpcStreamCreationService so that it's always valid
  /// when on_closed is called by brpc.
  std::shared_ptr<BrpcStreamCreationService> stream_creation_service_;
};

}  // namespace rpc
}  // namespace ray

#endif
