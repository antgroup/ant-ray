#ifndef RAY_RPC_BRPC_SERVER_STREAM_SERVICE_H
#define RAY_RPC_BRPC_SERVER_STREAM_SERVICE_H

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <thread>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"

#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "src/ray/protobuf/brpc_stream.pb.h"
// for CoreWorkerServiceMessageType, can be removed later.
#include "ray/rpc/brpc/util.h"
#include "src/ray/protobuf/core_worker.pb.h"

// brpc related headers
#include "brpc/channel.h"
#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "butil/logging.h"

// Copied from src/ray/rpc/server_call.h
using SendReplyCallback = std::function<void(
    ray::Status status, std::function<void()> success, std::function<void()> failure)>;

/// This file implements the RPC service for brpc streaming. For more details, refer to:
/// https://github.com/apache/incubator-brpc/blob/master/docs/en/streaming_rpc.md

namespace ray {
namespace rpc {

/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using BrpcHandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                           SendReplyCallback);

class StreamServiceMethod;

using BrpcStreamServiceHandler =
    std::function<void(::brpc::StreamId stream_id, butil::IOBuf iobuf)>;

using StreamDisconnectCallback = std::function<void(const ::brpc::StreamId stream_id)>;

/// Brpc stream service.
///
/// Subclass should implement `InitMethodHandlers` to decide
/// which kinds of requests this service should accept.
class BrpcStreamService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which service handler functions
  /// will be posted.
  explicit BrpcStreamService(brpcpb::StreamServiceType service_type)
      : service_type_(service_type) {}

  /// Destruct this gRPC service.
  virtual ~BrpcStreamService() = default;

  brpcpb::StreamServiceType GetServiceType() const { return service_type_; }

  /// Subclasses should implement this method to initialize the `StreamServiceMethod`
  /// instances.
  ///
  /// \param[out] server_call_methods The `StreamServiceMethod` objects.
  virtual void InitMethodHandlers(
      std::vector<std::shared_ptr<StreamServiceMethod>> *server_call_methods) = 0;

 protected:
  /// RPC service type.
  brpcpb::StreamServiceType service_type_;
};

/// A method of a service that can be called.
class StreamServiceMethod {
 public:
  virtual ~StreamServiceMethod() {}

  /// Returns the type of the request for this method.
  virtual uint32_t GetRequestType() const = 0;

  /// Process a request of specific type from a client.
  ///
  /// \param client The client that sent the message.
  /// \param length The length of the message data.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  virtual void HandleRequest(::brpc::StreamId stream_id, butil::IOBuf iobuf) = 0;
};

// Implementation of `StreamServiceMethod`.
///
/// \tparam ServiceMessageHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
/// \tparam MessageType Enum type for request/reply message.
template <class ServiceMessageHandler, class Request, class Reply, class MessageType>
class StreamServiceMethodImpl : public StreamServiceMethod {
 public:
  /// Constructor.
  ///
  /// \param[in] service_type The type of the RPC service that this method belongs to.
  /// \param[in] request_type Enum message type for request of this method.
  /// \param[in] reply_type Enum message type for reply of this method.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  StreamServiceMethodImpl(brpcpb::StreamServiceType service_type,
                          MessageType request_type, MessageType reply_type,
                          ServiceMessageHandler &service_handler,
                          BrpcHandleRequestFunction<ServiceMessageHandler, Request, Reply>
                              handle_request_function,
                          boost::asio::io_service &io_service)
      : service_type_(service_type),
        request_type_(request_type),
        reply_type_(reply_type),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        io_service_(io_service) {}

  uint32_t GetRequestType() const override {
    return static_cast<uint32_t>(request_type_);
  }

  /// Process a request of this method from a client.
  ///
  /// \param client The client that sent the message.
  /// \param length The length of the message data.
  /// \param message_data A pointer to the message data.
  /// \return Void.
  void HandleRequest(::brpc::StreamId stream_id, butil::IOBuf iobuf) override;

 private:
  /// Enum type for the RPC service.
  brpcpb::StreamServiceType service_type_;
  /// Enum type for request message.
  MessageType request_type_;
  /// Enum type for reply message.
  MessageType reply_type_;
  /// The service handler that handles the request.
  ServiceMessageHandler &service_handler_;
  /// Pointer to the service handler function.
  BrpcHandleRequestFunction<ServiceMessageHandler, Request, Reply>
      handle_request_function_;
  /// IO service to (possibly) serialize and write replies.
  boost::asio::io_service &io_service_;
};

template <class ServiceMessageHandler, class Request, class Reply, class MessageType>
void StreamServiceMethodImpl<ServiceMessageHandler, Request, Reply,
                             MessageType>::HandleRequest(::brpc::StreamId stream_id,
                                                         butil::IOBuf iobuf) {
  uint32_t header_size;
  iobuf.cutn(&header_size, sizeof(header_size));

  std::string header_message;
  iobuf.cutn(&header_message, header_size);

  auto request_header =
      flatbuffers::GetRoot<brpcfb::StreamRpcRequestMeta>(header_message.data());

  const auto request_id = request_header->request_id();
  const bool requires_reply = request_header->is_reply_required();

  if (RAY_LOG_ENABLED(DEBUG)) {
    RAY_LOG(DEBUG) << "Received RPC request: "
                   << GetTraceInfoMessage(*request_header->trace_info());
  }

  brpcfb::TraceInfoT trace_info_object;
  request_header->trace_info()->UnPackTo(&trace_info_object);

  auto request = std::make_shared<Request>();
  butil::IOBufAsZeroCopyInputStream wrapper(iobuf);
  request->ParseFromZeroCopyStream(&wrapper);

  auto reply = std::make_shared<Reply>();

  RAY_LOG(INFO) << "Handle request for service " << StreamServiceType_Name(service_type_)
                 << ", request id: " << request_id
                 << ", request type: " << static_cast<int>(request_type_)
                 << ", trace info: " << GetTraceInfoMessage(*request_header->trace_info());
  (service_handler_.*handle_request_function_)(
      *request, reply.get(),
      [this, stream_id, request_id, request, reply, trace_info_object, requires_reply](
          Status status, std::function<void()> success, std::function<void()> failure) {
        RAY_LOG(DEBUG) << "Calling send reply callback for request " << request_id
                       << ", service: " << StreamServiceType_Name(service_type_);
        // NOTE(zhijunfu): We check if reply is required here in RPC, instead of
        // in upper layer code (e.g. core worker transport) because:
        // - Upper layer can always call `send_reply_callback` without the need
        //   to know about whether the reply should be sent, this is simpler and
        //   it's consistent with grpc unary mode;
        // - More importantly, we need to make sure the stuff captured by the callback
        //   can get released, otherwise we have a leak. Doing this check here so that
        //   we don't need to change the signature of `handle_rquest_function`.
        //   `send_reply_callback` should still be called by application even when
        //   reply is not required, in order to release the captured data structures.
        if (!requires_reply) {
          return;
        }

        auto send_reply_func = [trace_info_object, request_id, status, reply, stream_id,
                                success, failure, this] {
          // Create reply meta.
          flatbuffers::FlatBufferBuilder fbb;
          auto reply_meta = brpcfb::CreateStreamRpcReplyMeta(
              fbb, brpcfb::TraceInfo::Pack(fbb, &trace_info_object), request_id,
              static_cast<uint32_t>(status.code()), fbb.CreateString(status.message()));
          fbb.Finish(reply_meta);

          uint32_t message_type = static_cast<uint32_t>(reply_type_);
          // Serialize the message type, request meta, request to IOBuf.
          // For more details refer to src/ray/rpc/brpc/util.h.
          butil::IOBuf iobuf;
          SerializeRpcMessageToIOBuf(message_type, fbb, *reply, &iobuf);

          if (::brpc::StreamWrite(stream_id, iobuf) == 0) {
            if (success != nullptr) {
              success();
            }
            RAY_LOG(DEBUG) << "Sent RPC reply: "
                           << GetTraceInfoMessage(trace_info_object);
          } else {
            if (failure != nullptr) {
              failure();
            }
            RAY_LOG(WARNING) << "Failed to send RPC reply "
                             << GetTraceInfoMessage(trace_info_object);
          }
        };

        io_service_.dispatch([send_reply_func] { send_reply_func(); });
      });
}

}  // namespace rpc
}  // namespace ray

#endif
