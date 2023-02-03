#ifndef RAY_RPC_BRPC_CLIENT_STREAM_CLIENT_H
#define RAY_RPC_BRPC_CLIENT_STREAM_CLIENT_H

#include <atomic>
#include <thread>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"

#include "ray/util/util.h"
// for CoreWorkerServiceMessageType, can be removed later.
#include "ray/rpc/brpc/client/client.h"
#include "ray/rpc/brpc/util.h"
#include "ray/rpc/client_call.h"
#include "src/ray/protobuf/core_worker.pb.h"

// brpc related headers
#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "butil/logging.h"

/// This file implements the RPC client for brpc streaming. For more details, refer to:
/// https://github.com/apache/incubator-brpc/blob/master/docs/en/streaming_rpc.md

namespace ray {
namespace rpc {

using DisconnectCallback = std::function<void()>;

// This macro wraps the logic to call a specific RPC method of a service,
// to make it easier to implement a new RPC client.
#define INVOKE_BRPC_CALL(SERVICE, METHOD, request, callback, rpc_client, tag)     \
  ({                                                                              \
    rpc_client->CallMethod<METHOD##Request, METHOD##Reply, SERVICE##MessageType>( \
        SERVICE##MessageType::METHOD##RequestMessage,                             \
        SERVICE##MessageType::METHOD##RequestMessage, request, callback, tag);    \
  })

using ReplyCallback =
    std::function<void(const brpcfb::StreamRpcReplyMeta *, butil::IOBuf *)>;

class BrpcAsyncCall {
 public:
  BrpcAsyncCall(std::function<void(const ray::Status &)> reply_callback)
      : reply_callback_(reply_callback) {}

  std::function<void(const ray::Status &)> reply_callback_;
};

/// This data structure is used to share a brpc channel for multiple clients of the same
/// remote.
using SharedBrpcChannel = std::pair<absl::Mutex, absl::optional<::brpc::Channel>>;

/// Class that represents a BRPC stream client.
class BrpcStreamClient : public BrpcClient,
                         public std::enable_shared_from_this<BrpcStreamClient> {
 public:
  virtual ~BrpcStreamClient() {
    RAY_LOG(DEBUG) << "BrpcStreamClient " << this
                   << " is destructed, stream id: " << stream_id_;
    // Invoke all the callbacks that are pending replies, this is necessary so that
    // the transport can put exceptions into store for these object ids, to avoid
    // the client from getting blocked on `ray.get`.
    InvokeAndClearPendingCallbacks();

    is_connected_ = false;
    if (stream_id_ != ::brpc::INVALID_STREAM_ID) {
      ::brpc::StreamClose(stream_id_);
    }
  }

  static void AsyncCreateRpcClient(
      brpcpb::StreamServiceType service_type, const std::string &address, const int port,
      instrumented_io_context &io_service,
      std::function<void(const ray::Status &status,
                         std::shared_ptr<BrpcStreamClient> rpc_client)>
          on_created) {
    // Note that std::make_shared cannot be used here because of the constructor of
    // BrpcStreamClient is protected.
    auto rpc_client = std::shared_ptr<BrpcStreamClient>(
        new BrpcStreamClient(service_type, address, port, io_service));
    // Async connect to remote endpoint, and invoke the callback to pass the created RPC
    // client to user when connect finishes successfully. We ensure that after the client
    // is returned to user it's immediately usable so we don't need to maintain a list of
    // pendings tasks in the RPC layer.
    RAY_UNUSED(
        rpc_client->AsyncConnect([rpc_client, on_created](const ray::Status &status) {
          on_created(status, status.ok() ? rpc_client : nullptr);
        }));
  }

 protected:
  explicit BrpcStreamClient(brpcpb::StreamServiceType service_type,
                            const std::string &address, const int port,
                            instrumented_io_context &reply_service)
      : BrpcClient(address, port, reply_service),
        service_type_(service_type),
        name_(StreamServiceType_Name(service_type)),
        request_id_(0),
        is_connected_(false) {
    // Generate a 32 bit random tag.
    std::string buffer(sizeof(uint32_t), 0);
    FillRandom(&buffer);
    uint32_t tag = *reinterpret_cast<const uint32_t *>(buffer.data());

    // Set the high 32 bit of trace id to the random tag.
    trace_id_ = static_cast<uint64_t>(tag) << 32;
  }

  /// Asynchronously connect to a remote server.
  ///
  /// NOTE(zhijunfu): This doesn't necessary establish a TCP connection to the server,
  /// but instead setup a stream to a brpc server. Multiple streams to the same address
  /// in the same process automatically share the same connection with brpc.
  /// For more details, refer to:
  /// https://github.com/apache/incubator-brpc/blob/master/docs/en/streaming_rpc.md
  ///
  /// \param[in] handler The callback function to be invoked after the stream
  ///            is established.
  void AsyncConnect(std::function<void(const ray::Status &)> on_connected);

 public:
  /// Call a service method.
  ///
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  /// \tparam MessageType Enum type for request/reply message.
  ///
  /// \param[in] request_type Enum message type for request of this method.
  /// \param[in] reply_type Enum message type for reply of this method.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///            If this is nullptr then reply is not required.
  ///
  /// \return Void.
  template <class Request, class Reply, class MessageType>
  void CallMethod(MessageType request_type, MessageType reply_type,
                  const Request &request, ClientCallback<Reply> &callback,
                  const std::string &method_name);

  /// Set a callback function which will be called when we receive a `DisconnectClient`
  /// message from server.
  void SetDisconnectCallback(DisconnectCallback callback) {
    disconnect_callback_ = callback;
  }

 protected:
  void HandleStreamConnected(::brpc::Controller *cntl, BrpcAsyncCall *call,
                             brpcpb::CreateStreamReply *reply) {
    // std::unique_ptr makes sure cntl/response will be deleted before returning.
    std::unique_ptr<::brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<brpcpb::CreateStreamReply> response_guard(reply);
    std::unique_ptr<BrpcAsyncCall> call_guard(call);

    if (cntl->Failed()) {
      RAY_LOG(WARNING) << "Fail to create stream " << stream_id_ << ", "
                       << cntl->ErrorText();
      ::brpc::StreamClose(stream_id_);
      call->reply_callback_(ray::Status::IOError("Failed to send request"));
      return;
    }

    is_connected_ = true;

    RAY_LOG(INFO) << "Sucessfully create brpc stream " << stream_id_ << " to "
                  << cntl->remote_side();
    call->reply_callback_(ray::Status::OK());
  }

  void AddPendingCallback(uint64_t request_id, ReplyCallback reply_callback);

  bool RemovePendingCallback(uint64_t request_id);

  void HandleReplyReceived(butil::IOBuf *const message);

  void InvokeAndClearPendingCallbacks();

  void HandleServerDisconnected();

  /// Type of the RPC service.
  const brpcpb::StreamServiceType service_type_;
  /// Name of this client, used for logging and debugging purpose.
  const std::string name_;

  /// Request sequence id which starts with 1.
  std::atomic<uint64_t> request_id_;

  /// Trace ID.
  std::atomic<uint64_t> trace_id_;

  ::brpc::StreamId stream_id_ = ::brpc::INVALID_STREAM_ID;

  ///
  /// Connection status.
  ///
  /// Whether we have connected to server.
  std::atomic<bool> is_connected_;

  /// The callback to be invoken when server is disconnected.
  DisconnectCallback disconnect_callback_;

  /// Map from request id to the corresponding reply callback, which will be
  /// invoked when the reply is received for the request.
  std::unordered_map<uint64_t, ReplyCallback> pending_callbacks_;
  /// Mutex to protect the `pending_callbacks_` above.
  std::mutex callback_mutex_;

  friend class BrpcStreamReplyHandler;
};

template <class Request, class Reply, class MessageType>
void BrpcStreamClient::CallMethod(MessageType request_type, MessageType reply_type,
                                  const Request &request, ClientCallback<Reply> &callback,
                                  const std::string &method_name) {
  ClientCallback<Reply> wrapped_callback;
  if (callback) {
    auto weak_this = std::weak_ptr<BrpcStreamClient>(shared_from_this());
    wrapped_callback = [this, weak_this, callback](const ray::Status &status,
                                                   const Reply &reply) {
      auto shared_this = weak_this.lock();
      if (!shared_this) {
        return;
      }
      if (callback && !reply_service_.stopped()) {
        reply_service_.post(std::bind(
            [status, reply](ClientCallback<Reply> &callback) { callback(status, reply); },
            std::move(callback)));
      }
    };
  }
  if (!is_connected_) {
    // There are errors, invoke the callback.
    Reply reply;
    if (wrapped_callback) {
      wrapped_callback(Status::IOError("server is not connected"), reply);
    }
    return;
  }

  // We rely on whether callback is specified to determine if reply is required.
  bool requires_reply = (wrapped_callback != nullptr);
  uint64_t request_id = ++request_id_;

  // NOTE(zhijunfu): We MUST add the reply callback to the map before
  // calling `brpc::StreamWrite`, because otherwise it's possible that we receive
  // the reply before adding the reply callback, then we won't be able to find
  // the callback in `HandleReplyReceived` and will crash.

  // NOTE(zhijunfu): There are a few places that a reply callback can be invoked:
  // - when the reply for a request is received, this is the usual case;
  // - when the upper layer detects the server disconnection, e.g. direct actor
  //   transport detects that via actor state update, and in this case it invokes
  //   all the pending callbacks with failure status, and removes RPC client;
  // - when BRPC detects the connection to server is disconnected, in this case
  //   `on_closed` callback in BrpcStreamReplyHandler is called, where all the
  //    pending callbacks are invoked with failure status.
  // For the last case we need to hold shared_ptr of this class to make sure
  // it's still valid when the callbacks are invokved.
  auto this_ptr = this->shared_from_this();
  if (requires_reply) {
    std::string name = name_;
    // Add the request to the records, so that
    // we can invoke the callback after receiving the reply.
    AddPendingCallback(request_id, [wrapped_callback, reply_type, name, this_ptr](
                                       const brpcfb::StreamRpcReplyMeta *reply_meta,
                                       butil::IOBuf *iobuf) {
      const auto request_id = reply_meta->request_id();
      auto error_code = static_cast<StatusCode>(reply_meta->error_code());
      auto error_message = string_from_flatbuf(*reply_meta->error_message());
      Status status = (error_code == StatusCode::OK) ? Status::OK()
                                                     : Status(error_code, error_message);

      Reply reply;

      if (status.ok()) {
        RAY_CHECK(iobuf != nullptr);
        // Deserialize IOBuf to the actual protobuf.
        butil::IOBufAsZeroCopyInputStream wrapper(*iobuf);
        reply.ParseFromZeroCopyStream(&wrapper);
      }

      RAY_LOG(DEBUG) << "Calling reply callback for message "
                     << static_cast<int>(reply_type) << " for service " << name
                     << ", request id " << request_id
                     << ", status: " << status.ToString();

      wrapped_callback(status, reply);
    });
  }

  // Create request meta.
  flatbuffers::FlatBufferBuilder fbb;
  auto trace_info = brpcfb::CreateTraceInfo(
      fbb, ++trace_id_, fbb.CreateString(brpcpb::StreamServiceType_Name(service_type_)),
      fbb.CreateString(method_name), fbb.CreateString(address_), port_, request_id);

  auto request_header =
      brpcfb::CreateStreamRpcRequestMeta(fbb, trace_info, request_id, requires_reply);
  fbb.Finish(request_header);

  uint32_t message_type = static_cast<uint32_t>(request_type);

  // Serialize the message type, request meta, request to IOBuf.
  // For more details refer to src/ray/rpc/brpc/util.h.
  butil::IOBuf iobuf;
  SerializeRpcMessageToIOBuf(message_type, fbb, request, &iobuf);

  // Only generate trace info string when necessary.
  std::string trace_log;
  if (RAY_LOG_ENABLED(DEBUG)) {
    auto message =
        flatbuffers::GetRoot<brpcfb::StreamRpcRequestMeta>(fbb.GetBufferPointer());
    trace_log = GetTraceInfoMessage(*message->trace_info());
  }

  RAY_LOG(INFO) << "Calling method for service " << name_
                 << ", request id: " << request_id
                 << ", request type: " << static_cast<int>(request_type);

  int brpc_errno = ::brpc::StreamWrite(stream_id_, iobuf);
  if (brpc_errno != 0) {
    RAY_LOG(WARNING) << "Failed to write request message "
                     << static_cast<int>(request_type) << " for service " << name_
                     << " to " << address_ << ":" << port_ << ", request id "
                     << request_id << ", method name: " << method_name
                     << ", brpc error number: " << brpc_errno;
    auto status = Status::IOError("Failed to write stream");
    if (wrapped_callback != nullptr) {
      if (!RemovePendingCallback(request_id)) {
        // If failed to remove the pending callback, it means it have been invoked.
        return;
      }
      Reply reply;
      wrapped_callback(status, reply);
    }
  } else {
    RAY_LOG(INFO) << "Sent RPC request " << trace_log;
  }
}

/// Implementation for reply handling for BRPC stream.
class BrpcStreamReplyHandler : public ::brpc::StreamInputHandler {
 public:
  BrpcStreamReplyHandler(std::shared_ptr<BrpcStreamClient> stream_client)
      : stream_client_(stream_client) {}

  virtual ~BrpcStreamReplyHandler() {
    auto stream_client = stream_client_.lock();
    if (!stream_client) {
      return;
    }
    RAY_LOG(INFO) << "BrpcStreamReplyHandler for stream " << stream_client->stream_id_
                  << " is destructed";
  }

  int on_received_messages(::brpc::StreamId id, butil::IOBuf *const messages[],
                           size_t size) override {
    auto stream_client = stream_client_.lock();
    if (!stream_client) {
      return 0;
    }

    for (size_t i = 0; i < size; ++i) {
      // NOTE(zhijunfu): it's ok to pass the pointer of `IOBuf`
      // as we don't post to other IO services fo async processing.
      // If later we do, this needs to be changed to passing `IObuf`
      // instead which requires a copy, that's also fine since
      // `IOBuf` supports reference counting, and thus this doesn't
      // copy the actual data.
      //
      // NOTE(zhijunfu): IOBuf has a limitation that it takes a 8K
      // block, thus its lifetime should be short, aka we shouldn't
      // cache too many `IOBuf` structs. We can revisit this and
      // possibly do the actual data copy for small objects.
      // https://github.com/apache/incubator-brpc/blob/master/docs/cn/iobuf.md

      stream_client->HandleReplyReceived(messages[i]);
    }

    return 0;
  }

  void on_idle_timeout(::brpc::StreamId id) override {
    RAY_LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
  }

  void on_closed(::brpc::StreamId id) override {
    RAY_LOG(DEBUG) << "Stream=" << id << " is closed";
    auto stream_client = stream_client_.lock();
    if (!stream_client) {
      return;
    }
    stream_client->HandleServerDisconnected();
  }

 private:
  // Hold the reference of BrpcStreamClient so that it's always valid when on_closed
  // is called by brpc.
  std::weak_ptr<BrpcStreamClient> stream_client_;
};

}  // namespace rpc
}  // namespace ray

#endif
