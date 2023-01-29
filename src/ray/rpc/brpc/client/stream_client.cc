#include <sys/types.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <thread>
#include <utility>

#include "ray/rpc/brpc/client/stream_client.h"

namespace ray {
namespace rpc {

void BrpcStreamClient::AsyncConnect(
    std::function<void(const ray::Status &)> on_connected) {
  ::brpc::Channel *channel;
  auto status = ConnectChannel(&channel);
  if (!status.ok()) {
    on_connected(status);
    return;
  }

  // Normally, you should not call a Channel directly, but instead construct
  // a stub Service wrapping it. stub can be shared by all threads as well.
  brpcpb::StreamCreationService_Stub stub(channel);

  ::brpc::Controller *cntl = new ::brpc::Controller();
  ::brpc::StreamOptions stream_options;
  stream_options.max_buf_size = 0;
  // NOTE(zhijunfu): reply handler will be freed by brpc when the specific stream is
  // destructed, and `handler_owned_by_brpc` in StreamOptions must be set to make this
  // happen.
  stream_options.handler = new BrpcStreamReplyHandler(shared_from_this());
  // Set this to true so that brpc will delete the handler when the stream is destroyed.
  stream_options.handler_owned_by_brpc = true;

  RAY_CHECK(::brpc::StreamCreate(&stream_id_, *cntl, &stream_options) == 0)
      << "Failed to create stream to server " << address_ << ":" << port_;

  // Since we are sending asynchronous RPC (`done' is not NULL),
  // these objects MUST remain valid until `done' is called.
  // As a result, we allocate these objects on heap
  brpcpb::CreateStreamReply *reply = new brpcpb::CreateStreamReply();

  // Notice that you don't have to new request, which can be modified
  // or destroyed just after stub.CreateStream is called.
  brpcpb::CreateStreamRequest request;
  // TODO(zhijunfu): we can specify other information necessary when
  // setting up the stream. In addtional to adding the info to request,
  // another approach is to use request/response attachment supported
  // by brpc.
  request.set_service_type(service_type_);

  // Send a rpc call to create the stream.
  BrpcAsyncCall *call = new BrpcAsyncCall(on_connected);
  google::protobuf::Closure *done = ::brpc::NewCallback(
      this, &BrpcStreamClient::HandleStreamConnected, cntl, call, reply);
  stub.CreateStream(cntl, &request, reply, done);
}

void BrpcStreamClient::HandleServerDisconnected() {
  RAY_LOG(INFO) << "RPC server is disconnected, server: " << address_ << ":" << port_;

  // Mark is as not connected, so that further requests will be rejected with failure.
  is_connected_ = false;
  if (stream_id_ != ::brpc::INVALID_STREAM_ID) {
    ::brpc::StreamClose(stream_id_);
  }

  // Invoke all the callbacks that are pending replies, this is necessary so that
  // the transport can put exceptions into store for these object ids, to avoid
  // the client from getting blocked on `ray.get`.
  InvokeAndClearPendingCallbacks();

  // Invoke the disconnect callback if it's registered.
  if (disconnect_callback_ != nullptr) {
    disconnect_callback_();
  }
}

void BrpcStreamClient::InvokeAndClearPendingCallbacks() {
  std::unordered_map<uint64_t, ReplyCallback> pending_callbacks;

  {
    std::unique_lock<std::mutex> guard(callback_mutex_);
    pending_callbacks = std::move(pending_callbacks_);
    pending_callbacks_.clear();
  }

  // Invoke the callback outside of the mutex to avoid deadlock.
  for (const auto &entry : pending_callbacks) {
    // Call request callback in reply service to avoid deadlock.
    if (!reply_service_.stopped()) {
      auto weak_this = std::weak_ptr<BrpcStreamClient>(shared_from_this());
      reply_service_.post(
          [weak_this, request_id = entry.first, callback = std::move(entry.second)]() {
            auto shared_this = weak_this.lock();
            if (!shared_this) {
              return;
            }
            Status status = Status::IOError("rpc server disconnected");
            flatbuffers::FlatBufferBuilder fbb;
            auto request_header = brpcfb::CreateStreamRpcReplyMeta(
                fbb, /* trace_info */ 0, request_id, static_cast<uint32_t>(status.code()),
                fbb.CreateString(status.message()));
            fbb.Finish(request_header);

            auto message =
                flatbuffers::GetRoot<brpcfb::StreamRpcReplyMeta>(fbb.GetBufferPointer());
            callback(message, /* iobuf */ nullptr);
          });
    }
  }
}

void BrpcStreamClient::HandleReplyReceived(butil::IOBuf *iobuf) {
  // Deserialize reply.
  uint32_t message_type;
  std::string meta_string;
  DeserializeStreamRpcMessage(iobuf, &message_type, &meta_string);

  auto reply_meta = flatbuffers::GetRoot<brpcfb::StreamRpcReplyMeta>(meta_string.data());

  RAY_LOG(DEBUG) << "Received RPC reply: "
                 << GetTraceInfoMessage(*reply_meta->trace_info());

  RAY_LOG(DEBUG) << "Processing server message for request id: "
                 << reply_meta->request_id();

  const auto request_id = reply_meta->request_id();
  ReplyCallback reply_callback;

  {
    std::unique_lock<std::mutex> guard(callback_mutex_);
    auto iter = pending_callbacks_.find(request_id);
    if (iter != pending_callbacks_.end()) {
      reply_callback = iter->second;
      pending_callbacks_.erase(iter);
      RAY_LOG(DEBUG) << "removing callback for request " << request_id;
    } else {
      RAY_LOG(FATAL) << "Cannot find corresponding request for reply"
                     << ", request id: " << request_id;
    }

    if (request_id % 10000 == 0) {
      RAY_LOG(INFO) << "received reply for request " << request_id
                    << ", pending: " << pending_callbacks_.size();
    }
  }

  // Invoke the callback outside of the mutex to avoid deadlock.
  if (reply_callback != nullptr) {
    reply_callback(reply_meta, iobuf);
  }
}

void BrpcStreamClient::AddPendingCallback(uint64_t request_id,
                                          ReplyCallback reply_callback) {
  // Add the request to the records, so that
  // we can invoke the callback after receiving the reply.
  std::unique_lock<std::mutex> guard(callback_mutex_);
  pending_callbacks_.emplace(request_id, std::move(reply_callback));
  RAY_LOG(DEBUG) << "adding callback for request " << request_id;
}

bool BrpcStreamClient::RemovePendingCallback(uint64_t request_id) {
  std::unique_lock<std::mutex> guard(callback_mutex_);
  auto success = pending_callbacks_.erase(request_id);
  if (success) {
    RAY_LOG(DEBUG) << "removed callback for request " << request_id;
  }
  return success;
}

}  // namespace rpc
}  // namespace ray
