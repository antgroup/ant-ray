
#include <sys/types.h>
#include <unistd.h>

#include <boost/asio.hpp>
#include <boost/bind/bind.hpp>
#include <thread>
#include <utility>

#include "ray/rpc/brpc/server/server.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {

void BrpcStreamCreationService::RegisterStreamService(BrpcStreamService &service) {
  std::vector<std::shared_ptr<StreamServiceMethod>> server_call_methods;
  brpcpb::StreamServiceType service_type;
  try {
    // The cast would throw std::bad_cast if it fails.
    service.InitMethodHandlers(&server_call_methods);
    service_type = service.GetServiceType();
  } catch (const std::bad_cast &) {
    RAY_LOG(FATAL) << "Incorrect service type, BrpcStreamService expected";
  }

  auto service_handler = [server_call_methods, service_type](::brpc::StreamId stream_id,
                                                             butil::IOBuf iobuf) {
    uint32_t message_type;
    iobuf.cutn(&message_type, sizeof(message_type));

    for (const auto &method : server_call_methods) {
      if (method->GetRequestType() == message_type) {
        // RAY_LOG(INFO) << "received type " << message_type << " from stream " <<
        // stream_id;
        method->HandleRequest(stream_id, iobuf);
        return;
      }
    }

    RAY_LOG(FATAL) << "Received unexpected message type " << message_type
                   << " for service " << brpcpb::StreamServiceType_Name(service_type);
  };

  service_handlers_.emplace(service_type, service_handler);
}

void BrpcStreamCreationService::CreateStream(google::protobuf::RpcController *controller,
                                             const brpcpb::CreateStreamRequest *request,
                                             brpcpb::CreateStreamReply *response,
                                             google::protobuf::Closure *done) {
  // This object helps you to call done->Run() in RAII style. If you need
  // to process the request asynchronously, pass done_guard.release().
  ::brpc::ClosureGuard done_guard(done);

  BrpcStreamServiceHandler service_handler;

  // Note that the service handler map is immutable after all the
  // stream services are registered, so we don't need a lock here.
  auto service_type = request->service_type();
  auto iter = service_handlers_.find(service_type);
  if (iter != service_handlers_.end()) {
    service_handler = iter->second;
  } else {
    RAY_LOG(FATAL) << "Received unexpected brpc stream service type " << service_type;
  }

  ::brpc::StreamId stream_id;
  ::brpc::Controller *cntl = static_cast<::brpc::Controller *>(controller);
  ::brpc::StreamOptions stream_options;
  stream_options.max_buf_size = 0;
  // NOTE(zhijunfu): reply handler will be freed by brpc when the specific stream is
  // destructed, and `handler_owned_by_brpc` in StreamOptions must be set to make this
  // happen.
  auto stream_receiver =
      new BrpcStreamRequestHandler(service_handler, shared_from_this());
  stream_options.handler = stream_receiver;
  // Set this to true so that brpc will delete the handler when the stream is destroyed.
  stream_options.handler_owned_by_brpc = true;

  if (::brpc::StreamAccept(&stream_id, *cntl, &stream_options) != 0) {
    RAY_LOG(WARNING) << "Failed to accept stream";
    cntl->SetFailed("Fail to accept stream");
    return;
  }

  { stream_receiver->Init(stream_id); }

  RAY_LOG(INFO) << "BrpcStreamCreationService::CreateStream " << stream_id;
}

void BrpcServer::Run() {
  std::string server_address = "0.0.0.0:" + std::to_string(port_);

  // It would be desirable to disable the builtin service for brpc server
  // for tests to speed up.
  bool disable_builtin_service =
      getenv("RAY_WORKER_BRPC_BUILTIN_SERVICE_DISABLED") != nullptr &&
      getenv("RAY_WORKER_BRPC_BUILTIN_SERVICE_DISABLED") == std::string("true");
#if defined(__APPLE__)
  // It's slow to start brpc builtin service on MAC, so disable it there.
  disable_builtin_service = true;
#endif

  ::brpc::ServerOptions options;
  options.has_builtin_services = !disable_builtin_service;
  options.num_threads = 8;
  options.in_place_execution = true;
#ifdef BRPC_WITH_RDMA
  options.use_rdma = RayConfig::instance().use_rdma();
#else
  if (RayConfig::instance().use_rdma()) {
    RAY_LOG(WARNING) << "brpc is not compiled with rdma, to enable it, set macro BRPC_WITH_RDMA to true.";
  }
  options.use_rdma = false; // by default
#endif
  RAY_CHECK(server_.Start(port_, &options) == 0);

  // assign the port to the one that assigned by kernel.
  port_ = server_.listen_address().port;

  is_closed_ = false;
  RAY_LOG(INFO) << name_ << " brpc server started, listening on " << port_
                << ", pid: " << getpid();
}

void BrpcServer::Shutdown() {
  if (!is_closed_.exchange(true)) {
    // Shutdown brpc server.
    server_.Stop(0);
    server_.Join();
    RAY_LOG(INFO) << "BRPC server of " << name_ << " shuts down.";
  }
}

void BrpcServer::RegisterStreamService(BrpcStreamService &service) {
  if (!stream_server_) {
    stream_server_ = std::make_shared<BrpcStreamCreationService>();
    RAY_CHECK(
        server_.AddService(stream_server_.get(), ::brpc::SERVER_DOESNT_OWN_SERVICE) == 0);
  }

  stream_server_->RegisterStreamService(service);
}

}  // namespace rpc
}  // namespace ray
