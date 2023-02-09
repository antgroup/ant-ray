#include "ray/rpc/brpc/client/client.h"
#include "ray/common/ray_config.h"

namespace ray {
namespace rpc {

absl::Mutex BrpcClient::cached_channels_mutex_;
std::unordered_map<std::string, std::weak_ptr<SharedBrpcChannel>>
    BrpcClient::cached_channels_;

BrpcClient::BrpcClient(const std::string &address, const int port,
                       instrumented_io_context &reply_service)
    : address_(address), port_(port), reply_service_(reply_service) {
  gflags::SetCommandLineOption(
      "max_body_size", std::to_string(RayConfig::instance().brpc_max_body_size()).data());
}

BrpcClient::~BrpcClient() {}

Status BrpcClient::ConnectChannel(::brpc::Channel **channel, bool is_object_manager_conn) {
  std::string server_ip_and_port = address_ + ":" + std::to_string(port_);

  // A Channel represents a communication line to a Server. Notice that
  // Channel is thread-safe and can be shared by all threads in your program.
  channel_ = nullptr;
  {
    absl::ReaderMutexLock lock(&cached_channels_mutex_);
    auto channel_it = cached_channels_.find(server_ip_and_port);
    if (channel_it != cached_channels_.end()) {
      channel_ = channel_it->second.lock();
      // It's still possible that lock() returns nullptr here.
      // e.g.
      // 1) BrpcStreamClient 1 is created and connected to remote address A.
      // 2) BrpcStreamClient 1 is no longer needed and destructed.
      // 3) BrpcStreamClient 2 is created and connected to remote address A.
      // In this case, after stage 2, they key of remote address A is still in the
      // channel map, but the value (`weak_ptr`) no longer holds a weak reference.
    }
  }
  if (!channel_) {
    absl::MutexLock lock(&cached_channels_mutex_);
    auto channel_it = cached_channels_.find(server_ip_and_port);
    if (channel_it != cached_channels_.end()) {
      channel_ = channel_it->second.lock();
    }
    if (!channel_) {
      channel_ = std::make_shared<SharedBrpcChannel>();
      // TODO(kfstorm): The unused keys in cached_channels_ map are never erased. The
      // memory consumption should be acceptable as values are of type weak_ptr. Here we
      // use operator[] instead of emplace() to replace the original value incase the
      // key already exists but the original value of type weak_ptr is no longer valid.
      cached_channels_[server_ip_and_port] = channel_;
    }
  }

  auto &optional_channel = channel_->second;
  {
    absl::ReleasableMutexLock lock(&channel_->first);
    if (!optional_channel.has_value()) {
      // Either the shared channel object is just created by this client, or another
      // client failed to init the `::brpc::Channel` object in this shared channel
      // object.
      optional_channel.emplace();

      // Initialize the channel, NULL means using default options.
      ::brpc::ChannelOptions options;
      options.protocol = ::brpc::PROTOCOL_BAIDU_STD;
      options.connection_type =
          "";  // Available values: single, pooled, short. Single by default.
      if (is_object_manager_conn) {
        options.connection_type = "pooled";
        RAY_LOG(INFO) << "connection_type: " << options.connection_type;
      }
      options.timeout_ms = -1;  // no timeout.
      options.max_retry = 0;    // Max retries(not including the first RPC).
#ifdef BRPC_WITH_RDMA
      options.use_rdma = RayConfig::instance().use_rdma();
#else
      if (RayConfig::instance().use_rdma()) {
        RAY_LOG(WARNING) << "brpc is not compiled with rdma, to enable it, set macro BRPC_WITH_RDMA to true.";
      }
      options.use_rdma = false; // by default
#endif
      if (optional_channel.value().Init(address_.c_str(), port_, &options) != 0) {
        optional_channel.reset();
        *channel = nullptr;
        return Status::IOError("Failed to init channel " + server_ip_and_port);
      }
    }
  }

  *channel = &optional_channel.value();
  return Status::OK();
}

class SpdLogSink : public ::logging::LogSink {
 public:
  bool OnLogMessage(int severity, const char *file, int line,
                    const butil::StringPiece &log_content) override {
    RayLogLevel level;
    if (severity <= ::logging::BLOG_VERBOSE) {
      level = RayLogLevel::DEBUG;
    } else {
      switch (severity) {
      case ::logging::BLOG_INFO:
        level = RayLogLevel::INFO;
        break;
      case ::logging::BLOG_NOTICE:
        level = RayLogLevel::INFO;
        break;
      case ::logging::BLOG_WARNING:
        level = RayLogLevel::WARNING;
        break;
      case ::logging::BLOG_ERROR:
        level = RayLogLevel::ERROR;
        break;
      case ::logging::BLOG_FATAL:
        level = RayLogLevel::FATAL;
        break;
      default:
        RAY_LOG(FATAL) << "Unknown brpc log severity: " << severity;
        level = RayLogLevel::FATAL;
        break;
      }
    }
    ::ray::RayLog(file, line, level) << log_content;
    return true;
  }
};

void ConfigureBrpcLogging() {
  static SpdLogSink sink_instance;
  ::logging::SetLogSink(&sink_instance);
}

}  // namespace rpc
}  // namespace ray
