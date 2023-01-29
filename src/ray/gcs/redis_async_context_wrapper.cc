#include "ray/gcs/redis_async_context_wrapper.h"

#include "ray/common/id.h"
#include "ray/stats/stats.h"
#include "src/ray/protobuf/gcs.pb.h"

using namespace ray;

#define APPEND_RAY_CONTEXT_ERROR(redis_async_context) \
  APPEND_CONTEXT_ERROR((redis_async_context)->GetRawRedisAsyncContext())

inline bool ContextConnected(ray::gcs::RedisAsyncContext *context) {
  auto flags = context->GetRawRedisAsyncContext()->c.flags;
  return (flags & REDIS_CONNECTED) && !(flags & REDIS_DISCONNECTING) &&
         !(flags & REDIS_FREEING);
}

inline RedisAsyncContextWrapper *GetWrapper(const redisAsyncContext *context) {
  return reinterpret_cast<RedisAsyncContextWrapper *>(context->data);
}

inline RedisAsyncContextWrapper *GetWrapper(const ray::gcs::RedisAsyncContext *context) {
  return GetWrapper(context->GetRawRedisAsyncContext());
}

const int64_t REDIS_MAX_COMMAND_SIZE = 1024 * 1024 * 10;

Status CheckCommandLength(int len) {
  if (len < 0) {
    // If the `redisvFormatCommand` failed, the return value of it is an negative integer,
    // otherwise, it's the length of the formatted command.
    return Status::Invalid("Failed to format command.");
  }
  if (len > REDIS_MAX_COMMAND_SIZE) {
    return Status::Invalid(
        std::string("Command to send is too large.") +
        " Maximum allowed size: " + std::to_string(REDIS_MAX_COMMAND_SIZE) +
        ". Actual size: " + std::to_string(len) + ".");
  }
  return Status::OK();
}

struct InitStepPrivdataWrapper {
  const char *step_name;
  std::function<void()> next_step;
};

PrivdataWrapper::PrivdataWrapper()
    : nested_callback(nullptr),
      cmd(nullptr),
      len(0),
      is_in_send_buffer(false),
      privdata(nullptr),
      command_type(CommandType::OTHERS) {}

void PrivdataWrapper::FillRetryInfo(const char *format, va_list ap) {
  auto redis_command = GetRedisCommand(format);

  static const std::unordered_map<std::string, CommandType> command_type_map = {
      {"SUBSCRIBE", CommandType::SUBSCRIBE},
      {"PSUBSCRIBE", CommandType::SUBSCRIBE},
      {"UNSUBSCRIBE", CommandType::UNSUBSCRIBE},
      {"PUNSUBSCRIBE", CommandType::UNSUBSCRIBE},
  };

  auto it = command_type_map.find(redis_command);
  if (it != command_type_map.end()) {
    command_type = it->second;
    if (command_type == CommandType::SUBSCRIBE ||
        command_type == CommandType::UNSUBSCRIBE) {
      // Calculate hash of subscribe/unsubscribe commands.
      // We use `redisvFormatCommand` here so we don't need to parse `ap` manually.
      auto hash_format = format;
      if (command_type == CommandType::UNSUBSCRIBE) {
        if (std::string("UNSUBSCRIBE %d") == format) {
          hash_format = "SUBSCRIBE %d";
        } else if (std::string("UNSUBSCRIBE %d:%b") == format) {
          hash_format = "SUBSCRIBE %d:%b";
        } else if (std::string("PUNSUBSCRIBE %b") == format) {
          hash_format = "PSUBSCRIBE %b";
        } else if (std::string("UNSUBSCRIBE %b") == format) {
          hash_format = "SUBSCRIBE %b";
        } else {
          RAY_LOG(FATAL) << "Unknown unsubscribe format: \"" << format
                         << "\", you may need to update the code here.";
        }
      }
      char *cmd;
      // Use `hash_format` instead of `format` here so we can have the same hash for
      // subscribe and unsubscribe commands with the same channel/pattern.
      int len = redisvFormatCommand(&cmd, hash_format, ap);
      RAY_CHECK_OK(CheckCommandLength(len));
      std::string cmd_string(cmd);
      redisFreeCommand(cmd);

      request_hash = cmd_string;
      return;
    }
  }
}

void PrivdataWrapper::FillRetryInfo(int argc, const char **argv, const size_t *argvlen) {
  // Assume that commands trigger this function have default retry info.
  command_type = CommandType::OTHERS;
}

PrivdataWrapper::~PrivdataWrapper() {
  if (cmd) {
    redisFreeCommand(cmd);
  }
}

std::string PrivdataWrapper::GetRedisCommand(const char *format) {
  RAY_CHECK(format);
  int index = 0;
  while (format[index] != '\0' && format[index] != ' ') {
    index++;
  }
  std::string command(format, index);
  boost::to_upper(command);
  return command;
}

const boost::posix_time::milliseconds RedisAsyncContextWrapper::TIMER_DELAY =
    boost::posix_time::milliseconds(
        RayConfig::instance().redis_db_connect_wait_milliseconds());

RedisAsyncContextWrapper::RedisAsyncContextWrapper(
    const std::string &host, int port, const std::string &password,
    std::function<void(void *)> on_clean_command_callback)
    : inner_context_(nullptr),
      io_service_(nullptr),
      host_(host),
      port_(port),
      password_(password),
      destructed_(false),
      used_buffer_size_(0),
      on_clean_command_callback_(std::move(on_clean_command_callback)) {}

RedisAsyncContextWrapper::~RedisAsyncContextWrapper() {
  if (send_waiting_command_timer_) {
    send_waiting_command_timer_->cancel();
  }

  destructed_ = true;
  ResetContext(true);

  for (PrivdataWrapper *privdata_wrapper : retry_operations_list_) {
    // free the memory of PrivdataWrapper
    delete privdata_wrapper;
  }
}

void RedisAsyncContextWrapper::Attach(instrumented_io_context &io_service) {
  RAY_CHECK(!io_service_) << "Attach shall be called only once";
  io_service_ = &io_service;
  connection_state_.reset(new ConnectionState());
  connection_state_->timer.reset(new boost::asio::deadline_timer(*io_service_));
  io_service_->dispatch([this]() { ConnectAsync(); });
  SendWaitingCommandsPeriodically();
}

void RedisAsyncContextWrapper::DisconnectAsync() {
  RAY_CHECK(inner_context_);
  inner_context_->RedisAsyncDisconnect();
}

void RedisAsyncContextWrapper::ReconnectAsync(const std::string &error_message) {
  if (destructed_) {
    return;
  }
  RAY_CHECK(!inner_context_);
  MaybeFailOnRedisError(error_message);
  if (connection_state_->retry_attempts > 0 &&
      connection_state_->retry_attempts % 100 == 0) {
    RAY_LOG(WARNING) << "Failed to connect to redis after "
                     << connection_state_->retry_attempts << " reties.";
  }
  connection_state_->timer->expires_from_now(TIMER_DELAY);
  connection_state_->timer->async_wait([this](const boost::system::error_code &error) {
    if (error != boost::asio::error::operation_aborted) {
      ConnectAsync();
    }
  });
}

void RedisAsyncContextWrapper::ConnectCallback(const redisAsyncContext *context,
                                               int status) {
  auto wrapper = GetWrapper(context);
  wrapper->ConnectCallback(status);
}

void RedisAsyncContextWrapper::SetSubscribeCallback(std::function<void()> callback) {
  subscribe_callback_ = callback;
}

void RedisAsyncContextWrapper::ConnectCallback(int status) {
  if (status == REDIS_ERR) {
    std::ostringstream oss;
    oss << "Failed to connect to redis." << APPEND_RAY_CONTEXT_ERROR(inner_context_);
    std::string error_message = oss.str();
    RAY_LOG(DEBUG) << error_message;
    ResetContext(false);
    ReconnectAsync(error_message);
  } else {
    RAY_LOG(DEBUG) << "Connected to redis."
                   << " context: " << inner_context_;
    // Post is required here. Othersise deadlock may occur.
    io_service_->post([this]() { InitAsync(); });
  }
}

void RedisAsyncContextWrapper::DisconnectCallback(const redisAsyncContext *context,
                                                  int status) {
  GetWrapper(context)->DisconnectCallback(status);
}

void RedisAsyncContextWrapper::DisconnectCallback(int status) {
  RAY_CHECK(inner_context_);
  std::ostringstream oss;
  oss << "ray::gcs::RedisAsyncContext disconnected. status: " << status
      << APPEND_RAY_CONTEXT_ERROR(inner_context_);
  std::string error_message = oss.str();
  RAY_LOG(DEBUG) << error_message;
  ResetContext(false);
  // NOTE: don't reset asio_client now. Because we may still in the callback of
  // RedisAsioClient::cleanup().

  ReconnectAsync(error_message);
}

void RedisAsyncContextWrapper::FinishInit() {
  RAY_CHECK(inner_context_);
  connection_state_->retry_attempts = 0;
  {
    std::lock_guard<std::mutex> lock(is_connected_mutex_);
    connection_state_->initialized = true;
  }
  // If this is reconnection, clear all the data in waiting queue.
  // The following replay operation will resend all the commands in order.
  waiting_queue_.clear();
  // After reconnection the buffer is clean and we need to reset the buffer size counter.
  used_buffer_size_ = 0;

  RAY_LOG(DEBUG) << "Finished initializing ray::gcs::RedisAsyncContext. context: "
                 << inner_context_;
  // replay commands
  if (!retry_operations_list_.empty()) {
    RAY_LOG(DEBUG) << "Will re-send " << retry_operations_list_.size() << " commands.";
    // Don't clear retry_operations_list_.
    // Only remove element from retry_operations_list_ when operation completed.
    waiting_queue_.assign(retry_operations_list_.begin(), retry_operations_list_.end());
    RAY_LOG(DEBUG) << "Finish putting items into waiting queue.";
  }
  RAY_IGNORE_EXPR(SendWaitingCommands(RayConfig::instance().max_command_redis_sending()));
  if (subscribe_callback_) {
    subscribe_callback_();
  }
}

void RedisAsyncContextWrapper::CommandCallback(redisAsyncContext *context, void *reply,
                                               void *privdata) {
  GetWrapper(context)->CommandCallback(reply, privdata);
}

void RedisAsyncContextWrapper::CommandCallback(void *reply, void *privdata) {
  auto privdata_wrapper = reinterpret_cast<PrivdataWrapper *>(privdata);
  auto redis_reply = reinterpret_cast<redisReply *>(reply);
  if (!ConnectionOK(inner_context_->GetRawRedisAsyncContext(), redis_reply)) {
    // no need to reconnect here because we already registered disconnect callback.
  } else {
    if (Busy(inner_context_, redis_reply)) {
      // it's possible that we connected to master before, but now it becomes a slave.
      // The connection is still OK, but the slave will keep READONLY.
      // So we need to reconnect to the new master.
      DisconnectAsync();
    } else {
      ProcessCommandReply(redis_reply, privdata_wrapper);
    }
  }
  // no need to free reply object in async callback.
}

void RedisAsyncContextWrapper::RemoveFromRetryList(PrivdataWrapper *privdata) {
  if (privdata->iter != retry_operations_list_.end()) {
    retry_operations_list_.erase(privdata->iter);
    privdata->iter = retry_operations_list_.end();
  }
}

void RedisAsyncContextWrapper::CleanCommand(PrivdataWrapper *privdata) {
  if (on_clean_command_callback_) {
    on_clean_command_callback_(privdata->privdata);
  }

  RemoveFromRetryList(privdata);

  // For corner cases, we try to deduct used_buffer_size_ here again.
  // One corner case is the unsubscribe command, which will not receive it's own reply.
  // The reply is sent to the callbacks of subscribe commands.
  if (privdata->is_in_send_buffer) {
    used_buffer_size_ -= privdata->len;
    privdata->is_in_send_buffer = false;
  }

  // Assume that the callback of this command will never be called anymore.
  // We can safely free the memory of PrivdataWrapper.
  delete privdata;
}

void RedisAsyncContextWrapper::ProcessCommandReply(redisReply *redis_reply,
                                                   PrivdataWrapper *privdata) {
  // We deduct the used_buffer_size_ here because we've received the reply of the command.
  if (privdata->is_in_send_buffer) {
    used_buffer_size_ -= privdata->len;
    privdata->is_in_send_buffer = false;
  }
  // command completed.
  if (privdata->nested_callback) {
    privdata->nested_callback(this, redis_reply, privdata->privdata);
  }

  if (privdata->command_type == CommandType::SUBSCRIBE && redis_reply &&
      redis_reply->type == REDIS_REPLY_ARRAY) {
    redisReply *first_element = redis_reply->element[0];
    if (first_element && first_element->type == REDIS_REPLY_STRING &&
        (std::string("unsubscribe") == first_element->str ||
         std::string("punsubscribe") == first_element->str)) {
      // Now we received the unsubscribe message. It's safe to clean the commands.
      // Note that it's possible that multiple subscribe commands with the same
      // channel/pattern are sent. So we need to clean all commands here.
      auto it = subscribe_requests_map_.find(privdata->request_hash);
      if (it != subscribe_requests_map_.end()) {
        for (auto subscribe_privdata : it->second) {
          CleanCommand(subscribe_privdata);
        }
        subscribe_requests_map_.erase(it);
      } else {
        RAY_LOG(WARNING) << "Unmatched unsubscribe command sent.";
      }
      // No need to clean `privdata`. It should've been cleaned in the for-loop above.
      return;
    }
  }

  if (privdata->command_type == CommandType::SUBSCRIBE) {
    auto iter = unfulfilled_subscribe_requests_map_.find(privdata->request_hash);
    if (iter != unfulfilled_subscribe_requests_map_.end()) {
      iter->second.erase(privdata);
      if (iter->second.size() == 0) {
        unfulfilled_subscribe_requests_map_.erase(iter);
      }
    }
    if (redis_reply->type == REDIS_REPLY_ERROR) {
      // The request notifications / subscribe command returned an error. Do not replay it
      // during reconnection.
      CleanCommand(privdata);
      return;
    }
    subscribe_requests_map_[privdata->request_hash].insert(privdata);
  }

  // always retry subscribe and un-canceled request notifications commands
  if (privdata->command_type != CommandType::SUBSCRIBE) {
    CleanCommand(privdata);
  }
}

void RedisAsyncContextWrapper::RunInitStepAsync(const char *step_name,
                                                const std::function<void()> &next_step,
                                                const char *command, ...) {
  auto privdata = new InitStepPrivdataWrapper();
  privdata->step_name = step_name;
  privdata->next_step = next_step;

  va_list ap;
  va_start(ap, command);
  Status status = inner_context_->RedisVAsyncCommand(
      &RedisAsyncContextWrapper::InitStepCallback, privdata, command, ap);
  va_end(ap);
  if (!status.ok()) {
    RAY_LOG(FATAL) << "Failed to send command to redis in init step '" << step_name
                   << "'." << APPEND_RAY_CONTEXT_ERROR(inner_context_);
  }
}

void RedisAsyncContextWrapper::InitStepCallback(redisAsyncContext *context, void *reply,
                                                void *privdata) {
  auto wrapper = GetWrapper(context);
  auto redis_reply = reinterpret_cast<redisReply *>(reply);
  auto privdata_wrapper = reinterpret_cast<InitStepPrivdataWrapper *>(privdata);
  if (!ConnectionOK(wrapper->inner_context_->GetRawRedisAsyncContext(), redis_reply)) {
    // no need to reconnect here because we already registered disconnect callback.
  } else {
    if (redis_reply->type == REDIS_REPLY_ERROR) {
      RAY_LOG(DEBUG) << "Init step '" << privdata_wrapper->step_name << "' failed."
                     << APPEND_REPLY_ERROR(wrapper->inner_context_, redis_reply);
      // it's possible that we connected to master before, but now it becomes a slave.
      // The connection is still OK, but the slave will keep READONLY.
      // So we need to reconnect to the new master.
      wrapper->DisconnectAsync();
    } else {
      privdata_wrapper->next_step();
    }
  }
  delete privdata_wrapper;
}

void RedisAsyncContextWrapper::InitAsync() {
  auto test_write = [this]() {
    this->RunInitStepAsync(
        "test write",
        [this]() {
          RunInitStepAsync(
              "config set", [this]() { this->RedisAsyncContextWrapper::FinishInit(); },
              // This config set is required by ray.
              // Only execute this command when the async context is writable.
              "CONFIG SET notify-keyspace-events Kl");
        },
        // test whether the reconnected async context is writable.
        "SET __DUMMY_KEY_WRITE_TEST 0");
  };

  if (!password_.empty()) {
    RunInitStepAsync("auth", test_write, "AUTH %s", password_.c_str());
  } else {
    test_write();
  }
}

void RedisAsyncContextWrapper::ConnectAsync() {
  if (destructed_) {
    return;
  }

  RAY_CHECK(io_service_);
  ray::stats::RedisAsyncConnectionCount().Record(1);
  RAY_LOG(DEBUG) << "Trying to connect to redis. retry_attempts: "
                 << connection_state_->retry_attempts++;

  RAY_CHECK(!inner_context_);

  bool setup_failed = false;
  std::string error_message;
  std::ostringstream oss;

  redisAsyncContext *context = ::redisAsyncConnect(host_.c_str(), port_);
  if (!context) {
    RAY_LOG(DEBUG) << "Failed to create context.";
    setup_failed = true;
  }

  if (!setup_failed) {
    inner_context_ = new ray::gcs::RedisAsyncContext(context);
    RAY_LOG(DEBUG) << "Context created: " << context;
    if (context->err) {
      oss << "Bad context: " << inner_context_
          << APPEND_RAY_CONTEXT_ERROR(inner_context_);
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      setup_failed = true;
    }
  }

  if (!setup_failed) {
    // Reset asio_client here instead of inside redis callbacks to avoid memory issue.
    asio_client_.reset(new RedisAsioClient(*io_service_, inner_context_));
    Status status = asio_client_->init();
    if (!status.ok()) {
      oss << "Failed to init asio client. " << status;
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      setup_failed = true;
    }
  }

  if (!setup_failed) {
    if (REDIS_OK != ::redisEnableKeepAlive(&context->c)) {
      oss << "Failed to enable keep alive." << APPEND_RAY_CONTEXT_ERROR(inner_context_);
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      setup_failed = true;
    }
  }

  if (setup_failed) {
    asio_client_.reset();
    ResetContext(true);
    ReconnectAsync(error_message);
    return;
  }

  context->data = this;

  RAY_CHECK(REDIS_OK == ::redisAsyncSetConnectCallback(
                            context, &RedisAsyncContextWrapper::ConnectCallback));
  RAY_CHECK(REDIS_OK == ::redisAsyncSetDisconnectCallback(
                            context, &RedisAsyncContextWrapper::DisconnectCallback));
}

void RedisAsyncContextWrapper::CallRedisAsyncFormattedCommand(PrivdataWrapper *privdata) {
  RAY_CHECK(IsSafeToSendCommands())
      << "The caller should make sure the connection is OK before calling this method";
  ray::Status command_status = inner_context_->RedisAsyncFormattedCommand(
      RedisAsyncContextWrapper::CommandCallback, privdata, privdata->cmd, privdata->len);
  if (!command_status.ok()) {
    // There is something wrong with this command and it is not put to redis buffer.
    // This case only happends when unsubscribing is called before subscribing.
    // Currently, ray does not call unsubscribe. If this happens, this could be
    // a bug of upper layer.
    RAY_LOG(FATAL) << "Failed to call redisAsyncFormattedCommand."
                   << " cmd: " << privdata->cmd;
  }
  // This command is successfully put to redis buffer.
  used_buffer_size_ += privdata->len;
  privdata->is_in_send_buffer = true;

  if (privdata->command_type == CommandType::UNSUBSCRIBE) {
    // We've sent the unsubscribe command to Redis. We can now remove itself and the
    // coresponding subscribe commands from retry list.
    auto it = subscribe_requests_map_.find(privdata->request_hash);
    if (it != subscribe_requests_map_.end()) {
      // Remove the subscribe commands from retry list.
      // Note that we don't clean them right now, because they will still receive replys.
      // We clean them when we receive the "unsubscribe" message in reply.
      for (auto subscribe_privdata : it->second) {
        RemoveFromRetryList(subscribe_privdata);
      }
    } else {
      RAY_LOG(WARNING) << "Unmatched unsubscribe command sent.";
    }
    CleanCommand(privdata);
  }
}

void RedisAsyncContextWrapper::SendWaitingCommands(size_t max_to_send) {
  for (size_t i = 0; i < max_to_send && !waiting_queue_.empty(); ++i) {
    PrivdataWrapper *data = waiting_queue_.front();
    if (data->len + used_buffer_size_ > RayConfig::instance().max_redis_buffer_size()) {
      // Buffer is full.
      WarnRedisBufferIsFull(data);
      break;
    }
    if (IsSafeToSendCommands()) {
      CallRedisAsyncFormattedCommand(data);
    } else {
      break;
    }
    waiting_queue_.pop_front();
  }
}

void RedisAsyncContextWrapper::CommandAsyncInternal(PrivdataWrapper *privdata) {
  auto it = retry_operations_list_.insert(retry_operations_list_.end(), privdata);
  privdata->iter = it;

  if (privdata->command_type == CommandType::SUBSCRIBE) {
    unfulfilled_subscribe_requests_map_[privdata->request_hash].insert(privdata);
  }

  if (!IsSafeToSendCommands()) {
    // already in retry list. Do nothing.
  } else {
    if (used_buffer_size_ + privdata->len >
            RayConfig::instance().max_redis_buffer_size() ||
        !waiting_queue_.empty()) {
      // The buffer is full or there are waiting commands.
      // Commands should be put at the end of the waiting queue to keep the order.
      waiting_queue_.push_back(privdata);

      if (waiting_queue_.size() % 1000000 == 0) {
        // TODO(yuhguo): implement back pressure mechanism.
        // This is a magic number to print some warining message.
        RAY_LOG(WARNING) << "Sending Redis message too fast or connection is not healthy."
                         << "Too many waiting redis commands are waiting:"
                         << waiting_queue_.size();
      }

      SendWaitingCommands(RayConfig::instance().max_command_redis_sending());
    } else {
      // Normally, in most cases, this function will go to the following code path.
      CallRedisAsyncFormattedCommand(privdata);
    }
  }
}

Status RedisAsyncContextWrapper::CommandAsync(RedisAsyncCommandCallback fn,
                                              void *privdata, const char *format, ...) {
  auto privdata_wrapper = new PrivdataWrapper();
  privdata_wrapper->nested_callback = std::move(fn);
  privdata_wrapper->privdata = privdata;
  privdata_wrapper->iter = retry_operations_list_.end();
  va_list ap_tmp;
  va_start(ap_tmp, format);
  privdata_wrapper->FillRetryInfo(format, ap_tmp);
  va_end(ap_tmp);
  va_list ap;
  va_start(ap, format);
  privdata_wrapper->len = redisvFormatCommand(&(privdata_wrapper->cmd), format, ap);
  va_end(ap);
  auto status = CheckCommandLength(privdata_wrapper->len);
  if (!status.ok()) {
    delete privdata_wrapper;
    return status;
  }

  RAY_CHECK(io_service_);
  io_service_->dispatch(
      [this, privdata_wrapper]() { CommandAsyncInternal(privdata_wrapper); });
  return Status::OK();
}

Status RedisAsyncContextWrapper::CommandArgvAsync(RedisAsyncCommandCallback fn,
                                                  void *privdata, int argc,
                                                  const char **argv,
                                                  const size_t *argvlen) {
  auto privdata_wrapper = new PrivdataWrapper();
  privdata_wrapper->nested_callback = std::move(fn);
  privdata_wrapper->privdata = privdata;
  privdata_wrapper->iter = retry_operations_list_.end();
  privdata_wrapper->FillRetryInfo(argc, argv, argvlen);
  privdata_wrapper->len =
      redisFormatCommandArgv(&(privdata_wrapper->cmd), argc, argv, argvlen);
  auto status = CheckCommandLength(privdata_wrapper->len);
  if (!status.ok()) {
    delete privdata_wrapper;
    return status;
  }
  RAY_CHECK(io_service_);
  io_service_->dispatch(
      [this, privdata_wrapper]() { CommandAsyncInternal(privdata_wrapper); });
  return Status::OK();
}

void RedisAsyncContextWrapper::ResetContext(bool do_free) {
  if (inner_context_) {
    if (do_free) {
      delete inner_context_;
    }
    // TODO (kfstorm): else, how do we reclaim the memory of *inner_context_?
    inner_context_ = nullptr;
  }
  std::lock_guard<std::mutex> lock(is_connected_mutex_);
  if (connection_state_) {
    connection_state_->initialized = false;
  }
}

bool RedisAsyncContextWrapper::IsConnected() {
  std::lock_guard<std::mutex> lock(is_connected_mutex_);
  return connection_state_ && connection_state_->initialized;
}

bool RedisAsyncContextWrapper::IsSafeToSendCommands() {
  return inner_context_ && ContextConnected(inner_context_) && IsConnected();
}

void RedisAsyncContextWrapper::SendWaitingCommandsPeriodically() {
  RAY_CHECK(io_service_);
  if (send_waiting_command_timer_ == nullptr) {
    send_waiting_command_timer_.reset(new boost::asio::steady_timer(*io_service_));
  }

  if (IsSafeToSendCommands()) {
    SendWaitingCommands(RayConfig::instance().max_command_redis_sending());
  }

  int64_t interval_ms =
      RayConfig::instance().redis_waiting_commands_consume_interval_ms();
  send_waiting_command_timer_->expires_from_now(std::chrono::milliseconds(interval_ms));
  send_waiting_command_timer_->async_wait([this](const boost::system::error_code &error) {
    if (error) {
      return;
    }

    SendWaitingCommandsPeriodically();
  });
}

void RedisAsyncContextWrapper::WarnRedisBufferIsFull(PrivdataWrapper *data) {
  int64_t current_time = current_time_ms();
  if (RayConfig::instance().minimum_interval_to_warn_redis_buffer_full_ms() +
          last_redis_buffer_if_full_warn_time_ms_ <
      current_time) {
    RAY_LOG(WARNING)
        << "The redis buffer is too full to consume new commands, waiting_queue_size = "
        << waiting_queue_.size() << ", current_data_size = " << data->len
        << ", used_buffer_size = " << used_buffer_size_
        << ", max_buffer_size = " << RayConfig::instance().max_redis_buffer_size();
    last_redis_buffer_if_full_warn_time_ms_ = current_time;
  }
}
