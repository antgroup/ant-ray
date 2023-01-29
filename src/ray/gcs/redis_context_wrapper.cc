#include "ray/gcs/redis_context_wrapper.h"
#include <unistd.h>
#include "ray/stats/stats.h"

using namespace ray;

inline void SleepAWhile() {
  // Sleep for a little.
  usleep(RayConfig::instance().redis_db_connect_wait_milliseconds() * 1000);
}

RedisContextWrapper::RedisContextWrapper(const std::string &host, int port,
                                         const std::string &password)
    : inner_context_(nullptr),
      host_(host),
      port_(port),
      password_(password),
      destructed_(false) {}

redisReply *RedisContextWrapper::Command(const char *format, ...) {
  std::lock_guard<std::mutex> lock(mutex_);
  int retry_attempts = 0;
  std::string error_message;
  redisReply *reply = nullptr;
  while (!destructed_ && !TooManyRetries(retry_attempts, error_message)) {
    if (!inner_context_) {
      if (!TryConnect()) {
        break;
      }
    }

    va_list ap;
    va_start(ap, format);
    reply = RedisVCommandInternal(inner_context_, format, ap);
    va_end(ap);
    retry_attempts++;
    if (!ConnectionOK(inner_context_, reply) || Busy(inner_context_, reply)) {
      std::ostringstream oss;
      oss << "Failed to send redis command (sync)."
          << APPEND_CONTEXT_ERROR(inner_context_)
          << " retry_attempts: " << retry_attempts;
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      freeReplyObject(reply);
      reply = nullptr;
      Disconnect();
      SleepAWhile();
    } else {
      break;
    }
  }

  if (!reply) {
    RAY_LOG(DEBUG) << "Redis connection is not ready. Reject command.";
  }
  return reply;
}

redisReply *RedisContextWrapper::Command(int argc, const char **argv,
                                         const size_t *argvlen) {
  std::lock_guard<std::mutex> lock(mutex_);
  int retry_attempts = 0;
  std::string error_message;
  redisReply *reply = nullptr;
  while (!destructed_ && !TooManyRetries(retry_attempts, error_message)) {
    if (!inner_context_) {
      if (!TryConnect()) {
        break;
      }
    }

    reply = RedisCommandArgvInternal(inner_context_, argc, argv, argvlen);
    retry_attempts++;
    if (!ConnectionOK(inner_context_, reply) || Busy(inner_context_, reply)) {
      std::ostringstream oss;
      oss << "Failed to send redis command (sync)."
          << APPEND_CONTEXT_ERROR(inner_context_)
          << " retry_attempts: " << retry_attempts;
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      freeReplyObject(reply);
      reply = nullptr;
      Disconnect();
      SleepAWhile();
    } else {
      break;
    }
  }

  if (!reply) {
    RAY_LOG(DEBUG) << "Redis connection is not ready. Reject command.";
  }

  return reply;
}

std::string RedisContextWrapper::GetErrorMessage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (inner_context_ != nullptr && inner_context_->err) {
    return std::string(inner_context_->errstr);
  }
  return "";
}

bool RedisContextWrapper::TryConnect() {
  Disconnect();
  int retry_attempts = 0;
  std::string error_message;
  while (!destructed_) {
    if (TooManyRetries(retry_attempts, error_message)) {
      RAY_LOG(ERROR) << "Too many retries. Abort connecting process (sync). ";
      break;
    }
    if (retry_attempts > 0 && retry_attempts % 100 == 0) {
      RAY_LOG(WARNING) << "Failed to connect to redis after " << retry_attempts
                       << " reties.";
    }
    ray::stats::RedisSyncConnectionCount().Record(1);
    RAY_LOG(DEBUG) << "Trying to connect to redis (sync). retry_attempts: "
                   << retry_attempts++;

    bool setup_failed = false;
    std::ostringstream oss;

    auto context = ::redisConnect(host_.c_str(), port_);
    if (context == nullptr || context->err) {
      oss << "Failed to connect to redis (sync)." << APPEND_CONTEXT_ERROR(context);
      error_message = oss.str();
      RAY_LOG(DEBUG) << error_message;
      setup_failed = true;
    }

    if (!setup_failed) {
      if (REDIS_OK != ::redisEnableKeepAlive(context)) {
        oss << "Failed to enable keep alive." << APPEND_CONTEXT_ERROR(context);
        error_message = oss.str();
        RAY_LOG(DEBUG) << error_message;
        setup_failed = true;
      }
    }

    if (!setup_failed) {
      bool init_success = Initialize(context);
      if (!init_success) {
        oss << "Failed to init context (sync).";
        error_message = oss.str();
        RAY_LOG(DEBUG) << error_message;
        setup_failed = true;
      }
    }

    if (setup_failed) {
      if (context != nullptr) {
        ::redisFree(context);
      }
      SleepAWhile();
      continue;
    }

    inner_context_ = context;
    RAY_LOG(DEBUG) << "Connected to redis (sync).";
    break;
  }
  return inner_context_ != nullptr;
}

bool RedisContextWrapper::Initialize(redisContext *redis_context) {
  redisReply *reply = nullptr;

  // authentication
  if (!password_.empty()) {
    reply = RedisCommandInternal(redis_context, "AUTH %s", password_.c_str());
    bool auth_success = ConnectionOK(redis_context, reply) && !Busy(redis_context, reply);
    freeReplyObject(reply);
    if (!auth_success) {
      return false;
    }
  }

  // Set config required by ray. It will also test if write operations are allowed.
  // If redis replies with READONLY or LOADING, trigger reconnect.
  reply = RedisCommandInternal(redis_context, "CONFIG SET notify-keyspace-events Kl");
  bool config_success = ConnectionOK(redis_context, reply) && !Busy(redis_context, reply);
  freeReplyObject(reply);
  return config_success;
}

void RedisContextWrapper::Disconnect() {
  if (inner_context_ != nullptr) {
    ::redisFree(inner_context_);
    inner_context_ = nullptr;
  }
}

RedisContextWrapper::~RedisContextWrapper() {
  std::lock_guard<std::mutex> lock(mutex_);
  destructed_ = true;
  Disconnect();
}

redisReply *RedisContextWrapper::RedisVCommandInternal(redisContext *redis_context,
                                                       const char *format, va_list ap) {
  return reinterpret_cast<redisReply *>(::redisvCommand(redis_context, format, ap));
}

redisReply *RedisContextWrapper::RedisCommandInternal(redisContext *redis_context,
                                                      const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  auto reply = RedisVCommandInternal(redis_context, format, ap);
  va_end(ap);
  return reply;
}

redisReply *RedisContextWrapper::RedisCommandArgvInternal(redisContext *redis_context,
                                                          int argc, const char **argv,
                                                          const size_t *argvlen) {
  return reinterpret_cast<redisReply *>(
      ::redisCommandArgv(redis_context, argc, argv, argvlen));
}
