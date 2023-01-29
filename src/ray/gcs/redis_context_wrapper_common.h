#ifndef RAY_REDIS_CONTEXT_WRAPPER_COMMON_H
#define RAY_REDIS_CONTEXT_WRAPPER_COMMON_H

extern "C" {
#include "hiredis/hiredis.h"
}

#include <string>
#include "ray/common/ray_config.h"
#include "ray/util/logging.h"

#define APPEND_CONTEXT_ERROR(context)                       \
  " context: " << (context) << " err: " << ((context)->err) \
               << " errstr: " << ((context)->errstr)

#define APPEND_REPLY_ERROR(context, reply) \
  " context: " << (context) << " message: " << (reply)->str

template <typename redisContext>
inline bool ConnectionOK(redisContext *redis_context, redisReply *redis_reply) {
  RAY_CHECK(redis_context);
  if (redis_context->err != REDIS_OK || redis_reply == nullptr) {
    RAY_LOG(DEBUG) << "Redis context received no reply."
                   << APPEND_CONTEXT_ERROR(redis_context);
    return false;
  }
  return true;
}

template <typename redisContext>
inline bool Busy(redisContext *redis_context, redisReply *redis_reply) {
  RAY_CHECK(redis_reply != nullptr);
  if (redis_reply->type == REDIS_REPLY_ERROR) {
    std::string message(redis_reply->str);
    std::string error_code(message.substr(0, message.find(' ')));
    RAY_LOG(DEBUG) << "Redis context received error reply. "
                   << APPEND_REPLY_ERROR(redis_context, redis_reply);
    return error_code == "LOADING" || error_code == "READONLY" ||
           message == "ERR REPLICATION LAG";
  }
  return false;
}

inline void MaybeFailOnRedisError(const std::string &error_message) {
  RAY_LOG(INFO) << "Redis connection error: " << error_message;
}

inline bool TooManyRetries(int retry_attempts, const std::string &error_message) {
  if (retry_attempts > 0) {
    MaybeFailOnRedisError(error_message);
  }
  if (retry_attempts >= RayConfig::instance().redis_db_reconnect_retries()) {
    RAY_LOG(ERROR) << "Too many retries. Last error: " << error_message;
    return true;
  }
  return false;
}

#endif  // RAY_REDIS_CONTEXT_WRAPPER_COMMON_H
