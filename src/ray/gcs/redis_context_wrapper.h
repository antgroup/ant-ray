#ifndef RAY_GCS_REDIS_CONTEXT_WRAPPER_H
#define RAY_GCS_REDIS_CONTEXT_WRAPPER_H

#include <mutex>
#include "redis_context_wrapper_common.h"

class RedisContextWrapper {
 public:
  RedisContextWrapper(const std::string &host, int port, const std::string &password);

  ~RedisContextWrapper();

  redisReply *Command(const char *format, ...);

  redisReply *Command(int argc, const char **argv, const size_t *argvlen);

  std::string GetErrorMessage() const;

 private:
  bool Initialize(redisContext *redis_context);

  bool TryConnect();

  void Disconnect();

  static redisReply *RedisVCommandInternal(redisContext *redis_context,
                                           const char *format, va_list ap);

  static redisReply *RedisCommandInternal(redisContext *redis_context, const char *format,
                                          ...);

  static redisReply *RedisCommandArgvInternal(redisContext *redis_context, int argc,
                                              const char **argv, const size_t *argvlen);

  redisContext *inner_context_;
  const std::string host_;
  const int port_;
  const std::string password_;
  bool destructed_;
  mutable std::mutex mutex_;
};

#endif  // RAY_GCS_REDIS_CONTEXT_WRAPPER_H
