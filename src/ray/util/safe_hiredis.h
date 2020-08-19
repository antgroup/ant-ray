//
// Created by qicosmos on 2020/8/18.
//

#ifndef RAY_SAFE_HIREDIS_H
#define RAY_SAFE_HIREDIS_H

#include <memory>
#include "hiredis/hiredis.h"

namespace ray {
class RedisContext final {
 public:
  RedisContext() = delete;
  RedisContext(const RedisContext &) = delete;
  RedisContext &operator=(const RedisContext &) = delete;

  RedisContext(RedisContext &&) = default;
  RedisContext &operator=(RedisContext &&) = default;
  RedisContext(redisContext *context) : ctx_(context) {}

  operator bool() { return ctx_ != nullptr && !ctx_->err; }

  redisContext *operator()() { return get(); }

  redisContext *get() {
    check();
    return ctx_.get();
  }

  bool operator==(const RedisContext &other) const { return other.ctx_ == this->ctx_; }

  bool operator!=(const RedisContext &other) const { return other.ctx_ != this->ctx_; }

  redisContext *operator->() const {
    check();
    return ctx_.get();
  }

 private:
  void check() const {
    if (ctx_ == nullptr) {
      throw std::invalid_argument("redisContext is null");
    }
    if (ctx_->err) {
      throw std::invalid_argument(ctx_->errstr);
    }
  }
  std::unique_ptr<redisContext> ctx_;
};

class RedisReply final {
 public:
  RedisReply() = delete;
  RedisReply(const RedisReply &) = delete;
  RedisReply &operator=(const RedisReply &) = delete;

  RedisReply(RedisReply &&) = default;
  RedisReply &operator=(RedisReply &&) = default;
  RedisReply(redisReply *reply) : reply_(reply) {}
  RedisReply(nullptr_t reply) : reply_(nullptr) {}

  RedisReply(void *reply) {
    if (reply == nullptr) {
      reply_ = nullptr;
    } else {
      reply_ = std::unique_ptr<redisReply>(static_cast<redisReply *>(reply));
    }
  }

  operator bool() { return reply_ != nullptr && reply_->type != REDIS_REPLY_ERROR; }

  bool operator==(const RedisReply &other) const { return other.reply_ == this->reply_; }

  bool operator!=(const RedisReply &other) const { return other.reply_ != this->reply_; }

  redisReply *operator()() { return get(); }

  redisReply *get() {
    check();
    return reply_.get();
  }

  redisReply *operator->() const {
    check();
    return reply_.get();
  }

 private:
  void check() const {
    if (reply_ == nullptr) {
      throw std::invalid_argument("redisContext is null");
    }
    if (reply_->type == REDIS_REPLY_ERROR) {
      throw std::invalid_argument("REDIS_REPLY_ERROR");
    }
  }
  std::unique_ptr<redisReply> reply_;
};

inline RedisContext redisConnect_s(const char *ip, int port) {
  return redisConnect(ip, port);
}

inline RedisReply redisCommand_s(redisContext *c, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  void *reply = redisvCommand(c, format, ap);
  va_end(ap);
  return reply;
}

}
#endif  // RAY_SAFE_HIREDIS_H
