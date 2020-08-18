//
// Created by qicosmos on 2020/8/18.
//

#ifndef RAY_SAFE_HIREDIS_H
#define RAY_SAFE_HIREDIS_H

#include "hiredis/hiredis.h"
namespace ray{
class RedisContext final {
 public:
  RedisContext() = delete;
  RedisContext(const RedisContext&) = delete;
  RedisContext&operator=(const RedisContext&) = delete;

  RedisContext(RedisContext&&) = default;
  RedisContext&operator=(RedisContext&&) = default;
  RedisContext(redisContext *context) : ctx_(context) {}

  operator  bool (){
    return ctx_!= nullptr;
  }

  redisContext *operator()(){
    return get();
  }

  redisContext * get(){
    return ctx_.get();
  }

  bool operator==(const RedisContext& other) const{
    return other.ctx_ == this->ctx_;
  }

  bool operator!=(const RedisContext& other) const{
    return other.ctx_ != this->ctx_;
  }

  redisContext *operator->() const { return ctx_.get(); }

 private:
  std::unique_ptr<redisContext> ctx_;
};

class RedisReply final {
 public:
  RedisReply() = delete;
  RedisReply(const RedisReply&) = delete;
  RedisReply&operator=(const RedisReply&) = delete;

  RedisReply(RedisReply&&) = default;
  RedisReply&operator=(RedisReply&&) = default;
  RedisReply(redisReply *reply) : reply_(reply) {}
  RedisReply(void *reply) : reply_(static_cast<redisReply*>(reply)) {}

  operator  bool (){
    return reply_!= nullptr;
  }

  bool operator==(const RedisReply& other) const{
    return other.reply_ == this->reply_;
  }

  bool operator!=(const RedisReply& other) const{
    return other.reply_ != this->reply_;
  }

  redisReply *operator()(){
    return get();
  }

  redisReply * get(){
    return reply_.get();
  }

  redisReply *operator->() const { return reply_.get(); }

 private:
  std::unique_ptr<redisReply> reply_;
};
}
#endif  // RAY_SAFE_HIREDIS_H
