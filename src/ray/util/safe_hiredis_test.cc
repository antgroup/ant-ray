//
// Created by qicosmos on 2020/8/18.
//
#include "ray/util/safe_hiredis.h"

#include "gtest/gtest.h"

namespace ray {

//Old not safe API
// static std::string GetSessionDir(std::string redis_ip, int port, std::string password)
// {
//  redisContext *context = redisConnect(redis_ip.c_str(), port);
//  RAY_CHECK(context != NULL && !context->err);
//  if (!password.empty()) {
//    auto auth_reply = (redisReply *)redisCommand(context, "AUTH %s", password.c_str());
//    RAY_CHECK(auth_reply->type != REDIS_REPLY_ERROR);
//    freeReplyObject(auth_reply);
//  }
//  auto reply = (redisReply *)redisCommand(context, "GET session_dir");
//  RAY_CHECK(reply->type != REDIS_REPLY_ERROR);
//  auto session_dir = std::string(reply->str);
//  freeReplyObject(reply);
//  redisFree(context);
//  return session_dir;
//}

// VS
// New safe API
//static std::string GetSessionDir(std::string redis_ip, int port, std::string password) {
//  RedisContext context = redisConnect_s(redis_ip.c_str(), port);
//
//  if (!password.empty()) {
//    RedisReply auth_reply = redisCommand_s(context.get(), "AUTH %s", password.c_str());
//    EXPECT_TRUE(auth_reply->type != REDIS_REPLY_ERROR);
//  }
//
//  RedisReply reply = redisCommand_s(context(), "GET session_dir");
//  auto session_dir = std::string(reply->str);
//
//  return session_dir;
//}

//new safe api make the code shorter and safer!

TEST(SafeHiredis, NoNeedCheckNull) {
  int invalid_port = 0;
  RedisContext context = redisConnect("127.0.0.1", invalid_port);
  EXPECT_THROW(context(), std::exception);
  EXPECT_THROW(context.get(), std::exception);
  RedisReply auth_reply((void *)nullptr);
  EXPECT_THROW(auth_reply(), std::exception);
  EXPECT_THROW(auth_reply.get(), std::exception);

  redisContext *ctx = nullptr;
  RedisContext c(ctx);
  EXPECT_FALSE(c);
  EXPECT_EQ(c, nullptr);
}

TEST(SafeHiredis, CheckAndCompare) {
  int invalid_port = 0;
  RedisContext context1 = redisConnect("127.0.0.1", invalid_port);
  RedisContext context2 = redisConnect("127.0.0.1", invalid_port);
  EXPECT_FALSE(context1);
  EXPECT_FALSE(context2);

  RedisReply auth_reply(nullptr);
  EXPECT_FALSE(auth_reply);

  redisReply *p = nullptr;
  RedisReply reply(p);
  EXPECT_FALSE(auth_reply);

  EXPECT_NE(context1, nullptr);
}

TEST(SafeHiredis, SafeConnect) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  if (!context) {
    return;
  }
  EXPECT_TRUE(context && !context->err);
}

TEST(SafeHiredis, SafeCommand) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  if (!context) {
    return;
  }
  EXPECT_TRUE(context && !context->err);
  RedisReply auth_reply =
      (redisReply *)redisCommand(context(), "AUTH %s", "5241590000000000");
  EXPECT_TRUE(auth_reply->type != REDIS_REPLY_ERROR);

  RedisReply reply = redisCommand(context(), "GET session_dir");
  EXPECT_TRUE(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  EXPECT_TRUE(!session_dir.empty());
}

TEST(SafeHiredis, SafeAPI) {
  RedisContext context = redisConnect_s("127.0.0.1", 6379);
  if (!context) {
    return;
  }

  EXPECT_TRUE(context && !context->err);
  RedisReply auth_reply = redisCommand_s(context(), "AUTH %s", "5241590000000000");
  EXPECT_TRUE(auth_reply->type != REDIS_REPLY_ERROR);

  RedisReply reply = redisCommand_s(context(), "GET session_dir");
  EXPECT_TRUE(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  EXPECT_TRUE(!session_dir.empty());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}