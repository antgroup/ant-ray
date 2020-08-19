//
// Created by qicosmos on 2020/8/18.
//
#include "ray/util/safe_hiredis.h"

#include "gtest/gtest.h"

namespace ray {

TEST(SafeHiredis, NoNeedCheckNull) {
  int invalid_port = 0;
  RedisContext context = redisConnect("127.0.0.1", invalid_port);
  EXPECT_THROW(context(), std::exception);
  EXPECT_THROW(context.get(), std::exception);
  RedisReply auth_reply((void *)nullptr);
  EXPECT_THROW(auth_reply(), std::exception);
  EXPECT_THROW(auth_reply.get(), std::exception);

  redisContext* ctx = nullptr;
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

  redisReply* p = nullptr;
  RedisReply reply(p);
  EXPECT_FALSE(auth_reply);


  EXPECT_NE(context1, nullptr);
}

TEST(SafeHiredis, SafeConnect) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  if(!context){
    return;
  }
  EXPECT_TRUE(context && !context->err);
}

TEST(SafeHiredis, SafeCommand) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  if(!context){
    return;
  }
  EXPECT_TRUE(context && !context->err);
  RedisReply auth_reply = (redisReply *)redisCommand(context(), "AUTH %s", "5241590000000000");
  EXPECT_TRUE(auth_reply->type != REDIS_REPLY_ERROR);

  RedisReply reply = redisCommand(context(), "GET session_dir");
  EXPECT_TRUE(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  EXPECT_TRUE(!session_dir.empty());
}

TEST(SafeHiredis, SafeAPI){
  RedisContext context = redisConnect_s("127.0.0.1", 6379);
  if(!context){
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