//
// Created by qicosmos on 2020/8/18.
//
#include "gtest/gtest.h"
#include "ray/util/safe_hiredis.h"

namespace ray {
//test passed in local env, make sure the redis server started!
TEST(SafeHiredis, SafeConnect) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  EXPECT_TRUE(context && !context->err);
}

TEST(SafeHiredis, SafeCommand) {
  RedisContext context = redisConnect("127.0.0.1", 6379);
  EXPECT_TRUE(context && !context->err);
  RedisReply auth_reply = (redisReply *)redisCommand(context(), "AUTH %s", "5241590000000000");
  EXPECT_TRUE(auth_reply->type != REDIS_REPLY_ERROR);

  RedisReply reply = redisCommand(context(), "GET session_dir");
  EXPECT_TRUE(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  EXPECT_TRUE(!session_dir.empty());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}