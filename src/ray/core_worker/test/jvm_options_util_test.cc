// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/jvm_options_util.h"

#include <boost/asio/error.hpp>
#include <list>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

namespace ray {

class JvmOptionsUtilTest : public ::testing::Test {
 public:
};

#define ASSERT_SIZE_EQ(STR, SIZE)           \
  {                                         \
    int64_t bytes = 0;                      \
    bool ok = ParseMemorySize(STR, &bytes); \
    ASSERT_TRUE(ok);                        \
    ASSERT_EQ(bytes, SIZE);                 \
  }

TEST_F(JvmOptionsUtilTest, TestParseSize) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    // Skip this case when gcs task scheduling disabled since
    // it doesn't work in that mode.
    return;
  }
  ASSERT_SIZE_EQ("1g", 1LL * 1024 * 1024 * 1024);
  ASSERT_SIZE_EQ("231g", 231LL * 1024 * 1024 * 1024);
  ASSERT_SIZE_EQ("56G", 56LL * 1024 * 1024 * 1024);

  ASSERT_SIZE_EQ("5M", 5LL * 1024 * 1024);
  ASSERT_SIZE_EQ("21M", 21LL * 1024 * 1024);
  ASSERT_SIZE_EQ("902m", 902LL * 1024 * 1024);
  ASSERT_SIZE_EQ("4453m", 4453LL * 1024 * 1024);

  ASSERT_SIZE_EQ("5K", 5LL * 1024);
  ASSERT_SIZE_EQ("21K", 21LL * 1024);
  ASSERT_SIZE_EQ("902k", 902LL * 1024);
  ASSERT_SIZE_EQ("4453k", 4453LL * 1024);

  ASSERT_SIZE_EQ("5B", 5);
  ASSERT_SIZE_EQ("21B", 21);
  ASSERT_SIZE_EQ("902b", 902);
  ASSERT_SIZE_EQ("4453b", 4453);

  ASSERT_SIZE_EQ("5", 5);
  ASSERT_SIZE_EQ("21", 21);
  ASSERT_SIZE_EQ("902", 902);
  ASSERT_SIZE_EQ("4453", 4453);
}

TEST_F(JvmOptionsUtilTest, TestParseInvalidJvmOptions) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    // Skip this case when gcs task scheduling disabled since
    // it doesn't work in that mode.
    return;
  }
  {
    std::vector<std::string> dummy_jvm_options = {"-Xmx:200g", "-Xss:100g"};
    auto result = ValidateJvmOptions(100, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_TRUE(StartsWithIgnoreCase(std::get<1>(result), "Failed to parse option -Xmx"));
  }

  {
    std::vector<std::string> dummy_jvm_options = {"-Xmx200g", "-Xss:100g"};
    auto result = ValidateJvmOptions(100, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_TRUE(StartsWithIgnoreCase(std::get<1>(result), "Failed to parse option -Xss"));
  }

  {
    std::vector<std::string> dummy_jvm_options = {"-Xmx200g", "-Xmn:100mb"};
    auto result = ValidateJvmOptions(100, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_TRUE(
        StartsWithIgnoreCase(std::get<1>(result), "Failed to parse option -Xmn:100mb"));
  }
}

TEST_F(JvmOptionsUtilTest, TestJvmOptionsParsingValidation) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    // Skip this case when gcs task scheduling disabled since
    // it doesn't work in that mode.
    return;
  }
  {
    /// xmx > maximum_memory
    std::vector<std::string> dummy_jvm_options = {"-Xmx501m"};
    auto result = ValidateJvmOptions(/*500mb*/ 500 * 1024 * 1024, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_TRUE(StartsWithIgnoreCase(std::get<1>(result),
                                     "Xmx must be less than maximum_memory."));
  }

  {
    /// xms > maximum_memory
    std::vector<std::string> dummy_jvm_options = {"-Xmx500m", "-Xms501m"};
    auto result = ValidateJvmOptions(/*500mb*/ 500 * 1024 * 1024, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_EQ(std::get<1>(result), "Xms must be less than maximum_memory.");
  }

  {
    /// xms > xmx
    std::vector<std::string> dummy_jvm_options = {"-Xmx500m", "-Xms501m"};
    auto result = ValidateJvmOptions(/*500mb*/ 600 * 1024 * 1024, dummy_jvm_options);
    ASSERT_FALSE(std::get<0>(result));
    ASSERT_EQ(std::get<1>(result), "Xms must be less than Xmx.");
  }

  {
    // Green case1.
    std::vector<std::string> dummy_jvm_options = {"-Xmx1g", "-Xms400m"};
    auto result =
        ValidateJvmOptions(/*500mb*/ 2LL * 1024 * 1024 * 1024, dummy_jvm_options);
    ASSERT_TRUE(std::get<0>(result));
    ASSERT_EQ(std::get<1>(result), "ok");
  }

  {
    // Green case2
    std::vector<std::string> dummy_jvm_options = {
        "-Xmn500M", "-Xss300m", "-XX:MaxDirectMemorySize=1g", "-Xmx1G", "-Xms400m"};
    auto result = ValidateJvmOptions(2LL * 1024 * 1024 * 1024, dummy_jvm_options);
    ASSERT_TRUE(std::get<0>(result));
    ASSERT_EQ(std::get<1>(result), "ok");
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
