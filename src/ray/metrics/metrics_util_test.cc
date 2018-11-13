
#include "gtest/gtest.h"
#include "metrics_util.h"

#include <iostream>
#include <chrono>

namespace ray {
namespace metrics {

class MetricsUtilTest : public ::testing::Test {
public:
  void SetUp() {}

  void TearDown() {}

};

TEST_F(MetricsUtilTest, ParseStringToMapTest) {
  const std::string source = "k1,v1,k2,v2";
  auto result = ParseStringToMap(source, ',');

  ASSERT_EQ(2, result.size());
  ASSERT_EQ("v1", result["k1"]);
  ASSERT_EQ("v2", result["k2"]);
}

TEST_F(MetricsUtilTest, ParsingComplexStringTest) {
  const std::string conf_str = "registry_name,test_registry,"
                               "reporter_name,test_reporter,"
                               "registry.delimiter,**,"
                               "registry.bucket_count,25,"
                               "registry.default_tag_map,k1:v1:k2:v2:k3:v3,"
                               "registry.default_percentiles,1.0:2.1:3:9.9,"
                               "reporter.user_name,test_user_name,"
                               "reporter.password,test_password,"
                               "reporter.job_name,test_job,"
                               "reporter.service_addr,127.0.0.1:8999,"
                               "reporter.report_interval,30,"
                               "reporter.max_retry_times,100";
  auto result = ParseStringToMap(conf_str, ',');

  ASSERT_EQ(12, result.size());
  ASSERT_EQ("test_registry", result["registry_name"]);
  ASSERT_EQ("**", result["registry.delimiter"]);
  ASSERT_EQ("25", result["registry.bucket_count"]);
  ASSERT_EQ("k1:v1:k2:v2:k3:v3", result["registry.default_tag_map"]);
  ASSERT_EQ("1.0:2.1:3:9.9", result["registry.default_percentiles"]);
  ASSERT_EQ("test_user_name", result["reporter.user_name"]);
  ASSERT_EQ("127.0.0.1:8999", result["reporter.service_addr"]);
}

TEST_F(MetricsUtilTest, GetFromMapTest) {
  const std::unordered_map<std::string, std::string> from{{"k1", "123"}, {"k2", "55"}};
  ASSERT_EQ(123, GetFromMap<int>(from, "k1"));
  ASSERT_EQ("123", GetFromMap<std::string>(from, "k1"));
  ASSERT_EQ(std::chrono::seconds{55}, GetFromMap<std::chrono::seconds>(from, "k2"));
}

} // namespace metrics
} //namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
