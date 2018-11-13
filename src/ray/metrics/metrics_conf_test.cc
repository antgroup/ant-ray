
#include "gtest/gtest.h"
#include "metrics_conf.h"

namespace ray {

namespace metrics {

class MetricsConfTest : public ::testing::Test {
public:
  void SetUp() {}

  void TearDown() {}
};

//TODO(qwang): Add more cases to cover more cases.
TEST_F(MetricsConfTest, ParsingConfStrTest) {
  const std::string conf_str = "registry_name,test_registry,reporter_name,test_reporter,"
                               "registry.delimiter,**,registry.bucket_count,25,"
                               "registry.default_tag_map,k1:v1:k2:v2:k3:v3,"
                               "registry.default_percentiles,1.0:2.1:3:9.9,"
                               "reporter.user_name,test_user_name,"
                               "reporter.password,test_password,"
                               "reporter.job_name,test_job,"
                               "reporter.service_addr,127.0.0.1:8999,"
                               "reporter.report_interval,30,"
                               "reporter.max_retry_times,100";

  MetricsConf metrics_conf{conf_str};

  const auto &reporter_name = metrics_conf.GetReporterName();
  ASSERT_EQ("test_reporter", reporter_name);

  const auto &registry_name = metrics_conf.GetRegistryName();
  ASSERT_EQ("test_registry", registry_name);

  // registry options cases
  const RegistryOption &registry_option = metrics_conf.GetRegistryOption();
  ASSERT_EQ(25, registry_option.bucket_count_);
  ASSERT_EQ("**", registry_option.delimiter_);
  // ASSERT_EQ(3, registry_option.default_tag_map_.size());
  // test default_tag_map_ value
  ASSERT_EQ(4, registry_option.default_percentiles_.size());

  // reporter options cases
  const ReporterOption &reporter_option = metrics_conf.GetReporterOption();
  ASSERT_EQ("test_user_name", reporter_option.user_name_);
  ASSERT_EQ("test_password", reporter_option.password_);
  ASSERT_EQ("test_job", reporter_option.job_name_);
  ASSERT_EQ("127.0.0.1:8999", reporter_option.service_addr_);
  ASSERT_EQ(30, reporter_option.report_interval_.count());
  ASSERT_EQ(100, reporter_option.max_retry_times_);
}

} // namespace metrics
} // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
