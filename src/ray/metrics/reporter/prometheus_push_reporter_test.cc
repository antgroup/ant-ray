#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/metrics/registry/prometheus_metrics_registry.h"
#include "ray/metrics/reporter/prometheus_push_reporter.h"

namespace ray {

namespace metrics {

class PrometheusPushReporterTest : public ::testing::Test {
 public:
  void SetUp() {
    registry_options_.default_tag_map_ = {{"app", "ray"}, {"host", "10.10.10.10"}};
    registry_ = new PrometheusMetricsRegistry(registry_options_);

    reporter_options_.user_name_ = "4Jf9GRMlXd8yXtCpJFEksOHLExhQrnel";
    reporter_options_.password_ = "";
    reporter_options_.job_name_ = "prometheus_push_test";
    reporter_options_.service_addr_ = "http://10.101.82.5:7200/prom";
    reporter_options_.report_interval_ = std::chrono::seconds(1);
    reporter_ = new PrometheusPushReporter(reporter_options_, service_);
    reporter_->RegisterRegistry(registry_);
  }

  void Run(int64_t timeout_ms) {
    UpdateCounter();

    ASSERT_TRUE(reporter_->Init());
    ASSERT_TRUE(reporter_->Start());

    auto timer_period = boost::posix_time::milliseconds(timeout_ms);
    auto timer = std::make_shared<boost::asio::deadline_timer>(service_, timer_period);
    timer->async_wait([this](const boost::system::error_code &error) {
      reporter_->Stop();
      service_.stop();
    });
    service_.run();
    service_.reset();
  }

  void UpdateCounter() {
    registry_->RegisterCounter(metric_name_);
    for (size_t loop = 1; loop <= loop_update_times_; ++loop) {
      registry_->UpdateValue(metric_name_, loop);
      total_value += loop;
    }
  }

  void TearDown() {
    delete reporter_;
    delete registry_;
  }

 protected:
  boost::asio::io_service service_;

  RegistryOption registry_options_;
  MetricsRegistryInterface *registry_;

  ReporterOption reporter_options_;
  MetricsReporterInterface *reporter_;

  std::string metric_name_{"report_test"};
  size_t loop_update_times_{1000};
  int64_t total_value{0};
};

TEST_F(PrometheusPushReporterTest, ReportTest) {
  int64_t timeout_ms = 2100;
  Run(timeout_ms);
  // TODO(micafan) Check report result
}

}  // namespace metrics

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
