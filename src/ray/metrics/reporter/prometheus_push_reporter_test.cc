#include <chrono>
#include <memory>
#include <iostream>
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
    registry_.reset(new PrometheusMetricsRegistry(registry_options_));

    reporter_options_.user_name_ = "4Jf9GRMlXd8yXtCpJFEksOHLExhQrnel";
    reporter_options_.password_ = "";
    reporter_options_.job_name_ = "prometheus_push_test";
    reporter_options_.service_addr_ = "10.101.82.5:7200/prom";
    reporter_options_.report_interval_ = std::chrono::seconds(1);
  }

  void RebuildReporterWithIOS() {
    reporter_.reset(new PrometheusPushReporter(reporter_options_, service_));
    reporter_->RegisterRegistry(registry_.get());
  }

  void RebuildReporter() {
    reporter_.reset(new PrometheusPushReporter(reporter_options_));
    reporter_->RegisterRegistry(registry_.get());
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
    std::cout << "--Run Done--" << std::endl;
  }

  void UpdateCounter() {
    registry_->RegisterCounter(counter_name_);
    registry_->RegisterGauge(gauge_name_);
    for (size_t loop = 1; loop <= loop_update_times_; ++loop) {
      counter_total_value_ += loop;
      registry_->UpdateValue(counter_name_, loop);
      registry_->UpdateValue(gauge_name_, loop);
    }
  }

  void TearDown() {
  }

 protected:
  boost::asio::io_service service_;

  RegistryOption registry_options_;
  std::unique_ptr<MetricsRegistryInterface> registry_;

  ReporterOption reporter_options_;
  std::unique_ptr<MetricsReporterInterface> reporter_;

  std::string counter_name_{"report_test"};
  size_t loop_update_times_{1000};
  int64_t counter_total_value_{0};
  std::string gauge_name_{"report_gauge_test"};
};

TEST_F(PrometheusPushReporterTest, ReportTest) {
  int64_t timeout_ms = 2100;

  RebuildReporter();
  Run(timeout_ms);

  RebuildReporterWithIOS();
  Run(timeout_ms);

  RebuildReporterWithIOS();
  Run(timeout_ms);

  // TODO(micafan) Check report result
}

}  // namespace metrics

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
