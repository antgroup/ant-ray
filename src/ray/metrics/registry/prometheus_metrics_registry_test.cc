#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/util/logging.h"
#include "ray/metrics/registry/prometheus_metrics_registry.h"

namespace ray {

namespace metrics {

class PrometheusMetricsRegistryTest : public ::testing::Test {
 public:
  void SetUp() {
    options_.default_tagmap_ = {{"host", "10.10.10.10"}, {"appname", "Alipay"}};
    registry_ = new PrometheusMetricsRegistry(options_);
  }

  void TearDown() {
    delete registry_;
  }

  void AsyncUpdateCounter() {
    std::string metric_name = "counter_test";
    for (size_t t_index = 0; t_index < op_thread_count_; ++t_index) {
      thread_pool_.emplace_back(std::bind(
          &PrometheusMetricsRegistryTest::DoUpdate,
          this,
          t_index,
          MetricType::kCount,
          metric_name));
    }
  }

  void AsyncUpdateGauge() {
    std::string metric_name = "gauge_test";
    for (size_t t_index = 0; t_index < op_thread_count_; ++t_index) {
      thread_pool_.emplace_back(std::bind(
          &PrometheusMetricsRegistryTest::DoUpdate,
          this,
          t_index,
          MetricType::kGauge,
          metric_name));
    }
  }

  void AsyncUpdateHistogram() {
    std::string metric_name = "histogram_test";
    for (size_t t_index = 0; t_index < op_thread_count_; ++t_index) {
      thread_pool_.emplace_back(std::bind(
          &PrometheusMetricsRegistryTest::DoUpdate,
          this,
          t_index,
          MetricType::kHistogram,
          metric_name));
    }
  }

  void WaitUpdateDone() {
    for (auto &thread : thread_pool_) {
      thread.join();
    }
  }

  void CheckCounterResult() {
    RAY_CHECK(DoCheckResult(MetricType::kCount, "counter_test"))
      << __FUNCTION__ << " failed!";
  }

  void CheckGaugeResult() {
    RAY_CHECK(DoCheckResult(MetricType::kGauge, "gauge_test"))
      << __FUNCTION__ << " failed!";
  }

  void CheckHistogramResult() {
    RAY_CHECK(DoCheckResult(MetricType::kHistogram, "histogram_test"))
      << __FUNCTION__ << " failed!";
  }

 protected:
  bool DoCheckResult(MetricType type, const std::string &metric_name) {
    std::vector<prometheus::MetricFamily> metrics;
    std::string regex_exp = ".*";
    registry_->ExportMetrics(regex_exp, &metrics);
    RAY_CHECK(!metrics.empty());

    // for histogram
    size_t his_sum = 0;
    if (type ==  MetricType::kHistogram) {
      for (size_t his_value = 1; his_value <= loop_update_times_; ++his_value) {
        his_sum += his_value;
      }
    }

    for (const auto &elem : metrics) {
      if (elem.name != metric_name) {
        continue;
      }
      const std::vector<prometheus::ClientMetric> &results = elem.metric;
      RAY_CHECK(results.size() == op_thread_count_);
      std::unordered_map<std::string ,std::string> all_tags;
      for (const auto &result : results) {
        switch (type) {
        case MetricType::kCount:
          RAY_CHECK(elem.type == prometheus::MetricType::Counter);
          RAY_CHECK(static_cast<size_t>(result.counter.value) == loop_update_times_)
            << " real value=" << result.counter.value;
          break;
        case MetricType::kGauge:
          RAY_CHECK(elem.type == prometheus::MetricType::Gauge);
          RAY_CHECK(static_cast<size_t>(result.gauge.value) == loop_update_times_)
            << " real value=" << result.gauge.value;
          break;
        case MetricType::kHistogram:
          RAY_CHECK(elem.type == prometheus::MetricType::Histogram);
          RAY_CHECK(static_cast<size_t>(result.histogram.sample_count)
                    == loop_update_times_)
            << " real sample_count=" << result.histogram.sample_count;
          RAY_CHECK(static_cast<size_t>(result.histogram.sample_sum)
                    == his_sum)
            << " real sample_sum=" << result.histogram.sample_sum;
          break;
        default:
          RAY_CHECK(0);
        }
        const std::vector<prometheus::ClientMetric::Label> &labels = result.label;
        for (const auto &label : labels) {
          all_tags.emplace(label.name, label.value);
        }
        RAY_CHECK(all_tags.size() == options_.default_tagmap_.size() + op_thread_count_);
      }
      return true;
    }
    return false;
  }

  void DoUpdate(size_t thread_index, MetricType type, std::string metric_name) {
    // std::chrono::microseconds sleep_period(10);
    // std::this_thread::sleep_for(sleep_period);
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::string, std::string> tag_map;
    tag_map.emplace("thread_id", std::to_string(thread_index));
    Tags tags(tag_map);
    switch (type) {
    case MetricType::kCount:
      for (size_t loop = 1; loop <= loop_update_times_; ++loop) {
        registry_->RegisterCounter(metric_name);
        registry_->UpdateValue(metric_name, 1, &tags);
      }
      break;
    case MetricType::kGauge:
      for (size_t loop = 1; loop <= loop_update_times_; ++loop) {
        registry_->RegisterGauge(metric_name);
        registry_->UpdateValue(metric_name, loop, &tags);
      }
      break;
    case MetricType::kHistogram:
      for (size_t loop = 1; loop <= loop_update_times_; ++loop) {
        registry_->RegisterHistogram(metric_name, 1, loop_update_times_);
        registry_->UpdateValue(metric_name, loop, &tags);
      }
      break;
    default:
      RAY_CHECK(0);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> elapsed = end - start;
    std::cout << "Thread " << thread_index << " update metric " << metric_name
      << " waited " << elapsed.count() << " us\n";
  }

  RegistryOption options_;
  PrometheusMetricsRegistry* registry_{nullptr};
  size_t loop_update_times_{10000};
  size_t op_thread_count_{10};
  std::vector<std::thread> thread_pool_;
};

TEST_F(PrometheusMetricsRegistryTest, UpdateTest) {
  AsyncUpdateCounter();
  AsyncUpdateGauge();
  AsyncUpdateHistogram();
  WaitUpdateDone();
  CheckCounterResult();
  CheckGaugeResult();
  CheckHistogramResult();
}

}  // namespace metrics

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
