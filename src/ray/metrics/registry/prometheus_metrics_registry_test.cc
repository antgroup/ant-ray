#include <chrono>
#include <thread>

#include "gtest/gtest.h"
#include "ray/metrics/registry/prometheus_metrics_registry.h"

namespace ray {

namespace metrics {

class PrometheusMetricsRegistryTest : public ::testing::Test {
 public:
  void SetUp() {
    options_.default_tag_map_ = {{"host", "10.10.10.10"}, {"appname", "Alipay"}};
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
          MetricType::kCounter,
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
    DoCheckResult(MetricType::kCounter, "counter_test");
  }

  void CheckGaugeResult() {
    DoCheckResult(MetricType::kGauge, "gauge_test");
  }

  void CheckHistogramResult() {
    DoCheckResult(MetricType::kHistogram, "histogram_test");
  }

 protected:
  void DoCheckResult(MetricType type, const std::string &metric_name) {
    std::vector<prometheus::MetricFamily> metrics;
    std::string regex_exp = ".*";
    registry_->ExportMetrics(regex_exp, &metrics);
    EXPECT_TRUE(!metrics.empty());

    // for histogram
    size_t his_sum = 0;
    if (type ==  MetricType::kHistogram) {
      for (size_t his_value = 1; his_value <= loop_update_times_; ++his_value) {
        his_sum += his_value;
      }
    }

    // check result
    size_t matched_family = 0;
    std::unordered_map<std::string ,std::string> all_tags;
    for (const auto &elem : metrics) {
      if (elem.name != metric_name) {
        continue;
      }
      // found match
      ++matched_family;
      std::unordered_map<std::string ,std::string> cur_tags;
      const std::vector<prometheus::ClientMetric> &results = elem.metric;
      for (const auto &result : results) {
        switch (type) {
        case MetricType::kCounter:
          EXPECT_TRUE(elem.type == prometheus::MetricType::Counter);
          EXPECT_EQ(static_cast<size_t>(result.counter.value), loop_update_times_);
          break;
        case MetricType::kGauge:
          EXPECT_TRUE(elem.type == prometheus::MetricType::Gauge);
          EXPECT_EQ(static_cast<size_t>(result.gauge.value), loop_update_times_);
          break;
        case MetricType::kHistogram:
          EXPECT_TRUE(elem.type == prometheus::MetricType::Histogram);
          EXPECT_EQ(static_cast<size_t>(result.histogram.sample_count),
                    loop_update_times_);
          EXPECT_EQ(static_cast<size_t>(result.histogram.sample_sum), his_sum);
          break;
        default:
          EXPECT_TRUE(0);
        }
        const std::vector<prometheus::ClientMetric::Label> &labels = result.label;
        EXPECT_EQ(labels.size(), 3U) << " unexpected label for metric=" << metric_name;
        for (const auto &label : labels) {
          cur_tags.emplace(label.name, label.value);
          all_tags.emplace(label.name, label.value);
        }
      }
      EXPECT_EQ(results.size(), 1)
        << " metric=" << metric_name;
      EXPECT_EQ(cur_tags.size(), options_.default_tag_map_.size() + 1)
        << " metric=" << metric_name;
    }
    EXPECT_EQ(matched_family, op_thread_count_)
      << " metric=" << metric_name;
    EXPECT_EQ(all_tags.size(), options_.default_tag_map_.size() + op_thread_count_)
      << " metric=" << metric_name;
  }

  void DoUpdate(size_t thread_index, MetricType type, std::string metric_name) {
    auto start = std::chrono::high_resolution_clock::now();
    std::map<std::string, std::string> tag_map;
    tag_map.emplace(std::to_string(thread_index), std::to_string(thread_index));
    Tags tags(tag_map);
    switch (type) {
    case MetricType::kCounter:
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
      EXPECT_TRUE(0);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> elapsed = end - start;
    std::string info = " Thread " + std::to_string(thread_index) + " update metric="
      + metric_name + " times=" + std::to_string(loop_update_times_) + ", cost "
      + std::to_string(elapsed.count()) + " us.";
    std::cout << info << std::endl;
  }

 protected:
  RegistryOption options_;
  PrometheusMetricsRegistry* registry_{nullptr};
  size_t loop_update_times_{10000};
  size_t op_thread_count_{4};
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
