#include <thread>
#include <chrono>

#include "gtest/gtest.h"
#include "perf_counter.h"

namespace ray {

namespace metrics {

class MockMetricsConf : public MetricsConf {
public:
  MockMetricsConf() : MetricsConf() {}

  void SetRegistryName(const std::string &registry_name) {
    this->registry_name_ = registry_name;
  }

  void SetReporterName(const std::string &reporter_name) {
    this->reporter_name_ = reporter_name;
  }

  void SetRegistryOption(const RegistryOption &registry_option) {
    this->registry_options_ = registry_option;
  }

  void SetReporterOption(const ReporterOption &reporter_option) {
    this->reporter_options_ = reporter_option;
  }
};

class PerfCounterTest :public ::testing::Test {
public:
  using UpdateFunc = std::function<void (size_t)>;

  void SetUp() {
    MockMetricsConf conf;
    conf.SetRegistryName("empty");
    conf.SetReporterName("empty");

    PerfCounter::Start(conf);
  }

  void TearDown() {
    PerfCounter::Shutdown();
  }

  void RegisterAndRun(MetricType type, UpdateFunc update_handler) {

    auto stat_time_handler = [type, this](size_t thread_index, UpdateFunc update_handler) {
      auto start = std::chrono::high_resolution_clock::now();

      for (size_t loop_index = 0; loop_index < loop_update_times_; ++loop_index) {
        update_handler(loop_index);
      }

      auto end = std::chrono::high_resolution_clock::now();
      std::chrono::duration<double, std::micro> elapsed = end - start;

      std::string info = "Thread=" + std::to_string(thread_index)
          + ", type=1"
          + ", times=" + std::to_string(loop_update_times_)
          + ", cost=" + std::to_string(elapsed.count()) + "us.";
      std::cout << info << std::endl;
    };

    for(size_t thread_index = 0; thread_index < op_thread_count_; ++thread_index) {
      thread_pool_.emplace_back(
          std::bind(stat_time_handler, thread_index, update_handler));
    }

    for (auto &thread : thread_pool_) {
      thread.join();
    }
  }

protected:
  size_t op_thread_count_{4};
  size_t loop_update_times_{100000};
  std::vector<std::thread> thread_pool_;

  MockMetricsConf metrics_conf_;
};

// case for updating one key.
TEST_F(PerfCounterTest, UpdateCounterWithOneKeyTest) {
  RegisterAndRun(MetricType::kCounter, [](size_t loop_index){
    PerfCounter::UpdateCounter("domain_a", "group_a", "a", loop_index);
  });
}

// case for updating many keys.
TEST_F(PerfCounterTest, UpdateCounterTest) {
  RegisterAndRun(MetricType::kCounter, [](size_t loop_index){
    auto loop_index_str = std::to_string(loop_index);
    PerfCounter::UpdateCounter("domain_a_" + loop_index_str,
                               "group_a_" + loop_index_str,
                               "a_" + loop_index_str,
                               loop_index);
  });
}

TEST_F(PerfCounterTest, UpdateGaugeWithOneKeyTest) {
  RegisterAndRun(MetricType::kGauge, [](size_t loop_index){
    PerfCounter::UpdateGauge("domain_a", "group_a", "a", loop_index);
  });
}

TEST_F(PerfCounterTest, UpdateGaugeTest) {
  RegisterAndRun(MetricType::kGauge, [](size_t loop_index){
    auto loop_index_str = std::to_string(loop_index);
    PerfCounter::UpdateGauge("domain_a_" + loop_index_str,
                               "group_a_" + loop_index_str,
                               "a_" + loop_index_str,
                               loop_index);
  });
}

TEST_F(PerfCounterTest, UpdateHistogramWithOneKeyTest) {
  RegisterAndRun(MetricType::kHistogram, [](size_t loop_index){
    PerfCounter::UpdateHistogram("domain_a", "group_a", "a", loop_index, 0, 100);
  });
}

TEST_F(PerfCounterTest, UpdateHistogramTest) {
  RegisterAndRun(MetricType::kHistogram, [](size_t loop_index){
    auto loop_index_str = std::to_string(loop_index);
    PerfCounter::UpdateHistogram("domain_a_" + loop_index_str,
                               "group_a_" + loop_index_str,
                               "a_" + loop_index_str,
                               loop_index, 0 ,100);
  });
}

} // namespace metrics

} // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}