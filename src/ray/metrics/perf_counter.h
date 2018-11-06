#ifndef RAY_METRICS_PERF_COUNTER_H
#define RAY_METRICS_PERF_COUNTER_H

#include <boost/asio.hpp>

#include "ray/metrics/metrics_conf.h"
#include "ray/metrics/group/metrics_group_interface.h"

namespace ray {

namespace metrics {

class PerfCounter final {
 public:
  /// Initialize the PerfCounter functions.
  ///
  /// \param conf The configuration of metrics.
  /// \param io_service The io service for event loop.
  /// \return True for success, and false for failure.
  static bool Start(const MetricsConf &conf, boost::asio::io_service &io_service);

  /// Shutdown the PerfCounter.
  static void Shutdown();

  static void UpdateCounter(const std::string &domain,
                            const std::string &group_name,
                            const std::string &short_name,
                            int64_t value);

  static void UpdateGauge(const std::string &domain,
                          const std::string &group_name,
                          const std::string &short_name,
                          int64_t value);

  static void UpdateHistogram(const std::string &domain,
                              const std::string &group_name,
                              const std::string &short_name,
                              int64_t value,
                              int64_t min_value,
                              int64_t max_value);

  static void AddCounterGroup(const std::string &domain,
                              const std::string &group_name,
                              const std::map<std::string, std::string> &tag_map = {});

  static void AddCounterGroup(const std::string &domain,
                              std::shared_ptr<MetricsGroupInterface> group);

 private:
  class Impl;
  static std::unique_ptr<Impl> impl_ptr_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_PERF_COUNTER_H
