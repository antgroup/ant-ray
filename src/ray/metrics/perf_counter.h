#ifndef RAY_METRICS_PERF_COUNTER_H
#define RAY_METRICS_PERF_COUNTER_H

#include <boost/asio.hpp>

#include "ray/metrics/metrics_conf.h"
#include "ray/metrics/group/metrics_group_interface.h"

namespace ray {

#define METRICS_UPDATE_COUNTER(domain, group_name, short_name, value) \
  PerfCounter::UpdateCounter(domain, group_name, short_name, value)

#define METRICS_UPDATE_GAUGE(domain, group_name, short_name, value) \
  PerfCounter::UpdateGauge(domain, group_name, short_name, value)

#define METRICS_UPDATE_HISTOGRAM( \
  domain, group_name, short_name, value, min_value, max_value) \
  PerfCounter::UpdateHistogram( \
  domain, group_name, short_name, value, min_value, max_value)

#define METRICS_ADD_COUNTER_GROUP(domain, group_name, tag_map) \
  PerfCounter::AddCounterGroup(domain, group_name, tag_map)

#define METRICS_ADD_UD_COUNTER_GROUP(domain, group_ptr) \
  PerfCounter::AddCounterGroup(domain, group_ptr)

namespace metrics {

class PerfCounter final {
 public:
  /// Initialize the PerfCounter functions.
  ///
  /// \param conf The configuration of metrics.
  /// \param io_service The io service for event loop.
  /// \return True for success, and false for failure.
  static bool Start(const MetricsConf &conf, boost::asio::io_service &io_service);

  /// Initialize the PerfCounter functions.
  ///
  /// \param conf The configuration of metrics.
  /// \return True for success, and false for failure.
  static bool Start(const MetricsConf &conf);

  /// Shutdown the PerfCounter.
  static void Shutdown();

  ///
  ///
  /// \param domain
  /// \param group_name
  /// \param short_name
  /// \param value
  static void UpdateCounter(const std::string &domain,
                            const std::string &group_name,
                            const std::string &short_name,
                            int64_t value);

  ///
  /// \param domain
  /// \param group_name
  /// \param short_name
  /// \param value
  static void UpdateGauge(const std::string &domain,
                          const std::string &group_name,
                          const std::string &short_name,
                          int64_t value);

  ///
  /// \param domain
  /// \param group_name
  /// \param short_name
  /// \param value
  /// \param min_value
  /// \param max_value
  static void UpdateHistogram(const std::string &domain,
                              const std::string &group_name,
                              const std::string &short_name,
                              int64_t value,
                              int64_t min_value,
                              int64_t max_value);

  ///
  /// \param domain
  /// \param group_name
  /// \param tag_map
  static void AddCounterGroup(const std::string &domain,
                              const std::string &group_name,
                              const std::map<std::string, std::string> &tag_map);

  /// 
  /// \param domain
  /// \param group
  static void AddCounterGroup(const std::string &domain,
                              std::shared_ptr<MetricsGroupInterface> group);

 private:
  class Impl;
  static std::unique_ptr<Impl> impl_ptr_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_PERF_COUNTER_H
