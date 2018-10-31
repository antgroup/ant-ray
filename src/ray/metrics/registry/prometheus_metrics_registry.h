#ifndef RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H
#define RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H

#include <boost/thread/pthread/shared_mutex.hpp>

#include "ray/metrics/metrics_registry_interface.h"
#include "prometheus/registry.h"

namespace ray {

namespace metrics {

class MetricFamily {
 public:
  MetricFamily(
    MetricType type,
    prometheus::Registry *registry,
    const std::string &metric_name,
    const std::vector<int64_t> &bucket_boundaries = {},
    const Tags *tags = nullptr);

  ~MetricFamily() = default;

  /// Update value with tags
  void UpdateValue(int64_t value, const Tags *tags = nullptr);

 private:
  /// Get counter by tags
  prometheus::Counter &GetCounter(const Tags *tags);
  /// Get gauge by tags
  prometheus::Gauge &GetGauge(const Tags *tags);
  /// Get histogram by tags
  prometheus::Histogram &GetHistogram(const Tags *tags);

  MetricType type_;
  /// Container of all counters
  prometheus::Family<prometheus::Counter> *counter_family_{nullptr};
  /// Countesr of each tag
  std::unordered_map<size_t, prometheus::Counter&> tag_to_counter_map_;
  /// Container of all gauges
  prometheus::Family<prometheus::Gauge> *gauge_family_{nullptr};
  /// Gauges of each tag
  std::unordered_map<size_t, prometheus::Gauge&> tag_to_gauge_map_;
  /// Container of all histogram
  prometheus::Family<prometheus::Histogram> *histogram_family_{nullptr};
  /// Histograms of each tag
  std::unordered_map<size_t, prometheus::Histogram&> tag_to_histogram_map_;
  /// Boundary of histogram bucket
  const std::vector<int64_t> &bucket_boundaries_;
  /// Shared lock
  boost::shared_mutex mutex_;
  typedef boost::unique_lock<boost::shared_mutex> ReadLock;
  typedef boost::shared_lock<boost::shared_mutex> WriteLock;
};

class PrometheusMetricsRegistry : public MetricsRegistryInterface {
 public:
  PrometheusMetricsRegistry(RegistryOption options);

  virtual ~PrometheusMetricsRegistry() = default;

 protected:
  virtual void DoRegisterCounter(const std::string &metric_name,
                                 const Tags *tags);

  virtual void DoRegisterGauge(const std::string &metric_name,
                               const Tags *tags);

  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   const std::unordered_set<double> &percentiles,
                                   const Tags *tags);

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags);

 private:
  /// prometheus registry of metric
  prometheus::Registry registry_;
  /// All metrics
  std::unordered_map<std::string, MetricFamily> metric_map_;
  /// Shared lock
  boost::shared_mutex mutex_;
  typedef boost::unique_lock<boost::shared_mutex> ReadLock;
  typedef boost::shared_lock<boost::shared_mutex> WriteLock;
};

}  // namespace metrics

}  // namespace ray


#endif  // RAY_METRICS_REGISTRY_PROMETHEUS_METRICS_REGISTRY_H
