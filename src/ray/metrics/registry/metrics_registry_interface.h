#ifndef RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H
#define RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H

#include <map>
#include <regex>
#include <string>
#include <unordered_set>
#include <boost/noncopyable.hpp>

#include "prometheus/metric_family.h"
#include "ray/metrics/tag/tags.h"

namespace ray {

namespace metrics {

enum class MetricType : int8_t {
  kCounter,
  kGauge,
  kHistogram,
};

class RegistryOption {
 public:
  RegistryOption() = default;

  std::string delimiter_{"."};
  std::map<std::string, std::string> default_tag_map_{};

  // Histogram params
  std::unordered_set<double> default_percentiles_{0.01, 1, 60, 90, 99, 99.99};
  size_t bucket_count_{20};
};

class MetricsRegistryInterface : public boost::noncopyable {
 public:
  virtual ~MetricsRegistryInterface() = default;

  void RegisterCounter(const std::string &metric_name,
                       const Tags *tags = nullptr) {
    DoRegisterCounter(metric_name, tags);
  }

  void RegisterGauge(const std::string &metric_name,
                     const Tags *tags = nullptr) {
    DoRegisterGauge(metric_name, tags);
  }

  void RegisterHistogram(
    const std::string &metric_name,
    int64_t min_value,
    int64_t max_value,
    const std::unordered_set<double> &percentiles = std::unordered_set<double>(),
    const Tags *tags = nullptr) {
    DoRegisterHistogram(metric_name, min_value, max_value, percentiles, tags);
  }

  void UpdateValue(const std::string &metric_name,
                   int64_t value,
                   const Tags *tags = nullptr) {
    DoUpdateValue(metric_name, value, tags);
  }

  virtual void ExportMetrics(const std::string &regex_filter,
                             std::vector<prometheus::MetricFamily> *metrics) = 0;

  virtual const std::string &GetDelimiter() {
    return options_.delimiter_;
  }

  virtual const Tags &GetDefaultTags() {
    return default_tags_;
  }

 protected:
  explicit MetricsRegistryInterface(const RegistryOption &options)
  : default_tags_(std::move(Tags(options.default_tag_map_))),
    options_(std::move(options)) {}

  virtual void DoRegisterCounter(const std::string &metric_name,
                                 const Tags *tags) = 0;

  virtual void DoRegisterGauge(const std::string &metric_name,
                               const Tags *tags) = 0;

  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   int64_t min_value,
                                   int64_t max_value,
                                   const std::unordered_set<double> &percentiles,
                                   const Tags *tags) = 0;

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags) = 0;

  std::vector<double> GenBucketBoundaries(int64_t min_value,
                                          int64_t max_value,
                                          size_t bucket_count) const;

  Tags default_tags_;
  RegistryOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_METRICS_REGISTRY_INTERFACE_H
