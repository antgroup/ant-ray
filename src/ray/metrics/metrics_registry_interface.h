#ifndef RAY_METRICS_METRICS_REGISTRY_INTERFACE_H
#define RAY_METRICS_METRICS_REGISTRY_INTERFACE_H

#include <map>
#include <regex>
#include <string>
#include <unordered_set>

#include "prometheus/metric_family.h"
#include "ray/metrics/tags.h"

namespace ray {

namespace metrics {

class RegistryOption {
 public:
  std::string delimiter_{"."};
  std::unordered_set<double> default_percentiles_{60, 90, 99, 99.99};
  std::map<std::string, std::string> default_tagmap_;
};

class MetricsRegistryInterface {
 public:
  virtual ~MetricsRegistryInterface() = default;

  void RegisterCounter(const std::string &metric_name,
                       const TagKeys *tag_keys = nullptr) {
    DoRegisterCounter(metric_name, tag_keys);
  }

  void RegisterGauge(const std::string &metric_name,
                     const TagKeys *tag_keys = nullptr) {
    DoRegisterGauge(metric_name, tag_keys);
  }

  void RegisterHistogram(const std::string &metric_name,
                         const std::unordered_set<double> &percentiles = {},
                         const TagKeys *tag_keys = nullptr) {
    DoRegisterHistogram(metric_name, percentiles, tag_keys);
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
  explicit MetricsRegistryInterface(RegistryOption options)
  : default_tags_(std::move(Tags(options.default_tagmap_))),
    options_(std::move(options)) {}

  MetricsRegistryInterface(const MetricsRegistryInterface &) = delete;
  MetricsRegistryInterface &operator=(const MetricsRegistryInterface &) = delete;

  virtual void DoRegisterCounter(const std::string &metric_name,
                                 const TagKeys *tag_keys) = 0;

  virtual void DoRegisterGauge(const std::string &metric_name,
                               const TagKeys *tag_keys) = 0;

  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   const std::unordered_set<double> &percentiles,
                                   const TagKeys *tag_keys) = 0;

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags) = 0;

  Tags default_tags_;
  RegistryOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_REGISTRY_INTERFACE_H
