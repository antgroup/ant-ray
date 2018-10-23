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

  virtual void RegisterCounter(const std::string &metric_name) = 0;

  virtual void RegisterGauge(const std::string &metric_name) = 0;

  virtual void RegisterHistogram(const std::string &metric_name) = 0;

  virtual void RegisterHistogram(const std::string &metric_name,
                                 const std::unordered_set<double> &percentiles) = 0;

  virtual void UpdateValue(const std::string &metric_name,
                           int64_t value) = 0;

  virtual void UpdateValue(const std::string &metric_name,
                           int64_t value,
                           const Tags &tags) = 0;

  virtual void ExportMetrics(const std::regex &filter,
                             std::vector<prometheus::MetricFamily> *metrics) = 0;

  virtual const std::string &GetDelimiter() {
    return options_.delimiter_;
  }

  virtual const Tags &GetDefaultTags() {
    return default_tags_;
  }

 protected:
  MetricsRegistryInterface(RegistryOption options)
  : default_tags_(std::move(Tags(options.default_tagmap_))),
    options_(std::move(options)) {}

  MetricsRegistryInterface(const MetricsRegistryInterface &) = delete;
  MetricsRegistryInterface &operator=(const MetricsRegistryInterface &) = delete;

  Tags default_tags_;
  RegistryOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_REGISTRY_INTERFACE_H
