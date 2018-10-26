#ifndef RAY_METRICS_EMPTY_METRICS_REGISTRY_H
#define RAY_METRICS_EMPTY_METRICS_REGISTRY_H

#include "opencensus/tags/tag_map.h"
#include "ray/metrics/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class EmptyMetricsRegistry : public MetricsRegistryInterface {
 public:
  explicit EmptyMetricsRegistry(RegistryOption options)
  : MetricsRegistryInterface(std::move(options)) {}

  virtual ~EmptyMetricsRegistry() {}

  virtual void ExportMetrics(const std::string &regex_filter,
                             std::vector<prometheus::MetricFamily> *metrics) {}

 protected:
  virtual void DoRegisterCounter(const std::string &metric_name, const TagKeys *tag_keys) {}

  virtual void DoRegisterGauge(const std::string &metric_name, const TagKeys *tag_keys) {}


  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   const std::unordered_set<double> &percentiles) {}

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags) {}

 private:
  std::unordered_map<size_t, TagMap> id_to_tagmap_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_EMPTY_METRICS_REGISTRY_H
