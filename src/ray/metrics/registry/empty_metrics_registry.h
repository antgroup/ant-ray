#ifndef RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H
#define RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H

#include "ray/metrics/registry/metrics_registry_interface.h"

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
  virtual void DoRegisterCounter(
    const std::string &metric_name, const Tags *tags) {}

  virtual void DoRegisterGauge(
    const std::string &metric_name, const Tags *tags) {}


  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   int64_t min_value,
                                   int64_t max_value,
                                   const std::unordered_set<double> &percentiles,
                                   const Tags *tags) {}

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags) {}
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REGISTRY_EMPTY_METRICS_REGISTRY_H
