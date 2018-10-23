#ifndef RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H
#define RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H

#include "opencensus/tags/tag_map.h"
#include "ray/metrics/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class OpenCensusMetricsRegistry : public MetricsRegistryInterface {
 public:
  OpenCensusMetricsRegistry(RegistryOption options);

  ~OpenCensusMetricsRegistry();

  virtual void RegisterCounter(const std::string &metric_name);

  virtual void RegisterGauge(const std::string &metric_name);

  virtual void RegisterHistogram(const std::string &metric_name);

  virtual void RegisterHistogram(const std::string &metric_name,
                                 const std::unordered_set<double> &percentiles);

  virtual void UpdateValue(const std::string &metric_name,
                           int64_t value);

  virtual void UpdateValue(const std::string &metric_name,
                           int64_t value,
                           const Tags &tags);

  virtual void ExportMetrics(const std::regex &filter,
                             std::vector<prometheus::MetricFamily> *metrics);

 private:
  std::unordered_map<size_t, TagMap> id_to_tag_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H
