#ifndef RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
#define RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H

#include "ray/metrics/metrics_group.h"

namespace ray {

namespace metrics {

class DefaultMetricsGroup : public MetricsGroupInterface {
 public:
   DefaultMetricsGroup(const std::string &domain,
                       const std::string &group_name,
                       MetricsRegistryInterface* registry,
                       const std::map<std::string, std::string> &tag_map = {});

  virtual ~DefaultMetricsGroup() = default;

  virtual void UpdateCounter(const std::string &short_name, int64_t value);

  virtual void UpdateGauge(const std::string &short_name, int64_t value);

  virtual void UpdateHistogram(const std::string &short_name, int64_t value);
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
