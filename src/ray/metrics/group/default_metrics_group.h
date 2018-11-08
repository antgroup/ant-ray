#ifndef RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
#define RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H

#include "ray/metrics/group/metrics_group_interface.h"

namespace ray {

namespace metrics {

class DefaultMetricsGroup : public MetricsGroupInterface {
 public:
   DefaultMetricsGroup(const std::string &domain,
                       const std::string &group_name,
                       const std::map<std::string, std::string> &tag_map = {});

  virtual ~DefaultMetricsGroup() = default;

  virtual void UpdateCounter(const std::string &short_name, int64_t value);

  virtual void UpdateGauge(const std::string &short_name, int64_t value);

  virtual void UpdateHistogram(const std::string &short_name,
                               int64_t value,
                               int64_t min_value,
                               int64_t max_value);

 protected:
  virtual std::string GetMetricName(const std::string &short_name) const;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
