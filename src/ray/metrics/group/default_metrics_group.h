#ifndef RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
#define RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H

#include "ray/metrics/metrics_group.h"

namespace ray {

namespace metrics {

class DefaultMetricsGroup : public MetricsGroupInterface {
 public:
   DefaultMetricsGroup(const std::string &domain,
                       const std::string &group_name,
                       const std::map<std::string, std::string> &tag_map,
                       std::shared_ptr<MetricsGroupInterface> parent_group,
                       MetricsRegistryInterface* registry);

  virtual ~DefaultMetricsGroup() = default;

  virtual void UpdateCounter(const std::string &short_name, int64_t value);

  virtual void UpdateGauge(const std::string &short_name, int64_t value);

  virtual void UpdateHistogram(const std::string &short_name, int64_t value);

  virtual void AddGroup(std::shared_ptr<MetricsGroupInterface> childGroup);

  virtual std::shared_ptr<MetricsGroupInterface> AddGroup(
    const std::string &domain,
    const std::string &group_name,
    const std::map<std::string, std::string> &tag_map);

  virtual void RemoveGroup(const std::string &group_name);

};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_GROUP_DEFAULT_METRICS_GROUP_H
