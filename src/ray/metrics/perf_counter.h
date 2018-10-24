#ifndef RAY_METRICS_PERF_COUNTER_H
#define RAY_METRICS_PERF_COUNTER_H

#include "ray/metrics/metrics_group_interface.h"

namespace ray {

namespace metrics {

class PerfCounter final {
 public:
  static void UpdateCounter(const std::string &domain,
                            const std::string &group_name,
                            const std::string &short_name,
                            int64_t value);

  static void UpdateGauge(const std::string &domain,
                          const std::string &group_name,
                          const std::string &short_name,
                          int64_t value);

  static void UpdateHistogram(const std::string &domain,
                              const std::string &group_name,
                              const std::string &short_name,
                              int64_t value);

  static void AddGroup(const std::string &domain,
                       const std::string &group_name,
                       const std::map<std::string, std::string> &tag_map = {});
 private:
  typedef std::unordered_map<std::string, std::shared_ptr<MetricsGroupInterface>> NameToGroupMap;
  typedef std::unordered_map<std::string, NameToGroupMap> DomainToGroupsMap;
  DomainToGroupsMap perf_counters_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_PERF_COUNTER_H
