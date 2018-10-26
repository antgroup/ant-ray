#ifndef RAY_METRICS_METRICS_GROUP_INTERFACE_H
#define RAY_METRICS_METRICS_GROUP_INTERFACE_H

#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "ray/metrics/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class MetricsGroupInterface : public std::enable_shared_from_this<MetricsGroupInterface> {
 public:
  virtual ~MetricsGroupInterface();

  /// update counter by short name
  virtual void UpdateCounter(const std::string &short_name, int64_t value) = 0;

  /// update gauge by short name
  virtual void UpdateGauge(const std::string &short_name, int64_t value) = 0;

  /// update histogram by short name
  virtual void UpdateHistogram(const std::string &short_name, int64_t value) = 0;

  virtual const std::string &GetGroupName() const {
    return group_name_;
  }

  virtual const std::string &GetDomain() const {
    return domain_;
  }

 protected:
  MetricsGroupInterface(const std::string& domain,
                        const std::string& group_name,
                        MetricsRegistry* registry,
                        const std::map<std::string, std::string> &tag_map = {});

  MetricsGroupInterface(const MetricsGroupInterface &) = delete;
  MetricsGroupInterface &operator=(const MetricsGroupInterface &) = delete;

  /// The domain of current group
  std::string domain_;
  /// The name of current group
  std::string group_name_;

  MetricsRegistry *registry_{nullptr};

  /// Tags of current group
  Tags *tags_(nullptr);
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_GROUP_INTERFACE_H
