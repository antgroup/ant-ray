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

  /// add child group
  virtual void AddGroup(std::shared_ptr<MetricsGroupInterface> childGroup) = 0;

  /// create child group
  virtual std::shared_ptr<MetricsGroupInterface> AddGroup(
    const std::string &domain,
    const std::string &group_name,
    const std::map<std::string, std::string> &tag_map) = 0;

  /// remove child group
  virtual void RemoveGroup(const std::string &group_name) = 0;

  virtual const std::string &GetGroupName() const {
    return group_name_;
  }

  virtual const std::string &GetDomain() const {
    return domain_;
  }

  virtual const std::string &GetFullDomain();

 protected:
  MetricsGroupInterface(const std::string& domain,
                        const std::string& group_name,
                        const std::map<std::string, std::string> &tag_map,
                        std::shared_ptr<MetricsGroupInterface> parent_group,
                        MetricsRegistry* registry);

  MetricsGroupInterface(const MetricsGroupInterface &) = delete;
  MetricsGroupInterface &operator=(const MetricsGroupInterface &) = delete;

  /// The domain of current group
  std::string domain_;
  /// The name of current group
  std::string group_name_;
  /// Full domain of current group in GroupTree(splicing by the specified symbol)
  std::string full_domain_;

  /// Parent of current group
  std::shared_ptr<MetricsGroupInterface> parent_group_{nullptr};

  /// Childs of current group
  typedef std::unordered_map<std::string, std::shared_ptr<MetricsGroupInterface> > ChildGroupMap;
  ChildGroupMap childs_;

  MetricsRegistry *registry_{nullptr};
  /// tags of current group
  Tags *tags_{nullptr};
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_GROUP_INTERFACE_H
