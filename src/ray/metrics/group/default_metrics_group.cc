#include "default_metrics_group.h"

namespace ray {

namespace metrics {
DefaultMetricsGroup::DefaultMetricsGroup(const std::string &domain,
                                         const std::string &group_name,
                                         const std::map<std::string, std::string> &tag_map,
                                         std::shared_ptr<MetricsGroupInterface> parent_group,
                                         MetricsRegistry* registry)
    : MetricsGroupInterface(domain, group_name, tag_map, parent_group, registry) {}

void DefaultMetricsGroup::UpdateCounter(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  registry_->RegisterCounter(metric_name);
  if (tags_ == nullptr) {
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

void DefaultMetricsGroup::UpdateGauge(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  registry_->RegisterGauge(metric_name);
  if (tags_ == nullptr) {
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

void DefaultMetricsGroup::UpdateHistogram(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  registry_->RegisterHistogram(metric_name);
  if (tags_ == nullptr) {
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

void DefaultMetricsGroup::AddGroup(std::shared_ptr<MetricsGroupInterface> childGroup) {
  childs_.emplace(childGroup->GetGroupName(), childGroup);
}

std::shared_ptr<MetricsGroupInterface> DefaultMetricsGroup::AddGroup(
  const std::string &domain,
  const std::string &group_name,
  const std::map<std::string, std::string> &tag_map) {
  std::shared_ptr<MetricsGroupInterface> child = std::make_shared<DefaultMetricsGroup>(
    domain, group_name, tag_map, shared_from_this(), registry_);
}

void DefaultMetricsGroup::RemoveGroup(const std::string &group_name) {

}

}  // namespace metrics

}  // namespace ray
