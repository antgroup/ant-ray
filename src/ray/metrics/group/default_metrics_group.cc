#include "default_metrics_group.h"

namespace ray {

namespace metrics {
DefaultMetricsGroup::DefaultMetricsGroup(const std::string &domain,
                                         const std::string &group_name,
                                         MetricsRegistry* registry,
                                         const std::map<std::string, std::string> &tag_map)
    : MetricsGroupInterface(domain, group_name, registry, parent_group, tag_map) {}

void DefaultMetricsGroup::UpdateCounter(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  if (tags_ == nullptr) {
    registry_->RegisterCounter(metric_name);
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->RegisterCounter(metric_name, &tags_->GetTagKeys());
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

void DefaultMetricsGroup::UpdateGauge(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  if (tags_ == nullptr) {
    registry_->RegisterGauge(metric_name);
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->RegisterGauge(metric_name, &tags_->GetTagKeys());
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

void DefaultMetricsGroup::UpdateHistogram(const std::string &short_name, int64_t value) {
  std::string metric_name = GetFullDomain() + registry_->GetDelimiter() + short_name;
  if (tags_ == nullptr) {
    registry_->RegisterHistogram(metric_name);
    registry_->UpdateValue(metric_name, value);
  } else {
    registry_->RegisterHistogram(metric_name, &tags_->GetTagKeys());
    registry_->UpdateValue(metric_name, value, *tags_)
  }
}

}  // namespace metrics

}  // namespace ray
