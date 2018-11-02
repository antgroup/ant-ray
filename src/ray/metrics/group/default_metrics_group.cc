#include "default_metrics_group.h"

namespace ray {

namespace metrics {
DefaultMetricsGroup::DefaultMetricsGroup(
  const std::string &domain,
  const std::string &group_name,
  MetricsRegistryInterface *registry,
  const std::map<std::string, std::string> &tag_map)
    : MetricsGroupInterface(domain, group_name, tag_map) {
  SetRegistry(registry);
}

void DefaultMetricsGroup::UpdateCounter(const std::string &short_name, int64_t value) {
  std::string metric_name = GetMetricName(short_name);
  registry_->RegisterCounter(metric_name, tags_);
  registry_->UpdateValue(metric_name, value, tags_);
}

void DefaultMetricsGroup::UpdateGauge(const std::string &short_name, int64_t value) {
  std::string metric_name = GetMetricName(short_name);
  registry_->RegisterGauge(metric_name, tags_);
  registry_->UpdateValue(metric_name, value, tags_);
}

void DefaultMetricsGroup::UpdateHistogram(const std::string &short_name,
                                          int64_t value,
                                          int64_t min_value,
                                          int64_t max_value) {
  std::string metric_name = GetMetricName(short_name);
  registry_->RegisterHistogram(metric_name, min_value, max_value, tags_);
  registry_->UpdateValue(metric_name, value, tags_);
}

std::string DefaultMetricsGroup::GetMetricName(const std::string &short_name) const {
  std::string metric_name = domain_
    + registry_->GetDelimiter() + group_name_
    + registry_->GetDelimiter() + short_name;
  return metric_name;
}

}  // namespace metrics

}  // namespace ray
