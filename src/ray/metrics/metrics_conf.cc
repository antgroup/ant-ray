#include <sstream>

#include "metrics_conf.h"
#include "ray/metrics/metrics_util.h"

namespace ray {

namespace metrics {

// Helper function to find a value from
// the given map and assign it to the field.
template <typename T>
void FindAndAssignToField(
    const std::unordered_map<std::string, std::string> &from,
    const std::string &key,
    T *field) {

  auto it = from.find(key);
  if (it != from.end()) {
    *field = static_cast<T>(atoi(it->second.c_str()));
  }
}

//TODO(qwang): Return field instead of output parameter.
template <>
void FindAndAssignToField<std::string>(
    const std::unordered_map<std::string, std::string> &from,
    const std::string &key,
    std::string *field) {

  auto it = from.find(key);
  if (it != from.end()) {
    *field = it->second;
  }
}

MetricsConf::MetricsConf(const std::string &conf_str) {
  Init(conf_str);
  if (registry_name_.empty()) {
    registry_name_ = kMetricsOptionPrometheusName;
  }
  if (reporter_name_.empty()) {
    reporter_name_ = kMetricsOptionPrometheusName;
  }
}

const RegistryOption &MetricsConf::GetRegistryOption() const {
  return registry_options_;
}

const ReporterOption &MetricsConf::GetReporterOption() const {
  return reporter_options_;
}

const std::string &MetricsConf::GetRegistryName() const {
  return registry_name_;
}

const std::string &MetricsConf::GetReporterName() const {
  return reporter_name_;
}

void MetricsConf::Init(const std::string &conf_str) {
  const auto &conf_map = ParseStringToMap(conf_str, ',');

  FindAndAssignToField<std::string>(conf_map, "registry_name", &registry_name_);
  FindAndAssignToField<std::string>(conf_map, "reporter_name", &reporter_name_);

  // Parse the registry options.
  FindAndAssignToField<std::string>(conf_map,
      "registry.delimiter", &registry_options_.delimiter_);
  FindAndAssignToField<size_t>(conf_map,
      "registry.bucket_count", &registry_options_.bucket_count_);

  std::string registry_default_tag_map_str;
  FindAndAssignToField<std::string>(conf_map,
      "registry.default_tag_map", &registry_default_tag_map_str);
  const auto &tags_map = ParseStringToMap(conf_str, ':');
  registry_options_.default_tag_map_.insert(tags_map.begin(), tags_map.end());

  // Parse percentiles
  std::string registry_default_percentiles;
  FindAndAssignToField<std::string>(conf_map,
      "registry.default_percentiles", &registry_default_percentiles);

  {
    std::string token;
    std::istringstream tokenStream(registry_default_percentiles);
    while (std::getline(tokenStream, token, ':')) {
      registry_options_.default_percentiles_.insert(atof(token.c_str()));
    }
  }

  // Parse the reporter options.
  FindAndAssignToField<std::string>(conf_map,
      "reporter.user_name", &reporter_options_.user_name_);
  FindAndAssignToField<std::string>(conf_map,
      "reporter.password", &reporter_options_.password_);
  FindAndAssignToField<std::string>(conf_map,
      "reporter.job_name", &reporter_options_.job_name_);
  FindAndAssignToField<std::string>(conf_map,
      "reporter.service_addr", &reporter_options_.service_addr_);
  FindAndAssignToField<std::chrono::seconds>(conf_map,
      "reporter.report_interval", &reporter_options_.report_interval_);
  FindAndAssignToField<int64_t>(conf_map,
      "reporter.max_retry_times", &reporter_options_.max_retry_times_);
}


} // namespace metrics

} // namespace ray
