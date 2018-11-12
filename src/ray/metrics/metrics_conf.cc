#include <sstream>

#include "metrics_conf.h"
#include "ray/metrics/metrics_util.h"

namespace ray {

namespace metrics {

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

  registry_name_ = GetFromMap<std::string>(conf_map, "registry_name");
  reporter_name_ = GetFromMap<std::string>(conf_map, "reporter_name");

  // Parse the registry options.
  registry_options_.delimiter_ = GetFromMap<std::string>(conf_map,
      "registry.delimiter");
  registry_options_.bucket_count_ = GetFromMap<size_t>(conf_map,
      "registry.bucket_count");

  std::string registry_default_tag_map_str =
      GetFromMap<std::string>(conf_map, "registry.default_tag_map");
  const auto &tags_map = ParseStringToMap(registry_default_tag_map_str, ':');
  registry_options_.default_tag_map_.insert(tags_map.begin(), tags_map.end());

  // Parse percentiles
  std::string registry_default_percentiles_str =
      GetFromMap<std::string>(conf_map, "registry.default_percentiles");

  {
    std::unordered_set<double> default_percentiles;
    std::string token;
    std::istringstream tokenStream(registry_default_percentiles_str);
    while (std::getline(tokenStream, token, ':')) {
      default_percentiles.insert(atof(token.c_str()));
    }

    registry_options_.default_percentiles_ = default_percentiles;
  }

  // Parse the reporter options.
  reporter_options_.user_name_ =
      GetFromMap<std::string>(conf_map, "reporter.user_name");
  reporter_options_.password_ = GetFromMap<std::string>(conf_map, "reporter.password");
  reporter_options_.job_name_ = GetFromMap<std::string>(conf_map, "reporter.job_name");
  reporter_options_.service_addr_ =
      GetFromMap<std::string>(conf_map, "reporter.service_addr");
  reporter_options_.report_interval_ =
      GetFromMap<std::chrono::seconds>(conf_map, "reporter.report_interval");
  reporter_options_.max_retry_times_ =
      GetFromMap<int64_t>(conf_map, "reporter.max_retry_times");
}

} // namespace metrics

} // namespace ray
