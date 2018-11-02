#ifndef RAY_METRICS_METRICS_CONF_H
#define RAY_METRICS_METRICS_CONF_H

#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class MetricsConf {
 public:
  MetricsConf();

  ~MetricsConf() = default;

  const RegistryOption &GetRegistryOption() const;

  const ReporterOption &GetReporterOption() const;

  const std::string &GetRegistryName() const;

  const std::string &GetReporterName() const;

 private:
  std::string registry_name_{"prometheus"};
  RegistryOption registry_options_;

  std::string reporter_name_{"prometheus"};
  ReporterOption reporter_options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_CONF_H
