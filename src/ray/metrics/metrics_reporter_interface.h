#ifndef RAY_METRICS_METRICS_REPORTER_H
#define RAY_METRICS_METRICS_REPORTER_H

#include <chrono>
#include <regex>
#include <string>

#include "ray/metrics/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class ReporterOption {
 public:
  std::string service_addr_;
  std::chrono::seconds report_interval_{10};
  std::regex filter_{".*"};
};

class MetricsReporterInterface {
 public:
  virtual ~MetricsReporterInterface() = default;

  virtual bool Init() = 0;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual void RegisterRegistry(MetricsRegistryInterface* registry) = 0;

  virtual void UnRegisterRegistry(MetricsRegistryInterface* registry) = 0;

 protected:
  MetricsReporterInterface(ReporterOption options)
  : options_(std::move(options)) {}

  ReporterOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_METRICS_REPORTER_H
