#ifndef RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H
#define RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H

#include <chrono>
#include <regex>
#include <string>

#include "ray/metrics/registry/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class ReporterOption {
 public:
  ReporterOption() = default;

  std::string user_name_;
  std::string password_;
  std::string job_name_;

  std::string service_addr_;

  std::chrono::seconds report_interval_{10};

  std::string regex_exp_{".*"};

  int64_t max_retry_times_{3};
};

class MetricsReporterInterface {
 public:
  virtual ~MetricsReporterInterface() = default;

  virtual bool Init() = 0;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual void RegisterRegistry(MetricsRegistryInterface* registry) = 0;

 protected:
  explicit MetricsReporterInterface(ReporterOption options)
  : options_(std::move(options)) {}

  ReporterOption options_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_METRICS_REPORTER_INTERFACE_H
