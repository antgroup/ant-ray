#ifndef RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
#define RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H

#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class EmptyMetricsReporter : public MetricsReporterInterface {
 public:
  explicit EmptyMetricsReporter(ReporterOption options)
  : MetricsReporterInterface(options) {}

  virtual ~EmptyMetricsReporter() = default;

  virtual bool Init() {
    return true;
  }

  virtual bool Start() {
    return true;
  }

  virtual bool Stop() {
    return true;
  }

  virtual void RegisterRegistry(MetricsRegistryInterface* registry) {}

};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
