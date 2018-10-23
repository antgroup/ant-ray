#ifndef RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
#define RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H

#include "ray/metrics/metrics_reporter.h"

namespace ray {

namespace metrics {

class EmptyMetricsReporter : public MetricsReporter {
 public:
  EmptyMetricsReporter(ReporterOption options)
  : MetricsReporter(options) {}

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

  virtual void RegisterRegistry(MetricsRegistry* registry) {}

  virtual void UnRegisterRegistry(MetricsRegistry* registry) {}
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_EMPTY_METRICS_REPORTER_H
