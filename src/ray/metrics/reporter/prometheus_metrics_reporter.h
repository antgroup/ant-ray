#ifndef RAY_METRICS_REPORTER_PROMETHEUS_METRICS_REPORTER_H
#define RAY_METRICS_REPORTER_PROMETHEUS_METRICS_REPORTER_H

#include "ray/metrics/metrics_reporter.h"

namespace ray {

namespace metrics {

class PrometheusMetricsReporter : public MetricsReporter {
 public:
  PrometheusMetricsReporter(ReporterOption options);

  virtual ~PrometheusMetricsReporter();

  virtual bool Init();

  virtual bool Start();

  virtual bool Stop();

  virtual void RegisterRegistry(MetricsRegistry* registry);

  virtual void UnRegisterRegistry(MetricsRegistry* registry);

};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_PROMETHEUS_METRICS_REPORTER_H
