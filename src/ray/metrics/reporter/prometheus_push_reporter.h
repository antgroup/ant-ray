#ifndef RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
#define RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H

#include <memory>
#include <mutex>
#include <boost/asio.hpp>

#include "prometheus/collectable.h"
#include "prometheus/gateway.h"
#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class RegistryExportHandler : public prometheus::Collectable {
 public:
  RegistryExportHandler(const std::string &regex_filter,
                        MetricsRegistryInterface *registry);

  virtual std::vector<prometheus::MetricFamily> Collect();

 private:
  const std::string &regex_filter_;
  MetricsRegistryInterface *registry_;
};

class PrometheusPushReporter : public MetricsReporterInterface {
 public:
  PrometheusPushReporter(ReporterOption options,
                         boost::asio::io_service &io_service);

  virtual ~PrometheusPushReporter();

  virtual bool Init();

  virtual bool Start();

  virtual bool Stop();

  virtual void RegisterRegistry(MetricsRegistryInterface *registry);

 private:
  void DispatchReportTimer();

  void DoReport();

  /// A timer that ticks every ReporterOption.report_interval_ seconds
  boost::asio::deadline_timer report_timer_;
  /// Prometheus gateway
  prometheus::Gateway* gate_way_{nullptr};
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
