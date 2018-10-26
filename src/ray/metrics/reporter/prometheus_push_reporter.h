#ifndef RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
#define RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H

#include <memory>
#include <mutex>
#include <boost/asio.hpp>

#include "prometheus/gateway.h"
#include "ray/metrics/metrics_reporter_interface.h"

namespace ray {

namespace metrics {

class RegistryExportHandler : public prometheus::Collect {
 public:
  RegistryExportHandler(const std::string &regex_filter,
                        MetricsRegistryInterface *registry);

  virtual std::vector<prometheus::MetricFamily> Collect();

 private:
  const std::string &regex_filter_,
  MetricsRegistryInterface* registry_;
};

class PrometheusPushReporter : public MetricsReporterInterface {
 public:
  PrometheusPushReporter(ReporterOption options,
                         boost::asio::io_service &io_service);

  virtual ~PrometheusPushReporter();

  virtual bool Init();

  virtual bool Start();

  virtual bool Stop();

  virtual void RegisterRegistry(MetricsRegistryInterface* registry);

 private:
  void DispatchReportTimer();

  void DoReport();

  std::mutex mutex_;
  boost::asio::io_service &io_service_;
  /// A timer that ticks every ReporterOption.report_interval_ seconds
  boost::asio::deadline_timer report_timer_;
  /// Prometheus gateway
  prometheus::Gateway* gate_way_{nullptr};
  /// The handlers used for collect metrics from registrys
  typedef std::unordered_set<MetricsRegistryInterface*,
          std::shared_ptr<RegistryExportHandler>> ExportHandlerMap;
  ExportHanderMap handler_map_;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_REPORTER_PROMETHEUS_PUSH_REPORTER_H
