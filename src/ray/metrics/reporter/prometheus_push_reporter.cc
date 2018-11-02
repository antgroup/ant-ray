#include "prometheus_push_reporter.h"

namespace ray {

namespace metrics {

RegistryExportHandler::RegistryExportHandler(const std::string &regex_filter,
                                             MetricsRegistryInterface *registry)
    : regex_filter_(regex_filter),
      registry_(registry) {}

std::vector<prometheus::MetricFamily> RegistryExportHandler::Collect() {
  std::vector<prometheus::MetricFamily> metrics;
  registry_->ExportMetrics(regex_filter_, &metrics);
  return metrics;
}

PrometheusPushReporter::PrometheusPushReporter(ReporterOption options,
                                               boost::asio::io_service &io_service)
    : MetricsReporterInterface(options),
      io_service_(io_service),
      report_timer_(io_service) {
  gate_way_ = new prometheus::Gateway(options_.service_addr_,
                                      options_.job_name_,
                                      {},
                                      options_.user_name_,
                                      options_.password_);
}

PrometheusPushReporter::~PrometheusPushReporter() {
  delete gate_way_;
  gate_way_ = nullptr;
}

bool PrometheusPushReporter::Init() {
  return true;
}

void PrometheusPushReporter::RegisterRegistry(MetricsRegistryInterface* registry) {
  // TODO(micafan) CHECK(registry != nullptr)
  std::shared_ptr<RegistryExportHandler> export_handler
    = std::make_shared<RegistryExportHandler>(options_.regex_exp_, registry);

  {
    std::lock_guard<std::mutex> guard(mutex_);
    bool ok = handler_map_.insert(std::make_pair(registry, export_handler)).second;
    if (!ok) {
      return;
    }
  }

  gate_way_->RegisterCollectable(export_handler);
}

bool PrometheusPushReporter::Start() {
  DispatchReportTimer();
  return true;
}

void PrometheusPushReporter::DispatchReportTimer() {
  auto report_period = boost::posix_time::seconds(options_.report_interval_.count());
  report_timer_.expires_from_now(report_period);
  report_timer_.async_wait([this](const boost::system::error_code &error) {
    // TODO(micafan) CHECK(!error)
    DoReport();
  });
}

void PrometheusPushReporter::DoReport() {
  // TODO(micafan) retry on failure
  std::future<int> push_rt = gate_way_->AsyncPushAdd();
  DispatchReportTimer();
}

bool PrometheusPushReporter::Stop() {
  boost::system::error_code ec;
  report_timer_.cancel(ec);
  return true;
}

}  // namespace metrics

}  // namespace ray
