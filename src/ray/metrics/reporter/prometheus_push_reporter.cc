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
    : MetricsReporterInterface(std::move(options)) {
  report_timer_.reset(new boost::asio::deadline_timer(io_service));

  gate_way_.reset(new prometheus::Gateway(options_.service_addr_,
                                          options_.job_name_,
                                          {},
                                          options_.user_name_,
                                          options_.password_));
}

PrometheusPushReporter::PrometheusPushReporter(ReporterOption options)
    : MetricsReporterInterface(std::move(options)) {
  gate_way_.reset(new prometheus::Gateway(options_.service_addr_,
                                          options_.job_name_,
                                          {},
                                          options_.user_name_,
                                          options_.password_));
}

PrometheusPushReporter::~PrometheusPushReporter() {
}

bool PrometheusPushReporter::Init() {
  return true;
}

void PrometheusPushReporter::RegisterRegistry(MetricsRegistryInterface *registry) {
  if (registry != nullptr) {
      std::shared_ptr<RegistryExportHandler> export_handler
      = std::make_shared<RegistryExportHandler>(options_.regex_exp_, registry);
      gate_way_->RegisterCollectable(export_handler);
  }
}

bool PrometheusPushReporter::Start() {
  if (report_timer_ != nullptr) {
    DispatchReportTimer();
    return true;
  }

  report_thread_.reset(new std::thread(
      std::bind(&PrometheusPushReporter::ThreadReportAction, this)));
  return true;
}

void PrometheusPushReporter::ThreadReportAction() {
  while (!is_stopped.load(std::memory_order_acquire)) {
    std::this_thread::sleep_for(options_.report_interval_);
    int ret_code = gate_way_->Push();
    int64_t left_retry_times = options_.max_retry_times_;
    // Retry
    while (ret_code != 200 && (left_retry_times-- > 0)) {
      ret_code = gate_way_->Push();
    }
  }
}

void PrometheusPushReporter::DispatchReportTimer() {
  auto report_period = boost::posix_time::seconds(options_.report_interval_.count());
  report_timer_->expires_from_now(report_period);
  report_timer_->async_wait([this](const boost::system::error_code &error) {
    TimerReportAction();
  });
}

void PrometheusPushReporter::TimerReportAction() {
  // TODO(micafan) Retry on failure.
  // Ignore the return status, otherwise an prometheus exception will be triggered.
  gate_way_->AsyncPush();
  DispatchReportTimer();
}

bool PrometheusPushReporter::Stop() {
  if (is_stopped.load(std::memory_order_acquire)) {
    return false;
  }
  is_stopped.store(true, std::memory_order_release);

  if (report_timer_ != nullptr) {
    boost::system::error_code ec;
    report_timer_->cancel(ec);
  }

  if (report_thread_ != nullptr) {
    report_thread_->join();
  }

  return true;
}

}  // namespace metrics

}  // namespace ray
