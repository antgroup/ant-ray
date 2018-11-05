
#include "perf_counter.h"
#include "metrics_conf.h"
#include "registry/metrics_registry_interface.h"
#include "registry/prometheus_metrics_registry.h"
#include "reporter/metrics_reporter_interface.h"
#include "reporter/prometheus_push_reporter.h"

#include <memory>
#include <boost/noncopyable.hpp>

namespace ray {

namespace metrics {

class PerfCounter::Impl : public boost::noncopyable {
public:
  bool Start(const MetricsConf &conf, boost::asio::io_service &io_service);

  void Shutdown();

    void UpdateCounter(const std::string &domain,
                       const std::string &group_name,
                       const std::string &short_name,
                       int64_t value) {

    }

    void UpdateGauge(const std::string &domain,
                     const std::string &group_name,
                     const std::string &short_name,
                     int64_t value) {

  }

    void UpdateHistogram(const std::string &domain,
                         const std::string &group_name,
                         const std::string &short_name,
                         int64_t value,
                         int64_t min_value,
                         int64_t max_value) {

  }

    void AddCounterGroup(const std::string &domain,
                         const std::string &group_name,
                         const std::map<std::string, std::string> &tag_map = {}) {

  }

    void AddCounterGroup(const std::string &domain,
                         std::shared_ptr<MetricsGroupInterface> group) {

  }

private:
  using NameToGroupMap =
      std::unordered_map<std::string, std::shared_ptr<MetricsGroupInterface>>;

  using DomainToGroupsMap = std::unordered_map<std::string, NameToGroupMap>;

  DomainToGroupsMap perf_counter_ = {};

  //TODO(qwang): We should make these pointers be smart pointers.
  MetricsRegistryInterface *registry_ = nullptr;
  MetricsReporterInterface *reporter_ = nullptr;
};


bool PerfCounter::Start(const MetricsConf &conf,
                               boost::asio::io_service &io_service) {
    if (nullptr != impl_ptr_) {
      return false;
    }

    impl_ptr_ = std::unique_ptr<Impl>(new Impl());
    return impl_ptr_->Start(conf, io_service);
  }

void PerfCounter::Shutdown() {
    return impl_ptr_->Shutdown();
  }

void PerfCounter::UpdateCounter(const std::string &domain,
                                         const std::string &group_name,
                                         const std::string &short_name,
                                         int64_t value) {
    return impl_ptr_->UpdateCounter(domain, group_name, short_name, value);
  }

void PerfCounter::UpdateGauge(const std::string &domain,
                                       const std::string &group_name,
                                       const std::string &short_name,
                                       int64_t value) {
    return impl_ptr_->UpdateGauge(domain, group_name, short_name, value);
  }

void PerfCounter::UpdateHistogram(const std::string &domain,
                                           const std::string &group_name,
                                           const std::string &short_name,
                                           int64_t value,
                                           int64_t min_value,
                                           int64_t max_value) {
    return impl_ptr_->UpdateHistogram(domain, group_name,
                                      short_name,value, min_value, max_value);
  }

void PerfCounter::AddCounterGroup(const std::string &domain,
                                           const std::string &group_name,
                                           const std::map<std::string, std::string> &tag_map) {
    return impl_ptr_->AddCounterGroup(domain, group_name, tag_map);
  }

void PerfCounter::AddCounterGroup(const std::string &domain,
                                           std::shared_ptr<MetricsGroupInterface> group) {
    return impl_ptr_->AddCounterGroup(domain, group);
  }

bool PerfCounter::Impl::Start(const MetricsConf &conf, boost::asio::io_service &io_service) {
  //TODO(qwang): We should use factory to create these instances.
  const auto &registry_name = conf.GetRegistryName();
  if (registry_name == "prometheus") {
    registry_ = new PrometheusMetricsRegistry(conf.GetRegistryOption());
  } else {
    return false;
  }

  const auto &reporter_name = conf.GetReporterName();
  if (reporter_name == "prometheus") {
    reporter_ = new PrometheusPushReporter(conf.GetReporterOption(), io_service);
  } else {
    return false;
  }

  return true;
}

void PerfCounter::Impl::Shutdown() {
  delete registry_;
  registry_ = nullptr;

  delete reporter_;
  reporter_ = nullptr;
}

} // namespace metrics

} // namespace ray
