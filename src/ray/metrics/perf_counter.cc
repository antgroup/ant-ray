#include "perf_counter.h"

#include <memory>
#include <mutex>
#include <boost/noncopyable.hpp>

#include "ray/metrics/group/default_metrics_group.h"
#include "ray/metrics/metrics_conf.h"
#include "ray/metrics/metrics_util.h"
#include "ray/metrics/registry/empty_metrics_registry.h"
#include "ray/metrics/registry/metrics_registry_interface.h"
#include "ray/metrics/registry/prometheus_metrics_registry.h"
#include "ray/metrics/reporter/empty_metrics_reporter.h"
#include "ray/metrics/reporter/metrics_reporter_interface.h"
#include "ray/metrics/reporter/prometheus_push_reporter.h"

namespace ray {

namespace metrics {

class PerfCounter::Impl : public boost::noncopyable {
public:
  virtual ~Impl() = default;

  bool Start(const MetricsConf &conf, boost::asio::io_service &io_service);

  bool Start(const MetricsConf &conf);

  void Shutdown();

  // If not found, we should insert one.
  void UpdateCounter(const std::string &domain,
                     const std::string &group_name,
                     const std::string &short_name,
                     int64_t value);

  void UpdateGauge(const std::string &domain,
                   const std::string &group_name,
                   const std::string &short_name,
                   int64_t value);

  void UpdateHistogram(const std::string &domain,
                       const std::string &group_name,
                       const std::string &short_name,
                       int64_t value,
                       int64_t min_value,
                       int64_t max_value);

  void AddCounterGroup(const std::string &domain,
                       const std::string &group_name,
                       const std::map<std::string, std::string> &tag_map);

  void AddCounterGroup(const std::string &domain,
                       std::shared_ptr<MetricsGroupInterface> group);

private:
  std::shared_ptr<MetricsGroupInterface> DoAddGroup(
    const std::string &domain,
    const std::string &group_name,
    const std::map<std::string, std::string> &tag_map = {},
    std::shared_ptr<MetricsGroupInterface> group = nullptr);

protected:
  std::mutex mutex_;

  using NameToGroupMap =
      std::unordered_map<std::string, std::shared_ptr<MetricsGroupInterface>>;
  using DomainToGroupsMap = std::unordered_map<std::string, NameToGroupMap>;

  DomainToGroupsMap perf_counter_;

  //TODO(qwang): We should make these pointers be smart pointers.
  MetricsRegistryInterface *registry_{nullptr};
  MetricsReporterInterface *reporter_{nullptr};
};

std::unique_ptr<PerfCounter::Impl> PerfCounter::impl_ptr_ = nullptr;

bool PerfCounter::Start(const MetricsConf &conf,
                        boost::asio::io_service &io_service) {
  if (nullptr != impl_ptr_) {
    return false;
  }

  impl_ptr_ = std::unique_ptr<Impl>(new Impl());
  return impl_ptr_->Start(conf, io_service);
}

bool PerfCounter::Start(const MetricsConf &conf) {
  if (nullptr != impl_ptr_) {
    return false;
  }

  impl_ptr_ = std::unique_ptr<Impl>(new Impl());
  return impl_ptr_->Start(conf);
}

void PerfCounter::Shutdown() {
  impl_ptr_->Shutdown();
  impl_ptr_ = nullptr;
}

void PerfCounter::UpdateCounter(const std::string &domain,
                                const std::string &group_name,
                                const std::string &short_name,
                                int64_t value) {
  impl_ptr_->UpdateCounter(domain, group_name, short_name, value);
}

void PerfCounter::UpdateGauge(const std::string &domain,
                              const std::string &group_name,
                              const std::string &short_name,
                              int64_t value) {
  impl_ptr_->UpdateGauge(domain, group_name, short_name, value);
}

void PerfCounter::UpdateHistogram(const std::string &domain,
                                  const std::string &group_name,
                                  const std::string &short_name,
                                  int64_t value,
                                  int64_t min_value,
                                  int64_t max_value) {
  impl_ptr_->UpdateHistogram(domain, group_name,
                             short_name,value, min_value, max_value);
}

void PerfCounter::AddCounterGroup(const std::string &domain,
                                  const std::string &group_name,
                                  const std::map<std::string, std::string> &tag_map) {
  impl_ptr_->AddCounterGroup(domain, group_name, tag_map);
}

void PerfCounter::AddCounterGroup(const std::string &domain,
                                  std::shared_ptr<MetricsGroupInterface> group) {
  impl_ptr_->AddCounterGroup(domain, group);
}

bool PerfCounter::Impl::Start(const MetricsConf &conf,
                              boost::asio::io_service &io_service) {
  //TODO(qwang): We should use factory to create these instances.
  const auto &registry_name = conf.GetRegistryName();
  if (registry_name == kMetricsOptionPrometheusName) {
    registry_ = new PrometheusMetricsRegistry(conf.GetRegistryOption());
  } else if (registry_name == kMetricsOptionEmptyName) {
    registry_ = new EmptyMetricsRegistry(conf.GetRegistryOption());
  } else {
    return false;
  }

  const auto &reporter_name = conf.GetReporterName();
  if (reporter_name == kMetricsOptionPrometheusName) {
    reporter_ = new PrometheusPushReporter(conf.GetReporterOption(), io_service);
  } else if (reporter_name == kMetricsOptionEmptyName) {
    reporter_ = new EmptyMetricsReporter(conf.GetReporterOption());
  } else {
    delete registry_;
    return false;
  }
  reporter_->RegisterRegistry(registry_);
  if (!reporter_->Init()) {
    return false;
  }
  if (!reporter_->Start()) {
    return false;
  }

  return true;
}

bool PerfCounter::Impl::Start(const MetricsConf &conf) {
  const auto &registry_name = conf.GetRegistryName();
  if (registry_name == kMetricsOptionPrometheusName) {
    registry_ = new PrometheusMetricsRegistry(conf.GetRegistryOption());
  } else if (registry_name == kMetricsOptionEmptyName) {
    registry_ = new EmptyMetricsRegistry(conf.GetRegistryOption());
  } else {
    return false;
  }

  const auto &reporter_name = conf.GetReporterName();
  if (reporter_name == kMetricsOptionPrometheusName) {
    reporter_ = new PrometheusPushReporter(conf.GetReporterOption());
  } else if (reporter_name == kMetricsOptionEmptyName) {
    reporter_ = new EmptyMetricsReporter(conf.GetReporterOption());
  } else {
    delete registry_;
    return false;
  }
  reporter_->RegisterRegistry(registry_);
  if (!reporter_->Init()) {
    return false;
  }
  if (!reporter_->Start()) {
    return false;
  }

  return true;
}

void PerfCounter::Impl::Shutdown() {
  reporter_->Stop();

  delete reporter_;
  reporter_ = nullptr;

  delete registry_;
  registry_ = nullptr;
}

void PerfCounter::Impl::UpdateCounter(const std::string &domain,
                                      const std::string &group_name,
                                      const std::string &short_name,
                                      int64_t value) {
  auto group_ptr = DoAddGroup(domain, group_name);
  group_ptr->UpdateCounter(short_name, value);
}

void PerfCounter::Impl::UpdateGauge(const std::string &domain,
                                    const std::string &group_name,
                                    const std::string &short_name,
                                    int64_t value) {
  auto group_ptr = DoAddGroup(domain, group_name);
  group_ptr->UpdateGauge(short_name, value);

}

void PerfCounter::Impl::UpdateHistogram(const std::string &domain,
                                        const std::string &group_name,
                                        const std::string &short_name,
                                        int64_t value,
                                        int64_t min_value,
                                        int64_t max_value) {
  auto group_ptr = DoAddGroup(domain, group_name);
  group_ptr->UpdateHistogram(short_name, value, min_value, max_value);
}

void PerfCounter::Impl::AddCounterGroup(
  const std::string &domain,
  const std::string &group_name,
  const std::map<std::string, std::string> &tag_map) {
  DoAddGroup(domain, group_name, tag_map);
}

void PerfCounter::Impl::AddCounterGroup(const std::string &domain,
                                        std::shared_ptr<MetricsGroupInterface> group) {
  const std::string &group_name = group->GetGroupName();
  DoAddGroup(domain, group_name, {}, group);
}

std::shared_ptr<MetricsGroupInterface> PerfCounter::Impl::DoAddGroup(
  const std::string &domain,
  const std::string &group_name,
  const std::map<std::string, std::string> &tag_map,
  std::shared_ptr<MetricsGroupInterface> group) {

  std::lock_guard<std::mutex> lock(mutex_);

  auto domain_it = perf_counter_.find(domain);
  if (domain_it == perf_counter_.end()) {
    std::shared_ptr<MetricsGroupInterface> new_group = (group == nullptr) ?
      std::make_shared<DefaultMetricsGroup>(domain, group_name, tag_map)
      : group;
    new_group->SetRegistry(registry_);

    NameToGroupMap group_map;
    group_map.emplace(group_name, new_group);
    perf_counter_.emplace(domain, group_map);
    return new_group;
  }

  auto &group_map = domain_it->second;
  auto group_it = group_map.find(group_name);
  if (group_it == group_map.end()) {
    std::shared_ptr<MetricsGroupInterface> new_group = (group == nullptr) ?
      std::make_shared<DefaultMetricsGroup>(domain, group_name, tag_map)
      : group;
    new_group->SetRegistry(registry_);

    group_map.emplace(group_name, new_group);
    return new_group;
  }

  return group_it->second;
}

} // namespace metrics

} // namespace ray
