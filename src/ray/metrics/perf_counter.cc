
#include "perf_counter.h"

#include <memory>
#include <boost/noncopyable.hpp>

class PerfCounter::Impl : public boost::noncopyable {
public:

  bool Start(MetricsConf conf, boost::asio::io_service &io_service);

  void Shutdown();

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
                       const std::map<std::string, std::string> &tag_map = {});

  void AddCounterGroup(const std::string &domain,
                       std::shared_ptr<MetricsGroupInterface> group);


private:
  using NameToGroupMap =
      std::unordered_map<std::string, std::shared_ptr<MetricsGroupInterface>>;

  using DomainToGroupsMap = std::unordered_map<std::string, NameToGroupMap>;

  DomainToGroupsMap  perf_counter_;
  //registry
  //reporter
};


static bool PerfCounter::Start(MetricsConf conf, boost::asio::io_service &io_service) {
  if (nullptr != impl_ptr_) {
    return false;
  }

  impl_ptr_ = std::unique_ptr<Impl>(new Impl());
  return impl_ptr_->Start();
}

static void Shutdown() {
  return impl_ptr_->Shutdown();
}

static void PerfCounter::UpdateCounter(const std::string &domain,
                                       const std::string &group_name,
                                       const std::string &short_name,
                                       int64_t value) {
  return impl_ptr_->UpdateCounter(domain, group_name, short_name, value);
}

static void PerfCounter::UpdateGauge(const std::string &domain,
                                     const std::string &group_name,
                                     const std::string &short_name,
                                     int64_t value) {
  return impl_ptr_->UpdateGauge(domain, group_name, short_name, value);
}

static void PerfCounter::UpdateHistogram(const std::string &domain,
                                         const std::string &group_name,
                                         const std::string &short_name,
                                         int64_t value,
                                         int64_t min_value,
                                         int64_t max_value) {
  return impl_ptr_->UpdateHistogram(domain, group_name, short_name, value, min_value, max_value);
}

static void PerfCounter::AddCounterGroup(const std::string &domain,
                                         const std::string &group_name,
                                         const std::map<std::string, std::string> &tag_map) {
  return impl_ptr_->AddCounterGroup(domain, group_name, tag_map);
}

static void PerfCounter::AddCounterGroup(const std::string &domain,
                                         std::shared_ptr<MetricsGroupInterface> group) {
  return impl_ptr_->AddCounterGroup(domain, group);
}
