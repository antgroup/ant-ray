#include "prometheus_metrics_registry.h"

namespace ray {

namespace metrics {

MetricFamily::MetricFamily(
  MetricType type,
  prometheus::Registry *registry,
  const std::string &metric_name,
  const std::vector<int64_t> &bucket_boundaries,
  const Tags *tags)
    : type_(type),
      bucket_boundaries_(bucket_boundaries) {
  // const std::map<std::string, std::string> &labels =
  switch (type_) {
  case MetricType::kCount:
    if (tags != nullptr) {
      counter_family_ = &prometheus::detail::BuildCounter()
        .Name(metric_name)
        .Labels(tags->GetTags())
        .Register(*registry);
    } else {
      counter_family_ = &prometheus::detail::BuildCounter()
        .Name(metric_name)
        .Register(*registry);
    }
    break;
  case MetricType::kGauge:
    {
      gauge_family_ = &prometheus::detail::BuildGauge()
        .Name(metric_name)
        .Labels(tags->GetTags())
        .Register(*registry);
    } else {
      gauge_family_ = &prometheus::detail::BuildGauge()
        .Name(metric_name)
        .Register(*registry);
    }
    break;
  case MetricType::kHistogram:
    {
      histogram_family_ = &prometheus::detail::BuildHistogram()
        .Name(metric_name)
        .Labels(tags->GetTags())
        .Register(*registry);
    } else {
      histogram_family_ = &prometheus::detail::BuildHistogram()
        .Name(metric_name)
        .Register(*registry);
    }
    break;
  default:
    // TODO(micafan) CHECK
    return;
  }
}

void MetricFamily::UpdateValue(int64_t value, const Tags *tags) {
  switch (type_) {
  case MetricType::kCounter:
    {
      prometheus::Counter &counter = GetCounter(tags);
      counter.Increment(value);
    }
    break;
  case MetricType::kGauge:
    {
      prometheus::Gauge &gauge = GetGauge(tags);
      gauge.Set(value);
    }
    break;
  case MetricType::kHistogram:
    {
      prometheus::Histogram &histogram = GetHistogram(tags);
      histogram.Observe(value);
    }
    break;
  default:
    // TODO(micafan) CHECK
    return;
  }
}

prometheus::Counter &MetricFamily::GetCounter(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return counter_family_->Add(labels);
  }

  {
    ReadLock lock(mutex_);
    auto it = tag_to_counter_map_.find(tags->GetID());
    if (it != tag_to_counter_map_.end()) {
      return it->second();
    }
  }

  {
    WriteLock lock(mutex_);
    auto it = tag_to_counter_map_.find(tags->GetID());
    if (it != tag_to_counter_map_.end()) {
      return it->second();
    }
    prometheus::Counter &counter = counter_family_->Add(tags->GetTags());
    tag_to_counter_map_.emplace(tags->GetID(), counter);
    return counter;
  }
}

prometheus::Gauge &MetricFamily::GetGauge(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return gauge_family_->Add(labels);
  }

  {
    ReadLock lock(mutex_);
    auto it = tag_to_gauge_map_.find(tags->GetID());
    if (it != tag_to_gauge_map_.end()) {
      return it->second();
    }
  }

  {
    WriteLock lock(mutex_);
    auto it = tag_to_gauge_map_.find(tags->GetID());
    if (it != tag_to_gauge_map_.end()) {
      return it->second();
    }
    prometheus::Gauge &gauge = gauge_family_->Add(tags->GetTags());
    tag_to_gauge_map_.emplace(tags->GetID(), gauge);
    return gauge;
  }
}

prometheus::Histogram &MetricFamily::GetHistogram(const Tags *tags) {
  if (tags == nullptr) {
    std::map<std::string, std::string> labels;
    return histogram_family_->Add(labels);
  }

  {
    ReadLock lock(mutex_);
    auto it = tag_to_histogram_map_.find(tags->GetID());
    if (it != tag_to_histogram_map_.end()) {
      return it->second();
    }
  }

  {
    WriteLock lock(mutex_);
    auto it = tag_to_histogram_map_.find(tags->GetID());
    if (it != tag_to_histogram_map_.end()) {
      return it->second();
    }
    prometheus::Histogram &histogram = histogram_family_->Add(tags->GetTags());
    tag_to_histogram_map_.emplace(tags->GetID(), histogram);
    return histogram;
  }
}

PrometheusMetricsRegistry::PrometheusMetricsRegistry(RegistryOption options)
    : MetricsRegistryInterface(std::move(options)) {}

void PrometheusMetricsRegistry::DoRegisterCounter(const std::string &metric_name,
                                                  const Tags *tags) {

}

void PrometheusMetricsRegistry::DoRegisterGauge(const std::string &metric_name,
                                                const Tags *tags) {

}

void PrometheusMetricsRegistry::DoRegisterHistogram(
  const std::string &metric_name,
  const std::unordered_set<double> &percentiles,
  const Tags *tags) {

}

void PrometheusMetricsRegistry::DoUpdateValue(const std::string &metric_name,
                                              int64_t value,
                                              const Tags *tags) {

}

}  // namespace metrics

}  // namespace ray
