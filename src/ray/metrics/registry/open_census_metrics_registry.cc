#include "open_census_metrics_registry.h"

#include <regex>
#include <opencensus/exporters/stats/prometheus/internal/prometheus_utils.h>

namespace ray {

namespace metrics {

void MetricDescr::Init(const RegistryOption &options,
                       const std::string &name,
                       const TagKeys *tag_keys) {
  measure_ = opencensus::stats::Measure::Register(name.c_str(), "", "");
  if (!measure_.IsValid()) {
    measure_= opencensus::stats::MeasureRegistry::GetMeasureInt64ByName(name.c_str());
    return;
  }
  // register view
  switch (type_) {
  case kCount:
    {
      opencensus::stats::ViewDescriptor count_descr = opencensus::stats::ViewDescriptor()
        .set_name(name + options.delimiter_ + "count")
        .set_measure(name)
        .set_aggregation(opencensus::stats::Aggregation::Count());
      AddColumnToDescr(tag_keys, &count_descr);
      count_descr.RegisterForExport();
      view_descrs_.emplace_back(std::move(count_descr));

      opencensus::stats::ViewDescriptor sum_descr = opencensus::stats::ViewDescriptor()
        .set_name(name + options.delimiter_ + "sum")
        .set_measure(name)
        .set_aggregation(opencensus::stats::Aggregation::Sum());
      AddColumnToDescr(tag_keys, &sum_descr);
      sum_descr.RegisterForExport();
      view_descrs_.emplace_back(std::move(sum_descr));
    }
    break;
  case kGauge:
    {
      opencensus::stats::ViewDescriptor gauge_descr = opencensus::stats::ViewDescriptor()
        .set_name(name + options.delimiter_ + "gauge")
        .set_measure(name)
        .set_aggregation(opencensus::stats::Aggregation::LastValue());
      AddColumnToDescr(tag_keys, &gauge_descr);
      gauge_descr.RegisterForExport();
      view_descrs_.emplace_back(std::move(gauge_descr));
    }
    break;
  case kHistogram:
    {
      opencensus::stats::ViewDescriptor hist_descr = opencensus::stats::ViewDescriptor()
        .set_name(name + options.delimiter_ + "hs")
        .set_measure(name)
        .set_aggregation(opencensus::stats::Aggregation::Distribution());
      AddColumnToDescr(tag_keys, &hist_descr);
      hist_descr.RegisterForExport();
      view_descrs_.emplace_back(std::move(hist_descr));
    }
    break;
  default:
    return;
  }
}

void MetricDescr::AddColumnToDescr(const TagKeys *tag_keys,
                                   opencensus::stats::ViewDescriptor* view_descr) {
  if (tag_keys == nullptr) {
    return;
  }

  for (const auto &key : tag_keys->GetTagKeys()) {
    auto tag_key = opencensus::tags::TagKey::Register(key);
    view_descr->add_column(tag_key);
  }
}


void MetricDescr::UpdateValue(int64_t value, opencensus::tags::TagMap *tag_map) {
  Measurement<int64_t> m(measure_, value);
  if (tag_map != nullptr) {
    opencensus::stats::Record({{measure_, value}}, *tag_map);
  } else {
    opencensus::stats::Record({{measure_, value}});
  }
}

OpenCensusMetricsRegistry::OpenCensusMetricsRegistry(RegistryOption options)
    : MetricsRegistry(std::move(options)) {
  opencensus::tags::TagMap oc_tag_map = GenerateTagMap(default_tags_);
  id_to_tag_.emplace(default_tags_.GetID(), oc_tag_map);
}

OpenCensusMetricsRegistry::~OpenCensusMetricsRegistry() {
  for (auto &elem : metric_descr_map_) {
    delete elem.second;
  }
}

void OpenCensusMetricsRegistry::ExportMetrics(const std::string &regex_filter,
                                              std::vector<prometheus::MetricFamily> *metrics) {
  bool need_filter = (regex_filter == ".*") ? false : true;
  std::regex filter(regex_filter.c_str());

  const auto view_datas = opencensus::stats::StatsExporter::GetViewData();
  for (const auto &elem : view_datas) {
    const std::string &metric_name = elem.first.measure_descriptor().name();
    bool match = (!need_filter) ? true
      : (std::regex_match(metric_name, filter) ? true : false);
    if (match) {
      prometheus::MetricFamily metric_family;
      opencensus::exporters::stats::SetMetricFamily(elem.first, elem.second, &metric_family);
      metrics->emplace_back(std::move(metric_family));
    }
  }
}

void OpenCensusMetricsRegistry::DoRegisterCounter(const std::string &metric_name,
                                                  const TagKeys *tag_keys) {
  DoRegisterMetric(metric_name, tag_keys, MetricDescr::MetricType::kCount);
}

void OpenCensusMetricsRegistry::DoRegisterGauge(const std::string &metric_name,
                                                const TagKeys *tag_keys) {
  DoRegisterMetric(metric_name, tag_keys, MetricDescr::MetricType::kGauge);
}

void OpenCensusMetricsRegistry::DoRegisterHistogram(
  const std::string &metric_name,
  const std::unordered_set<double> &percentiles,
  const TagKeys *tag_keys) {
  // TODO(micafan) percentiles
  DoRegisterMetric(metric_name, tag_keys, MetricDescr::MetricType::kHistogram);
}


MetricDescr *OpenCensusMetricsRegistry::DoRegister(const std::string &metric_name,
                                                   MetricDescr::MetricType type,
                                                   const TagKeys *tag_keys) {
  const TagKeys *cur_tag_keys = (tag_keys != nullptr) ? tag_keys
    : &default_tags_.GetTagKeys();
  // Check if it is already registered
  {
    ReadLock lock(shared_mutex_);
    auto it = metric_descr_map_.find(metric_name);
    if (it != metric_descr_map_.end()) {
      return &it->second;
    }
  }
  // Try to register
  {
    WriteLock lock(shared_mutex_);
    auto it = metric_descr_map_.find(metric_name);
    if (it != metric_descr_map_.end()) {
      return &it->second;
    }
    MetricDescr descr(type);
    descr.Init(options_, metric_name, cur_tag_keys);
    metric_descr_map_.emplace(metric_name, descr);
    return &descr;
  }
}

void OpenCensusMetricsRegistry::DoUpdateValue(const std::string &metric_name,
                                              int64_t value,
                                              const Tags *tags) {
  MetricDescr *descr = nullptr;
  {
    ReadLock lock(shared_mutex_);
    auto it = metric_descr_map_.find(metric_name);
    if (it != metric_descr_map_.end()) {
      descr = &it->second;
    }
  }

  // Register as a counter
  if (descr == nullptr) {
    const TagKeys *tag_keys = (tags == nullptr) ? nullptr
      : &tags->GetTagKeys();
    descr = DoRegister(metric_name, MetricDescr::MetricType::kCount, tag_keys);
  }

  const opencensus::tags::TagMap &oc_tag_map = (tags != nullptr) ? GetTagMap(*tags)
    : GetTagMap(default_tags_);
  descr->UpdateValue(value, oc_tag_map);
}

const opencensus::tags::TagMap &OpenCensusMetricsRegistry::GetTagMap(const Tags &tags) {
  size_t id = tags.GetID();
  {
    ReadLock lock(shared_mutex_);
    auto it = id_to_tag_.find(id);
    if (it != id_to_tag_.end()) {
      return it->second;
    }
  }

  opencensus::tags::TagMap oc_tag_map = GenerateTagMap(default_tags_);
  {
    WriteLock lock(shared_mutex_);
    auto it = id_to_tag_.find(id);
    if (it != id_to_tag_.end()) {
      return it->second;
    }
    id_to_tag_.emplace(default_tags_.GetID(), oc_tag_map);
    it = id_to_tag_.find(id);
    return it->second;
  }
}

opencensus::tags::TagMap OpenCensusMetricsRegistry::GenerateTagMap(const Tags &tags) {
  std::vector<std::pair<TagKey, std::string>> tag_vec;
  const std::map<std::string, std::string> &tag_map = tags.GetTags();
  for (const auto &item : tag_map) {
    opencensus::tags::TagKey key = opencensus::tags::TagKey::Register(item.first.c_str());
    tag_vec.emplace_back(std::make_pair(key, item.second.c_str()));
  }
  opencensus::tags::TagMap oc_tag_map(std::move(tag_vec));
  return oc_tag_map;
}

}  // namespace metrics

}  // ray
