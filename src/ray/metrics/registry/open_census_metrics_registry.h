#ifndef RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H
#define RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H

#include <boost/thread/pthread/shared_mutex.hpp>
#include <opencensus/stats/stats.h>

#include "ray/metrics/metrics_registry_interface.h"

namespace ray {

namespace metrics {

class MetricDescr {
 public:
  MetricDescr(MetricType type)
  : type_(type) {}

  ~MetricDescr() = default;

  void Init(const RegistryOption &options,
            const std::string &name,
            const TagKeys *tag_keys);

  void UpdateValue(int64_t value, opencensus::tags::TagMap *tag_map);

 private:
  void AddColumnToDescr(const TagKeys *tag_keys,
                        opencensus::stats::ViewDescriptor *view_descr) const;

  MetricType type_;
  opencensus::stats::MeasureInt64 measure_;
  std::vector<opencensus::stats::ViewDescriptor> view_descrs_;
};

class OpenCensusMetricsRegistry : public MetricsRegistryInterface {
 public:
  explicit OpenCensusMetricsRegistry(RegistryOption options);

  virtual ~OpenCensusMetricsRegistry();

  virtual void ExportMetrics(const std::string &regex_filter,
                             std::vector<prometheus::MetricFamily> *metrics);

 protected:
  virtual void DoRegisterCounter(const std::string &metric_name,
                                 const Tags *tags);

  virtual void DoRegisterGauge(const std::string &metric_name,
                               const Tags *tags);

  virtual void DoRegisterHistogram(const std::string &metric_name,
                                   int64_t min_value,
                                   int64_t max_value,
                                   const std::unordered_set<double> &percentiles,
                                   const Tags *tags);

  virtual void DoUpdateValue(const std::string &metric_name,
                             int64_t value,
                             const Tags *tags);

 private:
  MetricDescr *DoRegister(const std::string &metric_name,
                          MetricDescr::MetricType type,;
                          const Tags *tags);

  const opencensus::tags::TagMap &GetTagMap(const Tags &tags);

  opencensus::tags::TagMap GenerateTagMap(const Tags &tags);

  boost::shared_mutex shared_mutex_;
  /// Metric that have registered.
  std::unordered_map<std::string, MetricDescr> metric_descr_map_;
  /// Mapping from Tags id to opencensus::tags::TagMap.
  /// A opencensus::tags::TagMap is expensive to construct,
  /// and should be shared between uses where possible.
  std::unordered_map<size_t, opencensus::tags::TagMap> id_to_tag_;

  typedef boost::unique_lock<boost::shared_mutex> ReadLock;
  typedef boost::shared_lock<boost::shared_mutex> WriteLock;
};

}  // namespace metrics

}  // namespace ray

#endif  // RAY_METRICS_OPEN_CENSUS_METRICS_REGISTRY_H
