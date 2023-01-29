
#include "ray/api/metric.h"

#include "ray/stats/metric.h"
#include "ray/util/logging.h"

namespace ray {

Metric::~Metric() {
  if (metric_ != nullptr) {
    stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
    delete metric;
    metric_ = nullptr;
  }
}

std::string Metric::GetName() const {
  RAY_CHECK(metric_ != nullptr) << "The metric_ must not be nullptr.";
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
  return metric->GetName();
}

void Metric::Record(double value,
                    const std::unordered_map<std::string, std::string> &tags) {
  RAY_CHECK(metric_ != nullptr) << "The metric_ must not be nullptr.";
  stats::Metric *metric = reinterpret_cast<stats::Metric *>(metric_);
  metric->Record(value, tags);
}

Gauge::Gauge(const std::string &name, const std::string &description,
             const std::string &unit, const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Gauge(name, description, unit, tag_str_keys);
}

void Gauge::Set(double value, const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Histogram::Histogram(const std::string &name, const std::string &description,
                     const std::string &unit, const std::vector<double> boundaries,
                     const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Histogram(name, description, unit, boundaries, tag_str_keys);
}

void Histogram::Observe(double value,
                        const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Counter::Counter(const std::string &name, const std::string &description,
                 const std::string &unit, const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Count(name, description, unit, tag_str_keys);
}

void Counter::Inc(double value,
                  const std::unordered_map<std::string, std::string> &tags) {
  Record(value, tags);
}

Sum::Sum(const std::string &name, const std::string &description, const std::string &unit,
         const std::vector<std::string> &tag_str_keys) {
  metric_ = new stats::Sum(name, description, unit, tag_str_keys);
}

}  // namespace ray
