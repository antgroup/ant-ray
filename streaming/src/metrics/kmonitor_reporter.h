#ifndef RAY_STREAMING_KMONITOR_H
#define RAY_STREAMING_KMONITOR_H
#include <atomic>

#include "kmonitor/client/KMonitor.h"
#include "streaming_perf_metric.h"

namespace ray {
namespace streaming {

class StreamingKmonitorClient : public StreamingPerfBase {
 public:
  virtual ~StreamingKmonitorClient();

  bool Start(const StreamingMetricsConfig &conf) override;

  bool Start(const std::string &json_string);

  void Shutdown() override;

  void UpdateCounter(const std::string &domain, const std::string &group_name,
                     const std::string &short_name, double value) override;

  void UpdateGauge(const std::string &domain, const std::string &group_name,
                   const std::string &short_name, double value,
                   bool is_reset = true) override;

  void UpdateHistogram(const std::string &domain, const std::string &group_name,
                       const std::string &short_name, double value, double min_value,
                       double max_value) override;

  void UpdateCounter(const std::string &metric_name,
                     const std::map<std::string, std::string> &tags,
                     double value) override;

  void UpdateGauge(const std::string &metric_name,
                   const std::map<std::string, std::string> &tags, double value,
                   bool is_rest = true) override;

  void UpdateHistogram(const std::string &metric_name,
                       const std::map<std::string, std::string> &tags, double value,
                       double min_value, double max_value) override;

  void UpdateQPS(const std::string &metric_name,
                 const std::map<std::string, std::string> &tags, double value) override;

 private:
  kmonitor::KMonitor *kmonitor_client = nullptr;
  static std::atomic<int> kmonitor_cnt;
};

}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_
