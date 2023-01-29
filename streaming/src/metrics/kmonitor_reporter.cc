#include "kmonitor_reporter.h"

#include <mutex>

#include "kmonitor/client/KMonitorFactory.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/StatisticsType.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "logging.h"

namespace ray {
namespace streaming {
std::atomic<int> StreamingKmonitorClient::kmonitor_cnt(0);

bool StreamingKmonitorClient::Start(const StreamingMetricsConfig &conf) {
  std::string default_tags_json("");
  for (auto &it : conf.GetMetricsGlobalTags()) {
    if (default_tags_json != "") {
      default_tags_json += ",";
    }
    default_tags_json += "\"" + it.first + "\" : \"" + it.second + "\"";
  }
  const std::string metric_url = conf.GetMetricsUrl();
  std::string json_string =
      "{\"tenant_name\":\"default"
      "\", \"service_name\":\"" +
      conf.GetMetricsServiceName() + "\", \"sink_address\":\"" + metric_url +
      "\", \"system_metrics\":false,"
      "\"sink_period\": 2,"
      "\"sink_queue_capacity\": 1000,"
      "\"global_tags\":{" +
      default_tags_json + "}}";
  return Start(json_string);
}

bool StreamingKmonitorClient::Start(const std::string &json_string) {
  STREAMING_LOG(INFO) << "kmonitor init json string => " << json_string;
  if (0 == kmonitor_cnt) {
    kmonitor::KMonitorFactory::Init(json_string, true);
    // kmonitor::KMonitorFactory::Start();
  }
  kmonitor_cnt++;
  STREAMING_LOG(INFO) << "start, kmonitor ref cnt => " << kmonitor_cnt;
  kmonitor_client = kmonitor::KMonitorFactory::GetKMonitor("streaming", "streaming");
  kmonitor::KMonitorFactory::Start("streaming");
  STREAMING_LOG(INFO) << "kmonitor client init succ";
  return true;
}

StreamingKmonitorClient::~StreamingKmonitorClient() {
  STREAMING_LOG(WARNING) << "kmonitor client shutdown";
  Shutdown();
};

void StreamingKmonitorClient::Shutdown() {
  // shutdown maybe called by deconstructor or user's function
  if (nullptr != kmonitor_client) {
    kmonitor_client = nullptr;
    kmonitor_cnt--;
    // TODO(lingxuan.zlx): shutdown if no one process client report metrics
    if (kmonitor_cnt == 0) {
      STREAMING_LOG(WARNING) << "kmonitor_cnt is 0, but skip shutdown action";
      // kmonitor::KMonitorFactory::Shutdown("streaming");
    }
  }
  STREAMING_LOG(WARNING) << "shutdown, kmonitor ref cnt => " << kmonitor_cnt;
}

void StreamingKmonitorClient::UpdateCounter(const std::string &domain,
                                            const std::string &group_name,
                                            const std::string &short_name, double value) {
  const std::string merged_metric_name =
      METRIC_GROUP_JOIN(domain, group_name, short_name);
  kmonitor_client->Register(merged_metric_name, kmonitor::COUNTER);
  kmonitor_client->Report(merged_metric_name, value);
}

void StreamingKmonitorClient::UpdateCounter(
    const std::string &metric_name, const std::map<std::string, std::string> &tags,
    double value) {
  kmonitor_client->Register(metric_name, kmonitor::COUNTER);
  kmonitor::MetricsTags m_tags;
  auto it = tags.begin();
  while (it != tags.end()) {
    m_tags.AddTag(it->first, it->second);
    it++;
  }
  STREAMING_LOG(DEBUG) << "Report counter metric " << metric_name << " , value " << value;
  kmonitor_client->Report(metric_name, &m_tags, value);
}

void StreamingKmonitorClient::UpdateGauge(const std::string &domain,
                                          const std::string &group_name,
                                          const std::string &short_name, double value,
                                          bool is_reset) {
  const std::string merged_metric_name =
      METRIC_GROUP_JOIN(domain, group_name, short_name);
  STREAMING_LOG(DEBUG) << "Report gauge metric " << merged_metric_name << " , value "
                       << value;
  kmonitor_client->Register(merged_metric_name, kmonitor::GAUGE);
  kmonitor_client->Report(merged_metric_name, value);
}

void StreamingKmonitorClient::UpdateGauge(const std::string &metric_name,
                                          const std::map<std::string, std::string> &tags,
                                          double value, bool is_reset) {
  kmonitor_client->Register(metric_name, kmonitor::GAUGE);
  kmonitor::MetricsTags m_tags;
  auto it = tags.begin();
  while (it != tags.end()) {
    m_tags.AddTag(it->first, it->second);
    it++;
  }
  STREAMING_LOG(DEBUG) << "Report gauge metric " << metric_name << " , value " << value;
  kmonitor_client->Report(metric_name, &m_tags, value);
}

void StreamingKmonitorClient::UpdateHistogram(const std::string &domain,
                                              const std::string &group_name,
                                              const std::string &short_name, double value,
                                              double min_value, double max_value) {}

void StreamingKmonitorClient::UpdateHistogram(
    const std::string &metric_name, const std::map<std::string, std::string> &tags,
    double value, double min_value, double max_value) {}

void StreamingKmonitorClient::UpdateQPS(const std::string &metric_name,
                                        const std::map<std::string, std::string> &tags,
                                        double value) {
  kmonitor_client->Register(metric_name, kmonitor::QPS);
  kmonitor::MetricsTags m_tags;
  for (auto &it : tags) {
    m_tags.AddTag(it.first, it.second);
  }
  kmonitor_client->Report(metric_name, &m_tags, value);
}
}  // namespace streaming
}  // namespace ray
