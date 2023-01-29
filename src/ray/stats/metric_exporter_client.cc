// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/stats/metric_exporter_client.h"

#include <algorithm>

namespace ray {
namespace stats {

///
/// Stdout Exporter
///
void StdoutExporterClient::ReportMetrics(const std::vector<MetricPoint> &points) {
  RAY_LOG(DEBUG) << "Metric point size : " << points.size();
}

///
/// Metrics Exporter Decorator
///
MetricExporterDecorator::MetricExporterDecorator(
    std::shared_ptr<MetricExporterClient> exporter)
    : exporter_(exporter) {}

void MetricExporterDecorator::ReportMetrics(const std::vector<MetricPoint> &points) {
  if (exporter_) {
    exporter_->ReportMetrics(points);
  }
}

void OpentsdbExporterClient::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
  if (points.size() == 0) {
    return;
  }
  std::vector<OpentsdbPoint> opentsdb_points;
  std::transform(points.begin(), points.end(), std::back_inserter(opentsdb_points),
                 [](const MetricPoint &point) {
                   return OpentsdbPoint{.metric_name = point.metric_name,
                                        .timestamp = point.timestamp,
                                        .value = point.value,
                                        .tags = point.tags};
                 });

  if (num_pending_requests_ >= max_pending_num_) {
    RAY_LOG(WARNING) << "Skip reporting metrics to OpenTSDB. Because there are still "
                     << num_pending_requests_ << " pending report requests.";
    return;
  }

  num_pending_requests_++;
  io_service_.post(
      [this, self = shared_from_this(), points = std::move(opentsdb_points)] {
        opentsdb_client_->Post(points);
        num_pending_requests_--;
      });
}

///
/// Metrics Agent Exporter
///
MetricsAgentExporter::MetricsAgentExporter(std::shared_ptr<MetricExporterClient> exporter)
    : MetricExporterDecorator(exporter) {}

void MetricsAgentExporter::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
}

///
/// KMonitor Metrics Exporter
///
KMonitorExporterClient::KMonitorExporterClient(
    KMonitorConfig &config, std::shared_ptr<MetricExporterClient> exporter)
    : MetricExporterDecorator(exporter) {
  std::string json_string =
      "{\"tenant_name\":\"" + config.tenant_name + "\",\"service_name\":\"" +
      config.service_name + "\",\"golbal_tags\":" + config.global_tags +
      ",\"sink_address\":\"" + config.sink_address + "\",\"sink_period\":\"" +
      config.sink_period + "\",\"sink_queue_capacity\":\"" + config.sink_queue_capacity +
      "\"}";
  kmonitor::KMonitorFactory::Init(json_string);
  kMonitor_ = kmonitor::KMonitorFactory::GetKMonitor("ray");
  kmonitor::KMonitorFactory::Start();
  RAY_LOG(INFO) << "Initialized KMonitor exporter.";
}

void KMonitorExporterClient::ReportMetrics(const std::vector<MetricPoint> &points) {
  MetricExporterDecorator::ReportMetrics(points);
  if (kMonitor_ != nullptr) {
    for (auto &point : points) {
      kmonitor::MetricsTags m_tags;
      auto it = point.tags.begin();
      while (it != point.tags.end()) {
        // Note: Reporting to kmonitor will fail if tag value is empty.
        if (!it->second.empty()) {
          m_tags.AddTag(std::move(it->first), std::move(it->second));
        }
        it++;
      }
      kMonitor_->Register(point.metric_name, kmonitor::RAW, kmonitor::FATAL);
      kMonitor_->Report(point.metric_name, &m_tags, point.value);
      RAY_LOG(DEBUG) << "Reported a metric, name: " << point.metric_name
                     << ", value: " << point.value << ", timestamp: " << point.timestamp;
    }
    RAY_LOG(INFO) << "Finished report " << points.size() << " metrics to KMonitor.";
  }
}

KMonitorExporterClient::~KMonitorExporterClient() {
  // Note: Do not shutdown kmonitor here, because it will cause error when there
  // is another use.
  // kmonitor::KMonitorFactory::Shutdown();
}

}  // namespace stats
}  // namespace ray
