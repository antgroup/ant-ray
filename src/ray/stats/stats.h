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

#pragma once

#include <exception>
#include <string>
#include <unordered_map>

#include "absl/synchronization/mutex.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/stats.h"
#include "opencensus/tags/tag_key.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/io_service_pool.h"
#include "ray/common/ray_config.h"
#include "ray/stats/metric.h"
#include "ray/stats/metric_exporter.h"
#include "ray/stats/metric_exporter_client.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {

namespace stats {

#include <boost/asio.hpp>

/// Include metric_defs.h to define measure items.
#include "ray/stats/metric_defs.h"

// TODO(sang) Put all states and logic into a singleton class Stats.
static std::shared_ptr<IOServicePool> metrics_io_service_pool;
static std::shared_ptr<MetricExporterClient> exporter;
static absl::Mutex stats_mutex;

static std::shared_ptr<MetricExporterClient> InitCeresdbExporter(
    std::shared_ptr<MetricExporterClient> exporter) {
  if (StatsConfig::instance().GetCeresdbExporterEnabled()) {
    auto ceresdb_conf_map =
        LoadPropertiesFromFile(StatsConfig::instance().GetCeresdbConfFile());
    std::shared_ptr<OpentsdbClient> ceresdb_client(
        new OpentsdbClient(ceresdb_conf_map["ray.ceresdb.server.host"],
                           std::stoi(ceresdb_conf_map["ray.ceresdb.server.port"]),
                           ceresdb_conf_map["ray.ceresdb.server.user"],
                           ceresdb_conf_map["ray.ceresdb.server.key"]));
    std::shared_ptr<MetricExporterClient> ceresdb_exporter_client(
        new OpentsdbExporterClient(exporter, ceresdb_client,
                                   RayConfig::instance().opentsdb_report_queue_size()));
    // TODO(lingxuan.zlx): Report batch size is configurable, we will merge lastest
    // master from github laster.
    RAY_LOG(INFO) << "Stats ceresdb exporter registered.";
    return ceresdb_exporter_client;
  }
  return exporter;
}

static std::shared_ptr<MetricExporterClient> InitKMonitorExporter(
    std::shared_ptr<MetricExporterClient> exporter) {
  if (StatsConfig::instance().GetKmonitorExporterEnabled()) {
    auto kmonitor_conf_map =
        LoadPropertiesFromFile(StatsConfig::instance().GetKmonitorConfFile());
    KMonitorConfig kMonitor_config = {
        kmonitor_conf_map["tenant_name"], kmonitor_conf_map["service_name"],
        kmonitor_conf_map["global_tags"], kmonitor_conf_map["sink_address"],
        kmonitor_conf_map["sink_period"], kmonitor_conf_map["sink_queue_capacity"]};
    std::shared_ptr<MetricExporterClient> kMonitor_exporter_client(
        new KMonitorExporterClient(kMonitor_config, exporter));
    RAY_LOG(INFO) << "Stats KMonitor exporter registered.";
    return kMonitor_exporter_client;
  }
  return exporter;
}

/// Initialize stats for a process.
/// NOTE:
/// - stats::Init should be called only once per PROCESS. Redundant calls will be just
/// ignored.
/// - If you want to reinitialize, you should call stats::Shutdown().
/// - It is thread-safe.
/// We recommend you to use this only once inside a main script and add Shutdown() method
/// to any signal handler.
/// \param global_tags[in] Tags that will be appended to all metrics in this process.
/// \param metrics_agent_port[in] The port to export metrics at each node.
/// \param exporter_to_use[in] The exporter client you will use for this process' metrics.
static inline void Init(const TagsType &global_tags, const int metrics_agent_port,
                        std::shared_ptr<MetricExporterClient> exporter_to_use = nullptr,
                        int64_t metrics_report_batch_size =
                            RayConfig::instance().metrics_report_batch_size()) {
  absl::MutexLock lock(&stats_mutex);
  if (StatsConfig::instance().IsInitialized()) {
    RAY_CHECK(metrics_io_service_pool != nullptr);
    RAY_CHECK(exporter != nullptr);
    return;
  }
  RAY_CHECK(metrics_io_service_pool == nullptr);
  RAY_CHECK(exporter == nullptr);
  bool disable_stats = !RayConfig::instance().enable_metrics_collection();
  StatsConfig::instance().SetIsDisableStats(disable_stats);
  if (disable_stats) {
    RAY_LOG(INFO) << "Disabled stats.";
    return;
  }
  RAY_LOG(DEBUG) << "Initialized stats";

  metrics_io_service_pool = std::make_shared<IOServicePool>(1);
  metrics_io_service_pool->Run();
  instrumented_io_context *metrics_io_service = metrics_io_service_pool->Get();
  RAY_CHECK(metrics_io_service != nullptr);

  // Default exporter is a metrics agent exporter.
  if (exporter_to_use == nullptr) {
    std::shared_ptr<MetricExporterClient> stdout_exporter(new StdoutExporterClient());
    exporter.reset(new MetricsAgentExporter(stdout_exporter));
  } else {
    exporter = exporter_to_use;
  }

  auto ceresdb_exporter = InitCeresdbExporter(exporter);
  auto pack_exporter = InitKMonitorExporter(ceresdb_exporter);

  // Set interval.
  StatsConfig::instance().SetReportInterval(absl::Milliseconds(std::max(
      RayConfig::instance().metrics_report_interval_ms(), static_cast<uint64_t>(1000))));
  StatsConfig::instance().SetHarvestInterval(
      absl::Milliseconds(std::max(RayConfig::instance().metrics_report_interval_ms() / 2,
                                  static_cast<uint64_t>(500))));

  MetricPointExporter::Register(pack_exporter, metrics_report_batch_size);
  // TODO(buhe): disable exporter to agent process and will fix this later.
  if (metrics_agent_port > 0) {
    OpenCensusProtoExporter::Register(metrics_agent_port, (*metrics_io_service),
                                      "127.0.0.1");
  }
  opencensus::stats::StatsExporter::SetInterval(
      StatsConfig::instance().GetReportInterval());
  opencensus::stats::DeltaProducer::Get()->SetHarvestInterval(
      StatsConfig::instance().GetHarvestInterval());
  StatsConfig::instance().SetGlobalTags(global_tags);
  StatsConfig::instance().SetIsInitialized(true);
}

/// Shutdown the initialized stats library.
/// This cleans up various threads and metadata for stats library.
static inline void Shutdown() {
  absl::MutexLock lock(&stats_mutex);
  if (!StatsConfig::instance().IsInitialized()) {
    // Return if stats had never been initialized.
    return;
  }
  metrics_io_service_pool->Stop();
  opencensus::stats::DeltaProducer::Get()->Shutdown();
  opencensus::stats::StatsExporter::Shutdown();
  metrics_io_service_pool = nullptr;
  exporter = nullptr;
  StatsConfig::instance().SetIsInitialized(false);
  RAY_LOG(INFO) << "Finished shutdown stats.";
}

/// Generate default global tags.
static inline TagsType DefaultGlobalTags(const std::string &component) {
  return {{ClusterNameKey, RayConfig::instance().cluster_name()},
          {ComponentKey, component},
          {NodeAddressKey, boost::asio::ip::host_name()},
          {WorkerPidKey, std::to_string(getpid())}};
}

/// Initialize stats for a process, like raylet, core worker and gcs server.
/// This method includes not only extra configurations initialization but
/// also stats initialization. And naming this method as `Start` just to avoid
/// name conflicts with method `Init`.
static inline void Start(const TagsType &global_tags = {},
                         const int metrics_agent_port = 0,
                         const std::string &config_list = "",
                         const bool callback_shutdown = false,
                         const bool init_log = false, const std::string &app_name = "") {
  /// Initialize log just for Python while it's not necessary to do so in C++
  /// because C++ module has already initinalized log.
  if (init_log) {
    const std::string log_dir =
        getenv("RAY_LOG_DIR") != nullptr ? getenv("RAY_LOG_DIR") : "";
    ray::RayLog::StartRayLog(app_name.empty() ? "stats" : app_name,
                             ray::RayLogLevel::INFO, log_dir);
  }

  if (!config_list.empty()) {
    RayConfig::instance().initialize(config_list);
  }

  if (RayConfig::instance().enable_ceresdb_exporter() &&
      !RayConfig::instance().ceresdb_conf_file().empty()) {
    StatsConfig::instance().SetCeresdbExporterEnabled(true);
    StatsConfig::instance().SetCeresdbConfFile(RayConfig::instance().ceresdb_conf_file());
    RAY_LOG(INFO) << "Ceresdb exporter enabled and ceresdb conf file is "
                  << RayConfig::instance().ceresdb_conf_file();
  }

  if (RayConfig::instance().enable_kmonitor_exporter() &&
      !RayConfig::instance().kmonitor_conf_file().empty()) {
    StatsConfig::instance().SetKmonitorExporterEnabled(true);
    StatsConfig::instance().SetKmonitorConfFile(
        RayConfig::instance().kmonitor_conf_file());
    RAY_LOG(INFO) << "KMonitor exporter enabled and kmonitor conf file is "
                  << RayConfig::instance().kmonitor_conf_file();
  }

  for (auto pair : global_tags) {
    RAY_LOG(DEBUG) << pair.first.name() << " is " << pair.second;
  }

  Init(global_tags, metrics_agent_port);

  {
    /// Should call `Shutdown` and `ShutDownRayLog` methods if used in python
    /// module because they are not called automatically.
    if (callback_shutdown) atexit(Shutdown);
    if (init_log) atexit(ray::RayLog::ShutDownRayLog);
  }

  RAY_LOG(INFO) << "Finished start stats.";
}

}  // namespace stats

}  // namespace ray
