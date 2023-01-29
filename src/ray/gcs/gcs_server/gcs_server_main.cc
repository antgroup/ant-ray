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

#include <iostream>

#include "gflags/gflags.h"
#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/gcs/gcs_server/gcs_server.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/rpc/brpc/client/client.h"
#include "ray/stats/stats.h"
#include "ray/util/process.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs_service.pb.h"

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_int32(gcs_server_port, 0, "The port of gcs server.");
DEFINE_int32(metrics_agent_port, -1, "The port of metrics agent.");
DEFINE_string(config_list, "", "The config list of raylet.");
DEFINE_string(redis_password, "", "The password of redis.");
DEFINE_bool(retry_redis, false, "Whether we retry to connect to the redis.");
DEFINE_string(node_ip_address, "", "The ip address of the node.");
DEFINE_string(event_dir, "", "Event log directory for the active ray process.");

int main(int argc, char *argv[]) {
  // ANT-INTERNAL
  signal(SIGPIPE, SIG_IGN);
  ray::rpc::ConfigureBrpcLogging();

  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         []() { ray::RayLog::ShutDownRayLog(); }, argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/"");
  ray::RayLog::InstallFailureSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const int gcs_server_port = static_cast<int>(FLAGS_gcs_server_port);
  const int metrics_agent_port = static_cast<int>(FLAGS_metrics_agent_port);
  std::string config_list;
  RAY_CHECK(absl::Base64Unescape(FLAGS_config_list, &config_list))
      << "config_list is not a valid base64-encoded string.";
  const std::string redis_password = FLAGS_redis_password;
  const bool retry_redis = FLAGS_retry_redis;
  const std::string node_ip_address = FLAGS_node_ip_address;
  const std::string event_dir = FLAGS_event_dir;
  gflags::ShutDownCommandLineFlags();

  RayConfig::instance().initialize(config_list);

  // ANT-INTERNAL: DO NOT USE `ray::stats::Init`.
  // Because should do some extra stats initialization like opentsdb configuration
  // in ant internal ray, so here use `Start` instead of `Init`.
  ray::stats::Start(ray::stats::DefaultGlobalTags("gcs_server"), metrics_agent_port);

  ray::RayEventContext::Instance().SetEventContext(
      ray::rpc::Event_SourceType::Event_SourceType_GCS);
  // ANT-INTERNAL
  ray::RayEventContext::Instance().SetLabelBlacklist(
      RayConfig::instance().event_label_blacklist());
  if (RayConfig::instance().event_log_reporter_enabled() && !event_dir.empty()) {
    ray::EventManager::Instance().AddReporter(std::make_shared<ray::LogEventReporter>(
        ray::rpc::Event_SourceType::Event_SourceType_GCS, event_dir));
  }
  RAY_EVENT(INFO, EVENT_LABEL_PIPELINE) << "GCS process started";

  // IO Service for main loop.
  instrumented_io_context main_service;
  // Ensure that the IO service keeps running. Without this, the main_service will exit
  // as soon as there is no more work to be processed.
  boost::asio::io_service::work work(main_service);

  ray::gcs::GcsServerConfig gcs_server_config;
  gcs_server_config.grpc_server_name = "GcsServer";
  gcs_server_config.grpc_server_port = gcs_server_port;
  gcs_server_config.grpc_server_thread_num =
      RayConfig::instance().gcs_server_rpc_server_thread_num();
  gcs_server_config.redis_address = redis_address;
  gcs_server_config.redis_port = redis_port;
  gcs_server_config.redis_password = redis_password;
  gcs_server_config.retry_redis = retry_redis;
  gcs_server_config.node_ip_address = node_ip_address;
  gcs_server_config.pull_based_resource_reporting =
      RayConfig::instance().pull_based_resource_reporting();
  gcs_server_config.grpc_based_resource_broadcast =
      RayConfig::instance().grpc_based_resource_broadcast();
  gcs_server_config.raylet_config_list = config_list;
  ray::gcs::GcsServer gcs_server(gcs_server_config, main_service);

  // Destroy the GCS server on a SIGTERM. The pointer to main_service is
  // guaranteed to be valid since this function will run the event loop
  // instead of returning immediately.
  auto handler = [&main_service, &gcs_server](const boost::system::error_code &error,
                                              int signal_number) {
    RAY_LOG(INFO) << "GCS server received SIGTERM, shutting down...";
    RAY_EVENT(INFO, EVENT_LABEL_PIPELINE)
        << "GCS received SIGTERM, shutting down, error: " << error
        << "signal number: " << signal_number;
    ray::EventManager::Instance().ClearReporters();
    gcs_server.Stop();
    ray::stats::Shutdown();
    main_service.stop();
  };
  boost::asio::signal_set signals(main_service);
#ifdef _WIN32
  signals.add(SIGBREAK);
#else
  signals.add(SIGTERM);
#endif
  signals.async_wait(handler);

  gcs_server.Start();

  main_service.run();
}
