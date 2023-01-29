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

#include <string>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/id.h"
#include "ray/gcs/entry_change_notification.h"
#include "ray/gcs/gcs_client.h"
#include "ray/raylet/job_manager.h"
#include "ray/rpc/agent_manager/agent_manager_client.h"
#include "ray/rpc/agent_manager/agent_manager_server.h"
#include "ray/rpc/event/event_client.h"
#include "ray/rpc/job/job_client.h"
#include "ray/rpc/job/job_server.h"
#include "ray/rpc/reporter/reporter_client.h"
#include "ray/util/process.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace raylet {

typedef std::function<std::shared_ptr<rpc::JobClientInterface>(
    const std::string &ip_address, int port)>
    JobClientFactoryFn;

typedef std::function<std::shared_ptr<rpc::EventClientInterface>(
    const std::string &ip_address, int port)>
    EventClientFactoryFn;

typedef std::function<std::shared_ptr<rpc::ReporterClientInterface>(
    const std::string &ip_address, int port)>
    ReporterClientFactoryFn;

typedef std::function<std::shared_ptr<boost::asio::deadline_timer>(std::function<void()>,
                                                                   uint32_t delay_ms)>
    DelayExecutorFn;

struct DriverMonitorInfo {
  JobID job_id;
  bool has_registered;
  pid_t driver_pid;
  std::string driver_cmdline;
};

struct InitializingJobInfo {
  int64_t last_warn_uninitialized_time;
  std::string last_error_message;
  std::shared_ptr<rpc::JobTableData> job;
};

struct RuntimeResourceScheduleOptions {
  bool runtime_resource_scheduling_enabled_ =
      RayConfig::instance().runtime_resource_scheduling_enabled();
  int32_t runtime_resources_calculation_interval_s_ =
      RayConfig::instance().runtime_resources_calculation_interval_s();
  float runtime_memory_tail_percentile_ =
      RayConfig::instance().runtime_memory_tail_percentile();
  float runtime_cpu_tail_percentile_ =
      RayConfig::instance().runtime_cpu_tail_percentile();
  bool UpdateFromMapProto(
      const ::google::protobuf::Map<std::string, std::string> &schedule_option_proto) {
    bool updated = false;
    auto iter = schedule_option_proto.find("runtime_resource_scheduling_enabled");
    if (iter != schedule_option_proto.end() &&
        runtime_resource_scheduling_enabled_ != (iter->second == "true")) {
      updated = true;
      runtime_resource_scheduling_enabled_ = (iter->second == "true");
    }
    iter = schedule_option_proto.find("runtime_resources_calculation_interval_s");
    if (iter != schedule_option_proto.end() &&
        runtime_resources_calculation_interval_s_ != std::stoi(iter->second)) {
      updated = true;
      runtime_resources_calculation_interval_s_ = std::stoi(iter->second);
    }
    iter = schedule_option_proto.find("runtime_memory_tail_percentile");
    if (iter != schedule_option_proto.end() &&
        runtime_memory_tail_percentile_ != std::stof(iter->second)) {
      updated = true;
      runtime_memory_tail_percentile_ = std::stof(iter->second);
    }
    iter = schedule_option_proto.find("runtime_cpu_tail_percentile");
    if (iter != schedule_option_proto.end() &&
        runtime_cpu_tail_percentile_ != std::stof(iter->second)) {
      updated = true;
      runtime_cpu_tail_percentile_ = std::stof(iter->second);
    }
    return updated;
  }
};

class AgentManager : public rpc::AgentManagerServiceHandler {
 public:
  struct Options {
    const NodeID node_id;
    std::vector<std::string> agent_commands;
  };

  explicit AgentManager(
      Options options, std::shared_ptr<gcs::GcsClient> gcs_client,
      JobClientFactoryFn job_client_factory, EventClientFactoryFn event_client_factory,
      ReporterClientFactoryFn reporter_client_factory, DelayExecutorFn delay_executor,
      std::function<void(const JobID &, const Status &)> on_job_env_initialized,
      std::shared_ptr<JobManager> job_manager,
      std::unique_ptr<PeriodicalRunner> periodical_runner,
      std::function<void(rpc::GetWorkersInfoReply *reply)> fill_workers_info =
          [](rpc::GetWorkersInfoReply *) { return; },
      std::function<void(const rpc::ReportLocalRuntimeResourcesRequest &)>
          runtime_resources_updated_callback =
              [](const rpc::ReportLocalRuntimeResourcesRequest &) { return; })
      : options_(std::move(options)),
        gcs_client_(std::move(gcs_client)),
        job_client_factory_(std::move(job_client_factory)),
        event_client_factory_(std::move(event_client_factory)),
        reporter_client_factory_(std::move(reporter_client_factory)),
        delay_executor_(std::move(delay_executor)),
        on_job_env_initialized_(std::move(on_job_env_initialized)),
        job_manager_(job_manager),
        cur_heatbeats_timeout_(0),
        num_heartbeats_timeout_(RayConfig::instance().agent_num_heartbeats_timeout()),
        periodical_runner_(std::move(periodical_runner)),
        is_detecting_dead_agent_(false),
        fill_workers_info_(std::move(fill_workers_info)),
        runtime_resources_updated_callback_(
            std::move(runtime_resources_updated_callback)) {}
  virtual ~AgentManager() = default;

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleAgentHeartbeat(const rpc::AgentHeartbeatRequest &request,
                            rpc::AgentHeartbeatReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetWorkersInfo(const rpc::GetWorkersInfoRequest &request,
                            rpc::GetWorkersInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportLocalRuntimeResources(
      const rpc::ReportLocalRuntimeResourcesRequest &request,
      rpc::ReportLocalRuntimeResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void OnDriverRegistered(JobID job_id, pid_t driver_pid);

  void OnJobEnvInitNotification(const rpc::JobEnvInitNotification &notification);

  void WarnJobEnvUninitialized(const JobID &job_id);

  void ClearJobEnv(const JobTableData &job_data);

  void CheckJavaHsErrLog(std::string log_dir, std::string ip, int pid, bool is_driver,
                         const JobID job_id, const WorkerID worker_id,
                         const ActorID actor_id, const TaskID task_id);

  bool IsJobEnvInitialized(const JobID &job_id);

  void OnRuntimeResourceScheduleOptionsUpdated(
      const RuntimeResourceScheduleOptions &schedule_options);

  virtual void StartAgent();

 private:
  void InitializeJobEnv(std::shared_ptr<rpc::JobTableData> job_data, bool start_driver);
  void MonitorStartingDriver(std::shared_ptr<DriverMonitorInfo> driver_monitor_info);
  /// Check that if agent is inactive due to no heartbeat for a period of time.
  /// If found, restart it.
  void DetectDeadAgent();
  /// Start detect dead agent. Return true if start success else false.
  bool StartDetectDeadAgent();

 private:
  Options options_;
  std::shared_ptr<gcs::GcsClient> gcs_client_;

  pid_t agent_pid_ = 0;
  int agent_port_ = 0;
  std::string agent_ip_address_;
  std::shared_ptr<rpc::JobClientInterface> job_client_;
  JobClientFactoryFn job_client_factory_;

  std::shared_ptr<rpc::EventClientInterface> event_client_;
  EventClientFactoryFn event_client_factory_;

  std::shared_ptr<rpc::ReporterClientInterface> reporter_client_;
  ReporterClientFactoryFn reporter_client_factory_;

  DelayExecutorFn delay_executor_;
  std::function<void(const JobID &, const Status &status)> on_job_env_initialized_;

  absl::flat_hash_map<JobID, InitializingJobInfo> initializing_jobs_;
  std::unordered_set<JobID> initialized_jobs_;
  absl::flat_hash_map<JobID, std::shared_ptr<DriverMonitorInfo>> starting_drivers_;

  std::shared_ptr<JobManager> job_manager_;

  int64_t cur_heatbeats_timeout_;
  /// The number of heartbeats that can be missed before an agent is removed.
  const int64_t num_heartbeats_timeout_;
  /// The runner to run function periodically.
  std::shared_ptr<PeriodicalRunner> periodical_runner_;
  /// Whether detecting dead agent.
  bool is_detecting_dead_agent_;

  std::function<void(rpc::GetWorkersInfoReply *reply)> fill_workers_info_;

  /// The callback for each update of the (local) runtime resources.
  std::function<void(const rpc::ReportLocalRuntimeResourcesRequest &)>
      runtime_resources_updated_callback_;

  FRIEND_TEST(AgentManagerTest, TestInitializeJobEnv);
  FRIEND_TEST(AgentManagerTest, TestDetectDeadAgentOnce);

  friend class AgentManagerTest;
};

class DefaultAgentManagerServiceHandler : public rpc::AgentManagerServiceHandler {
 public:
  explicit DefaultAgentManagerServiceHandler(std::unique_ptr<AgentManager> &delegate)
      : delegate_(delegate) {}

  void HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                           rpc::RegisterAgentReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleRegisterAgent(request, reply, send_reply_callback);
  }

  void HandleAgentHeartbeat(const rpc::AgentHeartbeatRequest &request,
                            rpc::AgentHeartbeatReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleAgentHeartbeat(request, reply, send_reply_callback);
  }

  void HandleGetWorkersInfo(const rpc::GetWorkersInfoRequest &request,
                            rpc::GetWorkersInfoReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleGetWorkersInfo(request, reply, send_reply_callback);
  }

  void HandleReportLocalRuntimeResources(
      const rpc::ReportLocalRuntimeResourcesRequest &request,
      rpc::ReportLocalRuntimeResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override {
    RAY_CHECK(delegate_ != nullptr);
    delegate_->HandleReportLocalRuntimeResources(request, reply, send_reply_callback);
  }

 private:
  std::unique_ptr<AgentManager> &delegate_;
};

}  // namespace raylet
}  // namespace ray
