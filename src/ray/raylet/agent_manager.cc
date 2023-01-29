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

#include "ray/raylet/agent_manager.h"

#include <thread>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

void AgentManager::HandleRegisterAgent(const rpc::RegisterAgentRequest &request,
                                       rpc::RegisterAgentReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  agent_ip_address_ = request.agent_ip_address();
  agent_port_ = request.agent_port();
  agent_pid_ = request.agent_pid();
  RAY_LOG(INFO) << "HandleRegisterAgent, ip: " << agent_ip_address_
                << ", port: " << agent_port_ << ", pid: " << agent_pid_;
  job_client_ = job_client_factory_(agent_ip_address_, agent_port_);
  event_client_ = event_client_factory_(agent_ip_address_, agent_port_);
  reporter_client_ = reporter_client_factory_(agent_ip_address_, agent_port_);
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::HandleAgentHeartbeat(const rpc::AgentHeartbeatRequest &request,
                                        rpc::AgentHeartbeatReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "HandleAgentHeartbeat";
  cur_heatbeats_timeout_ = 0;
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::DetectDeadAgent() {
  if (job_client_ == nullptr || agent_pid_ == 0) {
    RAY_LOG(INFO) << "Agent has not registered, skip detecting dead agent.";
    return;
  }
  if (++cur_heatbeats_timeout_ > num_heartbeats_timeout_) {
    std::ostringstream error_message;
    error_message << "Kill agent pid=" << agent_pid_ << " after "
                  << num_heartbeats_timeout_ << " * hangs."
                  << RayConfig::instance().agent_heartbeat_period_milliseconds() << "ms";
    RAY_LOG(ERROR) << error_message.str();
    RAY_EVENT(ERROR, EVENT_LABEL_KILL_AGENT) << error_message.str();

    Process child = Process::FromPid(agent_pid_);
    child.Kill(/*with_group = */ true);
    child.Wait();
    cur_heatbeats_timeout_ = 0;
    agent_ip_address_.clear();
    agent_port_ = 0;
    agent_pid_ = 0;
    job_client_.reset();
    event_client_.reset();
  }
}

void AgentManager::OnDriverRegistered(JobID job_id, pid_t driver_pid) {
  RAY_LOG(INFO) << "OnDriverRegistered, job id: " << job_id
                << ", driver pid: " << driver_pid;
  auto it = starting_drivers_.find(job_id);
  if (it != starting_drivers_.end()) {
    it->second->has_registered = true;
  } else if (Process::IsAlive(driver_pid)) {
    auto driver_monitor_info = std::make_shared<DriverMonitorInfo>(
        DriverMonitorInfo{job_id, /* has registered */ true, driver_pid, ""});
    starting_drivers_.emplace(job_id, driver_monitor_info);
  }
}

void AgentManager::OnJobEnvInitNotification(
    const ray::rpc::JobEnvInitNotification &notification) {
  auto job_table_data =
      std::make_shared<rpc::JobTableData>(notification.job_table_data());
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  if (initializing_jobs_
          .emplace(job_id, InitializingJobInfo({current_time_ms(), "", job_table_data}))
          .second) {
    InitializeJobEnv(job_table_data, notification.start_driver());
  }
}

void AgentManager::InitializeJobEnv(std::shared_ptr<rpc::JobTableData> job_data,
                                    bool start_driver) {
  auto job_id = JobID::FromBinary(job_data->job_id());

  if (options_.agent_commands.empty()) {
    RAY_LOG(INFO) << "Not to initialize the env for job " << job_id
                  << ", the agent command is empty.";
    if (start_driver) {
      RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_ENV_PREPARE_START)
              .WithField("job_id", job_id.Hex())
          << "the agent command is empty.";
    }
    return;
  }

  if (!initializing_jobs_.contains(job_id)) {
    RAY_LOG(INFO) << "Cancel initializing env for job " << job_id;
    return;
  }

  if (job_client_ == nullptr) {
    delay_executor_(
        [this, job_data, start_driver] { InitializeJobEnv(job_data, start_driver); },
        RayConfig::instance().agent_retry_interval_ms());
    return;
  }

  RAY_LOG(INFO) << "Start initializing the env for job " << job_id
                << ", start_driver = " << start_driver;
  if (start_driver) {
    RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_ENV_PREPARE_START)
            .WithField("job_id", job_id.Hex())
        << "Start initializing the env.";
  }

  rpc::InitializeJobEnvRequest request;
  request.mutable_job_data()->CopyFrom(*job_data);
  request.set_start_driver(start_driver);
  job_client_->InitializeJobEnv(request, [this, job_id, job_data, start_driver](
                                             Status status,
                                             const rpc::InitializeJobEnvReply &reply) {
    if (status.ok()) {
      if (reply.status() == rpc::AGENT_RPC_STATUS_OK) {
        if (start_driver) {
          auto it = starting_drivers_.find(job_id);
          if (it != starting_drivers_.end()) {
            RAY_LOG(INFO) << "Driver " << reply.driver_pid() << " of job " << job_id
                          << " has been registered.";
            RAY_CHECK(it->second->has_registered);
            starting_drivers_.erase(job_id);
          } else {
            RAY_LOG(INFO) << "Monitor driver " << reply.driver_pid() << " of job "
                          << job_id;
            auto driver_monitor_info = std::make_shared<DriverMonitorInfo>(
                DriverMonitorInfo{job_id, /* has registered */ false, reply.driver_pid(),
                                  reply.driver_cmdline()});
            starting_drivers_.emplace(job_id, driver_monitor_info);
            MonitorStartingDriver(driver_monitor_info);
          }
          RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_ENV_PREPARE_END)
                  .WithField("job_id", job_id.Hex())
              << "Finished initializing the env.\n"
              << reply.stats();
        }
        RAY_LOG(INFO) << "Finished initializing the env for job " << job_id;
        initializing_jobs_.erase(job_id);
        initialized_jobs_.emplace(job_id);
        on_job_env_initialized_(job_id, Status::OK());
      } else {
        std::ostringstream ostr;
        ostr << "Failed to initialize env for job " << job_id
             << ", error message: " << reply.error_message();
        std::string error_msg = ostr.str();
        RAY_LOG(ERROR) << error_msg;
        auto error_data_ptr = gcs::CreateErrorTableData("InitializeJobEnv", error_msg,
                                                        current_time_ms(), job_id);
        RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
        if (start_driver) {
          // Mark job failed only if initailize env for driver failed.
          RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFailed(
              job_id, error_msg, reply.driver_cmdline(), nullptr));
          RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_ENV_PREPARE_END)
                  .WithField("job_id", job_id.Hex())
              << error_msg;
          initializing_jobs_.erase(job_id);
          on_job_env_initialized_(job_id, Status::IOError(error_msg));
        } else {
          // Retry initializing job even if the job has fatal error.
          auto it = initializing_jobs_.find(job_id);
          if (it != initializing_jobs_.end()) {
            if (!job_data->pre_initialize_job_runtime_env_enabled()) {
              initializing_jobs_.erase(job_id);
              on_job_env_initialized_(job_id, Status::IOError(error_msg));
            } else {
              it->second.last_error_message = error_msg;
              delay_executor_(
                  [this, job_data, start_driver] {
                    InitializeJobEnv(job_data, start_driver);
                  },
                  RayConfig::instance().agent_retry_fatal_interval_ms());
            }
          } else {
            RAY_LOG(INFO) << "Just ignore the job initialization failed as the job "
                          << job_id << " is already dead.";
          }
        }
      }
    } else {
      RAY_LOG(ERROR) << "Failed to initialize the env for job " << job_id
                     << " with status = " << status
                     << ", maybe there are some network problems, try initialize "
                        "this job env later.";
      job_client_.reset();
      delay_executor_(
          [this, job_data, start_driver] { InitializeJobEnv(job_data, start_driver); },
          RayConfig::instance().agent_retry_interval_ms());
    }
  });
}

void AgentManager::WarnJobEnvUninitialized(const JobID &job_id) {
  auto it = initializing_jobs_.find(job_id);
  if (it != initializing_jobs_.end()) {
    const auto current = current_time_ms();
    if (current - it->second.last_warn_uninitialized_time >
        RayConfig::instance().agent_warn_job_initialized_interval_ms()) {
      it->second.last_warn_uninitialized_time = current;
      std::ostringstream warn_message;
      warn_message << "The node " << options_.node_id
                   << " has not prepared the environment for job " << job_id;
      if (!it->second.last_error_message.empty()) {
        warn_message << ", error message: " << it->second.last_error_message;
      }
      RAY_LOG(WARNING) << warn_message.str();
      RAY_EVENT(WARNING, EVENT_LABEL_JOB_ENV_PREPARE_NOT_FINISH) << warn_message.str();
    }
  } else {
    RAY_LOG(ERROR) << "The node " << options_.node_id
                   << " is not initializing environment for job " << job_id;
  }
}

void AgentManager::StartAgent() {
  if (options_.agent_commands.empty()) {
    RAY_LOG(INFO) << "Not to start agent, the agent command is empty.";
    return;
  }

  if (RAY_LOG_ENABLED(DEBUG)) {
    std::stringstream stream;
    stream << "Starting agent process with command:";
    for (const auto &arg : options_.agent_commands) {
      stream << " " << arg;
    }
    RAY_LOG(DEBUG) << stream.str();
  }

  // Launch the process to create the agent.
  std::error_code ec;
  std::vector<const char *> argv;
  for (const std::string &arg : options_.agent_commands) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(NULL);
  // Set node id to agent.
  ProcessEnvironment env;
  env.insert({"RAY_NODE_ID", options_.node_id.Hex()});
  env.insert({"RAY_RAYLET_PID", std::to_string(getpid())});
  Process child(argv.data(), nullptr, ec, false, env);
  if (!child.IsValid() || ec) {
    // The worker failed to start. This is a fatal error.
    RAY_LOG(FATAL) << "Failed to start agent with return value " << ec << ": "
                   << ec.message();
    RAY_UNUSED(delay_executor_([this] { StartAgent(); },
                               RayConfig::instance().agent_restart_interval_ms()));
    return;
  }
  std::thread monitor_thread([this, child]() mutable {
    SetThreadName("agent.monitor");
    RAY_LOG(INFO) << "Monitor agent process with pid " << child.GetId()
                  << ", register timeout "
                  << RayConfig::instance().agent_register_timeout_ms() << "ms.";
    auto timer = delay_executor_(
        [this, child]() mutable {
          if (agent_pid_ != child.GetId()) {
            std::ostringstream error_message;
            error_message << "Agent process with pid " << child.GetId()
                          << " has not registered, restart it.";
            RAY_LOG(ERROR) << error_message.str();
            RAY_EVENT(ERROR, EVENT_LABEL_AGENT_NOT_REGISTERED) << error_message.str();
            child.Kill(/*with_group = */ true);
          }
        },
        RayConfig::instance().agent_register_timeout_ms());

    int exit_code = child.Wait();
    timer->cancel();

    std::ostringstream error_message;
    error_message << "Agent process with pid " << child.GetId() << " exit, return value "
                  << exit_code;
    RAY_LOG(ERROR) << error_message.str();
    RAY_EVENT(ERROR, EVENT_LABEL_AGENT_EXIT) << error_message.str();
    RAY_UNUSED(delay_executor_([this] { StartAgent(); },
                               RayConfig::instance().agent_restart_interval_ms()));
  });
  monitor_thread.detach();

  StartDetectDeadAgent();
}

bool AgentManager::StartDetectDeadAgent() {
  if (!is_detecting_dead_agent_ && periodical_runner_) {
    is_detecting_dead_agent_ = true;
    periodical_runner_->RunFnPeriodically(
        [this] { DetectDeadAgent(); },
        RayConfig::instance().agent_heartbeat_period_milliseconds());
    RAY_LOG(INFO) << "Detecting agent heartbeat every "
                  << RayConfig::instance().agent_heartbeat_period_milliseconds() << "ms";
    return true;
  } else {
    RAY_LOG(INFO)
        << "Detect agent heartbeat is already started or periodical runner is empty.";
    return false;
  }
}

void AgentManager::ClearJobEnv(const JobTableData &job_data) {
  auto job_id = JobID::FromBinary(job_data.job_id());
  if (options_.agent_commands.empty()) {
    RAY_LOG(INFO) << "Not to clear the env for job " << job_id
                  << ", the agent command is empty.";
    return;
  }

  initializing_jobs_.erase(job_id);
  starting_drivers_.erase(job_id);
  initialized_jobs_.erase(job_id);

  if (job_client_ == nullptr) {
    RAY_LOG(INFO) << "The agent is not available, try clean job " << job_id
                  << " env later.";
    RAY_UNUSED(delay_executor_([this, job_data] { ClearJobEnv(job_data); },
                               RayConfig::instance().agent_retry_interval_ms()));
    return;
  }
  RAY_LOG(INFO) << "Start clearing the env for job, job id = " << job_id;
  rpc::CleanJobEnvRequest request;
  request.set_job_id(job_id.Binary());
  request.set_driver_pid(
      agent_ip_address_ == job_data.driver_ip_address() ? job_data.driver_pid() : 0);
  job_client_->CleanJobEnv(
      request,
      [this, job_id, job_data](Status status, const rpc::CleanJobEnvReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Finished clearing the env for job, job id = " << job_id;
        } else {
          RAY_LOG(ERROR) << "Failed to clean job " << job_id
                         << " env with status = " << status
                         << ", try clean job env later.";
          delay_executor_([this, job_data] { ClearJobEnv(job_data); },
                          RayConfig::instance().agent_retry_interval_ms());
        }
      });
}

void AgentManager::MonitorStartingDriver(
    std::shared_ptr<DriverMonitorInfo> driver_monitor_info) {
  if (!starting_drivers_.contains(driver_monitor_info->job_id)) {
    RAY_LOG(INFO) << "Cancel monitoring job " << driver_monitor_info->job_id << " driver "
                  << driver_monitor_info->driver_pid;
  } else if (driver_monitor_info->has_registered) {
    RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_DRIVER_START)
            .WithField("job_id", driver_monitor_info->job_id.Hex())
        << "Driver has been registered.";
    RAY_LOG(INFO) << "Driver " << driver_monitor_info->driver_pid << " of job "
                  << driver_monitor_info->job_id << " has been registered.";
    starting_drivers_.erase(driver_monitor_info->job_id);
  } else if (Process::IsAlive(driver_monitor_info->driver_pid)) {
    delay_executor_(
        [this, driver_monitor_info] { MonitorStartingDriver(driver_monitor_info); },
        RayConfig::instance().agent_monitor_driver_starting_interval_ms());
  } else {
    RAY_LOG(INFO) << "Driver " << driver_monitor_info->driver_pid << " of job "
                  << driver_monitor_info->job_id << " was dead.";
    starting_drivers_.erase(driver_monitor_info->job_id);
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_DRIVER_START)
            .WithField("job_id", driver_monitor_info->job_id.Hex())
        << "Driver was dead.";
    std::ostringstream ostr;
    ostr << "Failed to start driver " << driver_monitor_info->driver_pid << " of job "
         << driver_monitor_info->job_id
         << ", the driver process exits before registering to the raylet. "
         << "Please check the driver log for details and verify the driver cmdline is "
            "correct.";
    RAY_CHECK_OK(gcs_client_->Jobs().AsyncMarkFailed(
        driver_monitor_info->job_id, ostr.str(), driver_monitor_info->driver_cmdline,
        nullptr));
  }
}

void AgentManager::CheckJavaHsErrLog(std::string log_dir, std::string ip, int pid,
                                     bool is_driver, const JobID job_id,
                                     const WorkerID worker_id, const ActorID actor_id,
                                     const TaskID task_id) {
  if (event_client_ == nullptr) {
    RAY_LOG(INFO) << "Can not CheckJavaHsErrLog because the event client is nullptr.";
    return;
  }

  rpc::CheckJavaHsErrLogRequest request;
  std::ostringstream oss;
  oss << log_dir << "/hs_err_pid" << pid << ".log";
  request.set_log_path(oss.str());
  request.set_job_id(job_id.Hex());
  auto job_data = job_manager_->GetJobData(job_id);
  request.set_job_name(job_data != nullptr ? job_data->job_name() : "");
  request.set_worker_id(worker_id.Hex());
  request.set_actor_id(actor_id.Hex());
  request.set_pid(pid);
  request.set_ip(ip);
  request.set_is_driver(is_driver);
  event_client_->CheckJavaHsErrLog(
      request,
      [job_id, request](Status status, const rpc::CheckJavaHsErrLogReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Finished check Java hs err log for job " << job_id
                        << ", log path: " << request.log_path();
        } else {
          RAY_LOG(ERROR) << "Failed to check Java hs err log for job " << job_id
                         << " with status " << status
                         << ", log path: " << request.log_path();
        }
      });
}

bool AgentManager::IsJobEnvInitialized(const JobID &job_id) {
  return initialized_jobs_.count(job_id);
}

void AgentManager::OnRuntimeResourceScheduleOptionsUpdated(
    const RuntimeResourceScheduleOptions &schedule_options) {
  if (reporter_client_ == nullptr) {
    RAY_LOG(INFO)
        << "Can not update schedule options because the reporter client is nullptr.";
    return;
  }

  rpc::UpdateRuntimeResourceScheduleOptionsRequest request;
  request.set_runtime_resource_scheduling_enabled(
      schedule_options.runtime_resource_scheduling_enabled_);
  request.set_runtime_resources_calculation_interval_s(
      schedule_options.runtime_resources_calculation_interval_s_);
  request.set_runtime_memory_tail_percentile(
      schedule_options.runtime_memory_tail_percentile_);
  request.set_runtime_cpu_tail_percentile(schedule_options.runtime_cpu_tail_percentile_);
  reporter_client_->UpdateRuntimeResourceScheduleOptions(
      request,
      [](Status status, const rpc::UpdateRuntimeResourceScheduleOptionsReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Runtime resource schedule options have been reported to "
                           "reporter agent.";
        }
      });
}

void AgentManager::HandleGetWorkersInfo(const rpc::GetWorkersInfoRequest &request,
                                        rpc::GetWorkersInfoReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "HandleGetWorkersInfo";
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  fill_workers_info_(reply);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void AgentManager::HandleReportLocalRuntimeResources(
    const rpc::ReportLocalRuntimeResourcesRequest &request,
    rpc::ReportLocalRuntimeResourcesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "HandleReportLocalRuntimeResources";
  runtime_resources_updated_callback_(request);
  reply->set_status(rpc::AGENT_RPC_STATUS_OK);
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

}  // namespace raylet
}  // namespace ray
