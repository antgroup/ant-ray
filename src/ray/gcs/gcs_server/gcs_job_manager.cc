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

#include "ray/gcs/gcs_server/gcs_job_manager.h"

#include <memory>

#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/resource_util.h"

namespace ray {
namespace gcs {

void GcsJobManager::Initialize(const GcsInitData &gcs_init_data) {
  const auto &nodes = gcs_init_data.Nodes();
  for (auto &item : gcs_init_data.Jobs()) {
    auto job_data = std::make_shared<JobTableData>(item.second);
    // Add job data to local cache.
    jobs_.emplace(item.first, job_data);
    if (!job_data->is_dead()) {
      auto driver_node_id = NodeID::FromBinary(job_data->raylet_id());
      auto node_it = nodes.find(driver_node_id);
      if (node_it != nodes.end() &&
          node_it->second.basic_gcs_node_info().state() == rpc::BasicGcsNodeInfo::ALIVE &&
          job_data->driver_exit_state() == rpc::JobTableData_DriverExitState_UNKNOWN) {
        AddDriverToNode(driver_node_id, item.first);
      }

      const auto &nodegroup_id = job_data->nodegroup_id();
      AddJobToNodegroup(job_data, nodegroup_id);
    } else {
      sorted_dead_job_list_.emplace_back(
          item.first, static_cast<int64_t>(job_data->timestamp() * 1000));
    }
    const auto &ray_namespace = job_data->config().ray_namespace();
    ray_namespaces_[item.first] = ray_namespace;
  }
  sorted_dead_job_list_.sort(
      [](const std::pair<JobID, int64_t> &left, const std::pair<JobID, int64_t> &right) {
        return left.second < right.second;
      });

  auto job_name_callback = [this](rpc::Event &event) -> void {
    auto it = event.custom_fields().find("job_id");
    if (it == event.custom_fields().end() || it->second == "") {
      return;
    }
    auto job_table_data = this->GetJob(::ray::JobID::FromHex(it->second));
    if (job_table_data != nullptr) {
      event.mutable_custom_fields()->insert({"job_name", job_table_data->job_name()});
    }
  };
  ::ray::RayEventContext::Instance().SetEventPostProcessor(job_name_callback);
}

void GcsJobManager::HandleAddJob(const rpc::AddJobRequest &request,
                                 rpc::AddJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.data().job_id());
  auto driver_pid = request.data().driver_pid();
  RAY_LOG(INFO) << "Adding job, job id = " << job_id << ", driver pid = " << driver_pid
                << ", config is:\n"
                << request.data().config().DebugString();

  std::shared_ptr<JobTableData> job_table_data;
  if (job_id.IsSubmittedFromDashboard()) {
    auto iter = jobs_.find(job_id);
    if (iter == jobs_.end()) {
      RAY_LOG(WARNING) << "Failed to add job " << job_id
                       << " as the job is not submitted.";
      GCS_RPC_SEND_REPLY(send_reply_callback, reply,
                         Status::Invalid("Job is not submitted."));
      return;
    }

    if (iter->second->state() != rpc::JobTableData_JobState_SUBMITTED) {
      if (iter->second->timestamp() == request.data().timestamp() &&
          iter->second->state() == rpc::JobTableData_JobState_RUNNING) {
        // It is a duplicated message, just reply ok.
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      } else {
        std::ostringstream ostr;
        ostr << "Failed to add job " << job_id
             << " as job id is conflicted or state is unexpected.";
        RAY_LOG(WARNING) << ostr.str();
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(ostr.str()));
      }
      return;
    }

    if (nodegroup_manager_->IsJobQuotaEnabled(iter->second->nodegroup_id()) &&
        !nodegroup_manager_->IsSubnodegroupID(iter->second->nodegroup_id())) {
      auto total_memory_units = iter->second->config().total_memory_units();
      auto max_total_memory_units = iter->second->config().max_total_memory_units();
      auto new_total_memory_units = request.data().config().total_memory_units();
      auto new_max_total_memory_units = request.data().config().max_total_memory_units();
      // If the driver has changed the job quota, report this error.
      if (total_memory_units != new_total_memory_units ||
          (new_max_total_memory_units >= new_total_memory_units &&
           max_total_memory_units != new_max_total_memory_units)) {
        std::ostringstream ostr;
        ostr << "Failed to add job " << job_id
             << " because of different job quota from dashboard and driver.\n"
             << "From dashboard: (total_memory_units: " << total_memory_units
             << ", max_total_memory_units: " << max_total_memory_units << ")"
             << "\nFrom driver: (total_memory_units: " << new_total_memory_units
             << ", max_total_memory_units: " << new_max_total_memory_units << ")";
        RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_DRIVER_START)
                .WithField("job_id", job_id.Hex())
            << ostr.str();
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(ostr.str()));
        return;
      }
    }

    job_table_data = iter->second;
    job_table_data->set_raylet_id(request.data().raylet_id());
    job_table_data->set_driver_ip_address(request.data().driver_ip_address());
    job_table_data->set_driver_hostname(request.data().driver_hostname());
    job_table_data->set_driver_pid(request.data().driver_pid());
    job_table_data->set_driver_cmdline(request.data().driver_cmdline());
    job_table_data->set_language(request.data().language());
    job_table_data->mutable_config()->CopyFrom(request.data().config());
    job_table_data->set_state(rpc::JobTableData_JobState_RUNNING);
  } else {
    auto iter = jobs_.find(job_id);
    if (iter != jobs_.end()) {
      if (iter->second->timestamp() == request.data().timestamp() &&
          iter->second->state() == rpc::JobTableData_JobState_RUNNING) {
        // It is a duplicated message, just reply ok.
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      } else {
        std::ostringstream ostr;
        ostr << "Failed to add job " << job_id
             << " as job id is conflicted or state is unexpected.";
        RAY_LOG(WARNING) << ostr.str();
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(ostr.str()));
      }
      return;
    }

    // Just use the job_table_data come from raylet.
    job_table_data = std::make_shared<JobTableData>();
    job_table_data->CopyFrom(request.data());
    job_table_data->set_state(rpc::JobTableData_JobState_RUNNING);
  }

  auto time = current_sys_time_seconds();
  job_table_data->set_start_time(time);
  job_table_data->set_timestamp(time);

  // See https://aone.alipay.com/issue/100224461 for detailed scenario.
  // 1. Add job with running state
  // 2. Drop job with canceled state
  // 3. The callback of adding job is invoked, then the canceled state and is_dead(false)
  // will be published, and this will lead to the crasing of node manager in
  // HandleJobFinished.
  // So the serialized job table data should not be changed before and after put.
  auto serialized_job_table_data =
      std::make_shared<std::string>(job_table_data->SerializeAsString());
  auto on_done = [this, job_table_data, serialized_job_table_data, driver_pid, reply,
                  send_reply_callback](const Status &status) {
    RAY_CHECK_OK(status);
    auto job_id = JobID::FromBinary(job_table_data->job_id());
    jobs_.emplace(job_id, job_table_data);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       *serialized_job_table_data, nullptr));

    if (job_table_data->config().has_runtime_env()) {
      runtime_env_manager_.AddURIReference(job_id.Hex(),
                                           job_table_data->config().runtime_env());
    }

    RAY_LOG(INFO) << "Finished adding job, job id = " << job_id
                  << ", driver pid = " << driver_pid;
    ray_namespaces_[job_id] = job_table_data->config().ray_namespace();
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done));
}

void GcsJobManager::ClearJobInfos(const JobID &job_id) {
  // Notify all listeners.
  for (auto &listener : job_finished_listeners_) {
    listener(std::make_shared<JobID>(job_id));
  }
}

/// Add listener to monitor the finish action of jobs.
///
/// \param listener The handler which process the finish of jobs.
void GcsJobManager::AddJobFinishedListener(
    std::function<void(std::shared_ptr<JobID>)> listener) {
  RAY_CHECK(listener);
  job_finished_listeners_.emplace_back(std::move(listener));
}

void GcsJobManager::HandleGetAllJobInfo(const rpc::GetAllJobInfoRequest &request,
                                        rpc::GetAllJobInfoReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all the jobs.";
  for (auto &entry : jobs_) {
    reply->add_job_info_list()->CopyFrom(*entry.second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting all the jobs, size = " << jobs_.size();
}

void GcsJobManager::HandleReportJobError(const rpc::ReportJobErrorRequest &request,
                                         rpc::ReportJobErrorReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_error().job_id());

  RAY_CHECK_OK(gcs_pub_sub_->Publish(ERROR_INFO_CHANNEL, job_id.Hex(),
                                     request.job_error().SerializeAsString(), nullptr));
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

std::string GcsJobManager::GetRayNamespace(const JobID &job_id) const {
  auto it = ray_namespaces_.find(job_id);
  RAY_CHECK(it != ray_namespaces_.end()) << "Couldn't find job with id: " << job_id;
  return it->second;
}

void GcsJobManager::HandleGetNextJobID(const rpc::GetNextJobIDRequest &request,
                                       rpc::GetNextJobIDReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  reply->set_job_id(gcs_table_storage_->GetNextJobID());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
