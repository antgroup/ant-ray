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

#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/gcs_server/gcs_frozen_node_manager.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/pb_util.h"
#include "ray/stats/stats.h"
#include "ray/util/resource_util.h"

namespace ray {
namespace gcs {

static inline std::string ToString(
    const ::google::protobuf::Map<std::string, double> &job_resources) {
  std::ostringstream ostr;
  for (const auto &entry : job_resources) {
    ostr << entry.first << ": " << entry.second << ", ";
  }
  return ostr.str();
}

static inline Status UpdateJobResourceRequirements(
    const ::google::protobuf::Map<std::string, double> &min_resource_requirements,
    const ::google::protobuf::Map<std::string, double> &max_resource_requirements,
    rpc::JobConfig *mutable_job_config) {
  auto iter = min_resource_requirements.find("totalMemoryMb");
  if (iter != min_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! totalMemoryMb(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_total_memory_units(
        ToMemoryUnits(static_cast<uint64_t>(iter->second) * 1024 * 1024));
  }

  iter = min_resource_requirements.find("totalCpus");
  if (iter != min_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! totalCpus(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_total_cpus(static_cast<uint64_t>(iter->second));
  }

  iter = min_resource_requirements.find("totalGpus");
  if (iter != min_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! totalGpus(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_total_gpus(static_cast<uint64_t>(iter->second));
  }

  iter = max_resource_requirements.find("maxTotalMemoryMb");
  if (iter != max_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! maxTotalMemoryMb(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_max_total_memory_units(
        ToMemoryUnits(static_cast<uint64_t>(iter->second) * 1024 * 1024));
  }

  iter = max_resource_requirements.find("maxTotalCpus");
  if (iter != max_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! maxTotalCpus(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_max_total_cpus(static_cast<uint64_t>(iter->second));
  }

  iter = max_resource_requirements.find("maxTotalGpus");
  if (iter != max_resource_requirements.end()) {
    if (iter->second <= 0) {
      std::ostringstream ostr;
      ostr << "Illegal arguments! maxTotalGpus(" << iter->second
           << ") must be greater than 0.";
      return Status::Invalid(ostr.str());
    }
    mutable_job_config->set_max_total_gpus(static_cast<uint64_t>(iter->second));
  }

  return Status::OK();
}

void GcsJobManager::HandlePutJobData(const rpc::PutJobDataRequest &request,
                                     rpc::PutJobDataReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  job_data_[job_id][request.key()] = request.data();
  RAY_CHECK_OK(gcs_table_storage_->JobDataTable().PutData(
      job_id, request.key(), request.data(),
      [send_reply_callback, reply](const Status &status) {
        RAY_CHECK_OK(status);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

void GcsJobManager::HandleGetJobData(const rpc::GetJobDataRequest &request,
                                     rpc::GetJobDataReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  auto it = job_data_.find(job_id);
  if (it != job_data_.end()) {
    auto found = it->second.find(request.key());
    if (found != it->second.end()) {
      reply->set_data(found->second);
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      return;
    }
  }

  RAY_CHECK_OK(gcs_table_storage_->JobDataTable().GetData(
      job_id, request.key(),
      [this, job_id, request, send_reply_callback, reply](const std::string &data) {
        job_data_[job_id][request.key()] = data;
        reply->set_data(data);
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
      }));
}

void GcsJobManager::HandleNotifyDriverExit(const rpc::NotifyDriverExitRequest &request,
                                           rpc::NotifyDriverExitReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  bool exit_with_error = request.exit_with_error();
  RAY_LOG(INFO) << "Received driver exit notification, job id = " << job_id
                << ", exit with error = " << exit_with_error;
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    RAY_LOG(WARNING) << "Failed to handle the notification of driver exit. job id = "
                     << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Invalid job id."));
    return;
  }
  auto job_table_data = iter->second;
  if (job_table_data->is_dead()) {
    RAY_LOG(INFO) << "Job is already dead, just ignore this notification, job id = "
                  << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  if (exit_with_error) {
    job_table_data->set_driver_exit_state(rpc::JobTableData_DriverExitState_ERROR);
  } else {
    job_table_data->set_driver_exit_state(rpc::JobTableData_DriverExitState_OK);
  }

  // Remove driver from node.
  RemoveDriverFromNode(NodeID::FromBinary(job_table_data->raylet_id()), job_id);

  if (job_table_data->config().long_running() &&
      job_table_data->driver_exit_state() == rpc::JobTableData_DriverExitState_OK) {
    RAY_LOG(INFO) << "This is a long-running job and the driver exits without "
                     "error. The job will not be marked as finished. job id = "
                  << job_id;
    RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(
        job_id, *job_table_data, [send_reply_callback, reply](const Status &status) {
          RAY_CHECK_OK(status);
          GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
        }));
    RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_LONG_RUNNING)
            .WithField("job_id", job_id.Hex())
        << "job long running.";
    return;
  }

  UpdateJobStateToDead(job_table_data,
                       [send_reply_callback, reply](const Status &status) {
                         RAY_CHECK_OK(status);
                         GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                       });
  RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_FINISHED).WithField("job_id", job_id.Hex())
      << "job finished and cleaned.";
}

void GcsJobManager::HandleMarkJobFailed(const rpc::MarkJobFailedRequest &request,
                                        rpc::MarkJobFailedReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Marking job as failed, job id = " << job_id;
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    RAY_LOG(WARNING) << "Failed to mark job as failed, job id = " << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid("Invalid job id."));
    return;
  }

  auto job_table_data = iter->second;
  if (job_table_data->is_dead()) {
    RAY_LOG(INFO) << "Job is already dead, just ignore this event, job id = " << job_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }

  // Remove driver from node.
  RemoveDriverFromNode(NodeID::FromBinary(job_table_data->raylet_id()), job_id);

  job_table_data->set_fail_error_message(request.error_message());
  job_table_data->set_driver_cmdline(request.driver_cmdline());
  UpdateJobStateToDead(job_table_data,
                       [send_reply_callback, reply](const Status &status) {
                         RAY_CHECK_OK(status);
                         GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
                       });
}

void GcsJobManager::HandleUpdateJobResourceRequirements(
    const rpc::UpdateJobResourceRequirementsRequest &request,
    rpc::UpdateJobResourceRequirementsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Updating job resource requirements. job_id = " << job_id
                << ", min_resource_requirements = "
                << ToString(request.min_resource_requirements())
                << ", max_resource_requirements = "
                << ToString(request.max_resource_requirements());

  auto job_table_data = GetJob(job_id);
  if (job_table_data == nullptr) {
    std::string msg =
        "Failed to update job resource requirements as the specified job is invalid. "
        "job_id = " +
        job_id.Hex();
    RAY_LOG(INFO) << msg;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(msg));
    return;
  }

  const auto &nodegroup_id = job_table_data->nodegroup_id();
  if (!nodegroup_manager_->IsJobQuotaEnabled(nodegroup_id)) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    return;
  }
  if (nodegroup_id != "" && nodegroup_id != NODEGROUP_RESOURCE_DEFAULT) {
    auto min_resource_requirements =
        ExtractMinJobResourceRequirements(request.min_resource_requirements());
    if (!min_resource_requirements.IsEmpty()) {
      auto max_resource_requirements =
          ExtractMaxJobResourceRequirements(request.max_resource_requirements());
      if (!max_resource_requirements.IsSuperset(min_resource_requirements)) {
        std::ostringstream ss;
        ss << "Failed to update job's resource requirements as the max resource "
              "requirements must be greater than the min resource requirements."
           << "\nJobID: " << job_id.Hex() << "\nNodegroup: " << nodegroup_id
           << "\nMin resource requirements: " << min_resource_requirements.ToString()
           << "\nMax resource requirements: " << max_resource_requirements.ToString();
        std::string message = ss.str();
        RAY_LOG(WARNING) << message;
        RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(message));
        return;
      }

      auto schedule_options = nodegroup_manager_->GetScheduleOptions(nodegroup_id);
      auto status = VerifyJobResourceRequirementsRatio(
          job_id, nodegroup_id, min_resource_requirements, max_resource_requirements,
          schedule_options);
      if (!status.ok()) {
        std::string message = status.ToString();
        RAY_LOG(WARNING) << message;
        RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
        return;
      }

      float overcommit_ratio = schedule_options->runtime_resource_scheduling_enabled_
                                   ? schedule_options->overcommit_ratio_
                                   : 1.0;
      ResourceSet available_resources;
      if (!HasEnoughAvailableResources(job_id, nodegroup_id, min_resource_requirements,
                                       /*overcommit_ratio=*/overcommit_ratio,
                                       /*use_runtime_resources*/ false,
                                       &available_resources)) {
        std::ostringstream ss;
        ss << "Failed to update job resource requirements as the nodegroup lacks of "
              "enough resources."
           << "\nJobID: " << job_id.Hex() << "\nNodegroup: " << nodegroup_id
           << "\nMin job resource requirements: " << min_resource_requirements.ToString()
           << "\nMax job resource requirements: " << max_resource_requirements.ToString()
           << "\nAvailable resources: " << available_resources.ToString();
        std::string message = ss.str();
        RAY_LOG(WARNING) << message;
        RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
        GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::Invalid(message));
        return;
      }
    }
  }

  JobTableData copied_job_table_data;
  copied_job_table_data.CopyFrom(*job_table_data);
  auto status = UpdateJobResourceRequirements(request.min_resource_requirements(),
                                              request.max_resource_requirements(),
                                              copied_job_table_data.mutable_config());
  if (!status.ok()) {
    std::ostringstream ss;
    ss << "Failed to update job resource requirements. " << status;
    std::string message = ss.str();
    RAY_LOG(WARNING) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
    return;
  }

  if (job_resource_requirements_changed_callback_) {
    auto status = job_resource_requirements_changed_callback_(copied_job_table_data);
    if (!status.ok()) {
      GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
      return;
    }
  }

  job_table_data->CopyFrom(copied_job_table_data);
  auto on_done = [this, job_id, job_table_data, send_reply_callback,
                  reply](const Status &status) {
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_QUOTA_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
    RAY_LOG(INFO) << "Finished updating job resource requirements. job_id = " << job_id;
  };
  RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done));
}

void GcsJobManager::UpdateJobStateToDead(std::shared_ptr<JobTableData> job_table_data,
                                         const ray::gcs::StatusCallback &callback) {
  auto cloned_job_table_data = std::make_shared<JobTableData>(*job_table_data);
  // Update job state.
  if (cloned_job_table_data->state() != rpc::JobTableData_JobState_CANCEL) {
    if (cloned_job_table_data->driver_exit_state() ==
        rpc::JobTableData_DriverExitState_OK) {
      cloned_job_table_data->set_state(rpc::JobTableData_JobState_FINISHED);
    } else {
      cloned_job_table_data->set_state(rpc::JobTableData_JobState_FAILED);
    }
  }
  JobID job_id = JobID::FromBinary(cloned_job_table_data->job_id());
  RAY_LOG(INFO) << "Updating job state to "
                << rpc::JobTableData_JobState_Name(cloned_job_table_data->state())
                << ", job id = " << job_id << ", driver exit state = "
                << rpc::JobTableData_DriverExitState_Name(
                       job_table_data->driver_exit_state());
  auto time = current_sys_time_seconds();
  cloned_job_table_data->set_timestamp(time);
  cloned_job_table_data->set_end_time(time);
  cloned_job_table_data->set_is_dead(true);
  auto on_done = [this, callback, job_id, job_table_data,
                  cloned_job_table_data](const Status &status) {
    RAY_CHECK_OK(status);
    // Update state in GCS memory after flushed to storage.
    // See https://aone.alipay.com/v2/project/1039366/req/36613254 for detailed scenario.
    *job_table_data = std::move(*cloned_job_table_data);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    // Remove running job from nodegroup.
    RemoveJobFromNodegroup(job_id, job_table_data->nodegroup_id());
    runtime_env_manager_.RemoveURIReference(job_id.Hex());
    ClearJobInfos(job_id);
    AddDeadJobToCache(job_table_data);
    if (callback) {
      callback(status);
    }
    RAY_LOG(INFO) << "Finished updating job state to "
                  << rpc::JobTableData_JobState_Name(job_table_data->state())
                  << ", job id = " << job_id << ", driver exit state = "
                  << rpc::JobTableData_DriverExitState_Name(
                         job_table_data->driver_exit_state());
  };
  RAY_CHECK_OK(
      gcs_table_storage_->JobTable().Put(job_id, *cloned_job_table_data, on_done));
}

void GcsJobManager::HandleGetJob(const rpc::GetJobRequest &request,
                                 rpc::GetJobReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(DEBUG) << "Getting job state, job id = " << job_id;
  auto iter = jobs_.find(job_id);
  if (iter != jobs_.end()) {
    reply->mutable_data()->CopyFrom(*iter->second);
  }
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  RAY_LOG(DEBUG) << "Finished getting job state, job id = " << job_id;
}

void GcsJobManager::HandleSubmitJob(const rpc::SubmitJobRequest &request,
                                    rpc::SubmitJobReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback](const Status &status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = SubmitJob(request, on_done);
  if (!status.ok()) {
    if (status.IsJobNameConflict()) {
      if (auto job = GetRunningJobByName(request.job_name())) {
        reply->set_conflict_job_id(job->job_id());
      }
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsJobManager::HandleDropJob(const rpc::DropJobRequest &request,
                                  rpc::DropJobReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  auto on_done = [reply, send_reply_callback](const Status &status) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };
  auto status = DropJob(request, on_done);
  if (!status.ok()) {
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  }
}

void GcsJobManager::SetJobResourceRequirementsChangedCallback(
    std::function<Status(const JobTableData &)> callback) {
  job_resource_requirements_changed_callback_ = std::move(callback);
}

Status GcsJobManager::DropJob(const ray::rpc::DropJobRequest &request,
                              const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  RAY_LOG(INFO) << "Starting drop job, job id = " << job_id;

  auto job_table_data = GetJob(job_id);
  if (job_table_data == nullptr) {
    std::ostringstream ostr;
    ostr << "Failed to drop job " << job_id << " as it does not exist, just reply ok.";
    RAY_LOG(WARNING) << ostr.str();
    callback(Status::OK());
    return Status::OK();
  }

  if (job_table_data->is_dead()) {
    std::ostringstream ostr;
    ostr << "Failed to drop job " << job_id << " as it is already dead, just reply ok.";
    callback(Status::OK());
    return Status::OK();
  }

  // Remove driver from node.
  RemoveDriverFromNode(NodeID::FromBinary(job_table_data->raylet_id()), job_id);

  const auto job_state = request.mark_as_failed() ? rpc::JobTableData_JobState_FAILED
                                                  : rpc::JobTableData_JobState_CANCEL;
  job_table_data->set_state(job_state);
  UpdateJobStateToDead(job_table_data, [job_id, callback](const Status &status) {
    RAY_CHECK_OK(status);
    callback(status);
    RAY_LOG(INFO) << "Finished dropping job " << job_id;
  });
  return Status::OK();
}

void GcsJobManager::OnDriverNodeRemoved(const JobID &job_id, const NodeID &node_id) {
  std::string event = "The driver exited as the node " + node_id.Hex() +
                      " it was in was dead, job id = " + job_id.Hex();
  RAY_LOG(WARNING) << event;
  auto iter = jobs_.find(job_id);
  if (iter == jobs_.end()) {
    RAY_LOG(WARNING) << "Ignore this event as the job is not submitted, job id = "
                     << job_id;
    return;
  }

  auto job_table_data = iter->second;
  if (job_table_data->is_dead()) {
    RAY_LOG(INFO) << "Job is already dead, just ignore this event. job id = " << job_id;
    return;
  }

  if (job_table_data->config().long_running() &&
      job_table_data->driver_exit_state() == rpc::JobTableData_DriverExitState_OK) {
    RAY_LOG(INFO) << "This is a long-running job and it already exits gracefully, so "
                     "the job state sould not be changed. job id = "
                  << job_id;
    return;
  }

  RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_DRIVER_EXIT).WithField("job_id", job_id.Hex())
      << event << ", long-running = " << job_table_data->config().long_running();

  UpdateJobStateToDead(job_table_data, nullptr);
}

std::shared_ptr<rpc::JobTableData> GcsJobManager::GetJob(const ray::JobID &job_id) const {
  auto iter = jobs_.find(job_id);
  return iter != jobs_.end() ? iter->second : nullptr;
}

std::string GcsJobManager::GetJobNodegroupId(const ray::JobID &job_id) const {
  if (auto job = GetJob(job_id)) {
    return job->nodegroup_id();
  }
  return "";
}

std::string GcsJobManager::GetJobName(const ray::JobID &job_id) const {
  if (auto job = GetJob(job_id)) {
    return job->job_name();
  }
  return "UNKNOWN";
}

void GcsJobManager::FlushJobTableDataToStorage(const JobID &job_id) {
  auto iter = jobs_.find(job_id);
  if (iter != jobs_.end()) {
    RAY_CHECK_OK(gcs_table_storage_->JobTable().Put(job_id, *iter->second, nullptr));
  } else {
    RAY_LOG(INFO) << "Failed to flush job table data to storage, because the job "
                  << job_id << " does not exist.";
  }
}

ResourceSet GcsJobManager::ExtractMinJobResourceRequirements(
    const ::google::protobuf::Map<std::string, double> &min_resource_requirements) {
  std::unordered_map<std::string, double> resource_map;
  auto iter = min_resource_requirements.find("totalMemoryMb");
  if (iter != min_resource_requirements.end()) {
    resource_map.emplace(
        kMemory_ResourceLabel,
        ToMemoryUnits(iter->second * 1024 * 1024 + MEMORY_RESOURCE_UNIT_BYTES - 1));
  }

  iter = min_resource_requirements.find("totalCpu");
  if (iter != min_resource_requirements.end()) {
    resource_map.emplace(kCPU_ResourceLabel, iter->second);
  }

  iter = min_resource_requirements.find("totalGpu");
  if (iter != min_resource_requirements.end()) {
    resource_map.emplace(kGPU_ResourceLabel, iter->second);
  }

  return ResourceSet(resource_map);
}

ResourceSet GcsJobManager::ExtractMaxJobResourceRequirements(
    const ::google::protobuf::Map<std::string, double> &max_resource_requirements) {
  std::unordered_map<std::string, double> resource_map;
  auto iter = max_resource_requirements.find("maxTotalMemoryMb");
  if (iter != max_resource_requirements.end()) {
    resource_map.emplace(
        kMemory_ResourceLabel,
        ToMemoryUnits(iter->second * 1024 * 1024 + MEMORY_RESOURCE_UNIT_BYTES - 1));
  }

  iter = max_resource_requirements.find("maxTotalCpu");
  if (iter != max_resource_requirements.end()) {
    resource_map.emplace(kCPU_ResourceLabel, iter->second);
  }

  iter = max_resource_requirements.find("maxTotalGpu");
  if (iter != max_resource_requirements.end()) {
    resource_map.emplace(kGPU_ResourceLabel, iter->second);
  }

  return ResourceSet(resource_map);
}

Status GcsJobManager::SubmitJob(const ray::rpc::SubmitJobRequest &request,
                                const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  const auto &job_name = request.job_name();
  const auto &nodegroup_id = request.nodegroup_id();

  RAY_LOG(INFO) << "Starting register job " << job_id << " with parent nodegroup "
                << nodegroup_id;

  if (auto job = GetJob(job_id)) {
    std::ostringstream ss;
    ss << "Failed to register job " << job_id << " to nodegroup " << nodegroup_id
       << " as the job id is conflict.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  if (auto job = GetRunningJobByName(job_name)) {
    std::ostringstream ss;
    ss << "Failed to register job " << job_id << " to nodegroup " << nodegroup_id
       << " as job " << JobID::FromBinary(job->job_id()) << " with the same job name ("
       << job_name << ") is still running.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::JobNameConflict(message);
  }

  auto nodegroup_data = nodegroup_manager_->GetNodegroupData(nodegroup_id);
  if (nodegroup_data == nullptr) {
    std::ostringstream ss;
    ss << "Failed to register job " << job_id << " to nodegroup " << nodegroup_id
       << " as the nodegroup does not exist.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  if (!nodegroup_data->enable_sub_nodegroup_isolation_) {
    return DoSubmitJob(request, callback);
  }

  if (request.node_shape_and_count_list_size() == 0) {
    std::ostringstream ss;
    ss << "Failed to submit job " << job_id << " to nodegroup " << nodegroup_id
       << " as the job does not specify the required node resources. This nodegroup does "
          "not allow mixed jobs, please check whether the job selects a wrong nodegroup.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  for (auto &entry : request.node_shape_and_count_list()) {
    if (entry.node_count() <= 0) {
      std::ostringstream ss;
      ss << "Failed to submit job " << job_id << " to nodegroup " << nodegroup_id
         << " as the node_count <= 0, expect (node_count > 0).";
      std::string message = ss.str();
      RAY_LOG(ERROR) << message;
      RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED)
              .WithField("job_id", job_id.Hex())
          << message;
      return Status::Invalid(message);
    }

    if (entry.node_shape().shape_group().empty() &&
        !entry.node_shape().resource_shape().empty()) {
      std::ostringstream ss;
      ss << "Failed to submit job " << job_id << " to nodegroup " << nodegroup_id
         << " as the RAY_SHAPE_GROUP env is not set.";
      return Status::Invalid(ss.str());
    }
  }

  auto new_submit_job_request = std::make_shared<ray::rpc::SubmitJobRequest>(request);
  auto add_sub_nodegroup_callback = [this, new_submit_job_request,
                                     callback](const std::string &subnodegroup_id) {
    new_submit_job_request->set_nodegroup_id(subnodegroup_id);
    auto status = DoSubmitJob(*new_submit_job_request, callback);
    if (!status.ok()) {
      callback(status);
    }
    return status;
  };

  return nodegroup_manager_->AddSubNodegroup(nodegroup_id, job_id, job_name,
                                             request.node_shape_and_count_list(),
                                             add_sub_nodegroup_callback);
}

Status GcsJobManager::DoSubmitJob(const ray::rpc::SubmitJobRequest &request,
                                  const ray::gcs::StatusCallback &callback) {
  auto job_id = JobID::FromBinary(request.job_id());
  const auto &job_name = request.job_name();
  const auto &nodegroup_id = request.nodegroup_id();

  if (auto job = GetRunningJobByName(job_name)) {
    std::ostringstream ss;
    ss << "Failed to register job " << job_id << " to nodegroup " << nodegroup_id
       << " as job " << JobID::FromBinary(job->job_id()) << " with the same job name ("
       << job_name << ") is still running.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return Status::JobNameConflict(message);
  }

  // Make sure the job's total memory is below nodegroup memory watermark.
  if (nodegroup_id != "" && nodegroup_id != NODEGROUP_RESOURCE_DEFAULT &&
      !GcsNodegroupManagerInterface::IsSubnodegroupID(nodegroup_id) &&
      nodegroup_manager_->IsJobQuotaEnabled(nodegroup_id)) {
    auto min_resource_requirements =
        ExtractMinJobResourceRequirements(request.min_resource_requirements());
    if (!min_resource_requirements.IsEmpty()) {
      auto max_resource_requirements =
          ExtractMaxJobResourceRequirements(request.max_resource_requirements());
      if (!max_resource_requirements.IsSuperset(min_resource_requirements)) {
        std::ostringstream ss;
        ss << "Failed to update job resource requirements as the max resource "
              "requirements must be greater than the min resource requirements."
           << "\nJobID: " << job_id.Hex() << "\nNodegroup: " << nodegroup_id
           << "\nMin resource requirements: " << min_resource_requirements.ToString()
           << "\nMax resource requirements: " << max_resource_requirements.ToString();
        std::string message = ss.str();
        RAY_LOG(WARNING) << message;
        RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
        return Status::Invalid(message);
      }

      ResourceSet total_available_resources;
      auto schedule_options = nodegroup_manager_->GetScheduleOptions(nodegroup_id);

      auto status = VerifyJobResourceRequirementsRatio(
          job_id, nodegroup_id, min_resource_requirements, max_resource_requirements,
          schedule_options);
      if (!status.ok()) {
        std::string message = status.ToString();
        RAY_LOG(WARNING) << message;
        RAY_EVENT(ERROR, EVENT_LABEL_UPDATE_JOB_TOTAL_RESOURCES_FAILED) << message;
        return status;
      }

      if (schedule_options->runtime_resource_scheduling_enabled_) {
        if (!HasEnoughAvailableResources(
                job_id, nodegroup_id, min_resource_requirements, /*overcommit_ratio=*/1.0,
                /*use_runtime_resources*/ true, &total_available_resources)) {
          return WarnClusterResourceNotEnough(
              job_id, nodegroup_id, min_resource_requirements, max_resource_requirements,
              total_available_resources);
        } else {
          total_available_resources = ResourceSet();
          if (!HasEnoughAvailableResources(
                  job_id, nodegroup_id, min_resource_requirements,
                  /*overcommit_ratio=*/schedule_options->overcommit_ratio_,
                  /*use_runtime_resources*/ false, &total_available_resources)) {
            return WarnClusterResourceNotEnough(
                job_id, nodegroup_id, min_resource_requirements,
                max_resource_requirements, total_available_resources);
          }
        }
      } else {
        if (!HasEnoughAvailableResources(
                job_id, nodegroup_id, min_resource_requirements, /*overcommit_ratio=*/1.0,
                /*use_runtime_resources*/ false, &total_available_resources)) {
          return WarnClusterResourceNotEnough(
              job_id, nodegroup_id, min_resource_requirements, max_resource_requirements,
              total_available_resources);
        }
      }
    }
  }

  auto job_table_data = std::make_shared<rpc::JobTableData>();
  job_table_data->set_job_id(request.job_id());
  job_table_data->set_job_name(request.job_name());
  job_table_data->set_nodegroup_id(request.nodegroup_id());
  job_table_data->set_language(request.language());
  job_table_data->set_job_payload(request.job_payload());
  job_table_data->set_state(rpc::JobTableData_JobState_SUBMITTED);
  job_table_data->set_pre_initialize_job_runtime_env_enabled(
      request.pre_initialize_job_runtime_env_enabled());
  RAY_LOG(INFO) << "The mode of initializing environment of job" << request.job_id()
                << " is: "
                << (request.pre_initialize_job_runtime_env_enabled() ? "eager."
                                                                     : "lazy.");
  auto status = UpdateJobResourceRequirements(request.min_resource_requirements(),
                                              request.max_resource_requirements(),
                                              job_table_data->mutable_config());
  if (!status.ok()) {
    std::ostringstream ss;
    ss << "Failed to register job " << job_id << " to nodegroup " << nodegroup_id << ". "
       << status;
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << message;
    return status;
  }

  auto driver_node_info = SelectDriver(*job_table_data);
  if (driver_node_info == nullptr) {
    std::ostringstream ss;
    ss << "Failed to select driver to init job " << job_id << " with nodegroup "
       << nodegroup_id << " as there are no available nodes.";
    std::string message = ss.str();
    RAY_LOG(ERROR) << message;
    RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_RESOURCES_CHECK)
            .WithField("job_id", job_id.Hex())
        << message;
    return Status::Invalid(message);
  }

  job_table_data->set_driver_hostname(
      driver_node_info->basic_gcs_node_info().node_manager_hostname());
  job_table_data->set_driver_ip_address(
      driver_node_info->basic_gcs_node_info().node_manager_address());
  job_table_data->set_raylet_id(driver_node_info->basic_gcs_node_info().node_id());

  // Add job to driver_node_to_jobs_.
  AddDriverToNode(NodeID::FromBinary(driver_node_info->basic_gcs_node_info().node_id()),
                  job_id);
  // Add job to nodegroup.
  AddJobToNodegroup(job_table_data, nodegroup_id);

  RAY_LOG(INFO) << "Submitting job, job id = " << job_id << ", config is "
                << job_table_data->config().DebugString()
                << ", the ip of the node which run driver is "
                << job_table_data->driver_ip_address();
  auto on_done = [this, job_id, job_table_data, callback](Status status) {
    if (!status.ok()) {
      RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_ADDED).WithField("job_id", job_id.Hex())
          << "Job added failed, the status: " << status.ToString();
    }
    RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_ADDED).WithField("job_id", job_id.Hex())
        << "Job added to job table.";
    RAY_CHECK(jobs_.emplace(job_id, job_table_data).second);
    RAY_CHECK_OK(gcs_pub_sub_->Publish(JOB_CHANNEL, job_id.Hex(),
                                       job_table_data->SerializeAsString(), nullptr));
    if (callback) {
      callback(status);
    }

    RAY_LOG(INFO) << "Finished submitting job, job id = " << job_id
                  << " with nodegroup id " << job_table_data->nodegroup_id();
    RAY_EVENT(INFO, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
        << "Job submitted to gcs.";
  };
  return gcs_table_storage_->JobTable().Put(job_id, *job_table_data, on_done);
}

void GcsJobManager::OnNodeRemoved(const NodeID &node_id) {
  // Notify the job manager that the driver node is removed.
  auto iter = node_to_drivers_.find(node_id);
  if (iter != node_to_drivers_.end()) {
    for (auto &job_id : iter->second) {
      OnDriverNodeRemoved(job_id, node_id);
    }
    node_to_drivers_.erase(iter);
  }
}

ResourceSet GcsJobManager::GetNodegroupAvailableResources(
    const std::string &nodegroup_id, const float overcommit_ratio,
    const bool use_runtime_resources, const JobID &excluded_job_id,
    ResourceSet *job_resources) const {
  ResourceSet available_resources;
  ResourceSet sum_of_running_jobs_resources;

  auto iter = nodegroup_to_running_jobs_.find(nodegroup_id);
  if (iter != nodegroup_to_running_jobs_.end()) {
    for (auto &entry : iter->second) {
      if (entry.first == excluded_job_id) {
        if (!job_resources) {
          *job_resources = GetJobResources(entry.second, use_runtime_resources);
        }
        continue;
      }
      sum_of_running_jobs_resources.AddResources(
          GetJobResources(entry.second, use_runtime_resources));
    }
  }

  const auto &cluster_resources = gcs_resource_manager_->GetClusterResources();
  for (const auto &node_id : nodegroup_manager_->GetRegisteredNodes(nodegroup_id)) {
    auto iter = cluster_resources.find(node_id);
    if (iter != cluster_resources.end()) {
      available_resources.AddResources(iter->second->GetTotalResources());
    }
  }

  if (overcommit_ratio > 1.0) {
    for (const auto &resource_entry : available_resources.GetResourceAmountMap()) {
      if (resource_entry.first == kMemory_ResourceLabel) {
        available_resources.AddOrUpdateResource(
            kMemory_ResourceLabel,
            FractionalResourceQuantity(resource_entry.second.ToDouble() *
                                       overcommit_ratio));
      } else if (resource_entry.first == kCPU_ResourceLabel) {
        available_resources.AddOrUpdateResource(
            kCPU_ResourceLabel, FractionalResourceQuantity(
                                    resource_entry.second.ToDouble() * overcommit_ratio));
      }
    }
  }
  available_resources.SubtractResources(sum_of_running_jobs_resources);

  // If there is no resource overcommit, then job_memory_check_ratio has to be enforced.
  if (overcommit_ratio <= 1.0) {
    auto available_memory_resource = available_resources.GetMemory();
    if (!available_memory_resource.IsEmpty()) {
      auto quantity =
          available_memory_resource.GetResource(kMemory_ResourceLabel).ToDouble();
      quantity *= RayConfig::instance().job_memory_check_ratio();
      available_resources.AddOrUpdateResource(kMemory_ResourceLabel,
                                              FractionalResourceQuantity(quantity));
    }
  }
  RAY_LOG(DEBUG) << "GetNodegroupAvailableResources:"
                 << "\n -- nodegroup_id: " << nodegroup_id
                 << "\n -- using overcommit_ratio: " << overcommit_ratio
                 << "\n -- using runtime_resources: " << use_runtime_resources
                 << "\n -- available resources: " << available_resources.ToString();
  return available_resources;
}

bool GcsJobManager::HasEnoughAvailableResources(
    const JobID &job_id, const std::string &nodegroup_id,
    const ResourceSet &min_resource_requirements, const float overcommit_ratio,
    const bool use_runtime_resources, ResourceSet *available_resources) const {
  ResourceSet job_resources;
  *available_resources = GetNodegroupAvailableResources(
      nodegroup_id, overcommit_ratio, use_runtime_resources, job_id, &job_resources);

  // remove resources that isn't required
  for (auto &resource_pair : available_resources->GetResourceMap()) {
    if (!min_resource_requirements.Contains(resource_pair.first)) {
      available_resources->DeleteResource(resource_pair.first);
    }
  }

  if (use_runtime_resources) {
    RAY_LOG(INFO) << "HasEnoughAvailableResources:"
                  << "\n -- nodegroup_available_runtime_resources: "
                  << available_resources->ToString()
                  << "\n -- current_job_runtime_resources: " << job_resources.ToString()
                  << "\n -- job.min_resource_requirements: "
                  << min_resource_requirements.ToString();
  } else {
    RAY_LOG(INFO) << "HasEnoughAvailableResources:"
                  << "\n -- nodegroup_available_resources: "
                  << available_resources->ToString()
                  << "\n -- job_resources: " << job_resources.ToString()
                  << "\n -- job.min_resource_requirements: "
                  << min_resource_requirements.ToString();
  }

  if (min_resource_requirements.IsSubset(job_resources)) {
    // Scale down.
    return true;
  }

  return available_resources->IsSuperset(min_resource_requirements);
}

Status GcsJobManager::WarnClusterResourceNotEnough(
    const JobID &job_id, const std::string &nodegroup_id,
    const ResourceSet &min_resource_requirements,
    const ResourceSet &max_resource_requirements,
    const ResourceSet &available_resources) const {
  std::ostringstream ss;
  ss << "Failed to submit job " << job_id.Hex() << " as the nodegroup " << nodegroup_id
     << " lacks of enough resources."
     << "\nJobID: " << job_id << "\nNodegroup: " << nodegroup_id
     << "\nMin resource requirements: " << min_resource_requirements.ToString()
     << "\nMax resource requirements: " << max_resource_requirements.ToString()
     << "\nAvailable resources: " << available_resources.ToString();
  std::string message = ss.str();
  RAY_LOG(ERROR) << message;
  RAY_EVENT(ERROR, EVENT_LABEL_JOB_PIPELINE_SUBMITTED).WithField("job_id", job_id.Hex())
      << message;
  return Status::Invalid(message);
}

std::shared_ptr<absl::flat_hash_set<NodeID>> GcsJobManager::GetNodesByJobId(
    const JobID &job_id) const {
  auto related_nodes = std::make_shared<absl::flat_hash_set<NodeID>>();
  if (auto job_table_data = GetJob(job_id)) {
    *related_nodes =
        nodegroup_manager_->GetRegisteredNodes(job_table_data->nodegroup_id());
  }
  RAY_LOG(INFO) << "Finished getting nodes, nodes count = " << related_nodes->size();
  return related_nodes;
}

std::shared_ptr<rpc::GcsNodeInfo> GcsJobManager::SelectDriver(
    const rpc::JobTableData &job_data) {
  auto registered_nodes = nodegroup_manager_->GetRegisteredNodes(job_data.nodegroup_id());

  // Remove frozen nodes.
  if (is_node_frozen_fn_) {
    for (auto iter = registered_nodes.begin(); iter != registered_nodes.end();) {
      if (is_node_frozen_fn_(*iter)) {
        registered_nodes.erase(iter++);
        continue;
      }
      iter++;
    }
  }

  if (registered_nodes.empty()) {
    return nullptr;
  }

  static std::mt19937_64 gen_(
      std::chrono::high_resolution_clock::now().time_since_epoch().count());
  std::uniform_int_distribution<int> distribution(0, registered_nodes.size() - 1);

  int index = distribution(gen_);
  auto iter = registered_nodes.begin();
  while (index-- > 0) {
    ++iter;
  }

  auto node = gcs_node_manager_->GetAliveNode(*iter);
  return node.has_value() ? *node : nullptr;
}

bool GcsJobManager::HasAnyRunningJobs(const std::string &nodegroup_id) const {
  auto iter = nodegroup_to_running_jobs_.find(nodegroup_id);
  return iter != nodegroup_to_running_jobs_.end() && !iter->second.empty();
}

bool GcsJobManager::HasAnyDriversInNode(const NodeID &node_id) const {
  auto iter = node_to_drivers_.find(node_id);
  return iter != node_to_drivers_.end() && !iter->second.empty();
}

void GcsJobManager::AddJobToNodegroup(std::shared_ptr<JobTableData> job_table_data,
                                      const std::string &nodegroup_id) {
  if (!nodegroup_id.empty()) {
    auto job_id = JobID::FromBinary(job_table_data->job_id());
    nodegroup_to_running_jobs_[nodegroup_id].emplace(job_id, job_table_data);
    auto parent_nodegroup_id = NodegroupData::ParseParentNodegroupID(nodegroup_id);
    if (!parent_nodegroup_id.empty()) {
      nodegroup_to_running_jobs_[parent_nodegroup_id].emplace(job_id, job_table_data);
    }
  }
}

void GcsJobManager::RemoveJobFromNodegroup(const JobID &job_id,
                                           const std::string &nodegroup_id) {
  auto iter = nodegroup_to_running_jobs_.find(nodegroup_id);
  if (iter != nodegroup_to_running_jobs_.end()) {
    iter->second.erase(job_id);
    if (iter->second.empty()) {
      nodegroup_to_running_jobs_.erase(iter);
    }
  }

  auto parent_nodegroup_id = NodegroupData::ParseParentNodegroupID(nodegroup_id);
  if (!parent_nodegroup_id.empty()) {
    RemoveJobFromNodegroup(job_id, parent_nodegroup_id);
  }
}

ResourceSet GcsJobManager::GetJobResources(std::shared_ptr<JobTableData> job_table_data,
                                           bool use_runtime_resources) const {
  if (job_table_data == nullptr) {
    return ResourceSet();
  }
  if (use_runtime_resources) {
    auto resources = get_job_resources_(job_table_data, JobResourceType::LONGTERM);
    if (!resources.IsEmpty()) {
      return resources;
    }
  }

  auto resources =
      get_job_resources_(job_table_data, JobResourceType::RUNTIME_RESOURCE_REQUIREMENTS);
  if (!resources.IsEmpty()) {
    return resources;
  }

  return ExtractMinJobResourceRequirements(*job_table_data);
}

std::shared_ptr<rpc::JobTableData> GcsJobManager::GetRunningJobByName(
    const std::string &job_name) const {
  for (auto &nodegroup_to_running_job : nodegroup_to_running_jobs_) {
    for (auto &jobs : nodegroup_to_running_job.second) {
      if (jobs.second->job_name() == job_name) {
        return jobs.second;
      }
    }
  }
  return nullptr;
}

void GcsJobManager::AddDriverToNode(const NodeID &node_id, const JobID &job_id) {
  node_to_drivers_[node_id].emplace(job_id);
}

void GcsJobManager::RemoveDriverFromNode(const NodeID &node_id, const JobID &job_id) {
  auto iter = node_to_drivers_.find(node_id);
  if (iter != node_to_drivers_.end() && iter->second.erase(job_id)) {
    if (iter->second.empty()) {
      node_to_drivers_.erase(iter);
    }
  }
}

ResourceSet GcsJobManager::ExtractMinJobResourceRequirements(
    const JobTableData &job_table_data) const {
  std::unordered_map<std::string, double> resource_map;
  auto &job_config = job_table_data.config();
  if (job_config.total_memory_units() > 0) {
    resource_map.emplace(kMemory_ResourceLabel, job_config.total_memory_units());
  }
  if (job_config.total_cpus() > 0) {
    resource_map.emplace(kCPU_ResourceLabel, job_config.total_cpus());
  }
  if (job_config.total_gpus() > 0) {
    resource_map.emplace(kGPU_ResourceLabel, job_config.total_gpus());
  }
  return ResourceSet(resource_map);
}

Status GcsJobManager::VerifyJobResourceRequirementsRatio(
    const JobID &job_id, const std::string &nodegroup_id,
    const ResourceSet &min_resource_requirements,
    const ResourceSet &max_resource_requirements,
    std::shared_ptr<ScheduleOptions> schedule_options) const {
  bool ok = true;
  const auto &max_resource_amount_map = max_resource_requirements.GetResourceAmountMap();
  for (auto &entry : min_resource_requirements.GetResourceAmountMap()) {
    auto iter = max_resource_amount_map.find(entry.first);
    if (iter != max_resource_amount_map.end()) {
      if (iter->second.ToDouble() >
          entry.second.ToDouble() *
              schedule_options->job_resource_requirements_max_min_ratio_limit_) {
        ok = false;
        break;
      }
    }
  }
  if (!ok) {
    std::ostringstream ss;
    ss << "Failed to set job's resource requirements as the max/min of the job "
          "resource requirements must between [1, "
       << schedule_options->job_resource_requirements_max_min_ratio_limit_ << "]."
       << "\nJobID: " << job_id.Hex() << "\nNodegroup: " << nodegroup_id
       << "\nMin resource requirements: " << min_resource_requirements.ToString()
       << "\nMax resource requirements: " << max_resource_requirements.ToString()
       << "\nLimit of max/min: "
       << schedule_options->job_resource_requirements_max_min_ratio_limit_;
    return Status::Invalid(ss.str());
  }
  return Status::OK();
}

void GcsJobManager::AddDeadJobToCache(std::shared_ptr<rpc::JobTableData> job_table_data) {
  if (sorted_dead_job_list_.size() >=
      RayConfig::instance().maximum_gcs_dead_job_cached_count()) {
    EvictOneDeadJob();
  }
  auto job_id = JobID::FromBinary(job_table_data->job_id());
  // NOTE: The unit of job_table_data->timestamp() is seconds.
  sorted_dead_job_list_.emplace_back(
      job_id, static_cast<int64_t>(job_table_data->timestamp() * 1000));
}

void GcsJobManager::EvictOneDeadJob() {
  if (!sorted_dead_job_list_.empty()) {
    auto iter = sorted_dead_job_list_.begin();
    const auto &job_id = iter->first;
    RAY_CHECK_OK(gcs_table_storage_->JobTable().Delete(job_id, nullptr));
    RemoveJobFromCache(job_id);
    sorted_dead_job_list_.erase(iter);
  }
}

void GcsJobManager::EvictExpiredJobs() {
  RAY_LOG(INFO) << "Try evicting expired jobs, there are " << sorted_dead_job_list_.size()
                << " dead jobs in the cache.";
  int evicted_job_number = 0;

  std::vector<JobID> batch_ids;
  size_t batch_size = RayConfig::instance().gcs_dead_data_max_batch_delete_size();
  batch_ids.reserve(batch_size);

  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_job_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_job_data_keep_duration_ms();
  while (!sorted_dead_job_list_.empty()) {
    auto timestamp = sorted_dead_job_list_.begin()->second;
    if (timestamp + gcs_dead_job_data_keep_duration_ms > current_time_ms) {
      break;
    }

    auto iter = sorted_dead_job_list_.begin();
    const auto &job_id = iter->first;
    batch_ids.emplace_back(job_id);
    RemoveJobFromCache(job_id);
    sorted_dead_job_list_.erase(iter);
    ++evicted_job_number;

    if (batch_ids.size() == batch_size) {
      RAY_CHECK_OK(gcs_table_storage_->JobTable().BatchDelete(batch_ids, nullptr));
      batch_ids.clear();
    }
  }

  if (!batch_ids.empty()) {
    RAY_CHECK_OK(gcs_table_storage_->JobTable().BatchDelete(batch_ids, nullptr));
  }
  RAY_LOG(INFO) << evicted_job_number << " jobs are evicted, there are still "
                << sorted_dead_job_list_.size() << " dead jobs in the cache.";
}

void GcsJobManager::RemoveJobFromCache(const JobID &job_id) {
  jobs_.erase(job_id);
  job_data_.erase(job_id);
  ray_namespaces_.erase(job_id);
}

void GcsJobManager::HandleGetAvailableQuotaOfAllNodegroups(
    const rpc::GetAvailableQuotaOfAllNodegroupsRequest &request,
    rpc::GetAvailableQuotaOfAllNodegroupsReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  std::unordered_map<std::string, rpc::NodegroupQuota> nodegroup_to_quota_map;
  const auto &nodegroup_data_map = nodegroup_manager_->GetNodegroupDataMap();
  for (const auto &nodegroup_entry : nodegroup_data_map) {
    if (nodegroup_entry.first == NODEGROUP_RESOURCE_DEFAULT) {
      continue;
    }
    ResourceSet logical_available_resources, runtime_available_resources;
    ResourceSet *quota;
    auto schedule_options = nodegroup_entry.second->schedule_options_;
    if (schedule_options->runtime_resource_scheduling_enabled_) {
      logical_available_resources = GetNodegroupAvailableResources(
          nodegroup_entry.first, schedule_options->overcommit_ratio_, false);
      runtime_available_resources =
          GetNodegroupAvailableResources(nodegroup_entry.first, 1.0, true);
      quota = logical_available_resources.IsSubset(runtime_available_resources)
                  ? &logical_available_resources
                  : &runtime_available_resources;
    } else {
      logical_available_resources =
          GetNodegroupAvailableResources(nodegroup_entry.first, 1.0, false);
      quota = &logical_available_resources;
    }
    for (const auto &entry : quota->GetResourceAmountMap()) {
      // Only export standard, rare, or user-defined resources.
      if (kStandardResourceLabels.count(entry.first) > 0 || IsRareResource(entry.first) ||
          (entry.first.find("_group_") == std::string::npos &&
           entry.first != kObjectStoreMemory_ResourceLabel &&
           !(entry.first.size() > 12 && StartsWith(entry.first, "node:")))) {
        (*(nodegroup_to_quota_map[nodegroup_entry.first].mutable_quota()))[entry.first] =
            entry.second.ToDouble();
      }
    }
  }
  reply->mutable_nodegroup_to_quota_map()->insert(nodegroup_to_quota_map.begin(),
                                                  nodegroup_to_quota_map.end());
  GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
}

}  // namespace gcs
}  // namespace ray
