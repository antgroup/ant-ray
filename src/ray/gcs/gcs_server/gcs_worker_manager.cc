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

#include "ray/gcs/gcs_server/gcs_worker_manager.h"
#include "ray/stats/stats.h"

namespace ray {
namespace gcs {

void GcsWorkerManager::HandleReportWorkerFailure(
    const rpc::ReportWorkerFailureRequest &request, rpc::ReportWorkerFailureReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  const rpc::Address worker_address = request.worker_failure().worker_address();
  const auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
  const auto node_id = NodeID::FromBinary(worker_address.raylet_id());
  auto worker_failure_data = std::make_shared<WorkerTableData>();
  worker_failure_data->CopyFrom(request.worker_failure());
  worker_failure_data->set_is_alive(false);

  std::stringstream log_stream;
  log_stream << "Reporting worker failure, worker id = " << worker_id
             << ", node id = " << node_id << ", address = " << worker_address.ip_address()
             << ", exit_type = "
             << rpc::WorkerExitType_Name(request.worker_failure().exit_type())
             << ", has creation task exception = "
             << request.worker_failure().has_creation_task_exception()
             << ", worker_failure_data_byte_size = "
             << worker_failure_data->ByteSizeLong();
  if (request.worker_failure().exit_type() == rpc::WorkerExitType::INTENDED_EXIT) {
    RAY_LOG(INFO) << log_stream.str();
  } else {
    RAY_LOG(WARNING) << log_stream.str()
                     << ". Unintentional worker failures have been reported. If there "
                        "are lots of this logs, that might indicate there are "
                        "unexpected failures in the cluster.";
  }

  for (auto &listener : worker_dead_listeners_) {
    listener(worker_failure_data);
  }

  AddDeadWorkerToCache(worker_failure_data);

  auto on_done = [this, worker_address, worker_id, node_id, worker_failure_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report worker failure, worker id = " << worker_id
                     << ", node id = " << node_id
                     << ", address = " << worker_address.ip_address();
    } else {
      stats::UnintentionalWorkerFailures.Record(1);
      auto job_info =
          get_job_info_func_(JobID::FromBinary(worker_failure_data->job_id()));
      if (!(job_info && job_info->config().enable_l1_fault_tolerance())) {
        // Only publish worker_id and raylet_id in address as they are the only fields
        // used by sub clients.
        auto worker_failure_delta = std::make_shared<rpc::WorkerDeltaData>();
        worker_failure_delta->set_worker_id(
            worker_failure_data->worker_address().worker_id());
        worker_failure_delta->set_raylet_id(
            worker_failure_data->worker_address().raylet_id());
        RAY_CHECK_OK(gcs_pub_sub_->Publish(WORKER_CHANNEL, worker_id.Hex(),
                                           worker_failure_delta->SerializeAsString(),
                                           nullptr));
      }
    }
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  // As soon as the worker starts, it will register with GCS. It ensures that GCS receives
  // the worker registration information first and then the worker failure message, so we
  // delete the get operation. Related issues:
  // https://github.com/ray-project/ray/pull/11599
  Status status =
      gcs_table_storage_->WorkerTable().Put(worker_id, *worker_failure_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsWorkerManager::HandleGetWorkerInfo(const rpc::GetWorkerInfoRequest &request,
                                           rpc::GetWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  WorkerID worker_id = WorkerID::FromBinary(request.worker_id());
  RAY_LOG(DEBUG) << "Getting worker info, worker id = " << worker_id;

  auto on_done = [worker_id, reply, send_reply_callback](
                     const Status &status,
                     const boost::optional<WorkerTableData> &result) {
    if (result) {
      reply->mutable_worker_table_data()->CopyFrom(*result);
    }
    RAY_LOG(DEBUG) << "Finished getting worker info, worker id = " << worker_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };

  Status status = gcs_table_storage_->WorkerTable().Get(worker_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void GcsWorkerManager::HandleGetAllWorkerInfo(
    const rpc::GetAllWorkerInfoRequest &request, rpc::GetAllWorkerInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all worker info.";
  auto on_done = [reply, send_reply_callback](
                     const std::unordered_map<WorkerID, WorkerTableData> &result) {
    for (auto &data : result) {
      reply->add_worker_table_data()->CopyFrom(data.second);
    }
    RAY_LOG(DEBUG) << "Finished getting all worker info.";
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, Status::OK());
  };
  Status status = gcs_table_storage_->WorkerTable().GetAll(on_done);
  if (!status.ok()) {
    on_done(std::unordered_map<WorkerID, WorkerTableData>());
  }
}

void GcsWorkerManager::HandleAddWorkerInfo(const rpc::AddWorkerInfoRequest &request,
                                           rpc::AddWorkerInfoReply *reply,
                                           rpc::SendReplyCallback send_reply_callback) {
  auto worker_data = std::make_shared<WorkerTableData>();
  worker_data->CopyFrom(request.worker_data());
  auto worker_id = WorkerID::FromBinary(worker_data->worker_address().worker_id());
  RAY_LOG(DEBUG) << "Adding worker " << worker_id;

  auto on_done = [worker_id, worker_data, reply,
                  send_reply_callback](const Status &status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add worker information, "
                     << worker_data->DebugString();
    }
    RAY_LOG(DEBUG) << "Finished adding worker " << worker_id;
    GCS_RPC_SEND_REPLY(send_reply_callback, reply, status);
  };

  Status status = gcs_table_storage_->WorkerTable().Put(worker_id, *worker_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void GcsWorkerManager::AddWorkerDeadListener(
    std::function<void(std::shared_ptr<WorkerTableData>)> listener) {
  RAY_CHECK(listener != nullptr);
  worker_dead_listeners_.emplace_back(std::move(listener));
}

void GcsWorkerManager::Initialize(const GcsInitData &gcs_init_data) {
  for (auto &entry : gcs_init_data.Workers()) {
    if (!entry.second.is_alive()) {
      dead_workers_.emplace(entry.first,
                            std::make_shared<rpc::WorkerTableData>(entry.second));
      sorted_dead_worker_list_.emplace_back(entry.first, entry.second.timestamp());
    }
  }
  sorted_dead_worker_list_.sort([](const std::pair<WorkerID, int64_t> &left,
                                   const std::pair<WorkerID, int64_t> &right) {
    return left.second < right.second;
  });
}

void GcsWorkerManager::AddDeadWorkerToCache(
    const std::shared_ptr<WorkerTableData> &worker_data) {
  if (dead_workers_.size() >=
      RayConfig::instance().maximum_gcs_dead_worker_cached_count()) {
    EvictOneDeadWorker();
  }
  auto worker_id = WorkerID::FromBinary(worker_data->worker_address().worker_id());
  dead_workers_.emplace(worker_id, worker_data);
  sorted_dead_worker_list_.emplace_back(worker_id, worker_data->timestamp());
}

void GcsWorkerManager::EvictOneDeadWorker() {
  if (!sorted_dead_worker_list_.empty()) {
    auto iter = sorted_dead_worker_list_.begin();
    const auto &worker_id = iter->first;
    RAY_CHECK_OK(gcs_table_storage_->WorkerTable().Delete(worker_id, nullptr));
    dead_workers_.erase(worker_id);
    sorted_dead_worker_list_.erase(iter);
  }
}

void GcsWorkerManager::EvictExpiredWorkers() {
  RAY_LOG(INFO) << "Try evicting expired workers, there are "
                << sorted_dead_worker_list_.size() << " dead workers in the cache.";
  int evicted_worker_number = 0;

  std::vector<WorkerID> batch_ids;
  size_t batch_size = RayConfig::instance().gcs_dead_data_max_batch_delete_size();
  batch_ids.reserve(batch_size);
  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_worker_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_worker_data_keep_duration_ms();
  while (!sorted_dead_worker_list_.empty()) {
    auto timestamp = sorted_dead_worker_list_.begin()->second;
    if (timestamp + gcs_dead_worker_data_keep_duration_ms > current_time_ms) {
      break;
    }

    auto iter = sorted_dead_worker_list_.begin();
    const auto &worker_id = iter->first;
    batch_ids.emplace_back(worker_id);
    dead_workers_.erase(worker_id);
    sorted_dead_worker_list_.erase(iter);
    ++evicted_worker_number;

    if (batch_ids.size() == batch_size) {
      RAY_CHECK_OK(gcs_table_storage_->WorkerTable().BatchDelete(batch_ids, nullptr));
      batch_ids.clear();
    }
  }

  if (!batch_ids.empty()) {
    RAY_CHECK_OK(gcs_table_storage_->WorkerTable().BatchDelete(batch_ids, nullptr));
  }

  RAY_LOG(INFO) << evicted_worker_number << " workers are evicted, there are still "
                << sorted_dead_worker_list_.size() << " dead workers in the cache.";
}

}  // namespace gcs
}  // namespace ray
