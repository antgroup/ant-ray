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

#include <list>
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

/// This implementation class of `WorkerInfoHandler`.
class GcsWorkerManager : public rpc::WorkerInfoHandler {
 public:
  explicit GcsWorkerManager(
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> &gcs_pub_sub,
      std::function<std::shared_ptr<rpc::JobTableData>(const JobID &)> get_job_info_func)
      : gcs_table_storage_(gcs_table_storage),
        gcs_pub_sub_(gcs_pub_sub),
        get_job_info_func_(get_job_info_func) {}

  void HandleReportWorkerFailure(const rpc::ReportWorkerFailureRequest &request,
                                 rpc::ReportWorkerFailureReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetWorkerInfo(const rpc::GetWorkerInfoRequest &request,
                           rpc::GetWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllWorkerInfo(const rpc::GetAllWorkerInfoRequest &request,
                              rpc::GetAllWorkerInfoReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleAddWorkerInfo(const rpc::AddWorkerInfoRequest &request,
                           rpc::AddWorkerInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void AddWorkerDeadListener(
      std::function<void(std::shared_ptr<WorkerTableData>)> listener);

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  void AddDeadWorkerToCache(const std::shared_ptr<WorkerTableData> &worker_data);

  void EvictOneDeadWorker();

  void EvictExpiredWorkers();

 private:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  std::function<std::shared_ptr<rpc::JobTableData>(const JobID &)> get_job_info_func_;
  std::vector<std::function<void(std::shared_ptr<WorkerTableData>)>>
      worker_dead_listeners_;

  /// All dead workers.
  absl::flat_hash_map<WorkerID, std::shared_ptr<WorkerTableData>> dead_workers_;
  /// The dead workers are sorted according to the timestamp, and the oldest is at the
  /// head of the list.
  std::list<std::pair<WorkerID, int64_t>> sorted_dead_worker_list_;
};

}  // namespace gcs
}  // namespace ray
