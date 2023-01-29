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
#include <memory>
#include <thread>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/asio/periodical_runner.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"
#include "ray/util/thread_pool.h"
#include "ray/util/util.h"

namespace ray {
namespace gcs {

struct WindowContext {
  std::vector<ResourceSet> resources_window;
  std::shared_ptr<ScheduleOptions> schedule_options;
  int64_t next_calc_time_ms = 0;
};

class GcsTableStorage;
class GcsRuntimeResourceManager;
class GcsJobDistribution;
class GcsRuntimeResourceManager : public rpc::RuntimeResourceHandler {
 public:
  explicit GcsRuntimeResourceManager(
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::shared_ptr<GcsJobDistribution> gcs_job_distribution,
      std::function<void(std::shared_ptr<absl::flat_hash_map<
                             NodeID, absl::flat_hash_map<int, ResourceSet>>>)>
          shortterm_runtime_resources_updated_callback,
      std::function<void(const JobID, const ResourceSet)>
          job_longterm_runtime_resources_updated_callback);

  virtual ~GcsRuntimeResourceManager();

  void Stop();

  void RecordJobShorttermRuntimeResources(
      absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
          job_shortterm_runtime_resources);

  void CalcJobLongtermRuntimeResources();

  void Initialize(const GcsInitData &gcs_init_data);

 protected:
  void HandleReportClusterRuntimeResources(
      const rpc::ReportClusterRuntimeResourcesRequest &request,
      rpc::ReportClusterRuntimeResourcesReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  /// Record the inputs of runtime resources to each node's window.
  void RecordRuntimeResources(
      std::shared_ptr<absl::flat_hash_map<NodeID, rpc::NodeRuntimeResources>>
          node_runtime_resources_map);

  /// Find the tail of a vector of elements.
  /// \param records The vector of elements.
  /// \param tail The tail of interest.
  /// \param upper Whether to find the upper tail or lower tail.
  double FindTail(std::vector<double> &records, float tail, bool upper);

  absl::flat_hash_map<JobID, WindowContext> TakeJobResourcesWindow();

  void PutShorttermRuntimeResourcesToJobResourceWindow(
      absl::flat_hash_map<JobID, std::pair<ResourceSet, std::shared_ptr<ScheduleOptions>>>
          job_shortterm_runtime_resources_map);

 private:
  instrumented_io_context calc_service_;
  std::unique_ptr<std::thread> calc_thread_;

  instrumented_io_context record_service_;
  std::unique_ptr<std::thread> record_thread_;

  std::unique_ptr<PeriodicalRunner> periodical_runner_;

  /// The gcs table storage.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// The gcs resource manager.
  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;
  /// The gcs job distribution.
  std::shared_ptr<GcsJobDistribution> gcs_job_distribution_;

  // This callback will be executed by another thread.
  std::function<void(std::shared_ptr<
                     absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>)>
      shortterm_runtime_resources_updated_callback_;
  // This callback will be executed by another thread.
  std::function<void(const JobID, const ResourceSet)>
      job_longterm_runtime_resources_updated_callback_;

  /// The map for each job's history window (of short-term runtime resources).
  absl::flat_hash_map<JobID, WindowContext> job_window_map_
      GUARDED_BY(job_window_map_mutex_);
  mutable absl::Mutex job_window_map_mutex_;
};

}  // namespace gcs
}  // namespace ray
