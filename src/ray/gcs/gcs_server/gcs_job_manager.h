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

#include "gtest/gtest.h"
#include "ray/common/runtime_env_manager.h"
#include "ray/common/task/scheduling_resources.h"
#include "ray/gcs/gcs_server/gcs_init_data.h"
#include "ray/gcs/gcs_server/gcs_object_manager.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/pubsub/gcs_pub_sub.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace gcs {

class GcsNodegroupManagerInterface;
class GcsResourceManager;
class GcsNodeManager;
class GcsFrozenNodeManager;
struct ScheduleOptions;

/// The types of job-wise resource statistics.
enum JobResourceType {
  /// The logical resources acquired by the job's PG and actors.
  ACQUIRED = 0,
  /// The short-term runtime resources.
  SHORTTERM = 1,
  /// The long-term runtime resources.
  LONGTERM = 2,
  /// The runtime resource requirements.
  RUNTIME_RESOURCE_REQUIREMENTS = 3,
};

/// This implementation class of `JobInfoHandler`.
class GcsJobManager : public rpc::JobInfoHandler {
 public:
  explicit GcsJobManager(
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub, RuntimeEnvManager &runtime_env_manager,
      std::shared_ptr<GcsNodegroupManagerInterface> nodegroup_manager,
      std::shared_ptr<GcsNodeManager> gcs_node_manager,
      std::shared_ptr<GcsResourceManager> gcs_resource_manager,
      std::function<bool(const NodeID &node_id)> is_node_frozen_fn,
      std::function<ResourceSet(std::shared_ptr<JobTableData>, JobResourceType)>
          get_job_resources = nullptr)
      : gcs_table_storage_(std::move(gcs_table_storage)),
        gcs_pub_sub_(std::move(gcs_pub_sub)),
        nodegroup_manager_(std::move(nodegroup_manager)),
        gcs_node_manager_(std::move(gcs_node_manager)),
        gcs_resource_manager_(std::move(gcs_resource_manager)),
        is_node_frozen_fn_(std::move(is_node_frozen_fn)),
        get_job_resources_(std::move(get_job_resources)),
        runtime_env_manager_(runtime_env_manager) {}

  /// Initialize with the gcs tables data synchronously.
  /// This should be called when GCS server restarts after a failure.
  ///
  /// \param gcs_init_data.
  void Initialize(const GcsInitData &gcs_init_data);

  void HandleAddJob(const rpc::AddJobRequest &request, rpc::AddJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetJob(const rpc::GetJobRequest &request, rpc::GetJobReply *reply,
                    rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAllJobInfo(const rpc::GetAllJobInfoRequest &request,
                           rpc::GetAllJobInfoReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleReportJobError(const rpc::ReportJobErrorRequest &request,
                            rpc::ReportJobErrorReply *reply,
                            rpc::SendReplyCallback send_reply_callback) override;

  /// ======== ANT-INTERNAL below =========
  void HandleNotifyDriverExit(const rpc::NotifyDriverExitRequest &request,
                              rpc::NotifyDriverExitReply *reply,
                              rpc::SendReplyCallback send_reply_callback) override;

  void HandleMarkJobFailed(const rpc::MarkJobFailedRequest &request,
                           rpc::MarkJobFailedReply *reply,
                           rpc::SendReplyCallback send_reply_callback) override;

  void HandleUpdateJobResourceRequirements(
      const rpc::UpdateJobResourceRequirementsRequest &request,
      rpc::UpdateJobResourceRequirementsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  void HandlePutJobData(const rpc::PutJobDataRequest &request,
                        rpc::PutJobDataReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetJobData(const rpc::GetJobDataRequest &request,
                        rpc::GetJobDataReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetNextJobID(const rpc::GetNextJobIDRequest &request,
                          rpc::GetNextJobIDReply *reply,
                          rpc::SendReplyCallback send_reply_callback) override;

  void HandleSubmitJob(const rpc::SubmitJobRequest &request, rpc::SubmitJobReply *reply,
                       rpc::SendReplyCallback send_reply_callback) override;

  void HandleDropJob(const rpc::DropJobRequest &request, rpc::DropJobReply *reply,
                     rpc::SendReplyCallback send_reply_callback) override;

  void HandleGetAvailableQuotaOfAllNodegroups(
      const rpc::GetAvailableQuotaOfAllNodegroupsRequest &request,
      rpc::GetAvailableQuotaOfAllNodegroupsReply *reply,
      rpc::SendReplyCallback send_reply_callback) override;

  Status SubmitJob(const rpc::SubmitJobRequest &request,
                   const ray::gcs::StatusCallback &callback);

  virtual Status DoSubmitJob(const rpc::SubmitJobRequest &request,
                             const ray::gcs::StatusCallback &callback);

  Status DropJob(const rpc::DropJobRequest &request,
                 const ray::gcs::StatusCallback &callback);

  std::shared_ptr<rpc::JobTableData> GetJob(const JobID &job_id) const;

  std::string GetJobNodegroupId(const JobID &job_id) const;

  std::string GetJobName(const JobID &job_id) const;

  void OnDriverNodeRemoved(const JobID &job_id, const NodeID &node_id);

  void OnNodeRemoved(const NodeID &node_id);

  void AddJobFinishedListener(
      std::function<void(std::shared_ptr<JobID>)> listener) override;

  std::string GetRayNamespace(const JobID &job_id) const;
  void SetJobResourceRequirementsChangedCallback(
      std::function<Status(const JobTableData &)> callback);

  void FlushJobTableDataToStorage(const JobID &job_id);

  ResourceSet ExtractMinJobResourceRequirements(const JobTableData &job_table_data) const;
  ResourceSet ExtractMinJobResourceRequirements(
      const ::google::protobuf::Map<std::string, double> &min_resource_requirements);

  ResourceSet ExtractMaxJobResourceRequirements(
      const ::google::protobuf::Map<std::string, double> &max_resource_requirements);

  /// Get the available resources of a specified nodegroup.
  ///
  /// \param[in] nodegroup_id The nodegroup's ID.
  /// \param[in] overcommit_ratio The ratio for overcommitting memory and CPU.
  /// \param[in] use_runtime_resources Whether checking the available runtime resources.
  /// \param[in] excluded_job_id The job to be excluded when calculating the nodegroup's
  /// available resources.
  /// \param[out] job_resources The resources of the excluded job.
  /// \return available_resources The nodegroup's available resources.
  ResourceSet GetNodegroupAvailableResources(const std::string &nodegroup_id,
                                             const float overcommit_ratio,
                                             const bool use_runtime_resources,
                                             const JobID &excluding_job_id = JobID::Nil(),
                                             ResourceSet *job_resources = nullptr) const;

  /// Check if the nodegroup has enough available resources for the job's
  /// requirement.
  ///
  /// \param[in] job_id The job's ID.
  /// \param[in] nodegroup_id The nodegroup's ID.
  /// \param[in] min_resource_requirements Minimum resources that the job required.
  /// \param[in] overcommit_ratio The ratio for overcommitting memory and CPU.
  /// \param[in] use_runtime_resources Whether checking the available runtime resources.
  /// \param[out] available_resources The nodegroup's available resources.
  bool HasEnoughAvailableResources(const JobID &job_id, const std::string &nodegroup_id,
                                   const ResourceSet &min_resource_requirements,
                                   const float overcommit_ratio,
                                   const bool use_runtime_resources,
                                   ResourceSet *available_resources) const;

  Status WarnClusterResourceNotEnough(const JobID &job_id,
                                      const std::string &nodegroup_id,
                                      const ResourceSet &min_resource_requirements,
                                      const ResourceSet &max_resource_requirements,
                                      const ResourceSet &available_resources) const;

  Status VerifyJobResourceRequirementsRatio(
      const JobID &job_id, const std::string &nodegroup_id,
      const ResourceSet &min_resource_requirements,
      const ResourceSet &max_resource_requirements,
      std::shared_ptr<ScheduleOptions> schedule_options) const;

  std::shared_ptr<absl::flat_hash_set<NodeID>> GetNodesByJobId(const JobID &job_id) const;

  std::shared_ptr<rpc::GcsNodeInfo> SelectDriver(const rpc::JobTableData &job_data);

  bool HasAnyRunningJobs(const std::string &nodegroup_id) const;

  bool HasAnyDriversInNode(const NodeID &node_id) const;

  /// Evict all dead nodes which ttl is expired.
  void EvictExpiredJobs();

 private:
  void UpdateJobStateToDead(std::shared_ptr<JobTableData> job_table_data,
                            const ray::gcs::StatusCallback &callback);

  void AddJobToNodegroup(std::shared_ptr<JobTableData> job_table_data,
                         const std::string &nodegroup_id);
  void RemoveJobFromNodegroup(const JobID &job_id, const std::string &nodegroup_id);

  ResourceSet GetJobResources(std::shared_ptr<JobTableData> job_table_data,
                              bool use_runtime_resources = false) const;

  std::shared_ptr<rpc::JobTableData> GetRunningJobByName(
      const std::string &job_name) const;

  /// Add driver to the specified node.
  ///
  /// \param node_id ID of node that the driver launched in.
  /// \param job_id ID of job that the driver associated with.
  void AddDriverToNode(const NodeID &node_id, const JobID &job_id);

  /// Remove driver from the specified node.
  ///
  /// \param node_id ID of node that the driver launched in.
  /// \param job_id ID of job that the driver associated with.
  void RemoveDriverFromNode(const NodeID &node_id, const JobID &job_id);

  /// Add the dead job to the cache. If the cache is full, the earliest dead job is
  /// evicted.
  ///
  /// \param job_table_data The info of dead job.
  void AddDeadJobToCache(std::shared_ptr<rpc::JobTableData> job_table_data);

  /// Evict one dead job from sorted_dead_job_list_ as well as jobs_.
  void EvictOneDeadJob();

  /// Erase entry from jobs_ / job_data_ / ray_namespaces_ by job_id.
  void RemoveJobFromCache(const JobID &job_id);

 protected:
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;

  absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>> jobs_;
  absl::flat_hash_map<JobID, absl::flat_hash_map<std::string, std::string>> job_data_;
  std::list<std::pair<JobID, int64_t>> sorted_dead_job_list_;

  /// Listeners which monitors the finish of jobs.
  std::vector<std::function<void(std::shared_ptr<JobID>)>> job_finished_listeners_;

  /// Listeners which monitor the change of job resource requirements.
  std::function<Status(const JobTableData &)> job_resource_requirements_changed_callback_;

  /// Map from driver node ID to the IDs of jobs associated with the driver node.
  absl::flat_hash_map<NodeID, absl::flat_hash_set<JobID>> node_to_drivers_;

  std::shared_ptr<GcsNodegroupManagerInterface> nodegroup_manager_;

  std::shared_ptr<GcsNodeManager> gcs_node_manager_;

  std::shared_ptr<GcsResourceManager> gcs_resource_manager_;

  std::function<bool(const NodeID &node_id)> is_node_frozen_fn_;

  std::function<ResourceSet(std::shared_ptr<JobTableData>, JobResourceType)>
      get_job_resources_;

  absl::flat_hash_map<std::string,
                      absl::flat_hash_map<JobID, std::shared_ptr<JobTableData>>>
      nodegroup_to_running_jobs_;

  /// A cached mapping from job id to namespace.
  std::unordered_map<JobID, std::string> ray_namespaces_;

  ray::RuntimeEnvManager &runtime_env_manager_;

  void ClearJobInfos(const JobID &job_id);

  FRIEND_TEST(GcsJobManagerTest, Initialize);
  FRIEND_TEST(GcsJobManagerTest, NodeToDrivers);
  FRIEND_TEST(GcsJobManagerTest, TestEvictDeadJobs);
};

}  // namespace gcs
}  // namespace ray
