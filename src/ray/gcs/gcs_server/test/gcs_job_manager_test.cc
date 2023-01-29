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

#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_manager.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
namespace gcs {

class MockJobTable : public GcsJobTable {
 public:
  using GcsJobTable::GcsJobTable;
  Status Put(const JobID &key, const JobTableData &value,
             const StatusCallback &callback) override {
    callback(Status::OK());
    return Status::OK();
  }
};

class MockInMemoryGcsTableStorage : public InMemoryGcsTableStorage {
 public:
  MockInMemoryGcsTableStorage(instrumented_io_context &main_io_service)
      : InMemoryGcsTableStorage(main_io_service) {
    job_table_.reset(new MockJobTable(store_client_));
  }
};

class GcsJobManagerTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
    gcs_table_storage_ = std::make_shared<MockInMemoryGcsTableStorage>(io_service_);
    gcs_nodegroup_manager_ = std::make_shared<gcs::GcsNodegroupManager>(
        gcs_table_storage_, gcs_pub_sub_,
        /*has_any_running_jobs_in_nodegroup_fn=*/
        [this](const std::string &nodegroup_id) { return has_any_running_jobs_; });
    gcs_node_manager_ = std::make_shared<gcs::GcsNodeManager>(
        gcs_pub_sub_, gcs_table_storage_,
        /*is_node_in_nodegroup_callback=*/
        [](const NodeID &, const std::string &) { return true; });
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, gcs_pub_sub_, gcs_table_storage_, /*redis_broadcast_enabled=*/true);
    runtime_env_mgr_ = std::make_unique<ray::RuntimeEnvManager>(
        /*deleter=*/[](auto, auto f) { f(true); });
    gcs_job_manager_ = std::make_shared<gcs::GcsJobManager>(
        gcs_table_storage_, gcs_pub_sub_, *runtime_env_mgr_, gcs_nodegroup_manager_,
        gcs_node_manager_, gcs_resource_manager_, nullptr);
  }

  rpc::CreateOrUpdateNodegroupRequest GenCreateOrUpdateNodegroupRequest(
      const std::string &nodegroup_id, int node_count) {
    rpc::CreateOrUpdateNodegroupRequest request;
    request.set_nodegroup_id(nodegroup_id);
    rpc::NodeShapeAndCount node_shape_and_count;
    node_shape_and_count.set_node_count(node_count);
    request.add_node_shape_and_count_list()->CopyFrom(node_shape_and_count);
    return request;
  }

 protected:
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::InMemoryGcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsNodegroupManager> gcs_nodegroup_manager_;
  std::shared_ptr<gcs::GcsNodeManager> gcs_node_manager_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::unique_ptr<ray::RuntimeEnvManager> runtime_env_mgr_;
  std::shared_ptr<gcs::GcsJobManager> gcs_job_manager_;
  bool has_any_running_jobs_ = false;
};

TEST_F(GcsJobManagerTest, TestSubmitJob) {
  int node_count = 10;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  auto request = GenCreateOrUpdateNodegroupRequest("nodegroup_1", node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  rpc::SubmitJobRequest submit_job_req;
  submit_job_req.set_job_id(JobID::FromInt(1).Binary());
  submit_job_req.set_nodegroup_id("nodegroup_unknown");
  submit_job_req.set_language(Language::JAVA);
  ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).IsInvalid());

  submit_job_req.set_nodegroup_id("nodegroup_1");
  ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).ok());

  ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).IsInvalid());
}

TEST_F(GcsJobManagerTest, Initialize) {
  int node_count = 10;
  int job_count = 10;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  auto request = GenCreateOrUpdateNodegroupRequest("nodegroup_1", node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());
  for (int i = 0; i < job_count; ++i) {
    auto job_id = JobID::FromInt(i + 1);
    rpc::SubmitJobRequest submit_job_req;
    submit_job_req.set_job_id(job_id.Binary());
    submit_job_req.set_job_name("job_name" + std::to_string(i));
    submit_job_req.set_nodegroup_id("nodegroup_1");
    submit_job_req.set_language(Language::JAVA);
    ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).ok());
  }
  instrumented_io_context io_service;
  auto gcs_table_storage = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service);
  MockGcsInitData gcs_init_data(gcs_table_storage);
  for (auto &entry : gcs_node_manager_->GetAllAliveNodes()) {
    gcs_init_data.MutableNodes().emplace(entry.first, *entry.second);
  }
  for (auto &entry : gcs_job_manager_->jobs_) {
    gcs_init_data.MutableJobs().emplace(entry.first, *entry.second);
  }
  for (auto &entry : gcs_nodegroup_manager_->nodegroup_data_map_) {
    gcs_init_data.MutableNodegroups().emplace(NodegroupID::FromBinary(entry.first),
                                              *entry.second->ToProto());
  }

  auto gcs_pub_sub = std::make_shared<GcsServerMocker::MockGcsPubSub>(nullptr);
  auto gcs_nodegroup_manager = std::make_shared<gcs::GcsNodegroupManager>(
      gcs_table_storage, gcs_pub_sub,
      /*has_any_running_jobs_in_nodegroup_fn=*/
      [this](const std::string &nodegroup_id) { return has_any_running_jobs_; });
  auto gcs_node_manager = std::make_shared<gcs::GcsNodeManager>(
      gcs_pub_sub, gcs_table_storage,
      /*is_node_in_nodegroup_callback=*/
      [](const NodeID &, const std::string &) { return true; });
  auto gcs_resource_manager = std::make_shared<gcs::GcsResourceManager>(
      io_service, gcs_pub_sub, gcs_table_storage, /*redis_broadcast_enabled=*/true);
  auto gcs_job_manager = std::make_shared<gcs::GcsJobManager>(
      gcs_table_storage, gcs_pub_sub, *runtime_env_mgr_, gcs_nodegroup_manager,
      gcs_node_manager, gcs_resource_manager, nullptr);
  gcs_job_manager->Initialize(gcs_init_data);

  int recovery_job_count = 0;
  ASSERT_EQ(gcs_job_manager->node_to_drivers_.size(),
            gcs_job_manager->node_to_drivers_.size());
  for (auto &entry : gcs_job_manager->node_to_drivers_) {
    ASSERT_TRUE(gcs_job_manager->node_to_drivers_.contains(entry.first));
    recovery_job_count += gcs_job_manager->node_to_drivers_[entry.first].size();
  }
  ASSERT_EQ(recovery_job_count, job_count);
}

TEST_F(GcsJobManagerTest, JobNameConflict) {
  std::string job_name = "job_name";
  std::string nodegroup_id = "nodegroup_id";
  int node_count = 10;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  auto request = GenCreateOrUpdateNodegroupRequest(nodegroup_id, node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());

  auto job_id = JobID::FromInt(1);
  rpc::SubmitJobRequest submit_job_req;
  submit_job_req.set_job_id(job_id.Binary());
  submit_job_req.set_job_name(job_name);
  submit_job_req.set_nodegroup_id(nodegroup_id);
  submit_job_req.set_language(Language::JAVA);
  ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).ok());

  job_id = JobID::FromInt(2);
  submit_job_req.set_job_id(job_id.Binary());
  ASSERT_FALSE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).ok());
}

TEST_F(GcsJobManagerTest, NodeToDrivers) {
  int node_count = 10;
  int job_count = 10;
  for (int i = 0; i < node_count; ++i) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_manager_hostname("localhost." +
                                                                   std::to_string(i));
    gcs_node_manager_->AddNode(node);
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_nodegroup_manager_->AddNode(*node);
  }

  auto request = GenCreateOrUpdateNodegroupRequest("nodegroup_1", node_count);
  ASSERT_TRUE(gcs_nodegroup_manager_->CreateOrUpdateNodegroup(request, nullptr).ok());
  for (int i = 0; i < job_count; ++i) {
    auto job_id = JobID::FromInt(i + 1);
    rpc::SubmitJobRequest submit_job_req;
    submit_job_req.set_job_id(job_id.Binary());
    submit_job_req.set_job_name("job_name" + std::to_string(i));
    submit_job_req.set_nodegroup_id("nodegroup_1");
    submit_job_req.set_language(Language::JAVA);
    ASSERT_TRUE(gcs_job_manager_->SubmitJob(submit_job_req, nullptr).ok());
    auto job_data = gcs_job_manager_->GetJob(job_id);
    ASSERT_TRUE(job_data != nullptr);
    job_data->set_state(rpc::JobTableData_JobState_RUNNING);
  }

  auto is_driver_exist = [this](const JobID &job_id) {
    for (auto &entry : gcs_job_manager_->node_to_drivers_) {
      if (entry.second.contains(job_id)) {
        return true;
      }
    }
    return false;
  };
  for (int i = 0; i < job_count; ++i) {
    auto job_id = JobID::FromInt(i + 1);
    ASSERT_TRUE(is_driver_exist(job_id));
  }

  {
    auto job_id = JobID::FromInt(1);
    rpc::DropJobRequest request;
    request.set_job_id(job_id.Binary());
    gcs_job_manager_->DropJob(request, [](const Status &status) {});
    ASSERT_FALSE(is_driver_exist(job_id));
  }

  {
    auto job_id = JobID::FromInt(2);
    rpc::NotifyDriverExitRequest request;
    request.set_job_id(job_id.Binary());
    request.set_exit_with_error(true);

    rpc::NotifyDriverExitReply reply;
    gcs_job_manager_->HandleNotifyDriverExit(
        request, &reply,
        [](Status status, std::function<void()> success, std::function<void()> failure) {
        });
    ASSERT_FALSE(is_driver_exist(job_id));
  }

  {
    ASSERT_FALSE(gcs_job_manager_->node_to_drivers_.empty());
    auto iter = gcs_job_manager_->node_to_drivers_.begin();
    auto node_id = iter->first;
    auto job_ids = iter->second;
    gcs_job_manager_->OnNodeRemoved(node_id);
    for (const auto &job_id : job_ids) {
      ASSERT_FALSE(is_driver_exist(job_id));
    }
  }
}

TEST_F(GcsJobManagerTest, TestEvictDeadJobs) {
  RayConfig::instance().initialize(R"({"maximum_gcs_dead_job_cached_count": 300})");
  auto maximum_gcs_dead_job_cached_count =
      RayConfig::instance().maximum_gcs_dead_job_cached_count();
  ASSERT_EQ(maximum_gcs_dead_job_cached_count, 300);

  size_t job_count = 1000;
  auto current_time_ms = current_sys_time_ms();
  auto gcs_dead_job_data_keep_duration_ms =
      RayConfig::instance().gcs_dead_job_data_keep_duration_ms();
  for (size_t i = 0; i < job_count; ++i) {
    auto job_id = JobID::FromInt(i + 1);
    auto job = std::make_shared<rpc::JobTableData>();
    job->set_job_id(job_id.Binary());
    job->set_is_dead(true);
    int64_t timestamp_s =
        (current_time_ms - gcs_dead_job_data_keep_duration_ms - 10000 + i) / 1000;
    job->set_timestamp(timestamp_s);
    gcs_job_manager_->jobs_.emplace(job_id, job);
    gcs_job_manager_->AddDeadJobToCache(job);
  }
  ASSERT_EQ(gcs_job_manager_->jobs_.size(), 300);
  ASSERT_EQ(gcs_job_manager_->sorted_dead_job_list_.size(), 300);

  ASSERT_EQ(gcs_job_manager_->jobs_.size(), maximum_gcs_dead_job_cached_count);
  ASSERT_EQ(gcs_job_manager_->sorted_dead_job_list_.size(),
            maximum_gcs_dead_job_cached_count);

  gcs_job_manager_->EvictExpiredJobs();
  ASSERT_EQ(gcs_job_manager_->jobs_.size(), 0);
  ASSERT_EQ(gcs_job_manager_->sorted_dead_job_list_.size(), 0);
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}