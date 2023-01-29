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

#include <memory>
#include <thread>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/gcs/pb_util.h"
#include "ray/util/logging.h"
#include "ray/util/process.h"

namespace ray {
namespace raylet {

class MockJobClient : public rpc::JobClientInterface {
 public:
  void InitializeJobEnv(const rpc::InitializeJobEnvRequest &request,
                        const rpc::ClientCallback<rpc::InitializeJobEnvReply> &callback) {
    callbacks.push_back(callback);
  }
  void CleanJobEnv(const rpc::CleanJobEnvRequest &request,
                   const rpc::ClientCallback<rpc::CleanJobEnvReply> &callback) {
    callback(Status::OK(), rpc::CleanJobEnvReply());
  }

  bool ReplyInitializeJobEnv(
      Status status = Status::OK(),
      rpc::AgentRpcStatus agent_status = rpc::AGENT_RPC_STATUS_OK) {
    if (callbacks.size() == 0) {
      return false;
    }
    auto callback = callbacks.front();
    auto reply = rpc::InitializeJobEnvReply();
    reply.set_status(agent_status);
    callback(status, reply);
    callbacks.pop_front();
    return true;
  }

  std::list<rpc::ClientCallback<rpc::InitializeJobEnvReply>> callbacks;
};

class MockAgentManager : public raylet::AgentManager {
 public:
  using raylet::AgentManager::AgentManager;

 protected:
  virtual void StartAgent() override {}
};

class MockJobInfoAccessor : public gcs::ServiceBasedJobInfoAccessor {
 public:
  MockJobInfoAccessor(gcs::ServiceBasedGcsClient *client)
      : gcs::ServiceBasedJobInfoAccessor(client) {}
  ~MockJobInfoAccessor() {}

  Status AsyncMarkFailed(const JobID &job_id, const std::string &error_message,
                         const std::string &driver_cmdline,
                         const gcs::StatusCallback &callback) override {
    return Status::OK();
  }
};

class MockErrorInfoAccessor : public gcs::ServiceBasedErrorInfoAccessor {
 public:
  MockErrorInfoAccessor(gcs::ServiceBasedGcsClient *client)
      : gcs::ServiceBasedErrorInfoAccessor(client) {}
  ~MockErrorInfoAccessor() {}

  Status AsyncReportJobError(const std::shared_ptr<rpc::ErrorTableData> &data_ptr,
                             const gcs::StatusCallback &callback) override {
    return Status::OK();
  }
};

class MockGcs : public gcs::ServiceBasedGcsClient {
 public:
  MockGcs() : gcs::ServiceBasedGcsClient(gcs::GcsClientOptions("", 0, "")){};

  void Init(gcs::JobInfoAccessor *job_accessor, gcs::ErrorInfoAccessor *error_accessor) {
    job_accessor_.reset(job_accessor);
    error_accessor_.reset(error_accessor);
  }
};

class AgentManagerTest : public ::testing::Test {
 public:
  AgentManagerTest()
      : job_client_(new MockJobClient()),
        mock_gcs_(new MockGcs()),
        job_accessor_(new MockJobInfoAccessor(mock_gcs_.get())),
        error_accessor_(new MockErrorInfoAccessor(mock_gcs_.get())) {
    auto options = MockAgentManager::Options({NodeID::FromRandom(), {"fake cmdline"}});
    agent_manager_.reset(new MockAgentManager(
        std::move(options), mock_gcs_,
        /*job_client_factory=*/
        [this](const std::string &ip_address, int port) { return job_client_; },
        /*event_client_factory=*/
        [](const std::string &ip_address, int port) { return nullptr; },
        /*reporter_client_factory=*/
        [](const std::string &ip_address, int port) { return nullptr; },
        /*delay_executor=*/
        [this](std::function<void()> task, uint32_t delay_ms) {
          delay_executor_.push_back(task);
          return nullptr;
        },
        /*on_job_env_initialized=*/
        [this](const JobID &job_id, const Status &status) {
          if (status.ok()) {
            job_env_initialized_.push_back(job_id);
          }
        },
        nullptr, std::unique_ptr<PeriodicalRunner>(new PeriodicalRunner(io_service_))));
    mock_gcs_->Init(job_accessor_, error_accessor_);
  }

  bool DelayExecute() {
    if (delay_executor_.size() == 0) {
      return false;
    }
    auto task = delay_executor_.front();
    task();
    delay_executor_.pop_front();
    return true;
  }

  void TestEnableInitializeJobEnv(bool pre_initialize_job_runtime_env_enabled);
  // Test the "eager" initialize that we initialize Env when the job is submitted
  // Test the "lazy" initialize that we initialize Env when the task is submitted

 protected:
  std::shared_ptr<MockJobClient> job_client_;
  std::shared_ptr<MockAgentManager> agent_manager_;
  std::list<JobID> job_env_initialized_;
  std::list<std::function<void()>> delay_executor_;
  std::shared_ptr<MockGcs> mock_gcs_;
  MockJobInfoAccessor *job_accessor_;
  MockErrorInfoAccessor *error_accessor_;
  instrumented_io_context io_service_;
};

void AgentManagerTest::TestEnableInitializeJobEnv(
    bool pre_initialize_job_runtime_env_enabled) {
  RayConfig::instance().initialize(R"({"agent_warn_job_initialized_interval_ms": 0})");
  rpc::JobEnvInitNotification job_env_init_notification;
  JobID job_id = JobID::FromInt(1);
  job_env_init_notification.mutable_job_table_data()->set_job_id(job_id.Binary());
  job_env_init_notification.mutable_job_table_data()
      ->set_pre_initialize_job_runtime_env_enabled(
          pre_initialize_job_runtime_env_enabled);
  rpc::RegisterAgentReply register_agent_reply;
  rpc::SendReplyCallback callback = [](Status status, std::function<void()> success,
                                       std::function<void()> failure) {};
  ASSERT_TRUE(agent_manager_->job_client_ == nullptr);

  agent_manager_->HandleRegisterAgent(rpc::RegisterAgentRequest(), &register_agent_reply,
                                      callback);

  ASSERT_TRUE(agent_manager_->job_client_ != nullptr);
  ASSERT_TRUE(agent_manager_->initializing_jobs_.empty());
  ASSERT_TRUE(job_env_initialized_.empty());

  agent_manager_->OnJobEnvInitNotification(job_env_init_notification);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(), 1);
  ASSERT_TRUE(job_env_initialized_.empty());

  // Reply OK and a good status, check that the job env is initialized.
  job_client_->ReplyInitializeJobEnv(ray::Status::OK(), rpc::AGENT_RPC_STATUS_OK);
  ASSERT_TRUE(agent_manager_->initializing_jobs_.empty());
  ASSERT_EQ(job_env_initialized_.size(), 1);

  job_env_initialized_.clear();
  agent_manager_->OnJobEnvInitNotification(job_env_init_notification);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(), 1);
  ASSERT_TRUE(job_env_initialized_.empty());

  // Reply Failed.
  job_client_->ReplyInitializeJobEnv(Status::Invalid("fail"), rpc::AGENT_RPC_STATUS_OK);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(), 1);
  ASSERT_TRUE(job_env_initialized_.empty());
  ASSERT_TRUE(agent_manager_->job_client_ == nullptr);
  ASSERT_EQ(delay_executor_.size(), 1);

  DelayExecute();
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(), 1);
  ASSERT_TRUE(job_env_initialized_.empty());
  ASSERT_TRUE(agent_manager_->job_client_ == nullptr);
  ASSERT_EQ(delay_executor_.size(), 1);

  agent_manager_->HandleRegisterAgent(rpc::RegisterAgentRequest(), &register_agent_reply,
                                      callback);
  ASSERT_TRUE(agent_manager_->job_client_ != nullptr);

  DelayExecute();
  ASSERT_EQ(delay_executor_.size(), 0);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(), 1);
  ASSERT_TRUE(job_env_initialized_.empty());

  // Reply OK and a bad status.
  job_client_->ReplyInitializeJobEnv(Status::OK(), rpc::AGENT_RPC_STATUS_FAILED);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(),
            pre_initialize_job_runtime_env_enabled ? 1 : 0);
  ASSERT_TRUE(job_env_initialized_.empty());
  ASSERT_TRUE(agent_manager_->job_client_ != nullptr);
  ASSERT_EQ(delay_executor_.size(), pre_initialize_job_runtime_env_enabled ? 1 : 0);

  DelayExecute();
  ASSERT_EQ(delay_executor_.size(), 0);
  ASSERT_EQ(agent_manager_->initializing_jobs_.size(),
            pre_initialize_job_runtime_env_enabled ? 1 : 0);
  ASSERT_TRUE(job_env_initialized_.empty());

  if (pre_initialize_job_runtime_env_enabled) {
    job_client_->ReplyInitializeJobEnv(ray::Status::OK(), rpc::AGENT_RPC_STATUS_OK);
    ASSERT_TRUE(agent_manager_->initializing_jobs_.empty());
    ASSERT_EQ(job_env_initialized_.size(), 1);

    // Test warn job uninitialized.
    agent_manager_->OnJobEnvInitNotification(job_env_init_notification);
  }
}

TEST_F(AgentManagerTest, TestEagerInitializeJobEnv) {
  // Test the "eager" initialize that we initialize Env when the job is submitted
  bool preInitializeJobRuntimeEnvEnabled = true;
  TestEnableInitializeJobEnv(preInitializeJobRuntimeEnvEnabled);
}

TEST_F(AgentManagerTest, TestLazyInitializeJobEnv) {
  // Test the "lazy" initialize that we initialize Env when the task is submitted
  bool preInitializeJobRuntimeEnvEnabled = false;
  TestEnableInitializeJobEnv(preInitializeJobRuntimeEnvEnabled);
}

TEST_F(AgentManagerTest, TestDetectDeadAgentOnce) {
  ASSERT_TRUE(agent_manager_->StartDetectDeadAgent());
  ASSERT_FALSE(agent_manager_->StartDetectDeadAgent());
  ASSERT_FALSE(agent_manager_->StartDetectDeadAgent());
}
}  // namespace raylet
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
