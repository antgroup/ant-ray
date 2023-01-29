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

#include "gtest/gtest.h"
#include "ray/common/test_util.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/gcs_server/test/gcs_table_storage_test_base.h"
#include "ray/gcs/store_client/redis_store_client.h"

namespace ray {

class RedisGcsTableStorageTest : public gcs::GcsTableStorageTestBase {
 public:
  static void SetUpTestCase() { TestSetupUtil::StartUpRedisServers(std::vector<int>()); }

  static void TearDownTestCase() { TestSetupUtil::ShutDownRedisServers(); }

  void SetUp() override {
    gcs::RedisClientOptions options("127.0.0.1", TEST_REDIS_SERVER_PORTS.front(), "",
                                    /*enable_sharding_conn=*/false);
    redis_client_ = std::make_shared<gcs::RedisClient>(options);
    RAY_CHECK_OK(redis_client_->Connect(io_service_pool_->GetAll()));

    gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
  }

  void TearDown() override { redis_client_->Disconnect(); }

 protected:
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(RedisGcsTableStorageTest, TestGcsTableApi) { TestGcsTableApi(); }

TEST_F(RedisGcsTableStorageTest, TestGcsTableWithJobIdApi) { TestGcsTableWithJobIdApi(); }

TEST_F(RedisGcsTableStorageTest, WatingCommandsConsume) {
  RayConfig::instance().initialize(
      R"(
        {
          "max_redis_buffer_size": 51200,
          "redis_waiting_commands_consume_interval_ms": 500,
          "minimum_interval_to_warn_redis_buffer_full_ms": 1000
        }
      )");

  {
    auto job_id = JobID::FromInt(1);

    int flush_count = 10000;
    auto promise = std::make_shared<std::promise<Status>>();
    std::shared_ptr<int> count_down(new int(flush_count));

    for (int i = 0; i < flush_count; ++i) {
      auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
      rpc::ActorTableData actor_table_data;
      actor_table_data.set_actor_id(actor_id.Binary());
      actor_table_data.set_job_id(job_id.Binary());
      auto status = gcs_table_storage_->ActorTable().Put(
          actor_id, actor_table_data, [promise, count_down](const Status &status) {
            if (--(*count_down) == 0) {
              promise->set_value(status);
            }
          });
      ASSERT_TRUE(status.ok());
    }

    auto future = promise->get_future();
    auto future_status = future.wait_for(std::chrono::seconds(30));
    ASSERT_EQ(future_status, std::future_status::ready);
    ASSERT_TRUE(future.get().ok());
  }

  {
    auto promise = std::make_shared<
        std::promise<std::unordered_map<ActorID, rpc::ActorTableData>>>();
    auto status = gcs_table_storage_->ActorTable().GetAll(
        [promise](const std::unordered_map<ActorID, rpc::ActorTableData> &data) {
          promise->set_value(data);
        });
    ASSERT_TRUE(status.ok());
    auto future = promise->get_future();
    auto future_status = future.wait_for(std::chrono::seconds(30));
    ASSERT_EQ(future_status, std::future_status::ready);
  }

  // Restore the configuration
  RayConfig::instance().initialize(
      R"(
      {
        "max_redis_buffer_size": 33554432,
        "redis_waiting_commands_consume_interval_ms": 5000,
        "minimum_interval_to_warn_redis_buffer_full_ms": 5000
      }
    )");
}

TEST_F(RedisGcsTableStorageTest, DeleteByJobId) {
  auto job_id = JobID::FromInt(1);

  {
    std::unordered_map<std::string, double> resource_map;
    resource_map.emplace(kMemory_ResourceLabel, 16);
    ResourceSet constraint_resources(resource_map);

    int flush_count = 1000;
    auto promise = std::make_shared<std::promise<Status>>();
    std::shared_ptr<int> count_down(new int(flush_count));

    for (int i = 0; i < flush_count; ++i) {
      auto actor_creation_task_spec =
          Mocker::GenActorCreationTask(job_id, /*max_restarts=*/-1, /*detached=*/true,
                                       /*name=*/"", /*owner_address=*/rpc::Address());
      auto task_id = actor_creation_task_spec.TaskId();
      auto task_table_data = std::make_shared<rpc::TaskTableData>();
      task_table_data->mutable_task()->mutable_task_spec()->CopyFrom(
          actor_creation_task_spec.GetMessage());
      auto status = gcs_table_storage_->TaskTable().Put(
          task_id, *task_table_data, [promise, count_down](const Status &status) {
            if (--(*count_down) == 0) {
              promise->set_value(status);
            }
          });
      ASSERT_TRUE(status.ok());
    }

    auto future = promise->get_future();
    auto future_status = future.wait_for(std::chrono::seconds(30));
    ASSERT_EQ(future_status, std::future_status::ready);
    ASSERT_TRUE(future.get().ok());
  }

  {
    auto promise = std::make_shared<std::promise<Status>>();
    RAY_UNUSED(gcs_table_storage_->TaskTable().DeleteByJobId(
        job_id, [promise](const Status &status) { promise->set_value(status); }));

    auto future = promise->get_future();
    auto future_status = future.wait_for(std::chrono::seconds(30));
    ASSERT_EQ(future_status, std::future_status::ready);
    ASSERT_TRUE(future.get().ok());
  }

  {
    auto promise =
        std::make_shared<std::promise<std::unordered_map<TaskID, rpc::TaskTableData>>>();
    auto status = gcs_table_storage_->TaskTable().GetAll(
        [promise](const std::unordered_map<TaskID, rpc::TaskTableData> &data) {
          promise->set_value(data);
        });
    ASSERT_TRUE(status.ok());

    auto future = promise->get_future();
    auto future_status = future.wait_for(std::chrono::seconds(30));
    ASSERT_EQ(future_status, std::future_status::ready);
    ASSERT_EQ(future.get().size(), 0);
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::TEST_REDIS_SERVER_EXEC_PATH = argv[1];
  ray::TEST_REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
