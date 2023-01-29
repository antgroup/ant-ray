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

#include <thread>

#include "gtest/gtest.h"
#include "ray/rpc/grpc_server.h"
#include "ray/rpc/object_manager/object_manager_client.h"
#include "ray/rpc/object_manager/object_manager_server.h"
#include "ray/rpc/worker/core_worker_client.h"
#include "ray/rpc/worker/core_worker_server.h"

namespace ray {
namespace rpc {

using namespace ray::rpc;

#define IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(METHOD)                                \
  virtual void Handle##METHOD(const rpc::METHOD##Request &request,                   \
                              rpc::METHOD##Reply *reply,                             \
                              rpc::SendReplyCallback send_reply_callback) override { \
    RAY_LOG(INFO) << "Called " << #METHOD;                                           \
    if (delay_ms_ > 0) {                                                             \
      RAY_LOG(INFO) << "Delay reply for " << delay_ms_ << "ms";                      \
      usleep(delay_ms_ * 1000);                                                      \
    }                                                                                \
    send_reply_callback(Status::OK(), nullptr, nullptr);                             \
  }

class MyHandler : public rpc::CoreWorkerServiceHandler {
 public:
  MyHandler(int delay_ms = 0) : delay_ms_(delay_ms) {}

  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(PushTask)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(DirectActorCallArgWaitComplete)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(GetObjectStatus)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(WaitForActorOutOfScope)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(SubscribeForObjectEviction)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(PubsubLongPolling)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(WaitForRefRemoved)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(AddObjectLocationOwner)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(RemoveObjectLocationOwner)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(GetObjectLocationsOwner)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(KillActor)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(CancelTask)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(RemoteCancelTask)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(GetCoreWorkerStats)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(LocalGC)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(SpillObjects)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(RestoreSpilledObjects)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(DeleteSpilledObjects)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(AddSpilledUrl)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(PlasmaObjectReady)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(RunOnUtilWorker)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(Exit)
  IMPL_VOID_BRPC_SERVICE_HANDLER_METHOD(BatchAssignObjectOwner)

 private:
  int delay_ms_ = 0;
};

class CoreWorkerClientTest : public ::testing::TestWithParam<bool> {
 public:
  CoreWorkerClientTest()
      : server_io_work(server_io_context),
        client_io_work(client_io_context),
        client_call_manager(client_io_context) {
    server_io_thread = std::thread([&]() { server_io_context.run(); });
    client_io_thread = std::thread([&]() { client_io_context.run(); });
  }

  std::unique_ptr<rpc::BrpcServer> SetupBrpcServer(int delay_ms = 0) {
    if (delay_ms > 0) {
      handler = MyHandler(delay_ms);
    }
    std::unique_ptr<rpc::BrpcServer> brpc_server =
        std::unique_ptr<rpc::BrpcServer>(new rpc::BrpcServer("name", 10003));
    std::unique_ptr<rpc::CoreWorkerBrpcStreamService> brpc_stream_service_ =
        std::unique_ptr<rpc::CoreWorkerBrpcStreamService>(
            new rpc::CoreWorkerBrpcStreamService(server_io_context, handler));
    brpc_server->RegisterStreamService(*brpc_stream_service_);
    brpc_server->Run();
    return brpc_server;
  }

  std::shared_ptr<CoreWorkerClient> SetupBrpcClient(bool infinite_reconnect) {
    rpc::Address address;
    address.set_ip_address("0.0.0.0");
    address.set_port(10003);
    WorkerID worker_id = ComputeDriverIdFromJob(JobID::FromInt(100));
    address.set_worker_id(worker_id.Binary());
    auto client = std::make_shared<CoreWorkerClient>(address, client_call_manager,
                                                     infinite_reconnect);
    return client;
  }

  void TearDown() override {
    server_io_context.stop();
    if (server_io_thread.joinable()) {
      server_io_thread.join();
    }
    client_io_context.stop();
    if (client_io_thread.joinable()) {
      client_io_thread.join();
    }
  }

 protected:
  instrumented_io_context server_io_context;
  instrumented_io_context::work server_io_work;
  std::thread server_io_thread;
  instrumented_io_context client_io_context;
  instrumented_io_context::work client_io_work;
  std::thread client_io_thread;
  MyHandler handler;
  ClientCallManager client_call_manager;
};

/// Mock connect fail at the first time, requests should fail and trigger callback
/// immediately. Test both enable reconnect and disable reconnect.
TEST_P(CoreWorkerClientTest, TestConnectFail) {
  bool infinite_reconnect = GetParam();
  RAY_LOG(INFO) << "infinite_reconnect: " << infinite_reconnect;
  auto brpc_client = SetupBrpcClient(infinite_reconnect);

  std::promise<Status> promise;
  brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                             [&](const Status &status, const PushTaskReply &reply) {
                               RAY_LOG(INFO) << "Callback status: " << status;
                               promise.set_value(status);
                             });

  if (infinite_reconnect) {
    // Since the reconnection process runs infinitely, we need to manually close the RPC
    // client to abort reconnection.
    sleep(1);
    brpc_client->Close();
  }
  auto future = promise.get_future();
  auto status = future.wait_for(std::chrono::seconds(10));
  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_TRUE(future.get().IsIOError());

  brpc_client->Close();
}

INSTANTIATE_TEST_CASE_P(CoreWorkerClientTest, CoreWorkerClientTest,
                        testing::Values(false, true));

/// With reconnect disabled, the following requests fails immediately after disconnect.
TEST_F(CoreWorkerClientTest, TestReconnectDisabled) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ false);

  int count = 0;
  std::vector<Status> status_result;
  while (count < 20) {
    std::promise<Status> promise;
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 promise.set_value(status);
                                 status_result.push_back(status);
                               });
    auto status = promise.get_future().wait_for(std::chrono::seconds(10));
    EXPECT_EQ(status, std::future_status::ready);

    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    if (count == 7) {
      RAY_LOG(INFO) << "Run";
      brpc_server->Run();
    }
    sleep(1);
  }

  while (brpc_client->IsReconnecting()) {
    sleep(1);
  }

  EXPECT_EQ(20, status_result.size());
  for (const auto &status : status_result) {
    RAY_LOG(INFO) << "status: " << status;
  }
  EXPECT_TRUE(status_result.at(0).ok());
  for (int i = 5; i < 20; i++) {
    EXPECT_TRUE(status_result.at(i).IsIOError());
  }

  brpc_client->Close();
}

/// With reconnect enabled, the following requests should be pended during reconnecting.
TEST_F(CoreWorkerClientTest, TestPendingRequestWhenReconnect) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ true);

  int count = 0;
  while (count < 10) {
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                               });
    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    sleep(1);
  }

  /// The pending queue is should not be empty.
  EXPECT_FALSE(brpc_client->IsPendingPushActorTaskRequestsEmpty());

  brpc_client->Close();
}

/// With reconnect enabled, the following requests should be pended during reconnecting.
/// And pending callbacks triggered after clients are closed.
TEST_F(CoreWorkerClientTest, TestPendingRequestFailAfterClientClosed) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ true);

  int count = 0;
  std::vector<Status> status_result;
  while (count < 10) {
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 status_result.push_back(status);
                               });
    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    sleep(1);
  }

  brpc_client->Close();
  sleep(1);

  /// Wait for reconnect end
  while (brpc_client->IsReconnecting()) {
    RAY_LOG(INFO) << "Reconnecting, wait";
    sleep(1);
  }

  EXPECT_EQ(10, status_result.size());
  for (int i = 5; i < 10; i++) {
    EXPECT_TRUE(status_result.at(i).IsIOError());
  }
  /// The pending queue is should be empty.
  EXPECT_TRUE(brpc_client->IsPendingPushActorTaskRequestsEmpty());
}

/// With reconnect enabled, the following requests should be pended during reconnecting.
/// But if push task too fast, the following pushing maybe blocked by future instead of
/// pending. So If the client is closed finally, the future should be set to release the
/// thread above.
TEST_F(CoreWorkerClientTest, TestFutureBlockingReleaseAfterClientClosed) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ true);
  brpc_client->SetOnDisconnectedCallback([]() {
    RAY_LOG(WARNING) << "TestPendingRequestFailAfterReconnectFail2 disconnect callback";
  });
  int count = 0;
  while (count < 1000) {
    RAY_LOG(INFO) << "PushActorTask begin " << count;
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                               });
    RAY_LOG(INFO) << "PushActorTask end " << count;

    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    /// Push tasks quickly after disconnect, so do not sleep. To trigger future wait.
    if (count < 5) {
      sleep(1);
    }
  }

  brpc_client->Close();
  sleep(1);

  /// Wait for reconnect end
  while (brpc_client->IsReconnecting()) {
    RAY_LOG(INFO) << "Reconnecting, wait";
    sleep(1);
  }

  /// The pending queue is should be empty.
  EXPECT_TRUE(brpc_client->IsPendingPushActorTaskRequestsEmpty());
}

/// With reconnect enabled, the following requests should be pended during reconnecting.
/// And pending callbacks triggered after the client is closed.
/// After the client is closed, send request and faill immediately.
TEST_F(CoreWorkerClientTest, TestSendRequestFailAfterClientClosed) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ true);

  int count = 0;
  std::vector<Status> status_result;
  while (count < 10) {
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 status_result.push_back(status);
                               });
    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    sleep(1);
  }

  brpc_client->Close();
  sleep(1);

  /// Wait for reconnect end
  while (brpc_client->IsReconnecting()) {
    RAY_LOG(INFO) << "Reconnecting, wait";
    sleep(1);
  }

  EXPECT_EQ(10, status_result.size());
  for (int i = 5; i < 10; i++) {
    EXPECT_TRUE(status_result.at(i).IsIOError());
  }
  /// The pending queue is should be empty.
  EXPECT_TRUE(brpc_client->IsPendingPushActorTaskRequestsEmpty());

  count = 0;
  status_result.clear();
  while (count < 10) {
    std::promise<Status> promise;
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 promise.set_value(status);
                                 status_result.push_back(status);
                               });
    auto status = promise.get_future().wait_for(std::chrono::seconds(10));
    EXPECT_EQ(status, std::future_status::ready);
    count++;
    sleep(1);
  }
  EXPECT_EQ(10, status_result.size());
  for (const auto &status : status_result) {
    EXPECT_TRUE(status.IsIOError());
  }
}

/// With reconnect enabled, the following requests should be pended during reconnecting,
/// and then send out after reconnect success.
TEST_F(CoreWorkerClientTest, TestSendPendingAfterReconnect) {
  auto brpc_server = SetupBrpcServer();
  auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ true);

  int count = 0;
  std::vector<Status> status_result;
  while (count < 20) {
    // std::promise<Status> promise;
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 //  promise.set_value(status);
                                 status_result.push_back(status);
                               });
    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      brpc_server->Shutdown();
    }
    if (count == 10) {
      RAY_LOG(INFO) << "Run";
      brpc_server->Run();
    }
    sleep(1);
  }

  while (brpc_client->IsReconnecting()) {
    sleep(1);
  }

  EXPECT_EQ(20, status_result.size());
  RAY_LOG(INFO) << "Print all status:";
  for (const auto &status : status_result) {
    EXPECT_TRUE(status.ok() ||
                (status.IsIOError() && status.message() == "rpc server disconnected"));
  }
  EXPECT_TRUE(brpc_client->IsPendingPushActorTaskRequestsEmpty());

  brpc_client->Close();
}

TEST_F(CoreWorkerClientTest, TestSendAndDestruct) {
  // The RPC server will delay sending replies. This makes sure `brpc_client` is
  // out-of-scope before the reply is received.
  auto brpc_server = SetupBrpcServer(/*delay_ms*/ 1000);

  std::promise<Status> promise;
  {
    auto brpc_client = SetupBrpcClient(/*infinite_reconnect*/ false);
    brpc_client->PushActorTask(std::make_unique<PushTaskRequest>(), false,
                               [&](const Status &status, const PushTaskReply &reply) {
                                 RAY_LOG(INFO) << "Callback status: " << status;
                                 promise.set_value(status);
                               });
  }
  auto future = promise.get_future();
  EXPECT_EQ(future.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_TRUE(future.get().ok());

  brpc_server->Shutdown();
}

TEST(ObjectManagerClient, TestBadNetwork) {
  rpc::Address address;
  address.set_ip_address("127.0.0.1");
  address.set_port(10001);
  WorkerID worker_id = ComputeDriverIdFromJob(JobID::FromInt(100));
  address.set_worker_id(worker_id.Binary());

  instrumented_io_context io_context;
  instrumented_io_context::work io_work(io_context);
  ClientCallManager client_call_manager(io_context);

  std::thread io_thread([&]() { io_context.run(); });

  std::promise<Status> promise;
  ObjectManagerClient client("127.0.0.1", 10001, client_call_manager);

  rpc::PullRequest pull_request;
  ObjectID object_id;
  pull_request.set_object_id(object_id.Binary());

  client.Pull(pull_request, [&](const Status &status, const rpc::PullReply &reply) {
    if (!status.ok()) {
      RAY_LOG(WARNING) << "Send pull " << object_id << " request to client "
                       << " failed due to" << status.message();
      promise.set_value(status);
    }
  });

  auto future = promise.get_future();
  auto status = future.wait_for(std::chrono::seconds(10));
  EXPECT_EQ(status, std::future_status::ready);
  EXPECT_TRUE(future.get().IsIOError());

  io_context.stop();
  io_thread.join();
}

class MockHandler : public ObjectManagerServiceHandler {
  void HandlePush(const PushRequest &request, PushReply *reply,
                  SendReplyCallback send_reply_callback) override {}
  /// Handle a `Pull` request
  void HandlePull(const PullRequest &request, PullReply *reply,
                  SendReplyCallback send_reply_callback) override {
    send_reply_callback(Status::OK(), nullptr, nullptr);
    RAY_LOG(INFO) << "HandlePull called";
  }
  /// Handle a `FreeObjects` request
  void HandleFreeObjects(const FreeObjectsRequest &request, FreeObjectsReply *reply,
                         SendReplyCallback send_reply_callback) override {}
};

TEST(ObjectManagerClient, TestReconnect) {
  instrumented_io_context server_io_context;
  instrumented_io_context::work server_io_work(server_io_context);
  std::thread server_io_thread([&]() { server_io_context.run(); });

  MockHandler handler;
  std::unique_ptr<GrpcServer> server = std::make_unique<GrpcServer>("Test", 10002, 2);
  std::unique_ptr<ObjectManagerGrpcService> service =
      std::make_unique<ObjectManagerGrpcService>(server_io_context, handler);
  server->RegisterService(*service);
  server->Run();

  instrumented_io_context client_io_context;
  instrumented_io_context::work client_io_work(client_io_context);
  ClientCallManager client_call_manager(client_io_context);
  std::thread client_io_thread([&]() { client_io_context.run(); });

  ObjectManagerClient client("0.0.0.0", 10002, client_call_manager);

  rpc::PullRequest pull_request;
  ObjectID object_id;
  pull_request.set_object_id(object_id.Binary());

  int count = 0;
  std::vector<Status> status_result;
  while (count < 20) {
    std::promise<Status> promise;
    client.Pull(pull_request, [&](const Status &status, const rpc::PullReply &reply) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Send pull " << object_id << " request to client "
                         << " failed due to" << status.message();

      } else {
        RAY_LOG(INFO) << "Send pull return.";
      }
      promise.set_value(status);
      status_result.push_back(status);
    });

    auto status = promise.get_future().wait_for(std::chrono::seconds(10));
    EXPECT_EQ(status, std::future_status::ready);

    count++;
    if (count == 5) {
      RAY_LOG(INFO) << "Shutdown";
      server->Shutdown();
    }
    if (count == 7) {
      RAY_LOG(INFO) << "Run";
      service = std::make_unique<ObjectManagerGrpcService>(server_io_context, handler);
      server = std::make_unique<GrpcServer>("Test", 10002, 2);
      server->RegisterService(*service);
      server->Run();
    }
    sleep(1);
  }

  for (const auto &status : status_result) {
    RAY_LOG(INFO) << "status: " << status;
  }
  EXPECT_TRUE(status_result.at(0).ok());
  EXPECT_TRUE(status_result.at(5).IsIOError());
  EXPECT_TRUE(status_result.at(19).ok());

  server_io_context.stop();
  client_io_context.stop();
  server_io_thread.join();
  client_io_thread.join();
}

}  // namespace rpc
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
