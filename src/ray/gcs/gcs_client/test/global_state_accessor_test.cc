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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_client/accessor.h"
#include "ray/gcs/gcs_client/gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {

class MockServer : public rpc::HeartbeatInfoHandler {
  void HandleReportHeartbeat(const rpc::ReportHeartbeatRequest &request,
                             rpc::ReportHeartbeatReply *reply,
                             rpc::SendReplyCallback send_reply_callback) override {
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }

  /// Handle check alive request for GCS.
  void HandleCheckAlive(const rpc::CheckAliveRequest &request,
                        rpc::CheckAliveReply *reply,
                        rpc::SendReplyCallback send_reply_callback) override {
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
};

void RunTest() {
  RayConfig::instance().gcs_max_active_rpcs_per_handler() = -1;

  instrumented_io_context server_io_service;
  MockServer server;
  auto service =
      std::make_unique<rpc::HeartbeatInfoGrpcService>(server_io_service, server);
  auto rpc_server = std::make_unique<rpc::GrpcServer>(
      /*grpc_server_name=*/"MockedGcsServer",
      /*grpc_server_port=*/6379,
      /*listen_to_localhost_only=*/true,
      /*grpc_server_thread_num=*/1,
      /*keepalive_time_ms=*/RayConfig::instance().grpc_keepalive_time_ms());
  rpc_server->RegisterService(*service);
  rpc_server->Run();
  auto server_work = std::make_unique<boost::asio::io_service::work>(server_io_service);
  std::unique_ptr<std::thread> server_thread_io_service(
      new std::thread([&server_io_service] { server_io_service.run(); }));

  instrumented_io_context io_service;
  std::promise<bool> promise1;
  std::unique_ptr<std::thread> thread_io_service(
      new std::thread([&io_service, &promise1] {
        promise1.set_value(true);
        auto work = std::make_unique<boost::asio::io_service::work>(io_service);
        io_service.run();
      }));
  promise1.get_future().get();

  // Create GCS client and global state.
  gcs::GcsClientOptions options("127.0.0.1:6379");
  auto gcs_client = std::make_unique<gcs::GcsClient>(options);
  RAY_CHECK_OK(gcs_client->Connect(io_service));
  RAY_LOG(INFO) << "global_state_->Connect() done";

  std::vector<std::string> job_table_data;
  std::promise<bool> promise2;
  auto heartbeat = std::make_shared<rpc::HeartbeatTableData>();
  RAY_LOG(INFO) << "Before RPC";
  RAY_CHECK_OK(
      gcs_client->Nodes().AsyncReportHeartbeat(heartbeat, [&promise2](Status status) {
        RAY_LOG(INFO) << "Callback invoked. Status: " << status;
        promise2.set_value(true);
      }));
  RAY_LOG(INFO) << "After RPC";
  promise2.get_future().get();
  RAY_LOG(INFO) << "TEST FINISHED";

  // usleep(1000 * 1000 * 300);
  gcs_client->Disconnect();
  gcs_client.reset();

  rpc_server->Shutdown();
  io_service.stop();
  server_io_service.stop();
  thread_io_service->join();
  server_thread_io_service->join();
}
}  // namespace ray

int main(int argc, char **argv) {
  ray::RunTest();
  return 0;
}
