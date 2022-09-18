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

void RunTest() {
  RayConfig::instance().gcs_max_active_rpcs_per_handler() = -1;

  instrumented_io_context server_io_service;
  auto server_work = std::make_unique<boost::asio::io_service::work>(server_io_service);
  std::unique_ptr<std::thread> server_thread_io_service(
      new std::thread([&server_io_service] { server_io_service.run(); }));

  std::unique_ptr<instrumented_io_context> io_service_ptr;
  // Pass if created in main thread.
  // io_service_ptr.reset(new instrumented_io_context());
  std::promise<bool> promise1;
  std::unique_ptr<std::thread> thread_io_service(
      new std::thread([&io_service_ptr, &promise1] {
        // Fail if created in child thread.
        io_service_ptr.reset(new instrumented_io_context());
        auto &io_service = *io_service_ptr;
        promise1.set_value(true);
        auto work = std::make_unique<boost::asio::io_service::work>(io_service);
        io_service.run();
      }));
  promise1.get_future().get();
  auto &io_service = *io_service_ptr;

  // Create GCS client and global state.
  gcs::GcsClientOptions options("127.0.0.1:6379");
  auto gcs_client = std::make_unique<gcs::GcsClient>(options);
  RAY_CHECK_OK(gcs_client->Connect(io_service));
  RAY_LOG(INFO) << "global_state_->Connect() done";

  std::promise<bool> promise2;
  RAY_LOG(INFO) << "Before RPC";
  server_io_service.post([&io_service, &promise2]() {
    io_service.post([&promise2]() {
      RAY_LOG(INFO) << "Callback invoked in fake post.";
      promise2.set_value(true);
    }, "DUMMY");
  }, "DUMMY");
  RAY_LOG(INFO) << "After RPC";
  promise2.get_future().get();
  RAY_LOG(INFO) << "TEST FINISHED";

  // // usleep(1000 * 1000 * 300);
  // gcs_client->Disconnect();
  // gcs_client.reset();

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
