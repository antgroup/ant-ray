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

#include "ray/core_worker/worker_handler.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <vector>

namespace ray {
class WorkerHandlerTest : public ::testing::Test {
 public:
  WorkerHandlerTest() {}

  void MockWorkerSignalHandler(bool start) { signal_handler_calls.push_back(start); }

  std::shared_ptr<boost::asio::deadline_timer> MockExecuteAfter(std::function<void()> fn,
                                                                uint32_t timeout) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service);
    timer->expires_from_now(boost::posix_time::milliseconds(1000));
    timer->async_wait([timer, fn](const boost::system::error_code &error) {
      if (error != boost::asio::error::operation_aborted && fn) {
        fn();
      }
    });
    return timer;
  }

  void Run(uint64_t timeout_ms) {
    auto timer_period = boost::posix_time::milliseconds(timeout_ms);
    auto timer = std::make_shared<boost::asio::deadline_timer>(io_service, timer_period);
    timer->async_wait([this, timer](const boost::system::error_code &error) {
      ASSERT_FALSE(error);
      io_service.stop();
    });
    io_service.run();
    io_service.reset();
  }

  std::vector<bool> signal_handler_calls;
  std::function<void()> execute_after;
  boost::asio::io_service io_service;
};

TEST_F(WorkerHandlerTest, TestWorkerSignal) {
  InitWorkerProfiler([this](bool start) { MockWorkerSignalHandler(start); },
                     [this](std::function<void()> callback, uint32_t timeout) {
                       return MockExecuteAfter(callback, timeout);
                     });
  kill(getpid(), SIGPROF);
  ASSERT_THAT(signal_handler_calls, testing::ElementsAre(true));
  Run(2000);
  ASSERT_THAT(signal_handler_calls, testing::ElementsAre(true, false));
  signal_handler_calls.clear();
  kill(getpid(), SIGPROF);
  ASSERT_THAT(signal_handler_calls, testing::ElementsAre(true));
  kill(getpid(), SIGPROF);
  ASSERT_THAT(signal_handler_calls, testing::ElementsAre(true, false));
  Run(2000);
  ASSERT_THAT(signal_handler_calls, testing::ElementsAre(true, false));
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
