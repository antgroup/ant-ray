// Copyright 2021 The Ray Authors.
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
#include <signal.h>
#include "ray/common/ray_config.h"
#include "ray/core_worker/core_worker.h"
#include "ray/util/logging.h"

static void HandleProfilingSignal(int signal);

namespace ray {

class WorkerProfiler : public std::enable_shared_from_this<WorkerProfiler> {
 public:
  WorkerProfiler(std::function<void(bool)> profile_handler,
                 ExecuteAfterFn profile_handler_done_callback)
      : profile_handler_(profile_handler),
        profile_handler_done_callback_(profile_handler_done_callback) {}

  void ProfileWorker() {
    RAY_CHECK(profile_handler_ != nullptr);
    if (profile_timer_) {
      RAY_LOG(INFO) << "Worker profile handler cancelled.";
      profile_timer_->cancel();
      profile_timer_.reset();
      profile_handler_(false);
    } else {
      RAY_LOG(INFO) << "Worker profile handler will start for "
                    << RayConfig::instance().worker_profiling_duration_ms() << " ms.";
      profile_handler_(true);
      profile_timer_ = profile_handler_done_callback_(
          [this, self = shared_from_this()] {
            RAY_LOG(INFO) << "Worker profile handler stopped.";
            profile_timer_.reset();
            profile_handler_(false);
          },
          RayConfig::instance().worker_profiling_duration_ms());
    }
  }

 private:
  std::function<void(bool)> profile_handler_;
  ExecuteAfterFn profile_handler_done_callback_;
  std::shared_ptr<boost::asio::deadline_timer> profile_timer_;
};

static std::shared_ptr<WorkerProfiler> worker_profiler;

void InitWorkerProfiler(std::function<void(bool)> profile_handler,
                        ExecuteAfterFn profile_handler_done_callback) {
  if (profile_handler) {
    if (!worker_profiler) {
      worker_profiler = std::make_shared<WorkerProfiler>(profile_handler,
                                                         profile_handler_done_callback);
    }
    signal(SIGPROF, HandleProfilingSignal);
    RAY_LOG(INFO) << "Register C++ SIGPROF signal handler.";
  } else {
    RAY_LOG(INFO) << "Not to register C++ SIGPROF signal handler, "
                  << "the worker profile handler is empty.";
  }
}
}  // namespace ray

static void HandleProfilingSignal(int signal) {
  if (signal == SIGPROF) {
    RAY_LOG(INFO) << "SIGPROF signal received.";
    RAY_CHECK(ray::worker_profiler != nullptr);
    ray::worker_profiler->ProfileWorker();
  }
}
