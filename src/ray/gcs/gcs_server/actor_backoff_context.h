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

#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/constants.h"

namespace ray {
namespace gcs {
class GcsActor;
class ActorBackoffContext {
 public:
  void ScheduleActorBackoff(instrumented_io_context &io_context,
                            std::shared_ptr<GcsActor> actor,
                            const std::function<void()> &schedule_fn);

  bool IsInBackoff() const;

 private:
  int64_t GetNextScheduleTimeMs() const;
  void DoSchedule(const std::function<void()> &schedule_fn);

 private:
  int64_t last_schedule_time_ms_ = 0;
  int64_t failure_count_ = 0;
  bool in_backoff_ = false;
};
}  // namespace gcs
}  // namespace ray
