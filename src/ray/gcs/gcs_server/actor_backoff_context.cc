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

#include "ray/common/asio/asio_util.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"

namespace ray {
namespace gcs {

int64_t ActorBackoffContext::GetNextScheduleTimeMs() const {
  int64_t actor_backoff_max_failure_count =
      RayConfig::instance().gcs_actor_backoff_max_failure_count();
  int64_t faiure_count = std::min(actor_backoff_max_failure_count, failure_count_);
  int64_t interval_ms = (1LL << (faiure_count - 1));
  if (interval_ms < RayConfig::instance().gcs_actor_backoff_min_interval_ms()) {
    interval_ms = RayConfig::instance().gcs_actor_backoff_min_interval_ms();
  }
  return interval_ms + last_schedule_time_ms_;
}

bool ActorBackoffContext::IsInBackoff() const { return in_backoff_; }

void ActorBackoffContext::ScheduleActorBackoff(instrumented_io_context &io_context,
                                               std::shared_ptr<GcsActor> actor,
                                               const std::function<void()> &schedule_fn) {
  // Set backoff in-progress.
  in_backoff_ = true;
  ++failure_count_;

  uint64_t num_restarts = actor->GetActorTableData().num_restarts();
  int64_t cur_time_ms = current_time_ms();
  auto actor_backoff_trigger_min_duration_ms =
      RayConfig::instance().gcs_actor_backoff_trigger_min_duration_ms();
  if (cur_time_ms > last_schedule_time_ms_ + actor_backoff_trigger_min_duration_ms) {
    RAY_LOG(INFO) << "The scheduling of actor " << actor->GetActorID()
                  << " will start immediately, num_restarts = " << num_restarts;
    DoSchedule(schedule_fn);
    return;
  }

  int64_t next_schedule_time_ms = GetNextScheduleTimeMs();
  if (next_schedule_time_ms <= cur_time_ms) {
    RAY_LOG(INFO) << "The scheduling of actor " << actor->GetActorID()
                  << " will start immediately, num_restarts = " << num_restarts;
    DoSchedule(schedule_fn);
    return;
  }

  int64_t delay_ms = next_schedule_time_ms - cur_time_ms;
  RAY_LOG(INFO) << "The scheduling of actor " << actor->GetActorID()
                << " will start after " << delay_ms
                << " ms, num_restarts = " << num_restarts;

  std::weak_ptr<GcsActor> weak_actor(actor);
  auto func = [schedule_fn, weak_actor] {
    if (auto actor = weak_actor.lock()) {
      if (auto backoff_context = actor->GetBackoffContext()) {
        backoff_context->DoSchedule(schedule_fn);
      }
    }
  };
  execute_after(io_context, func, delay_ms);
}

void ActorBackoffContext::DoSchedule(const std::function<void()> &schedule_fn) {
  last_schedule_time_ms_ = current_time_ms();
  in_backoff_ = false;
  schedule_fn();
}

}  // namespace gcs
}  // namespace ray
