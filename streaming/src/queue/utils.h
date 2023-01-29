#ifndef _STREAMING_QUEUE_UTILS_H_
#define _STREAMING_QUEUE_UTILS_H_
#include <future>
#include <thread>

#include "logging.h"
#include "ray/core_worker/common.h"
#include "utility.h"

namespace ray {
namespace streaming {

/// Helper class encapulate std::future to help multithread async wait.
class PromiseWrapper {
 public:
  Status Wait() {
    std::future<bool> fut = promise_.get_future();
    fut.get();
    return status_;
  }

  Status WaitFor(uint64_t timeout_ms) {
    std::future<bool> fut = promise_.get_future();
    std::future_status status;
    do {
      status = fut.wait_for(std::chrono::milliseconds(timeout_ms));
      if (status == std::future_status::deferred) {
      } else if (status == std::future_status::timeout) {
        return Status::Invalid("timeout");
      } else if (status == std::future_status::ready) {
        return status_;
      }
    } while (status == std::future_status::deferred);

    return status_;
  }

  void Notify(Status status) {
    status_ = status;
    promise_.set_value(true);
  }

  Status GetResultStatus() { return status_; }

 private:
  std::promise<bool> promise_;
  Status status_;
};

inline ray::ActorID RandomActorID() {
  JobID job_id = JobID::FromInt(0);
  TaskID task_id = TaskID::ForDriverTask(job_id);
  return ray::ActorID::Of(job_id, task_id, rand() % 100 + 1);
}

}  // namespace streaming
}  // namespace ray
#endif
