
#include "local_mode_ray_runtime.h"

#include <ray/api.h>

#include "./object/local_mode_object_store.h"
#include "./object/object_store.h"
#include "./task/local_mode_task_submitter.h"

namespace ray {
namespace internal {

LocalModeRayRuntime::LocalModeRayRuntime()
    : worker_(ray::rpc::WorkerType::DRIVER, ComputeDriverIdFromJob(JobID::Nil()),
              JobID::Nil()) {
  object_store_ = std::unique_ptr<ObjectStore>(new LocalModeObjectStore(*this));
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new LocalModeTaskSubmitter(*this));
}

ActorID LocalModeRayRuntime::GetNextActorID() {
  const auto next_task_index = worker_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_.GetCurrentJobID(), worker_.GetCurrentTaskID(), next_task_index);
  return actor_id;
}

const WorkerContext &LocalModeRayRuntime::GetWorkerContext() { return worker_; }

std::string LocalModeRayRuntime::Put(std::shared_ptr<msgpack::sbuffer> data) {
  ObjectID object_id =
      ObjectID::FromIndex(worker_.GetCurrentTaskID(), worker_.GetNextPutIndex());
  AbstractRayRuntime::Put(data, &object_id);
  return object_id.Binary();
}

}  // namespace internal
}  // namespace ray
