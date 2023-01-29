
#include "native_ray_runtime.h"

#include <ray/api.h>

#include "./object/native_object_store.h"
#include "./object/object_store.h"
#include "./task/native_task_submitter.h"

namespace ray {
namespace internal {

NativeRayRuntime::NativeRayRuntime() {
  object_store_ = std::unique_ptr<ObjectStore>(new NativeObjectStore());
  task_submitter_ = std::unique_ptr<TaskSubmitter>(new NativeTaskSubmitter());
  task_executor_ = std::make_unique<TaskExecutor>();

  auto redis_ip = ConfigInternal::Instance().redis_ip;
  if (redis_ip.empty()) {
    redis_ip = GetNodeIpAddress();
  }
  std::string redis_address =
      redis_ip + ":" + std::to_string(ConfigInternal::Instance().redis_port);
  global_state_accessor_ = ProcessHelper::GetInstance().CreateGlobalStateAccessor(
      redis_address, ConfigInternal::Instance().redis_password);
}

const WorkerContext &NativeRayRuntime::GetWorkerContext() {
  return CoreWorkerProcess::GetCoreWorker().GetWorkerContext();
}

}  // namespace internal
}  // namespace ray
