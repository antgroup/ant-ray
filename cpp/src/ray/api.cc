
#include <ray/api.h>

#include "config_internal.h"
#include "ray/core_worker/core_worker.h"
#include "runtime/abstract_ray_runtime.h"

namespace ray {

static bool is_init_ = false;

void Init(ray::RayConfigCpp &config, int argc, char **argv) {
  if (!IsInitialized()) {
    internal::ConfigInternal::Instance().Init(config, argc, argv);
    auto runtime = internal::AbstractRayRuntime::DoInit();
    is_init_ = true;
  }
}

void Init(ray::RayConfigCpp &config) { Init(config, 0, nullptr); }

void Init() {
  RayConfigCpp config;
  Init(config, 0, nullptr);
}

bool IsInitialized() { return is_init_; }

void Shutdown() {
  // TODO(guyang.sgy): Clean the ray runtime.
  internal::AbstractRayRuntime::DoShutdown();
  is_init_ = false;
}

void RunTaskExecutionLoop() { ::ray::CoreWorkerProcess::RunTaskExecutionLoop(); }

}  // namespace ray
