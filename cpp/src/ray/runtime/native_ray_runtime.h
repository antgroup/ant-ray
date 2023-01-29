
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace internal {

class NativeRayRuntime : public AbstractRayRuntime {
 public:
  NativeRayRuntime();

  const WorkerContext &GetWorkerContext();
};

}  // namespace internal
}  // namespace ray
