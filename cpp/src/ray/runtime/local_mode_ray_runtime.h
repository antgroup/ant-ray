
#pragma once

#include <unordered_map>

#include "abstract_ray_runtime.h"

namespace ray {
namespace internal {

class LocalModeRayRuntime : public AbstractRayRuntime {
 public:
  LocalModeRayRuntime();

  ActorID GetNextActorID();
  std::string Put(std::shared_ptr<msgpack::sbuffer> data);
  const WorkerContext &GetWorkerContext();
  bool IsLocalMode() { return true; }

 private:
  WorkerContext worker_;
};

}  // namespace internal
}  // namespace ray
