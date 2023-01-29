#pragma once

#include <ray/api/ray_runtime.h>

#include <memory>

#include "invocation_spec.h"

namespace ray {
namespace internal {

class TaskSubmitter {
 public:
  TaskSubmitter(){};

  virtual ~TaskSubmitter(){};

  virtual ObjectID SubmitTask(InvocationSpec &invocation,
                              const CallOptions &call_options) = 0;

  virtual ActorID CreateActor(InvocationSpec &invocation,
                              const ActorCreationOptions &create_options) = 0;

  virtual ObjectID SubmitActorTask(InvocationSpec &invocation,
                                   const CallOptions &call_options) = 0;

  virtual ActorID GetActor(const std::string &actor_name,
                           const std::string &ray_namespace) const = 0;

  virtual ray::PlacementGroup CreatePlacementGroup(
      const ray::PlacementGroupCreationOptionsCpp &create_options) = 0;

  virtual void RemovePlacementGroup(const std::string &group_id) = 0;

  virtual bool WaitPlacementGroupReady(const std::string &group_id, int timeout_seconds) {
    return true;
  }
};
}  // namespace internal
}  // namespace ray