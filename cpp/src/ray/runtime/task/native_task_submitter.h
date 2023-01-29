#pragma once

#include "../native_ray_runtime.h"
#include "invocation_spec.h"
#include "task_submitter.h"

namespace ray {
namespace internal {

class NativeTaskSubmitter : public TaskSubmitter {
 public:
  ObjectID SubmitTask(InvocationSpec &invocation, const CallOptions &call_options);

  ActorID CreateActor(InvocationSpec &invocation,
                      const ActorCreationOptions &create_options);

  ObjectID SubmitActorTask(InvocationSpec &invocation, const CallOptions &call_options);

  ActorID GetActor(const std::string &actor_name, const std::string &ray_namespace) const;

  ray::PlacementGroup CreatePlacementGroup(
      const ray::PlacementGroupCreationOptionsCpp &create_options);
  void RemovePlacementGroup(const std::string &group_id);
  bool WaitPlacementGroupReady(const std::string &group_id, int timeout_seconds);

 private:
  ObjectID Submit(InvocationSpec &invocation, const CallOptions &call_options);
};
}  // namespace internal
}  // namespace ray