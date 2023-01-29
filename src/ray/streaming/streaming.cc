#include "ray/streaming/streaming.h"

#include "ray/core_worker/core_worker.h"

namespace ray {
namespace streaming {

void SendInternal(const WorkerID &worker_id, const ActorID &peer_actor_id,
                  std::vector<std::shared_ptr<Buffer>> buffers, RayFunction &function,
                  int return_num, std::vector<ObjectID> &return_ids) {
  std::unordered_map<std::string, double> resources;
  /// Make ignore_return true when return_num is 0 for performance optimization.
  TaskOptions options{/*name=*/"",
                      return_num,
                      resources,
                      /*serialized_runtime_env*/ "{}",
                      /*override_environment_variables=*/{},
                      /*ignore_return*/ return_num == 0};

  auto meta_data = "RAW";
  std::shared_ptr<LocalMemoryBuffer> meta =
      std::make_shared<LocalMemoryBuffer>((uint8_t *)meta_data, strlen(meta_data), true);

  std::vector<std::unique_ptr<TaskArg>> args;
  for (auto &buffer : buffers) {
    RAY_CHECK(buffer);
    if (function.GetLanguage() == Language::PYTHON) {
      auto dummy = "__RAY_DUMMY__";
      std::shared_ptr<LocalMemoryBuffer> dummyBuffer =
          std::make_shared<LocalMemoryBuffer>((uint8_t *)dummy, strlen(dummy), true);
      args.emplace_back(new TaskArgByValue(std::make_shared<RayObject>(
          std::move(dummyBuffer), meta, std::vector<ObjectID>(), true)));
    }
    args.emplace_back(new TaskArgByValue(
        std::make_shared<RayObject>(buffer, meta, std::vector<ObjectID>(), true)));
  }

  CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id);
  RAY_UNUSED(CoreWorkerProcess::GetCoreWorker().SubmitActorTask(
      peer_actor_id, function, args, options, &return_ids));
}

const ActorID &GetCurrentActorIDInternal() {
  return ray::CoreWorkerProcess::GetCoreWorker().GetWorkerContext().GetCurrentActorID();
}

bool IsInitializedInternal() { return ray::CoreWorkerProcess::IsInitialized(); }

}  // namespace streaming
}  // namespace ray
