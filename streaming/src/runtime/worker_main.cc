#include <ray/api.h>

#include <chrono>
#include <thread>

#include "logging.h"
#include "worker.h"

// Name of the worker actor.
const std::string WORKER_NAME = "main_actor_2";
// Resources that each actor needs: 1 CPU core and 1 GB memory.
const std::unordered_map<std::string, double> RESOUECES{
    {"CPU", 1.0}, {"memory", 1024.0 * 1024.0 * 1024.0}};

ray::ActorHandleCpp<ray::streaming::Worker> StartWorker() {
  // Start the main server and the backup server.
  // Each of them needs 1 cpu core and 1 GB memory.
  // We use Ray's placement group to make sure that the 2 actors will be scheduled to 2
  // different nodes if possible.
  std::vector<std::unordered_map<std::string, double>> bundles{RESOUECES, RESOUECES};
  // Create the main server actor and backup server actor.
  return ray::Actor(ray::streaming::CreateWorker)
      .SetName(WORKER_NAME)     // Set name of this actor.
      .SetResources(RESOUECES)  // Set the resources that this actor needs.
      .SetMaxRestarts(-1)  // Tell Ray to restart this actor automatically upon failures.
      .Remote();
}

int main(int argc, char **argv) {
  // Start ray cluster and ray runtime.
  ray::RayConfigCpp config;
  ray::Init(config, argc, argv);

  STREAMING_LOG(INFO) << "Start worker.";
  ray::ActorHandleCpp<ray::streaming::Worker> worker_main = StartWorker();
  STREAMING_LOG(INFO) << "Get worker actor.";
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // auto worker_main = *ray::GetActor<ray::streaming::Worker>(WORKER_NAME);
  std::string context_config = "config";
  auto r =
      worker_main.Task(&ray::streaming::Worker::RegisterContext).Remote(context_config);
  STREAMING_LOG(INFO) << *(r.Get());
  auto r2 = worker_main.Task(&ray::streaming::Worker::RunLocalFunction)
                .Remote("./function_example.so", "CreateExampleSourceFunction");
  r2.Get();
  auto r3 = worker_main.Task(&ray::streaming::Worker::RunLocalFunction)
                .Remote("./function_example.so", "CreateLambdaExampleSourceFunction");
  r3.Get();

  // Stop ray cluster and ray runtime.
  ray::Shutdown();
  return 0;
}
