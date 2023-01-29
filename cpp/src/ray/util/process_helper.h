#pragma once
#include <string>

#include "../config_internal.h"
#include "ray/core_worker/core_worker.h"
#include "ray/gcs/gcs_client/global_state_accessor.h"
#include "util.h"

namespace ray {
namespace internal {

class ProcessHelper {
 public:
  void RayStart(CoreWorkerOptions::TaskExecutionCallback callback);
  void RayStop();
  void StartRayNode(const int redis_port, const std::string redis_password,
                    const std::vector<std::string> &head_args = {});
  void StopRayNode();

  static ProcessHelper &GetInstance() {
    static ProcessHelper processHelper;
    return processHelper;
  }

  std::unique_ptr<ray::gcs::GlobalStateAccessor> CreateGlobalStateAccessor(
      const std::string &redis_address, const std::string &redis_password);

  ProcessHelper(ProcessHelper const &) = delete;
  void operator=(ProcessHelper const &) = delete;

 private:
  ProcessHelper(){};

  void AddJobWorkerEnvToJobConfig(rpc::JobConfig &job_config,
                                  const std::string &job_worker_env);
};

}  // namespace internal
}  // namespace ray
