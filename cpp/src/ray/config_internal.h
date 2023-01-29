
#pragma once
#include <ray/api/ray_config.h>

#include <memory>
#include <string>

#include "ray/core_worker/common.h"

namespace ray {
namespace internal {

enum class RunMode { SINGLE_PROCESS, CLUSTER };

class ConfigInternal {
 public:
  WorkerType worker_type = WorkerType::DRIVER;

  RunMode run_mode = RunMode::SINGLE_PROCESS;

  std::string redis_ip;

  int redis_port = 6379;

  std::string redis_password = "5241590000000000";

  int node_manager_port = 0;

  std::vector<std::string> code_search_path;

  std::string plasma_store_socket_name = "";

  std::string raylet_socket_name = "";

  std::string session_dir = "";

  std::string job_id = "";

  std::string logs_dir = "";

  std::string node_ip_address = "";

  std::vector<std::string> head_args = {};

  // The default actor lifetime type.
  rpc::JobConfig_ActorLifetime default_actor_lifetime =
      rpc::JobConfig_ActorLifetime_NON_DETACHED;

  // ======== ANT-INTERNAL begin ========
  long job_total_memory_mb;

  long max_job_total_memory_mb;

  // The environment variables with json format to be set on worker processes.
  std::string job_worker_env;
  // ======== ANT-INTERNAL end ========

  // For java worker
  long java_worker_process_default_memory_mb;
  double java_heap_fraction;
  int num_java_workers_per_process;
  std::string ray_namespace = "";

  static ConfigInternal &Instance() {
    static ConfigInternal config;
    return config;
  };

  void Init(RayConfigCpp &config, int argc, char **argv);

  void SetRedisAddress(const std::string address);

  void UpdateSessionDir(const std::string dir);

  ConfigInternal(ConfigInternal const &) = delete;

  void operator=(ConfigInternal const &) = delete;

 private:
  ConfigInternal(){};
};

}  // namespace internal
}  // namespace ray
