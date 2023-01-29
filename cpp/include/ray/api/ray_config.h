
#pragma once
#include <ray/api/ray_exception.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "boost/optional.hpp"

namespace ray {

class RayConfigCpp {
 public:
  // The address of the Ray cluster to connect to.
  // If not provided, it will be initialized from environment variable "RAY_ADDRESS" by
  // default.
  std::string address = "";

  // Whether or not to run this application in a local mode. This is used for debugging.
  bool local_mode = false;

  // An array of directories or dynamic library files that specify the search path for
  // user code. This parameter is not used when the application runs in local mode.
  // Only searching the top level under a directory.
  std::vector<std::string> code_search_path;

  // The command line args to be appended as parameters of the `ray start` command. It
  // takes effect only if Ray head is started by a driver. Run `ray start --help` for
  // details.
  std::vector<std::string> head_args = {};

  /* The following are unstable parameters and their use is discouraged. */

  // Prevents external clients without the password from connecting to Redis if provided.
  boost::optional<std::string> redis_password_;

  long java_worker_process_default_memory_mb = 250;
  double java_heap_fraction = 0.8;
  int num_java_workers_per_process = 1;

  // The default actor lifetime type, `detached` or `non_detached`.
  std::string default_actor_lifetime = "non_detached";

  // The environment variables with json format to be set on worker processes.
  std::string job_worker_env;

  bool is_worker = false;

  // A namespace is a logical grouping of jobs and named actors.
  std::string ray_namespace = "";
};

}  // namespace ray