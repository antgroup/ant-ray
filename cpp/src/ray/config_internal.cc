#include "config_internal.h"

#include <boost/dll/runtime_symbol_info.hpp>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_split.h"

ABSL_FLAG(std::string, ray_address, "", "The address of the Ray cluster to connect to.");

/// absl::flags does not provide a IsDefaultValue method, so use a non-empty dummy default
/// value to support empty redis password.
ABSL_FLAG(std::string, ray_redis_password, "absl::flags dummy default value",
          "Prevents external clients without the password from connecting to Redis "
          "if provided.");

ABSL_FLAG(std::string, ray_code_search_path, "",
          "A list of directories or files of dynamic libraries that specify the "
          "search path for user code. Only searching the top level under a directory. "
          "':' is used as the separator.");

ABSL_FLAG(std::string, ray_job_id, "", "Assigned job id.");

ABSL_FLAG(int32_t, ray_node_manager_port, 0, "The port to use for the node manager.");

ABSL_FLAG(std::string, ray_raylet_socket_name, "",
          "It will specify the socket name used by the raylet if provided.");

ABSL_FLAG(std::string, ray_plasma_store_socket_name, "",
          "It will specify the socket name used by the plasma store if provided.");

ABSL_FLAG(std::string, ray_session_dir, "", "The path of this session.");

ABSL_FLAG(std::string, ray_logs_dir, "", "Logs dir for workers.");

ABSL_FLAG(std::string, ray_node_ip_address, "", "The ip address for this node.");

ABSL_FLAG(std::string, ray_head_args, "",
          "The command line args to be appended as parameters of the `ray start` "
          "command. It takes effect only if Ray head is started by a driver. Run `ray "
          "start --help` for details.");

ABSL_FLAG(std::string, ray_job_default_actor_lifetime, "",
          "The default actor lifetime type, `detached` or `non_detached`.");

ABSL_FLAG(std::string, ray_job_namespace, "",
          "The namespace of job. If not set,"
          " a unique value will be randomly generated.");

ABSL_FLAG(int64_t, ray_job_total_memory_mb, 0, "ANT-INTERNAL: Total memory used by job");

ABSL_FLAG(int64_t, ray_job_max_total_memory_mb, 0,
          "ANT-INTERNAL: Max total memory used by job");

ABSL_FLAG(std::string, ray_job_worker_env, "",
          "ANT-INTERNAL: The environment variables with json format to be set on worker "
          "processes.");

namespace ray {
namespace internal {

rpc::JobConfig_ActorLifetime ParseDefaultActorLifetimeType(
    const std::string &default_actor_lifetime_string) {
  if (default_actor_lifetime_string.empty() ||
      default_actor_lifetime_string == "non_detached") {
    return rpc::JobConfig_ActorLifetime_NON_DETACHED;
  }
  RAY_CHECK(default_actor_lifetime_string == "detached")
      << "The default_actor_lifetime_string config must be `detached` or `non_detached`.";
  return rpc::JobConfig_ActorLifetime_DETACHED;
}

void ConfigInternal::Init(RayConfigCpp &config, int argc, char **argv) {
  java_worker_process_default_memory_mb = config.java_worker_process_default_memory_mb;
  java_heap_fraction = config.java_heap_fraction;
  num_java_workers_per_process = config.num_java_workers_per_process;
  if (!config.address.empty()) {
    SetRedisAddress(config.address);
  }
  run_mode = config.local_mode ? RunMode::SINGLE_PROCESS : RunMode::CLUSTER;
  if (!config.code_search_path.empty()) {
    code_search_path = config.code_search_path;
  }
  if (config.redis_password_) {
    redis_password = *config.redis_password_;
  }
  if (!config.head_args.empty()) {
    head_args = config.head_args;
  }
  if (!config.default_actor_lifetime.empty()) {
    default_actor_lifetime = ParseDefaultActorLifetimeType(config.default_actor_lifetime);
  }
  if (!config.job_worker_env.empty()) {
    job_worker_env = config.job_worker_env;
  }

  if (argc != 0 && argv != nullptr) {
    // Parse config from command line.
    absl::ParseCommandLine(argc, argv);

    if (!FLAGS_ray_code_search_path.CurrentValue().empty()) {
      // Code search path like this "/path1/xxx.so:/path2".
      RAY_LOG(INFO) << "The code search path is "
                    << FLAGS_ray_code_search_path.CurrentValue();
      code_search_path = absl::StrSplit(FLAGS_ray_code_search_path.CurrentValue(), ':',
                                        absl::SkipEmpty());
    }
    if (!FLAGS_ray_address.CurrentValue().empty()) {
      SetRedisAddress(FLAGS_ray_address.CurrentValue());
    }
    // Don't rewrite `ray_redis_password` when it is not set in the command line.
    if (FLAGS_ray_redis_password.CurrentValue() !=
        FLAGS_ray_redis_password.DefaultValue()) {
      redis_password = FLAGS_ray_redis_password.CurrentValue();
    }
    if (!FLAGS_ray_job_id.CurrentValue().empty()) {
      job_id = FLAGS_ray_job_id.CurrentValue();
    }
    node_manager_port = absl::GetFlag(FLAGS_ray_node_manager_port);
    if (!FLAGS_ray_raylet_socket_name.CurrentValue().empty()) {
      raylet_socket_name = FLAGS_ray_raylet_socket_name.CurrentValue();
    }
    if (!FLAGS_ray_plasma_store_socket_name.CurrentValue().empty()) {
      plasma_store_socket_name = FLAGS_ray_plasma_store_socket_name.CurrentValue();
    }
    if (!FLAGS_ray_session_dir.CurrentValue().empty()) {
      session_dir = FLAGS_ray_session_dir.CurrentValue();
    }
    if (!FLAGS_ray_logs_dir.CurrentValue().empty()) {
      logs_dir = FLAGS_ray_logs_dir.CurrentValue();
    }
    if (!FLAGS_ray_node_ip_address.CurrentValue().empty()) {
      node_ip_address = FLAGS_ray_node_ip_address.CurrentValue();
    }
    if (!FLAGS_ray_head_args.CurrentValue().empty()) {
      std::vector<std::string> args =
          absl::StrSplit(FLAGS_ray_head_args.CurrentValue(), ' ', absl::SkipEmpty());
      head_args.insert(head_args.end(), args.begin(), args.end());
    }
    if (!FLAGS_ray_job_default_actor_lifetime.CurrentValue().empty()) {
      default_actor_lifetime = ParseDefaultActorLifetimeType(
          FLAGS_ray_job_default_actor_lifetime.CurrentValue());
    }
    if (!FLAGS_ray_job_worker_env.CurrentValue().empty()) {
      job_worker_env = FLAGS_ray_job_worker_env.CurrentValue();
    }
    job_total_memory_mb = absl::GetFlag(FLAGS_ray_job_total_memory_mb);
    max_job_total_memory_mb = absl::GetFlag(FLAGS_ray_job_max_total_memory_mb);
  }
  worker_type = config.is_worker ? WorkerType::WORKER : WorkerType::DRIVER;
  if (worker_type == WorkerType::DRIVER && run_mode == RunMode::CLUSTER) {
    if (redis_ip.empty()) {
      auto ray_address_env = std::getenv("RAY_ADDRESS");
      if (ray_address_env) {
        RAY_LOG(DEBUG) << "Initialize Ray cluster address to \"" << ray_address_env
                       << "\" from environment variable \"RAY_ADDRESS\".";
        SetRedisAddress(ray_address_env);
      }
    }
    if (code_search_path.empty()) {
      auto program_path = boost::dll::program_location().parent_path();
      RAY_LOG(INFO) << "No code search path found yet. "
                    << "The program location path " << program_path
                    << " will be added for searching dynamic libraries by default."
                    << " And you can add some search paths by '--ray_code_search_path'";
      code_search_path.emplace_back(program_path.string());
    } else {
      // Convert all the paths to absolute path to support configuring relative paths in
      // driver.
      std::vector<std::string> absolute_path;
      for (const auto &path : code_search_path) {
        absolute_path.emplace_back(boost::filesystem::absolute(path).string());
      }
      code_search_path = absolute_path;
    }
  }
  if (worker_type == WorkerType::DRIVER) {
    ray_namespace = config.ray_namespace;
    if (!FLAGS_ray_job_namespace.CurrentValue().empty()) {
      ray_namespace = FLAGS_ray_job_namespace.CurrentValue();
    }
    if (ray_namespace.empty()) {
      ray_namespace = GenerateUUIDV4();
    }
  }
};

void ConfigInternal::SetRedisAddress(const std::string address) {
  auto pos = address.find(':');
  RAY_CHECK(pos != std::string::npos);
  redis_ip = address.substr(0, pos);
  redis_port = std::stoi(address.substr(pos + 1, address.length()));
}

void ConfigInternal::UpdateSessionDir(const std::string dir) {
  if (session_dir.empty()) {
    session_dir = dir;
  }
  if (logs_dir.empty()) {
    logs_dir = session_dir + "/logs";
  }
}

}  // namespace internal
}  // namespace ray
