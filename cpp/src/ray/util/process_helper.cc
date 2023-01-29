#include "process_helper.h"

#include "cpp/src/ray/config_internal.h"
#include "hiredis/hiredis.h"
#include "nlohmann/json.hpp"
#include "process_helper.h"
#include "ray/util/process.h"
#include "ray/util/resource_util.h"
#include "ray/util/util.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace internal {

static std::string GetSessionDir(std::string redis_ip, int port, std::string password) {
  redisContext *context = redisConnect(redis_ip.c_str(), port);
  RAY_CHECK(context != nullptr && !context->err);
  if (!password.empty()) {
    auto auth_reply = (redisReply *)redisCommand(context, "AUTH %s", password.c_str());
    RAY_CHECK(auth_reply->type != REDIS_REPLY_ERROR);
    freeReplyObject(auth_reply);
  }
  auto reply = (redisReply *)redisCommand(context, "GET session_dir");
  RAY_CHECK(reply->type != REDIS_REPLY_ERROR);
  std::string session_dir(reply->str);
  freeReplyObject(reply);
  redisFree(context);
  return session_dir;
}

void ProcessHelper::StartRayNode(const int redis_port, const std::string redis_password,
                                 const std::vector<std::string> &head_args) {
  std::vector<std::string> cmdargs({"ray", "start", "--head", "--port",
                                    std::to_string(redis_port), "--redis-password",
                                    redis_password});
  if (!head_args.empty()) {
    cmdargs.insert(cmdargs.end(), head_args.begin(), head_args.end());
  }
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  auto spawn_result = Process::Spawn(cmdargs, true);
  RAY_CHECK(!spawn_result.second);
  spawn_result.first.Wait();
  return;
}

void ProcessHelper::StopRayNode() {
  std::vector<std::string> cmdargs({"ray", "stop"});
  RAY_LOG(INFO) << CreateCommandLine(cmdargs);
  auto spawn_result = Process::Spawn(cmdargs, true);
  RAY_CHECK(!spawn_result.second);
  spawn_result.first.Wait();
  return;
}

std::unique_ptr<ray::gcs::GlobalStateAccessor> ProcessHelper::CreateGlobalStateAccessor(
    const std::string &redis_address, const std::string &redis_password) {
  auto global_state_accessor =
      std::make_unique<ray::gcs::GlobalStateAccessor>(redis_address, redis_password);
  RAY_CHECK(global_state_accessor->Connect()) << "Failed to connect to GCS.";
  return global_state_accessor;
}

void ProcessHelper::AddJobWorkerEnvToJobConfig(rpc::JobConfig &job_config,
                                               const std::string &job_worker_env) {
  if (job_worker_env.empty()) {
    return;
  }
  auto mutable_worker_env = job_config.mutable_worker_env();
  auto json = nlohmann::json::parse(job_worker_env);
  for (const auto &el : json.items()) {
    (*mutable_worker_env)[el.key()] = el.value();
  }
}

void ProcessHelper::RayStart(CoreWorkerOptions::TaskExecutionCallback callback) {
  std::string redis_ip = ConfigInternal::Instance().redis_ip;
  if (ConfigInternal::Instance().worker_type == ray::WorkerType::DRIVER &&
      redis_ip.empty()) {
    redis_ip = "127.0.0.1";
    StartRayNode(ConfigInternal::Instance().redis_port,
                 ConfigInternal::Instance().redis_password,
                 ConfigInternal::Instance().head_args);
  }
  if (redis_ip == "127.0.0.1") {
    redis_ip = GetNodeIpAddress();
  }

  std::string redis_address =
      redis_ip + ":" + std::to_string(ConfigInternal::Instance().redis_port);
  std::string node_ip = ConfigInternal::Instance().node_ip_address;
  if (node_ip.empty()) {
    if (!ConfigInternal::Instance().redis_ip.empty()) {
      node_ip = GetNodeIpAddress(redis_address);
    } else {
      node_ip = GetNodeIpAddress();
    }
  }

  std::unique_ptr<ray::gcs::GlobalStateAccessor> global_state_accessor = nullptr;
  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER) {
    global_state_accessor = CreateGlobalStateAccessor(
        redis_address, ConfigInternal::Instance().redis_password);
    std::string node_to_connect;
    auto status =
        global_state_accessor->GetNodeToConnectForDriver(node_ip, &node_to_connect);
    RAY_CHECK_OK(status);
    ray::rpc::GcsNodeInfo node_info;
    node_info.ParseFromString(node_to_connect);
    ConfigInternal::Instance().raylet_socket_name = node_info.raylet_socket_name();
    ConfigInternal::Instance().plasma_store_socket_name =
        node_info.object_store_socket_name();
    ConfigInternal::Instance().node_manager_port =
        node_info.basic_gcs_node_info().node_manager_port();
  }
  RAY_CHECK(!ConfigInternal::Instance().raylet_socket_name.empty());
  RAY_CHECK(!ConfigInternal::Instance().plasma_store_socket_name.empty());
  RAY_CHECK(ConfigInternal::Instance().node_manager_port > 0);

  if (ConfigInternal::Instance().worker_type == WorkerType::DRIVER) {
    auto session_dir = GetSessionDir(redis_ip, ConfigInternal::Instance().redis_port,
                                     ConfigInternal::Instance().redis_password);
    ConfigInternal::Instance().UpdateSessionDir(session_dir);
  }
  gcs::GcsClientOptions gcs_options =
      gcs::GcsClientOptions(redis_ip, ConfigInternal::Instance().redis_port,
                            ConfigInternal::Instance().redis_password);

  CoreWorkerOptions options;
  options.worker_type = ConfigInternal::Instance().worker_type;
  options.language = Language::CPP;
  options.store_socket = ConfigInternal::Instance().plasma_store_socket_name;
  options.raylet_socket = ConfigInternal::Instance().raylet_socket_name;
  if (options.worker_type == WorkerType::DRIVER) {
    if (!ConfigInternal::Instance().job_id.empty()) {
      options.job_id = JobID::FromHex(ConfigInternal::Instance().job_id);
    } else {
      /// TODO(Guyang Song): Get next job id from core worker by GCS client.
      /// Random a number to avoid repeated job ids.
      /// The repeated job ids will lead to task hang when driver connects to a existing
      /// cluster more than once.
      std::srand(std::time(nullptr));
      options.job_id = JobID::FromInt(std::rand());
    }
  }
  options.gcs_options = gcs_options;
  options.enable_logging = true;
  options.log_dir = ConfigInternal::Instance().logs_dir;
  options.install_failure_signal_handler = true;
  options.node_ip_address = node_ip;
  options.node_manager_port = ConfigInternal::Instance().node_manager_port;
  options.raylet_ip_address = node_ip;
  options.driver_name = "cpp_worker";
  options.ref_counting_enabled = true;
  options.num_workers = 1;
  options.metrics_agent_port = -1;
  options.task_execution_callback = callback;
  rpc::JobConfig job_config;
  job_config.set_total_memory_units(ToMemoryUnits(
      ConfigInternal::Instance().job_total_memory_mb * 1024 * 1024, /*round_up=*/true));
  job_config.set_max_total_memory_units(
      ToMemoryUnits(ConfigInternal::Instance().max_job_total_memory_mb * 1024 * 1024,
                    /*round_up=*/true));
  // TODO: internal, the target actor could not get the right logging level,
  // but empty str. Will fix it in future. (WangTao)
  job_config.set_logging_level("INFO");
  job_config.set_default_actor_lifetime(
      ConfigInternal::Instance().default_actor_lifetime);
  job_config.set_serialized_runtime_env("{}");

  for (const auto &path : ConfigInternal::Instance().code_search_path) {
    job_config.add_code_search_path(path);
  }
  job_config.set_java_worker_process_default_memory_units(ToMemoryUnits(
      ConfigInternal::Instance().java_worker_process_default_memory_mb * 1024 * 1024));
  job_config.set_java_heap_fraction(ConfigInternal::Instance().java_heap_fraction);
  job_config.set_num_java_workers_per_process(
      ConfigInternal::Instance().num_java_workers_per_process);
  AddJobWorkerEnvToJobConfig(job_config, ConfigInternal::Instance().job_worker_env);
  job_config.set_ray_namespace(ConfigInternal::Instance().ray_namespace);
  std::string serialized_job_config;
  RAY_CHECK(job_config.SerializeToString(&serialized_job_config));
  options.serialized_job_config = serialized_job_config;
  CoreWorkerProcess::Initialize(options);
}

void ProcessHelper::RayStop() {
  CoreWorkerProcess::Shutdown(false);
  if (ConfigInternal::Instance().redis_ip.empty()) {
    StopRayNode();
  }
}

}  // namespace internal
}  // namespace ray
