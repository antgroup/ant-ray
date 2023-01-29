// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "ray/core_worker/core_worker.h"

#include <boost/filesystem.hpp>
#include <cctype>
#include <cstdlib>

#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "boost/fiber/all.hpp"
#include "brpc/builtin/common.h"  // For "SetProgramName()"
#include "brpc/rdma/rdma_helper.h"
#include "ray/common/asio/asio_util.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/constants.h"
#include "ray/common/ray_config.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/common/task/task_util.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/jvm_options_util.h"
#include "ray/core_worker/stats.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/worker_handler.h"
#include "ray/event/event.h"
#include "ray/gcs/gcs_client/service_based_gcs_client.h"
#include "ray/stats/stats.h"
#include "ray/util/process.h"
#include "ray/util/util.h"

namespace {

// Duration between internal book-keeping heartbeats.
const uint64_t kInternalHeartbeatMillis = 1000;

// Job name environment name.
const char *job_name_env = "ANTC_JOB_NAME";

using ActorLifetime = ray::rpc::JobConfig_ActorLifetime;

inline bool IsDetachedHelper(ActorLifetime actor_lifetime) {
  RAY_CHECK(actor_lifetime != ray::rpc::JobConfig_ActorLifetime_NONE);
  return actor_lifetime == ray::rpc::JobConfig_ActorLifetime_DETACHED;
}

ray::JobID GetProcessJobID(const ray::CoreWorkerOptions &options) {
  if (options.worker_type == ray::WorkerType::DRIVER) {
    RAY_CHECK(!options.job_id.IsNil());
  } else {
    RAY_CHECK(options.job_id.IsNil());
  }

  if (options.worker_type == ray::WorkerType::WORKER) {
    // For workers, the job ID is assigned by Raylet via an environment variable.
    const char *job_id_env = std::getenv(kEnvVarKeyJobId);
    RAY_CHECK(job_id_env);
    return ray::JobID::FromHex(job_id_env);
  }
  return options.job_id;
}

/// Initialize job data directory and set an environment variable.
void InitJobDataDir(JobID job_id) {
  const char *job_data_dir_base = std::getenv(kEnvVarKeyJobDataDirBase);
  namespace fs = boost::filesystem;
  fs::path data_dir_base_path;
  if (job_data_dir_base == nullptr) {
    RAY_LOG(WARNING) << "Job data dir base not found.";
    data_dir_base_path = fs::path(kDefaultJobDataDirBase);
  } else {
    data_dir_base_path = fs::path(job_data_dir_base);
  }
  std::ostringstream ss;
  ss << getpid();
  fs::path pid_dir(ss.str());
  auto job_data_dir = data_dir_base_path / pid_dir;
  boost::system::error_code ec;
  auto check_result = fs::exists(job_data_dir, ec);
  if (check_result) {
    if (job_id.IsSubmittedFromDashboard()) {
      RAY_LOG(FATAL) << "Job data directory " << job_data_dir.string()
                     << " already exists. Exit and try another pid.";
    } else {
      RAY_LOG(WARNING) << "Job data directory " << job_data_dir.string()
                       << " already exists.";
    }
  }
}

}  // namespace

namespace ray {

/// The global instance of `CoreWorkerProcess`.
static std::unique_ptr<CoreWorkerProcess> core_worker_process;

thread_local std::weak_ptr<CoreWorker> CoreWorkerProcess::current_core_worker_;

void CoreWorkerProcess::Initialize(const CoreWorkerOptions &options) {
  RAY_CHECK(!core_worker_process)
      << "The process is already initialized for core worker.";
  core_worker_process =
      std::unique_ptr<CoreWorkerProcess>(new CoreWorkerProcess(options));
}

void CoreWorkerProcess::Shutdown(bool shutdown_with_error) {
  if (!core_worker_process) {
    return;
  }
  RAY_CHECK(core_worker_process->options_.worker_type == WorkerType::DRIVER)
      << "The `Shutdown` interface is for driver only.";
  RAY_CHECK(core_worker_process->global_worker_);
  if (shutdown_with_error) {
    core_worker_process->global_worker_->MarkShutdownWithError();
  }
  core_worker_process->global_worker_->Disconnect();
  core_worker_process->global_worker_->Shutdown();
  core_worker_process->RemoveWorker(core_worker_process->global_worker_);
  core_worker_process.reset();
}

bool CoreWorkerProcess::IsInitialized() { return core_worker_process != nullptr; }

std::unique_ptr<rpc::BrpcServer> CreateCoreWorkerServer(
    std::string name, const uint32_t port, boost::asio::io_service &io_service) {
  return std::unique_ptr<rpc::BrpcServer>(new rpc::BrpcServer(name, port));
}

CoreWorkerProcess::CoreWorkerProcess(const CoreWorkerOptions &options)
    : options_(options),
      global_worker_id_(
          options.worker_type == WorkerType::DRIVER
              ? ComputeDriverIdFromJob(options_.job_id)
              : (options_.num_workers == 1 ? WorkerID::FromRandom() : WorkerID::Nil())),
      shared_io_work_(shared_io_service_),
      worker_stats_(1000) {
  JobID process_job_id = GetProcessJobID(options_);
  if (options_.enable_logging) {
    std::stringstream app_name;
    app_name << LanguageString(options_.language) << "-core-"
             << WorkerTypeString(options_.worker_type);
    if (options.worker_type != rpc::WorkerType::SPILL_WORKER &&
        options.worker_type != rpc::WorkerType::RESTORE_WORKER) {
      app_name << "-" << process_job_id.Hex();
    }
    RayLog::StartRayLog(app_name.str(), RayLogLevel::INFO, options_.log_dir);
    if (options_.install_failure_signal_handler) {
      RayLog::InstallFailureSignalHandler();
    }
  } else {
    RAY_CHECK(options_.log_dir.empty())
        << "log_dir must be empty because ray log is disabled.";
    RAY_CHECK(!options_.install_failure_signal_handler)
        << "install_failure_signal_handler must be false because ray log is disabled.";
  }

  // ANT-INTERNAL
  rpc::ConfigureBrpcLogging();

  RAY_CHECK(options_.num_workers > 0);
  if (options_.worker_type == WorkerType::DRIVER) {
    // Driver process can only contain one worker.
    RAY_CHECK(options_.num_workers == 1);
  }

  RAY_LOG(INFO) << "Constructing CoreWorkerProcess. pid: " << getpid();

  // NOTE(kfstorm): any initialization depending on RayConfig must happen after this line.
  InitializeSystemConfig();

  // Begin to get gcs server address from raylet.
  gcs_server_address_updater_ = std::unique_ptr<GcsServerAddressUpdater>(
      new GcsServerAddressUpdater(options_.raylet_ip_address, options_.node_manager_port,
                                  [this](std::string ip, int port) {
                                    absl::MutexLock lock(&gcs_server_address_mutex_);
                                    gcs_server_address_.first = ip;
                                    gcs_server_address_.second = port;
                                  }));

  // Initialize job data dir.
  InitJobDataDir(process_job_id);

  // Initialize gcs client.
  // As the synchronous and the asynchronous context of redis client is not used in this
  // gcs client. We would not open connection for it by setting `enable_sync_conn` and
  // `enable_async_conn` as false.
  gcs::GcsClientOptions gcs_options = gcs::GcsClientOptions(
      options_.gcs_options.server_ip_, options_.gcs_options.server_port_,
      options_.gcs_options.password_,
      /*enable_sync_conn=*/false, /*enable_async_conn=*/false,
      /*enable_subscribe_conn=*/true);
  gcs_client_ = std::make_shared<ray::gcs::ServiceBasedGcsClient>(
      gcs_options, /*gcs_service_address_check_enable=*/true,
      [this](std::pair<std::string, int> *address) {
        absl::MutexLock lock(&gcs_server_address_mutex_);
        if (gcs_server_address_.second != 0) {
          address->first = gcs_server_address_.first;
          address->second = gcs_server_address_.second;
          return true;
        }
        return false;
      });
  RAY_CHECK_OK(gcs_client_->Connect(shared_io_service_));
  shared_actor_info_accessor_ = std::make_shared<SharedActorInfoAccessor>(gcs_client_);

  // Register a callback to monitor removed nodes.
  auto on_node_change = [this](const NodeID &node_id, const rpc::BasicGcsNodeInfo &data) {
    if (data.state() == rpc::BasicGcsNodeInfo::DEAD) {
      std::vector<std::shared_ptr<CoreWorker>> workers_copy;
      {
        absl::ReaderMutexLock lock(&worker_map_mutex_);
        for (auto &entry : workers_) {
          workers_copy.push_back(entry.second);
        }
      }

      for (auto &worker : workers_copy) {
        worker->OnNodeRemoved(node_id);
      }
    }
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeToNodeChange(on_node_change, nullptr));

  shared_io_thread_ = std::thread(&CoreWorkerProcess::RunIOService, this);

  // Start RPC server after all the task receivers are properly initialized.
  core_worker_server_ =
      CreateCoreWorkerServer(WorkerTypeString(options.worker_type),
                             0 /* let grpc/asio choose a port */, shared_io_service_);
  try {
    // The cast would throw std::bad_cast if it fails.
    rpc::BrpcServer &brpc_server = dynamic_cast<rpc::BrpcServer &>(*core_worker_server_);
    brpc_stream_service_ = std::unique_ptr<rpc::CoreWorkerBrpcStreamService>(
        new rpc::CoreWorkerBrpcStreamService(shared_io_service_, *this));
    brpc_server.RegisterStreamService(*brpc_stream_service_);
  } catch (const std::bad_cast &) {
    RAY_LOG(FATAL) << "Incorrect service type, BrpcServer expected";
  }
  core_worker_server_->Run();

  if (options_.num_workers == 1) {
    // We need to create the worker instance here if:
    // 1. This is a driver process. In this case, the driver is ready to use right after
    // the CoreWorkerProcess::Initialize.
    // 2. This is a Python worker process. In this case, Python will invoke some core
    // worker APIs before `CoreWorkerProcess::RunTaskExecutionLoop` is called. So we need
    // to create the worker instance here. One example of invocations is
    // https://github.com/ray-project/ray/blob/45ce40e5d44801193220d2c546be8de0feeef988/python/ray/worker.py#L1281.
    if (options_.worker_type == WorkerType::DRIVER ||
        options_.language == Language::PYTHON) {
      CreateWorker();
    }
  }

  // Assume stats module will be initialized exactly once in once process.
  // So it must be called in CoreWorkerProcess constructor and will be reused
  // by all of core worker.
  RAY_LOG(DEBUG) << "Stats setup in core worker.";

  // Initialize stats.
  // NOTE(lingxuan.zlx): We assume RayConfig is initialized before it's used.
  // RayConfig is generated in Java_io_ray_runtime_RayNativeRuntime_nativeInitialize
  // for java worker or in constructor of CoreWorker for python worker.
  ray::stats::TagsType global_tags = ray::stats::DefaultGlobalTags("core_worker");
  const char *antc_job_name = std::getenv(job_name_env);
  std::string job_name = antc_job_name == nullptr ? "" : std::string(antc_job_name);
  global_tags.push_back({ray::stats::JobNameKey, job_name});
  global_tags.push_back({ray::stats::JobIdKey, process_job_id.Hex()});
  global_tags.push_back(
      {ray::stats::WorkerTypeKey, WorkerTypeString(options_.worker_type)});
  global_tags.push_back({ray::stats::LanguageKey, rpc::Language_Name(options_.language)});
  ray::stats::Start(global_tags, options_.metrics_agent_port);

  if (RayConfig::instance().event_log_reporter_enabled()) {
    if (!options_.log_dir.empty()) {
      auto event_dir = options_.log_dir + "/events";
      ray::EventManager::Instance().AddReporter(std::make_shared<ray::LogEventReporter>(
          ray::rpc::Event_SourceType::Event_SourceType_CORE_WORKER, event_dir));
    }
  }

  // Record worker status.
  stats::WorkerStatus().Record(1);

#ifndef _WIN32
  // NOTE(kfstorm): std::atexit should be put at the end of `CoreWorkerProcess`
  // constructor. We assume that spdlog has been initialized before this line. When the
  // process is exiting, `HandleAtExit` will be invoked before destructing spdlog static
  // variables. We explicitly destruct `CoreWorkerProcess` instance in the callback to
  // ensure the static `CoreWorkerProcess` instance is destructed while spdlog is still
  // usable. This prevents crashing (or hanging) when using `RAY_LOG` in
  // `CoreWorkerProcess` destructor.
  RAY_CHECK(std::atexit(CoreWorkerProcess::HandleAtExit) == 0);
#endif
}

CoreWorkerProcess::~CoreWorkerProcess() {
  RAY_LOG(INFO) << "Destructing CoreWorkerProcess. pid: " << getpid();

  if (options_.worker_type == WorkerType::WORKER) {
    /// A separated thread to monitor the exiting progress.
    static std::thread monitor_exiting_progress_thread{[]() {
      constexpr uint32_t seconds_to_wait_process_exiting = 5;
      std::this_thread::sleep_for(std::chrono::seconds(seconds_to_wait_process_exiting));
      RAY_LOG(ERROR) << "Failed to exit this core worker process, "
                     << "so abort it forcefully. pid : " << getpid();

      /// If this program reaches this line, that means this core worker process has
      /// been exiting in at least 5 seconds. Use `_exit()` to force-kill it.
      _exit(-1);
      RAY_LOG(FATAL) << "It shouldn't reach here. pid : " << getpid();
    }};
    monitor_exiting_progress_thread.detach();
  }

  stats::WorkerStatus().Record(0);
  ray::EventManager::Instance().ClearReporters();
  // Shutdown stats module if worker process exits.
  RAY_LOG(DEBUG) << "Stats stop in core worker.";
  ray::stats::Shutdown();
  shared_io_service_.stop();
  shared_io_thread_.join();
  gcs_client_->Disconnect();
  gcs_server_address_updater_.reset();
  RAY_LOG(DEBUG) << "Finish Destructing CoreWorkerProcess";
  if (options_.enable_logging) {
    RayLog::ShutDownRayLog();
  }
}

void CoreWorkerProcess::EnsureInitialized() {
  RAY_CHECK(core_worker_process)
      << "The core worker process is not initialized yet or already "
      << "shutdown.";
}

void CoreWorkerProcess::HandleAtExit() { core_worker_process.reset(); }

void CoreWorkerProcess::InitializeSystemConfig() {
  // We have to create a short-time thread here because the RPC request to get the system
  // config from Raylet is asynchronous, and we need to synchronously initialize the
  // system config in the constructor of `CoreWorkerProcess`.
  std::promise<std::string> promise;
  std::thread thread([&] {
    instrumented_io_context io_service;
    boost::asio::io_service::work work(io_service);
    rpc::ClientCallManager client_call_manager(io_service);
    auto grpc_client = rpc::NodeManagerWorkerClient::make(
        options_.raylet_ip_address, options_.node_manager_port, client_call_manager);
    raylet::RayletClient raylet_client(grpc_client);

    std::function<void(int64_t)> get_once = [&get_once, &raylet_client, &promise,
                                             &io_service](int64_t num_attempts) {
      raylet_client.GetSystemConfig([num_attempts, &get_once, &promise, &io_service](
                                        const Status &status,
                                        const rpc::GetSystemConfigReply &reply) {
        RAY_LOG(DEBUG) << "Getting system config from raylet, remaining retries = "
                       << num_attempts;
        if (!status.ok()) {
          if (num_attempts <= 1) {
            RAY_LOG(FATAL) << "Failed to get the system config from Raylet: " << status;
          } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(
                RayConfig::instance().raylet_client_connect_timeout_milliseconds()));
            get_once(num_attempts - 1);
          }
        } else {
          promise.set_value(reply.system_config());
          io_service.stop();
        }
      });
    };

    get_once(RayConfig::instance().raylet_client_num_connect_attempts());
    io_service.run();
  });
  thread.join();

  RayConfig::instance().initialize(promise.get_future().get());
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::TryGetWorker(const WorkerID &worker_id) {
  if (!core_worker_process) {
    return nullptr;
  }
  absl::ReaderMutexLock workers_lock(&core_worker_process->worker_map_mutex_);
  auto it = core_worker_process->workers_.find(worker_id);
  if (it != core_worker_process->workers_.end()) {
    return it->second;
  }
  return nullptr;
}

CoreWorker &CoreWorkerProcess::GetCoreWorker() {
  EnsureInitialized();
  if (core_worker_process->options_.num_workers == 1) {
    RAY_CHECK(core_worker_process->global_worker_) << "global_worker_ must not be NULL";
    return *core_worker_process->global_worker_;
  }
  auto ptr = current_core_worker_.lock();
  RAY_CHECK(ptr != nullptr)
      << "The current thread is not bound with a core worker instance.";
  return *ptr;
}

void CoreWorkerProcess::SetCurrentThreadWorkerId(const WorkerID &worker_id) {
  EnsureInitialized();
  if (core_worker_process->options_.num_workers == 1) {
    RAY_CHECK(core_worker_process->global_worker_->GetWorkerID() == worker_id);
    return;
  }
  current_core_worker_ = core_worker_process->GetWorker(worker_id);
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::GetWorker(
    const WorkerID &worker_id) const {
  absl::ReaderMutexLock lock(&worker_map_mutex_);
  auto it = workers_.find(worker_id);
  RAY_CHECK(it != workers_.end()) << "Worker " << worker_id << " not found.";
  return it->second;
}

std::shared_ptr<CoreWorker> CoreWorkerProcess::CreateWorker() {
  auto worker_id =
      global_worker_id_ != WorkerID::Nil() ? global_worker_id_ : WorkerID::FromRandom();
  {
    // Update `worker_ids_` before actually creating the worker.
    absl::MutexLock lock(&worker_map_mutex_);
    worker_ids_.insert(worker_id);
  }
  auto worker = std::make_shared<CoreWorker>(
      options_, worker_id, gcs_client_, shared_actor_info_accessor_,
      core_worker_server_ ? core_worker_server_->GetPort() : 0, worker_stats_);
  RAY_LOG(INFO) << "Worker " << worker->GetWorkerID() << " is created.";
  if (options_.num_workers == 1) {
    global_worker_ = worker;
  }
  current_core_worker_ = worker;

  absl::MutexLock lock(&worker_map_mutex_);
  workers_.emplace(worker->GetWorkerID(), worker);
  RAY_CHECK(workers_.size() <= static_cast<size_t>(options_.num_workers));
  ray::stats::LogicWorkerCount().Record(workers_.size());
  return worker;
}

void CoreWorkerProcess::RemoveWorker(std::shared_ptr<CoreWorker> worker) {
  worker->WaitForShutdown();
  if (global_worker_) {
    RAY_CHECK(global_worker_ == worker);
  } else {
    RAY_CHECK(current_core_worker_.lock() == worker);
  }
  current_core_worker_.reset();
  {
    absl::MutexLock lock(&worker_map_mutex_);
    worker_ids_.erase(worker->GetWorkerID());
    workers_.erase(worker->GetWorkerID());
    ray::stats::LogicWorkerCount().Record(workers_.size());
    RAY_LOG(INFO) << "Removed worker " << worker->GetWorkerID();
  }
  if (global_worker_ == worker) {
    global_worker_ = nullptr;
  }
}

void CoreWorkerProcess::RunTaskExecutionLoop() {
  EnsureInitialized();
  RAY_CHECK(core_worker_process->options_.worker_type == WorkerType::WORKER);
  if (core_worker_process->options_.num_workers == 1) {
    if (core_worker_process->options_.profile_handler != nullptr) {
      InitWorkerProfiler(core_worker_process->options_.profile_handler,
                         [](std::function<void()> callback, uint32_t delay_ms) {
                           return execute_after(core_worker_process->shared_io_service_,
                                                callback, delay_ms);
                         });
    }
    // Run the task loop in the current thread only if the number of workers is 1.
    auto worker = core_worker_process->global_worker_
                      ? core_worker_process->global_worker_
                      : core_worker_process->CreateWorker();
    worker->RunTaskExecutionLoop();
    core_worker_process->RemoveWorker(worker);
  } else {
    std::vector<std::thread> worker_threads;
    for (int i = 0; i < core_worker_process->options_.num_workers; i++) {
      worker_threads.emplace_back([i] {
        SetThreadName("worker.task" + std::to_string(i));
        auto worker = core_worker_process->CreateWorker();
        worker->RunTaskExecutionLoop();
        core_worker_process->RemoveWorker(worker);
      });
    }
    for (auto &thread : worker_threads) {
      thread.join();
    }
  }

  core_worker_process.reset();
}

void CoreWorkerProcess::RunIOService() {
  BlockSignal();
  SetThreadName("worker.io");
  shared_io_service_.run();
}

#define DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD_TEMPLATE(METHOD, REQUEST_TYPE,          \
                                                         REPLY_TYPE)                    \
  void CoreWorkerProcess::Handle##METHOD(REQUEST_TYPE request, REPLY_TYPE reply,        \
                                         rpc::SendReplyCallback send_reply_callback) {  \
    auto worker_id = WorkerID::FromBinary(request.intended_worker_id());                \
    RAY_CHECK(!worker_id.IsNil());                                                      \
    std::shared_ptr<CoreWorker> core_worker;                                            \
    bool worker_id_exists = false;                                                      \
    {                                                                                   \
      absl::ReaderMutexLock lock(&worker_map_mutex_);                                   \
      auto it = workers_.find(worker_id);                                               \
      if (it != workers_.end()) {                                                       \
        core_worker = it->second;                                                       \
      }                                                                                 \
      worker_id_exists = worker_ids_.find(worker_id) != worker_ids_.end();              \
    }                                                                                   \
    if (core_worker == nullptr) {                                                       \
      if (worker_id_exists) {                                                           \
        RAY_LOG(INFO) << "Worker " << worker_id                                         \
                      << " is not ready yet. Deplay handling of this request.";         \
        /* The worker is still creating. Delay handling of this request. */             \
        auto timer = std::make_shared<boost::asio::deadline_timer>(shared_io_service_); \
        timer->expires_from_now(boost::posix_time::milliseconds(100));                  \
        timer->async_wait([this, timer, request, reply, send_reply_callback](           \
                              const boost::system::error_code &error) {                 \
          if (error == boost::asio::error::operation_aborted) {                         \
            return;                                                                     \
          }                                                                             \
          Handle##METHOD(request, reply, send_reply_callback);                          \
        });                                                                             \
        return;                                                                         \
      }                                                                                 \
      send_reply_callback(                                                              \
          Status::Invalid("Worker " + worker_id.Hex() + " is not found."), nullptr,     \
          nullptr);                                                                     \
    } else {                                                                            \
      std::dynamic_pointer_cast<rpc::CoreWorkerServiceHandler>(core_worker)             \
          ->Handle##METHOD(request, reply, send_reply_callback);                        \
    }                                                                                   \
  }

#define DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD(METHOD)                                  \
  DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD_TEMPLATE(METHOD, const rpc::METHOD##Request &, \
                                                   rpc::METHOD##Reply *)

RAY_CORE_WORKER_DECLARE_RPC_HANDLERS

#undef DECLARE_VOID_RPC_SERVICE_HANDLER_METHOD

CoreWorker::CoreWorker(
    const CoreWorkerOptions &options, const WorkerID &worker_id,
    std::shared_ptr<gcs::GcsClient> gcs_client,
    std::shared_ptr<SharedActorInfoAccessor> shared_actor_info_accessor,
    int core_worker_port, CoreWorkerStats &worker_stats)
    : options_(options),
      get_call_site_(RayConfig::instance().record_ref_creation_sites()
                         ? options_.get_lang_stack
                         : nullptr),
      worker_context_(options_.worker_type, worker_id, GetProcessJobID(options_)),
      io_work_(io_service_),
      reply_io_work_(reply_service_),
      client_call_manager_(
          new rpc::ClientCallManager(request_service_, reply_service_, io_service_)),
      periodical_runner_(io_service_),
      gcs_client_(gcs_client),
      shared_actor_info_accessor_(shared_actor_info_accessor),
      task_queue_length_(0),
      num_executed_tasks_(0),
      task_execution_service_work_(task_execution_service_),
      resource_ids_(new ResourceMappingType()),
      worker_stats_(worker_stats),
      request_io_work_(request_service_),
      core_worker_port_(core_worker_port),
      plasma_get_time_cost_(
          10000, [](double value) { ray::stats::PlasmaGetTimeCostP99().Record(value); },
          [](double value) { ray::stats::PlasmaGetTimeCostP95().Record(value); },
          [](double value) { ray::stats::PlasmaGetTimeCostP90().Record(value); }),
      memory_get_time_cost_(
          10000,
          [](double value) { ray::stats::MemoryStoreGetTimeCostP99().Record(value); },
          [](double value) { ray::stats::MemoryStoreGetTimeCostP95().Record(value); },
          [](double value) { ray::stats::MemoryStoreGetTimeCostP90().Record(value); }) {
  RAY_LOG(INFO) << "Constructing CoreWorker, worker_id: " << worker_id;

  // Initialize task receivers.
  if (options_.worker_type == WorkerType::WORKER || options_.is_local_mode) {
    RAY_CHECK(options_.task_execution_callback != nullptr);
    auto execute_task = std::bind(&CoreWorker::ExecuteTask, this, std::placeholders::_1,
                                  std::placeholders::_2, std::placeholders::_3,
                                  std::placeholders::_4, std::placeholders::_5);
    direct_task_receiver_ = std::make_unique<CoreWorkerDirectTaskReceiver>(
        worker_context_, task_execution_service_, execute_task,
        [this] { return local_raylet_client_->TaskDone(); });
  }

  // Initialize raylet client.
  // NOTE(edoakes): the core_worker_server_ must be running before registering with
  // the raylet, as the raylet will start sending some RPC messages immediately.
  // TODO(zhijunfu): currently RayletClient would crash in its constructor if it cannot
  // connect to Raylet after a number of retries, this can be changed later
  // so that the worker (java/python .etc) can retrieve and handle the error
  // instead of crashing.
  auto grpc_client = rpc::NodeManagerWorkerClient::make(
      options_.raylet_ip_address, options_.node_manager_port, *client_call_manager_);
  Status raylet_client_status;
  NodeID local_raylet_id;
  int assigned_port;
  std::string serialized_job_config = options_.serialized_job_config;
  local_raylet_client_ = std::make_shared<raylet::RayletClient>(
      io_service_, std::move(grpc_client), options_.raylet_socket, GetWorkerID(),
      options_.worker_type, worker_context_.GetCurrentJobID(), options_.runtime_env_hash,
      options_.language, options_.node_ip_address, &raylet_client_status,
      &local_raylet_id, &assigned_port, &serialized_job_config, options_.worker_shim_pid,
      &node_name_);
  if (!raylet_client_status.ok()) {
    // Avoid using FATAL log or RAY_CHECK here because they may create a core dump file.
    RAY_LOG(ERROR) << "Failed to register worker " << worker_id << " to Raylet. "
                   << raylet_client_status;
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // NOTE(hchen): Use `_Exit()` to force-exit this process without doing cleanup.
    // `exit()` will destruct static objects in an incorrect order, which will lead to
    // core dumps.
    _Exit(1);
  }

  connected_ = true;

  RAY_CHECK(assigned_port >= 0);

  // Parse job config from serialized string.
  job_config_.reset(new rpc::JobConfig());
  job_config_->ParseFromString(serialized_job_config);

  RAY_CHECK(assigned_port == 0)
      << "Core worker server has been started at process level. Assigning the port "
         "from Raylet is not allowed.";

  // ANT-INTERNAL: The `core_worker_server_` field in `CoreWorker` shouldn't be used
  // because we share the RPC server among `CoreWorker` instances.
  // core_worker_server_ = std::make_unique<rpc::GrpcServer>(
  //     WorkerTypeString(options_.worker_type), assigned_port);

  // Set our own address.
  RAY_CHECK(!local_raylet_id.IsNil());
  rpc_address_.set_ip_address(options_.node_ip_address);
  rpc_address_.set_port(core_worker_port_);
  rpc_address_.set_raylet_id(local_raylet_id.Binary());
  rpc_address_.set_worker_id(worker_context_.GetWorkerID().Binary());
  RAY_LOG(INFO) << "Initializing worker at address: " << rpc_address_.ip_address() << ":"
                << rpc_address_.port() << ", worker ID " << worker_context_.GetWorkerID()
                << ", raylet " << local_raylet_id;

  RegisterToGcs();

  task_stats_ = worker_stats_.AddTaskStats(GetWorkerID());
  running_task_stats_ = worker_stats_.GetRunningTaskStats();

  // Initialize profiler.
  profiler_ = std::make_shared<worker::Profiler>(
      worker_context_, options_.node_ip_address, io_service_, gcs_client_);

  core_worker_client_pool_ =
      std::make_shared<rpc::CoreWorkerClientPool>(*client_call_manager_);

  object_status_publisher_ = std::make_unique<pubsub::Publisher>(
      /*periodical_runner=*/&periodical_runner_,
      /*get_time_ms=*/[]() { return absl::GetCurrentTimeNanos() / 1e6; },
      /*subscriber_timeout_ms=*/RayConfig::instance().subscriber_timeout_ms(),
      /*publish_batch_size_=*/RayConfig::instance().publish_batch_size());
  object_status_subscriber_ = std::make_unique<pubsub::Subscriber>(
      /*subscriber_id=*/GetWorkerID(),
      /*subscriber_address=*/rpc_address_.ip_address(),
      /*subscriber_port=*/rpc_address_.port(),
      /*publisher_client_pool=*/*(core_worker_client_pool_.get()));

  reference_counter_ = std::make_shared<ReferenceCounter>(
      rpc_address_,
      /*object_status_publisher=*/object_status_publisher_.get(),
      /*object_status_subscriber=*/object_status_subscriber_.get(),
      RayConfig::instance().distributed_ref_counting_enabled(),
      RayConfig::instance().lineage_pinning_enabled(), [this](const rpc::Address &addr) {
        return std::shared_ptr<rpc::CoreWorkerClient>(
            new rpc::CoreWorkerClient(addr, *client_call_manager_));
      });

  if (options_.worker_type == ray::WorkerType::WORKER) {
    periodical_runner_.RunFnPeriodically(
        [this] { CheckForRayletFailure(); },
        RayConfig::instance().raylet_death_check_interval_milliseconds());
  }

  plasma_store_provider_.reset(new CoreWorkerPlasmaStoreProvider(
      options_.store_socket, local_raylet_client_, reference_counter_,
      options_.check_signals,
      /*warmup=*/
      (options_.worker_type != ray::WorkerType::SPILL_WORKER &&
       options_.worker_type != ray::WorkerType::RESTORE_WORKER &&
       options_.worker_type != ray::WorkerType::UTIL_WORKER),
      /*get_current_call_site=*/boost::bind(&CoreWorker::CurrentCallSite, this)));
  memory_store_stats_ = worker_stats_.AddMemoryStoreStats(GetWorkerID());
  memory_store_.reset(new CoreWorkerMemoryStore(
      [this](const RayObject &object, const ObjectID &object_id) {
        PutObjectIntoPlasma(object, object_id);
        return Status::OK();
      },
      options_.ref_counting_enabled ? reference_counter_ : nullptr, local_raylet_client_,
      options_.check_signals,
      [this](const RayObject &obj) {
        // Run this on the event loop to avoid calling back into the language runtime
        // from the middle of user operations.
        io_service_.post(
            [this, obj]() {
              if (options_.unhandled_exception_handler != nullptr) {
                options_.unhandled_exception_handler(obj);
              }
            },
            "CoreWorker.HandleException");
      },
      memory_store_stats_, options.object_allocator));
  periodical_runner_.RunFnPeriodically([this] { InternalHeartbeat(); },
                                       kInternalHeartbeatMillis);

  auto check_node_alive_fn = [this](const NodeID &node_id) {
    auto node = gcs_client_->Nodes().Get(node_id);
    return node != nullptr;
  };
  auto reconstruct_object_callback = [this](const ObjectID &object_id) {
    io_service_.post(
        [this, object_id]() {
          RAY_CHECK(object_recovery_manager_->RecoverObject(object_id));
        },
        "CoreWorker.ReconstructObject");
  };
  auto push_error_callback = [this](const JobID &job_id, const std::string &type,
                                    const std::string &error_message, double timestamp) {
    return PushError(job_id, type, error_message, timestamp);
  };
  task_manager_.reset(new TaskManager(
      memory_store_, reference_counter_,
      /* retry_task_callback= */
      [this](TaskSpecification &spec, bool delay) {
        if (delay) {
          // Retry after a delay to emulate the existing Raylet reconstruction
          // behaviour. TODO(ekl) backoff exponentially.
          uint32_t delay = RayConfig::instance().task_retry_delay_ms();
          RAY_LOG(ERROR) << "Will resubmit task after a " << delay
                         << "ms delay: " << spec.DebugString();
          absl::MutexLock lock(&mutex_);
          to_resubmit_.push_back(std::make_pair(current_time_ms() + delay, spec));
        } else {
          RAY_LOG(ERROR) << "Resubmitting task that produced lost plasma object: "
                         << spec.DebugString();
          if (spec.IsActorTask()) {
            auto actor_handle = actor_manager_->GetActorHandle(spec.ActorId());
            actor_handle->SetResubmittedActorTaskSpec(spec, spec.ActorDummyObject());
            direct_actor_submitter_->AppendTask(spec);
          } else {
            RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
          }
        }
      },
      check_node_alive_fn, reconstruct_object_callback, push_error_callback));

  // ANT-INTERNAL
  WarnAboutHangingTasksPeriodically();

  // Create an entry for the driver task in the task table. This task is
  // added immediately with status RUNNING. This allows us to push errors
  // related to this driver task back to the driver. For example, if the
  // driver creates an object that is later evicted, we should notify the
  // user that we're unable to reconstruct the object, since we cannot
  // rerun the driver.
  if (options_.worker_type == WorkerType::DRIVER) {
    TaskSpecBuilder builder;
    const TaskID task_id = TaskID::ForDriverTask(worker_context_.GetCurrentJobID());
    builder.SetDriverTaskSpec(task_id, options_.language,
                              worker_context_.GetCurrentJobID(),
                              TaskID::ComputeDriverTaskId(worker_context_.GetWorkerID()),
                              GetCallerId(), rpc_address_);

    std::shared_ptr<rpc::TaskTableData> data = std::make_shared<rpc::TaskTableData>();
    data->mutable_task()->mutable_task_spec()->CopyFrom(builder.Build().GetMessage());
    SetCurrentTaskId(task_id);
  }

  auto raylet_client_factory = [this](const std::string ip_address, int port) {
    auto grpc_client =
        rpc::NodeManagerWorkerClient::make(ip_address, port, *client_call_manager_);
    return std::shared_ptr<raylet::RayletClient>(
        new raylet::RayletClient(std::move(grpc_client)));
  };

  std::shared_ptr<ActorCreatorInterface> actor_creator =
      std::make_shared<DefaultActorCreator>(gcs_client_);

  // ANT-INTERNAL: Because the RPC client parameters are different, we can't use
  // `core_worker_client_pool_` for actor submitter.
  auto actor_client_pool =
      std::make_shared<rpc::CoreWorkerClientPool>([this](const rpc::Address &addr) {
        return std::make_shared<rpc::CoreWorkerClient>(addr, *client_call_manager_,
                                                       /*infinite_reconnect=*/true);
      });

  if (job_config_->actor_task_back_pressure_enabled()) {
    RAY_LOG(INFO) << "Core worker actor task backpressure enabled. max_pending_calls "
                  << job_config_->max_pending_calls();
  } else {
    RAY_LOG(INFO) << "Core worker actor task backpressure disabled.";
  }

  object_barrier_ =
      std::make_shared<ObjectBarrier>(io_service_, core_worker_client_pool_);

  direct_actor_submitter_ = std::shared_ptr<CoreWorkerDirectActorTaskSubmitter>(
      new CoreWorkerDirectActorTaskSubmitter(
          actor_client_pool, memory_store_, task_manager_, worker_context_,
          job_config_->actor_task_back_pressure_enabled(),
          job_config_->max_pending_calls()));

  auto node_addr_factory = [this](const NodeID &node_id) {
    absl::optional<rpc::Address> addr;
    if (auto node_info = gcs_client_->Nodes().Get(node_id)) {
      rpc::Address address;
      address.set_raylet_id(node_info->node_id());
      address.set_ip_address(node_info->node_manager_address());
      address.set_port(node_info->node_manager_port());
      addr = address;
    }
    return addr;
  };
  auto lease_policy = RayConfig::instance().locality_aware_leasing_enabled()
                          ? std::shared_ptr<LeasePolicyInterface>(
                                std::make_shared<LocalityAwareLeasePolicy>(
                                    reference_counter_, node_addr_factory, rpc_address_))
                          : std::shared_ptr<LeasePolicyInterface>(
                                std::make_shared<LocalLeasePolicy>(rpc_address_));
  direct_task_submitter_ = std::make_unique<CoreWorkerDirectTaskSubmitter>(
      rpc_address_, local_raylet_client_, core_worker_client_pool_, raylet_client_factory,
      std::move(lease_policy), memory_store_, task_manager_, local_raylet_id,
      RayConfig::instance().worker_lease_timeout_milliseconds(), std::move(actor_creator),
      worker_context_.GetCurrentJobID(),
      RayConfig::instance().max_tasks_in_flight_per_worker(),
      boost::asio::steady_timer(io_service_),
      RayConfig::instance().max_pending_lease_requests_per_scheduling_category());
  auto report_locality_data_callback =
      [this](const ObjectID &object_id, const absl::flat_hash_set<NodeID> &locations,
             uint64_t object_size) {
        reference_counter_->ReportLocalityData(object_id, locations, object_size);
      };
  future_resolver_.reset(new FutureResolver(memory_store_,
                                            std::move(report_locality_data_callback),
                                            core_worker_client_pool_, rpc_address_));
  // Unfortunately the raylet client has to be constructed after the receivers.
  if (direct_task_receiver_ != nullptr) {
    task_argument_waiter_.reset(new DependencyWaiterImpl(*local_raylet_client_));
    direct_task_receiver_->Init(core_worker_client_pool_, rpc_address_,
                                task_argument_waiter_, task_execution_service_);
  }

  actor_manager_ = std::unique_ptr<ActorManager>(
      new ActorManager(worker_id, gcs_client_, direct_actor_submitter_,
                       reference_counter_, shared_actor_info_accessor_));

  std::function<Status(const ObjectID &object_id, const ObjectLookupCallback &callback)>
      object_lookup_fn;

  if (RayConfig::instance().ownership_based_object_directory_enabled()) {
    object_lookup_fn = [this, node_addr_factory](const ObjectID &object_id,
                                                 const ObjectLookupCallback &callback) {
      std::vector<rpc::Address> locations;
      const absl::optional<absl::flat_hash_set<NodeID>> object_locations =
          reference_counter_->GetObjectLocations(object_id);
      if (object_locations.has_value()) {
        locations.reserve(object_locations.value().size());
        for (const auto &node_id : object_locations.value()) {
          absl::optional<rpc::Address> addr = node_addr_factory(node_id);
          if (addr.has_value()) {
            locations.push_back(addr.value());
          } else {
            // We're getting potentially stale locations directly from the reference
            // counter, so the location might be a dead node.
            RAY_LOG(DEBUG) << "Location " << node_id
                           << " is dead, not using it in the recovery of object "
                           << object_id;
          }
        }
      }
      callback(object_id, locations);
      return Status::OK();
    };
  } else {
    object_lookup_fn = [this](const ObjectID &object_id,
                              const ObjectLookupCallback &callback) {
      return gcs_client_->Objects().AsyncGetLocations(
          object_id, [this, object_id, callback](
                         const Status &status,
                         const boost::optional<rpc::ObjectLocationInfo> &result) {
            RAY_CHECK_OK(status);
            std::vector<rpc::Address> locations;
            for (const auto &loc : result->locations()) {
              const auto &node_id = NodeID::FromBinary(loc.manager());
              auto node = gcs_client_->Nodes().Get(node_id);
              RAY_CHECK(node != nullptr);
              rpc::Address address;
              address.set_raylet_id(node->node_id());
              address.set_ip_address(node->node_manager_address());
              address.set_port(node->node_manager_port());
              locations.push_back(address);
            }
            callback(object_id, locations);
          });
    };
  }
  object_recovery_manager_ = std::make_unique<ObjectRecoveryManager>(
      rpc_address_, raylet_client_factory, local_raylet_client_, object_lookup_fn,
      task_manager_, reference_counter_, memory_store_,
      [this](const ObjectID &object_id, bool pin_object) {
        RAY_CHECK_OK(Put(RayObject(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE),
                         /*contained_object_ids=*/{}, object_id,
                         /*pin_object=*/pin_object));
      },
      RayConfig::instance().lineage_pinning_enabled());

  request_io_thread_ = std::thread(&CoreWorker::RunRequestService, this);
  for (uint32_t i = 0; i < RayConfig::instance().worker_num_reply_io_threads(); i++) {
    reply_io_threads_.emplace_back(std::thread(&CoreWorker::RunReplyService, this));
  }

  // Start the IO thread after all other members have been initialized, in case
  // the thread calls back into any of our members.
  io_thread_ = std::thread([this]() { RunIOService(); });

  auto job_name_callback = [](rpc::Event &event) -> void {
    auto job_name = std::getenv(job_name_env);
    if (job_name != nullptr) {
      event.mutable_custom_fields()->insert({"job_name", std::getenv(job_name_env)});
    }
  };
  ::ray::RayEventContext::Instance().SetEventPostProcessor(job_name_callback);
  auto custom_fields = std::unordered_map<std::string, std::string>();
  custom_fields.emplace("node_id", local_raylet_id.Hex());
  custom_fields.emplace("job_id", GetProcessJobID(options_).Hex());
  ray::RayEventContext::Instance().SetEventContext(
      ray::rpc::Event_SourceType::Event_SourceType_CORE_WORKER, custom_fields);
  // ANT-INTERNAL
  ray::RayEventContext::Instance().SetLabelBlacklist(
      RayConfig::instance().event_label_blacklist());

  UpdateTaskStats();

  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  if (options.connect_on_start) {
    RAY_CHECK_OK(local_raylet_client_->AnnounceWorkerPort(core_worker_port_));
  }
  // Used to detect if the object is in the plasma store.
  max_direct_call_object_size_ = RayConfig::instance().max_direct_call_object_size();

  // Used to detect if the task returned object is in the plasma store.
  max_direct_call_returned_object_size_ =
      RayConfig::instance().max_direct_call_returned_object_size();
  if (max_direct_call_returned_object_size_ <= 0) {
    max_direct_call_returned_object_size_ =
        RayConfig::instance().max_direct_call_object_size();
  }

  RAY_LOG(INFO) << "The max direct call task returned object size: "
                << max_direct_call_returned_object_size_ << "(bytes)";

  // ANT-INTERNAL: The `core_worker_server_` field in `CoreWorker` shouldn't be used
  // because we share the RPC server among `CoreWorker` instances.
  RAY_CHECK(core_worker_server_ == nullptr);

  // Create global owner.
  if (job_config_->global_owner_number() > 0 &&
      options_.worker_type == WorkerType::DRIVER) {
    CreateHAObjectGlobalOwner();
  }
#ifdef BRPC_WITH_RDMA
RAY_LOG(DEBUG) << "Initialize RDMA environment in core_worker brpc client side";
  if(RayConfig::instance().use_rdma()){
    RAY_LOG(DEBUG) << "Initialize RDMA environment in core_worker brpc client side";
    ::brpc::rdma::GlobalRdmaInitializeOrDie();
  }
#endif
}

void CoreWorker::Shutdown() {
  // Cancel subscritions for this worker so that further actor notifications won't be
  // triggered, otherwise SIGSEGV would occur because CoreWorker has been destroyed.
  shared_actor_info_accessor_->OnWorkerShutdown(GetWorkerID());
  request_service_.stop();
  reply_service_.stop();
  io_service_.stop();
  // Shutdown the direct_actor_submitter_ to make IO service unblocked when backpressued
  // in rpc client.
  direct_actor_submitter_->Shutdown();
  if (options_.worker_type == WorkerType::WORKER) {
    task_execution_service_.stop();
  }
  if (task_stats_) {
    worker_stats_.RemoveTaskStats(GetWorkerID());
    // Don't reset `task_stats_` here. It may be accessed in IO thread after shutdown.
  }
  if (memory_store_stats_) {
    worker_stats_.RemoveMemoryStoreStats(GetWorkerID());
  }
  if (options_.on_worker_shutdown) {
    options_.on_worker_shutdown(GetWorkerID());
  }
}

void CoreWorker::MarkShutdownWithError() { has_shutdown_error_ = true; }

void CoreWorker::ConnectToRaylet() {
  RAY_CHECK(!options_.connect_on_start);
  // Tell the raylet the port that we are listening on.
  // NOTE: This also marks the worker as available in Raylet. We do this at the
  // very end in case there is a problem during construction.
  RAY_CHECK_OK(local_raylet_client_->AnnounceWorkerPort(core_worker_port_));
}

void CoreWorker::Disconnect(
    rpc::WorkerExitType exit_type,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  if (connected_) {
    connected_ = false;
    // If has_shutdown_error_ is true, then we don't disconnect with Raylet explicitly
    // and Raylet will treat the disconnection as unintentionally.
    if (local_raylet_client_ && !has_shutdown_error_) {
      RAY_IGNORE_EXPR(
          local_raylet_client_->Disconnect(exit_type, creation_task_exception_pb_bytes));
    }
  }
}

void CoreWorker::Exit(
    rpc::WorkerExitType exit_type,
    const std::shared_ptr<LocalMemoryBuffer> &creation_task_exception_pb_bytes) {
  RAY_LOG(INFO) << "Exit signal received, this process will exit after all outstanding "
                   "tasks have finished"
                << ", exit_type=" << rpc::WorkerExitType_Name(exit_type);
  exiting_ = true;
  // Release the resources early in case draining takes a long time.
  RAY_CHECK_OK(
      local_raylet_client_->NotifyDirectCallTaskBlocked(/*release_resources*/ true));

  RAY_LOG(DEBUG) << "Exit signal received, remove all local references.";
  /// Since this core worker is exiting, it's necessary to release all local references,
  /// otherwise the frontend code may not release its references and this worker will be
  /// leaked. See https://github.com/ray-project/ray/issues/19639.
  reference_counter_->ReleaseAllLocalReferences();
  // Callback to shutdown.
  auto shutdown = [this, exit_type, creation_task_exception_pb_bytes]() {
    // To avoid problems, make sure shutdown is always called from the same
    // event loop each time.
    task_execution_service_.post(
        [this, exit_type, creation_task_exception_pb_bytes]() {
          if (exit_type == rpc::WorkerExitType::CREATION_TASK_ERROR ||
              exit_type == rpc::WorkerExitType::INTENDED_EXIT) {
            // Notify the raylet about this exit.
            // Only CREATION_TASK_ERROR and INTENDED_EXIT needs to disconnect
            // manually.
            Disconnect(exit_type, creation_task_exception_pb_bytes);
          }
          Shutdown();
        },
        "CoreWorker.Shutdown");
  };
  // Callback to drain objects once all pending tasks have been drained.
  auto drain_references_callback = [this, shutdown]() {
    // Post to the event loop to avoid a deadlock between the TaskManager and
    // the ReferenceCounter. The deadlock can occur because this callback may
    // get called by the TaskManager while the ReferenceCounter's lock is held,
    // but the callback itself must acquire the ReferenceCounter's lock to
    // drain the object references.
    task_execution_service_.post(
        [this, shutdown]() {
          bool not_actor_task = false;
          {
            absl::MutexLock lock(&mutex_);
            not_actor_task = actor_id_.IsNil();
          }
          if (not_actor_task) {
            // If we are a task, then we cannot hold any object references in the
            // heap. Therefore, any active object references are being held by other
            // processes. Wait for these processes to release their references
            // before we shutdown. NOTE(swang): This could still cause this worker
            // process to stay alive forever if another process holds a reference
            // forever.
            reference_counter_->DrainAndShutdown(shutdown);
          } else {
            // If we are an actor, then we may be holding object references in the
            // heap. Then, we should not wait to drain the object references before
            // shutdown since this could hang.
            shutdown();
          }
        },
        "CoreWorker.DrainAndShutdown");
  };

  task_manager_->DrainAndShutdown(drain_references_callback);
}

void CoreWorker::RunIOService() {
#ifndef _WIN32
  // Block SIGINT and SIGTERM so they will be handled by the main thread.
  sigset_t mask;
  sigemptyset(&mask);
  sigaddset(&mask, SIGINT);
  sigaddset(&mask, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &mask, NULL);
#endif
  SetThreadName("worker.io");
  BlockSignal();
  io_service_.run();
}

void CoreWorker::OnNodeRemoved(const NodeID &node_id) {
  RAY_LOG(INFO) << "Node failure " << node_id;
  const auto lost_objects = reference_counter_->ResetObjectsOnRemovedNode(node_id);
  // Delete the objects from the in-memory store to indicate that they are not
  // available. The object recovery manager will guarantee that a new value
  // will eventually be stored for the objects (either an
  // UnreconstructableError or a value reconstructed from lineage).
  memory_store_->Delete(lost_objects);
  for (const auto &object_id : lost_objects) {
    RAY_LOG(INFO) << "Object " << object_id << " lost due to node failure " << node_id;
    // NOTE(swang): There is a race condition where this can return false if
    // the reference went out of scope since the call to the ref counter to get
    // the lost objects. It's okay to not mark the object as failed or recover
    // the object since there are no reference holders.
    static_cast<void>(object_recovery_manager_->RecoverObject(object_id));
  }
}

void CoreWorker::WaitForShutdown() {
  direct_actor_submitter_->WaitForShutdown();
  if (request_io_thread_.joinable()) {
    request_io_thread_.join();
  }
  for (auto &t : reply_io_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  if (io_thread_.joinable()) {
    io_thread_.join();
  }
  if (options_.worker_type == WorkerType::WORKER) {
    RAY_CHECK(task_execution_service_.stopped());
    // Asyncio coroutines could still run after CoreWorker is removed because it is
    // running in a different thread. This can cause segfault because coroutines try to
    // access CoreWorker methods that are already garbage collected. We should complete
    // all coroutines before shutting down in order to prevent this.
    if (worker_context_.CurrentActorIsAsync()) {
      options_.terminate_asyncio_thread();
    }
  }
}

const WorkerID &CoreWorker::GetWorkerID() const { return worker_context_.GetWorkerID(); }

void CoreWorker::SetCurrentTaskId(const TaskID &task_id) {
  worker_context_.SetCurrentTaskId(task_id);
  {
    absl::MutexLock lock(&mutex_);
    main_thread_task_id_ = task_id;
  }
}

void CoreWorker::RegisterToGcs() {
  std::unordered_map<std::string, std::string> worker_info;
  const auto &worker_id = GetWorkerID();
  worker_info.emplace("node_ip_address", options_.node_ip_address);
  worker_info.emplace("plasma_store_socket", options_.store_socket);
  worker_info.emplace("raylet_socket", options_.raylet_socket);

  if (options_.worker_type == WorkerType::DRIVER) {
    auto start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    worker_info.emplace("driver_id", worker_id.Binary());
    worker_info.emplace("start_time", std::to_string(start_time));
    if (!options_.driver_name.empty()) {
      worker_info.emplace("name", options_.driver_name);
    }
  }

  if (!options_.stdout_file.empty()) {
    worker_info.emplace("stdout_file", options_.stdout_file);
  }
  if (!options_.stderr_file.empty()) {
    worker_info.emplace("stderr_file", options_.stderr_file);
  }

  auto worker_data = std::make_shared<rpc::WorkerTableData>();
  worker_data->mutable_worker_address()->set_worker_id(worker_id.Binary());
  worker_data->set_worker_type(options_.worker_type);
  worker_data->mutable_worker_info()->insert(worker_info.begin(), worker_info.end());
  worker_data->set_is_alive(true);

  RAY_CHECK_OK(gcs_client_->Workers().AsyncAdd(worker_data, nullptr));
}

void CoreWorker::CheckForRayletFailure() {
  // When running worker process in container, the worker parent process is not raylet.
  // So we add RAY_RAYLET_PID enviroment to ray worker process.
  bool should_shutdown = false;
  if (const char *env_pid = std::getenv("RAY_RAYLET_PID")) {
    pid_t pid = static_cast<pid_t>(std::atoi(env_pid));
    if (!IsProcessAlive(pid)) {
      should_shutdown = true;
    }
  } else if (!IsParentProcessAlive()) {
    should_shutdown = true;
  }

  if (should_shutdown) {
    RAY_LOG(WARNING) << "Shutting down the core worker because the local raylet failed. "
                     << "Check out the raylet.out log file.";
    QuickExit();
  }
}

void CoreWorker::InternalHeartbeat() {
  // Retry tasks.
  std::vector<TaskSpecification> tasks_to_resubmit;
  {
    absl::MutexLock lock(&mutex_);
    while (!to_resubmit_.empty() && current_time_ms() > to_resubmit_.front().first) {
      tasks_to_resubmit.push_back(std::move(to_resubmit_.front().second));
      to_resubmit_.pop_front();
    }
  }

  for (auto &spec : tasks_to_resubmit) {
    if (spec.IsActorTask()) {
      direct_actor_submitter_->AppendTask(spec);
    } else {
      RAY_CHECK_OK(direct_task_submitter_->SubmitTask(spec));
    }
  }

  // Check timeout tasks that are waiting for death info.
  if (direct_actor_submitter_ != nullptr) {
    direct_actor_submitter_->CheckTimeoutTasks();
  }

  // Check for unhandled exceptions to raise after a timeout on the driver.
  // Only do this for TTY, since shells like IPython sometimes save references
  // to the result and prevent normal result deletion from handling.
  // See also: https://github.com/ray-project/ray/issues/14485
  if (options_.worker_type == WorkerType::DRIVER && options_.interactive) {
    memory_store_->NotifyUnhandledErrors();
  }
}

Status CoreWorker::ValidatePlacementGroupBundleIndex(
    const rpc::SchedulingStrategy &scheduling_strategy) {
  if (scheduling_strategy.scheduling_strategy_case() !=
      rpc::SchedulingStrategy::SchedulingStrategyCase::
          kPlacementGroupSchedulingStrategy) {
    return Status::OK();
  }
  auto placement_group_id = PlacementGroupID::FromBinary(
      scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
  auto bundle_index = scheduling_strategy.placement_group_scheduling_strategy()
                          .placement_group_bundle_index();
  if (!placement_group_id.IsNil() && bundle_index != -1) {
    const auto &all_bundle_indexes =
        GetAllAndSubscribePlacementGroupBundleEvent(placement_group_id);
    if (RAY_LOG_ENABLED(DEBUG)) {
      std::ostringstream debug_info;
      debug_info << "Got the latest bundles view: ";
      for (const auto &bundle_view : all_bundle_indexes) {
        debug_info << "{" << bundle_view.first << ":" << bundle_view.second << "},";
      }
      debug_info << " from placement group: " << placement_group_id;
      RAY_LOG(DEBUG) << debug_info.str();
    }

    const auto bundle_pos =
        std::find_if(all_bundle_indexes.begin(), all_bundle_indexes.end(),
                     [bundle_index](const auto &index) {
                       return index.second && index.first == bundle_index;
                     });
    if (bundle_pos == all_bundle_indexes.end()) {
      std::vector<int> valid_bundle_indexes;
      for (const auto &index : all_bundle_indexes) {
        if (index.second) {
          valid_bundle_indexes.push_back(index.first);
        }
      }
      std::ostringstream valid_bundle_indexes_str;
      std::copy(valid_bundle_indexes.begin(), valid_bundle_indexes.end(),
                std::ostream_iterator<int>(valid_bundle_indexes_str, ","));
      std::ostringstream error_msg;
      error_msg << "Invalid bundle index: " << bundle_index << " in ["
                << valid_bundle_indexes_str.str()
                << "] when adding resource constraint for placement group: "
                << placement_group_id;
      return Status::Invalid(error_msg.str());
    }
  }
  return Status::OK();
}

std::unordered_map<ObjectID, std::pair<size_t, size_t>>
CoreWorker::GetAllReferenceCounts() const {
  auto counts = reference_counter_->GetAllReferenceCounts();
  std::vector<ObjectID> actor_handle_ids = actor_manager_->GetActorHandleIDsFromHandles();
  // Strip actor IDs from the ref counts since there is no associated ObjectID
  // in the language frontend.
  for (const auto &actor_handle_id : actor_handle_ids) {
    counts.erase(actor_handle_id);
  }
  return counts;
}

void CoreWorker::PutObjectIntoPlasma(const RayObject &object, const ObjectID &object_id) {
  bool object_exists;
  // This call will only be used by PromoteObjectToPlasma, which means that the
  // object will always owned by us.
  RAY_CHECK_OK(plasma_store_provider_->Put(
      object, object_id, /* owner_address = */ rpc_address_, &object_exists));
  if (!object_exists) {
    // Tell the raylet to pin the object **after** it is created.
    RAY_LOG(DEBUG) << "Pinning put object " << object_id;
    local_raylet_client_->PinObjectIDs(
        rpc_address_, {object_id},
        [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
          // Only release the object once the raylet has responded to avoid the race
          // condition that the object could be evicted before the raylet pins it.
          if (!plasma_store_provider_->Release(object_id).ok()) {
            RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                           << "), might cause a leak in plasma.";
          }
        });
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
}

void CoreWorker::PromoteObjectToPlasma(const ObjectID &object_id) {
  auto value = memory_store_->GetOrPromoteToPlasma(object_id);
  if (value) {
    PutObjectIntoPlasma(*value, object_id);
  }
}

const rpc::Address &CoreWorker::GetRpcAddress() const { return rpc_address_; }

rpc::Address CoreWorker::GetOwnerAddress(const ObjectID &object_id) const {
  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be passed as a task argument because Ray "
         "does not know which task will create them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/";
  return owner_address;
}

void CoreWorker::GetOwnershipInfo(const ObjectID &object_id,
                                  rpc::Address *owner_address) {
  auto has_owner = reference_counter_->GetOwner(object_id, owner_address);
  RAY_CHECK(has_owner)
      << "Object IDs generated randomly (ObjectID.from_random()) or out-of-band "
         "(ObjectID.from_binary(...)) cannot be serialized because Ray does not know "
         "which task will create them. "
         "If this was not how your object ID was generated, please file an issue "
         "at https://github.com/ray-project/ray/issues/: "
      << object_id;
  RAY_LOG(DEBUG) << "Promoted object to plasma " << object_id;
}

void CoreWorker::RegisterOwnershipInfoAndResolveFuture(
    const ObjectID &object_id, const ObjectID &outer_object_id,
    const rpc::Address &owner_address) {
  // Add the object's owner to the local metadata in case it gets serialized
  // again.
  reference_counter_->AddBorrowedObject(object_id, outer_object_id, owner_address);

  // We will ask the owner about the object until the object is
  // created or we can no longer reach the owner.
  future_resolver_->ResolveFutureAsync(object_id, owner_address);
}

Status CoreWorker::SetClientOptions(std::string name, int64_t limit_bytes) {
  // Currently only the Plasma store supports client options.
  return plasma_store_provider_->SetClientOptions(name, limit_bytes);
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       ObjectID *object_id) {
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentTaskID(),
                                   worker_context_.GetNextPutIndex());
  reference_counter_->AddOwnedObject(
      *object_id, contained_object_ids, rpc_address_, CurrentCallSite(), object.GetSize(),
      /*is_reconstructable=*/false, NodeID::FromBinary(rpc_address_.raylet_id()));
  auto status = Put(object, contained_object_ids, *object_id, /*pin_object=*/true);
  if (!status.ok()) {
    reference_counter_->RemoveOwnedObject(*object_id);
  }
  return status;
}

Status CoreWorker::Put(const RayObject &object,
                       const std::vector<ObjectID> &contained_object_ids,
                       const ObjectID &object_id, bool pin_object) {
  bool object_exists;
  if (options_.is_local_mode ||
      (RayConfig::instance().put_small_object_in_memory_store() &&
       static_cast<int64_t>(object.GetSize()) < max_direct_call_object_size_)) {
    RAY_LOG(DEBUG) << "Put " << object_id << " in memory store";
    RAY_CHECK(memory_store_->Put(object, object_id));
    return Status::OK();
  }
  RAY_RETURN_NOT_OK(plasma_store_provider_->Put(
      object, object_id, /* owner_address = */ rpc_address_, &object_exists));
  if (!object_exists) {
    if (pin_object) {
      // Tell the raylet to pin the object **after** it is created.
      RAY_LOG(DEBUG) << "Pinning put object " << object_id;
      local_raylet_client_->PinObjectIDs(
          rpc_address_, {object_id},
          [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
            // Only release the object once the raylet has responded to avoid the race
            // condition that the object could be evicted before the raylet pins it.
            if (!plasma_store_provider_->Release(object_id).ok()) {
              RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                             << "), might cause a leak in plasma.";
            }
          });
    } else {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    }
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  return Status::OK();
}

Status CoreWorker::CreateOwned(const std::shared_ptr<Buffer> &metadata,
                               const size_t data_size,
                               const std::vector<ObjectID> &contained_object_ids,
                               ObjectID *object_id, std::shared_ptr<Buffer> *data,
                               const std::shared_ptr<rpc::Address> owner_address) {
  if (options_.is_local_mode) {
    RAY_CHECK(owner_address == nullptr)
        << "Specifying owner in `ray.put` is not allowed when running in local mode.";
  }
  *object_id = ObjectID::FromIndex(worker_context_.GetCurrentTaskID(),
                                   worker_context_.GetNextPutIndex());
  rpc::Address real_owner_address =
      owner_address != nullptr ? *owner_address : rpc_address_;
  bool owned_by_us = WorkerID::FromBinary(real_owner_address.worker_id()) ==
                     WorkerID::FromBinary(rpc_address_.worker_id());
  if (owned_by_us) {
    reference_counter_->AddOwnedObject(*object_id, contained_object_ids, rpc_address_,
                                       CurrentCallSite(), data_size + metadata->Size(),
                                       /*is_reconstructable=*/false,
                                       NodeID::FromBinary(rpc_address_.raylet_id()));
  } else {
    // Because in the remote worker's `HandleBatchAssignObjectOwner`,
    // a `WaitForRefRemoved` RPC request will be sent back to
    // the current worker. So we need to make sure ref count is > 0
    // by invoking `AddLocalReference` first. Note that in worker.py we set
    // skip_adding_local_ref=True to avoid double referencing the object.
    AddLocalReference(*object_id);
    RAY_UNUSED(reference_counter_->AddBorrowedObject(*object_id, ObjectID::Nil(),
                                                     real_owner_address));
  }

  if ((options_.is_local_mode ||
       (RayConfig::instance().put_small_object_in_memory_store() &&
        static_cast<int64_t>(data_size) < max_direct_call_object_size_)) &&
      owned_by_us) {
    *data = std::make_shared<LocalMemoryBuffer>(data_size);
  } else {
    auto status =
        plasma_store_provider_->Create(metadata, data_size, *object_id,
                                       /* owner_address = */ real_owner_address, data);
    if (!status.ok() || !data) {
      if (owned_by_us) {
        reference_counter_->RemoveOwnedObject(*object_id);
      } else {
        RemoveLocalReference(*object_id);
      }
      return status;
    }
  }

  if (!owned_by_us) {
    object_barrier_->AddAssignOwnerRequest(
        *object_id, real_owner_address, rpc_address_, contained_object_ids,
        CurrentCallSite(), data_size + metadata->Size(),
        // Avoid object_id modify.
        [real_owner_address, object_id_copy = ObjectID::FromBinary(object_id->Binary())](
            const Status &status) {
          if (status.ok()) return;
          RAY_LOG(ERROR) << "The Owner(" << real_owner_address.worker_id()
                         << ") is died, "
                         << " the object(" << object_id_copy << ") will be unavailable! "
                         << " Reply Status: " << status;
        });
  }

  return Status::OK();
}

Status CoreWorker::CreateExisting(const std::shared_ptr<Buffer> &metadata,
                                  const size_t data_size, const ObjectID &object_id,
                                  const rpc::Address &owner_address,
                                  std::shared_ptr<Buffer> *data) {
  if (options_.is_local_mode) {
    return Status::NotImplemented(
        "Creating an object with a pre-existing ObjectID is not supported in local "
        "mode");
  } else {
    return plasma_store_provider_->Create(metadata, data_size, object_id, owner_address,
                                          data);
  }
}

Status CoreWorker::SealOwned(const ObjectID &object_id, bool pin_object,
                             const std::shared_ptr<rpc::Address> owner_address) {
  bool owned_by_us = owner_address != nullptr
                         ? WorkerID::FromBinary(owner_address->worker_id()) ==
                               WorkerID::FromBinary(rpc_address_.worker_id())
                         : true;
  auto status = SealExisting(object_id, pin_object, owner_address);
  if (status.ok()) return status;
  if (owned_by_us) {
    reference_counter_->RemoveOwnedObject(object_id);
  } else {
    RemoveLocalReference(object_id);
  }
  return status;
}

Status CoreWorker::SealExisting(const ObjectID &object_id, bool pin_object,
                                const std::shared_ptr<rpc::Address> owner_address) {
  RAY_RETURN_NOT_OK(plasma_store_provider_->Seal(object_id));
  if (pin_object) {
    // Tell the raylet to pin the object **after** it is created.
    rpc::Address real_owner_address =
        owner_address != nullptr ? *owner_address : rpc_address_;
    // We need to pin the object after AssignOwner reply is done, if not,
    // WaitForObjectFree will delete the object.
    auto pin_object_callable = [this, object_id,
                                real_owner_address](const Status &status) {
      if (!status.ok()) {
        // If assign owner failed(the owner is died), will continue to pin current object,
        // and will delete this object's primary copy.
        RAY_LOG(ERROR) << "The Owner(" << real_owner_address.worker_id() << ") is died, "
                       << " the object(" << object_id << ") will be unavailable! "
                       << " Reply Status: " << status;
      }
      local_raylet_client_->PinObjectIDs(
          real_owner_address, {object_id},
          [this, object_id](const Status &status, const rpc::PinObjectIDsReply &reply) {
            // Only release the object once the raylet has responded to avoid the
            // race condition that the object could be evicted before the raylet
            // pins it.
            if (!plasma_store_provider_->Release(object_id).ok()) {
              RAY_LOG(ERROR) << "Failed to release ObjectID (" << object_id
                             << "), might cause a leak in plasma.";
            }
          });
    };
    io_service_.post(
        [this, object_id, pin_object_callable = std::move(pin_object_callable)]() {
          object_barrier_->AsyncWaitForAssignmentFinish(object_id,
                                                        std::move(pin_object_callable));
        },
        "CoreWorker.PinObjectIDs");
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Release(object_id));
    reference_counter_->FreePlasmaObjects({object_id});
  }
  RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  return Status::OK();
}

Status CoreWorker::Get(const std::vector<ObjectID> &ids, const int64_t timeout_ms,
                       std::vector<std::shared_ptr<RayObject>> *results,
                       bool plasma_objects_only) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  bool got_exception = false;
  bool fetched_local = false;
  uint64_t fetched_local_count = 0;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  auto start_time = current_time_ms();

  if (!plasma_objects_only) {
    if (!memory_object_ids.empty()) {
      RAY_RETURN_NOT_OK(memory_store_->Get(memory_object_ids, timeout_ms, worker_context_,
                                           &result_map, &got_exception));
    }

    // Erase any objects that were promoted to plasma from the results. These get
    // requests will be retried at the plasma store.
    for (auto it = result_map.begin(); it != result_map.end();) {
      auto current = it++;
      if (current->second->IsInPlasmaError()) {
        RAY_LOG(DEBUG) << current->first << " in plasma, doing fetch-and-get";
        plasma_object_ids.insert(current->first);
        result_map.erase(current);
      }
    }
  } else {
    plasma_object_ids = std::move(memory_object_ids);
  }

  if (!got_exception) {
    // If any of the objects have been promoted to plasma, then we retry their
    // gets at the provider plasma. Once we get the objects from plasma, we flip
    // the transport type again and return them for the original direct call ids.
    int64_t local_timeout_ms = timeout_ms;
    if (timeout_ms >= 0) {
      local_timeout_ms = std::max(static_cast<int64_t>(0),
                                  timeout_ms - (current_time_ms() - start_time));
    }
    RAY_LOG(DEBUG) << "Plasma GET timeout " << local_timeout_ms;
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(plasma_object_ids, local_timeout_ms,
                                                  worker_context_, &result_map,
                                                  &got_exception, &fetched_local));
  }

  if (plasma_object_ids.empty()) {
    fetched_local_count += memory_object_ids.size();
    auto plasma_get_time = current_time_ms() - start_time;
    memory_get_time_cost_.Record(plasma_get_time);
  } else {
    fetched_local_count += memory_object_ids.size();
    auto plasma_get_time = current_time_ms() - start_time;
    plasma_get_time_cost_.Record(plasma_get_time);
  }

  memory_store_stats_->UpdateLocalObjectHitRate(fetched_local_count, ids.size());

  // Loop through `ids` and fill each entry for the `results` vector,
  // this ensures that entries `results` have exactly the same order as
  // they are in `ids`. When there are duplicate object ids, all the entries
  // for the same id are filled in.
  bool missing_result = false;
  bool will_throw_exception = false;
  for (size_t i = 0; i < ids.size(); i++) {
    const auto pair = result_map.find(ids[i]);
    if (pair != result_map.end()) {
      (*results)[i] = pair->second;
      RAY_CHECK(!pair->second->IsInPlasmaError());
      if (pair->second->IsException()) {
        // The language bindings should throw an exception if they see this
        // object.
        will_throw_exception = true;
      }
    } else {
      missing_result = true;
    }
  }
  // If no timeout was set and none of the results will throw an exception,
  // then check that we fetched all results before returning.
  if (timeout_ms < 0 && !will_throw_exception) {
    RAY_CHECK(!missing_result);
  }
  return Status::OK();
}

Status CoreWorker::GetIfLocal(const std::vector<ObjectID> &ids,
                              std::vector<std::shared_ptr<RayObject>> *results) {
  results->resize(ids.size(), nullptr);

  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  RAY_RETURN_NOT_OK(plasma_store_provider_->GetIfLocal(ids, &result_map));
  for (size_t i = 0; i < ids.size(); i++) {
    auto pair = result_map.find(ids[i]);
    // The caller of this method should guarantee that the object exists in the plasma
    // store when this method is called.
    RAY_CHECK(pair != result_map.end());
    RAY_CHECK(pair->second != nullptr);
    (*results)[i] = pair->second;
  }

  return Status::OK();
}

Status CoreWorker::Contains(const ObjectID &object_id, bool *has_object,
                            bool *is_in_plasma) {
  bool found = false;
  bool in_plasma = false;
  found = memory_store_->Contains(object_id, &in_plasma);
  if (in_plasma) {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Contains(object_id, &found));
  }
  *has_object = found;
  if (is_in_plasma != nullptr) {
    *is_in_plasma = found && in_plasma;
  }
  return Status::OK();
}

// For any objects that are ErrorType::OBJECT_IN_PLASMA, we need to move them from
// the ready set into the plasma_object_ids set to wait on them there.
void RetryObjectInPlasmaErrors(std::shared_ptr<CoreWorkerMemoryStore> &memory_store,
                               WorkerContext &worker_context,
                               absl::flat_hash_set<ObjectID> &memory_object_ids,
                               absl::flat_hash_set<ObjectID> &plasma_object_ids,
                               absl::flat_hash_set<ObjectID> &ready) {
  for (auto iter = memory_object_ids.begin(); iter != memory_object_ids.end();) {
    auto current = iter++;
    const auto &mem_id = *current;
    auto ready_iter = ready.find(mem_id);
    if (ready_iter != ready.end()) {
      std::vector<std::shared_ptr<RayObject>> found;
      RAY_CHECK_OK(memory_store->Get({mem_id}, /*num_objects=*/1, /*timeout=*/0,
                                     worker_context,
                                     /*remote_after_get=*/false, &found));
      if (found.size() == 1 && found[0]->IsInPlasmaError()) {
        plasma_object_ids.insert(mem_id);
        ready.erase(ready_iter);
        memory_object_ids.erase(current);
      }
    }
  }
}

Status CoreWorker::Wait(const std::vector<ObjectID> &ids, int num_objects,
                        int64_t timeout_ms, std::vector<bool> *results,
                        bool fetch_local) {
  results->resize(ids.size(), false);

  if (num_objects <= 0 || num_objects > static_cast<int>(ids.size())) {
    return Status::Invalid(
        "Number of objects to wait for must be between 1 and the number of ids.");
  }

  absl::flat_hash_set<ObjectID> plasma_object_ids;
  absl::flat_hash_set<ObjectID> memory_object_ids(ids.begin(), ids.end());

  if (memory_object_ids.size() != ids.size()) {
    return Status::Invalid("Duplicate object IDs not supported in wait.");
  }

  absl::flat_hash_set<ObjectID> ready;
  int64_t start_time = current_time_ms();
  RAY_RETURN_NOT_OK(memory_store_->Wait(
      memory_object_ids,
      std::min(static_cast<int>(memory_object_ids.size()), num_objects), timeout_ms,
      worker_context_, &ready));
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);
  if (timeout_ms > 0) {
    timeout_ms =
        std::max(0, static_cast<int>(timeout_ms - (current_time_ms() - start_time)));
  }
  if (fetch_local) {
    RetryObjectInPlasmaErrors(memory_store_, worker_context_, memory_object_ids,
                              plasma_object_ids, ready);
    if (static_cast<int>(ready.size()) < num_objects && plasma_object_ids.size() > 0) {
      RAY_RETURN_NOT_OK(plasma_store_provider_->Wait(
          plasma_object_ids,
          std::min(static_cast<int>(plasma_object_ids.size()),
                   num_objects - static_cast<int>(ready.size())),
          timeout_ms, worker_context_, &ready));
    }
  }
  RAY_CHECK(static_cast<int>(ready.size()) <= num_objects);

  for (size_t i = 0; i < ids.size(); i++) {
    if (ready.find(ids[i]) != ready.end()) {
      results->at(i) = true;
    }
  }

  return Status::OK();
}

Status CoreWorker::Delete(const std::vector<ObjectID> &object_ids, bool local_only) {
  // Release the object from plasma. This does not affect the object's ref
  // count. If this was called from a non-owning worker, then a warning will be
  // logged and the object will not get released.
  reference_counter_->FreePlasmaObjects(object_ids);

  // Store an error in the in-memory store to indicate that the plasma value is
  // no longer reachable.
  memory_store_->Delete(object_ids);
  for (const auto &object_id : object_ids) {
    RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE),
                                 object_id));
  }

  // We only delete from plasma, which avoids hangs (issue #7105). In-memory
  // objects can only be deleted once the ref count goes to 0.
  absl::flat_hash_set<ObjectID> plasma_object_ids(object_ids.begin(), object_ids.end());
  return plasma_store_provider_->Delete(plasma_object_ids, local_only);
}

void CoreWorker::TriggerGlobalGC() {
  local_raylet_client_->GlobalGC(
      [](const Status &status, const rpc::GlobalGCReply &reply) {
        if (!status.ok()) {
          RAY_LOG(ERROR) << "Failed to send global GC request: " << status.ToString();
        }
      });
}

std::string CoreWorker::MemoryUsageString() {
  // Currently only the Plasma store returns a debug string.
  return plasma_store_provider_->MemoryUsageString();
}

TaskID CoreWorker::GetCallerId() const {
  TaskID caller_id;
  ActorID actor_id = GetActorId();
  if (!actor_id.IsNil()) {
    caller_id = TaskID::ForActorCreationTask(actor_id);
  } else {
    absl::MutexLock lock(&mutex_);
    caller_id = main_thread_task_id_;
  }
  return caller_id;
}

Status CoreWorker::PushError(const JobID &job_id, const std::string &type,
                             const std::string &error_message, double timestamp) {
  if (options_.is_local_mode) {
    RAY_LOG(ERROR) << "Pushed Error with JobID: " << job_id << " of type: " << type
                   << " with message: " << error_message << " at time: " << timestamp;
    return Status::OK();
  }
  return local_raylet_client_->PushError(job_id, type, error_message, timestamp);
}

Status CoreWorker::SetResource(const std::string &resource_name, const double capacity,
                               const NodeID &node_id) {
  return local_raylet_client_->SetResource(resource_name, capacity, node_id);
}

void CoreWorker::SpillOwnedObject(const ObjectID &object_id,
                                  const std::shared_ptr<RayObject> &obj,
                                  std::function<void()> callback) {
  if (!obj->IsInPlasmaError()) {
    RAY_LOG(ERROR) << "Cannot spill inlined object " << object_id;
    callback();
    return;
  }

  // Find the raylet that hosts the primary copy of the object.
  bool owned_by_us = false;
  NodeID pinned_at;
  bool spilled = false;
  RAY_CHECK(reference_counter_->IsPlasmaObjectPinnedOrSpilled(object_id, &owned_by_us,
                                                              &pinned_at, &spilled));
  RAY_CHECK(owned_by_us);
  if (spilled) {
    // The object has already been spilled.
    return;
  }
  auto node = gcs_client_->Nodes().Get(pinned_at);
  if (pinned_at.IsNil() || !node) {
    RAY_LOG(ERROR) << "Primary raylet for object " << object_id << " unreachable";
    callback();
    return;
  }

  // Ask the raylet to spill the object.
  RAY_LOG(DEBUG) << "Sending spill request to raylet for object " << object_id;
  auto raylet_client =
      std::make_shared<raylet::RayletClient>(rpc::NodeManagerWorkerClient::make(
          node->node_manager_address(), node->node_manager_port(),
          *client_call_manager_));
  raylet_client->RequestObjectSpillage(
      object_id, [object_id, callback](const Status &status,
                                       const rpc::RequestObjectSpillageReply &reply) {
        if (!status.ok() || !reply.success()) {
          RAY_LOG(ERROR) << "Failed to spill object " << object_id
                         << ", raylet unreachable or object could not be spilled.";
        }
        // TODO(Clark): Provide spilled URL and spilled node ID to callback so it can
        // added them to the reference.
        callback();
      });
}

std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const rpc::SchedulingStrategy &scheduling_strategy) {
  auto placement_group_id = PlacementGroupID::Nil();
  auto bundle_index = -1;
  if (scheduling_strategy.scheduling_strategy_case() ==
      rpc::SchedulingStrategy::SchedulingStrategyCase::
          kPlacementGroupSchedulingStrategy) {
    placement_group_id = PlacementGroupID::FromBinary(
        scheduling_strategy.placement_group_scheduling_strategy().placement_group_id());
    bundle_index = scheduling_strategy.placement_group_scheduling_strategy()
                       .placement_group_bundle_index();
  }
  if (bundle_index < 0) {
    RAY_CHECK(bundle_index == -1) << "Invalid bundle index " << bundle_index;
  }
  std::unordered_map<std::string, double> new_resources;
  if (placement_group_id != PlacementGroupID::Nil()) {
    for (auto iter = resources.begin(); iter != resources.end(); iter++) {
      auto new_name = FormatPlacementGroupResource(iter->first, placement_group_id, -1);
      new_resources[new_name] = iter->second;
      if (bundle_index >= 0) {
        auto index_name =
            FormatPlacementGroupResource(iter->first, placement_group_id, bundle_index);
        new_resources[index_name] = iter->second;
      }
    }
    return new_resources;
  }
  return resources;
}

Status CoreWorker::SpillObjects(const std::vector<ObjectID> &object_ids) {
  auto mutex = std::make_shared<absl::Mutex>();
  auto num_remaining = std::make_shared<size_t>(object_ids.size());
  auto ready_promise = std::make_shared<std::promise<void>>(std::promise<void>());
  Status final_status;

  // TODO(Clark): Add spilled URL and spilled node ID to reference in this callback.
  auto callback = [mutex, num_remaining, ready_promise]() {
    absl::MutexLock lock(mutex.get());
    (*num_remaining)--;
    if (*num_remaining == 0) {
      ready_promise->set_value();
    }
  };

  for (const auto &object_id : object_ids) {
    RAY_LOG(DEBUG) << "Requesting spill for object " << object_id;
    // Acquire a temporary reference to make sure that the object is still in
    // scope by the time we register the callback to spill the object.
    // Otherwise, the callback may never get called.
    AddLocalReference(object_id, "<temporary (get object status)>");

    rpc::Address owner_address;
    auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
    if (!has_owner) {
      final_status =
          Status::Invalid("Cannot call spill on objects that have gone out of scope.");
      callback();
    } else if (WorkerID::FromBinary(owner_address.worker_id()) !=
               worker_context_.GetWorkerID()) {
      final_status = Status::Invalid("Cannot call spill on objects that we do not own.");
      callback();
    } else {
      memory_store_->GetAsync(
          object_id, [this, object_id, callback](std::shared_ptr<RayObject> obj) {
            SpillOwnedObject(object_id, obj, callback);
          });
    }

    // Remove the temporary reference.
    RemoveLocalReference(object_id);
  }

  ready_promise->get_future().wait();

  for (const auto &object_id : object_ids) {
    // TODO(Clark): Move this to the callback (unless we really wanted to batch it) and
    // also include the spilled URL, spilled node ID, and updated object size.
    reference_counter_->HandleObjectSpilled(object_id, "", NodeID::Nil(), -1,
                                            /*release*/ true);
  }
  return final_status;
}

std::unordered_map<std::string, double> AddMemoryConstraint(
    const rpc::JobConfig &job_config, const rpc::Language &language,
    const std::unordered_map<std::string, double> &resources) {
  auto new_resources = resources;
  if (!new_resources.count(kMemory_ResourceLabel)) {
    auto worker_process_default_memory_units = 50ULL * 1024 * 1024;
    if (language == rpc::Language::JAVA) {
      worker_process_default_memory_units =
          job_config.java_worker_process_default_memory_units();
    }
    new_resources.emplace(kMemory_ResourceLabel, worker_process_default_memory_units);
  }
  return new_resources;
}

void CoreWorker::BuildCommonTaskSpec(
    ray::TaskSpecBuilder &builder, const JobID &job_id, const TaskID &task_id,
    const std::string name, const TaskID &current_task_id, const uint64_t task_index,
    const TaskID &caller_id, const ray::rpc::Address &address,
    const ray::RayFunction &function,
    const std::vector<std::unique_ptr<ray::TaskArg>> &args, uint64_t num_returns,
    const std::unordered_map<std::string, double> &required_resources,
    const std::unordered_map<std::string, double> &required_placement_resources,
    std::vector<ObjectID> *return_ids, const std::string debugger_breakpoint,
    int64_t depth, const std::string &serialized_runtime_env,
    const std::unordered_map<std::string, std::string> &override_environment_variables,
    const ActorID &source_actor_id, bool no_reply) {
  // Build common task spec.
  builder.SetCommonTaskSpec(
      task_id, name, function.GetLanguage(), function.GetFunctionDescriptor(), job_id,
      current_task_id, task_index, caller_id, address, num_returns, required_resources,
      required_placement_resources, debugger_breakpoint, depth,
      // TODO(SongGuyang): Move the logic of `prepare_runtime_env` from Python to Core
      // Worker. A common process is needed.
      // If runtime env is not provided, use job config. Only for Java and C++ because it
      // has been set in Python by `prepare_runtime_env`.
      (serialized_runtime_env.empty() || serialized_runtime_env == "{}")
          ? job_config_->serialized_runtime_env()
          : serialized_runtime_env,
      override_environment_variables, source_actor_id, no_reply);
  // Set task arguments.
  for (const auto &arg : args) {
    builder.AddArg(*arg);
  }

  // Compute return IDs.
  return_ids->resize(num_returns);
  for (size_t i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::FromIndex(task_id, i + 1);
  }
}

Status CoreWorker::SubmitTask(const RayFunction &function,
                              const std::vector<std::unique_ptr<TaskArg>> &args,
                              const TaskOptions &task_options,
                              std::vector<ObjectID> *return_ids, int max_retries,
                              bool retry_exceptions,
                              const rpc::SchedulingStrategy &scheduling_strategy,
                              const std::string &debugger_breakpoint) {
  RAY_CHECK(scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const JobID job_id = worker_context_.GetCurrentJobID();
  const auto task_id =
      TaskID::ForNormalTask(job_id, worker_context_.GetCurrentTaskID(), next_task_index);

  // Check whether the bundle index is valid or not if the actor is using placement group.
  const auto &status = ValidatePlacementGroupBundleIndex(scheduling_strategy);
  if (!status.ok()) {
    return status;
  }

  const auto &status_strategy = ValidateSchedulingStrategy(scheduling_strategy);
  if (!status_strategy.ok()) {
    return status_strategy;
  }

  auto constrained_resources =
      AddPlacementGroupConstraint(task_options.resources, scheduling_strategy);

  const std::unordered_map<std::string, double> required_resources;
  auto task_name = task_options.name.empty()
                       ? function.GetFunctionDescriptor()->DefaultTaskName()
                       : task_options.name;
  // Propagate existing environment variable overrides, but override them with any new
  // ones
  std::unordered_map<std::string, std::string> current_override_environment_variables =
      worker_context_.GetCurrentOverrideEnvironmentVariables();
  std::unordered_map<std::string, std::string> override_environment_variables =
      task_options.override_environment_variables;
  override_environment_variables.insert(current_override_environment_variables.begin(),
                                        current_override_environment_variables.end());
  int64_t depth = worker_context_.GetTaskDepth() + 1;
  // TODO(ekl) offload task building onto a thread pool for performance
  BuildCommonTaskSpec(builder, job_id, task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, task_options.num_returns,
                      constrained_resources, required_resources, return_ids,
                      debugger_breakpoint, depth, task_options.serialized_runtime_env,
                      override_environment_variables, worker_context_.GetCurrentActorID(),
                      task_options.ignore_return);
  builder.SetNormalTaskSpec(max_retries, retry_exceptions, scheduling_strategy);
  TaskSpecification task_spec = builder.Build();
  if (options_.is_local_mode) {
    ExecuteTaskLocalMode(task_spec);
  } else {
    task_manager_->AddPendingTask(task_spec.CallerAddress(), task_spec, CurrentCallSite(),
                                  max_retries);
    io_service_.post(
        [this, task_spec]() {
          RAY_UNUSED(direct_task_submitter_->SubmitTask(task_spec));
        },
        "CoreWorker.SubmitTask");
  }
  return Status::OK();
}

Status CoreWorker::CreateActor(const RayFunction &function,
                               const std::vector<std::unique_ptr<TaskArg>> &args,
                               ActorCreationOptions actor_creation_options,
                               const std::string &extension_data,
                               ActorID *return_actor_id) {
  RAY_CHECK(actor_creation_options.scheduling_strategy.scheduling_strategy_case() !=
            rpc::SchedulingStrategy::SchedulingStrategyCase::SCHEDULING_STRATEGY_NOT_SET);

  const auto &status_strategy =
      ValidateSchedulingStrategy(actor_creation_options.scheduling_strategy);
  if (!status_strategy.ok()) {
    return status_strategy;
  }

  if (actor_creation_options.is_asyncio && options_.is_local_mode) {
    return Status::NotImplemented(
        "Async actor is currently not supported for the local mode");
  }

  RAY_CHECK(job_config_ != nullptr);
  if (actor_creation_options.is_detached == nullptr) {
    /// Since this actor is not specified the lifetime when creating, let's use
    /// the default value of the job.
    actor_creation_options.is_detached =
        std::make_shared<bool>(IsDetachedHelper(job_config_->default_actor_lifetime()));
  }
  const bool is_detached = *actor_creation_options.is_detached;

  if (function.GetLanguage() == Language::JAVA &&
      RayConfig::instance().gcs_task_scheduling_enabled()) {
    auto &resources = actor_creation_options.resources;
    auto it = resources.find("memory");
    auto actor_memory_bytes =
        it == resources.end() ? 0.0 : FromMemoryUnitsToBytes(it->second);
    auto status = ValidateJvmOptions(job_config_, *return_actor_id, actor_memory_bytes,
                                     actor_creation_options.dynamic_worker_options);
    if (!status.ok()) {
      return status;
    }
    auto &options = actor_creation_options.dynamic_worker_options;
    for (auto option : options) {
      option.erase(std::remove_if(option.begin(), option.end(),
                                  [](unsigned char c) { return std::isspace(c); }),
                   option.end());
    }
  }

  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const ActorID actor_id =
      ActorID::Of(worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
                  next_task_index);
  const TaskID actor_creation_task_id = TaskID::ForActorCreationTask(actor_id);
  const JobID job_id = worker_context_.GetCurrentJobID();

  // Propagate existing environment variable overrides, but override them with any new
  // ones
  std::unordered_map<std::string, std::string> current_override_environment_variables =
      worker_context_.GetCurrentOverrideEnvironmentVariables();
  std::unordered_map<std::string, std::string> override_environment_variables =
      actor_creation_options.override_environment_variables;
  override_environment_variables.insert(current_override_environment_variables.begin(),
                                        current_override_environment_variables.end());
  std::vector<ObjectID> return_ids;
  TaskSpecBuilder builder;

  auto resources = actor_creation_options.resources;
  auto placement_resources = actor_creation_options.placement_resources;
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    if (ray::NeedSoleWorkerProcess(function.GetLanguage(), resources)) {
      if (placement_resources.empty()) {
        placement_resources = resources;
      }
      placement_resources =
          AddMemoryConstraint(*job_config_, function.GetLanguage(), placement_resources);
    }
  }

  // Check whether the bundle index is valid or not if the actor is using placement group.
  auto status =
      ValidatePlacementGroupBundleIndex(actor_creation_options.scheduling_strategy);
  if (!status.ok()) {
    return status;
  }

  auto new_placement_resources = AddPlacementGroupConstraint(
      placement_resources, actor_creation_options.scheduling_strategy);
  auto new_resource =
      AddPlacementGroupConstraint(resources, actor_creation_options.scheduling_strategy);
  const auto actor_name = actor_creation_options.name;
  const auto task_name =
      actor_name.empty()
          ? function.GetFunctionDescriptor()->DefaultTaskName()
          : actor_name + ":" + function.GetFunctionDescriptor()->CallString();
  int64_t depth = worker_context_.GetTaskDepth() + 1;
  BuildCommonTaskSpec(builder, job_id, actor_creation_task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, 1, new_resource,
                      new_placement_resources, &return_ids, "" /* debugger_breakpoint */,
                      depth, actor_creation_options.serialized_runtime_env,
                      override_environment_variables, worker_context_.GetCurrentActorID(),
                      false /*no_reply*/);

  // If the namespace is not specified, get it from the job.
  const auto &ray_namespace = (actor_creation_options.ray_namespace.empty()
                                   ? job_config_->ray_namespace()
                                   : actor_creation_options.ray_namespace);
  auto actor_handle = std::make_unique<ActorHandle>(
      actor_id, GetCallerId(), rpc_address_, job_id, /*actor_cursor=*/return_ids[0],
      function.GetLanguage(), function.GetFunctionDescriptor(), extension_data,
      actor_creation_options.max_task_retries, actor_name, ray_namespace,
      actor_creation_options.enable_task_fast_fail);
  std::string serialized_actor_handle;
  actor_handle->Serialize(&serialized_actor_handle);

  // valify labels and insert default label key
  status = ValidateLabels(actor_creation_options.labels);
  if (!status.ok()) {
    return status;
  }
  std::unordered_map<std::string, std::string> labels = actor_creation_options.labels;
  InsertDefaultLabelKeys(labels, actor_id);

  builder.SetActorCreationTaskSpec(
      actor_id, serialized_actor_handle, actor_creation_options.scheduling_strategy,
      actor_creation_options.max_restarts, actor_creation_options.max_task_retries,
      actor_creation_options.dynamic_worker_options,
      actor_creation_options.max_concurrency, is_detached, actor_name, ray_namespace,
      actor_creation_options.is_asyncio, actor_creation_options.concurrent_groups,
      extension_data, actor_creation_options.extended_properties, labels);
  // Add the actor handle before we submit the actor creation task, since the
  // actor handle must be in scope by the time the GCS sends the
  // WaitForActorOutOfScopeRequest.
  // Note we need set `cached_actor_name` to empty string as we only cache named actors
  // when getting them from GCS.
  RAY_CHECK(actor_manager_->AddNewActorHandle(std::move(actor_handle), CurrentCallSite(),
                                              rpc_address_, is_detached))
      << "Actor " << actor_id << " already exists";
  *return_actor_id = actor_id;
  TaskSpecification task_spec = builder.Build();
  if (options_.is_local_mode) {
    // TODO(suquark): Should we consider namespace in local mode? Currently
    // it looks like two actors with two different namespaces become the
    // same actor in local mode. Maybe this is not an issue if we consider
    // the actor name globally unique.
    if (task_spec.IsDetachedActor()) {
      // Since local mode doesn't pass GCS actor management code path,
      // it just register actor names in memory.
      local_mode_named_actor_registry_.emplace(actor_name, actor_id);
    }
    ExecuteTaskLocalMode(task_spec);
  } else {
    int max_retries;
    if (actor_creation_options.max_restarts == -1) {
      max_retries = -1;
    } else {
      max_retries = std::max((int64_t)RayConfig::instance().actor_creation_min_retries(),
                             actor_creation_options.max_restarts);
    }
    task_manager_->AddPendingTask(rpc_address_, task_spec, CurrentCallSite(),
                                  max_retries);
    status = direct_task_submitter_->SubmitTask(task_spec);
  }
  task_stats_->AddTaskSubmitted(function);
  RAY_EVENT(INFO, EVENT_LABEL_ACTOR_SUBMITTED)
          .WithField("job_id", job_id.Hex())
          .WithField("actor_id", actor_id.Hex())
      << "Actor is submitted. actor id is " << actor_id;
  return status;
}

Status CoreWorker::CreatePlacementGroup(
    const PlacementGroupCreationOptions &placement_group_creation_options,
    PlacementGroupID *return_placement_group_id) {
  auto constrained_bundles = placement_group_creation_options.bundles;
  for (auto &bundle : constrained_bundles) {
    if (RayConfig::instance().gcs_task_scheduling_enabled() &&
        bundle.count(kMemory_ResourceLabel) == 0) {
      bundle[kMemory_ResourceLabel] =
          RayConfig::instance().pg_bundle_default_memory_mb_gcs_scheduling() * 1024 *
          1024;
    }
  }

  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  const auto &bundles = placement_group_creation_options.bundles;
  for (const auto &bundle : bundles) {
    for (const auto &resource : bundle) {
      if (resource.first == kBundle_ResourceLabel) {
        std::ostringstream stream;
        stream << kBundle_ResourceLabel << " is a system reserved resource, which is not "
               << "allowed to be used in placement groupd ";
        return Status::Invalid(stream.str());
      }
    }
  }
  const PlacementGroupID placement_group_id = PlacementGroupID::Of(GetCurrentJobId());
  PlacementGroupSpecBuilder builder;
  builder.SetPlacementGroupSpec(
      placement_group_id, placement_group_creation_options.name, constrained_bundles,
      placement_group_creation_options.strategy,
      placement_group_creation_options.is_detached, worker_context_.GetCurrentJobID(),
      worker_context_.GetCurrentActorID(), worker_context_.CurrentActorDetached());
  PlacementGroupSpecification placement_group_spec = builder.Build();
  *return_placement_group_id = placement_group_id;
  RAY_LOG(INFO) << "Submitting Placement Group creation to GCS: " << placement_group_id;
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncCreatePlacementGroup(
      placement_group_spec,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in creating the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::AddPlacementGroupBundles(
    const PlacementGroupID &placement_group_id,
    const std::vector<std::unordered_map<std::string, double>> &bundles) {
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    for (auto &bundle : bundles) {
      if (bundle.count(kMemory_ResourceLabel) == 0) {
        return Status::Invalid(
            "Resource 'memory' must be specified in bundles if gcs scheduler enabled.");
      }
    }
  }

  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  RAY_LOG(DEBUG) << "Submitting add placement group bundes request to GCS: "
                 << placement_group_id;
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncAddPlacementGroupBundles(
      placement_group_id, bundles,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in adding bundles for the placement group: "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    RAY_LOG(ERROR) << stream.str();
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::RemovePlacementGroupBundles(const PlacementGroupID &placement_group_id,
                                               const std::vector<int> &bundle_indexes) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  RAY_LOG(DEBUG) << "Submitting remove placement group bundes request to GCS: "
                 << placement_group_id;
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncRemovePlacementGroupBundles(
      placement_group_id, bundle_indexes,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in removing bundles from the placement group: "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::RemovePlacementGroup(const PlacementGroupID &placement_group_id) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  // Synchronously wait for placement group removal.
  RAY_UNUSED(gcs_client_->PlacementGroups().AsyncRemovePlacementGroup(
      placement_group_id,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(
          RayConfig::instance().gcs_server_request_timeout_seconds())) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in removing the placement group of id "
           << placement_group_id
           << ". It is probably "
              "because GCS server is dead or there's a high load there.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::WaitPlacementGroupReady(const PlacementGroupID &placement_group_id,
                                           int timeout_seconds) {
  std::shared_ptr<std::promise<Status>> status_promise =
      std::make_shared<std::promise<Status>>();
  RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncWaitUntilReady(
      placement_group_id,
      [status_promise](const Status &status) { status_promise->set_value(status); }));
  auto status_future = status_promise->get_future();
  if (status_future.wait_for(std::chrono::seconds(timeout_seconds)) !=
      std::future_status::ready) {
    std::ostringstream stream;
    stream << "There was timeout in waiting for placement group " << placement_group_id
           << " creation.";
    return Status::TimedOut(stream.str());
  }
  return status_future.get();
}

Status CoreWorker::SubmitActorTask(const ActorID &actor_id, const RayFunction &function,
                                   const std::vector<std::unique_ptr<TaskArg>> &args,
                                   const TaskOptions &task_options,
                                   std::vector<ObjectID> *return_ids) {
  if (job_config_->actor_task_back_pressure_enabled() and
      direct_actor_submitter_->IncPendingTaskNumsIfNotFull(actor_id)) {
    task_stats_->OnBackPressured();
    RAY_LOG(DEBUG) << "Back pressure occur. actor_id: " << actor_id;
    return Status::TryAgain("Back pressure, please try again.");
  }

  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  // Subscribe the actor state when we first submit the actor task. It is to reduce the
  // number of connections. The method is idempotent.
  actor_manager_->SubscribeActorState(actor_id);

  // ANT-INTERNAL
  if (task_options.ignore_return) {
    // NOTE(kfstorm): In theory, we should return a bad status here. But that will cause a
    // lot of code diff with the community version.
    RAY_CHECK(actor_handle->MaxTaskRetries() == 0)
        << "\"Max task retries\" and \"ignore return\" cannot be used together.";
  }

  // Add one for actor cursor object id for tasks.
  const int num_returns = task_options.num_returns + 1;

  // Build common task spec.
  TaskSpecBuilder builder;
  const auto next_task_index = worker_context_.GetNextTaskIndex();
  const TaskID actor_task_id = TaskID::ForActorTask(
      worker_context_.GetCurrentJobID(), worker_context_.GetCurrentTaskID(),
      next_task_index, actor_handle->GetActorID());
  const std::unordered_map<std::string, double> required_resources;
  const auto task_name = task_options.name.empty()
                             ? function.GetFunctionDescriptor()->DefaultTaskName()
                             : task_options.name;
  const std::unordered_map<std::string, std::string> override_environment_variables = {};
  // Depth shouldn't matter for an actor task, but for consistency it should be
  // the same as the actor creation task's depth.
  int64_t depth = worker_context_.GetTaskDepth();
  BuildCommonTaskSpec(builder, actor_handle->CreationJobID(), actor_task_id, task_name,
                      worker_context_.GetCurrentTaskID(), next_task_index, GetCallerId(),
                      rpc_address_, function, args, num_returns, task_options.resources,
                      required_resources, return_ids, "", /* debugger_breakpoint */
                      depth,                              /*depth*/
                      "{}",                               /* serialized_runtime_env */
                      override_environment_variables, worker_context_.GetCurrentActorID(),
                      task_options.ignore_return);
  // NOTE: placement_group_capture_child_tasks and runtime_env will
  // be ignored in the actor because we should always follow the actor's option.

  const ObjectID new_cursor = return_ids->back();
  actor_handle->SetActorTaskSpec(builder, new_cursor, task_options.enable_task_fast_fail);
  // Remove cursor from return ids.
  return_ids->pop_back();

  // Submit task.
  TaskSpecification task_spec = builder.Build();
  if (options_.is_local_mode) {
    ExecuteTaskLocalMode(task_spec, actor_id);
  } else {
    // Note this check is internal only and is a performance optimization.
    if (!task_options.ignore_return) {
      task_manager_->AddPendingTask(rpc_address_, task_spec, CurrentCallSite(),
                                    actor_handle->MaxTaskRetries());
    }
    direct_actor_submitter_->AppendTask(task_spec);
  }
  task_stats_->AddTaskSubmitted(function);
  return Status::OK();
}

Status CoreWorker::CancelTask(const ObjectID &object_id, bool force_kill,
                              bool recursive) {
  if (actor_manager_->CheckActorHandleExists(object_id.TaskId().ActorId())) {
    return Status::Invalid("Actor task cancellation is not supported.");
  }
  rpc::Address obj_addr;
  if (!reference_counter_->GetOwner(object_id, &obj_addr)) {
    return Status::Invalid("No owner found for object.");
  }
  if (obj_addr.SerializeAsString() != rpc_address_.SerializeAsString()) {
    return direct_task_submitter_->CancelRemoteTask(object_id, obj_addr, force_kill,
                                                    recursive);
  }

  auto task_spec = task_manager_->GetTaskSpec(object_id.TaskId());
  if (task_spec && !task_spec.value().IsActorCreationTask()) {
    return direct_task_submitter_->CancelTask(task_spec.value(), force_kill, recursive);
  }
  return Status::OK();
}

Status CoreWorker::CancelChildren(const TaskID &task_id, bool force_kill) {
  bool recursive_success = true;
  for (const auto &child_id : task_manager_->GetPendingChildrenTasks(task_id)) {
    auto child_spec = task_manager_->GetTaskSpec(child_id);
    if (child_spec.has_value()) {
      auto result =
          direct_task_submitter_->CancelTask(child_spec.value(), force_kill, true);
      recursive_success = recursive_success && result.ok();
    } else {
      recursive_success = false;
    }
  }
  if (recursive_success) {
    return Status::OK();
  } else {
    return Status::UnknownError("Recursive task cancelation failed--check warning logs.");
  }
}

bool CoreWorker::KillJob(const JobID &job_id) {
  constexpr size_t max_retry_num = 15;
  for (size_t i = 0; i < max_retry_num; ++i) {
    auto status = InternalKillJob(job_id);
    if (status.ok()) {
      RAY_LOG(INFO) << "Succeeded to kill job " << job_id;
      return true;
    } else {
      RAY_LOG(ERROR) << "Failed to kill job " << job_id << " with error " << status
                     << ". Retrying...";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }
  return false;
}

Status CoreWorker::InternalKillJob(const JobID &job_id) {
  RAY_CHECK(!options_.is_local_mode);
  std::promise<Status> promise;
  const auto mark_as_failed = job_config_->mark_job_state_as_failed_when_killing();
  RAY_CHECK_OK(gcs_client_->Jobs().AsyncKill(
      job_id, [&promise](const Status &status) { promise.set_value(status); },
      mark_as_failed));
  return promise.get_future().get();
}

Status CoreWorker::KillActor(const ActorID &actor_id, bool force_kill, bool no_restart) {
  if (options_.is_local_mode) {
    return KillActorLocalMode(actor_id);
  }
  std::promise<Status> p;
  auto f = p.get_future();
  io_service_.post(
      [this, p = &p, actor_id, force_kill, no_restart]() {
        auto cb = [this, p, actor_id, force_kill, no_restart](Status status) mutable {
          if (status.ok()) {
            RAY_CHECK_OK(gcs_client_->Actors().AsyncKillActor(actor_id, force_kill,
                                                              no_restart, nullptr));
          }
          p->set_value(std::move(status));
        };
        if (actor_manager_->CheckActorHandleExists(actor_id)) {
          cb(Status::OK());
        } else {
          std::stringstream stream;
          stream << "Failed to find a corresponding actor handle for " << actor_id;
          cb(Status::Invalid(stream.str()));
        }
      },
      "CoreWorker.KillActor");
  const auto &status = f.get();
  actor_manager_->OnActorKilled(actor_id);
  return status;
}

Status CoreWorker::KillActorLocalMode(const ActorID &actor_id) {
  // KillActor doesn't do anything in local mode. We only remove named actor entry if
  // exists.
  for (auto it = local_mode_named_actor_registry_.begin();
       it != local_mode_named_actor_registry_.end();) {
    auto current = it++;
    if (current->second == actor_id) {
      local_mode_named_actor_registry_.erase(current);
    }
  }
  return Status::OK();
}

void CoreWorker::RemoveActorHandleReference(const ActorID &actor_id) {
  ObjectID actor_handle_id = ObjectID::ForActorHandle(actor_id);
  reference_counter_->RemoveLocalReference(actor_handle_id, nullptr);
}

ActorID CoreWorker::DeserializeAndRegisterActorHandle(const std::string &serialized,
                                                      const ObjectID &outer_object_id) {
  std::unique_ptr<ActorHandle> actor_handle(new ActorHandle(serialized));
  return actor_manager_->RegisterActorHandle(std::move(actor_handle), outer_object_id,
                                             CurrentCallSite(), rpc_address_);
}

Status CoreWorker::SerializeActorHandle(const ActorID &actor_id, std::string *output,
                                        ObjectID *actor_handle_id) const {
  auto actor_handle = actor_manager_->GetActorHandle(actor_id);
  actor_handle->Serialize(output);
  *actor_handle_id = ObjectID::ForActorHandle(actor_id);
  return Status::OK();
}

void CoreWorker::SubscribeActorReconnected(
    const std::function<void(const ActorID &)> &callback) {
  direct_actor_submitter_->SubscribeActorReconnected(callback);
}

std::shared_ptr<const ActorHandle> CoreWorker::GetActorHandle(
    const ActorID &actor_id) const {
  return actor_manager_->GetActorHandle(actor_id);
}

std::pair<std::shared_ptr<const ActorHandle>, Status> CoreWorker::GetNamedActorHandle(
    const std::string &name, const std::string &ray_namespace) {
  RAY_CHECK(!name.empty());
  if (options_.is_local_mode) {
    return GetNamedActorHandleLocalMode(name);
  }
  return actor_manager_->GetNamedActorHandle(
      name, ray_namespace.empty() ? job_config_->ray_namespace() : ray_namespace,
      CurrentCallSite(), rpc_address_);
}

std::pair<std::shared_ptr<const ActorHandle>, Status>
CoreWorker::GetNamedActorHandleLocalMode(const std::string &name) {
  auto it = local_mode_named_actor_registry_.find(name);
  if (it == local_mode_named_actor_registry_.end()) {
    std::ostringstream stream;
    stream << "Failed to look up actor with name '" << name;
    return std::make_pair(nullptr, Status::NotFound(stream.str()));
  }

  return std::make_pair(GetActorHandle(it->second), Status::OK());
}

std::pair<bool, Status> CoreWorker::PutKV(const std::string &key,
                                          const std::string &value, bool overwrite,
                                          bool isGlobal) {
  bool added = false;
  std::string finalKey = AddPrefixForKey(key, isGlobal);
  auto status = gcs_client_->InternalKV().Put(finalKey, value, overwrite, added);
  return std::make_pair(added, status);
}

std::pair<std::string, Status> CoreWorker::GetKV(const std::string &key, bool isGlobal) {
  std::string value;
  std::string finalKey = AddPrefixForKey(key, isGlobal);
  auto status = gcs_client_->InternalKV().Get(finalKey, value);
  return std::make_pair(value, status);
}

std::pair<bool, Status> CoreWorker::ExistsKV(const std::string &key, bool isGlobal) {
  bool exist;
  std::string finalKey = AddPrefixForKey(key, isGlobal);
  auto status = gcs_client_->InternalKV().Exists(finalKey, exist);
  return std::make_pair(exist, status);
}

std::pair<bool, Status> CoreWorker::DeleteKV(const std::string &key, bool isGlobal) {
  bool deleted = false;
  std::string finalKey = AddPrefixForKey(key, isGlobal);
  auto status = gcs_client_->InternalKV().Del(finalKey, deleted);
  return std::make_pair(deleted, status);
}

const ResourceMappingType CoreWorker::GetResourceIDs() const {
  absl::MutexLock lock(&mutex_);
  return *resource_ids_;
}

std::unique_ptr<worker::ProfileEvent> CoreWorker::CreateProfileEvent(
    const std::string &event_type) {
  return std::make_unique<worker::ProfileEvent>(profiler_, event_type);
}

void CoreWorker::RunTaskExecutionLoop() {
  /// If periodic asio stats print is enabled, it will print it.
  const auto asio_stats_print_interval_ms =
      RayConfig::instance().asio_stats_print_interval_ms();
  if (asio_stats_print_interval_ms != -1 &&
      RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    periodical_runner_.RunFnPeriodically(
        [this, self = shared_from_this()] {
          RAY_LOG(INFO) << "Event loop stats:\n\n" << io_service_.StatsString() << "\n\n";
        },
        asio_stats_print_interval_ms);
  }

  task_execution_service_.run();
}

Status CoreWorker::AllocateReturnObject(const ObjectID &object_id,
                                        const size_t &data_size,
                                        const std::shared_ptr<Buffer> &metadata,
                                        const std::vector<ObjectID> &contained_object_id,
                                        std::shared_ptr<RayObject> *return_object) {
  rpc::Address owner_address(options_.is_local_mode
                                 ? rpc::Address()
                                 : worker_context_.GetCurrentTask()->CallerAddress());

  bool object_already_exists = false;
  std::shared_ptr<Buffer> data_buffer;
  if (data_size > 0) {
    RAY_LOG(DEBUG) << "Creating return object " << object_id;
    // Mark this object as containing other object IDs. The ref counter will
    // keep the inner IDs in scope until the outer one is out of scope.
    if (!contained_object_id.empty() && !options_.is_local_mode) {
      reference_counter_->AddNestedObjectIds(object_id, contained_object_id,
                                             owner_address);
    }

    // Allocate a buffer for the return object.
    if (options_.is_local_mode ||
        static_cast<int64_t>(data_size) < max_direct_call_returned_object_size_) {
      data_buffer = std::make_shared<LocalMemoryBuffer>(data_size);
    } else {
      RAY_RETURN_NOT_OK(
          CreateExisting(metadata, data_size, object_id, owner_address, &data_buffer));
      object_already_exists = !data_buffer;
    }
  }
  // Leave the return object as a nullptr if the object already exists.
  if (!object_already_exists) {
    *return_object =
        std::make_shared<RayObject>(data_buffer, metadata, contained_object_id);
  }

  return Status::OK();
}

Status CoreWorker::ExecuteTask(const TaskSpecification &task_spec,
                               const std::shared_ptr<ResourceMappingType> &resource_ids,
                               std::vector<std::shared_ptr<RayObject>> *return_objects,
                               ReferenceCounter::ReferenceTableProto *borrowed_refs,
                               bool *is_application_level_error) {
  RAY_LOG(DEBUG) << "Executing task, task info = " << task_spec.DebugString();
  task_queue_length_ -= 1;
  num_executed_tasks_ += 1;
  UpdateTaskStats();

  if (!options_.is_local_mode) {
    worker_context_.SetCurrentTask(task_spec);
    SetCurrentTaskId(task_spec.TaskId());
    ray::RayEventContext::Instance().SetCustomField("task_id", task_spec.TaskId().Hex());
  }
  {
    absl::MutexLock lock(&mutex_);
    current_task_ = task_spec;
    if (resource_ids) {
      resource_ids_ = resource_ids;
    }
  }

  RayFunction func{task_spec.GetLanguage(), task_spec.FunctionDescriptor()};

  std::vector<std::shared_ptr<RayObject>> args;
  std::vector<ObjectID> arg_reference_ids;
  // This includes all IDs that were passed by reference and any IDs that were
  // inlined in the task spec. These references will be pinned during the task
  // execution and unpinned once the task completes. We will notify the caller
  // about any IDs that we are still borrowing by the time the task completes.
  std::vector<ObjectID> borrowed_ids;
  RAY_CHECK_OK(
      GetAndPinArgsForExecutor(task_spec, &args, &arg_reference_ids, &borrowed_ids));

  std::vector<ObjectID> return_ids;
  for (size_t i = 0; i < task_spec.NumReturns(); i++) {
    return_ids.push_back(task_spec.ReturnId(i));
  }

  task_stats_->AddTaskReceived(func);
  running_task_stats_->TaskStarted(task_spec);

  Status status;
  TaskType task_type = TaskType::NORMAL_TASK;
  if (task_spec.IsActorCreationTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_CREATION_TASK;
    SetActorId(task_spec.ActorCreationId());
    {
      std::unique_ptr<ActorHandle> self_actor_handle(
          new ActorHandle(task_spec.GetSerializedActorHandle()));
      // Register the handle to the current actor itself.
      actor_manager_->RegisterActorHandle(std::move(self_actor_handle), ObjectID::Nil(),
                                          CurrentCallSite(), rpc_address_,
                                          /*is_self=*/true);
    }
    RAY_LOG(INFO) << "Creating actor: " << task_spec.ActorCreationId();
  } else if (task_spec.IsActorTask()) {
    RAY_CHECK(return_ids.size() > 0);
    return_ids.pop_back();
    task_type = TaskType::ACTOR_TASK;
  }

  // Because we support concurrent actor calls, we need to update the
  // worker ID for the current thread.
  CoreWorkerProcess::SetCurrentThreadWorkerId(GetWorkerID());

  std::shared_ptr<LocalMemoryBuffer> creation_task_exception_pb_bytes = nullptr;

  status = options_.task_execution_callback(
      task_type, task_spec.GetName(), func,
      task_spec.GetRequiredResources().GetResourceMap(), args, arg_reference_ids,
      return_ids, task_spec.GetDebuggerBreakpoint(), return_objects,
      creation_task_exception_pb_bytes, is_application_level_error);
  // Get the reference counts for any IDs that we borrowed during this task and
  // return them to the caller. This will notify the caller of any IDs that we
  // (or a nested task) are still borrowing. It will also notify the caller of
  // any new IDs that were contained in a borrowed ID that we (or a nested
  // task) are now borrowing.
  if (!borrowed_ids.empty()) {
    reference_counter_->GetAndClearLocalBorrowers(borrowed_ids, borrowed_refs);
  }
  // Unpin the borrowed IDs.
  std::vector<ObjectID> deleted;
  for (const auto &borrowed_id : borrowed_ids) {
    RAY_LOG(DEBUG) << "Decrementing ref for borrowed ID " << borrowed_id;
    reference_counter_->RemoveLocalReference(borrowed_id, &deleted);
  }
  if (options_.ref_counting_enabled) {
    memory_store_->Delete(deleted);
  }

  if (task_spec.IsNormalTask() && reference_counter_->NumObjectIDsInScope() != 0) {
    RAY_LOG(DEBUG)
        << "There were " << reference_counter_->NumObjectIDsInScope()
        << " ObjectIDs left in scope after executing task " << task_spec.TaskId()
        << ". This is either caused by keeping references to ObjectIDs in Python "
           "between "
           "tasks (e.g., in global variables) or indicates a problem with Ray's "
           "reference counting, and may cause problems in the object store.";
  }

  running_task_stats_->TaskFinished(task_spec);
  if (!options_.is_local_mode) {
    SetCurrentTaskId(TaskID::Nil());
    worker_context_.ResetCurrentTask();
    ray::RayEventContext::Instance().SetCustomField("task_id", "");
  }
  {
    absl::MutexLock lock(&mutex_);
    current_task_ = TaskSpecification();
    if (task_spec.IsNormalTask()) {
      resource_ids_.reset(new ResourceMappingType());
    }
  }
  RAY_LOG(DEBUG) << "Finished executing task " << task_spec.TaskId()
                 << ", status=" << status;
  if (status.IsCreationTaskError()) {
    Exit(rpc::WorkerExitType::CREATION_TASK_ERROR, creation_task_exception_pb_bytes);
  } else if (status.IsIntentionalSystemExit()) {
    Exit(rpc::WorkerExitType::INTENDED_EXIT, creation_task_exception_pb_bytes);
  } else if (status.IsUnexpectedSystemExit()) {
    Exit(rpc::WorkerExitType::SYSTEM_ERROR_EXIT, creation_task_exception_pb_bytes);
  } else if (!status.ok()) {
    RAY_LOG(FATAL) << "Unexpected task status type : " << status;
  }

  return status;
}

Status CoreWorker::SealReturnObject(const ObjectID &return_id,
                                    std::shared_ptr<RayObject> return_object) {
  Status status = Status::OK();
  if (!return_object) {
    return status;
  }
  std::shared_ptr<rpc::Address> caller_address =
      options_.is_local_mode ? nullptr
                             : std::make_shared<rpc::Address>(
                                   worker_context_.GetCurrentTask()->CallerAddress());
  if (return_object->GetData() != nullptr && return_object->GetData()->IsPlasmaBuffer()) {
    status = SealExisting(return_id, /*pin_object=*/true, caller_address);
    if (!status.ok()) {
      RAY_LOG(FATAL) << "Failed to seal object " << return_id
                     << " in store: " << status.message();
    }
  }
  return status;
}

void CoreWorker::ExecuteTaskLocalMode(const TaskSpecification &task_spec,
                                      const ActorID &actor_id) {
  auto resource_ids = std::make_shared<ResourceMappingType>();
  auto return_objects = std::vector<std::shared_ptr<RayObject>>();
  auto borrowed_refs = ReferenceCounter::ReferenceTableProto();
  if (!task_spec.IsActorCreationTask()) {
    for (size_t i = 0; i < task_spec.NumReturns(); i++) {
      reference_counter_->AddOwnedObject(task_spec.ReturnId(i),
                                         /*inner_ids=*/{}, rpc_address_,
                                         CurrentCallSite(), -1,
                                         /*is_reconstructable=*/false);
    }
  }
  auto old_id = GetActorId();
  SetActorId(actor_id);
  bool is_application_level_error;
  RAY_UNUSED(ExecuteTask(task_spec, resource_ids, &return_objects, &borrowed_refs,
                         &is_application_level_error));
  SetActorId(old_id);
}

Status CoreWorker::GetAndPinArgsForExecutor(const TaskSpecification &task,
                                            std::vector<std::shared_ptr<RayObject>> *args,
                                            std::vector<ObjectID> *arg_reference_ids,
                                            std::vector<ObjectID> *borrowed_ids) {
  auto num_args = task.NumArgs();
  args->resize(num_args);
  arg_reference_ids->resize(num_args);

  absl::flat_hash_set<ObjectID> by_ref_ids;
  absl::flat_hash_map<ObjectID, std::vector<size_t>> by_ref_indices;

  for (size_t i = 0; i < task.NumArgs(); ++i) {
    if (task.ArgByRef(i)) {
      // We need to put an OBJECT_IN_PLASMA error here so the subsequent call to Get()
      // properly redirects to the plasma store.
      if (!options_.is_local_mode) {
        RAY_UNUSED(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA),
                                      task.ArgId(i)));
      }
      const auto &arg_id = task.ArgId(i);
      by_ref_ids.insert(arg_id);
      auto it = by_ref_indices.find(arg_id);
      if (it == by_ref_indices.end()) {
        by_ref_indices.emplace(arg_id, std::vector<size_t>({i}));
      } else {
        it->second.push_back(i);
      }
      arg_reference_ids->at(i) = arg_id;
      // Pin all args passed by reference for the duration of the task.  This
      // ensures that when the task completes, we can retrieve metadata about
      // any borrowed ObjectIDs that were serialized in the argument's value.
      RAY_LOG(DEBUG) << "Incrementing ref for argument ID " << arg_id;
      reference_counter_->AddLocalReference(arg_id, task.CallSiteString());
      // Attach the argument's owner's address. This is needed to retrieve the
      // value from plasma.
      reference_counter_->AddBorrowedObject(arg_id, ObjectID::Nil(),
                                            task.ArgRef(i).owner_address());
      borrowed_ids->push_back(arg_id);
    } else {
      // A pass-by-value argument.
      std::shared_ptr<LocalMemoryBuffer> data = nullptr;
      if (task.ArgDataSize(i)) {
        data = std::make_shared<LocalMemoryBuffer>(const_cast<uint8_t *>(task.ArgData(i)),
                                                   task.ArgDataSize(i));
      }
      std::shared_ptr<LocalMemoryBuffer> metadata = nullptr;
      if (task.ArgMetadataSize(i)) {
        metadata = std::make_shared<LocalMemoryBuffer>(
            const_cast<uint8_t *>(task.ArgMetadata(i)), task.ArgMetadataSize(i));
      }
      // NOTE: this is a workaround to avoid an extra copy for Java workers.
      // Python workers need this copy to pass test case
      // test_inline_arg_memory_corruption.
      bool copy_data = options_.language == Language::PYTHON;
      args->at(i) =
          std::make_shared<RayObject>(data, metadata, task.ArgInlinedIds(i), copy_data);
      arg_reference_ids->at(i) = ObjectID::Nil();
      // The task borrows all ObjectIDs that were serialized in the inlined
      // arguments. The task will receive references to these IDs, so it is
      // possible for the task to continue borrowing these arguments by the
      // time it finishes.
      for (const auto &inlined_id : task.ArgInlinedIds(i)) {
        RAY_LOG(DEBUG) << "Incrementing ref for borrowed ID " << inlined_id;
        // We do not need to add the ownership information here because it will
        // get added once the language frontend deserializes the value, before
        // the ObjectID can be used.
        reference_counter_->AddLocalReference(inlined_id, task.CallSiteString());
        borrowed_ids->push_back(inlined_id);
      }
    }
  }

  // Fetch by-reference arguments directly from the plasma store.
  bool got_exception = false;
  bool fetched_local;
  absl::flat_hash_map<ObjectID, std::shared_ptr<RayObject>> result_map;
  if (options_.is_local_mode) {
    RAY_RETURN_NOT_OK(
        memory_store_->Get(by_ref_ids, -1, worker_context_, &result_map, &got_exception));
  } else {
    RAY_RETURN_NOT_OK(plasma_store_provider_->Get(
        by_ref_ids, -1, worker_context_, &result_map, &got_exception, &fetched_local));
  }
  for (const auto &it : result_map) {
    for (size_t idx : by_ref_indices[it.first]) {
      args->at(idx) = it.second;
    }
  }

  return Status::OK();
}

void CoreWorker::HandlePushTask(const rpc::PushTaskRequest &request,
                                rpc::PushTaskReply *reply,
                                rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Increment the task_queue_length
  task_queue_length_ += 1;
  UpdateTaskStats();

  // Avoid copying rpc::PushTaskRequest. Instead, we use swap here.
  auto copied_request = std::make_shared<rpc::PushTaskRequest>();
  copied_request->Swap(const_cast<rpc::PushTaskRequest *>(&request));

  // For actor tasks, we just need to post a HandleActorTask instance to the task
  // execution service.
  if (copied_request->task_spec().type() == TaskType::ACTOR_TASK) {
    task_execution_service_.post(
        std::bind(
            [this, copied_request, reply](rpc::SendReplyCallback &send_reply_callback) {
              // We have posted an exit task onto the main event loop,
              // so shouldn't bother executing any further work.
              if (exiting_) return;
              direct_task_receiver_->HandleTask(*copied_request, reply,
                                                std::move(send_reply_callback));
            },
            std::move(send_reply_callback)),
        "CoreWorker.HandlePushTaskActor");
  } else {
    // Normal tasks are enqueued here, and we post a RunNormalTasksFromQueue instance to
    // the task execution service.
    direct_task_receiver_->HandleTask(*copied_request, reply, send_reply_callback);
    task_execution_service_.post(
        [this] {
          // We have posted an exit task onto the main event loop,
          // so shouldn't bother executing any further work.
          if (exiting_) return;
          direct_task_receiver_->RunNormalTasksFromQueue();
        },
        "CoreWorker.HandlePushTask");
  }
}

void CoreWorker::HandleDirectActorCallArgWaitComplete(
    const rpc::DirectActorCallArgWaitCompleteRequest &request,
    rpc::DirectActorCallArgWaitCompleteReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Post on the task execution event loop since this may trigger the
  // execution of a task that is now ready to run.
  task_execution_service_.post(
      [this, request] {
        RAY_LOG(DEBUG) << "Arg wait complete for tag " << request.tag();
        task_argument_waiter_->OnWaitComplete(request.tag());
      },
      "CoreWorker.ArgWaitComplete");

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleGetObjectStatus(const rpc::GetObjectStatusRequest &request,
                                       rpc::GetObjectStatusReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.owner_worker_id()),
                           send_reply_callback)) {
    RAY_LOG(INFO) << "Handling GetObjectStatus for object produced by a previous worker "
                     "with the same address";
    return;
  }

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Received GetObjectStatus " << object_id;
  // Acquire a reference to the object. This prevents the object from being
  // evicted out from under us while we check the object status and start the
  // Get.
  AddLocalReference(object_id, "<temporary (get object status)>");

  rpc::Address owner_address;
  auto has_owner = reference_counter_->GetOwner(object_id, &owner_address);
  if (!has_owner) {
    // We owned this object, but the object has gone out of scope.
    reply->set_status(rpc::GetObjectStatusReply::OUT_OF_SCOPE);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    RAY_CHECK(owner_address.worker_id() == request.owner_worker_id());
    bool is_freed = reference_counter_->IsPlasmaObjectFreed(object_id);

    // Send the reply once the value has become available. The value is
    // guaranteed to become available eventually because we own the object and
    // its ref count is > 0.
    memory_store_->GetAsync(object_id, [this, object_id, reply, send_reply_callback,
                                        is_freed](std::shared_ptr<RayObject> obj) {
      if (is_freed) {
        reply->set_status(rpc::GetObjectStatusReply::FREED);
      } else {
        // If obj is the concrete object value, it is small, so we
        // send the object back to the caller in the GetObjectStatus
        // reply, bypassing a Plasma put and object transfer. If obj
        // is an indicator that the object is in Plasma, we set an
        // in_plasma indicator on the message, and the caller will
        // have to facilitate a Plasma object transfer to get the
        // object value.
        auto *object = reply->mutable_object();
        if (obj->HasData()) {
          const auto &data = obj->GetData();
          object->set_data(data->Data(), data->Size());
        }
        if (obj->HasMetadata()) {
          const auto &metadata = obj->GetMetadata();
          object->set_metadata(metadata->Data(), metadata->Size());
        }
        for (const auto &nested_id : obj->GetNestedIds()) {
          object->add_nested_inlined_ids(nested_id.Binary());
        }
        reply->set_status(rpc::GetObjectStatusReply::CREATED);
        // Set locality data.
        const auto &locality_data = reference_counter_->GetLocalityData(object_id);
        if (locality_data.has_value()) {
          for (const auto &node_id : locality_data.value().nodes_containing_object) {
            reply->add_node_ids(node_id.Binary());
          }
          reply->set_object_size(locality_data.value().object_size);
        }
      }
      send_reply_callback(Status::OK(), nullptr, nullptr);
    });
  }

  RemoveLocalReference(object_id);
}  // namespace ray

void CoreWorker::HandleWaitForActorOutOfScope(
    const rpc::WaitForActorOutOfScopeRequest &request,
    rpc::WaitForActorOutOfScopeReply *reply, rpc::SendReplyCallback send_reply_callback) {
  // Currently WaitForActorOutOfScope is only used when GCS actor service is enabled.
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  // Send a response to trigger cleaning up the actor state once the handle is
  // no longer in scope.
  auto respond = [send_reply_callback](const ActorID &actor_id) {
    RAY_LOG(DEBUG) << "Replying to HandleWaitForActorOutOfScope for " << actor_id;
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };

  const auto actor_id = ActorID::FromBinary(request.actor_id());
  RAY_LOG(DEBUG) << "Received HandleWaitForActorOutOfScope for " << actor_id;
  actor_manager_->WaitForActorOutOfScope(actor_id, std::move(respond));
}

void CoreWorker::HandleSubscribeForObjectEviction(
    const rpc::SubscribeForObjectEvictionRequest &request,
    rpc::SubscribeForObjectEvictionReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  // TODO(swang): Drop requests from raylets that executed an older version of
  // the task.
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  const auto subscriber_node_id =
      NodeID::FromBinary(request.subscriber_address().raylet_id());
  // Send a response to trigger unpinning the object when it is no longer in scope.
  auto respond = [this, subscriber_node_id](const ObjectID &object_id) {
    RAY_LOG(DEBUG) << "Object " << object_id << " is deleted. Unpinning the object.";

    rpc::PubMessage pub_message;
    pub_message.set_key_id(object_id.Binary());
    pub_message.set_channel_type(rpc::ChannelType::WORKER_OBJECT_EVICTION);
    pub_message.mutable_worker_object_eviction_message()->set_object_id(
        object_id.Binary());

    object_status_publisher_->Publish(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                      pub_message, object_id.Binary());
    object_status_publisher_->UnregisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_node_id, object_id.Binary());
  };

  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  // Always register the subscription before we register the delete callback to avoid race
  // condition.
  object_status_publisher_->RegisterSubscription(rpc::ChannelType::WORKER_OBJECT_EVICTION,
                                                 subscriber_node_id, object_id.Binary());

  // Returns true if the object was present and the callback was added. It might have
  // already been evicted by the time we get this request, in which case we should
  // respond immediately so the raylet unpins the object.
  if (!reference_counter_->SetDeleteCallback(object_id, respond)) {
    // If the object is already evicted (callback cannot be set), unregister the
    // subscription.
    object_status_publisher_->UnregisterSubscription(
        rpc::ChannelType::WORKER_OBJECT_EVICTION, subscriber_node_id, object_id.Binary());
    std::ostringstream stream;
    stream << "Reference for object " << object_id << " has already been freed.";
    RAY_LOG(DEBUG) << stream.str();
    send_reply_callback(Status::NotFound(stream.str()), nullptr, nullptr);
  } else {
    send_reply_callback(Status::OK(), nullptr, nullptr);
  }
}

void CoreWorker::HandlePubsubLongPolling(const rpc::PubsubLongPollingRequest &request,
                                         rpc::PubsubLongPollingReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  const auto subscriber_id = NodeID::FromBinary(request.subscriber_address().raylet_id());
  RAY_LOG(DEBUG) << "Got long polling request from node " << subscriber_id;
  object_status_publisher_->ConnectToSubscriber(subscriber_id, reply,
                                                std::move(send_reply_callback));
}

void CoreWorker::HandleAddObjectLocationOwner(
    const rpc::AddObjectLocationOwnerRequest &request,
    rpc::AddObjectLocationOwnerReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  auto reference_exists = reference_counter_->AddObjectLocation(
      object_id, NodeID::FromBinary(request.node_id()));
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleRemoveObjectLocationOwner(
    const rpc::RemoveObjectLocationOwnerRequest &request,
    rpc::RemoveObjectLocationOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  auto reference_exists = reference_counter_->RemoveObjectLocation(
      object_id, NodeID::FromBinary(request.node_id()));
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleGetObjectLocationsOwner(
    const rpc::GetObjectLocationsOwnerRequest &request,
    rpc::GetObjectLocationsOwnerReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }
  auto object_id = ObjectID::FromBinary(request.object_id());
  const auto &callback = [object_id, reply, send_reply_callback](
                             const absl::flat_hash_set<NodeID> &locations,
                             int64_t object_size, const std::string &spilled_url,
                             const NodeID &spilled_node_id, int64_t current_version) {
    RAY_LOG(DEBUG) << "Replying to HandleGetObjectLocationsOwner for " << object_id
                   << " with location update version " << current_version << ", "
                   << locations.size() << " locations, spilled url: " << spilled_url
                   << ", spilled node ID: " << spilled_node_id
                   << ", and object size: " << object_size;
    for (const auto &node_id : locations) {
      reply->add_node_ids(node_id.Binary());
    }
    reply->set_object_size(object_size);
    reply->set_spilled_url(spilled_url);
    reply->set_spilled_node_id(spilled_node_id.Binary());
    reply->set_current_version(current_version);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  };
  auto status = reference_counter_->SubscribeObjectLocations(
      object_id, request.last_version(), callback);
  if (!status.ok()) {
    send_reply_callback(status, nullptr, nullptr);
  }
}

void CoreWorker::HandleWaitForRefRemoved(const rpc::WaitForRefRemovedRequest &request,
                                         rpc::WaitForRefRemovedReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  if (HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()),
                           send_reply_callback)) {
    return;
  }

  const ObjectID &object_id = ObjectID::FromBinary(request.reference().object_id());
  ObjectID contained_in_id = ObjectID::FromBinary(request.contained_in_id());
  const WorkerID subscriber_worker_id =
      WorkerID::FromBinary(request.subscriber_worker_id());
  const auto owner_address = request.reference().owner_address();

  // We need to reply first to avoid race condition where publish
  // happens before subscriber receives the reply.
  // Note that this function should be called after accessing fields from the request.
  // Otherwise, request could be GC'ed.
  send_reply_callback(Status::OK(), nullptr, nullptr);

  object_status_publisher_->RegisterSubscription(
      rpc::ChannelType::WORKER_REF_REMOVED_CHANNEL, subscriber_worker_id,
      object_id.Binary());
  // Set a callback to publish the message when the requested object ID's ref count
  // goes to 0.
  auto ref_removed_callback =
      boost::bind(&ReferenceCounter::HandleRefRemoved, reference_counter_, object_id,
                  subscriber_worker_id);
  reference_counter_->SetRefRemovedCallback(object_id, contained_in_id, owner_address,
                                            ref_removed_callback);
}

void CoreWorker::HandleRemoteCancelTask(const rpc::RemoteCancelTaskRequest &request,
                                        rpc::RemoteCancelTaskReply *reply,
                                        rpc::SendReplyCallback send_reply_callback) {
  auto status = CancelTask(ObjectID::FromBinary(request.remote_object_id()),
                           request.force_kill(), request.recursive());
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleCancelTask(const rpc::CancelTaskRequest &request,
                                  rpc::CancelTaskReply *reply,
                                  rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  TaskID task_id = TaskID::FromBinary(request.intended_task_id());
  bool requested_task_running = main_thread_task_id_ == task_id;
  bool success = requested_task_running;

  // Try non-force kill
  if (requested_task_running && !request.force_kill()) {
    RAY_LOG(INFO) << "Interrupting a running task " << main_thread_task_id_;
    success = options_.kill_main();
  } else if (!requested_task_running) {
    // If the task is not currently running, check if it is in the worker's queue of
    // normal tasks, and remove it if found.
    success = direct_task_receiver_->CancelQueuedNormalTask(task_id);
  }
  if (request.recursive()) {
    auto recursive_cancel = CancelChildren(task_id, request.force_kill());
    if (recursive_cancel.ok()) {
      RAY_LOG(INFO) << "Recursive cancel failed!";
    }
  }

  // TODO: fix race condition to avoid using this hack
  requested_task_running = main_thread_task_id_ == task_id;

  reply->set_attempt_succeeded(success);
  send_reply_callback(Status::OK(), nullptr, nullptr);

  // Do force kill after reply callback sent
  if (requested_task_running && request.force_kill()) {
    RAY_LOG(INFO) << "Force killing a worker running " << main_thread_task_id_;
    RAY_EVENT(INFO, EVENT_LABEL_PROCESS_EXIT)
        << "Force killing a worker running " << main_thread_task_id_;
    Disconnect();
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // NOTE(hchen): Use `_Exit()` to force-exit this process without doing cleanup.
    // `exit()` will destruct static objects in an incorrect order, which will lead to
    // core dumps.
    _Exit(1);
  }
}

void CoreWorker::HandleKillActor(const rpc::KillActorRequest &request,
                                 rpc::KillActorReply *reply,
                                 rpc::SendReplyCallback send_reply_callback) {
  ActorID intended_actor_id = ActorID::FromBinary(request.intended_actor_id());
  if (intended_actor_id != worker_context_.GetCurrentActorID()) {
    std::ostringstream stream;
    stream << "Mismatched ActorID: ignoring KillActor for previous actor "
           << intended_actor_id
           << ", current actor ID: " << worker_context_.GetCurrentActorID();
    auto msg = stream.str();
    RAY_LOG(ERROR) << msg;
    send_reply_callback(Status::Invalid(msg), nullptr, nullptr);
    return;
  }

  RAY_EVENT(INFO, EVENT_LABEL_PROCESS_EXIT)
      << "Killing actor: " << intended_actor_id.Hex();

  if (request.force_kill()) {
    RAY_LOG(INFO) << "Got KillActor, exiting immediately...";
    if (request.no_restart()) {
      Disconnect();
    }

    ray::EventManager::Instance().ClearReporters();

    if (options_.num_workers > 1) {
      // TODO (kfstorm): Should we add some kind of check before sending the killing
      // request?
      RAY_LOG(ERROR)
          << "Killing an actor which is running in a worker process with multiple "
             "workers will also kill other actors in this process. To avoid this, "
             "please create the Java actor with some dynamic options to make it being "
             "hosted in a dedicated worker process.";
    }
    if (options_.enable_logging) {
      RayLog::ShutDownRayLog();
    }
    // NOTE(hchen): Use `_Exit()` to force-exit this process without doing cleanup.
    // `exit()` will destruct static objects in an incorrect order, which will lead to
    // core dumps.
    _Exit(1);
  } else {
    Exit(rpc::WorkerExitType::INTENDED_EXIT);
  }
}

void CoreWorker::HandleGetCoreWorkerStats(const rpc::GetCoreWorkerStatsRequest &request,
                                          rpc::GetCoreWorkerStatsReply *reply,
                                          rpc::SendReplyCallback send_reply_callback) {
  absl::MutexLock lock(&mutex_);
  auto stats = reply->mutable_core_worker_stats();
  // TODO(swang): Differentiate between tasks that are currently pending
  // execution and tasks that have finished but may be retried.
  stats->set_num_pending_tasks(task_manager_->NumSubmissibleTasks());
  stats->set_task_queue_length(task_queue_length_);
  stats->set_num_executed_tasks(num_executed_tasks_);
  stats->set_num_object_refs_in_scope(reference_counter_->NumObjectIDsInScope());
  stats->set_current_task_name(current_task_.GetName());
  stats->set_current_task_func_desc(current_task_.FunctionDescriptor()->ToString());
  stats->set_ip_address(rpc_address_.ip_address());
  stats->set_port(rpc_address_.port());
  stats->set_pid(getpid());
  stats->set_language(options_.language);
  stats->set_job_id(worker_context_.GetCurrentJobID().Binary());
  stats->set_worker_id(worker_context_.GetWorkerID().Binary());
  stats->set_actor_id(actor_id_.Binary());
  stats->set_worker_type(worker_context_.GetWorkerType());
  auto used_resources_map = stats->mutable_used_resources();
  for (auto const &it : *resource_ids_) {
    rpc::ResourceAllocations allocations;
    for (auto const &pair : it.second) {
      auto resource_slot = allocations.add_resource_slots();
      resource_slot->set_slot(pair.first);
      resource_slot->set_allocation(pair.second);
    }
    (*used_resources_map)[it.first] = allocations;
  }
  stats->set_actor_title(actor_title_);
  google::protobuf::Map<std::string, std::string> webui_map(webui_display_.begin(),
                                                            webui_display_.end());
  (*stats->mutable_webui_display()) = webui_map;

  MemoryStoreStats memory_store_stats = memory_store_->GetMemoryStoreStatisticalData();
  stats->set_num_in_plasma(memory_store_stats.num_in_plasma);
  stats->set_num_local_objects(memory_store_stats.num_local_objects);
  stats->set_used_object_store_memory(memory_store_stats.used_object_store_memory);

  if (request.include_memory_info()) {
    reference_counter_->AddObjectRefStats(plasma_store_provider_->UsedObjectsList(),
                                          stats);
  }

  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::HandleLocalGC(const rpc::LocalGCRequest &request,
                               rpc::LocalGCReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (options_.gc_collect != nullptr) {
    // NOTE(kfstorm): The dispatch is needed here. Without it, we can't attach a current
    // thread (which is a brpc IO thread) to JVM.
    io_service_.dispatch([this, request, send_reply_callback]() {
      options_.gc_collect(request.triggered_by_global_gc());
      send_reply_callback(Status::OK(), nullptr, nullptr);
    });
  } else {
    send_reply_callback(Status::NotImplemented("GC callback not defined"), nullptr,
                        nullptr);
  }
}

void CoreWorker::HandleRunOnUtilWorker(const rpc::RunOnUtilWorkerRequest &request,
                                       rpc::RunOnUtilWorkerReply *reply,
                                       rpc::SendReplyCallback send_reply_callback) {
  if (options_.run_on_util_worker_handler) {
    std::vector<std::string> args(request.args().begin(), request.args().end());

    options_.run_on_util_worker_handler(request.request(), args);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(Status::NotImplemented("RunOnUtilWorker is not supported"),
                        nullptr, nullptr);
  }
}

void CoreWorker::HandleSpillObjects(const rpc::SpillObjectsRequest &request,
                                    rpc::SpillObjectsReply *reply,
                                    rpc::SendReplyCallback send_reply_callback) {
  if (options_.spill_objects != nullptr) {
    std::vector<ObjectID> object_ids_to_spill;
    object_ids_to_spill.reserve(request.object_ids_to_spill_size());
    for (const auto &id_binary : request.object_ids_to_spill()) {
      object_ids_to_spill.push_back(ObjectID::FromBinary(id_binary));
    }
    std::vector<std::string> owner_addresses;
    owner_addresses.reserve(request.owner_addresses_size());
    for (const auto &owner_address : request.owner_addresses()) {
      owner_addresses.push_back(owner_address.SerializeAsString());
    }
    std::vector<std::string> object_urls =
        options_.spill_objects(object_ids_to_spill, owner_addresses);
    for (size_t i = 0; i < object_urls.size(); i++) {
      reply->add_spilled_objects_url(std::move(object_urls[i]));
    }
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(Status::NotImplemented("Spill objects callback not defined"),
                        nullptr, nullptr);
  }
}

void CoreWorker::HandleAddSpilledUrl(const rpc::AddSpilledUrlRequest &request,
                                     rpc::AddSpilledUrlReply *reply,
                                     rpc::SendReplyCallback send_reply_callback) {
  const ObjectID object_id = ObjectID::FromBinary(request.object_id());
  const std::string &spilled_url = request.spilled_url();
  const NodeID node_id = NodeID::FromBinary(request.spilled_node_id());
  RAY_LOG(DEBUG) << "Received AddSpilledUrl request for object " << object_id
                 << ", which has been spilled to " << spilled_url << " on node "
                 << node_id;
  auto reference_exists = reference_counter_->HandleObjectSpilled(
      object_id, spilled_url, node_id, request.size(), /*release*/ false);
  Status status =
      reference_exists
          ? Status::OK()
          : Status::ObjectNotFound("Object " + object_id.Hex() + " not found");
  send_reply_callback(status, nullptr, nullptr);
}

void CoreWorker::HandleRestoreSpilledObjects(
    const rpc::RestoreSpilledObjectsRequest &request,
    rpc::RestoreSpilledObjectsReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (options_.restore_spilled_objects != nullptr) {
    // Get a list of object ids.
    std::vector<ObjectID> object_ids_to_restore;
    object_ids_to_restore.reserve(request.object_ids_to_restore_size());
    for (const auto &id_binary : request.object_ids_to_restore()) {
      object_ids_to_restore.push_back(ObjectID::FromBinary(id_binary));
    }
    // Get a list of spilled_object_urls.
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    auto total =
        options_.restore_spilled_objects(object_ids_to_restore, spilled_objects_url);
    reply->set_bytes_restored_total(total);
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Restore spilled objects callback not defined"), nullptr,
        nullptr);
  }
}

void CoreWorker::HandleDeleteSpilledObjects(
    const rpc::DeleteSpilledObjectsRequest &request,
    rpc::DeleteSpilledObjectsReply *reply, rpc::SendReplyCallback send_reply_callback) {
  if (options_.delete_spilled_objects != nullptr) {
    std::vector<std::string> spilled_objects_url;
    spilled_objects_url.reserve(request.spilled_objects_url_size());
    for (const auto &url : request.spilled_objects_url()) {
      spilled_objects_url.push_back(url);
    }
    options_.delete_spilled_objects(spilled_objects_url, worker_context_.GetWorkerType());
    send_reply_callback(Status::OK(), nullptr, nullptr);
  } else {
    send_reply_callback(
        Status::NotImplemented("Delete spilled objects callback not defined"), nullptr,
        nullptr);
  }
}

void CoreWorker::HandleExit(const rpc::ExitRequest &request, rpc::ExitReply *reply,
                            rpc::SendReplyCallback send_reply_callback) {
  bool own_objects = reference_counter_->OwnObjects();
  int64_t pins_in_flight = local_raylet_client_->GetPinsInFlight();
  // We consider the worker to be idle if it doesn't own any objects and it doesn't have
  // any object pinning RPCs in flight.
  bool is_idle = !own_objects && pins_in_flight == 0;
  reply->set_success(is_idle);
  send_reply_callback(Status::OK(),
                      [this, is_idle]() {
                        // If the worker is idle, we exit.
                        if (is_idle) {
                          Exit(rpc::WorkerExitType::INTENDED_EXIT);
                        }
                      },
                      // We need to kill it regardless if the RPC failed.
                      [this]() { Exit(rpc::WorkerExitType::INTENDED_EXIT); });
}

void CoreWorker::HandleBatchAssignObjectOwner(
    const rpc::BatchAssignObjectOwnerRequest &request,
    rpc::BatchAssignObjectOwnerReply *reply, rpc::SendReplyCallback send_reply_callback) {
  const auto &borrower_address = request.borrower_address();
  NodeID borrower_node_id = NodeID::FromBinary(borrower_address.raylet_id());

  for (int index = 0; index < request.object_ids().size(); index++) {
    const std::string object_id_binary = request.object_ids()[index];
    const size_t object_size = request.object_sizes()[index];
    const rpc::ContainedObjects contained_object_binaries =
        request.contained_objects()[index];
    const std::string call_site = request.call_sites()[index];

    ObjectID object_id = ObjectID::FromBinary(object_id_binary);
    // Get a list of contained object ids.
    std::vector<ObjectID> contained_object_ids;
    for (const auto &id_binary : contained_object_binaries.object_ids()) {
      contained_object_ids.push_back(ObjectID::FromBinary(id_binary));
    }

    reference_counter_->AddOwnedObject(object_id, contained_object_ids, rpc_address_,
                                       call_site, object_size,
                                       /*is_reconstructable=*/false,
                                       /*pinned_at_raylet_id=*/borrower_node_id);
    reference_counter_->AddBorrowerAddress(object_id, borrower_address);
    RAY_CHECK(memory_store_->Put(RayObject(rpc::ErrorType::OBJECT_IN_PLASMA), object_id));
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::YieldCurrentFiber(FiberEvent &event) {
  RAY_CHECK(worker_context_.CurrentActorIsAsync());
  auto context = worker_context_.GetThreadContextCopy();
  boost::this_fiber::yield();
  event.Wait();
  worker_context_.SetThreadContext(context);
}

void CoreWorker::GetAsync(const ObjectID &object_id, SetResultCallback success_callback,
                          void *python_future) {
  auto fallback_callback =
      std::bind(&CoreWorker::PlasmaCallback, this, success_callback,
                std::placeholders::_1, std::placeholders::_2, std::placeholders::_3);

  memory_store_->GetAsync(object_id, [python_future, success_callback, fallback_callback,
                                      object_id](std::shared_ptr<RayObject> ray_object) {
    if (ray_object->IsInPlasmaError()) {
      fallback_callback(ray_object, object_id, python_future);
    } else {
      success_callback(ray_object, object_id, python_future);
    }
  });
}

void CoreWorker::PlasmaCallback(SetResultCallback success,
                                std::shared_ptr<RayObject> ray_object, ObjectID object_id,
                                void *py_future) {
  RAY_CHECK(ray_object->IsInPlasmaError());

  // First check if the object is available in local plasma store.
  // Note that we are using Contains instead of Get so it won't trigger pull request
  // to remote nodes.
  bool object_is_local = false;
  if (Contains(object_id, &object_is_local).ok() && object_is_local) {
    std::vector<std::shared_ptr<RayObject>> vec;
    if (Get(std::vector<ObjectID>{object_id}, 0, &vec).ok()) {
      RAY_CHECK(vec.size() > 0)
          << "Failed to get local object but Raylet notified object is local.";
      return success(vec.front(), object_id, py_future);
    }
  }

  // Object is not available locally. We now add the callback to listener queue.
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto plasma_arrived_callback = [this, success, object_id, py_future]() {
      // This callback is invoked on the io_service_ event loop, so it cannot call
      // blocking call like Get(). We used GetAsync here, which should immediate call
      // PlasmaCallback again with object available locally.
      GetAsync(object_id, success, py_future);
    };

    async_plasma_callbacks_[object_id].push_back(plasma_arrived_callback);
  }

  // Ask raylet to subscribe to object notification. Raylet will call this core worker
  // when the object is local (and it will fire the callback immediately if the object
  // exists). CoreWorker::HandlePlasmaObjectReady handles such request.
  local_raylet_client_->SubscribeToPlasma(object_id, GetOwnerAddress(object_id));
}

void CoreWorker::HandlePlasmaObjectReady(const rpc::PlasmaObjectReadyRequest &request,
                                         rpc::PlasmaObjectReadyReply *reply,
                                         rpc::SendReplyCallback send_reply_callback) {
  std::vector<std::function<void(void)>> callbacks;
  {
    absl::MutexLock lock(&plasma_mutex_);
    auto it = async_plasma_callbacks_.extract(ObjectID::FromBinary(request.object_id()));
    callbacks = it.mapped();
  }
  for (auto callback : callbacks) {
    // This callback needs to be asynchronous because it runs on the io_service_, so no
    // RPCs can be processed while it's running. This can easily lead to deadlock (for
    // example if the callback calls ray.get() on an object that is dependent on an RPC
    // to be ready).
    callback();
  }
  send_reply_callback(Status::OK(), nullptr, nullptr);
}

void CoreWorker::SetActorId(const ActorID &actor_id) {
  absl::MutexLock lock(&mutex_);
  if (!options_.is_local_mode) {
    RAY_CHECK(actor_id_.IsNil());
  }
  actor_id_ = actor_id;
}

void CoreWorker::OnPlacementGroupBundlesChanged(
    const rpc::PlacementGroupBundlesChangedNotification &notification) {
  absl::MutexLock lock(&placement_group_valid_bundles_map_mutex_);
  const PlacementGroupID &placement_group_id =
      PlacementGroupID::FromBinary(notification.placement_group_id());
  const auto bundle_index_it =
      placement_group_valid_bundle_index_.find(placement_group_id);
  // It can't be null because we'll get the placement group from GCS when first using it.
  RAY_CHECK(bundle_index_it != placement_group_valid_bundle_index_.end());
  bundle_index_it->second.clear();

  LogBundlesChangedEventDebugInfo(notification);
  for (int index = 0; index < notification.bundles_size(); index++) {
    if (notification.bundles(index)) {
      bundle_index_it->second.insert({index, true});
    } else {
      bundle_index_it->second.insert({index, false});
    }
  }
}

const absl::flat_hash_map<int32_t, bool>
    &CoreWorker::GetAllAndSubscribePlacementGroupBundleEvent(
        const PlacementGroupID &placement_group_id) {
  absl::MutexLock lock(&placement_group_valid_bundles_map_mutex_);
  const auto bundle_index_it =
      placement_group_valid_bundle_index_.find(placement_group_id);
  if (bundle_index_it == placement_group_valid_bundle_index_.end()) {
    RAY_LOG(DEBUG) << "It's the first time occuring placement group: "
                   << placement_group_id
                   << ", will register the listener of bundles changed and get the "
                      "information from GCS immediately.";
    // Register a valid bundles index listener and get all bundles at the
    // first time.
    const auto bundles_changed_notification_handler =
        [this, placement_group_id](
            const PlacementGroupID &updated_placement_group_id,
            const rpc::PlacementGroupBundlesChangedNotification &notification) {
          RAY_CHECK(updated_placement_group_id == placement_group_id);
          OnPlacementGroupBundlesChanged(notification);
        };
    RAY_CHECK_OK(gcs_client_->PlacementGroups().AsyncSubscribeBundlesChangedEvent(
        placement_group_id, bundles_changed_notification_handler,
        [placement_group_id](const ray::Status &status) {
          RAY_CHECK_OK(status);
          RAY_LOG(INFO)
              << "Finished subscribing the change of bundles for placement group: "
              << placement_group_id;
        }));
    // Get all placement group bundles immediately.
    std::promise<boost::optional<rpc::PlacementGroupTableData>> promise;
    RAY_UNUSED(gcs_client_->PlacementGroups().AsyncGet(
        placement_group_id,
        [&promise](const Status &status,
                   const boost::optional<rpc::PlacementGroupTableData> &result) {
          RAY_CHECK(result);
          promise.set_value(result);
        }));
    // Wait for the get operation done.
    const auto &result = promise.get_future().get();
    placement_group_valid_bundle_index_.insert({placement_group_id, {}});
    const auto valid_bundle_index_it =
        placement_group_valid_bundle_index_.find(placement_group_id);

    for (int i = 0; i < result->bundles_size(); i++) {
      const auto &bundle = result->bundles(i);
      int32_t bundle_index = bundle.bundle_id().bundle_index();
      if (bundle.is_valid()) {
        valid_bundle_index_it->second.insert({bundle_index, true});
      } else {
        valid_bundle_index_it->second.insert({bundle_index, false});
      }
    }
  }
  const auto valid_bundle_index_it =
      placement_group_valid_bundle_index_.find(placement_group_id);
  RAY_CHECK(valid_bundle_index_it != placement_group_valid_bundle_index_.end());
  return valid_bundle_index_it->second;
}

void CoreWorker::SetWebuiDisplay(const std::string &key, const std::string &message) {
  absl::MutexLock lock(&mutex_);
  webui_display_[key] = message;
}

void CoreWorker::SetActorTitle(const std::string &title) {
  absl::MutexLock lock(&mutex_);
  actor_title_ = title;
}

const rpc::JobConfig &CoreWorker::GetJobConfig() const { return *job_config_; }

std::shared_ptr<gcs::GcsClient> CoreWorker::GetGcsClient() const { return gcs_client_; }

bool CoreWorker::IsExiting() const { return exiting_; }

void CoreWorker::UpdateTaskStats() {
  task_stats_->UpdateTaskQueueLength(task_queue_length_.load());
  task_stats_->UpdateExecutedTasksCount(num_executed_tasks_.load());
}

bool CoreWorkerProcess::SubscribeActorStateChangeEvents(
    ActorEventCallbackType actor_event_callback) {
  EnsureInitialized();
  RAY_LOG(INFO) << "Subscribing actor state change events.";
  auto subscribe = [actor_event_callback](ActorID actor_id,
                                          rpc::ActorTableData actor_data) {
    actor_event_callback(actor_id, actor_data);
  };
  std::promise<bool> promise;
  auto status = core_worker_process->gcs_client_->Actors().AsyncSubscribeAll(
      subscribe, /*done=*/[&promise](Status status) { promise.set_value(true); });
  const bool result = promise.get_future().get();
  return status.ok() && result;
}

bool CoreWorkerProcess::SubscribeActorMigrationEvents(
    ActorMigrationEventCallbackType event_callback) {
  EnsureInitialized();
  RAY_LOG(INFO) << "Subscribing actor migration events.";
  std::promise<bool> promise;
  auto status = core_worker_process->gcs_client_->ActorMigration()
                    .AsyncSubscribeActorMigrationEvents(
                        event_callback,
                        /*done=*/[&promise](Status status) { promise.set_value(true); });
  const bool result = promise.get_future().get();
  return status.ok() && result;
}

Status CoreWorker::WaitForObjectOwnerReply(const ObjectID &object_id,
                                           const rpc::Address &owner_address) {
  std::promise<Status> promise;
  io_service_.post(
      [&object_id, &promise, &owner_address, this]() {
        object_barrier_->TrySendAssignOwnerRequest(object_id, owner_address);
        object_barrier_->AsyncWaitForAssignmentFinish(
            object_id, [&promise](const Status &status) { promise.set_value(status); });
      },
      "CoreWorker.WaitForObjectOwnerReply");
  return promise.get_future().get();
}

std::vector<ObjectID> CoreWorker::GetCurrentReturnIds(int num_returns,
                                                      const ActorID &callee_actor_id) {
  std::vector<ObjectID> return_ids(num_returns);
  const auto next_task_index = worker_context_.GetTaskIndex() + 1;
  TaskID task_id;
  if (callee_actor_id.IsNil()) {
    /// Return ids for normal task call.
    task_id = TaskID::ForNormalTask(worker_context_.GetCurrentJobID(),
                                    worker_context_.GetCurrentTaskID(), next_task_index);
  } else {
    /// Return ids for actor task call.
    task_id = TaskID::ForActorTask(worker_context_.GetCurrentJobID(),
                                   worker_context_.GetCurrentTaskID(), next_task_index,
                                   callee_actor_id);
  }
  for (int i = 0; i < num_returns; i++) {
    return_ids[i] = ObjectID::FromIndex(task_id, i + 1);
  }
  return return_ids;
}

void CoreWorker::RunRequestService() {
  SetThreadName("request");
  BlockSignal();
  request_service_.run();
}

void CoreWorker::RunReplyService() {
  SetThreadName("reply");
  BlockSignal();
  reply_service_.run();
}

std::string CoreWorker::AddPrefixForKey(const std::string &key, bool isGlobal) {
  if (isGlobal) {
    return key;
  }
  JobID jobId = worker_context_.GetCurrentJobID();
  std::string finalKey = "kv-" + jobId.Hex() + "-" + key;
  return finalKey;
}

Status CoreWorker::ValidateLabels(
    const std::unordered_map<std::string, std::string> &labels) {
  for (const auto &preset_key : kPresetLabelKeys) {
    if (labels.find(preset_key) != labels.end()) {
      std::ostringstream error_msg;
      error_msg << "Invalid label key: The label key name " << preset_key
                << " is used by ray internal, please change it to a different name.";
      RAY_LOG(ERROR) << error_msg.str();
      return Status::Invalid(error_msg.str());
    }
  }
  return Status::OK();
}

void CoreWorker::InsertDefaultLabelKeys(
    std::unordered_map<std::string, std::string> &labels, const ActorID &actor_id) {
  labels[kLabelKeyActorID] = actor_id.Hex();
}

Status CoreWorker::ValidateSchedulingStrategy(
    const rpc::SchedulingStrategy &scheduling_strategy) {
  if (scheduling_strategy.has_actor_affinity_scheduling_strategy()) {
    if (scheduling_strategy.actor_affinity_scheduling_strategy()
            .match_expressions_size() == 0) {
      std::string error_msg =
          "The match expressions of actor affinity scheduling strategy can't be empty.";
      RAY_LOG(ERROR) << "Invalid actor scheduling strategy: " << error_msg;
      return Status::Invalid(error_msg);
    }
  } else if (scheduling_strategy.has_node_affinity_scheduling_strategy()) {
    const auto &node_affinity_strategy =
        scheduling_strategy.node_affinity_scheduling_strategy();
    if (node_affinity_strategy.nodes_size() == 0) {
      std::string error_msg =
          "The nodes of node affinity scheduling strategy can't be empty.";
      RAY_LOG(ERROR) << "Invalid node scheduling strategy: " << error_msg;
      return Status::Invalid(error_msg);
    }

    if (!node_affinity_strategy.anti_affinity() &&
        node_affinity_strategy.nodes_size() > 1) {
      std::string error_msg = "The nodes size must be 1 when anti-affinity is false.";
      RAY_LOG(ERROR) << "Invalid node scheduling strategy: " << error_msg;
      return Status::Invalid(error_msg);
    }
  }
  return Status::OK();
}

void CoreWorker::CreateHAObjectGlobalOwner() {
  for (uint32_t index = 0; index < job_config_->global_owner_number(); index++) {
    auto buffer = std::make_shared<LocalMemoryBuffer>(0);
    RayFunction func(ray::Language::PYTHON, ray::FunctionDescriptorBuilder::BuildPython(
                                                "ray._private.global_owner",
                                                "HAObjectGlobalOwner", "__init__", ""));

    std::string name = "__global_owner_" + std::to_string(index);
    std::string ray_namespace = "";

    // Spread as much as possible to each node.
    rpc::SchedulingStrategy scheduling_strategy;
    scheduling_strategy.mutable_spread_scheduling_strategy();

    std::unordered_map<std::string, double> resources;
    ActorCreationOptions actor_options{-1,
                                       /*max_task_retries=*/0,
                                       /*max_concurrency*/ 1,
                                       resources,
                                       resources,
                                       {},
                                       /*is_detached=*/std::make_shared<bool>(true),
                                       name,
                                       ray_namespace,
                                       /*is_asyncio=*/false,
                                       scheduling_strategy};
    // Create an actor.
    ActorID actor_id;
    RAY_CHECK_OK(CreateActor(func, {}, actor_options, /*extension_data*/ "", &actor_id));
    RAY_LOG(INFO) << "Launch global owner, name: " << name
                  << ", ray_namespace: " << ray_namespace << ", actor_id: " << actor_id;
  }
}

}  // namespace ray
