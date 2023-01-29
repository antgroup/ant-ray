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

#include "ray/raylet/worker_pool.h"

#include <stdlib.h>

#include <algorithm>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "ray/core_worker/common.h"
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <boost/filesystem.hpp>

#include "ray/common/constants.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/common/status.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/core_worker/common.h"
#include "ray/event/event.h"
#include "ray/event/event_label.h"
#include "ray/gcs/pb_util.h"
#include "ray/raylet/actor_worker_assignment_manager.h"
#include "ray/stats/stats.h"
#include "ray/util/logging.h"
#include "ray/util/resource_util.h"
#include "ray/util/util.h"

namespace {

// A helper function to get a worker from a list.
std::shared_ptr<ray::raylet::WorkerInterface> GetWorker(
    const std::unordered_set<std::shared_ptr<ray::raylet::WorkerInterface>> &worker_pool,
    const std::shared_ptr<ray::ClientConnection> &connection) {
  for (auto it = worker_pool.begin(); it != worker_pool.end(); it++) {
    if ((*it)->Connection() == connection) {
      return (*it);
    }
  }
  return nullptr;
}

// A helper function to remove a worker from a list. Returns true if the worker
// was found and removed.
bool RemoveWorker(
    std::unordered_set<std::shared_ptr<ray::raylet::WorkerInterface>> &worker_pool,
    const std::shared_ptr<ray::raylet::WorkerInterface> &worker) {
  return worker_pool.erase(worker) > 0;
}

// A helper function to fill job id for string.
std::string FillJobId(std::string path_tmpl, const ray::JobID &job_id) {
  // replace job_id
  auto pos = path_tmpl.find(kWorkerCommandJobIdPlaceholder);
  if (pos == std::string::npos) {
    return path_tmpl;
  } else {
    path_tmpl.replace(pos, strlen(kWorkerCommandJobIdPlaceholder), job_id.Hex());
    return path_tmpl;
  }
}

void AddJvmMemoryOptions(std::vector<std::string> *options, uint64_t maximum_memory_bytes,
                         float java_heap_fraction,
                         const ray::rpc::JobConfig &job_config) {
  /// Note: If user set -xmx for per job, no need to add any jvm memory options by
  /// default. Also if user did not set xmx but setting xmn, we have no need to check here
  /// since the java worker has the necessary to check it.
  auto per_job_jvm_options = job_config.jvm_options();
  for (auto &op : per_job_jvm_options) {
    if (StartsWithIgnoreCase(op, "-xmx")) {
      uint64_t maximum_memory_mb = maximum_memory_bytes / 1024 / 1024;
      options->emplace_back(
          "-Dray.maximum-memory-mb=" + std::to_string(maximum_memory_mb) + "m");
      RAY_LOG(INFO) << "The job is set -xmx, so we do not add more jvm memory options.";
      return;
    }
  }

  //   direct : xmx : native = 1 : 8 : 1 (default configuration)
  //   xmx : xms : xmn = 2 : 1 : 1
  //   We don't limit native memory for now as there's no easy way to implement.
  uint64_t maximum_memory_mb = maximum_memory_bytes / 1024 / 1024;
  uint64_t xmx = static_cast<uint64_t>(java_heap_fraction * maximum_memory_mb);
  uint64_t direct_memory = (maximum_memory_mb - xmx) / 2;
  uint64_t xms = xmx / 2;
  uint64_t xmn = xmx / 2;
  options->emplace_back(std::string("-Xmx") + std::to_string(xmx) + "m");
  options->emplace_back(std::string("-Xms") + std::to_string(xms) + "m");
  options->emplace_back(std::string("-Xmn") + std::to_string(xmn) + "m");
  options->emplace_back("-Xss512k");
  options->emplace_back("-XX:MaxDirectMemorySize=" + std::to_string(direct_memory) + "m");
  //   The ray.maximum-memory-mb represent the max memory the jvm could use.
  options->emplace_back("-Dray.maximum-memory-mb=" + std::to_string(maximum_memory_mb) +
                        "m");
}

void AddDefaultJvmMemoryOptions(std::vector<std::string> *options,
                                const ray::rpc::JobConfig &job_config) {
  AddJvmMemoryOptions(options, 4000ULL * 1024 * 1024, 0.8, job_config);
}

}  // namespace

namespace ray {

namespace raylet {

WorkerPool::WorkerPool(
    instrumented_io_context &io_service, const NodeID node_id,
    const std::string node_address, int num_workers_soft_limit,
    int num_initial_python_workers_for_first_job, int maximum_startup_concurrency,
    int min_worker_port, int max_worker_port, const std::vector<int> &worker_ports,
    std::shared_ptr<gcs::GcsClient> gcs_client, const WorkerCommandMap &worker_commands,
    const std::string &native_library_path,
    std::function<void()> starting_worker_timeout_callback,
    const std::function<double()> get_time, const std::string &per_job_python_env_path,
    const std::string &job_dir_template,
    std::shared_ptr<ActorWorkerAssignmentManager> actor_worker_assignment_manager,
    const std::string &log_dir)
    : io_service_(&io_service),
      node_id_(node_id),
      node_address_(node_address),
      num_workers_soft_limit_(num_workers_soft_limit),
      maximum_startup_concurrency_(maximum_startup_concurrency),
      gcs_client_(std::move(gcs_client)),
      native_library_path_(native_library_path),
      starting_worker_timeout_callback_(starting_worker_timeout_callback),
      first_job_registered_python_worker_count_(0),
      first_job_driver_wait_num_python_workers_(std::min(
          num_initial_python_workers_for_first_job, maximum_startup_concurrency)),
      num_initial_python_workers_for_first_job_(num_initial_python_workers_for_first_job),
      periodical_runner_(io_service),
      get_time_(get_time),
      per_job_python_env_path_(per_job_python_env_path),
      job_dir_template_(job_dir_template),
      actor_worker_assignment_manager_(std::move(actor_worker_assignment_manager)),
      log_dir_(log_dir) {
  RAY_CHECK(maximum_startup_concurrency > 0);
  RAY_CHECK(actor_worker_assignment_manager_ != nullptr);
  // We need to record so that the metric exists. This way, we report that 0
  // processes have started before a task runs on the node (as opposed to the
  // metric not existing at all).
  stats::NumWorkersStarted.Record(0);

  if (RayConfig::instance().worker_process_watchdog_enabled()) {
    watchdog_ = std::make_unique<ChildProcessWatchdog<std::string>>(
        [this](pid_t pid, boost::optional<int> signal_no,
               boost::optional<std::string> signal_name, boost::optional<int> exit_code,
               const std::string &job_id_in_hex) {
          RAY_CHECK((signal_no && !exit_code) || (!signal_no && exit_code));
          RAY_CHECK((signal_no && signal_name) || (!signal_no && !signal_name));

          std::ostringstream message_stream;
          message_stream << "Process terminated, process id: " << pid
                         << ", exit code: " << exit_code
                         << ", signal number: " << signal_no
                         << ", signal name: " << signal_name
                         << ", job id: " << job_id_in_hex;
          std::string message_str = message_stream.str();

          // `RAY_EVENT` will access `JobManager` which construct in main-thread
          // and not thread-safe, so we use `io_service_` to make sure `RAY_EVENT`
          // only be called in main-thread
          // TODO: remove it when `RAY_EVENT` is thread-safe.
          io_service_->post([pid, exit_code, signal_no, signal_name, job_id_in_hex,
                             message_str, this]() {
            // If the worker has exited before registration, it has to be marked so that
            // the registration timeout handler can avoid pushing it to
            // `past_worker_registration_timeouts_`.
            if (exit_code) {
              // TODO(Chong-Li): even though the number of starting workers are
              // upper-bounded, it would be better to improve this loop.
              for (auto &state : states_by_lang_) {
                for (auto &worker_entry : state.second.starting_worker_processes) {
                  if (worker_entry.first.GetId() == pid) {
                    worker_entry.second.exited = true;
                    break;
                  }
                }
              }
            }
            RAY_EVENT(INFO, EVENT_LABEL_WORKER_PROCESS_TERMINATED)
                    .WithField("pid", std::to_string(pid))
                    .WithField("signal_no", signal_no ? std::to_string(*signal_no) : "--")
                    .WithField("signal_name", signal_name ? *signal_name : "--")
                    .WithField("exit_code", exit_code ? std::to_string(*exit_code) : "--")
                    .WithField("job_id", job_id_in_hex)
                << message_str;
          });

          RAY_LOG(INFO) << message_str;
        });
  } else {
#ifndef _WIN32
    // Ignore SIGCHLD signals. If we don't do this, then worker processes will
    // become zombies instead of dying gracefully.
    signal(SIGCHLD, SIG_IGN);
#endif
  }

  for (const auto &entry : worker_commands) {
    // Initialize the pool state for this language.
    auto &state = states_by_lang_[entry.first];
    state.multiple_for_warning = maximum_startup_concurrency;
    // Set worker command for this language.
    state.worker_command = entry.second;
    RAY_CHECK(!state.worker_command.empty()) << "Worker command must not be empty.";
  }
  // Initialize free ports list with all ports in the specified range.
  if (!worker_ports.empty()) {
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port : worker_ports) {
      free_ports_->push(port);
    }
  } else if (min_worker_port != 0) {
    if (max_worker_port == 0) {
      max_worker_port = 65535;  // Maximum valid port number.
    }
    RAY_CHECK(min_worker_port > 0 && min_worker_port <= 65535);
    RAY_CHECK(max_worker_port >= min_worker_port && max_worker_port <= 65535);
    free_ports_ = std::make_unique<std::queue<int>>();
    for (int port = min_worker_port; port <= max_worker_port; port++) {
      free_ports_->push(port);
    }
  }
  if (RayConfig::instance().kill_idle_workers_interval_ms() > 0) {
    periodical_runner_.RunFnPeriodically(
        [this] { TryKillingIdleWorkers(); },
        RayConfig::instance().kill_idle_workers_interval_ms(),
        "RayletWorkerPool.deadline_timer.kill_idle_workers");
  }
  periodical_runner_.RunFnPeriodically(
      [this] { RecordMetrics(); }, RayConfig::instance().metrics_report_interval_ms());
}

WorkerPool::~WorkerPool() {
  std::unordered_set<Process> procs_to_kill;
  for (const auto &entry : states_by_lang_) {
    // Kill all registered workers. NOTE(swang): This assumes that the registered
    // workers were started by the pool.
    for (const auto &worker : entry.second.registered_workers) {
      procs_to_kill.insert(worker->GetProcess());
    }
    // Kill all the workers that have been started but not registered.
    for (const auto &starting_worker : entry.second.starting_worker_processes) {
      procs_to_kill.insert(starting_worker.first);
    }
  }
  for (Process proc : procs_to_kill) {
    if (watchdog_) watchdog_->UnWatchChildProcess(proc.GetId());
    proc.Kill();
    // NOTE: Avoid calling Wait() here. It fails with ECHILD, as SIGCHLD is disabled.
  }
}

// NOTE(kfstorm): The node manager cannot be passed via WorkerPool constructor because the
// grpc server is started after the WorkerPool instance is constructed.
void WorkerPool::SetNodeManagerPort(int node_manager_port) {
  node_manager_port_ = node_manager_port;
}

Process WorkerPool::StartWorkerProcess(
    const Language &language, const rpc::WorkerType worker_type, const JobID &job_id,
    int workers_to_start, bool for_actor, const std::vector<std::string> &dynamic_options,
    const std::string &serialized_runtime_env, const int &runtime_env_hash,
    std::unordered_map<std::string, std::string> override_environment_variables,
    const std::string &allocated_instances_serialized_json) {
  if (workers_to_start > 1) {
    RAY_CHECK(language == Language::JAVA);
  }
  rpc::JobConfig *job_config = nullptr;
  if (!IsIOWorkerType(worker_type)) {
    RAY_CHECK(!job_id.IsNil());
    if (finished_jobs_.contains(job_id)) {
      RAY_LOG(WARNING) << "Failed to start worker process as the job " << job_id
                       << " has finished.";
      return Process();
    }
    auto it = all_jobs_.find(job_id);
    if (it == all_jobs_.end()) {
      RAY_LOG(DEBUG) << "Job config of job " << job_id << " are not local yet.";
      // Will reschedule ready tasks in `NodeManager::HandleJobStarted`.
      return Process();
    }
    job_config = &it->second;
  }

  auto &state = GetStateForLanguage(language);
  // If we are already starting up too many workers of the same worker type, then return
  // without starting more.
  int starting_workers = 0;
  for (auto &entry : state.starting_worker_processes) {
    if (entry.second.worker_type == worker_type) {
      starting_workers += entry.second.num_starting_workers;
    }
  }

  // Here we consider both task workers and I/O workers.
  if (starting_workers >= GetMaximumStartupConcurrency(language, for_actor)) {
    // Workers have been started, but not registered. Force start disabled -- returning.
    RAY_LOG(WARNING) << "Worker not started, " << starting_workers
                     << " workers of language type " << static_cast<int>(language)
                     << " pending registration";
    return Process();
  }
  // Either there are no workers pending registration or the worker start is being forced.
  RAY_LOG(INFO) << "Starting new worker process of language "
                << rpc::Language_Name(language) << " and type "
                << rpc::WorkerType_Name(worker_type) << ", current pool has "
                << state.idle_normal_task_workers.size() << " normal task workers, and "
                << state.idle_actor_workers.size()
                << " workers for actor creation tasks.";

  std::vector<std::string> options;
  if (language == Language::JAVA) {
    if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
      AddDefaultJvmMemoryOptions(&options, *job_config);
    }
  }

  // Append Ray-defined per-job options here
  std::string code_search_path;
  if (language == Language::JAVA || language == Language::CPP) {
    if (job_config) {
      std::string code_search_path_str;
      for (int i = 0; i < job_config->code_search_path_size(); i++) {
        auto path = job_config->code_search_path(i);
        if (i != 0) {
          code_search_path_str += ":";
        }
        code_search_path_str += path;
      }
      if (!code_search_path_str.empty()) {
        code_search_path = code_search_path_str;
        if (language == Language::JAVA) {
          code_search_path_str = "-Dray.job.code-search-path=" + code_search_path_str;
        } else if (language == Language::CPP) {
          code_search_path_str = "--ray_code_search_path=" + code_search_path_str;
        } else {
          RAY_LOG(FATAL) << "Unknown language " << Language_Name(language);
        }
        options.push_back(code_search_path_str);
      }
    }
  }

  if (job_config) {
    auto default_actor_lifetime = job_config->default_actor_lifetime();
    if (language == Language::JAVA) {
      options.push_back("-Dray.job.logging-level=" + job_config->logging_level());

      if (default_actor_lifetime == rpc::JobConfig_ActorLifetime_DETACHED) {
        options.push_back("-Dray.job.default-actor-lifetime=DETACHED");
      } else if (default_actor_lifetime == rpc::JobConfig_ActorLifetime_NON_DETACHED) {
        options.push_back("-Dray.job.default-actor-lifetime=NON_DETACHED");
      } else {
        // It's rpc::JobConfig_ActorLifetime_NONE, and it indicates we don't specify it.
        RAY_LOG(DEBUG) << "default_actor_lifetime is not specified to this worker, "
                       << "so it's no need to append any option for that.";
      }
    } else if (language == Language::PYTHON) {
      options.push_back("--logging-level=" + job_config->logging_level());

      if (default_actor_lifetime == rpc::JobConfig_ActorLifetime_DETACHED) {
        options.push_back("--default-actor-lifetime=detached");
      } else if (default_actor_lifetime == rpc::JobConfig_ActorLifetime_NON_DETACHED) {
        options.push_back("--default-actor-lifetime=non-detached");
      } else {
        // It's rpc::JobConfig_ActorLifetime_NONE, and it indicates we don't specify it.
        RAY_LOG(DEBUG) << "--default-actor-lifetime is not specified to this worker, "
                       << "so it's no need to append any option for that.";
      }
    }
  }

  // Append user-defined per-job options here
  if (language == Language::JAVA) {
    if (job_config && !job_config->jvm_options().empty()) {
      options.insert(options.end(), job_config->jvm_options().begin(),
                     job_config->jvm_options().end());
    }
  }

  // Append Ray-defined per-process options here
  if (language == Language::JAVA) {
    options.push_back("-Dray.job.num-java-workers-per-process=" +
                      std::to_string(workers_to_start));
  }

  // Append user-defined per-process options here
  options.insert(options.end(), dynamic_options.begin(), dynamic_options.end());

  // Extract pointers from the worker command to pass into execvp.
  std::vector<std::string> worker_command_args;
  for (auto const &token : state.worker_command) {
    if (token == kWorkerDynamicOptionPlaceholder) {
      worker_command_args.insert(worker_command_args.end(), options.begin(),
                                 options.end());
      continue;
    }
    RAY_CHECK(node_manager_port_ != 0)
        << "Node manager port is not set yet. This shouldn't happen unless we are trying "
           "to start a worker process before node manager server is started. In this "
           "case, it's a bug and it should be fixed.";
    auto node_manager_port_position = token.find(kNodeManagerPortPlaceholder);
    if (node_manager_port_position != std::string::npos) {
      auto replaced_token = token;
      replaced_token.replace(node_manager_port_position,
                             strlen(kNodeManagerPortPlaceholder),
                             std::to_string(node_manager_port_));
      worker_command_args.push_back(replaced_token);
      continue;
    }

    worker_command_args.push_back(token);
  }

  if (language == Language::PYTHON) {
    if (!IsIOWorkerType(worker_type) && job_id.IsSubmittedFromDashboard()) {
      std::string path = FillJobId(per_job_python_env_path_, job_id);
      worker_command_args[0] = path;
    }
  }

  if (RayConfig::instance().gcs_task_scheduling_enabled() && language == Language::JAVA) {
    // TODO(kfstorm): Check there's no memory-related JVM option in job config.
    // TODO(kfstorm): Check Java worker command line arguments.
  }

  if (language == Language::PYTHON) {
    RAY_CHECK(worker_type == rpc::WorkerType::WORKER || IsIOWorkerType(worker_type));
    if (IsIOWorkerType(worker_type)) {
      // Without "--worker-type", by default the worker type is rpc::WorkerType::WORKER.
      worker_command_args.push_back("--worker-type=" + rpc::WorkerType_Name(worker_type));
    }
  }

  if (IsIOWorkerType(worker_type)) {
    RAY_CHECK(!RayConfig::instance().object_spilling_config().empty());
    RAY_LOG(DEBUG) << "Adding object spill config "
                   << RayConfig::instance().object_spilling_config();
    worker_command_args.push_back(
        "--object-spilling-config=" +
        absl::Base64Escape(RayConfig::instance().object_spilling_config()));
  }

  ProcessEnvironment env;
  std::string job_dir;
  if (!IsIOWorkerType(worker_type)) {
    // We pass the job ID to worker processes via an environment variable, so we don't
    // need to add a new CLI parameter for both Python and Java workers.
    env.emplace(kEnvVarKeyJobId, job_id.Hex());
    job_dir = FillJobId(job_dir_template_, job_id);
    env.emplace(kEnvVarKeyJobDir, job_dir);
  }
  if (job_config) {
    env.insert(job_config->worker_env().begin(), job_config->worker_env().end());
  }

  for (const auto &pair : override_environment_variables) {
    env[pair.first] = pair.second;
  }

  // TODO(SongGuyang): Maybe Python and Java also need native library path in future.
  if (language == Language::CPP) {
    // Set native library path for shared library search.
    if (!native_library_path_.empty() || !code_search_path.empty()) {
#if defined(__APPLE__) || defined(__linux__) || defined(_WIN32)
#if defined(__APPLE__)
      static const std::string kLibraryPathEnvName = "DYLD_LIBRARY_PATH";
#elif defined(__linux__)
      static const std::string kLibraryPathEnvName = "LD_LIBRARY_PATH";
#elif defined(_WIN32)
      static const std::string kLibraryPathEnvName = "PATH";
#endif
      auto path_env_p = std::getenv(kLibraryPathEnvName.c_str());
      std::string path_env = native_library_path_;
      if (path_env_p != nullptr && strlen(path_env_p) != 0) {
        path_env.append(":").append(path_env_p);
      }
      // Append per-job code search path to library path.
      if (!code_search_path.empty()) {
        path_env.append(":").append(code_search_path);
      }
      auto path_env_iter = env.find(kLibraryPathEnvName);
      if (path_env_iter == env.end()) {
        env.emplace(kLibraryPathEnvName, path_env);
      } else {
        env[kLibraryPathEnvName] = path_env_iter->second.append(":").append(path_env);
      }
#endif
    }
  }

  if (language == Language::PYTHON || language == Language::JAVA) {
    if (serialized_runtime_env != "{}" && serialized_runtime_env != "") {
      worker_command_args.push_back("--serialized-runtime-env=" + serialized_runtime_env);
      worker_command_args.push_back("--language=" + Language_Name(language));
      // Allocated_resource_json is only used in "shim process".
      worker_command_args.push_back("--allocated-instances-serialized-json=" +
                                    allocated_instances_serialized_json);
    } else {
      // The "shim process" setup worker is not needed, so do not run it.
      // Check that the arg really is the path to the setup worker before erasing it, to
      // prevent breaking tests that mock out the worker command args.
      if (worker_command_args.size() >= 4 &&
          worker_command_args[1].find(kSetupWorkerFilename) != std::string::npos) {
        if (language == Language::PYTHON) {
          worker_command_args.erase(worker_command_args.begin() + 1,
                                    worker_command_args.begin() + 4);
        } else {
          // Erase the python executable as well for other languages.
          worker_command_args.erase(worker_command_args.begin(),
                                    worker_command_args.begin() + 4);
        }
      }
    }

    const std::string runtime_env_hash_str = std::to_string(runtime_env_hash);
    worker_command_args.push_back("--runtime-env-hash=" + runtime_env_hash_str);
  }

  // We use setproctitle to change python worker process title,
  // causing the process's /proc/PID/environ being empty.
  // Add `SPT_NOENV` env to prevent setproctitle breaking /proc/PID/environ.
  // Refer this issue for more details: https://github.com/ray-project/ray/issues/15061
  if (language == Language::PYTHON) {
    env.insert({"SPT_NOENV", "1"});
  }

  std::string std_streams_redirect_file_prefix = log_dir_;
  std_streams_redirect_file_prefix.append("/")
      .append(ray::LanguageString(language))
      .append("-")
      .append(ray::WorkerTypeString(worker_type))
      .append("-")
      .append(job_id.Hex())
      .append("-");
  // Start a process and measure the startup time.
  auto start = std::chrono::high_resolution_clock::now();
  Process proc =
      StartProcess(worker_command_args, env, job_dir, std_streams_redirect_file_prefix);
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  stats::ProcessStartupTimeMs.Record(duration.count());
  stats::NumWorkersStarted.Record(1);

  RAY_LOG(INFO) << "Started worker process of " << workers_to_start
                << " worker(s) with pid " << proc.GetId();
  MonitorStartingWorkerProcess(proc, language, worker_type, job_id);
  state.starting_worker_processes.emplace(
      proc, StartingWorkerProcessInfo{workers_to_start, workers_to_start, worker_type,
                                      for_actor, /*exited=*/false});
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.num_starting_io_workers++;
  }
  if (watchdog_) watchdog_->WatchChildProcess(proc.GetId(), job_id.Hex());
  return proc;
}

void WorkerPool::MonitorStartingWorkerProcess(const Process &proc,
                                              const Language &language,
                                              const rpc::WorkerType worker_type,
                                              const JobID &job_id) {
  auto timer = std::make_shared<boost::asio::deadline_timer>(
      *io_service_, boost::posix_time::seconds(
                        RayConfig::instance().worker_register_timeout_seconds()));
  // Capture timer in lambda to copy it once, so that it can avoid destructing timer.
  timer->async_wait([timer, language, proc, worker_type, job_id,
                     this](const boost::system::error_code e) {
    // check the error code.
    auto &state = this->GetStateForLanguage(language);
    // Since this process times out to start, remove it from starting_worker_processes
    // to avoid the zombie worker.
    auto it = state.starting_worker_processes.find(proc);
    if (it != state.starting_worker_processes.end()) {
      std::ostringstream ostr;
      ostr << "Some workers of the worker process(" << proc.GetId()
           << ") have not registered to raylet within timeout ("
           << RayConfig::instance().worker_register_timeout_seconds()
           << " s), so the process " << proc.GetId()
           << " will be killed immediately and a new one will be forked along with "
              "scheduling later, job_id = "
           << job_id;
      std::string message = ostr.str();
      RAY_LOG(WARNING) << message;
      RAY_EVENT(WARNING, EVENT_LABEL_WORKER_REGISTER_TIMEOUT)
              .WithField("pid", std::to_string(proc.GetId()))
              .WithField("host_name", boost::asio::ip::host_name())
              .WithField("job_id", job_id.Hex())
              .WithField("language", rpc::Language_Name(language))
          << message;

      // If this worker has not exited before registration timeout, then it potentially
      // implies system overloading. So record this timeout and the pending tasks might
      // have to be rescheduled to other nodes (in `starting_worker_timeout_callback_`).
      if (!it->second.exited) {
        past_worker_registration_timeouts_.push_back(current_sys_time_seconds());
      }

      if (watchdog_) watchdog_->UnWatchChildProcess(proc.GetId());
      proc.Kill();
      state.starting_worker_processes.erase(it);
      auto worker_to_task_it = state.dedicated_workers_to_tasks.find(proc);
      if (worker_to_task_it != state.dedicated_workers_to_tasks.end()) {
        state.tasks_to_dedicated_workers.erase(worker_to_task_it->second);
        state.dedicated_workers_to_tasks.erase(worker_to_task_it);
      }
      if (IsIOWorkerType(worker_type)) {
        // Mark the I/O worker as failed.
        auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
        io_worker_state.num_starting_io_workers--;
      }
      // We may have places to start more workers now.
      TryStartIOWorkers(language);
      actor_worker_assignment_manager_->IncAssignmentWorkRegisterTimeoutNumByProc(proc);
      actor_worker_assignment_manager_->UnbindAssignmentFromProcess(proc);
      starting_worker_timeout_callback_();
    }
  });
}

Process WorkerPool::StartProcess(const std::vector<std::string> &worker_command_args,
                                 const ProcessEnvironment &env, const std::string &cwd,
                                 const std::string &std_streams_redirect_file_prefix) {
  if (RAY_LOG_ENABLED(INFO)) {
    std::string debug_info;
    debug_info.append("Starting worker process with command:");
    for (const auto &arg : worker_command_args) {
      debug_info.append(" ").append(arg);
    }
    debug_info.append(", and the envs:");
    for (const auto &entry : env) {
      debug_info.append(" ")
          .append(entry.first)
          .append(":")
          .append(entry.second)
          .append(",");
    }
    if (!env.empty()) {
      // Erase the last ","
      debug_info.pop_back();
    }
    debug_info.append(".");
    RAY_LOG(INFO) << debug_info;
  }

  // Launch the process to create the worker.
  std::error_code ec;
  std::vector<const char *> argv;
  for (const std::string &arg : worker_command_args) {
    argv.push_back(arg.c_str());
  }
  argv.push_back(NULL);

  Process child(argv.data(), io_service_, ec, /*decouple=*/false, env, cwd,
                std_streams_redirect_file_prefix);
  if (!child.IsValid() || ec) {
    // errorcode 24: Too many files. This is caused by ulimit.
    if (ec.value() == 24) {
      RAY_LOG(FATAL) << "Too many workers, failed to create a file. Try setting "
                     << "`ulimit -n <num_files>` then restart Ray.";
    } else {
      // The worker failed to start. This is a fatal error.
      RAY_LOG(FATAL) << "Failed to start worker with return value " << ec << ": "
                     << ec.message();
    }
  }
  return child;
}

Status WorkerPool::GetNextFreePort(int *port) {
  if (!free_ports_) {
    *port = 0;
    return Status::OK();
  }

  // Try up to the current number of ports.
  int current_size = free_ports_->size();
  for (int i = 0; i < current_size; i++) {
    *port = free_ports_->front();
    free_ports_->pop();
    if (CheckFree(*port)) {
      return Status::OK();
    }
    // Return to pool to check later.
    free_ports_->push(*port);
  }
  return Status::Invalid(
      "No available ports. Please specify a wider port range using --min-worker-port and "
      "--max-worker-port.");
}

void WorkerPool::MarkPortAsFree(int port) {
  if (free_ports_) {
    RAY_CHECK(port != 0) << "";
    free_ports_->push(port);
  }
}

void WorkerPool::HandleJobStarted(const JobID &job_id, const rpc::JobConfig &job_config) {
  all_jobs_[job_id] = job_config;
}

void WorkerPool::HandleJobFinished(const JobID &job_id) {
  finished_jobs_.emplace(job_id);
}

bool WorkerPool::IsJobRunning(const JobID &job_id) {
  return !finished_jobs_.contains(job_id) && all_jobs_.contains(job_id);
}

bool WorkerPool::IsJobFinished(const JobID &job_id) {
  return finished_jobs_.contains(job_id);
}

boost::optional<const rpc::JobConfig &> WorkerPool::GetJobConfig(
    const JobID &job_id) const {
  auto iter = all_jobs_.find(job_id);
  return iter == all_jobs_.end() ? boost::none
                                 : boost::optional<const rpc::JobConfig &>(iter->second);
}

Status WorkerPool::RegisterWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  pid_t pid, pid_t worker_shim_pid,
                                  std::function<void(Status, int)> send_reply_callback) {
  RAY_CHECK(worker);

  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto shim_process = Process::FromPid(worker_shim_pid);
  worker->SetShimProcess(shim_process);

  auto it = state.starting_worker_processes.find(shim_process);
  if (it == state.starting_worker_processes.end()) {
    RAY_LOG(WARNING) << "Received a register request from an unknown worker shim process:"
                     << shim_process.GetId();
    Status status = Status::Invalid("Unknown worker");
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  auto &starting_process_info = it->second;
  worker->SetIsForActor(starting_process_info.for_actor);

  // If the job is finished, we will remove the idle worker.
  if (!IsIOWorkerType(worker->GetWorkerType()) &&
      !IsJobRunning(worker->GetAssignedJobId())) {
    state.starting_worker_processes.erase(shim_process);
    Status status =
        Status::Invalid("The job is not running now. Reject all worker registrations.");
    send_reply_callback(status, /*port=*/0);
    return status;
  }

  if (worker->IsForActor()) {
    if (!actor_worker_assignment_manager_->IsValid(shim_process)) {
      // This is a leaked worker, just kill it.
      std::ostringstream ostr;
      ostr << "The worker " << worker->WorkerId() << " with process id "
           << shim_process.GetId()
           << " is a leaked worker. Reject all worker registrations.";
      std::string message = ostr.str();
      state.starting_worker_processes.erase(shim_process);
      Status status = Status::Invalid(message);
      send_reply_callback(status, /*port=*/0);
      return status;
    }
  }
  auto process = Process::FromPid(pid);
  worker->SetProcess(process);

  // The port that this worker's gRPC server should listen on. 0 if the worker
  // should bind on a random port.
  int port = 0;
  Status status = GetNextFreePort(&port);
  if (!status.ok()) {
    send_reply_callback(status, /*port=*/0);
    return status;
  }
  RAY_LOG(INFO) << "Registering worker " << worker->WorkerId() << " with pid " << pid
                << ", shim pid " << worker_shim_pid << ", port: " << port
                << ", worker_type: " << rpc::WorkerType_Name(worker->GetWorkerType());
  worker->SetAssignedPort(port);

  state.registered_workers.insert(worker);

  // Send the reply immediately for worker registrations.
  send_reply_callback(Status::OK(), port);
  std::ostringstream str_pid;
  str_pid << pid;
  RAY_EVENT(INFO, EVENT_LABEL_WORKER_START)
          .WithField("worker_id", worker->WorkerId().Hex())
          .WithField("pid", str_pid.str())
          .WithField("host_name", boost::asio::ip::host_name())
          .WithField("job_id", worker->GetAssignedJobId().Hex())
          .WithField("language", rpc::Language_Name(worker->GetLanguage()))
      << "Worker starts";
  return Status::OK();
}

void WorkerPool::OnWorkerStarted(const std::shared_ptr<WorkerInterface> &worker) {
  auto &state = GetStateForLanguage(worker->GetLanguage());
  const auto &shim_process = worker->GetShimProcess();
  RAY_CHECK(shim_process.IsValid());

  auto it = state.starting_worker_processes.find(shim_process);
  if (it != state.starting_worker_processes.end()) {
    it->second.num_starting_workers--;
    if (it->second.num_starting_workers == 0) {
      state.starting_worker_processes.erase(it);
      // We may have slots to start more workers now.
      TryStartIOWorkers(worker->GetLanguage());
    }
  }
  const auto &worker_type = worker->GetWorkerType();
  if (IsIOWorkerType(worker_type)) {
    auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);
    io_worker_state.registered_io_workers.insert(worker);
    io_worker_state.num_starting_io_workers--;
  }

  // This is a workaround to finish driver registration after all initial workers are
  // registered to Raylet if and only if Raylet is started by a Python driver and the
  // job config is not set in `ray.init(...)`.
  if (first_job_ == worker->GetAssignedJobId() &&
      worker->GetLanguage() == Language::PYTHON) {
    if (++first_job_registered_python_worker_count_ ==
        first_job_driver_wait_num_python_workers_) {
      if (first_job_send_register_client_reply_to_driver_) {
        first_job_send_register_client_reply_to_driver_();
        first_job_send_register_client_reply_to_driver_ = nullptr;
      }
    }
  }
}

Status WorkerPool::RegisterDriver(const std::shared_ptr<WorkerInterface> &driver,
                                  const rpc::JobConfig &job_config,
                                  std::function<void(Status, int)> send_reply_callback) {
  int port;
  RAY_CHECK(!driver->GetAssignedTaskId().IsNil());
  Status status = GetNextFreePort(&port);
  if (!status.ok()) {
    return status;
  }
  driver->SetAssignedPort(port);
  auto &state = GetStateForLanguage(driver->GetLanguage());
  state.registered_drivers.insert(std::move(driver));
  const auto job_id = driver->GetAssignedJobId();
  all_jobs_[job_id] = job_config;

  // This is a workaround to start initial workers on this node if and only if Raylet is
  // started by a Python driver and the job config is not set in `ray.init(...)`.
  // Invoke the `send_reply_callback` later to only finish driver
  // registration after all initial workers are registered to Raylet.
  bool delay_callback = false;
  // If this is the first job.
  if (first_job_.IsNil()) {
    first_job_ = job_id;
    // If the number of Python workers we need to wait is positive.
    if (num_initial_python_workers_for_first_job_ > 0) {
      delay_callback = true;
      // Start initial Python workers for the first job.
      for (int i = 0; i < num_initial_python_workers_for_first_job_; i++) {
        // Start num_initial_python_workers_for_first_job_ actor workers
        // and num_initial_python_workers_for_first_job_ normal task workers.
        StartWorkerProcess(Language::PYTHON, rpc::WorkerType::WORKER, job_id,
                           /*num_workers_to_start=*/1, /*for_actor=*/true);
        StartWorkerProcess(Language::PYTHON, rpc::WorkerType::WORKER, job_id,
                           /*num_workers_to_start=*/1, /*for_actor=*/false);
      }
    }
  }

  if (delay_callback) {
    RAY_CHECK(!first_job_send_register_client_reply_to_driver_);
    first_job_send_register_client_reply_to_driver_ = [send_reply_callback, port]() {
      send_reply_callback(Status::OK(), port);
    };
  } else {
    send_reply_callback(Status::OK(), port);
  }

  return Status::OK();
}

std::shared_ptr<WorkerInterface> WorkerPool::GetRegisteredWorker(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto worker = GetWorker(entry.second.registered_workers, connection);
    if (worker != nullptr) {
      return worker;
    }
  }
  return nullptr;
}

std::shared_ptr<WorkerInterface> WorkerPool::GetRegisteredDriver(
    const std::shared_ptr<ClientConnection> &connection) const {
  for (const auto &entry : states_by_lang_) {
    auto driver = GetWorker(entry.second.registered_drivers, connection);
    if (driver != nullptr) {
      return driver;
    }
  }
  return nullptr;
}

void WorkerPool::PushSpillWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::SPILL_WORKER);
}

void WorkerPool::PopSpillWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::SPILL_WORKER, callback);
}

void WorkerPool::PushRestoreWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::RESTORE_WORKER);
}

void WorkerPool::PopRestoreWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::RESTORE_WORKER, callback);
}

void WorkerPool::PushUtilWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::UTIL_WORKER);
}

void WorkerPool::PopUtilWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::UTIL_WORKER, callback);
}

void WorkerPool::PushIOWorkerInternal(const std::shared_ptr<WorkerInterface> &worker,
                                      const rpc::WorkerType &worker_type) {
  RAY_CHECK(IsIOWorkerType(worker->GetWorkerType()));
  auto &state = GetStateForLanguage(Language::PYTHON);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  if (!io_worker_state.registered_io_workers.count(worker)) {
    RAY_LOG(DEBUG)
        << "The IO worker has failed. Skip pushing it to the worker pool. Worker type: "
        << rpc::WorkerType_Name(worker_type) << ", worker id: " << worker->WorkerId();
    return;
  }

  RAY_LOG(DEBUG) << "Pushing an IO worker to the worker pool. Worker type: "
                 << rpc::WorkerType_Name(worker_type)
                 << ", worker id: " << worker->WorkerId();
  if (io_worker_state.pending_io_tasks.empty()) {
    io_worker_state.idle_io_workers.emplace(worker);
  } else {
    auto callback = io_worker_state.pending_io_tasks.front();
    io_worker_state.pending_io_tasks.pop();
    callback(worker);
  }
}

void WorkerPool::PopIOWorkerInternal(
    const rpc::WorkerType &worker_type,
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  auto &state = GetStateForLanguage(Language::PYTHON);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  if (io_worker_state.idle_io_workers.empty()) {
    // We must fill the pending task first, because 'TryStartIOWorkers' will
    // start I/O workers according to the number of pending tasks.
    io_worker_state.pending_io_tasks.push(callback);
    RAY_LOG(DEBUG) << "Failed to pop an IO worker. Try starting a new one. Worker type: "
                   << rpc::WorkerType_Name(worker_type);
    TryStartIOWorkers(Language::PYTHON, worker_type);
  } else {
    const auto it = io_worker_state.idle_io_workers.begin();
    auto io_worker = *it;
    io_worker_state.idle_io_workers.erase(it);
    RAY_LOG(DEBUG) << "Popped an IO worker. Worker type: "
                   << rpc::WorkerType_Name(worker_type)
                   << ", worker ID: " << io_worker->WorkerId();
    callback(io_worker);
  }
}

void WorkerPool::PushDeleteWorker(const std::shared_ptr<WorkerInterface> &worker) {
  RAY_CHECK(IsIOWorkerType(worker->GetWorkerType()));
  if (worker->GetWorkerType() == rpc::WorkerType::RESTORE_WORKER) {
    PushRestoreWorker(worker);
  } else {
    PushSpillWorker(worker);
  }
}

void WorkerPool::PopDeleteWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  auto &state = GetStateForLanguage(Language::PYTHON);
  // Choose an I/O worker with more idle workers.
  size_t num_spill_idle_workers = state.spill_io_worker_state.idle_io_workers.size();
  size_t num_restore_idle_workers = state.restore_io_worker_state.idle_io_workers.size();

  if (num_restore_idle_workers < num_spill_idle_workers) {
    PopSpillWorker(callback);
  } else {
    PopRestoreWorker(callback);
  }
}

void WorkerPool::PushDumpWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::DUMP_WORKER);
}

void WorkerPool::PopDumpWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::DUMP_WORKER, callback);
}

void WorkerPool::PushLoadWorker(const std::shared_ptr<WorkerInterface> &worker) {
  PushIOWorkerInternal(worker, rpc::WorkerType::LOAD_WORKER);
}

void WorkerPool::PopLoadWorker(
    std::function<void(std::shared_ptr<WorkerInterface>)> callback) {
  PopIOWorkerInternal(rpc::WorkerType::LOAD_WORKER, callback);
}

void WorkerPool::PushWorker(const std::shared_ptr<WorkerInterface> &worker) {
  // Since the worker is now idle, unset its assigned task ID.
  RAY_CHECK(worker->GetAssignedTaskId().IsNil())
      << "Idle workers cannot have an assigned task ID";
  auto &state = GetStateForLanguage(worker->GetLanguage());
  auto it = state.dedicated_workers_to_tasks.find(worker->GetShimProcess());
  if (it != state.dedicated_workers_to_tasks.end()) {
    // The worker is used for the actor creation task with dynamic options.
    // Put it into idle dedicated worker pool.
    const auto task_id = it->second;
    state.idle_dedicated_workers[task_id] = worker;
  } else {
    // The worker is not used for the actor creation task with dynamic options.
    // Put the worker to the idle pool.
    if (worker->IsForActor()) {
      // This worker is used for actor creation tasks.
      state.idle_actor_workers.insert(worker);
      if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
        int64_t now = get_time_();
        idle_of_all_languages_.emplace_back(worker, now);
        idle_of_all_languages_map_[worker] = now;
      }
    } else if (worker->GetActorId().IsNil()) {
      // Used for normal tasks.
      state.idle_normal_task_workers.insert(worker);
      int64_t now = get_time_();
      idle_of_all_languages_.emplace_back(worker, now);
      idle_of_all_languages_map_[worker] = now;
    } else {
      // Used for actor tasks.
      state.idle_actor_workers.insert(worker);
      int64_t now = get_time_();
      idle_of_all_languages_.emplace_back(worker, now);
      idle_of_all_languages_map_[worker] = now;
    }
  }
}

void WorkerPool::TryKillingIdleWorkers() {
  // NOTE: ANT-INTERNAL: if `gcs_task_scheduling_enabled` is false, we try to kill workers
  // from both `idle_actor_workers` and `idle_normal_task_workers`, otherwise, We only
  // kill workers for normal tasks. So the meaning of `idle_of_all_languages_` differs
  // based on flag `gcs_task_scheduling_enabled`.
  RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());

  int64_t now = get_time_();
  size_t running_size = 0;
  for (const auto &worker : GetAllRegisteredWorkers()) {
    if (!worker->IsDead() && worker->GetWorkerType() == rpc::WorkerType::WORKER) {
      running_size++;
    }
  }
  // Subtract the number of pending exit workers first. This will help us killing more
  // idle workers that it needs to.
  RAY_CHECK(running_size >= pending_exit_idle_workers_.size());
  running_size -= pending_exit_idle_workers_.size();
  // Kill idle workers in FIFO order.
  for (const auto &idle_pair : idle_of_all_languages_) {
    if (running_size <= static_cast<size_t>(num_workers_soft_limit_)) {
      break;
    }

    const auto &idle_worker = idle_pair.first;
    if (now - idle_pair.second <
        RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
      break;
    }

    if (idle_worker->IsDead()) {
      // This worker has already been killed.
      // This is possible because a Java worker process may hold multiple workers.
      continue;
    }
    auto shim_process = idle_worker->GetShimProcess();
    auto &worker_state = GetStateForLanguage(idle_worker->GetLanguage());

    if (worker_state.starting_worker_processes.count(shim_process) > 0) {
      // A Java worker process may hold multiple workers.
      // Some workers of this process are pending registration. Skip killing this worker.
      continue;
    }

    auto process = idle_worker->GetProcess();
    // Make sure all workers in this worker process are idle.
    // This block of code is needed by Java workers.
    auto workers_in_the_same_process = GetWorkersByProcess(process);
    bool can_be_killed = true;
    for (const auto &worker : workers_in_the_same_process) {
      if ((worker_state.idle_normal_task_workers.count(worker) == 0 &&
           (RayConfig::instance().gcs_task_scheduling_enabled()
                ? true
                : worker_state.idle_actor_workers.count(worker) == 0)) ||
          now - idle_of_all_languages_map_[worker] <
              RayConfig::instance().idle_worker_killing_time_threshold_ms()) {
        // Another worker in this process isn't idle, or hasn't been idle for a while, so
        // this process can't be killed.
        can_be_killed = false;
        break;
      }

      // Skip killing the worker process if there's any inflight `Exit` RPC requests to
      // this worker process.
      if (pending_exit_idle_workers_.count(worker->WorkerId())) {
        can_be_killed = false;
        break;
      }
    }
    if (!can_be_killed) {
      continue;
    }

    RAY_CHECK(running_size >= workers_in_the_same_process.size());
    if (running_size - workers_in_the_same_process.size() <
        static_cast<size_t>(num_workers_soft_limit_)) {
      // A Java worker process may contain multiple workers. Killing more workers than we
      // expect may slow the job.
      return;
    }

    for (const auto &worker : workers_in_the_same_process) {
      RAY_LOG(DEBUG) << "The worker pool has " << running_size
                     << " registered workers which exceeds the soft limit of "
                     << num_workers_soft_limit_ << ", and worker " << worker->WorkerId()
                     << " with pid " << process.GetId()
                     << " has been idle for a a while. Kill it.";
      // To avoid object lost issue caused by forcibly killing, send an RPC request to the
      // worker to allow it to do cleanup before exiting.
      if (!worker->IsDead()) {
        // Register the worker to pending exit so that we can correctly calculate the
        // running_size.
        // This also means that there's an inflight `Exit` RPC request to the worker.
        pending_exit_idle_workers_.emplace(worker->WorkerId(), worker);
        auto rpc_client = worker->rpc_client();
        RAY_CHECK(rpc_client);
        RAY_CHECK(running_size > 0);
        running_size--;
        rpc::ExitRequest request;
        rpc_client->Exit(request, [this, worker](const ray::Status &status,
                                                 const rpc::ExitReply &r) {
          RAY_CHECK(pending_exit_idle_workers_.count(worker->WorkerId()));
          RAY_CHECK(pending_exit_idle_workers_.erase(worker->WorkerId()));
          if (!status.ok()) {
            RAY_LOG(ERROR) << "Failed to send exit request: " << status.ToString();
          }

          // In case of failed to send request, we remove it from pool as well
          // TODO (iycheng): We should handle the grpc failure in better way.
          if (!status.ok() || r.success()) {
            auto &worker_state = GetStateForLanguage(worker->GetLanguage());
            // If we could kill the worker properly, we remove them from the idle pool.
            RemoveWorker(worker_state.idle_normal_task_workers, worker);
            RemoveWorker(worker_state.idle_actor_workers, worker);
            // We always mark the worker as dead.
            // If the worker is not idle at this moment, we'd want to mark it as dead
            // so it won't be reused later.
            if (!worker->IsDead()) {
              worker->MarkDead();
            }
          } else {
            // We re-insert the idle worker to the back of the queue if it fails to kill
            // the worker (e.g., when the worker owns the object). Without this, if the
            // first N workers own objects, it can't kill idle workers that are >= N+1.
            const auto &idle_pair = idle_of_all_languages_.front();
            idle_of_all_languages_.push_back(idle_pair);
            idle_of_all_languages_.pop_front();
            RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());
          }
        });
      } else {
        // Even it's a dead worker, we still need to remove them from the pool.
        RemoveWorker(worker_state.idle_normal_task_workers, worker);
        RemoveWorker(worker_state.idle_actor_workers, worker);
      }
    }
  }

  std::list<std::pair<std::shared_ptr<WorkerInterface>, int64_t>>
      new_idle_of_all_languages;
  idle_of_all_languages_map_.clear();
  for (const auto &idle_pair : idle_of_all_languages_) {
    if (!idle_pair.first->IsDead()) {
      new_idle_of_all_languages.push_back(idle_pair);
      idle_of_all_languages_map_.emplace(idle_pair);
    }
  }

  idle_of_all_languages_ = std::move(new_idle_of_all_languages);
  RAY_CHECK(idle_of_all_languages_.size() == idle_of_all_languages_map_.size());
}

void WorkerPool::KillWorker(std::shared_ptr<WorkerInterface> worker, bool force) {
  if (watchdog_) watchdog_->UnWatchChildProcess(worker->GetProcess().GetId());
  if (force) {
    worker->GetProcess().Kill();
    return;
  }
#ifdef _WIN32
  // TODO(mehrdadn): implement graceful process termination mechanism
#else
  // If we're just cleaning up a single worker, allow it some time to clean
  // up its state before force killing. The client socket will be closed
  // and the worker struct will be freed after the timeout.
  kill(worker->GetProcess().GetId(), SIGTERM);
#endif

  auto retry_timer = std::make_shared<boost::asio::deadline_timer>(*io_service_);
  auto retry_duration = boost::posix_time::milliseconds(
      RayConfig::instance().kill_worker_timeout_milliseconds());
  retry_timer->expires_from_now(retry_duration);
  retry_timer->async_wait([retry_timer, worker](const boost::system::error_code &error) {
    RAY_LOG(INFO) << "Send SIGKILL to worker, pid=" << worker->GetProcess().GetId();
    // Force kill worker
    worker->GetProcess().Kill();
  });
}

std::shared_ptr<WorkerInterface> WorkerPool::PopWorker(
    const TaskSpecification &task_spec,
    const std::string &allocated_instances_serialized_json) {
  auto job_id = task_spec.JobId();
  auto language = task_spec.GetLanguage();
  rpc::JobConfig *job_config = nullptr;
  RAY_CHECK(!job_id.IsNil());
  if (finished_jobs_.contains(job_id)) {
    RAY_LOG(WARNING) << "Failed to pop worker as the job " << job_id << " has finished.";
    return nullptr;
  }
  auto it = all_jobs_.find(job_id);
  if (it == all_jobs_.end()) {
    RAY_LOG(DEBUG) << "The config of job " << job_id << " is not local yet.";
    // Will reschedule ready tasks in `NodeManager::HandleJobStarted`.
    return nullptr;
  }
  job_config = &it->second;

  std::vector<std::string> options;
  if (task_spec.IsActorCreationTask()) {
    auto task_options = task_spec.DynamicWorkerOptions();
    options.insert(options.end(), task_options.begin(), task_options.end());
  }

  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    return PopWorkerWhenGcsTaskSchedulingEnabled(task_spec, job_config, options,
                                                 allocated_instances_serialized_json);
  }

  // gcs task scheduling is disabled.
  std::shared_ptr<WorkerInterface> worker = nullptr;
  auto &state = GetStateForLanguage(language);
  const bool for_actor = task_spec.IsActorCreationTask();

  Process proc;
  if (task_spec.IsActorTask()) {
    // Code path of actor task.
    RAY_CHECK(false) << "Direct call shouldn't reach here.";
  } else if (task_spec.IsActorCreationTask() &&
             !task_spec.DynamicWorkerOptions().empty()) {
    // Code path of task that needs a dedicated worker: an actor creation task with
    // dynamic worker options.
    // Try to pop it from idle dedicated pool.
    auto it = state.idle_dedicated_workers.find(task_spec.TaskId());
    if (it != state.idle_dedicated_workers.end()) {
      // There is an idle dedicated worker for this task.
      worker = std::move(it->second);
      state.idle_dedicated_workers.erase(it);
      // Because we found a worker that can perform this task,
      // we can remove it from dedicated_workers_to_tasks.
      state.dedicated_workers_to_tasks.erase(worker->GetProcess());
      state.tasks_to_dedicated_workers.erase(task_spec.TaskId());
    } else if (!HasPendingWorkerForTask(task_spec.GetLanguage(), task_spec.TaskId())) {
      // We are not pending a registration from a worker for this task,
      // so start a new worker process for this task.
      proc = StartWorkerProcess(
          task_spec.GetLanguage(), rpc::WorkerType::WORKER, task_spec.JobId(),
          /*num_workers_to_start=*/1, for_actor, options,
          task_spec.SerializedRuntimeEnv(), task_spec.GetRuntimeEnvHash(),
          task_spec.OverrideEnvironmentVariables(), allocated_instances_serialized_json);
      if (proc.IsValid()) {
        state.dedicated_workers_to_tasks[proc] = task_spec.TaskId();
        state.tasks_to_dedicated_workers[task_spec.TaskId()] = proc;
      }
    }
  } else {
    // Find an available worker which is already assigned to this job and which has
    // the specified runtime env.
    // Try to pop the most recently pushed worker.
    const int runtime_env_hash = task_spec.GetRuntimeEnvHash();
    for (auto it = idle_of_all_languages_.rbegin(); it != idle_of_all_languages_.rend();
         it++) {
      auto &state_idle = task_spec.IsActorCreationTask() ? state.idle_actor_workers
                                                         : state.idle_normal_task_workers;
      if (task_spec.GetLanguage() != it->first->GetLanguage() ||
          it->first->GetAssignedJobId() != task_spec.JobId() ||
          state.pending_disconnection_workers.count(it->first) > 0 ||
          state_idle.count(it->first) == 0 || it->first->IsDead()) {
        continue;
      }
      // These workers are exiting. So skip them.
      if (pending_exit_idle_workers_.count(it->first->WorkerId())) {
        continue;
      }
      // Skip if the runtime env doesn't match.
      if (runtime_env_hash != it->first->GetRuntimeEnvHash()) {
        continue;
      }

      state_idle.erase(it->first);
      // We can't erase a reverse_iterator.
      auto lit = it.base();
      lit--;
      worker = std::move(lit->first);
      idle_of_all_languages_.erase(lit);
      idle_of_all_languages_map_.erase(worker);
      break;
    }

    if (worker == nullptr) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker process.
      int num_workers_to_start = 1;
      if (task_spec.GetLanguage() == Language::JAVA) {
        num_workers_to_start = job_config->num_java_workers_per_process();
      }
      proc = StartWorkerProcess(task_spec.GetLanguage(), rpc::WorkerType::WORKER,
                                task_spec.JobId(), num_workers_to_start,
                                /*for_actor*/ task_spec.IsActorCreationTask(), options,
                                task_spec.SerializedRuntimeEnv(), runtime_env_hash,
                                task_spec.OverrideEnvironmentVariables(),
                                allocated_instances_serialized_json);
    }
  }

  if (worker == nullptr && proc.IsValid()) {
    WarnAboutSize();
  }

  if (worker) {
    RAY_CHECK(worker->GetAssignedJobId() == task_spec.JobId());
  }
  return worker;
}

void WorkerPool::PrestartWorkers(const TaskSpecification &task_spec,
                                 int64_t backlog_size) {
  // NOTE: ANT-INTERNAL: Prestarting workers doesn't work in Ant. To make it compile, we
  // simply replace `state.idle` to `state.idle_normal_task_workers` with `for_actor`
  // always false here.

  // Code path of task that needs a dedicated worker.
  if ((task_spec.IsActorCreationTask() && !task_spec.DynamicWorkerOptions().empty()) ||
      task_spec.OverrideEnvironmentVariables().size() > 0 ||
      !(task_spec.SerializedRuntimeEnv() == "{}" ||
        task_spec.SerializedRuntimeEnv() == "")) {
    return;  // Not handled.
    // TODO(architkulkarni): We'd eventually like to prestart workers with the same
    // runtime env to improve initial startup performance.
  }

  auto &state = GetStateForLanguage(task_spec.GetLanguage());
  // The number of available workers that can be used for this task spec.
  int num_usable_workers = state.idle_normal_task_workers.size();
  for (auto &entry : state.starting_worker_processes) {
    num_usable_workers += entry.second.num_starting_workers;
  }
  // The number of workers total regardless of suitability for this task.
  int num_workers_total = 0;
  for (const auto &worker : GetAllRegisteredWorkers()) {
    if (!worker->IsDead()) {
      num_workers_total++;
    }
  }
  auto desired_usable_workers =
      std::min<int64_t>(num_workers_soft_limit_ - num_workers_total, backlog_size);
  if (num_usable_workers < desired_usable_workers) {
    int64_t num_needed = desired_usable_workers - num_usable_workers;
    RAY_LOG(DEBUG) << "Prestarting " << num_needed << " workers given task backlog size "
                   << backlog_size << " and soft limit " << num_workers_soft_limit_;
    auto job_config = GetJobConfig(task_spec.JobId());
    RAY_CHECK(job_config);
    for (int i = 0; i < num_needed; i++) {
      StartWorkerProcess(task_spec.GetLanguage(), rpc::WorkerType::WORKER,
                         task_spec.JobId(),
                         task_spec.GetLanguage() == Language::JAVA
                             ? job_config->num_java_workers_per_process()
                             : 1,
                         /*for_actor=*/false);
    }
  }
}

bool WorkerPool::DisconnectWorker(const std::shared_ptr<WorkerInterface> &worker,
                                  rpc::WorkerExitType disconnect_type) {
  std::ostringstream str_pid;
  str_pid << worker->GetProcess().GetId();
  RAY_EVENT(INFO, EVENT_LABEL_WORKER_EXIT)
          .WithField("worker_id", worker->WorkerId().Hex())
          .WithField("pid", str_pid.str())
          .WithField("host_name", boost::asio::ip::host_name())
          .WithField("job_id", worker->GetAssignedJobId().Hex())
          .WithField("language", rpc::Language_Name(worker->GetLanguage()))
      << "Worker exits";

  MarkPortAsFree(worker->AssignedPort());

  auto &state = GetStateForLanguage(worker->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_workers, worker));

  if (IsIOWorkerType(worker->GetWorkerType())) {
    auto &io_worker_state =
        GetIOWorkerStateFromWorkerType(worker->GetWorkerType(), state);
    // If the IO Worker exits between the following two-timestamp,
    // it may cause the IO Worker to not be in `registered_io_workers`
    //     1. Finished construct `RayletClient`
    //     2. Finish calling raylet_client->AnnounceWorkerPort
    RemoveWorker(io_worker_state.registered_io_workers, worker);
    return RemoveWorker(io_worker_state.idle_io_workers, worker);
  }

  RAY_UNUSED(RemoveWorker(state.pending_disconnection_workers, worker));

  for (auto it = idle_of_all_languages_.begin(); it != idle_of_all_languages_.end();
       it++) {
    if (it->first == worker) {
      idle_of_all_languages_.erase(it);
      idle_of_all_languages_map_.erase(worker);
      break;
    }
  }

  const bool removed_from_actor_pool = RemoveWorker(state.idle_actor_workers, worker);
  const bool removed_from_normal_task_pool =
      RemoveWorker(state.idle_normal_task_workers, worker);
  auto status = removed_from_actor_pool || removed_from_normal_task_pool;
  if (disconnect_type != rpc::WorkerExitType::INTENDED_EXIT) {
    // A Java worker process may have multiple workers. If one of them disconnects
    // unintentionally (which means that the worker process has died), we remove the
    // others from idle pool so that the failed actor will not be rescheduled on the same
    // process.
    auto pid = worker->GetProcess().GetId();
    for (auto worker2 : state.registered_workers) {
      if (worker2->GetProcess().GetId() == pid) {
        // NOTE(kfstorm): We have to use a new field to record these workers (instead of
        // just removing them from idle sets) because they may haven't announced worker
        // port yet. When they announce worker port, they'll be marked idle again. So
        // removing them from idle sets here doesn't really prevent them from being popped
        // later.
        state.pending_disconnection_workers.insert(worker2);
      }
    }
  }
  return status;
}

void WorkerPool::DisconnectDriver(const std::shared_ptr<WorkerInterface> &driver) {
  auto &state = GetStateForLanguage(driver->GetLanguage());
  RAY_CHECK(RemoveWorker(state.registered_drivers, driver));
  MarkPortAsFree(driver->AssignedPort());
}

inline WorkerPool::State &WorkerPool::GetStateForLanguage(const Language &language) {
  auto state = states_by_lang_.find(language);
  RAY_CHECK(state != states_by_lang_.end())
      << "Required Language isn't supported: " << Language_Name(language);
  return state->second;
}

inline bool WorkerPool::IsIOWorkerType(const rpc::WorkerType &worker_type) {
  return worker_type == rpc::WorkerType::SPILL_WORKER ||
         worker_type == rpc::WorkerType::RESTORE_WORKER ||
         worker_type == rpc::WorkerType::UTIL_WORKER;
}

std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetWorkersRunningTasksForJob(
    const JobID &job_id) const {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->GetAssignedJobId() == job_id) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetRegisteredWorkersByProcess(
    const Process &process) const {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (worker->GetProcess().GetId() == process.GetId()) {
        workers.push_back(worker);
      }
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetAllRegisteredWorkers(
    bool filter_dead_workers) const {
  std::vector<std::shared_ptr<WorkerInterface>> workers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &worker : entry.second.registered_workers) {
      if (!worker->IsRegistered()) {
        continue;
      }

      if (filter_dead_workers && worker->IsDead()) {
        continue;
      }
      workers.push_back(worker);
    }
  }

  return workers;
}

const std::vector<std::shared_ptr<WorkerInterface>> WorkerPool::GetAllRegisteredDrivers(
    bool filter_dead_drivers) const {
  std::vector<std::shared_ptr<WorkerInterface>> drivers;

  for (const auto &entry : states_by_lang_) {
    for (const auto &driver : entry.second.registered_drivers) {
      if (!driver->IsRegistered()) {
        continue;
      }

      if (filter_dead_drivers && driver->IsDead()) {
        continue;
      }
      drivers.push_back(driver);
    }
  }

  return drivers;
}

void WorkerPool::WarnAboutSize() {
  for (auto &entry : states_by_lang_) {
    auto &state = entry.second;
    int64_t num_workers_started_or_registered = 0;
    num_workers_started_or_registered +=
        static_cast<int64_t>(state.registered_workers.size());
    for (const auto &starting_process : state.starting_worker_processes) {
      num_workers_started_or_registered += starting_process.second.num_starting_workers;
    }
    int64_t multiple = num_workers_started_or_registered / state.multiple_for_warning;
    std::stringstream warning_message;
    if (multiple >= 3 && multiple > state.last_warning_multiple) {
      // Push an error message to the user if the worker pool tells us that it is
      // getting too big.
      state.last_warning_multiple = multiple;
      warning_message << "WARNING: " << num_workers_started_or_registered << " "
                      << Language_Name(entry.first)
                      << " workers have been started on a node of the id: " << node_id_
                      << " "
                      << "and address: " << node_address_ << ". "
                      << "This could be a result of using "
                      << "a large number of actors, or it could be a consequence of "
                      << "using nested tasks "
                      << "(see https://github.com/ray-project/ray/issues/3644) for "
                      << "some a discussion of workarounds.";
      std::string warning_message_str = warning_message.str();
      RAY_LOG(WARNING) << warning_message_str;
      auto error_data_ptr = gcs::CreateErrorTableData("worker_pool_large",
                                                      warning_message_str, get_time_());
      RAY_CHECK_OK(gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
    }
  }
}

bool WorkerPool::HasPendingWorkerForTask(const Language &language,
                                         const TaskID &task_id) {
  auto &state = GetStateForLanguage(language);
  auto it = state.tasks_to_dedicated_workers.find(task_id);
  return it != state.tasks_to_dedicated_workers.end();
}

void WorkerPool::TryStartIOWorkers(const Language &language) {
  TryStartIOWorkers(language, rpc::WorkerType::RESTORE_WORKER);
  TryStartIOWorkers(language, rpc::WorkerType::SPILL_WORKER);
  TryStartIOWorkers(language, rpc::WorkerType::UTIL_WORKER);
}

void WorkerPool::TryStartIOWorkers(const Language &language,
                                   const rpc::WorkerType &worker_type) {
  if (language != Language::PYTHON) {
    return;
  }
  auto &state = GetStateForLanguage(language);
  auto &io_worker_state = GetIOWorkerStateFromWorkerType(worker_type, state);

  int available_io_workers_num = io_worker_state.num_starting_io_workers +
                                 io_worker_state.registered_io_workers.size();
  int max_workers_to_start =
      RayConfig::instance().max_io_workers() - available_io_workers_num;
  // Compare first to prevent unsigned underflow.
  if (io_worker_state.pending_io_tasks.size() > io_worker_state.idle_io_workers.size()) {
    int expected_workers_num =
        io_worker_state.pending_io_tasks.size() - io_worker_state.idle_io_workers.size();
    if (expected_workers_num > max_workers_to_start) {
      expected_workers_num = max_workers_to_start;
    }
    for (; expected_workers_num > 0; expected_workers_num--) {
      Process proc = StartWorkerProcess(ray::Language::PYTHON, worker_type, JobID::Nil(),
                                        /*workers_to_start=*/1, /*for_actor=*/false);
      if (!proc.IsValid()) {
        // We may hit the maximum worker start up concurrency limit. Stop.
        return;
      }
    }
  }
}

std::unordered_set<std::shared_ptr<WorkerInterface>> WorkerPool::GetWorkersByProcess(
    const Process &process) {
  std::unordered_set<std::shared_ptr<WorkerInterface>> workers_of_process;
  for (auto &entry : states_by_lang_) {
    auto &worker_state = entry.second;
    for (const auto &worker : worker_state.registered_workers) {
      if (worker->GetProcess().GetId() == process.GetId()) {
        workers_of_process.insert(worker);
      }
    }
  }
  return workers_of_process;
}

std::string WorkerPool::DebugString() const {
  std::stringstream result;
  result << "WorkerPool:";
  for (const auto &entry : states_by_lang_) {
    result << "\n- num " << Language_Name(entry.first)
           << " workers: " << entry.second.registered_workers.size();
    result << "\n- num " << Language_Name(entry.first)
           << " drivers: " << entry.second.registered_drivers.size();
    result << "\n- num object spill callbacks queued: "
           << entry.second.spill_io_worker_state.pending_io_tasks.size();
    result << "\n- num object restore queued: "
           << entry.second.restore_io_worker_state.pending_io_tasks.size();
    result << "\n- num util functions queued: "
           << entry.second.util_io_worker_state.pending_io_tasks.size();
  }
  result << "\n- num idle workers: " << idle_of_all_languages_.size();
  return result.str();
}

void WorkerPool::RecordMetrics() const {
  for (const auto &entry : states_by_lang_) {
    absl::flat_hash_set<pid_t> registered_worker_process_set;
    for (const auto &registered_worker : entry.second.registered_workers) {
      registered_worker_process_set.emplace(registered_worker->GetProcess().GetId());
    }
    stats::CurrentWorker().Record(registered_worker_process_set.size(),
                                  {{stats::WorkerStateKey, "Registered"},
                                   {stats::LanguageKey, Language_Name(entry.first)}});
    stats::CurrentWorker().Record(entry.second.starting_worker_processes.size(),
                                  {{stats::WorkerStateKey, "Starting"},
                                   {stats::LanguageKey, Language_Name(entry.first)}});

    absl::flat_hash_set<pid_t> registered_driver_process_set;
    for (const auto &registered_driver : entry.second.registered_drivers) {
      registered_driver_process_set.emplace(registered_driver->GetProcess().GetId());
    }
    stats::CurrentDriver().Record(registered_driver_process_set.size(),
                                  {{stats::LanguageKey, Language_Name(entry.first)}});
  }
}

WorkerPool::IOWorkerState &WorkerPool::GetIOWorkerStateFromWorkerType(
    const rpc::WorkerType &worker_type, WorkerPool::State &state) const {
  RAY_CHECK(worker_type != rpc::WorkerType::WORKER)
      << worker_type << " type cannot be used to retrieve io_worker_state";
  switch (worker_type) {
  case rpc::WorkerType::SPILL_WORKER:
    return state.spill_io_worker_state;
  case rpc::WorkerType::RESTORE_WORKER:
    return state.restore_io_worker_state;
  case rpc::WorkerType::UTIL_WORKER:
    return state.util_io_worker_state;
  default:
    RAY_LOG(FATAL) << "Unknown worker type: " << worker_type;
  }
  UNREACHABLE;
}

///////////////////////// belows are ANT-INTERNAL method////////////////////////////////
std::shared_ptr<WorkerInterface> WorkerPool::PopWorkerWhenGcsTaskSchedulingEnabled(
    const TaskSpecification &task_spec, rpc::JobConfig *job_config,
    std::vector<std::string> options,
    const std::string &allocated_instances_serialized_json) {
  //  auto job_id = task_spec.JobId();
  auto language = task_spec.GetLanguage();
  std::shared_ptr<WorkerInterface> worker = nullptr;
  auto &state = GetStateForLanguage(language);

  const bool is_normal_task =
      (!task_spec.IsActorCreationTask() && !task_spec.IsActorTask());
  if (is_normal_task) {
    // Find an available worker which is already assigned to this job and which has
    // the specified runtime env.
    // Try to pop the most recently pushed worker.
    const int runtime_env_hash = task_spec.GetRuntimeEnvHash();
    for (auto it = idle_of_all_languages_.rbegin(); it != idle_of_all_languages_.rend();
         it++) {
      auto &state_idle = task_spec.IsActorCreationTask() ? state.idle_actor_workers
                                                         : state.idle_normal_task_workers;
      if (task_spec.GetLanguage() != it->first->GetLanguage() ||
          it->first->GetAssignedJobId() != task_spec.JobId() ||
          state.pending_disconnection_workers.count(it->first) > 0 ||
          state_idle.count(it->first) == 0 || it->first->IsDead()) {
        continue;
      }
      // These workers are exiting. So skip them.
      if (pending_exit_idle_workers_.count(it->first->WorkerId())) {
        continue;
      }
      // Skip if the runtime env doesn't match.
      if (runtime_env_hash != it->first->GetRuntimeEnvHash()) {
        continue;
      }

      state_idle.erase(it->first);
      // We can't erase a reverse_iterator.
      auto lit = it.base();
      lit--;
      worker = std::move(lit->first);
      idle_of_all_languages_.erase(lit);
      idle_of_all_languages_map_.erase(worker);
      break;
    }

    if (worker == nullptr) {
      // There are no more non-actor workers available to execute this task.
      // Start a new worker process.
      int num_workers_to_start = 1;
      if (task_spec.GetLanguage() == Language::JAVA) {
        num_workers_to_start = job_config->num_java_workers_per_process();
      }
      auto proc = StartWorkerProcess(
          task_spec.GetLanguage(), rpc::WorkerType::WORKER, task_spec.JobId(),
          num_workers_to_start,
          /*for_actor*/ task_spec.IsActorCreationTask(), options,
          task_spec.SerializedRuntimeEnv(), runtime_env_hash,
          task_spec.OverrideEnvironmentVariables(), allocated_instances_serialized_json);
      if (proc.IsValid()) {
        WarnAboutSize();
      }
    }

    return worker;
  }

  // Actor tasks use gcs task scheduling to decide its worker.
  RAY_CHECK(job_config);
  auto process_and_assignment_id =
      actor_worker_assignment_manager_->GetBindProcessAndAssignment(task_spec.TaskId());
  if (!process_and_assignment_id.second.IsNil()) {
    RAY_CHECK(RayConfig::instance().gcs_task_scheduling_enabled())
        << "gcs_task_scheduling_enabled must be true.";

    if (!process_and_assignment_id.first.IsValid()) {
      int num_workers_to_start = 1;
      std::vector<std::string> final_options;
      if (language == Language::JAVA) {
        uint64_t maximum_memory_bytes;
        if (!ray::ExtractMemoryResourceBytes(task_spec.GetRequiredPlacementResources(),
                                             &maximum_memory_bytes)) {
          num_workers_to_start = job_config->num_java_workers_per_process();
          maximum_memory_bytes = FromMemoryUnitsToBytes(static_cast<double>(
              job_config->java_worker_process_default_memory_units()));
        }
        AddJvmMemoryOptions(&final_options, maximum_memory_bytes,
                            job_config->java_heap_fraction(), *job_config);
      }

      /// Append the dynamic options after default JvmMemoryOptions.
      /// Note: this is used for dynamic options first rule. That means
      /// dynamic options should be appended at the last.
      final_options.insert(final_options.end(), options.begin(), options.end());
      auto process = StartWorkerProcess(
          language, rpc::WorkerType::WORKER, task_spec.JobId(), num_workers_to_start,
          task_spec.IsActorCreationTask(), final_options,
          task_spec.SerializedRuntimeEnv(), task_spec.GetRuntimeEnvHash(),
          task_spec.OverrideEnvironmentVariables(), allocated_instances_serialized_json);
      if (process.IsValid()) {
        if (!actor_worker_assignment_manager_->BindAssignmentToProcess(
                process_and_assignment_id.second, process)) {
          RAY_LOG(FATAL) << "Failed to bind assignment "
                         << process_and_assignment_id.second << " to process "
                         << process.GetId();
        }
        WarnAboutSize();
      }
      return worker;
    }

    for (auto it = state.idle_actor_workers.begin(); it != state.idle_actor_workers.end();
         ++it) {
      if ((*it)->GetShimProcess().GetId() == process_and_assignment_id.first.GetId()) {
        worker = std::move(*it);
        worker->SetAssignmentId(process_and_assignment_id.second);
        state.idle_actor_workers.erase(it);
        break;
      }
    }
  }
  return worker;
}

int WorkerPool::GetMaximumStartupConcurrency(const Language &language, bool for_actor) {
  auto maximum_startup_concurrency = maximum_startup_concurrency_;
  if (RayConfig::instance().gcs_task_scheduling_enabled() && !for_actor &&
      language == Language::JAVA) {
    maximum_startup_concurrency = 1;
  }
  return maximum_startup_concurrency;
}

uint32_t WorkerPool::GetPastWorkerRegistrationTimeoutsCount() {
  auto window_start_timestamp =
      current_sys_time_seconds() -
      RayConfig::instance().worker_registration_timeouts_window_s();
  for (auto timestamp_iter = past_worker_registration_timeouts_.begin();
       timestamp_iter != past_worker_registration_timeouts_.end();) {
    if (*timestamp_iter >= window_start_timestamp) {
      break;
    }
    timestamp_iter = past_worker_registration_timeouts_.erase(timestamp_iter);
  }
  return past_worker_registration_timeouts_.size();
}

}  // namespace raylet

}  // namespace ray
