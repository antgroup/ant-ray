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

#include "ray/gcs/gcs_server/gcs_server.h"
#include <memory>

#include "ray/common/asio/asio_util.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/common/network_util.h"
#include "ray/common/ray_config.h"
#include "ray/event/event.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_schedule_strategy.h"
#include "ray/gcs/gcs_server/gcs_job_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_nodegroup_manager.h"
#include "ray/gcs/gcs_server/gcs_object_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager_ex.h"
#include "ray/gcs/gcs_server/gcs_placement_group_scheduler_ex.h"
#include "ray/gcs/gcs_server/gcs_runtime_resource_manager.h"
#include "ray/gcs/gcs_server/gcs_worker_manager.h"
#include "ray/gcs/gcs_server/stats_handler_impl.h"
#include "ray/gcs/gcs_server/task_info_handler_impl.h"
#include "ray/stats/stats.h"
#include "ray/util/resource_util.h"

namespace ray {
namespace gcs {

using RayletClient = ray::raylet::RayletClient;

//////////////////////////////////////////////////////////////////////////////////
GcsServer::GcsServer(const ray::gcs::GcsServerConfig &config,
                     instrumented_io_context &main_service)
    : config_(config),
      main_service_(main_service),
      rpc_server_(config.grpc_server_name, config.grpc_server_port,
                  config.grpc_server_thread_num),
      client_call_manager_(main_service),
      raylet_client_pool_(
          std::make_shared<rpc::NodeManagerClientPool>(client_call_manager_)),
      print_debug_info_timer_(main_service_),
      detect_hang_timer_(main_service_) {}

GcsServer::~GcsServer() { Stop(); }

void GcsServer::Start() {
  // Record start time which is used to metric.
  int64_t start_time = absl::GetCurrentTimeNanos() / 1000;

  // Init backend client.
  RedisClientOptions redis_client_options(config_.redis_address, config_.redis_port,
                                          config_.redis_password,
                                          config_.enable_sharding_conn);
  redis_client_ = std::make_shared<RedisClient>(redis_client_options);
  auto status = redis_client_->Connect(main_service_);
  RAY_CHECK(status.ok()) << "Failed to init redis gcs client as " << status;

  // Init redis failure detector.
  gcs_redis_failure_detector_ = std::make_shared<GcsRedisFailureDetector>(
      main_service_, redis_client_->GetPrimaryContext(), [this]() { Stop(); });
  gcs_redis_failure_detector_->Start();

  // Init gcs table storage.
  gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);
  // Check whether internal config exists in storage.
  auto storage_config_json_str = std::make_shared<std::string>();
  RAY_LOG(INFO) << "Getting internal config from storage.";
  RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Get(
      ray::UniqueID::Nil(), [storage_config_json_str, this](
                                const ray::Status &status,
                                const boost::optional<ray::rpc::StoredConfig> &config) {
        RAY_LOG(INFO) << "Get internal config from storage done! has_value="
                      << config.has_value() << ", status=" << status;
        RAY_CHECK_OK(status);
        if (config.has_value()) {
          *storage_config_json_str = config.get().config();
        }
        main_service_.stop();
      }));
  // Here we need to make sure the Get of internal config is happening in sync
  // way. But since the storage API is async, we need to run the main_service_
  // to block current thread.
  // This will run async operations from InternalConfigTable().Get() above
  // inline.
  main_service_.run();
  // Reset the main service to the initial status otherwise, the signal handler
  // will be called.
  main_service_.restart();

  // If exists, override current config
  if (!storage_config_json_str->empty()) {
    RayConfig::instance().update(*storage_config_json_str);
  } else {
    // Otherwise, write the user specified config to storage. This should happen in the
    // first time the GCS starts.
    // The internal_config is only set on the gcs--other nodes get it from GCS.
    RAY_LOG(INFO) << "Putting user specified config to storage.";
    auto on_done = [this](const ray::Status &status) {
      RAY_CHECK(status.ok()) << "Failed to put internal config";
      this->main_service_.stop();
    };
    ray::rpc::StoredConfig stored_config;
    stored_config.set_config(config_.raylet_config_list);
    RAY_CHECK_OK(gcs_table_storage_->InternalConfigTable().Put(ray::UniqueID::Nil(),
                                                               stored_config, on_done));
    // Here we need to make sure the Put of internal config is happening in sync
    // way. But since the storage API is async, we need to run the main_service_
    // to block currenct thread.
    // This will run async operations from InternalConfigTable().Put() above
    // inline.
    main_service_.run();
    // Reset the main service to the initial status otherwise, the signal handler
    // will be called.
    main_service_.restart();
  }

  // Init gcs pub sub instance.
  gcs_pub_sub_ = std::make_shared<gcs::GcsPubSub>(redis_client_);

  // Init gcs table storage.
  gcs_table_storage_ = std::make_shared<gcs::RedisGcsTableStorage>(redis_client_);

  // Determine whether it is restart state or not.
  is_restart_ = IsGcsServerRestart();

  // Load gcs tables data asynchronously.
  auto gcs_init_data = std::make_shared<GcsInitData>(gcs_table_storage_);
  gcs_init_data->AsyncLoad([this, gcs_init_data, start_time] {
    DoStart(*gcs_init_data);
    // Record metric when init done.
    if (is_restart_) {
      int64_t end_time = absl::GetCurrentTimeNanos() / 1000;
      ray::stats::GcsServerFailoverElapsedTime().Record(end_time - start_time);
      // Record Event.
      RAY_EVENT(ERROR, EVENT_LABEL_GCS_RESTART)
          << "GCS server restarts and recovers from storage.";
    }
  });
}

void GcsServer::DoStart(const GcsInitData &gcs_init_data) {
  // Init gcs deploy manager.
  InitGcsNodegroupManager(gcs_init_data);

  // Init gcs resource manager.
  InitGcsResourceManager(gcs_init_data);

  // Init gcs resource scheduler.
  InitGcsResourceScheduler();

  // Init gcs node manager.
  InitGcsNodeManager(gcs_init_data);

  // Init gcs heartbeat manager.
  InitGcsHeartbeatManager(gcs_init_data);

  // Init KV Manager
  InitKVManager();

  // Init RuntimeENv manager
  InitRuntimeEnvManager();

  // Init gcs job manager.
  InitGcsJobManager(gcs_init_data);

  // Init gcs placement group manager.
  InitGcsPlacementGroupManager(gcs_init_data);

  // Init gcs frozen node manager.
  InitGcsFrozenNodeManager(gcs_init_data);

  // Init gcs actor manager.
  InitGcsActorManager(gcs_init_data);

  InitGcsActorMigrationManager(gcs_init_data);

  // Init object manager.
  InitObjectManager(gcs_init_data);

  // Init gcs worker manager.
  InitGcsWorkerManager(gcs_init_data);

  // Init task info handler.
  InitTaskInfoHandler();

  // Init stats handler.
  InitStatsHandler();

  // Init resource report polling.
  InitResourceReportPolling(gcs_init_data);

  // Init resource report broadcasting.
  InitResourceReportBroadcasting(gcs_init_data);

  // Init runtime resource manager.
  InitRuntimeResourceManager(gcs_init_data);

  // Init dead data cleaner.
  InitDeadDataCleaner();

  // Install event listeners.
  InstallEventListeners();

  // Start RPC server when all tables have finished loading initial
  // data.
  rpc_server_.Run();

  // Store gcs rpc server address in redis.
  StoreGcsServerAddressInRedis();
  // Only after the rpc_server_ is running can the heartbeat manager
  // be run. Otherwise the node failure detector will mistake
  // some living nodes as dead as the timer inside node failure
  // detector is already run.
  gcs_heartbeat_manager_->Start();
  gcs_nodegroup_manager_->StartNodegroupMonitor(main_service_);

  // Quick detect node failure.
  gcs_node_manager_->QuickDetectNodeFailure();

  // Print debug info periodically.
  PrintDebugInfo();

  // Print the asio event loop stats periodically if configured.
  PrintAsioStats();

  CollectStats();

  // Detect that if the main thread is hang.
  DetectHang();

  is_started_ = true;
}

void GcsServer::Stop() {
  if (!is_stopped_) {
    RAY_LOG(INFO) << "Stopping GCS server.";
    // GcsHeartbeatManager should be stopped before RPCServer.
    // Because closing RPC server will cost several seconds, during this time,
    // GcsHeartbeatManager is still checking nodes' heartbeat timeout. Since RPC Server
    // won't handle heartbeat calls anymore, some nodes will be marked as dead during this
    // time, causing many nodes die after GCS's failure.
    gcs_heartbeat_manager_->Stop();

    // Shutdown the rpc server
    rpc_server_.Shutdown();

    if (config_.pull_based_resource_reporting) {
      gcs_resource_report_poller_->Stop();
    }

    if (config_.grpc_based_resource_broadcast) {
      grpc_based_resource_broadcaster_->Stop();
    }

    is_stopped_ = true;
    RAY_LOG(INFO) << "GCS server stopped.";
  }
}

void GcsServer::InitGcsNodeManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(redis_client_ && gcs_table_storage_ && gcs_pub_sub_);
  gcs_node_manager_ = std::make_shared<GcsNodeManager>(
      gcs_pub_sub_, gcs_table_storage_,
      /*is_node_in_nodegroup_callback=*/
      [this](const NodeID &node_id, const std::string &nodegroup_id) {
        return gcs_nodegroup_manager_->IsNodeInNodegroup(node_id, nodegroup_id);
      },
      /*reject_node_fn=*/
      [this](const NodeID &node_id) {
        gcs_heartbeat_manager_->RejectNode(node_id, Status::ExitNode("Exit this node."));
      });
  // Initialize by gcs tables data.
  gcs_node_manager_->Initialize(gcs_init_data);
  // Register service.
  node_info_service_.reset(
      new rpc::NodeInfoGrpcService(main_service_, *gcs_node_manager_));
  rpc_server_.RegisterService(*node_info_service_);
}

void GcsServer::InitGcsHeartbeatManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_node_manager_);
  gcs_heartbeat_manager_ = std::make_shared<GcsHeartbeatManager>(
      heartbeat_manager_io_service_, /*on_node_death_callback=*/
      [this](const NodeID &node_id) {
        main_service_.post(
            [this, node_id] { return gcs_node_manager_->OnNodeFailure(node_id); },
            "GcsServer.NodeDeathCallback");
      });
  // Initialize by gcs tables data.
  gcs_heartbeat_manager_->Initialize(gcs_init_data);
  // Register service.
  heartbeat_info_service_.reset(new rpc::HeartbeatInfoGrpcService(
      heartbeat_manager_io_service_, *gcs_heartbeat_manager_));
  rpc_server_.RegisterService(*heartbeat_info_service_);
}

void GcsServer::InitGcsResourceManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_ && gcs_nodegroup_manager_);
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    gcs_resource_manager_ = std::make_shared<GcsResourceManagerEx>(
        main_service_, gcs_pub_sub_, gcs_table_storage_,
        !config_.grpc_based_resource_broadcast, gcs_nodegroup_manager_,
        /*get_nodegroup_id=*/
        [this](const JobID &job_id) -> const std::string & {
          return gcs_job_manager_->GetJob(job_id)->nodegroup_id();
        },
        /*get_pending_resource_demands=*/
        [this]()
            -> absl::flat_hash_map<std::string, absl::flat_hash_map<ResourceSet, int>> {
          return gcs_actor_manager_->GetPendingResourceDemands();
        },
        /*has_any_drivers_in_node=*/
        [this](const NodeID &node_id) {
          return gcs_job_manager_->HasAnyDriversInNode(node_id);
        });
  } else {
    gcs_resource_manager_ = std::make_shared<GcsResourceManager>(
        main_service_, gcs_pub_sub_, gcs_table_storage_,
        !config_.grpc_based_resource_broadcast);
  }
  // Initialize by gcs tables data.
  gcs_resource_manager_->Initialize(gcs_init_data);
  // Add delt resource to avoid overload.
  gcs_resource_manager_->AddResourceDelta();

  // Register service.
  node_resource_info_service_.reset(
      new rpc::NodeResourceInfoGrpcService(main_service_, *gcs_resource_manager_));
  rpc_server_.RegisterService(*node_resource_info_service_);
}

void GcsServer::InitGcsResourceScheduler() {
  RAY_CHECK(gcs_resource_manager_);
  gcs_resource_scheduler_ = std::make_shared<GcsResourceScheduler>(
      *gcs_resource_manager_,
      /*is_node_schedulable_callback*/
      [this](const NodeContext &node, const ResourceScheduleContext *context) {
        if (gcs_frozen_node_manager_->IsNodeFrozen(node.node_name_)) {
          return false;
        }
        // Placement group is already scheduled before, we don't need to schedule it
        // again. Just pass it.
        if (context->RequirePlacementGroup()) {
          return true;
        }
        return gcs_nodegroup_manager_->IsNodeInNodegroup(node.node_id_,
                                                         context->GetNodegroupId());
      });
}

void GcsServer::InitGcsJobManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_ && gcs_nodegroup_manager_ &&
            gcs_node_manager_ && gcs_resource_manager_);
  gcs_job_manager_ = std::make_shared<GcsJobManager>(
      gcs_table_storage_, gcs_pub_sub_, *runtime_env_manager_, gcs_nodegroup_manager_,
      gcs_node_manager_, gcs_resource_manager_,
      /*is_node_frozen_fn=*/
      [this](const NodeID &node_id) {
        return gcs_frozen_node_manager_->IsNodeFrozen(node_id);
      },
      /*get_job_resources=*/
      [this](std::shared_ptr<JobTableData> job_data,
             JobResourceType resource_type) -> ResourceSet {
        if (gcs_job_distribution_ == nullptr) {
          return ResourceSet();
        }
        auto job_scheduling_context = gcs_job_distribution_->GetJobSchedulingContext(
            JobID::FromBinary(job_data->job_id()));
        if (job_scheduling_context == nullptr) {
          return ResourceSet();
        }
        switch (resource_type) {
        case JobResourceType::ACQUIRED:
          return job_scheduling_context->GetAcquiredResources();
        case JobResourceType::SHORTTERM:
          return job_scheduling_context->GetShorttermRuntimeResources();
        case JobResourceType::LONGTERM:
          return job_scheduling_context->GetLongtermRuntimeResources();
        case JobResourceType::RUNTIME_RESOURCE_REQUIREMENTS:
          return job_scheduling_context->GetRuntimeResourceRequirements();
        default:
          return ResourceSet();
        }
      });
  // Initialize by gcs tables data.
  gcs_job_manager_->Initialize(gcs_init_data);

  // Init job distribution.
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    auto runtime_resource_flusher =
        std::make_shared<GcsWorkerProcessRuntimeResourceFlusher>();
    auto runner = std::make_shared<PeriodicalRunner>(main_service_);
    runner->RunFnPeriodically(
        [this, runtime_resource_flusher, runner] {
          runtime_resource_flusher->Flush(gcs_table_storage_);
        },
        RayConfig::instance().gcs_runtime_resource_flush_interval_ms());
    gcs_job_distribution_ = std::make_shared<GcsJobDistribution>(
        /*gcs_job_scheduling_factory=*/
        [this](const JobID &job_id) {
          auto job_data = gcs_job_manager_->GetJob(job_id);
          GcsJobConfig gcs_job_config(
              job_id, job_data->nodegroup_id(), job_data->config().ray_namespace(),
              job_data->config().num_java_workers_per_process(),
              job_data->config().java_worker_process_default_memory_units(),
              job_data->config().total_memory_units(),
              job_data->config().max_total_memory_units(), job_data->job_name(),
              gcs_nodegroup_manager_->IsJobQuotaEnabled(job_data->nodegroup_id()));
          auto schedule_options =
              gcs_nodegroup_manager_->GetScheduleOptions(job_data->nodegroup_id());
          return std::make_shared<GcsJobSchedulingContext>(gcs_job_config,
                                                           std::move(schedule_options));
        },
        gcs_resource_manager_, runtime_resource_flusher);
  }

  // Register service.
  job_info_service_.reset(new rpc::JobInfoGrpcService(main_service_, *gcs_job_manager_));
  rpc_server_.RegisterService(*job_info_service_);
}

void GcsServer::InitGcsActorManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_ && gcs_node_manager_ &&
            gcs_resource_manager_ && gcs_resource_scheduler_);
  auto gcs_label_manager_ = std::make_shared<GcsLabelManager>();
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    RAY_CHECK(gcs_job_distribution_);
    gcs_actor_scheduling_strategy_ = std::make_shared<ResourceBasedSchedulingStrategy>(
        gcs_resource_manager_, gcs_job_distribution_, gcs_table_storage_,
        gcs_resource_scheduler_, gcs_placement_group_manager_,
        /*is_bundle_resource_reserved=*/
        [this](const BundleID &bundle_id) {
          auto placement_group_manager =
              std::dynamic_pointer_cast<GcsPlacementGroupManagerEx>(
                  gcs_placement_group_manager_);
          return placement_group_manager->IsBundleResourceReserved(bundle_id);
        },
        /*is_node_in_nodegroup_callback=*/
        [this](const NodeID &node_id, const std::string &nodegroup_id) {
          return gcs_nodegroup_manager_->IsNodeInNodegroup(node_id, nodegroup_id);
        },
        gcs_label_manager_);
  } else {
    gcs_actor_scheduling_strategy_ =
        std::make_shared<GcsRandomActorScheduleStrategy>(gcs_node_manager_);
  }
  // Initialize gcs actor scheduling policy by gcs tables data.
  gcs_actor_scheduling_strategy_->Initialize(gcs_init_data);

  auto scheduler = std::make_shared<GcsActorScheduler>(
      main_service_, gcs_table_storage_->ActorTable(),
      gcs_table_storage_->ActorTaskSpecTable(), *gcs_node_manager_, gcs_pub_sub_,
      /*schedule_failure_handler=*/
      [this](std::shared_ptr<GcsActor> actor,
             rpc::RequestWorkerLeaseReply::SchedulingFailureType failure_type,
             const std::string &scheduling_failure_message) {
        // When there are no available nodes to schedule the actor the
        // gcs_actor_scheduler will treat it as failed and invoke this handler. In
        // this case, the actor manager should schedule the actor once an
        // eligible node is registered.
        gcs_actor_manager_->OnActorSchedulingFailed(std::move(actor), failure_type,
                                                    scheduling_failure_message);
      },
      /*schedule_success_handler=*/
      [this](std::shared_ptr<GcsActor> actor) {
        gcs_actor_manager_->OnActorCreationSuccess(std::move(actor));
      },
      raylet_client_pool_, gcs_actor_scheduling_strategy_,
      /*client_factory=*/
      [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      },
      /*get_job_info_func=*/
      [this](const JobID &job_id) { return gcs_job_manager_->GetJob(job_id); },
      /*resources_seized_by_normal_tasks_callback=*/
      [this](const NodeID &node_id, const rpc::ResourcesData &resources_data) {
        return gcs_resource_manager_->UpdateNodeRealtimeResources(node_id,
                                                                  resources_data);
      },
      gcs_label_manager_);
  gcs_actor_manager_ = std::make_shared<GcsActorManager>(
      main_service_, scheduler, gcs_job_manager_, gcs_table_storage_, gcs_pub_sub_,
      *runtime_env_manager_,
      [this](const JobID &job_id) { return gcs_job_manager_->GetRayNamespace(job_id); },
      [this](std::function<void(void)> fn, boost::posix_time::milliseconds delay) {
        auto timer = std::make_shared<boost::asio::deadline_timer>(main_service_);
        timer->expires_from_now(delay);
        timer->async_wait([timer, fn](const boost::system::error_code &error) {
          if (error != boost::asio::error::operation_aborted) {
            fn();
          } else {
            RAY_LOG(WARNING)
                << "The GCS actor metadata garbage collector timer failed to fire. This "
                   "could old actor metadata not being properly cleaned up. For more "
                   "information, check logs/gcs_server.err and logs/gcs_server.out";
          }
        });
      },
      [this](const rpc::Address &address) {
        return std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
      });
  auto gcs_actor_manager_l1_handler = std::make_shared<GcsActorManagerL1Handler>(
      gcs_job_manager_, gcs_actor_manager_, main_service_,
      /*node_client_factory=*/
      [this](const rpc::Address &address) {
        auto node_client = rpc::NodeManagerWorkerClient::make(
            address.ip_address(), address.port(), client_call_manager_);
        return std::make_shared<ray::raylet::RayletClient>(std::move(node_client));
      },
      /*get_job_nodes=*/
      [this](const JobID &job_id) { return gcs_job_manager_->GetNodesByJobId(job_id); },
      /*get_node_info=*/
      [this](const ray::NodeID &node_id) {
        return gcs_node_manager_->GetAliveNode(node_id);
      },
      /*trigger_pending_actors_scheduling=*/
      [this]() { gcs_actor_manager_->SchedulePendingActors(); });
  gcs_actor_manager_->SetL1Handler(gcs_actor_manager_l1_handler);
  // Initialize by gcs tables data.
  gcs_actor_manager_->Initialize(gcs_init_data);
  // Register service.
  actor_info_service_.reset(
      new rpc::ActorInfoGrpcService(main_service_, *gcs_actor_manager_));
  rpc_server_.RegisterService(*actor_info_service_);
}

void GcsServer::InitGcsNodegroupManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_);
  gcs_nodegroup_manager_ = std::make_shared<gcs::GcsNodegroupManager>(
      gcs_table_storage_, gcs_pub_sub_,
      /*has_any_running_jobs_in_nodegroup_fn=*/
      [this](const std::string &nodegroup_id) {
        return gcs_job_manager_->HasAnyRunningJobs(nodegroup_id);
      },
      /*exit_node_fn=*/
      [this](const NodeID &node_id, const Status &status) {
        gcs_heartbeat_manager_->RejectNode(node_id, status);
      },
      /*has_any_running_jobs_in_node_fn=*/
      [this](const NodeID &node_id) {
        if ((gcs_job_distribution_ &&
             gcs_job_distribution_->GetNodeToJobs().contains(node_id)) ||
            gcs_job_manager_->HasAnyDriversInNode(node_id)) {
          return true;
        }
        // Check if the node has any placement group.
        const auto &cluster_resoruces = gcs_resource_manager_->GetClusterResources();
        auto iter = cluster_resoruces.find(node_id);
        if (iter != cluster_resoruces.end()) {
          return iter->second->GetTotalResources().ContainsPlacementGroup();
        }
        return false;
      },
      /*is_frozen_node_fn=*/
      [this](const NodeID &node_id) {
        auto node = gcs_node_manager_->GetAliveNode(node_id);
        return node && gcs_frozen_node_manager_->IsNodeFrozen(
                           node.value()->basic_gcs_node_info().node_name());
      },
      /*updating_nodegroup_schedule_options_fn=*/
      [this](std::shared_ptr<NodegroupData> nodegroup_data) {
        auto local_schedule_options =
            gcs_nodegroup_manager_->GetScheduleOptions(nodegroup_data->nodegroup_id_);
        auto schedule_options = nodegroup_data->schedule_options_;
        if (local_schedule_options->runtime_resource_scheduling_enabled_ !=
            schedule_options->runtime_resource_scheduling_enabled_) {
          gcs_job_distribution_->OnRuntimeResourceSchedulingConfigChanged(
              nodegroup_data->nodegroup_id_, schedule_options);
        }
      });
  // Initialize by gcs tables data.
  gcs_nodegroup_manager_->Initialize(gcs_init_data);
  // Register service.
  nodegroup_service_.reset(
      new rpc::NodegroupInfoGrpcService(main_service_, *gcs_nodegroup_manager_));
  rpc_server_.RegisterService(*nodegroup_service_);
}

void GcsServer::InitGcsPlacementGroupManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_node_manager_ && gcs_pub_sub_ &&
            gcs_resource_scheduler_);
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    RAY_CHECK(gcs_job_distribution_);
    auto scheduler = std::make_shared<GcsPlacementGroupSchedulerEx>(
        main_service_, gcs_table_storage_, *gcs_node_manager_, *gcs_resource_manager_,
        gcs_resource_scheduler_, gcs_job_distribution_, raylet_client_pool_,
        /*is_placement_group_lifetime_done_*/
        [this](const PlacementGroupID &placement_group_id) {
          return gcs_placement_group_manager_->IsPlacementGroupLifetimeDone(
              placement_group_id);
        },
        /*is_node_in_nodegroup_fn=*/
        [this](const NodeID &node_id, const std::string &nodegroup_id) {
          return gcs_nodegroup_manager_->IsNodeInNodegroup(node_id, nodegroup_id);
        });
    gcs_placement_group_manager_ = std::make_shared<GcsPlacementGroupManagerEx>(
        main_service_, scheduler, gcs_table_storage_, *gcs_resource_manager_,
        gcs_pub_sub_,
        /*get_ray_namespace=*/
        [this](const JobID &job_id) { return gcs_job_manager_->GetRayNamespace(job_id); },
        gcs_job_distribution_,
        /*placement_group_create_success_callback=*/
        [this](const PlacementGroupID &placement_group_id) {
          main_service_.post([this] {
            // Because placement group create successfully, we need to try to schedule
            // the pending actors with this placement group.
            gcs_actor_manager_->SchedulePendingActors();
          });
        });
  } else {
    auto scheduler = std::make_shared<GcsPlacementGroupScheduler>(
        main_service_, gcs_table_storage_, *gcs_node_manager_, *gcs_resource_manager_,
        raylet_client_pool_,
        /*is_placement_group_lifetime_done_*/
        [this](const PlacementGroupID &placement_group_id) {
          return gcs_placement_group_manager_->IsPlacementGroupLifetimeDone(
              placement_group_id);
        });
    gcs_placement_group_manager_ = std::make_shared<GcsPlacementGroupManager>(
        main_service_, scheduler, gcs_table_storage_, *gcs_resource_manager_,
        gcs_pub_sub_, [this](const JobID &job_id) {
          return gcs_job_manager_->GetRayNamespace(job_id);
        });
  }

  // Initialize by gcs tables data.
  gcs_placement_group_manager_->Initialize(gcs_init_data);
  // Register service.
  placement_group_info_service_.reset(new rpc::PlacementGroupInfoGrpcService(
      main_service_, *gcs_placement_group_manager_));
  rpc_server_.RegisterService(*placement_group_info_service_);
}

void GcsServer::InitObjectManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_ && gcs_node_manager_);
  gcs_object_manager_.reset(
      new GcsObjectManager(gcs_table_storage_, gcs_pub_sub_, *gcs_node_manager_));
  // Initialize by gcs tables data.
  gcs_object_manager_->Initialize(gcs_init_data);
  // Register service.
  object_info_service_.reset(
      new rpc::ObjectInfoGrpcService(main_service_, *gcs_object_manager_));
  rpc_server_.RegisterService(*object_info_service_);
}

void GcsServer::StoreGcsServerAddressInRedis() {
  std::string ip = config_.node_ip_address;
  if (ip.empty()) {
    ip = GetValidLocalIp(
        GetPort(),
        RayConfig::instance().internal_gcs_service_connect_wait_milliseconds());
  }
  std::string address = ip + ":" + std::to_string(GetPort());
  RAY_LOG(INFO) << "Gcs server address = " << address;

  RAY_CHECK_OK(redis_client_->GetPrimaryContext()->RunArgvAsync(
      {"SET", "GcsServerAddress", address}));
  RAY_LOG(INFO) << "Finished setting gcs server address: " << address;
}

bool GcsServer::IsGcsServerRestart() {
  RAY_CHECK(redis_client_ != nullptr);
  auto reply =
      redis_client_->GetPrimaryContext()->RunArgvSync({"GET", "GcsServerAddress"});
  if (!reply || reply->IsNil()) {
    RAY_LOG(DEBUG) << "Can't get gcs server address from redis.";
    return false;
  }
  return true;
}

void GcsServer::InitTaskInfoHandler() {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_);
  task_info_handler_.reset(
      new rpc::DefaultTaskInfoHandler(gcs_table_storage_, gcs_pub_sub_));
  // Register service.
  task_info_service_.reset(
      new rpc::TaskInfoGrpcService(main_service_, *task_info_handler_));
  rpc_server_.RegisterService(*task_info_service_);
}

void GcsServer::InitResourceReportPolling(const GcsInitData &gcs_init_data) {
  if (config_.pull_based_resource_reporting) {
    gcs_resource_report_poller_.reset(new GcsResourceReportPoller(
        raylet_client_pool_, [this](const rpc::ResourcesData &report) {
          gcs_resource_manager_->UpdateFromResourceReport(report);
        }));

    gcs_resource_report_poller_->Initialize(gcs_init_data);
    gcs_resource_report_poller_->Start();
  }
}

void GcsServer::InitResourceReportBroadcasting(const GcsInitData &gcs_init_data) {
  if (config_.grpc_based_resource_broadcast) {
    grpc_based_resource_broadcaster_.reset(new GrpcBasedResourceBroadcaster(
        raylet_client_pool_,
        [this](rpc::ResourceUsageBatchData &buffer) {
          gcs_resource_manager_->GetResourceUsageBatchForBroadcast(buffer);
        }

        ));

    grpc_based_resource_broadcaster_->Initialize(gcs_init_data);
    grpc_based_resource_broadcaster_->Start();
  }
}

void GcsServer::InitStatsHandler() {
  RAY_CHECK(gcs_table_storage_);
  stats_handler_.reset(new rpc::DefaultStatsHandler(gcs_table_storage_));
  // Register service.
  stats_service_.reset(new rpc::StatsGrpcService(main_service_, *stats_handler_));
  rpc_server_.RegisterService(*stats_service_);
}

void GcsServer::InitKVManager() {
  kv_manager_ = std::make_unique<GcsInternalKVManager>(redis_client_);
  kv_service_ = std::make_unique<rpc::InternalKVGrpcService>(main_service_, *kv_manager_);
  // Register service.
  rpc_server_.RegisterService(*kv_service_);
}

void GcsServer::InitRuntimeEnvManager() {
  runtime_env_manager_ =
      std::make_unique<RuntimeEnvManager>([this](const std::string &uri, auto cb) {
        std::string sep = "://";
        auto pos = uri.find(sep);
        if (pos == std::string::npos || pos + sep.size() == uri.size()) {
          RAY_LOG(ERROR) << "Invalid uri: " << uri;
          cb(false);
        } else {
          auto scheme = uri.substr(0, pos);
          if (scheme != "gcs") {
            // Skip other uri
            cb(true);
          } else {
            this->kv_manager_->InternalKVDelAsync(uri, [cb](int deleted_num) {
              if (deleted_num == 0) {
                cb(false);
              } else {
                cb(true);
              }
            });
          }
        }
      });
}

void GcsServer::InitGcsWorkerManager(const GcsInitData &gcs_init_data) {
  gcs_worker_manager_ = std::unique_ptr<GcsWorkerManager>(new GcsWorkerManager(
      gcs_table_storage_, gcs_pub_sub_,
      [this](const JobID &job_id) { return gcs_job_manager_->GetJob(job_id); }));
  // Initialize by gcs tables data.
  gcs_worker_manager_->Initialize(gcs_init_data);
  // Register service.
  worker_info_service_.reset(
      new rpc::WorkerInfoGrpcService(main_service_, *gcs_worker_manager_));
  rpc_server_.RegisterService(*worker_info_service_);
}

void GcsServer::InstallEventListeners() {
  // Install node event listeners.
  gcs_node_manager_->AddNodeAddedListener([this](std::shared_ptr<rpc::GcsNodeInfo> node) {
    RAY_EVENT(INFO, "NodeAdded")
        << "Node " << NodeID::FromBinary(node->basic_gcs_node_info().node_id())
        << " added. Address: " << node->basic_gcs_node_info().node_manager_address()
        << ":" << node->basic_gcs_node_info().node_manager_port()
        << ", Hostname: " << node->basic_gcs_node_info().node_manager_hostname();
    // This method should invoked in advance.
    gcs_node_manager_->QuickDetectNodeFailureByName(
        node->basic_gcs_node_info().node_name());
    gcs_resource_manager_->OnNodeAdd(*node);
    gcs_nodegroup_manager_->AddNode(*node);
    // Because a new node has been added, we need to try to schedule the pending
    // placement groups and the pending actors.
    gcs_placement_group_manager_->SchedulePendingPlacementGroups();
    gcs_actor_manager_->SchedulePendingActors();
    gcs_heartbeat_manager_->AddNode(
        NodeID::FromBinary(node->basic_gcs_node_info().node_id()));
    if (config_.pull_based_resource_reporting) {
      gcs_resource_report_poller_->HandleNodeAdded(*node);
    }
    if (config_.grpc_based_resource_broadcast) {
      grpc_based_resource_broadcaster_->HandleNodeAdded(*node);
    }
  });
  gcs_node_manager_->AddNodeRemovedListener([this](
                                                std::shared_ptr<rpc::GcsNodeInfo> node) {
    const auto node_ip_address = node->basic_gcs_node_info().node_manager_address();
    auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
    RAY_EVENT(ERROR, "NodeRemoved")
        << "Node " << node_id << " removed. Address: " << node_ip_address << ":"
        << node->basic_gcs_node_info().node_manager_port()
        << ", Hostname: " << node->basic_gcs_node_info().node_manager_hostname();
    gcs_resource_manager_->OnNodeDead(node_id);
    // Update deploy info.
    gcs_nodegroup_manager_->RemoveNode(node);
    gcs_job_manager_->OnNodeRemoved(node_id);
    // Update gcs_actor_scheduling_strategy states.
    const auto &jobs = gcs_actor_scheduling_strategy_->OnNodeRemoved(node_id);
    for (auto &iter : jobs) {
      RAY_EVENT(ERROR, "NodeRemoved").WithField("job_id", iter.Hex())
          << "Node " << node_id
          << " removed. Address: " << node->basic_gcs_node_info().node_manager_address()
          << ":" << node->basic_gcs_node_info().node_manager_port()
          << ", Hostname: " << node->basic_gcs_node_info().node_manager_hostname();
    }
    // All of the related placement groups and actors should be reconstructed when a
    // node is removed from the GCS.
    gcs_placement_group_manager_->OnNodeDead(node_id);
    gcs_actor_manager_->OnNodeDead(node_id, node_ip_address);
    raylet_client_pool_->Disconnect(
        NodeID::FromBinary(node->basic_gcs_node_info().node_id()));
    if (config_.pull_based_resource_reporting) {
      gcs_resource_report_poller_->HandleNodeRemoved(*node);
    }
    if (config_.grpc_based_resource_broadcast) {
      grpc_based_resource_broadcaster_->HandleNodeRemoved(*node);
    }
    ray::stats::RayletCrashCount().Record(1);
  });

  // Install worker event listener.
  gcs_worker_manager_->AddWorkerDeadListener(
      [this](std::shared_ptr<rpc::WorkerTableData> worker_failure_data) {
        auto &worker_address = worker_failure_data->worker_address();
        auto worker_id = WorkerID::FromBinary(worker_address.worker_id());
        auto node_id = NodeID::FromBinary(worker_address.raylet_id());
        auto worker_ip = worker_address.ip_address();
        std::shared_ptr<rpc::RayException> creation_task_exception = nullptr;
        if (worker_failure_data->has_creation_task_exception()) {
          creation_task_exception = std::make_shared<rpc::RayException>(
              worker_failure_data->creation_task_exception());
        }
        auto worker_process_id =
            UniqueID::FromBinary(worker_failure_data->worker_process_id());
        auto job_id = JobID::FromBinary(worker_failure_data->job_id());
        gcs_actor_scheduling_strategy_->OnWorkerProcessDead(node_id, worker_id,
                                                            worker_process_id, job_id);
        gcs_actor_manager_->OnWorkerDead(node_id, worker_id, worker_ip,
                                         worker_failure_data->exit_type(),
                                         creation_task_exception);
        ray::stats::WorkerCrashCount().Record(1);
      });

  // Install job event listeners.
  gcs_job_manager_->AddJobFinishedListener([this](std::shared_ptr<JobID> job_id) {
    gcs_actor_manager_->OnJobFinished(*job_id);
    gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(*job_id);
    gcs_actor_scheduling_strategy_->OnJobFinished(*job_id);

    auto job_data = gcs_job_manager_->GetJob(*job_id);
    gcs_nodegroup_manager_->OnJobFinished(job_data);

    RAY_CHECK_OK(gcs_table_storage_->WorkerProcessRuntimeResourceTable().DeleteByJobId(
        *job_id, nullptr));
    kv_manager_->DeleteJobKV(*job_id);
  });

  // Install scheduling policy event listeners.
  if (RayConfig::instance().gcs_task_scheduling_enabled()) {
    gcs_resource_manager_->AddResourcesChangedListener([this] {
      main_service_.post([this] {
        // Because a new node has been added, we need to try to schedule the pending
        // placement groups and the pending actors.
        gcs_placement_group_manager_->SchedulePendingPlacementGroups();
        gcs_actor_manager_->SchedulePendingActors();
      });
    });

    auto scheduling_policy = std::dynamic_pointer_cast<ResourceBasedSchedulingStrategy>(
        gcs_actor_scheduling_strategy_);
    scheduling_policy->AddClusterResourcesChangedListener([this] {
      main_service_.post([this] {
        gcs_placement_group_manager_->OnClusterResourcesChanged();
        gcs_actor_manager_->SchedulePendingActors();
      });
    });

    gcs_job_manager_->SetJobResourceRequirementsChangedCallback(
        [scheduling_policy](const rpc::JobTableData &job_table_data) {
          return scheduling_policy->UpdateJobResourceRequirements(job_table_data);
        });
  }

  gcs_nodegroup_manager_->AddNodegroupNodeAddedListener(
      [this](const std::string &nodegroup_id) {
        main_service_.post([this] {
          // Because a new node has been added, we need to try to schedule the pending
          // placement groups and the pending actors.
          gcs_placement_group_manager_->SchedulePendingPlacementGroups();
          gcs_actor_manager_->SchedulePendingActors();
        });
      });
}

void GcsServer::CollectStats() {
  gcs_actor_manager_->CollectStats();
  gcs_placement_group_manager_->CollectStats();
  if (gcs_job_distribution_) {
    gcs_job_distribution_->CollectStats();
  }
  execute_after(
      main_service_, [this] { CollectStats(); },
      (RayConfig::instance().metrics_report_interval_ms() / 2) /* milliseconds */);
}

void GcsServer::InitDeadDataCleaner() {
  auto ttl_runner = std::make_shared<PeriodicalRunner>(main_service_);
  // Check clean the dead data.
  ttl_runner->RunFnPeriodically(
      [this, ttl_runner] {
        gcs_actor_manager_->EvictExpiredActors();
        gcs_node_manager_->EvictExpiredNodes();
        gcs_worker_manager_->EvictExpiredWorkers();
        gcs_job_manager_->EvictExpiredJobs();
      },
      RayConfig::instance().gcs_dead_data_check_interval_ms());
}

void GcsServer::PrintDebugInfo() {
  std::ostringstream stream;
  stream << gcs_node_manager_->DebugString() << "\n"
         << gcs_actor_manager_->DebugString() << "\n"
         << gcs_object_manager_->DebugString() << "\n"
         << gcs_placement_group_manager_->DebugString() << "\n"
         << gcs_pub_sub_->DebugString() << "\n"
         << gcs_resource_manager_->DebugString() << "\n"
         << ((rpc::DefaultTaskInfoHandler *)task_info_handler_.get())->DebugString();

  if (config_.grpc_based_resource_broadcast) {
    stream << "\n" << grpc_based_resource_broadcaster_->DebugString();
  }
  // TODO(ffbin): We will get the session_dir in the next PR, and write the log to
  // gcs_debug_state.txt.
  RAY_LOG(INFO) << stream.str();
  execute_after(main_service_, [this] { PrintDebugInfo(); },
                (RayConfig::instance().gcs_dump_debug_log_interval_minutes() *
                 60000) /* milliseconds */);
}

void GcsServer::PrintAsioStats() {
  /// If periodic asio stats print is enabled, it will print it.
  const auto asio_stats_print_interval_ms =
      RayConfig::instance().asio_stats_print_interval_ms();
  if (asio_stats_print_interval_ms != -1 &&
      RayConfig::instance().asio_event_loop_stats_collection_enabled()) {
    RAY_LOG(INFO) << "Event loop stats:\n\n" << main_service_.StatsString() << "\n\n";
    execute_after(main_service_, [this] { PrintAsioStats(); },
                  asio_stats_print_interval_ms /* milliseconds */);
  }
}

void GcsServer::DetectHang() {
  // Detect if the main thread is hang.
  auto last_detect_time_ms = std::make_shared<int64_t>(current_time_ms());
  auto detect_interval_ms =
      RayConfig::instance().gcs_main_thread_busy_detect_interval_ms();
  detect_hang_timer_.expires_from_now(std::chrono::milliseconds(detect_interval_ms));
  detect_hang_timer_.async_wait([this, last_detect_time_ms, detect_interval_ms](
                                    const boost::system::error_code &error) {
    if (error) {
      return;
    }

    auto current_ms = current_time_ms();
    auto real_interval_ms = current_ms - *last_detect_time_ms;
    auto maximum_miss_detect_count =
        RayConfig::instance().gcs_main_thread_maximum_miss_detect_count();
    if (real_interval_ms > maximum_miss_detect_count * detect_interval_ms) {
      std::ostringstream ostr;
      ostr << "The detect task was delayed by " << (real_interval_ms - detect_interval_ms)
           << "(ms) to execute, the main thread of gcs maybe high load.";
      std::string message = ostr.str();
      RAY_LOG(WARNING) << message;
      RAY_EVENT(ERROR, EVENT_LABEL_GCS_MAIN_THREAD_BUSY) << message;
    }
    *last_detect_time_ms = current_ms;
    DetectHang();
  });
}

void GcsServer::InitGcsFrozenNodeManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_table_storage_ && gcs_pub_sub_ && gcs_node_manager_ &&
            gcs_resource_manager_ && gcs_placement_group_manager_);
  gcs_frozen_node_manager_ = std::make_shared<gcs::GcsFrozenNodeManager>(
      main_service_, gcs_table_storage_, gcs_pub_sub_, gcs_node_manager_,
      gcs_resource_manager_,
      std::dynamic_pointer_cast<GcsPlacementGroupManagerEx>(gcs_placement_group_manager_),
      /*freeze_done_listener=*/
      [](const std::string &node_name, const NodeID &node_id) {
        // Do nothing currently.
      },
      /*unfreeze_done_listener=*/
      [this](const std::unordered_set<std::string> &node_name_set) {
        // Because a new node was unfrozen, we need to try to schedule the pending
        // placement groups and the pending actors.
        gcs_placement_group_manager_->SchedulePendingPlacementGroups();
        gcs_actor_manager_->SchedulePendingActors();
      });
  // Initialize by gcs tables data.
  gcs_frozen_node_manager_->Initialize(gcs_init_data);
  // Register service.
  frozen_node_service_.reset(
      new rpc::FrozenNodeGrpcService(main_service_, *gcs_frozen_node_manager_));
  rpc_server_.RegisterService(*frozen_node_service_);
}

void GcsServer::InitGcsActorMigrationManager(const GcsInitData &gcs_init_data) {
  RAY_CHECK(gcs_pub_sub_ && gcs_frozen_node_manager_ && gcs_actor_manager_ &&
            gcs_node_manager_ && gcs_resource_manager_ && gcs_placement_group_manager_ &&
            gcs_job_manager_);
  gcs_actor_migration_manager_ = std::make_shared<gcs::GcsActorMigrationManager>(
      main_service_, gcs_pub_sub_, gcs_frozen_node_manager_, gcs_actor_manager_,
      gcs_node_manager_, gcs_resource_manager_, gcs_placement_group_manager_,
      gcs_job_manager_);
  // Initialize by gcs tables data.
  gcs_actor_migration_manager_->Initialize(gcs_init_data);
  // Register service.
  actor_migration_service_.reset(
      new rpc::ActorMigrationGrpcService(main_service_, *gcs_actor_migration_manager_));
  rpc_server_.RegisterService(*actor_migration_service_);
}

void GcsServer::InitRuntimeResourceManager(const GcsInitData &gcs_init_data) {
  if (!RayConfig::instance().gcs_task_scheduling_enabled()) {
    return;
  }

  RAY_CHECK(gcs_table_storage_ && gcs_resource_manager_ && gcs_job_distribution_);
  gcs_runtime_resource_manager_ = std::make_shared<GcsRuntimeResourceManager>(
      gcs_table_storage_, gcs_resource_manager_, gcs_job_distribution_,
      /*shortterm_runtime_resources_updated_callback=*/
      [this](std::shared_ptr<
             absl::flat_hash_map<NodeID, absl::flat_hash_map<int, ResourceSet>>>
                 worker_process_resources) {
        main_service_.post([this, captured_worker_process_resources =
                                      std::move(worker_process_resources)] {
          auto job_shortterm_runtime_resources =
              gcs_job_distribution_->OnRuntimeWorkerProcessResourcesUpdated(
                  std::move(captured_worker_process_resources));
          gcs_runtime_resource_manager_->RecordJobShorttermRuntimeResources(
              std::move(job_shortterm_runtime_resources));
        });
      },
      /*job_longterm_runtime_resources_updated_callback=*/
      [this](const JobID job_id, const ResourceSet resource_set) {
        main_service_.post([this, job_id,
                            captured_resource_set = std::move(resource_set)] {
          auto job_scheduling_context =
              gcs_job_distribution_->GetJobSchedulingContext(job_id);
          if (job_scheduling_context) {
            job_scheduling_context->UpdateLongtermRuntimeResources(captured_resource_set);
          }
        });
      });
  // Initialize by gcs tables data.
  gcs_runtime_resource_manager_->Initialize(gcs_init_data);

  // Subtract the deta resource, it's paired with `AddResourceDelta()`.
  gcs_resource_manager_->SubtractResourceDelta();

  // Register service.
  runtime_resource_service_.reset(new rpc::RuntimeResourceInfoGrpcService(
      main_service_, *gcs_runtime_resource_manager_));
  rpc_server_.RegisterService(*runtime_resource_service_);
}

}  // namespace gcs
}  // namespace ray
