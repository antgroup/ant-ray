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

#include "ray/gcs/gcs_client/service_based_gcs_client.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/gcs_client/service_based_accessor.h"

extern "C" {
#include "hiredis/hiredis.h"
}

namespace ray {
namespace gcs {

ServiceBasedGcsClient::ServiceBasedGcsClient(
    const GcsClientOptions &options, bool gcs_service_address_check_enable,
    std::function<bool(std::pair<std::string, int> *)> get_gcs_server_address_func)
    : GcsClient(options),
      gcs_service_address_check_enable_(gcs_service_address_check_enable),
      get_server_address_func_(get_gcs_server_address_func),
      last_reconnect_timestamp_ms_(0),
      last_reconnect_address_(std::make_pair("", -1)) {}

Status ServiceBasedGcsClient::Connect(instrumented_io_context &io_service) {
  RAY_CHECK(!is_connected_);

  if (options_.server_ip_.empty()) {
    RAY_LOG(ERROR) << "Failed to connect, gcs service address is empty.";
    return Status::Invalid("gcs service address is invalid!");
  }

  // Connect to redis.
  // We don't access redis shardings in GCS client, so we set `enable_sharding_conn` to
  // false.
  RedisClientOptions redis_client_options(
      options_.server_ip_, options_.server_port_, options_.password_,
      /*enable_sharding_conn=*/false, options_.enable_sync_conn_,
      options_.enable_async_conn_, options_.enable_subscribe_conn_);
  redis_client_.reset(new RedisClient(redis_client_options));
  RAY_CHECK_OK(redis_client_->Connect(io_service));

  // Init gcs pub sub instance.
  gcs_pub_sub_.reset(new GcsPubSub(redis_client_));

  // Get gcs server address.
  if (get_server_address_func_) {
    get_server_address_func_(&current_gcs_server_address_);
    int i = 0;
    while (current_gcs_server_address_.first.empty() &&
           i < RayConfig::instance().gcs_service_connect_retries()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(
          RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()));
      get_server_address_func_(&current_gcs_server_address_);
      i++;
    }
  } else {
    get_server_address_func_ = [this](std::pair<std::string, int> *address) {
      return GetGcsServerAddressFromRedis(
          redis_client_->GetPrimaryContext()->sync_context(), address);
    };
    RAY_CHECK(GetGcsServerAddressFromRedis(
        redis_client_->GetPrimaryContext()->sync_context(), &current_gcs_server_address_,
        RayConfig::instance().gcs_service_connect_retries()))
        << "Failed to get gcs server address when init gcs client.";
  }

  resubscribe_func_ = [this](bool is_pubsub_server_restarted) {
    job_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    actor_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    node_resource_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    task_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    object_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    worker_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    nodegroup_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    frozen_node_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
    actor_migration_accessor_->AsyncResubscribe(is_pubsub_server_restarted);
  };

  // Connect to gcs service.
  client_call_manager_.reset(
      new rpc::ClientCallManager(io_service, /*num_threads=*/1,
                                 /*request_in_current_thread=*/true));
  gcs_rpc_client_.reset(new rpc::GcsRpcClient(
      current_gcs_server_address_.first, current_gcs_server_address_.second,
      *client_call_manager_,
      [this](rpc::GcsServiceFailureType type) { GcsServiceFailureDetected(type); }));
  job_accessor_.reset(new ServiceBasedJobInfoAccessor(this));
  actor_accessor_.reset(new ServiceBasedActorInfoAccessor(this));
  node_accessor_.reset(new BasicNodeInfoAccessor(this));
  full_node_info_accessor_.reset(new FullNodeInfoAccessor(this));
  node_resource_accessor_.reset(new ServiceBasedNodeResourceInfoAccessor(this));
  task_accessor_.reset(new ServiceBasedTaskInfoAccessor(this));
  object_accessor_.reset(new ServiceBasedObjectInfoAccessor(this));
  stats_accessor_.reset(new ServiceBasedStatsInfoAccessor(this));
  error_accessor_.reset(new ServiceBasedErrorInfoAccessor(this));
  worker_accessor_.reset(new ServiceBasedWorkerInfoAccessor(this));
  placement_group_accessor_.reset(new ServiceBasedPlacementGroupInfoAccessor(this));
  nodegroup_accessor_.reset(new ServiceBasedNodegroupInfoAccessor(this));
  frozen_node_accessor_.reset(new ServiceBasedFrozenNodeAccessor(this));
  actor_migration_accessor_.reset(new ServiceBasedActorMigrationAccessor(this));

  // ANT-INTERNAL: Getting data again after subscribe context being reconnected in case of
  // data loss.
  std::weak_ptr<ray::gcs::GcsClient> weak_self(shared_from_this());
  redis_client_->SetSubscribeCallback([weak_self, &io_service]() {
    if (auto self = weak_self.lock()) {
      RAY_LOG(INFO) << "Get all data after subscribe context being re-connected.";
      io_service.post([weak_self] {
        if (auto self = weak_self.lock()) {
          auto gcs_client = std::dynamic_pointer_cast<ServiceBasedGcsClient>(self);
          gcs_client->resubscribe_func_(false);
        }
      });
    }
  });
  internal_kv_accessor_ = std::make_unique<ServiceBasedInternalKVAccessor>(this);
  // Init gcs service address check timer.
  if (gcs_service_address_check_enable_) {
    periodical_runner_.reset(new PeriodicalRunner(io_service));
    periodical_runner_->RunFnPeriodically(
        [this] { PeriodicallyCheckGcsServerAddress(); },
        RayConfig::instance().gcs_service_address_check_interval_milliseconds(),
        "GcsClient.deadline_timer.check_gcs_service_address");
  }

  is_connected_ = true;

  RAY_LOG(DEBUG) << "ServiceBasedGcsClient connected.";
  return Status::OK();
}

void ServiceBasedGcsClient::Disconnect() {
  if (is_connected_) {
    if (gcs_service_address_check_enable_) {
      periodical_runner_.reset();
    }
    gcs_pub_sub_.reset();
    redis_client_->Disconnect();
    redis_client_.reset();
    is_connected_ = false;
    RAY_LOG(DEBUG) << "ServiceBasedGcsClient Disconnected.";
  }
}

std::pair<std::string, int> ServiceBasedGcsClient::GetGcsServerAddress() {
  return current_gcs_server_address_;
}

std::pair<std::string, int> ServiceBasedGcsClient::GetApiServerAddress() {
  RAY_CHECK(redis_client_)
      << "`Redis Client` can't be null when getting `Api Server` address.";
  const auto context = redis_client_->GetPrimaryContext()->sync_context();
  const auto reply = context->Command("GET api_server");
  RAY_CHECK(reply) << "Redis did not reply to ApiServerAddress. Is redis running?";

  if (reply->type != REDIS_REPLY_STRING) {
    RAY_LOG(WARNING) << "Expected string, found Redis type " << reply->type
                     << " for ApiServerAddress";
    freeReplyObject(reply);
    return std::make_pair("", 0);
  }
  std::string result(reply->str);
  freeReplyObject(reply);
  RAY_CHECK(!result.empty()) << "Api server address is empty";
  size_t pos = result.find(':');
  RAY_CHECK(pos != std::string::npos)
      << "Api server address format is erroneous: " << result;
  return std::make_pair(result.substr(0, pos), std::stoi(result.substr(pos + 1)));
}

bool ServiceBasedGcsClient::GetGcsServerAddressFromRedis(
    RedisContextWrapper *context, std::pair<std::string, int> *address,
    int max_attempts) {
  // Get gcs server address.
  int num_attempts = 0;
  redisReply *reply = nullptr;
  while (num_attempts < max_attempts) {
    reply = context->Command("GET GcsServerAddress");
    if (reply && reply->type != REDIS_REPLY_NIL) {
      break;
    }

    // Sleep for a little, and try again if the entry isn't there yet.
    freeReplyObject(reply);
    num_attempts++;

    if (num_attempts < max_attempts) {
      std::this_thread::sleep_for(std::chrono::milliseconds(
          RayConfig::instance().internal_gcs_service_connect_wait_milliseconds()));
    }
  }

  if (num_attempts < max_attempts) {
    RAY_CHECK(reply) << "Redis did not reply to GcsServerAddress. Is redis running?";
    if (reply->type != REDIS_REPLY_STRING) {
      RAY_LOG(WARNING) << "Expected string, found Redis type " << reply->type
                       << " for GcsServerAddress";
      freeReplyObject(reply);
      return false;
    }
    std::string result(reply->str);
    freeReplyObject(reply);

    RAY_CHECK(!result.empty()) << "Gcs service address is empty";
    size_t pos = result.find(':');
    RAY_CHECK(pos != std::string::npos)
        << "Gcs service address format is erroneous: " << result;
    address->first = result.substr(0, pos);
    address->second = std::stoi(result.substr(pos + 1));
    return true;
  }
  return false;
}

void ServiceBasedGcsClient::PeriodicallyCheckGcsServerAddress() {
  std::pair<std::string, int> address;
  if (get_server_address_func_(&address)) {
    if (address != current_gcs_server_address_) {
      // If GCS server address has changed, invoke the `GcsServiceFailureDetected`
      // callback.
      current_gcs_server_address_ = address;
      GcsServiceFailureDetected(rpc::GcsServiceFailureType::GCS_SERVER_RESTART);
    }
  }
}

void ServiceBasedGcsClient::GcsServiceFailureDetected(rpc::GcsServiceFailureType type) {
  switch (type) {
  case rpc::GcsServiceFailureType::RPC_DISCONNECT:
    // If the GCS server address does not change, reconnect to GCS server.
    ReconnectGcsServer();
    break;
  case rpc::GcsServiceFailureType::GCS_SERVER_RESTART:
    // If GCS sever address has changed, reconnect to GCS server and redo
    // subscription.
    ReconnectGcsServer();
    // NOTE(ffbin): Currently we don't support the case where the pub-sub server restarts,
    // because we use the same Redis server for both GCS storage and pub-sub. So the
    // following flag is always false.
    resubscribe_func_(false);
    // Resend resource usage after reconnected, needed by resource view in GCS.
    node_resource_accessor_->AsyncReReportResourceUsage();
    break;
  default:
    RAY_LOG(FATAL) << "Unsupported failure type: " << type;
    break;
  }
}

void ServiceBasedGcsClient::ReconnectGcsServer() {
  std::pair<std::string, int> address;
  int index = 0;
  for (; index < RayConfig::instance().ping_gcs_rpc_server_max_retries(); ++index) {
    if (get_server_address_func_(&address)) {
      // After GCS is restarted, the gcs client will reestablish the connection. At
      // present, every failed RPC request will trigger `ReconnectGcsServer`. In order to
      // avoid repeated connections in a short period of time, we add a protection
      // mechanism: if the address does not change (meaning gcs server doesn't restart),
      // the connection can be made at most once in
      // `minimum_gcs_reconnect_interval_milliseconds` milliseconds.
      if (last_reconnect_address_ == address &&
          (current_sys_time_ms() - last_reconnect_timestamp_ms_) <
              RayConfig::instance().minimum_gcs_reconnect_interval_milliseconds()) {
        RAY_LOG(DEBUG)
            << "Repeated reconnection in "
            << RayConfig::instance().minimum_gcs_reconnect_interval_milliseconds()
            << " milliseconds, return directly.";
        return;
      }

      RAY_LOG(DEBUG) << "Attemptting to reconnect to GCS server: " << address.first << ":"
                     << address.second;
      if (Ping(address.first, address.second, 100)) {
        // If `last_reconnect_address_` port is -1, it means that this is the first
        // connection and no log will be printed.
        if (last_reconnect_address_.second != -1) {
          RAY_LOG(INFO) << "Reconnected to GCS server: " << address.first << ":"
                        << address.second;
        }
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(
        RayConfig::instance().ping_gcs_rpc_server_interval_milliseconds()));
  }

  if (index < RayConfig::instance().ping_gcs_rpc_server_max_retries()) {
    gcs_rpc_client_->Reset(address.first, address.second, *client_call_manager_);
    last_reconnect_address_ = address;
    last_reconnect_timestamp_ms_ = current_sys_time_ms();
  } else {
    RAY_LOG(FATAL) << "Couldn't reconnect to GCS server. The last attempted GCS "
                      "server address was "
                   << address.first << ":" << address.second;
  }
}

}  // namespace gcs
}  // namespace ray
