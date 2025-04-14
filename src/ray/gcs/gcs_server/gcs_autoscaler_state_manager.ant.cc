// Copyright 2023 The Ray Authors.
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

#include "ray/gcs/gcs_server/gcs_autoscaler_state_manager.h"

#include "ray/gcs/gcs_server/gcs_actor_manager.h"
#include "ray/gcs/gcs_server/gcs_node_manager.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/pb_util.h"

namespace ray {
namespace gcs {

void GcsAutoscalerStateManager::HandleGetVirtualClusterResourceStates(
    rpc::autoscaler::GetVirtualClusterResourceStatesRequest request,
    rpc::autoscaler::GetVirtualClusterResourceStatesReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_CHECK(thread_checker_.IsOnSameThread());
  RAY_CHECK(request.last_seen_cluster_resource_state_version() <=
            last_cluster_resource_state_version_);

  RAY_LOG(INFO) << "Getting virtual cluster resource states";
  auto states = reply->mutable_virtual_cluster_resource_states()->mutable_states();
  auto primary_cluster = gcs_virtual_cluster_manager_->GetPrimaryCluster();
  primary_cluster->ForeachVirtualCluster(
      [states, this](const std::shared_ptr<VirtualCluster> &virtual_cluster) {
        rpc::autoscaler::VirtualClusterState state;
        auto parent_id =
            (VirtualClusterID::FromBinary(virtual_cluster->GetID())).ParentID();
        if (!parent_id.IsNil()) {
          state.set_parent_virtual_cluster_id(parent_id.Binary());
        }
        state.set_divisible(virtual_cluster->Divisible());
        state.set_revision(virtual_cluster->GetRevision());
        const auto &visible_node_instances = virtual_cluster->GetVisibleNodeInstances();
        if (virtual_cluster->Divisible()) {
          for (const auto &[template_id, job_node_instances] : visible_node_instances) {
            const auto unassigned_instances_iter =
                job_node_instances.find(kUndividedClusterId);
            if (unassigned_instances_iter != job_node_instances.end()) {
              for (const auto &[id, node_instance] : unassigned_instances_iter->second) {
                state.add_nodes(id);
              }
            }
          }
        } else {
          for (const auto &[template_id, job_node_instances] : visible_node_instances) {
            for (const auto &[job_cluster_id, node_instances] : job_node_instances) {
              for (const auto &[id, node_instance] : node_instances) {
                state.add_nodes(id);
              }
            }
          }
          GetVirtualClusterResources(&state);
        }
        (*states)[virtual_cluster->GetID()] = state;
      });

  rpc::autoscaler::VirtualClusterState primary_cluster_state;
  primary_cluster_state.set_divisible(true);
  primary_cluster_state.set_revision(primary_cluster->GetRevision());
  for (const auto &[template_id, job_node_instances] :
       primary_cluster->GetVisibleNodeInstances()) {
    const auto unassigned_instances_iter = job_node_instances.find(kUndividedClusterId);
    if (unassigned_instances_iter != job_node_instances.end()) {
      for (const auto &[id, node_instance] : unassigned_instances_iter->second) {
        primary_cluster_state.add_nodes(id);
      }
    }
  }
  (*states)[kPrimaryClusterID] = primary_cluster_state;

  reply->mutable_virtual_cluster_resource_states()
      ->set_last_seen_autoscaler_state_version(last_seen_autoscaler_state_version_);
  reply->mutable_virtual_cluster_resource_states()->set_cluster_resource_state_version(
      IncrementAndGetNextClusterResourceStateVersion());
  reply->mutable_virtual_cluster_resource_states()->set_cluster_session_name(
      session_name_);

  rpc::autoscaler::ClusterResourceState cluster_resource_state;
  GetNodeStates(&cluster_resource_state);
  GetClusterResourceConstraints(&cluster_resource_state);
  reply->mutable_virtual_cluster_resource_states()->mutable_node_states()->Assign(
      cluster_resource_state.node_states().begin(),
      cluster_resource_state.node_states().end());
  reply->mutable_virtual_cluster_resource_states()
      ->mutable_cluster_resource_constraints()
      ->CopyFrom(cluster_resource_state.cluster_resource_constraints());

  // We are not using GCS_RPC_SEND_REPLY like other GCS managers to avoid the client
  // having to parse the gcs status code embedded.
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsAutoscalerStateManager::GetVirtualClusterResources(
    rpc::autoscaler::VirtualClusterState *state) {
  absl::flat_hash_map<google::protobuf::Map<std::string, double>, rpc::ResourceDemand>
      aggregate_load;
  for (const auto &node : state->nodes()) {
    const auto node_id = NodeID::FromHex(node);
    auto const node_resource_iter = node_resource_info_.find(node_id);
    if (node_resource_iter != node_resource_info_.end()) {
      auto const &node_resource_data = node_resource_iter->second.second;
      gcs::FillAggregateLoad(node_resource_data, &aggregate_load);
    }
  }

  for (const auto &[shape, demand] : aggregate_load) {
    auto num_pending = demand.num_infeasible_requests_queued() + demand.backlog_size() +
                       demand.num_ready_requests_queued();
    if (num_pending > 0) {
      auto pending_req = state->add_pending_resource_requests();
      pending_req->set_count(num_pending);
      auto req = pending_req->mutable_request();
      req->mutable_resources_bundle()->insert(shape.begin(), shape.end());
    }
  }

  // TODO: add pending resource requests.
}

}  // namespace gcs
}  // namespace ray
