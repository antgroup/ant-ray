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

#include "ray/gcs/gcs_server/gcs_placement_group_scheduler.h"

#include "ray/event/event.h"
#include "ray/gcs/gcs_server/gcs_placement_group_manager.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

GcsPlacementGroupScheduler::GcsPlacementGroupScheduler(
    instrumented_io_context &io_context,
    std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
    const gcs::GcsNodeManager &gcs_node_manager, GcsResourceManager &gcs_resource_manager,
    std::shared_ptr<rpc::NodeManagerClientPool> raylet_client_pool,
    std::function<bool(const PlacementGroupID &)> is_placement_group_lifetime_done)
    : return_timer_(io_context),
      gcs_table_storage_(std::move(gcs_table_storage)),
      gcs_node_manager_(gcs_node_manager),
      gcs_resource_manager_(gcs_resource_manager),
      raylet_client_pool_(raylet_client_pool),
      is_placement_group_lifetime_done_(is_placement_group_lifetime_done) {
  scheduler_strategies_.push_back(std::make_shared<GcsPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsSpreadStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictPackStrategy>());
  scheduler_strategies_.push_back(std::make_shared<GcsStrictSpreadStrategy>());
}

bool GcsScheduleStrategy::IsAvailableResourceSufficient(
    const ResourceSet &available_resources, const ResourceSet &allocated_resources,
    const ResourceSet &to_allocate_resources) const {
  const auto &to_allocate_resource_capacity =
      to_allocate_resources.GetResourceAmountMap();
  for (const auto &resource_pair : to_allocate_resource_capacity) {
    const auto &resource_name = resource_pair.first;
    const auto &lhs_quantity = resource_pair.second;
    const auto &rhs_quantity = available_resources.GetResource(resource_name) -
                               allocated_resources.GetResource(resource_name);
    if (lhs_quantity > rhs_quantity) {
      // lhs capacity exceeds rhs capacity.
      return false;
    }
  }
  return true;
}

ScheduleMap GcsStrictPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // Aggregate required resources.
  ResourceSet required_resources;
  for (const auto &bundle : bundles) {
    required_resources.AddResources(bundle->GetRequiredResources());
  }

  // Filter candidate nodes.
  std::vector<std::pair<int64_t, NodeID>> candidate_nodes;
  for (auto &node : context->cluster_resources_) {
    if (required_resources.IsSubset(node.second->GetAvailableResources())) {
      candidate_nodes.emplace_back((*context->node_to_bundles_)[node.first], node.first);
    }
  }

  // Select the node with the least number of bundles.
  ScheduleMap schedule_map;
  if (candidate_nodes.empty()) {
    return schedule_map;
  }

  std::sort(
      std::begin(candidate_nodes), std::end(candidate_nodes),
      [](const std::pair<int64_t, NodeID> &left,
         const std::pair<int64_t, NodeID> &right) { return left.first < right.first; });

  for (auto &bundle : bundles) {
    schedule_map[bundle->BundleId()] = candidate_nodes.front().second;
  }
  return schedule_map;
}

ScheduleMap GcsPackStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // The current algorithm is to select a node and deploy as many bundles as possible.
  // First fill up a node. If the node resource is insufficient, select a new node.
  // TODO(ffbin): We will speed this up in next PR. Currently it is a double for loop.
  ScheduleMap schedule_map;
  absl::flat_hash_map<NodeID, ResourceSet> allocated_resources;
  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    for (const auto &node : context->cluster_resources_) {
      if (IsAvailableResourceSufficient(node.second->GetAvailableResources(),
                                        allocated_resources[node.first],
                                        required_resources)) {
        schedule_map[bundle->BundleId()] = node.first;
        allocated_resources[node.first].AddResources(required_resources);
        break;
      }
    }
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

ScheduleMap GcsSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // When selecting nodes, if you traverse from the beginning each time, a large number of
  // bundles will be deployed to the previous nodes. So we start with the next node of the
  // last selected node.
  ScheduleMap schedule_map;
  const auto &candidate_nodes = context->cluster_resources_;
  if (candidate_nodes.empty()) {
    return schedule_map;
  }

  absl::flat_hash_map<NodeID, ResourceSet> allocated_resources;
  auto iter = candidate_nodes.begin();
  auto iter_begin = iter;
  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    // Traverse all nodes from `iter_begin` to `candidate_nodes.end()` to find a node
    // that meets the resource requirements. `iter_begin` is the next node of the last
    // selected node.
    for (; iter != candidate_nodes.end(); ++iter) {
      if (IsAvailableResourceSufficient(iter->second->GetAvailableResources(),
                                        allocated_resources[iter->first],
                                        required_resources)) {
        schedule_map[bundle->BundleId()] = iter->first;
        allocated_resources[iter->first].AddResources(required_resources);
        break;
      }
    }

    // We've traversed all the nodes from `iter_begin` to `candidate_nodes.end()`, but we
    // haven't found one that meets the requirements.
    // If `iter_begin` is `candidate_nodes.begin()`, it means that all nodes are not
    // satisfied, we will return directly. Otherwise, we will traverse the nodes from
    // `candidate_nodes.begin()` to `iter_begin` to find the nodes that meet the
    // requirements.
    if (iter == candidate_nodes.end()) {
      if (iter_begin != candidate_nodes.begin()) {
        // Traverse all the nodes from `candidate_nodes.begin()` to `iter_begin`.
        for (iter = candidate_nodes.begin(); iter != iter_begin; ++iter) {
          if (IsAvailableResourceSufficient(iter->second->GetAvailableResources(),
                                            allocated_resources[iter->first],
                                            required_resources)) {
            schedule_map[bundle->BundleId()] = iter->first;
            allocated_resources[iter->first].AddResources(required_resources);
            break;
          }
        }
        if (iter == iter_begin) {
          // We have traversed all the nodes, so return directly.
          break;
        }
      } else {
        // We have traversed all the nodes, so return directly.
        break;
      }
    }
    // NOTE: If `iter == candidate_nodes.end()`, ++iter causes crash.
    iter_begin = ++iter;
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

ScheduleMap GcsStrictSpreadStrategy::Schedule(
    std::vector<std::shared_ptr<ray::BundleSpecification>> &bundles,
    const std::unique_ptr<ScheduleContext> &context) {
  // TODO(ffbin): A bundle may require special resources, such as GPU. We need to
  // schedule bundles with special resource requirements first, which will be implemented
  // in the next pr.
  ScheduleMap schedule_map;
  const auto &candidate_nodes = context->cluster_resources_;

  // The number of bundles is more than the number of nodes, scheduling fails.
  if (bundles.size() > candidate_nodes.size()) {
    return schedule_map;
  }

  // Filter out the nodes already scheduled by this placement group.
  absl::flat_hash_map<NodeID, ResourceSet> allocated_resources;
  if (context->bundle_locations_.has_value()) {
    const auto &bundle_locations = context->bundle_locations_.value();
    for (auto &bundle : *bundle_locations) {
      allocated_resources[bundle.second.first] = ResourceSet();
    }
  }

  for (const auto &bundle : bundles) {
    const auto &required_resources = bundle->GetRequiredResources();
    auto iter = candidate_nodes.begin();
    for (; iter != candidate_nodes.end(); ++iter) {
      if (!allocated_resources.contains(iter->first) &&
          IsAvailableResourceSufficient(iter->second->GetAvailableResources(),
                                        allocated_resources[iter->first],
                                        required_resources)) {
        schedule_map[bundle->BundleId()] = iter->first;
        allocated_resources[iter->first].AddResources(required_resources);
        break;
      }
    }

    // Node resource is not satisfied, scheduling failed.
    if (iter == candidate_nodes.end()) {
      break;
    }
  }

  if (schedule_map.size() != bundles.size()) {
    schedule_map.clear();
  }
  return schedule_map;
}

void GcsPlacementGroupScheduler::CancelBundleResourcesFromRaylet(
    const rpc::Bundle &bundle, StatusCallback callback) {
  auto bundle_spec = std::make_shared<BundleSpecification>(bundle);
  RAY_LOG(DEBUG) << "Removing bundle resources from raylet, node_id="
                 << bundle_spec->NodeId();
  CancelResourceReserve(bundle_spec,
                        gcs_node_manager_.GetAliveNode(bundle_spec->NodeId()), callback);
  committed_bundle_location_index_.Erase(bundle_spec->PlacementGroupId(),
                                         bundle_spec->BundleId());
}

void GcsPlacementGroupScheduler::RemoveBundlesFromPlacementGroup(
    const std::shared_ptr<GcsPlacementGroup> placement_group) {
  // Get all removing bundles from this placement group.
  // Its state should be `invalid` and the node id is not null.
  const auto &removing_bundles = placement_group->GetAllRemovingBundles();
  for (const auto &bundle : removing_bundles) {
    RAY_LOG(DEBUG) << "Removing bundle: " << bundle->Index()
                   << " from node: " << bundle->NodeId();
    // Reuse the cancel resource reserve method and it can keep idempotence.
    CancelResourceReserve(bundle, gcs_node_manager_.GetAliveNode(bundle->NodeId()));
    committed_bundle_location_index_.Erase(placement_group->GetPlacementGroupID(),
                                           bundle->BundleId());
  }
}

void GcsPlacementGroupScheduler::ScheduleUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> failure_callback,
    std::function<void(std::shared_ptr<GcsPlacementGroup>)> success_callback) {
  // We need to ensure that the PrepareBundleResources won't be sent before the reply of
  // ReleaseUnusedBundles is returned.
  if (!nodes_of_releasing_unused_bundles_.empty()) {
    RAY_LOG(INFO) << "Failed to schedule placement group " << placement_group->GetName()
                  << ", id: " << placement_group->GetPlacementGroupID() << ", because "
                  << nodes_of_releasing_unused_bundles_.size()
                  << " nodes have not released unused bundles.";
    failure_callback(placement_group);
    return;
  }
  // Remove bundles firstly forever.
  // NOTE: This is an operation that does not need to be retried unless occurring
  // network error or node dead and it can always keep idempotence.
  RemoveBundlesFromPlacementGroup(placement_group);

  auto bundles = placement_group->GetUnplacedBundles();
  if (bundles.empty()) {
    RAY_LOG(INFO) << "The placement group: " << placement_group->GetPlacementGroupID()
                  << " will be only remove bundles.";
    success_callback(placement_group);
    return;
  }
  auto strategy = placement_group->GetStrategy();
  RAY_LOG(INFO) << "Scheduling placement group " << placement_group->GetName()
                << ", id: " << placement_group->GetPlacementGroupID()
                << ", bundles size: " << bundles.size()
                << ", strategy: " << rpc::PlacementStrategy_Name(strategy);

  committed_bundle_location_index_.AddNodes(gcs_node_manager_.GetAllAliveNodes());
  auto selected_nodes = SelectNodesForUnplacedBundles(placement_group, bundles);
  // If no nodes are available, scheduling fails.
  if (selected_nodes.empty()) {
    failure_callback(placement_group);
    return;
  }

  auto lease_status_tracker =
      std::make_shared<LeaseStatusTracker>(placement_group, bundles, selected_nodes);
  RAY_CHECK(placement_group_leasing_in_progress_
                .emplace(placement_group->GetPlacementGroupID(), lease_status_tracker)
                .second);

  // Prepare bundle resources from gcs resources manager.
  PrepareBundleResources(lease_status_tracker->GetBundleLocations());

  /// TODO(AlisaWu): Change the strategy when reserve resource failed.
  for (auto &bundle : bundles) {
    const auto &bundle_id = bundle->BundleId();
    const auto &node_id = selected_nodes[bundle_id];
    lease_status_tracker->MarkPreparePhaseStarted(node_id, bundle);
    // TODO(sang): The callback might not be called at all if nodes are dead. We should
    // handle this case properly.
    PrepareResources(bundle, gcs_node_manager_.GetAliveNode(node_id),
                     [this, bundle, node_id, lease_status_tracker, failure_callback,
                      success_callback](const Status &status) {
                       lease_status_tracker->MarkPrepareRequestReturned(node_id, bundle,
                                                                        status);
                       if (lease_status_tracker->AllPrepareRequestsReturned()) {
                         OnAllBundlePrepareRequestReturned(
                             lease_status_tracker, failure_callback, success_callback);
                       }
                     });
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupBundleResourcesIfExists(
    const PlacementGroupID &placement_group_id) {
  auto &bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  if (bundle_locations.has_value()) {
    // There could be leasing bundles and committed bundles at the same time if placement
    // groups are rescheduling, so we need to destroy prepared bundles and committed
    // bundles at the same time.
    DestroyPlacementGroupPreparedBundleResources(placement_group_id);
    DestroyPlacementGroupCommittedBundleResources(placement_group_id);

    // Return destroyed bundles resources to the cluster resource.
    ReturnBundleResources(bundle_locations.value());
  }
}

void GcsPlacementGroupScheduler::MarkScheduleCancelled(
    const PlacementGroupID &placement_group_id) {
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  if (it != placement_group_leasing_in_progress_.end()) {
    RAY_LOG(INFO) << "Mark the scheduling of placement group: " << placement_group_id
                  << " canceld.";
    it->second->MarkPlacementGroupScheduleCancelled();
  }
}

void GcsPlacementGroupScheduler::PrepareResources(
    const std::shared_ptr<BundleSpecification> &bundle,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node_opt,
    const StatusCallback &callback) {
  if (!node_opt.has_value()) {
    callback(Status::NotFound("Node is already dead."));
    return;
  }

  auto node = node_opt.value();
  const auto lease_client = GetLeaseClientFromNode(node);
  const auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  RAY_LOG(INFO) << "Preparing resource from node " << node_id
                << " for a bundle: " << bundle->DebugString();
  lease_client->PrepareBundleResources(
      *bundle, [node_id, bundle, callback](
                   const Status &status, const rpc::PrepareBundleResourcesReply &reply) {
        auto result = reply.success() ? Status::OK()
                                      : Status::IOError("Failed to reserve resource");
        if (result.ok()) {
          RAY_LOG(INFO) << "Finished leasing resource from " << node_id
                        << " for bundle: " << bundle->DebugString();
        } else {
          RAY_LOG(WARNING) << "Failed to lease resource from " << node_id
                           << " for bundle: " << bundle->DebugString();
        }
        callback(result);
      });
}

void GcsPlacementGroupScheduler::CommitResources(
    const std::shared_ptr<BundleSpecification> &bundle,
    const std::shared_ptr<ray::rpc::GcsNodeInfo> &node, const StatusCallback callback) {
  RAY_CHECK(node != nullptr);
  const auto lease_client = GetLeaseClientFromNode(node);
  const auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  RAY_LOG(INFO) << "Committing resource to a node " << node_id
                << " for a bundle: " << bundle->DebugString();
  lease_client->CommitBundleResources(
      *bundle, [bundle, node_id, callback](const Status &status,
                                           const rpc::CommitBundleResourcesReply &reply) {
        if (status.ok()) {
          RAY_LOG(INFO) << "Finished committing resource to " << node_id
                        << " for bundle: " << bundle->DebugString();
        } else {
          RAY_LOG(WARNING) << "Failed to commit resource to " << node_id
                           << " for bundle: " << bundle->DebugString()
                           << ", status: " << status.ToString();
        }
        RAY_CHECK(callback);
        callback(status);
      });
}

void GcsPlacementGroupScheduler::CancelResourceReserve(
    const std::shared_ptr<BundleSpecification> &bundle_spec,
    const absl::optional<std::shared_ptr<ray::rpc::GcsNodeInfo>> &node_opt,
    StatusCallback callback) {
  if (!node_opt.has_value()) {
    RAY_LOG(WARNING) << "Node for a placement group id "
                     << bundle_spec->PlacementGroupId() << " and a bundle index, "
                     << bundle_spec->Index()
                     << " has already removed. Cancellation request will be ignored.";
    return;
  }
  auto node = node_opt.value();
  auto node_id = NodeID::FromBinary(node->basic_gcs_node_info().node_id());
  RAY_LOG(INFO) << "Cancelling the resource reserved for bundle: "
                << bundle_spec->DebugString() << " at node " << node_id;
  const auto return_client = GetLeaseClientFromNode(node);
  return_client->CancelResourceReserve(
      *bundle_spec,
      [callback, bundle_spec, node_id](const Status &status,
                                       const rpc::CancelResourceReserveReply &reply) {
        RAY_LOG(DEBUG) << "Finished cancelling the resource reserved for bundle: "
                       << bundle_spec->DebugString() << " at node " << node_id;
        if (callback) {
          callback(status);
        }
      });
}

std::shared_ptr<ResourceReserveInterface>
GcsPlacementGroupScheduler::GetOrConnectLeaseClient(const rpc::Address &raylet_address) {
  return raylet_client_pool_->GetOrConnectByAddress(raylet_address);
}

std::shared_ptr<ResourceReserveInterface>
GcsPlacementGroupScheduler::GetLeaseClientFromNode(
    const std::shared_ptr<ray::rpc::GcsNodeInfo> &node) {
  rpc::Address remote_address;
  remote_address.set_raylet_id(node->basic_gcs_node_info().node_id());
  remote_address.set_ip_address(node->basic_gcs_node_info().node_manager_address());
  remote_address.set_port(node->basic_gcs_node_info().node_manager_port());
  return GetOrConnectLeaseClient(remote_address);
}

void GcsPlacementGroupScheduler::CommitAllBundles(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  const std::shared_ptr<BundleLocations> &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  lease_status_tracker->MarkCommitPhaseStarted();
  for (const auto &bundle_to_commit : *prepared_bundle_locations) {
    const auto &node_id = bundle_to_commit.second.first;
    const auto &node = gcs_node_manager_.GetAliveNode(node_id);
    const auto &bundle = bundle_to_commit.second.second;

    auto commit_resources_callback = [this, lease_status_tracker, bundle, node_id,
                                      schedule_failure_handler,
                                      schedule_success_handler](const Status &status) {
      lease_status_tracker->MarkCommitRequestReturned(node_id, bundle, status);
      if (lease_status_tracker->AllCommitRequestReturned()) {
        OnAllBundleCommitRequestReturned(lease_status_tracker, schedule_failure_handler,
                                         schedule_success_handler);
      }
    };

    if (node.has_value()) {
      CommitResources(bundle, node.value(), commit_resources_callback);
    } else {
      RAY_LOG(INFO) << "Failed to commit resources because the node is dead, node id = "
                    << node_id;
      commit_resources_callback(Status::Interrupted("Node is dead"));
    }
  }
}

void GcsPlacementGroupScheduler::OnAllBundlePrepareRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  RAY_CHECK(lease_status_tracker->AllPrepareRequestsReturned())
      << "This method can be called only after all bundle scheduling requests are "
         "returned.";
  const auto &placement_group = lease_status_tracker->GetPlacementGroup();
  const auto &bundles = lease_status_tracker->GetBundlesToSchedule();
  const auto &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  if (!lease_status_tracker->AllPrepareRequestsSuccessful()) {
    // Erase the status tracker from a in-memory map if exists.
    // NOTE: A placement group may be scheduled several times to succeed.
    // If a prepare failure occurs during scheduling, we just need to release the prepared
    // bundle resources of this scheduling.
    DestroyPlacementGroupPreparedBundleResources(placement_group_id);
    auto it = placement_group_leasing_in_progress_.find(placement_group_id);
    RAY_CHECK(it != placement_group_leasing_in_progress_.end());
    placement_group_leasing_in_progress_.erase(it);
    CommitBundleResources(lease_status_tracker->GetBundleLocations());
    ReturnBundleResources(lease_status_tracker->GetBundleLocations());
    schedule_failure_handler(placement_group);
    return;
  }

  // If the prepare requests succeed, update the bundle location.
  for (const auto &iter : *prepared_bundle_locations) {
    const auto &location = iter.second;
    placement_group->GetMutableBundle(location.second->Index())
        ->set_node_id(location.first.Binary());
  }

  // Store data to GCS.
  rpc::ScheduleData data;
  for (const auto &iter : bundles) {
    // TODO(ekl) this is a hack to get a string key for the proto
    auto key = iter->PlacementGroupId().Hex() + "_" + std::to_string(iter->Index());
    data.mutable_schedule_plan()->insert(
        {key, (*prepared_bundle_locations)[iter->BundleId()].first.Binary()});
  }
  RAY_CHECK_OK(gcs_table_storage_->PlacementGroupScheduleTable().Put(
      placement_group_id, data,
      [this, schedule_success_handler, schedule_failure_handler,
       lease_status_tracker](Status status) {
        CommitAllBundles(lease_status_tracker, schedule_failure_handler,
                         schedule_success_handler);
      }));
}

void GcsPlacementGroupScheduler::OnAllBundleCommitRequestReturned(
    const std::shared_ptr<LeaseStatusTracker> &lease_status_tracker,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_failure_handler,
    const std::function<void(std::shared_ptr<GcsPlacementGroup>)>
        &schedule_success_handler) {
  const auto &placement_group = lease_status_tracker->GetPlacementGroup();
  const auto &prepared_bundle_locations =
      lease_status_tracker->GetPreparedBundleLocations();
  const auto &placement_group_id = placement_group->GetPlacementGroupID();

  // Clean up the leasing progress map.
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  RAY_CHECK(it != placement_group_leasing_in_progress_.end());
  placement_group_leasing_in_progress_.erase(it);

  // Add a prepared bundle locations to committed bundle locations.
  committed_bundle_location_index_.AddBundleLocations(placement_group_id,
                                                      prepared_bundle_locations);

  // NOTE: If the placement group scheduling has been cancelled, we just need to destroy
  // the committed bundles. The reason is that only `RemovePlacementGroup` will mark the
  // state of placement group as `CANCELLED` and it will also destroy all prepared and
  // committed bundles of the placement group.
  // However, it cannot destroy the newly submitted bundles in this scheduling, so we need
  // to destroy them separately.
  if (lease_status_tracker->GetLeasingState() == LeasingState::CANCELLED) {
    DestroyPlacementGroupCommittedBundleResources(placement_group_id);
    CommitBundleResources(prepared_bundle_locations);
    ReturnBundleResources(prepared_bundle_locations);
    schedule_failure_handler(placement_group);
    return;
  }

  if (!lease_status_tracker->AllCommitRequestsSuccessful()) {
    // Update the state to be reschedule so that the failure handle will reschedule the
    // failed bundles.
    const auto &uncommitted_bundle_locations =
        lease_status_tracker->GetUnCommittedBundleLocations();
    for (const auto &bundle : *uncommitted_bundle_locations) {
      placement_group->GetMutableBundle(bundle.first.second)->clear_node_id();
      committed_bundle_location_index_.Erase(placement_group_id, bundle.first);
    }
    CommitBundleResources(uncommitted_bundle_locations);
    ReturnBundleResources(uncommitted_bundle_locations);
    placement_group->UpdateState(rpc::PlacementGroupTableData::RESCHEDULING);
    schedule_failure_handler(placement_group);
  } else {
    schedule_success_handler(placement_group);
  }

  CommitBundleResources(lease_status_tracker->GetCommittedBundleLocations());
}

std::unique_ptr<ScheduleContext> GcsPlacementGroupScheduler::GetScheduleContext(
    const PlacementGroupID &placement_group_id) {
  auto node_to_bundles = std::make_shared<absl::flat_hash_map<NodeID, int64_t>>();
  for (const auto &node_it : gcs_node_manager_.GetAllAliveNodes()) {
    const auto &node_id = node_it.first;
    const auto &bundle_locations_on_node =
        committed_bundle_location_index_.GetBundleLocationsOnNode(node_id);
    RAY_CHECK(bundle_locations_on_node)
        << "Bundle locations haven't been registered for node id " << node_id;
    const int bundles_size = bundle_locations_on_node.value()->size();
    node_to_bundles->emplace(node_id, bundles_size);
  }

  auto &bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  return std::unique_ptr<ScheduleContext>(
      new ScheduleContext(std::move(node_to_bundles), bundle_locations,
                          gcs_resource_manager_.GetClusterResources()));
}

absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>>
GcsPlacementGroupScheduler::GetBundlesOnNode(const NodeID &node_id) {
  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles_on_node;
  const auto &maybe_bundle_locations =
      committed_bundle_location_index_.GetBundleLocationsOnNode(node_id);
  if (maybe_bundle_locations.has_value()) {
    const auto &bundle_locations = maybe_bundle_locations.value();
    for (auto &bundle : *bundle_locations) {
      const auto &bundle_placement_group_id = bundle.first.first;
      const auto &bundle_index = bundle.first.second;
      bundles_on_node[bundle_placement_group_id].push_back(bundle_index);
    }
    committed_bundle_location_index_.Erase(node_id);
  }
  return bundles_on_node;
}

void GcsPlacementGroupScheduler::ReleaseUnusedBundles(
    const std::unordered_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles) {
  // The purpose of this function is to release bundles that may be leaked.
  // When GCS restarts, it doesn't know which bundles it has scheduled in the
  // previous lifecycle. In this case, GCS will send a list of bundle ids that
  // are still needed. And Raylet will release other bundles. If the node is
  // dead, there is no need to send the request of release unused bundles.
  std::ostringstream debug_info;
  debug_info
      << "Start to release unused bundles from node end, current created bundles view: ";
  for (const auto &node_bundle : node_to_bundles) {
    debug_info << "{" << node_bundle.first << ": [";
    for (const auto &bundle : node_bundle.second) {
      debug_info << bundle.bundle_id().bundle_index() << ",";
    }
    debug_info << "]}, ";
  }
  RAY_LOG(INFO) << debug_info.str();
  const auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  for (const auto &alive_node : alive_nodes) {
    const auto &node_id = alive_node.first;
    nodes_of_releasing_unused_bundles_.insert(node_id);

    auto lease_client = GetLeaseClientFromNode(alive_node.second);
    auto release_unused_bundles_callback =
        [this, node_id](const Status &status,
                        const rpc::ReleaseUnusedBundlesReply &reply) {
          nodes_of_releasing_unused_bundles_.erase(node_id);
        };
    auto iter = node_to_bundles.find(alive_node.first);

    // When GCS restarts, some nodes maybe do not have bundles.
    // In this case, GCS will send an empty list.
    auto bundles_in_use =
        iter != node_to_bundles.end() ? iter->second : std::vector<rpc::Bundle>{};
    lease_client->ReleaseUnusedBundles(bundles_in_use, release_unused_bundles_callback);
  }
}

void GcsPlacementGroupScheduler::Initialize(
    const std::unordered_map<PlacementGroupID,
                             std::vector<std::shared_ptr<BundleSpecification>>>
        &group_to_bundles) {
  // We need to reinitialize the `committed_bundle_location_index_`, otherwise,
  // it will get an empty bundle set when raylet fo occurred after GCS server restart.

  // Init the container that contains the map relation between node and bundle.
  auto &alive_nodes = gcs_node_manager_.GetAllAliveNodes();
  committed_bundle_location_index_.AddNodes(alive_nodes);

  for (const auto &group : group_to_bundles) {
    const auto &placement_group_id = group.first;
    std::shared_ptr<BundleLocations> committed_bundle_locations =
        std::make_shared<BundleLocations>();
    for (const auto &bundle : group.second) {
      if (!bundle->NodeId().IsNil()) {
        committed_bundle_locations->emplace(bundle->BundleId(),
                                            std::make_pair(bundle->NodeId(), bundle));
      }
    }
    committed_bundle_location_index_.AddBundleLocations(placement_group_id,
                                                        committed_bundle_locations);
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupPreparedBundleResources(
    const PlacementGroupID &placement_group_id) {
  // Get the locations of prepared bundles.
  auto it = placement_group_leasing_in_progress_.find(placement_group_id);
  if (it != placement_group_leasing_in_progress_.end()) {
    const auto &leasing_context = it->second;
    const auto &leasing_bundle_locations = leasing_context->GetPreparedBundleLocations();

    // Cancel all resource reservation of prepared bundles.
    bool is_lifetime_done = is_placement_group_lifetime_done_(placement_group_id);
    RAY_LOG(INFO) << "Cancelling all prepared bundles of a placement group, id is "
                  << placement_group_id << ", is lefetime done: " << is_lifetime_done;
    for (const auto &iter : *(leasing_bundle_locations)) {
      auto &bundle_spec = iter.second.second;
      auto &node_id = iter.second.first;
      if (is_lifetime_done) {
        bundle_spec->MarkAsInvalid();
      }
      CancelResourceReserve(bundle_spec, gcs_node_manager_.GetAliveNode(node_id));
    }
  }
}

void GcsPlacementGroupScheduler::DestroyPlacementGroupCommittedBundleResources(
    const PlacementGroupID &placement_group_id) {
  // Get the locations of committed bundles.
  const auto &maybe_bundle_locations =
      committed_bundle_location_index_.GetBundleLocations(placement_group_id);
  if (maybe_bundle_locations.has_value()) {
    const auto &committed_bundle_locations = maybe_bundle_locations.value();

    // Cancel all resource reservation of committed bundles.
    bool is_lifetime_done = is_placement_group_lifetime_done_(placement_group_id);
    RAY_LOG(INFO) << "Cancelling all committed bundles of a placement group, id is "
                  << placement_group_id << ", is lefetime done: " << is_lifetime_done;
    for (const auto &iter : *(committed_bundle_locations)) {
      auto &bundle_spec = iter.second.second;
      auto &node_id = iter.second.first;
      if (is_lifetime_done) {
        bundle_spec->MarkAsInvalid();
      }
      CancelResourceReserve(bundle_spec, gcs_node_manager_.GetAliveNode(node_id));
    }
    committed_bundle_location_index_.Erase(placement_group_id);
  }
}

void GcsPlacementGroupScheduler::PrepareBundleResources(
    const std::shared_ptr<BundleLocations> bundle_locations) {
  // Prepare resources from gcs resources manager.
  auto &cluster_resources = gcs_resource_manager_.GetMutableClusterResources();
  for (auto &bundle : *bundle_locations) {
    auto &node_resources = cluster_resources[bundle.second.first];
    if (node_resources->PrepareBundleResources(
            bundle.first.first, bundle.first.second,
            bundle.second.second->GetRequiredResources())) {
      if (RayConfig::instance().gcs_task_scheduling_enabled()) {
        gcs_resource_manager_.AddNodeTotalRequiredResources(
            bundle.second.first, bundle.second.second->GetRequiredResources());
      }
    }
  }
}

void GcsPlacementGroupScheduler::CommitBundleResources(
    const std::shared_ptr<BundleLocations> bundle_locations) {
  // Commit resources to gcs resources manager.
  auto &cluster_resources = gcs_resource_manager_.GetMutableClusterResources();
  for (auto &bundle : *bundle_locations) {
    auto iter = cluster_resources.find(bundle.second.first);
    if (iter == cluster_resources.end()) {
      RAY_LOG(INFO) << "Cann't commit the bundle " << bundle.second.second->Index()
                    << " to placement group: " << bundle.second.second->PlacementGroupId()
                    << " as the node " << bundle.second.first << " is already dead";
      continue;
    }
    auto &node_resources = *iter->second;
    node_resources.CommitBundleResources(bundle.first.first, bundle.first.second,
                                         bundle.second.second->GetRequiredResources());
  }
}

void GcsPlacementGroupScheduler::ReturnBundleResources(
    const std::shared_ptr<BundleLocations> bundle_locations) {
  // Release resources to gcs resources manager.
  auto &cluster_resources = gcs_resource_manager_.GetMutableClusterResources();
  for (auto &bundle : *bundle_locations) {
    auto iter = cluster_resources.find(bundle.second.first);
    if (iter != cluster_resources.end() &&
        IsBundleResourceReleasable(bundle.second.second, *iter->second)) {
      if (iter->second->ReturnBundleResources(bundle.first.first, bundle.first.second)) {
        if (RayConfig::instance().gcs_task_scheduling_enabled()) {
          gcs_resource_manager_.SubtractNodeTotalRequiredResources(
              bundle.second.first, bundle.second.second->GetRequiredResources());
        }
      }
    } else {
      RAY_LOG(INFO) << "Cann't release the bundle: " << bundle.second.second->Index()
                    << " from placement group: "
                    << bundle.second.second->PlacementGroupId()
                    << " tomporarily in scheduler, maybe few workers still occupy the pg "
                       "resources, will try it later.";
    }
  }
}

void GcsPlacementGroupScheduler::WarnPlacementGroupSchedulingFailure(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    const absl::flat_hash_map<NodeID, std::shared_ptr<SchedulingResources>>
        &cluster_resources) const {
  uint64_t current_time = current_time_ms();
  if (current_time - placement_group->last_log_reschedule_debug_info_time_ms_ >
      RayConfig::instance().placement_group_scheduling_failed_report_interval_ms()) {
    placement_group->last_log_reschedule_debug_info_time_ms_ = current_time;

    std::ostringstream stream;
    stream << "Failed to schedule placement group "
           << placement_group->GetPlacementGroupID()
           << ", strategy: " << PlacementStrategy_Name(placement_group->GetStrategy())
           << ", bundle size: " << placement_group->GetUnplacedBundles().size()
           << ", cluster node size: " << cluster_resources.size()
           << ". \nDetailed bundle info : {";

    for (const auto &bundle : placement_group->GetUnplacedBundles()) {
      stream << " \n{" << bundle->Index() << " : "
             << bundle->GetRequiredResources().ToString() << "}";
    }
    stream << " }.";

    RAY_EVENT(ERROR, EVENT_LABEL_PLACEMENT_GROUP_SCHEDULE_FAILED)
            .WithField("job_id", placement_group->GetCreatorJobId().Hex())
        << stream.str();

    stream << "\nCurrent realtime resource: { ";
    for (const auto &node : cluster_resources) {
      stream << " \n{" << node.first << " : " << node.second->DebugString() << "}";
    }
    stream << " }.";
    RAY_LOG(ERROR) << stream.str();
  }
}

ScheduleMap GcsPlacementGroupScheduler::SelectNodesForUnplacedBundles(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::vector<std::shared_ptr<BundleSpecification>> &bundles) {
  auto strategy = placement_group->GetStrategy();
  auto scheduler_context = GetScheduleContext(placement_group->GetPlacementGroupID());
  auto selected_nodes =
      scheduler_strategies_[strategy]->Schedule(bundles, scheduler_context);

  // If no nodes are available, scheduling fails.
  if (selected_nodes.empty()) {
    WarnPlacementGroupSchedulingFailure(placement_group,
                                        scheduler_context->cluster_resources_);
  }
  return selected_nodes;
}

const BundleLocationIndex &GcsPlacementGroupScheduler::GetCommittedBundleLocationIndex() {
  return committed_bundle_location_index_;
}

void BundleLocationIndex::AddBundleLocations(
    const PlacementGroupID &placement_group_id,
    std::shared_ptr<BundleLocations> bundle_locations) {
  // Update `placement_group_to_bundle_locations_`.
  // The placement group may be scheduled several times to succeed, so we need to merge
  // `bundle_locations` instead of covering it directly.
  auto iter = placement_group_to_bundle_locations_.find(placement_group_id);
  if (iter == placement_group_to_bundle_locations_.end()) {
    placement_group_to_bundle_locations_.emplace(placement_group_id, bundle_locations);
  } else {
    iter->second->insert(bundle_locations->begin(), bundle_locations->end());
  }

  // Update `node_to_leased_bundles_`.
  for (auto iter : *bundle_locations) {
    const auto &node_id = iter.second.first;
    if (!node_to_leased_bundles_.contains(node_id)) {
      node_to_leased_bundles_[node_id] = std::make_shared<BundleLocations>();
    }
    node_to_leased_bundles_[node_id]->emplace(iter.first, iter.second);
  }
}

bool BundleLocationIndex::Erase(const NodeID &node_id) {
  const auto leased_bundles_it = node_to_leased_bundles_.find(node_id);
  if (leased_bundles_it == node_to_leased_bundles_.end()) {
    return false;
  }

  const auto &bundle_locations = leased_bundles_it->second;
  for (const auto &bundle_location : *bundle_locations) {
    // Remove corresponding placement group id.
    const auto &bundle_id = bundle_location.first;
    const auto &bundle_spec = bundle_location.second.second;
    const auto placement_group_id = bundle_spec->PlacementGroupId();
    auto placement_group_it =
        placement_group_to_bundle_locations_.find(placement_group_id);
    if (placement_group_it != placement_group_to_bundle_locations_.end()) {
      auto &pg_bundle_locations = placement_group_it->second;
      auto pg_bundle_it = pg_bundle_locations->find(bundle_id);
      if (pg_bundle_it != pg_bundle_locations->end()) {
        pg_bundle_locations->erase(pg_bundle_it);
      }
    }
  }
  node_to_leased_bundles_.erase(leased_bundles_it);
  return true;
}

bool BundleLocationIndex::Erase(const PlacementGroupID &placement_group_id) {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return false;
  }

  const auto &bundle_locations = it->second;
  // Remove bundles from node_to_leased_bundles_ because bundles are removed now.
  for (const auto &bundle_location : *bundle_locations) {
    const auto &bundle_id = bundle_location.first;
    const auto &node_id = bundle_location.second.first;
    const auto leased_bundles_it = node_to_leased_bundles_.find(node_id);
    // node could've been already dead at this point.
    if (leased_bundles_it != node_to_leased_bundles_.end()) {
      leased_bundles_it->second->erase(bundle_id);
    }
  }
  placement_group_to_bundle_locations_.erase(it);

  return true;
}

void BundleLocationIndex::Erase(const PlacementGroupID &placement_group_id,
                                const BundleID &bundle_id) {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return;
  }

  NodeID node_id = NodeID::Nil();
  // Remove from `placement_group_to_bundle_locations_` if exist.
  auto &pg_bundle_locations = it->second;
  auto pg_bundle_it = pg_bundle_locations->find(bundle_id);
  if (pg_bundle_it != pg_bundle_locations->end()) {
    node_id = pg_bundle_it->second.first;
    pg_bundle_locations->erase(pg_bundle_it);
    if (pg_bundle_locations->empty()) {
      placement_group_to_bundle_locations_.erase(it);
    }
  }

  // Remove from `node_to_leased_bundles_` if exist.
  if (node_id == NodeID::Nil()) {
    return;
  }
  auto node_it = node_to_leased_bundles_.find(node_id);
  if (node_it == node_to_leased_bundles_.end()) {
    // Node dead.
    return;
  }

  auto &node_bundle_locations = node_it->second;
  auto node_bundle_it = node_bundle_locations->find(bundle_id);
  if (node_bundle_it != node_bundle_locations->end()) {
    node_bundle_locations->erase(node_bundle_it);
  }
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocations(
    const PlacementGroupID &placement_group_id) const {
  auto it = placement_group_to_bundle_locations_.find(placement_group_id);
  if (it == placement_group_to_bundle_locations_.end()) {
    return {};
  }
  return it->second;
}

const absl::optional<std::shared_ptr<BundleLocations> const>
BundleLocationIndex::GetBundleLocationsOnNode(const NodeID &node_id) {
  auto it = node_to_leased_bundles_.find(node_id);
  if (it == node_to_leased_bundles_.end()) {
    return {};
  }
  return it->second;
}

std::string BundleLocationIndex::DebugString() const {
  std::ostringstream ostr;
  ostr << "{ \"placment group to bundle locations\": [\n";
  for (const auto &iter : placement_group_to_bundle_locations_) {
    ostr << "{placement group id: " << iter.first << ", \n";
    ostr << "bundle locations:" << GetBundleLocationDebugString(*iter.second);
    ostr << "},\n";
  }
  ostr << "],\n \"node to bundles\": [\n";
  for (const auto &iter : node_to_leased_bundles_) {
    ostr << "{node id: " << iter.first << ", \n";
    ostr << "bundle locations:" << GetBundleLocationDebugString(*iter.second);
    ostr << "},\n";
  }
  ostr << "]}\n";
  return ostr.str();
}

std::string BundleLocationIndex::GetBundleLocationDebugString(
    const BundleLocations &bundle_locations) const {
  std::ostringstream ostr;
  ostr << "[";
  for (const auto &iter : bundle_locations) {
    ostr << "{pg_id:" << iter.first.first << ", bundle_index:" << iter.first.second
         << ", node_id:" << iter.second.first << "},\n";
  }
  ostr << "]";
  return ostr.str();
}

void BundleLocationIndex::AddNodes(
    const absl::flat_hash_map<NodeID, std::shared_ptr<rpc::GcsNodeInfo>> &nodes) {
  for (const auto &iter : nodes) {
    if (!node_to_leased_bundles_.contains(iter.first)) {
      node_to_leased_bundles_[iter.first] = std::make_shared<BundleLocations>();
    }
  }
}

LeaseStatusTracker::LeaseStatusTracker(
    std::shared_ptr<GcsPlacementGroup> placement_group,
    std::vector<std::shared_ptr<BundleSpecification>> &unplaced_bundles,
    ScheduleMap &schedule_map)
    : placement_group_(placement_group), bundles_to_schedule_(unplaced_bundles) {
  preparing_bundle_locations_ = std::make_shared<BundleLocations>();
  uncommitted_bundle_locations_ = std::make_shared<BundleLocations>();
  committed_bundle_locations_ = std::make_shared<BundleLocations>();
  bundle_locations_ = std::make_shared<BundleLocations>();
  for (const auto &bundle : unplaced_bundles) {
    (*bundle_locations_)[bundle->BundleId()] =
        std::make_pair(schedule_map[bundle->BundleId()], bundle);
  }
}

bool LeaseStatusTracker::MarkPreparePhaseStarted(
    const NodeID &node_id, std::shared_ptr<BundleSpecification> bundle) {
  const auto &bundle_id = bundle->BundleId();
  return node_to_bundles_when_preparing_[node_id].emplace(bundle_id).second;
}

void LeaseStatusTracker::MarkPrepareRequestReturned(
    const NodeID &node_id, const std::shared_ptr<BundleSpecification> bundle,
    const Status &status) {
  RAY_CHECK(prepare_request_returned_count_ <= bundles_to_schedule_.size());
  auto leasing_bundles = node_to_bundles_when_preparing_.find(node_id);
  RAY_CHECK(leasing_bundles != node_to_bundles_when_preparing_.end());
  auto bundle_iter = leasing_bundles->second.find(bundle->BundleId());
  RAY_CHECK(bundle_iter != leasing_bundles->second.end());

  // Remove the bundle from the leasing map as the reply is returned from the
  // remote node.
  leasing_bundles->second.erase(bundle_iter);
  if (leasing_bundles->second.empty()) {
    node_to_bundles_when_preparing_.erase(leasing_bundles);
  }

  // If the request succeeds, record it.
  const auto &bundle_id = bundle->BundleId();
  if (status.ok()) {
    preparing_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  }
  prepare_request_returned_count_ += 1;
}

bool LeaseStatusTracker::AllPrepareRequestsReturned() const {
  return prepare_request_returned_count_ == bundles_to_schedule_.size();
}

bool LeaseStatusTracker::AllPrepareRequestsSuccessful() const {
  return AllPrepareRequestsReturned() &&
         (preparing_bundle_locations_->size() == bundles_to_schedule_.size()) &&
         (leasing_state_ != LeasingState::CANCELLED);
}

void LeaseStatusTracker::MarkCommitRequestReturned(
    const NodeID &node_id, const std::shared_ptr<BundleSpecification> bundle,
    const Status &status) {
  commit_request_returned_count_ += 1;
  // If the request succeeds, record it.
  const auto &bundle_id = bundle->BundleId();
  if (!status.ok()) {
    uncommitted_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  } else {
    committed_bundle_locations_->emplace(bundle_id, std::make_pair(node_id, bundle));
  }
}

bool LeaseStatusTracker::AllCommitRequestReturned() const {
  return commit_request_returned_count_ == bundles_to_schedule_.size();
}

bool LeaseStatusTracker::AllCommitRequestsSuccessful() const {
  // We don't check cancel state here because we shouldn't destroy bundles when
  // commit requests failed. Cancel state should be treated separately.
  return AllCommitRequestReturned() &&
         preparing_bundle_locations_->size() == bundles_to_schedule_.size() &&
         uncommitted_bundle_locations_->empty();
}

const std::shared_ptr<GcsPlacementGroup> &LeaseStatusTracker::GetPlacementGroup() const {
  return placement_group_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetPreparedBundleLocations()
    const {
  return preparing_bundle_locations_;
}

const std::shared_ptr<BundleLocations>
    &LeaseStatusTracker::GetUnCommittedBundleLocations() const {
  return uncommitted_bundle_locations_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetCommittedBundleLocations()
    const {
  return committed_bundle_locations_;
}

const std::shared_ptr<BundleLocations> &LeaseStatusTracker::GetBundleLocations() const {
  return bundle_locations_;
}

const std::vector<std::shared_ptr<BundleSpecification>>
    &LeaseStatusTracker::GetBundlesToSchedule() const {
  return bundles_to_schedule_;
}

const LeasingState LeaseStatusTracker::GetLeasingState() const { return leasing_state_; }

void LeaseStatusTracker::MarkPlacementGroupScheduleCancelled() {
  UpdateLeasingState(LeasingState::CANCELLED);
}

bool LeaseStatusTracker::UpdateLeasingState(LeasingState leasing_state) {
  // If the lease was cancelled, we cannot update the state.
  if (leasing_state_ == LeasingState::CANCELLED) {
    return false;
  }
  leasing_state_ = leasing_state;
  return true;
}

void LeaseStatusTracker::MarkCommitPhaseStarted() {
  UpdateLeasingState(LeasingState::COMMITTING);
}

}  // namespace gcs
}  // namespace ray
