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

#include "ray/gcs/gcs_server/gcs_virtual_cluster.h"

namespace ray {
namespace gcs {
///////////////////////// AbstractCluster /////////////////////////
void AbstractCluster::UpdateNodeInstances(ReplicaInstances replica_instances_to_add,
                                          ReplicaInstances replica_instances_to_remove) {
  // Insert node instances to the virtual cluster.
  InsertNodeInstances(std::move(replica_instances_to_add));
  // Remove node instances from the virtual cluster.
  RemoveNodeInstances(std::move(replica_instances_to_remove));
  // Update the revision of the cluster.
  revision_ = current_sys_time_ns();
}

void AbstractCluster::InsertNodeInstances(ReplicaInstances replica_instances) {
  for (auto &[template_id, job_node_instances] : replica_instances) {
    auto &template_node_instances = visible_node_instances_[template_id];
    auto &replicas = replica_sets_[template_id];
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto &job_node_instances = template_node_instances[job_cluster_id];
      for (auto &[id, node_instance] : job_node_instances) {
        job_node_instances[id] = std::move(node_instance);
        ++replicas;
      }
    }
  }
}

void AbstractCluster::RemoveNodeInstances(ReplicaInstances replica_instances) {
  for (auto &[template_id, job_node_instances] : replica_instances) {
    auto template_iter = visible_node_instances_.find(template_id);
    RAY_CHECK(template_iter != visible_node_instances_.end());

    auto replica_set_iter = replica_sets_.find(template_id);
    RAY_CHECK(replica_set_iter != replica_sets_.end());

    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      auto job_cluster_iter = template_iter->second.find(job_cluster_id);
      RAY_CHECK(job_cluster_iter != template_iter->second.end());

      for (auto &[id, _] : node_instances) {
        RAY_CHECK(job_cluster_iter->second.erase(id) == 1);
        RAY_CHECK(--(replica_set_iter->second) >= 0);
      }

      if (job_cluster_iter->second.empty()) {
        template_iter->second.erase(job_cluster_iter);
      }
    }

    if (template_iter->second.empty()) {
      visible_node_instances_.erase(template_iter);
    }

    if (replica_set_iter->second == 0) {
      replica_sets_.erase(replica_set_iter);
    }
  }
}

Status AbstractCluster::LookupIdleNodeInstances(
    const ReplicaSets &replica_sets, ReplicaInstances &replica_instances) const {
  bool success = true;
  for (const auto &[template_id, replicas] : replica_sets) {
    auto &template_node_instances = replica_instances[template_id];
    if (replicas <= 0) {
      continue;
    }

    auto iter = visible_node_instances_.find(template_id);
    if (iter == visible_node_instances_.end()) {
      success = false;
      continue;
    }

    static const std::string kEmptyJobClusterId;
    auto empty_iter = iter->second.find(kEmptyJobClusterId);
    if (empty_iter == iter->second.end()) {
      success = false;
      continue;
    }

    auto &job_node_instances = template_node_instances[kEmptyJobClusterId];
    for (const auto &[id, node_instance] : empty_iter->second) {
      if (!IsIdleNodeInstance(kEmptyJobClusterId, *node_instance)) {
        continue;
      }
      job_node_instances.emplace(id, node_instance);
      if (job_node_instances.size() == static_cast<size_t>(replicas)) {
        break;
      }
    }
    if (job_node_instances.size() < static_cast<size_t>(replicas)) {
      success = false;
    }
  }

  if (!success) {
    // TODO(Shanly): Give a more detailed error message about the demand replica set and
    // the idle replica instances.
    return Status::OutOfResource("No enough node instances to assign.");
  }

  return Status::OK();
}

bool AbstractCluster::MarkNodeInstanceAsDead(const std::string &template_id,
                                             const std::string &node_instance_id) {
  auto iter = visible_node_instances_.find(template_id);
  if (iter == visible_node_instances_.end()) {
    return false;
  }

  for (auto &[job_cluster_id, node_instances] : iter->second) {
    auto iter = node_instances.find(node_instance_id);
    if (iter != node_instances.end()) {
      iter->second->set_is_dead(true);
      return true;
    }
  }

  return false;
}

std::shared_ptr<rpc::VirtualClusterTableData> AbstractCluster::ToProto() const {
  auto data = std::make_shared<rpc::VirtualClusterTableData>();
  data->set_id(GetID());
  data->set_name(GetName());
  data->set_mode(GetMode());
  data->set_revision(GetRevision());
  data->mutable_replica_sets()->insert(replica_sets_.begin(), replica_sets_.end());
  for (auto &[template_id, job_node_instances] : visible_node_instances_) {
    for (auto &[job_cluster_id, node_instances] : job_node_instances) {
      for (auto &[id, node_instance] : node_instances) {
        (*data->mutable_node_instances())[id] = std::move(*node_instance->ToProto());
      }
    }
  }
  return data;
}

///////////////////////// JobClusterManager /////////////////////////
Status JobClusterManager::CreateJobCluster(
    const std::string &job_cluster_id,
    const std::string &cluster_id,
    ReplicaSets replica_sets,
    CreateOrUpdateVirtualClusterCallback callback) {
  auto iter = job_clusters_.find(job_cluster_id);
  if (iter != job_clusters_.end()) {
    return Status::InvalidArgument("The job cluster already exists.");
  }

  ReplicaInstances replica_instances_to_add;
  // Lookup idle node instances from main cluster based on `replica_sets_to_add`.
  auto status = LookupIdleNodeInstances(replica_sets, replica_instances_to_add);
  if (!status.ok()) {
    // TODO(Shanly): Give a more detailed error message about the demand replica set and
    // the idle replica instances.
    return Status::OutOfResource("No enough node instances to create the job cluster.");
  }

  RemoveNodeInstances(replica_instances_to_add);
  static const std::string kEmptyJobClusterId;
  for (auto &[template_id, job_node_instances] : replica_instances_to_add) {
    job_node_instances[job_cluster_id] =
        std::move(job_node_instances[kEmptyJobClusterId]);
    job_node_instances.erase(kEmptyJobClusterId);
  }
  InsertNodeInstances(replica_instances_to_add);

  // Create a job cluster.
  auto job_cluster = std::make_shared<JobCluster>(job_cluster_id, cluster_id);
  job_cluster->UpdateNodeInstances(std::move(replica_instances_to_add),
                                   ReplicaInstances());
  RAY_CHECK(job_clusters_.emplace(job_cluster_id, job_cluster).second);

  // Flush and publish the job cluster data.
  return async_data_flusher_(job_cluster->ToProto(), std::move(callback));
}

///////////////////////// PrimaryCluster /////////////////////////
std::shared_ptr<VirtualCluster> PrimaryCluster::GetVirtualCluster(
    const std::string &virtual_cluster_id) const {
  auto iter = virtual_clusters_.find(virtual_cluster_id);
  return iter != virtual_clusters_.end() ? iter->second : nullptr;
}

Status PrimaryCluster::CreateOrUpdateVirtualCluster(
    rpc::CreateOrUpdateVirtualClusterRequest request,
    CreateOrUpdateVirtualClusterCallback callback) {
  // Calculate the node instances that to be added and to be removed.
  ReplicaInstances replica_instances_to_add_to_virtual_cluster;
  ReplicaInstances replica_instances_to_remove_from_virtual_cluster;
  auto status = DetermineNodeInstanceAdditionsAndRemovals(
      request,
      replica_instances_to_add_to_virtual_cluster,
      replica_instances_to_remove_from_virtual_cluster);
  if (!status.ok()) {
    return status;
  }

  auto virtual_cluster = GetVirtualCluster(request.virtual_cluster_id());
  if (virtual_cluster == nullptr) {
    // replica_instances_to_remove must be empty as the virtual cluster is a new one.
    RAY_CHECK(replica_instances_to_remove_from_virtual_cluster.empty());
    virtual_cluster = std::make_shared<VirtualCluster>(async_data_flusher_,
                                                       request.virtual_cluster_id(),
                                                       request.virtual_cluster_name(),
                                                       request.mode());
    virtual_clusters_[request.virtual_cluster_id()] = virtual_cluster;
  }

  // Update the main cluster replica sets and node instances.
  // NOTE: The main cluster unnecessary to flush and pub data to other nodes.
  auto replica_instances_to_add_to_primary_cluster =
      replica_instances_to_remove_from_virtual_cluster;
  auto replica_instances_to_remove_from_primary_cluster =
      replica_instances_to_add_to_virtual_cluster;
  UpdateNodeInstances(std::move(replica_instances_to_add_to_primary_cluster),
                      std::move(replica_instances_to_remove_from_primary_cluster));

  // Update the virtual cluster replica sets and node instances.
  virtual_cluster->UpdateNodeInstances(
      std::move(replica_instances_to_add_to_virtual_cluster),
      std::move(replica_instances_to_remove_from_virtual_cluster));
  return async_data_flusher_(virtual_cluster->ToProto(), std::move(callback));
}

Status PrimaryCluster::DetermineNodeInstanceAdditionsAndRemovals(
    const rpc::CreateOrUpdateVirtualClusterRequest &request,
    ReplicaInstances &replica_instances_to_add,
    ReplicaInstances &replica_instances_to_remove) {
  replica_instances_to_add.clear();
  replica_instances_to_remove.clear();

  const auto &virtual_cluster_id = request.virtual_cluster_id();
  auto virtual_cluster = GetVirtualCluster(virtual_cluster_id);
  if (virtual_cluster != nullptr) {
    auto replica_sets_to_remove =
        ReplicasDifference(virtual_cluster->GetReplicaSets(), request.replica_sets());
    // Lookup idle node instances from the virtual cluster based on
    // `replica_sets_to_remove`.
    auto status = virtual_cluster->LookupIdleNodeInstances(replica_sets_to_remove,
                                                           replica_instances_to_remove);
    if (!status.ok()) {
      return status;
    }
  }

  auto replica_sets_to_add = ReplicasDifference(
      request.replica_sets(),
      virtual_cluster ? virtual_cluster->GetReplicaSets() : ReplicaSets());
  // Lookup idle node instances from main cluster based on `replica_sets_to_add`.
  return LookupIdleNodeInstances(replica_sets_to_add, replica_instances_to_add);
}

bool PrimaryCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                        const gcs::NodeInstance &node_instance) const {
  RAY_CHECK(GetMode() == rpc::WorkloadMode::Exclusive);
  return job_cluster_id.empty();
}

void PrimaryCluster::OnNodeAdded(const rpc::GcsNodeInfo &node) {
  const auto &template_id = node.template_id();
  const auto &node_instance_id = node.node_id();
  auto node_instance = std::make_shared<gcs::NodeInstance>();
  node_instance->set_template_id(template_id);
  node_instance->set_hostname(node.node_manager_hostname());
  node_instance->set_is_dead(false);

  visible_node_instances_[template_id][kEmptyJobClusterId].emplace(
      node_instance_id, std::move(node_instance));
}

void PrimaryCluster::OnNodeRemoved(const rpc::GcsNodeInfo &node) {
  const auto &template_id = node.template_id();
  const auto &node_instance_id = node.node_id();

  // TODO(Shanly): Build an index from node instance id to cluster id.
  if (MarkNodeInstanceAsDead(template_id, node_instance_id)) {
    return;
  }

  for (const auto &[virtual_cluster_id, virtual_cluster] : virtual_clusters_) {
    if (virtual_cluster->MarkNodeInstanceAsDead(template_id, node_instance_id)) {
      return;
    }
  }
}

///////////////////////// VirtualCluster /////////////////////////
bool VirtualCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                        const gcs::NodeInstance &node_instance) const {
  if (GetMode() == rpc::WorkloadMode::Exclusive) {
    return job_cluster_id.empty();
  }

  // TODO(Shanly): The job_cluster_id will always be empty in mixed mode although the node
  // instance is assigned to one or two jobs, so we need to check the node resources
  // usage.
  return false;
}

///////////////////////// JobCluster /////////////////////////
bool JobCluster::IsIdleNodeInstance(const std::string &job_cluster_id,
                                    const gcs::NodeInstance &node_instance) const {
  RAY_CHECK(GetMode() == rpc::WorkloadMode::Exclusive);
  return job_cluster_id.empty();
}

}  // namespace gcs
}  // namespace ray