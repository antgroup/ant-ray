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

#include "ray/gcs/gcs_server/gcs_label_manager.h"
#include "ray/gcs/gcs_server/gcs_actor_manager.h"

namespace ray {
namespace gcs {

using json = nlohmann::json;

const LabelToNodes &GcsLabelManager::GetLabelToNodes(
    const std::string &nodegroup_id, const std::string &ray_namespace) const {
  RAY_CHECK(ContainsNodegroupAndNamespace(nodegroup_id, ray_namespace))
      << "Gcs label manager don't exist the nodegroup_id " << nodegroup_id
      << ", namespace " << ray_namespace;
  return label_to_nodes_.at(nodegroup_id).at(ray_namespace);
}

LabelToNodes &GcsLabelManager::GetMutableLabelToNodes(const std::string &nodegroup_id,
                                                      const std::string &ray_namespace) {
  RAY_CHECK(ContainsNodegroupAndNamespace(nodegroup_id, ray_namespace))
      << "Gcs label manager don't exist the nodegroup_id " << nodegroup_id
      << ", namespace " << ray_namespace;
  ;
  return label_to_nodes_.at(nodegroup_id).at(ray_namespace);
}

bool GcsLabelManager::ContainsNodegroupAndNamespace(
    const std::string &nodegroup_id, const std::string &ray_namespace) const {
  if (!label_to_nodes_.contains(nodegroup_id)) {
    return false;
  }
  return label_to_nodes_.at(nodegroup_id).contains(ray_namespace);
}

void GcsLabelManager::EraseNodegroupAndNamespace(const std::string &nodegroup_id,
                                                 const std::string &ray_namespace) {
  label_to_nodes_.at(nodegroup_id).erase(ray_namespace);
  if (label_to_nodes_.at(nodegroup_id).empty()) {
    label_to_nodes_.erase(nodegroup_id);
  }
}

void GcsLabelManager::AddActorLabels(const std::shared_ptr<GcsActor> &actor) {
  const auto &nodegroup_id = actor->GetNodegroupId();
  const auto &ray_namespace = actor->GetRayNamespace();
  const auto &labels = actor->GetLabels();
  const auto &node_id = actor->GetNodeID();
  RAY_CHECK(!node_id.IsNil()) << "The GcsActor node id is nil.";
  node_to_actors_[node_id].emplace(actor);
  for (const auto &[key, value] : labels) {
    label_to_nodes_[nodegroup_id][ray_namespace][key][value].emplace(node_id);
  }
}

void GcsLabelManager::RemoveActorLabels(const std::shared_ptr<GcsActor> &actor) {
  const auto &nodegroup_id = actor->GetNodegroupId();
  const auto &ray_namespace = actor->GetRayNamespace();
  const auto &labels = actor->GetLabels();
  const auto &node_id = actor->GetNodeID();
  node_to_actors_[node_id].erase(actor);

  for (const auto &[key, value] : labels) {
    if (!ContainsNodegroupAndNamespace(nodegroup_id, ray_namespace)) {
      continue;
    }
    auto &label_to_nodes = GetMutableLabelToNodes(nodegroup_id, ray_namespace);
    if (!label_to_nodes.contains(key)) {
      continue;
    }
    if (!label_to_nodes.at(key).contains(value)) {
      continue;
    }
    if (label_to_nodes[key][value].contains(node_id) &&
        (!IsNodeHasSameLabel(node_id, key, value))) {
      label_to_nodes[key][value].erase(node_id);
      if (label_to_nodes[key][value].empty()) {
        label_to_nodes[key].erase(value);
        if (label_to_nodes[key].empty()) {
          label_to_nodes.erase(key);
          if (label_to_nodes.empty()) {
            EraseNodegroupAndNamespace(nodegroup_id, ray_namespace);
          }
        }
      }
    }
  }
}

bool GcsLabelManager::IsNodeHasSameLabel(const NodeID &node_id, const std::string &key,
                                         const std::string &value) {
  if (node_to_actors_.contains(node_id)) {
    for (const auto &actor : node_to_actors_.at(node_id)) {
      if (actor->GetLabels().contains(key)) {
        if (actor->GetLabels().at(key) == value) {
          return true;
        }
      }
    }
  }
  return false;
}

absl::flat_hash_set<NodeID> GcsLabelManager::GetNodesByKeyAndValue(
    const std::string &nodegroup_id, const std::string &ray_namespace,
    const std::string &key, const absl::flat_hash_set<std::string> &values) const {
  absl::flat_hash_set<NodeID> nodes;
  if (!ContainsNodegroupAndNamespace(nodegroup_id, ray_namespace)) {
    return nodes;
  }
  const auto &label_to_node = GetLabelToNodes(nodegroup_id, ray_namespace);
  if (!label_to_node.contains(key)) {
    return nodes;
  }
  for (const auto &value : values) {
    if (label_to_node.at(key).contains(value)) {
      for (const auto &node_id : label_to_node.at(key).at(value)) {
        nodes.insert(node_id);
      }
    }
  }
  return nodes;
}

absl::flat_hash_set<NodeID> GcsLabelManager::GetNodesByKey(
    const std::string &nodegroup_id, const std::string &ray_namespace,
    const std::string &key) const {
  absl::flat_hash_set<NodeID> nodes;
  if (!ContainsNodegroupAndNamespace(nodegroup_id, ray_namespace)) {
    return nodes;
  }
  const auto &label_to_node = GetLabelToNodes(nodegroup_id, ray_namespace);
  if (!label_to_node.contains(key)) {
    return nodes;
  }

  for (const auto &[value, node_actor_map] : label_to_node.at(key)) {
    for (const auto &node_id : node_actor_map) {
      nodes.insert(node_id);
    }
  }
  return nodes;
}

std::string GcsLabelManager::DebugString() const {
  json object;
  for (const auto &[nodegroupd_id, namespace_map] : label_to_nodes_) {
    for (const auto &[ray_namespace, label_to_nodes] : namespace_map) {
      object["label_to_nodes"][nodegroupd_id][ray_namespace] =
          GetLabelToNodeActorsJsonObject(label_to_nodes);
    }
  }
  object["node_to_actor_labels"] = GetNodeToActorLabelsJsonObject(node_to_actors_);
  return object.dump();
}

nlohmann::json GcsLabelManager::GetLabelToNodeActorsJsonObject(
    const LabelToNodes &label_to_nodes) const {
  json object;
  for (const auto &[key, value_map] : label_to_nodes) {
    for (const auto &[value, node_map] : value_map) {
      for (const auto &node_id : node_map) {
        object[key][value].push_back(node_id.Hex());
      }
    }
  }
  return object;
}

nlohmann::json GcsLabelManager::GetNodeToActorLabelsJsonObject(
    const absl::flat_hash_map<NodeID, absl::flat_hash_set<std::shared_ptr<GcsActor>>>
        &node_to_actors) const {
  json object;
  for (const auto &[node_id, actors] : node_to_actors) {
    for (const auto &actor : actors) {
      for (const auto &[key, value] : actor->GetLabels()) {
        object[node_id.Hex()][actor->GetActorID().Hex()][key] = value;
      }
    }
  }
  return object;
}
}  // namespace gcs
}  // namespace ray