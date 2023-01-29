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

#pragma once

#include <memory>
#include <string>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "nlohmann/json.hpp"
#include "ray/common/id.h"

namespace ray {
namespace gcs {
class GcsActor;
// <label_key, <lable_value, [node_id]>>
using LabelToNodes =
    absl::flat_hash_map<std::string,
                        absl::flat_hash_map<std::string, absl::flat_hash_set<NodeID>>>;

class GcsLabelManager {
 public:
  absl::flat_hash_set<NodeID> GetNodesByKeyAndValue(
      const std::string &nodegroup_id, const std::string &ray_namespace,
      const std::string &key, const absl::flat_hash_set<std::string> &values) const;

  absl::flat_hash_set<NodeID> GetNodesByKey(const std::string &nodegroup_id,
                                            const std::string &ray_namespace,
                                            const std::string &key) const;

  void AddActorLabels(const std::shared_ptr<GcsActor> &actor);

  void RemoveActorLabels(const std::shared_ptr<GcsActor> &actor);

  std::string DebugString() const;

 private:
  const LabelToNodes &GetLabelToNodes(const std::string &nodegroup_id,
                                      const std::string &ray_namespace) const;

  LabelToNodes &GetMutableLabelToNodes(const std::string &nodegroup_id,
                                       const std::string &ray_namespace);

  bool ContainsNodegroupAndNamespace(const std::string &nodegroup_id,
                                     const std::string &ray_namespace) const;

  void EraseNodegroupAndNamespace(const std::string &nodegroup_id,
                                  const std::string &ray_namespace);

  bool IsNodeHasSameLabel(const NodeID &node_id, const std::string &key,
                          const std::string &value);

  nlohmann::json GetLabelToNodeActorsJsonObject(
      const LabelToNodes &label_to_node_actors) const;

  nlohmann::json GetNodeToActorLabelsJsonObject(
      const absl::flat_hash_map<NodeID, absl::flat_hash_set<std::shared_ptr<GcsActor>>>
          &node_to_actors) const;

  // <nodegroup_id, <namespace, <label_key, <lable_value, [node_id]>>>>
  absl::flat_hash_map<std::string, absl::flat_hash_map<std::string, LabelToNodes>>
      label_to_nodes_;
  // <node_id, [actor]>
  absl::flat_hash_map<NodeID, absl::flat_hash_set<std::shared_ptr<GcsActor>>>
      node_to_actors_;
};
}  // namespace gcs
}  // namespace ray