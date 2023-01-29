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

#include "ray/gcs/gcs_server/gcs_node_context_cache.h"

namespace ray {
namespace gcs {

const std::vector<NodeContext> &NodeContextCache::GetNodeContextList() {
  return node_context_list_;
}

void NodeContextCache::OnNodeTotalResourceChanged(
    const NodeID &node_id, std::shared_ptr<SchedulingResources> node_resources) {
  if (auto node_context = GetMutableNodeContext(node_id)) {
    // Update the field `contains_rare_resources_`.
    node_context->contains_rare_resources_ =
        node_resources->GetTotalResources().ContainsRareResources();
    node_context->scheduling_resources_ = std::move(node_resources);
  }
}

void NodeContextCache::OnNodeAdded(const NodeID &node_id,
                                   std::shared_ptr<SchedulingResources> node_resources,
                                   const std::string &node_name /* = ""*/) {
  NodeContext node_context;
  node_context.node_id_ = node_id;
  node_context.node_name_ = node_name;
  node_context.contains_rare_resources_ =
      node_resources->GetTotalResources().ContainsRareResources();
  node_context.scheduling_resources_ = std::move(node_resources);

  node_context_list_.emplace_back(std::move(node_context));
  node_context_index_.emplace(node_id, node_context_list_.size() - 1);
}

void NodeContextCache::OnNodeRemoved(const NodeID &node_id) {
  auto iter = node_context_index_.find(node_id);
  if (iter != node_context_index_.end()) {
    size_t index = iter->second;
    RAY_CHECK(index < node_context_list_.size());
    // Erase old index.
    node_context_index_.erase(iter);
    // Swap the removed node with the last one.
    size_t last_index = node_context_list_.size() - 1;
    if (index != last_index) {
      std::swap(node_context_list_[index], node_context_list_[last_index]);
      // Update new index.
      iter = node_context_index_.find(node_context_list_[index].node_id_);
      iter->second = index;
    }
    node_context_list_.resize(last_index);
  }
}

NodeContext *NodeContextCache::GetMutableNodeContext(const NodeID &node_id) {
  auto iter = node_context_index_.find(node_id);
  if (iter == node_context_index_.end()) {
    return nullptr;
  }
  RAY_CHECK(iter->second < node_context_list_.size());
  return &node_context_list_[iter->second];
}

boost::optional<const NodeContext &> NodeContextCache::GetNodeContext(
    const NodeID &node_id) {
  NodeContext *node_context = GetMutableNodeContext(node_id);
  if (node_context == nullptr) {
    return {};
  }
  return *node_context;
}

std::vector<NodeContext> NodeContextCache::GetContextsByNodeIDs(
    const absl::flat_hash_set<NodeID> &node_ids) {
  std::vector<NodeContext> node_context_list;
  node_context_list.reserve(node_ids.size());
  for (const auto &node_id : node_ids) {
    auto node_context_opt = GetNodeContext(node_id);
    if (node_context_opt) {
      node_context_list.emplace_back(*node_context_opt);
    }
  }
  return node_context_list;
}

absl::flat_hash_set<NodeID> GetNodeIDsByContexts(
    const std::vector<NodeContext> &node_context_list) {
  absl::flat_hash_set<NodeID> node_ids;
  node_ids.reserve(node_context_list.size());
  for (const auto &context : node_context_list) {
    node_ids.emplace(context.node_id_);
  }
  return node_ids;
}

}  // namespace gcs
}  // namespace ray
