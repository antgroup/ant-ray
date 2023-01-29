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

#include <boost/optional.hpp>
#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "ray/common/id.h"
#include "ray/common/task/scheduling_resources.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

/// The structure used to store node resource data when searching for feasible nodes
/// during scheduling
struct NodeContext {
  NodeID node_id_;
  std::string node_name_;
  std::shared_ptr<SchedulingResources> scheduling_resources_;
  bool contains_rare_resources_ = false;
};

class GcsResourceManager;
class NodeContextCache {
 public:
  const std::vector<NodeContext> &GetNodeContextList();

  void OnNodeTotalResourceChanged(const NodeID &node_id,
                                  std::shared_ptr<SchedulingResources> node_resources);

  void OnNodeAdded(const NodeID &node_id,
                   std::shared_ptr<SchedulingResources> node_resources,
                   const std::string &node_name = "");

  void OnNodeRemoved(const NodeID &node_id);

  boost::optional<const NodeContext &> GetNodeContext(const NodeID &node_id);

  std::vector<NodeContext> GetContextsByNodeIDs(
      const absl::flat_hash_set<NodeID> &node_ids);

 private:
  NodeContext *GetMutableNodeContext(const NodeID &node_id);

 private:
  std::vector<NodeContext> node_context_list_;
  absl::flat_hash_map<NodeID, size_t> node_context_index_;
};

absl::flat_hash_set<NodeID> GetNodeIDsByContexts(
    const std::vector<NodeContext> &node_context_list);
}  // namespace gcs
}  // namespace ray
