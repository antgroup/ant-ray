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

#include <functional>
#include <string>

#include "ray/gcs/gcs_server/node_shape.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {
class NodeSpec {
 public:
  NodeSpec() = default;

  explicit NodeSpec(const rpc::GcsNodeInfo &node_info);

  explicit NodeSpec(const rpc::NodeSpec &node_spec);

  const NodeShape &GetNodeShape() const;

  const std::string &GetPodName() const;

  std::shared_ptr<rpc::NodeSpec> ToProto() const;

  bool operator==(const NodeSpec &rhs) const {
    return pod_name_ == rhs.pod_name_ && node_shape_ == rhs.node_shape_;
  }

  bool operator!=(const NodeSpec &rhs) const { return !(*this == rhs); }

  std::string ToString() const;

  std::string pod_name_;
  NodeShape node_shape_;
};

inline std::ostream &operator<<(std::ostream &os, const NodeSpec &node_spec) {
  os << node_spec.ToString();
  return os;
}

}  // namespace gcs
}  // namespace ray
