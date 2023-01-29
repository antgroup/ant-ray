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

#include "ray/gcs/gcs_server/node_shape.h"

#include "absl/container/flat_hash_set.h"
#include "ray/common/task/scheduling_resources_util.h"

namespace ray {

namespace gcs {

NodeShape::NodeShape() { node_shape_proto_.set_shape_group(kDefaultShapeGroup); }

NodeShape::NodeShape(rpc::NodeShape node_shape_proto) {
  if (node_shape_proto.shape_group().empty() &&
      node_shape_proto.resource_shape().empty()) {
    node_shape_proto_.set_shape_group(kDefaultShapeGroup);
    return;
  }

  absl::flat_hash_set<std::string> invalid_resource_labels;
  for (auto &entry : node_shape_proto.resource_shape()) {
    const auto &resource_label = entry.first;
    auto iter = std::find_if(
        kNodeShapeLabels.begin(), kNodeShapeLabels.end(),
        [&resource_label](const auto &item) { return resource_label == item.first; });
    if (iter == kNodeShapeLabels.end()) {
      invalid_resource_labels.emplace(resource_label);
    }
  }
  if (!invalid_resource_labels.empty()) {
    for (auto &resource_label : invalid_resource_labels) {
      node_shape_proto.mutable_resource_shape()->erase(resource_label);
    }
  }
  resource_shape_str_ = ray::ToResourceShapeStr(node_shape_proto.resource_shape());
  node_shape_proto_ = std::move(node_shape_proto);
}

bool NodeShape::IsAny() const {
  return node_shape_proto_.shape_group() == kDefaultShapeGroup;
}

std::string NodeShape::ToString(const rpc::NodeShape &node_shape_proto) {
  std::ostringstream ostr;
  ostr << "{shape_group: " << node_shape_proto.shape_group();
  ostr << ", resource_shape: "
       << ray::ToResourceShapeStr(node_shape_proto.resource_shape()) << "}";
  return ostr.str();
}

std::string NodeShape::ToString() const {
  std::ostringstream ostr;
  ostr << "{shape_group: " << node_shape_proto_.shape_group()
       << ", resource_shape: " << resource_shape_str_ << "}";
  return ostr.str();
}

}  // namespace gcs
}  // namespace ray
