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

#include "ray/gcs/gcs_server/node_spec.h"

#include <sstream>
#include "ray/common/task/scheduling_resources_util.h"

namespace ray {
namespace gcs {
NodeSpec::NodeSpec(const rpc::GcsNodeInfo &node_info) : pod_name_(node_info.pod_name()) {
  rpc::NodeShape node_shape_proto;
  if (node_info.shape_group().empty()) {
    node_shape_proto.set_shape_group(
        ray::ToResourceShapeStr(node_info.resources_total()));
  } else {
    node_shape_proto.set_shape_group(node_info.shape_group());
  }
  node_shape_proto.mutable_resource_shape()->insert(node_info.resources_total().begin(),
                                                    node_info.resources_total().end());
  node_shape_ = NodeShape(std::move(node_shape_proto));
}

NodeSpec::NodeSpec(const rpc::NodeSpec &node_spec)
    : pod_name_(node_spec.pod_name()), node_shape_(node_spec.node_shape()) {}

const NodeShape &NodeSpec::GetNodeShape() const { return node_shape_; }

const std::string &NodeSpec::GetPodName() const { return pod_name_; }

std::shared_ptr<rpc::NodeSpec> NodeSpec::ToProto() const {
  auto proto = std::make_shared<rpc::NodeSpec>();
  proto->set_pod_name(pod_name_);
  proto->mutable_node_shape()->CopyFrom(node_shape_.GetProto());
  return proto;
}

std::string NodeSpec::ToString() const {
  std::ostringstream ostr;
  ostr << "{pod_name: " << pod_name_ << ", node_shape: " << node_shape_ << "}";
  return ostr.str();
}

}  // namespace gcs
}  // namespace ray