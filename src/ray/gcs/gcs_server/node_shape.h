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

#include <map>
#include <string>
#include "ray/common/id.h"
#include "src/ray/protobuf/gcs.pb.h"

namespace ray {

namespace gcs {
class NodeShape {
 public:
  NodeShape();
  explicit NodeShape(rpc::NodeShape node_shape);

  bool IsAny() const;

  const rpc::NodeShape &GetProto() const { return node_shape_proto_; }

  const std::string &GetShapeGroup() const { return node_shape_proto_.shape_group(); }

  size_t Hash() const {
    // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
    // default value 0)
    if (!hash_) {
      hash_ = MurmurHash64A(node_shape_proto_.shape_group().data(),
                            node_shape_proto_.shape_group().size(), 0);
    }
    return hash_;
  }

  bool operator==(const NodeShape &rhs) const {
    return GetShapeGroup() == rhs.GetShapeGroup();
  }

  bool operator!=(const NodeShape &rhs) const { return !(*this == rhs); }

  static std::string ToString(const rpc::NodeShape &node_shape_proto);

  std::string ToString() const;

 private:
  rpc::NodeShape node_shape_proto_;
  std::string resource_shape_str_;
  mutable size_t hash_ = 0;
};

inline std::ostream &operator<<(std::ostream &os, const NodeShape &node_shape) {
  os << node_shape.ToString();
  return os;
}
}  // namespace gcs
}  // namespace ray

namespace std {

template <>
struct hash<ray::gcs::NodeShape> {
  size_t operator()(const ray::gcs::NodeShape &node_shape) const {
    return node_shape.Hash();
  }
};
template <>
struct hash<const ray::gcs::NodeShape> {
  size_t operator()(const ray::gcs::NodeShape &node_shape) const {
    return node_shape.Hash();
  }
};

}  // namespace std