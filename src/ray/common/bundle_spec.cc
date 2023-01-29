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

#include "ray/common/bundle_spec.h"
#include "ray/common/task/scheduling_resources_util.h"

namespace ray {

void BundleSpecification::ComputeResources() {
  auto unit_resource = MapFromProtobuf(message_->unit_resources());

  if (unit_resource.empty()) {
    // A static nil object is used here to avoid allocating the empty object every time.
    unit_resource_ = ResourceSet::Nil();
  } else {
    unit_resource_.reset(new ResourceSet(unit_resource));
  }

  // Generate placement group bundle labels.
  ComputeBundleResourceLabels();
}

void BundleSpecification::ComputeBundleResourceLabels() {
  RAY_CHECK(unit_resource_);

  for (const auto &resource_pair : unit_resource_->GetResourceMap()) {
    double resource_value = resource_pair.second;

    /// With bundle index (e.g., CPU_group_i_zzz).
    const std::string &resource_label =
        FormatPlacementGroupResource(resource_pair.first, PlacementGroupId(), Index());
    bundle_resource_labels_[resource_label] = resource_value;

    /// Without bundle index (e.g., CPU_group_zzz).
    const std::string &wildcard_label =
        FormatPlacementGroupResource(resource_pair.first, PlacementGroupId(), -1);
    bundle_resource_labels_[wildcard_label] = resource_value;
  }
  auto bundle_label =
      FormatPlacementGroupResource(kBundle_ResourceLabel, PlacementGroupId(), -1);
  auto index_bundle_label =
      FormatPlacementGroupResource(kBundle_ResourceLabel, PlacementGroupId(), Index());
  bundle_resource_labels_[index_bundle_label] = bundle_resource_labels_[bundle_label] =
      1000;
}

const ResourceSet &BundleSpecification::GetRequiredResources() const {
  return *unit_resource_;
}

BundleID BundleSpecification::BundleId() const {
  if (message_->bundle_id()
          .placement_group_id()
          .empty() /* e.g., empty proto default */) {
    int64_t index = message_->bundle_id().bundle_index();
    return std::make_pair(PlacementGroupID::Nil(), index);
  }
  int64_t index = message_->bundle_id().bundle_index();
  return std::make_pair(
      PlacementGroupID::FromBinary(message_->bundle_id().placement_group_id()), index);
}

PlacementGroupID BundleSpecification::PlacementGroupId() const {
  return PlacementGroupID::FromBinary(message_->bundle_id().placement_group_id());
}

int64_t BundleSpecification::Index() const {
  return message_->bundle_id().bundle_index();
}

NodeID BundleSpecification::NodeId() const {
  return NodeID::FromBinary(message_->node_id());
}

void BundleSpecification::MarkAsInvalid() { message_->set_is_valid(false); }

bool BundleSpecification::IsValid() const { return message_->is_valid(); }

std::string BundleSpecification::DebugString() const {
  std::ostringstream stream;
  auto bundle_id = BundleId();
  stream << "placement group id={" << bundle_id.first << "}, bundle index={"
         << bundle_id.second << "}";
  return stream.str();
}

std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const PlacementGroupID &group_id,
                                         int64_t bundle_index) {
  std::string str;
  if (bundle_index >= 0) {
    str = original_resource_name + "_group_" + std::to_string(bundle_index) + "_" +
          group_id.Hex();
  } else {
    RAY_CHECK(bundle_index == -1) << "Invalid index " << bundle_index;
    str = original_resource_name + "_group_" + group_id.Hex();
  }
  RAY_CHECK(GetOriginalResourceName(str) == original_resource_name) << str;
  return str;
}

std::string FormatPlacementGroupResource(const std::string &original_resource_name,
                                         const BundleSpecification &bundle_spec) {
  return FormatPlacementGroupResource(
      original_resource_name, bundle_spec.PlacementGroupId(), bundle_spec.Index());
}

bool IsBundleIndex(const std::string &resource, const PlacementGroupID &group_id,
                   const int bundle_index) {
  return resource.find("_group_" + std::to_string(bundle_index) + "_" + group_id.Hex()) !=
         std::string::npos;
}

std::unordered_map<std::string, double> AddPlacementGroupConstraint(
    const std::unordered_map<std::string, double> &resources,
    const PlacementGroupID &placement_group_id, int64_t bundle_index) {
  std::unordered_map<std::string, double> new_resources;
  if (!placement_group_id.IsNil()) {
    RAY_CHECK((bundle_index == -1 || bundle_index >= 0))
        << "Invalid bundle index " << bundle_index;
    for (auto iter = resources.begin(); iter != resources.end(); iter++) {
      auto wildcard_name =
          FormatPlacementGroupResource(iter->first, placement_group_id, -1);
      new_resources[wildcard_name] = iter->second;
      if (bundle_index >= 0) {
        auto index_name =
            FormatPlacementGroupResource(iter->first, placement_group_id, bundle_index);
        new_resources[index_name] = iter->second;
      }
    }
    return new_resources;
  }
  return resources;
}

bool ParseBundleResource(const std::string &resource,
                         PlacementGroupID *placement_group_id, int64_t *index) {
  // cpu_group_0_id : true
  // cpu_group_id : false
  size_t group_index = resource.find("_group_");
  if (group_index == std::string::npos) {
    return false;
  }
  // cpu_group_0_id : true
  //    ^      ^
  //  g_index  g_index+7
  size_t index_start = group_index + 7;
  size_t index_end = resource.find('_', index_start);
  if (index_end == std::string::npos) {
    return false;
  }
  *index = std::stoll(resource.substr(index_start, index_end - index_start));

  size_t id_start = index_end + 1;
  *placement_group_id = PlacementGroupID::FromHex(resource.substr(id_start));
  return true;
}

std::string GetOriginalResourceName(const std::string &resource) {
  auto idx = resource.find("_group_");
  RAY_CHECK(idx >= 0) << "This isn't a placement group resource " << resource;
  return resource.substr(0, idx);
}

const std::unordered_map<int64_t, std::string> GetBundleConstraintResources(
    const std::vector<std::shared_ptr<BundleSpecification>> bundles) {
  std::unordered_map<int64_t, std::string> res;
  for (auto &bundle : bundles) {
    auto required_resources = ray::GetConstraintResources(bundle->GetRequiredResources());
    res.insert({bundle->Index(), required_resources.ToString()});
  }
  return res;
}

bool IsBundleResourceReleasable(const std::shared_ptr<BundleSpecification> &bundle,
                                const SchedulingResources &resources) {
  // Check whether exist the specificed bundle resources in the cluster resources.
  const auto &total_resources = resources.GetTotalResources();
  const auto &available_resources = resources.GetAvailableResources();

  auto bundle_index = bundle->Index();
  auto group_id = bundle->PlacementGroupId();

  std::string suffix = "_group_" + std::to_string(bundle_index) + "_" + group_id.Hex();
  for (const auto &entry : total_resources.GetResourceAmountMap()) {
    if (EndsWith(entry.first, suffix)) {
      if (available_resources.GetResource(entry.first) < entry.second) {
        RAY_LOG(WARNING)
            << "The resource: " << entry.first
            << " in the currently available resources is less than the bundle "
               "total resource when removing bundle: "
            << bundle_index << " from placement group: " << group_id
            << ", maybe not all workers corresponding to this "
               "bundle release the resources";
        return false;
      }
    }
  }

  return true;
}

}  // namespace ray
