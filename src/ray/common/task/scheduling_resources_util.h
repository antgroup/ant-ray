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

#include "ray/common/constants.h"
#include "ray/common/task/task_spec.h"
#include "ray/util/resource_util.h"

#include <vector>

namespace ray {

const std::vector<std::string> RUNTIME_RESOURCE_LABELS({kMemory_ResourceLabel,
                                                        kCPU_ResourceLabel});

const ResourceSet RESOURCE_DELTA(RUNTIME_RESOURCE_LABELS,
                                 {ToMemoryUnits(1000ULL * 1204 * 1024 * 1024), 1000});

const std::vector<std::pair<std::string, std::string>> kNodeShapeLabels{
    {kCPU_ResourceLabel, "c"}, {kMEM_ResourceLabel, "g"}, {kGPU_ResourceLabel, "gpu"}};

/// If the task need a sole worker process.
inline bool NeedSoleWorkerProcess(
    rpc::Language language,
    const std::unordered_map<std::string, double> &required_resources) {
  if (language != rpc::Language::JAVA) {
    return true;
  }
  return !required_resources.empty();
}

inline bool NeedSoleWorkerProcess(const TaskSpecification &task_spec) {
  if (task_spec.GetLanguage() != rpc::Language::JAVA) {
    return true;
  }

  return !task_spec.GetRequiredPlacementResources().GetResourceAmountMap().empty();
}

inline bool ExtractMemoryResourceUnits(const ResourceSet &resources,
                                       uint64_t *memory_units) {
  const auto &resource_amout_map = resources.GetResourceAmountMap();
  for (auto &entry : resource_amout_map) {
    if (entry.first.find(kMemory_ResourceLabel) == 0) {
      *memory_units = static_cast<uint64_t>(entry.second.ToDouble());
      return true;
    }
  }
  return false;
}

inline bool ExtractMemoryResourceBytes(const ResourceSet &resources, uint64_t *value) {
  uint64_t memory_units;
  if (ExtractMemoryResourceUnits(resources, &memory_units)) {
    *value = FromMemoryUnitsToBytes(static_cast<double>(memory_units));
    return true;
  }

  return false;
}

inline ResourceSet GetConstraintResources(const ResourceSet &resources) {
  std::unordered_map<std::string, FractionalResourceQuantity> resource_mapping;
  const auto &resource_amout_map = resources.GetResourceAmountMap();
  for (auto &entry : resource_amout_map) {
    if (entry.first.find(kMemory_ResourceLabel) == 0) {
      // The placement group resources contains:
      // memory_group_{index}_{placement_group_id}  with bundle index
      // memory_group_{placement_group_id}  without bundle index
      // So it shold filter all entry which key with kMemory_ResourceLabel prefix.
      resource_mapping.emplace(entry);
    }
  }
  return ResourceSet(resource_mapping);
}

inline ResourceSet GetCPUResources(const ResourceSet &resources) {
  std::unordered_map<std::string, FractionalResourceQuantity> resource_mapping;
  const auto &resource_amout_map = resources.GetResourceAmountMap();
  for (auto &entry : resource_amout_map) {
    if (entry.first.find(kCPU_ResourceLabel) == 0) {
      resource_mapping.emplace(entry);
    }
  }
  return ResourceSet(resource_mapping);
}

inline std::string GetGPUResourceName(const ResourceSet &resources) {
  if (resources.Contains(kGPU_ResourceLabel)) {
    return kGPU_ResourceLabel;
  } else {
    for (const auto &resource : resources.GetResourceAmountMap()) {
      if (resource.first.find("nvidia.com/") == 0) {
        return resource.first;
      }
    }
  }
  return "";
}

inline bool IsRareResource(const std::string &resource_label) {
  return resource_label.find("nvidia.com/") == 0 ||
         resource_label == kBigMemory_ResourceLabel ||
         resource_label == kGPU_ResourceLabel || resource_label == kACPU_ResourceLabel;
}

template <class ResourceMap>
inline std::string ToResourceShapeStr(const ResourceMap &resource_map) {
  std::ostringstream ostr;
  for (auto &resource_label : kNodeShapeLabels) {
    auto iter = resource_map.find(resource_label.first);
    if (iter != resource_map.end()) {
      ostr << static_cast<uint64_t>(iter->second) << resource_label.second;
    }
  }
  return ostr.str();
}

inline bool ConvertBundleResourcesToOriginalResources(const ResourceSet &bundle_resources,
                                                      ResourceSet *original_resources,
                                                      BundleID *bundle_id) {
  std::string bundle_index_str;
  std::string placement_group_id_hex;
  std::unordered_map<std::string, FractionalResourceQuantity> resource_amount_map;
  for (auto &entry : bundle_resources.GetResourceAmountMap()) {
    std::string pattern = "_group_";
    auto pos = entry.first.find(pattern);
    if (pos == std::string::npos) {
      return false;
    }
    std::string origin_resource_label = entry.first.substr(0, pos);
    // NOTE: use emplace to avoid the wildcard_resource.
    resource_amount_map.emplace(origin_resource_label, entry.second);

    pos += pattern.size();
    pattern = "_";
    auto pos2 = entry.first.find(pattern, pos);
    if (pos2 != std::string::npos) {
      auto index_str = entry.first.substr(pos, pos2 - pos);
      if (bundle_index_str.empty()) {
        bundle_index_str = index_str;
      } else if (index_str != bundle_index_str) {
        return false;
      }
      pos = pos2 + pattern.size();
    }

    auto hex = entry.first.substr(pos);
    if (placement_group_id_hex.empty()) {
      placement_group_id_hex = hex;
    } else if (placement_group_id_hex != hex) {
      return false;
    }
  }

  if (original_resources != nullptr) {
    *original_resources = ResourceSet(resource_amount_map);
  }

  if (bundle_id != nullptr) {
    int bundle_index = -1;
    if (!bundle_index_str.empty()) {
      bundle_index = std::stoi(bundle_index_str);
    }
    auto placement_group_id = PlacementGroupID::FromHex(placement_group_id_hex);

    bundle_id->first = placement_group_id;
    bundle_id->second = bundle_index;
  }
  return true;
}

}  // namespace ray
