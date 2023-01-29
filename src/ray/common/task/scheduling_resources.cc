#include "ray/common/task/scheduling_resources.h"

#include <algorithm>
#include <cmath>
#include <sstream>

#include "absl/container/flat_hash_map.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/task/scheduling_resources_util.h"
#include "ray/util/logging.h"

namespace ray {

FractionalResourceQuantity::FractionalResourceQuantity() { resource_quantity_ = 0; }

FractionalResourceQuantity::FractionalResourceQuantity(double resource_quantity) {
  // We check for nonnegativeity due to the implicit conversion to
  // FractionalResourceQuantity from ints/doubles when we do logical
  // comparisons.
  RAY_CHECK(resource_quantity >= 0)
      << "Resource capacity, " << resource_quantity << ", should be nonnegative.";

  resource_quantity_ =
      static_cast<int64_t>(resource_quantity * kResourceConversionFactor);
}

const FractionalResourceQuantity FractionalResourceQuantity::operator+(
    const FractionalResourceQuantity &rhs) const {
  FractionalResourceQuantity result = *this;
  result += rhs;
  return result;
}

const FractionalResourceQuantity FractionalResourceQuantity::operator-(
    const FractionalResourceQuantity &rhs) const {
  FractionalResourceQuantity result = *this;
  result -= rhs;
  return result;
}

void FractionalResourceQuantity::operator+=(const FractionalResourceQuantity &rhs) {
  resource_quantity_ += rhs.resource_quantity_;
}

void FractionalResourceQuantity::operator-=(const FractionalResourceQuantity &rhs) {
  resource_quantity_ -= rhs.resource_quantity_;
}

bool FractionalResourceQuantity::operator==(const FractionalResourceQuantity &rhs) const {
  return resource_quantity_ == rhs.resource_quantity_;
}

bool FractionalResourceQuantity::operator!=(const FractionalResourceQuantity &rhs) const {
  return !(*this == rhs);
}

bool FractionalResourceQuantity::operator<(const FractionalResourceQuantity &rhs) const {
  return resource_quantity_ < rhs.resource_quantity_;
}

bool FractionalResourceQuantity::operator>(const FractionalResourceQuantity &rhs) const {
  return rhs < *this;
}

bool FractionalResourceQuantity::operator<=(const FractionalResourceQuantity &rhs) const {
  return !(*this > rhs);
}

bool FractionalResourceQuantity::operator>=(const FractionalResourceQuantity &rhs) const {
  bool result = !(*this < rhs);
  return result;
}

double FractionalResourceQuantity::ToDouble() const {
  return static_cast<double>(resource_quantity_) / kResourceConversionFactor;
}

ResourceSet::ResourceSet() {}

ResourceSet::ResourceSet(
    const std::unordered_map<std::string, FractionalResourceQuantity> &resource_map)
    : resource_capacity_(resource_map) {
  for (auto const &resource_pair : resource_map) {
    RAY_CHECK(resource_pair.second > 0);
  }
}

ResourceSet::ResourceSet(const std::unordered_map<std::string, double> &resource_map) {
  for (auto const &resource_pair : resource_map) {
    RAY_CHECK(resource_pair.second > 0);
    resource_capacity_[resource_pair.first] =
        FractionalResourceQuantity(resource_pair.second);
  }
}

ResourceSet::ResourceSet(const std::vector<std::string> &resource_labels,
                         const std::vector<double> resource_capacity) {
  RAY_CHECK(resource_labels.size() == resource_capacity.size());
  for (size_t i = 0; i < resource_labels.size(); i++) {
    RAY_CHECK(resource_capacity[i] > 0);
    resource_capacity_[resource_labels[i]] =
        FractionalResourceQuantity(resource_capacity[i]);
  }
}

ResourceSet::~ResourceSet() {}

bool ResourceSet::operator==(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

bool ResourceSet::IsEmpty() const {
  // Check whether the capacity of each resource type is zero. Exit early if not.
  return resource_capacity_.empty();
}

bool ResourceSet::IsSubset(const ResourceSet &other) const {
  // Check to make sure all keys of this are in other.
  for (const auto &resource_pair : resource_capacity_) {
    const auto &resource_name = resource_pair.first;
    const FractionalResourceQuantity &lhs_quantity = resource_pair.second;
    const FractionalResourceQuantity &rhs_quantity = other.GetResource(resource_name);
    if (lhs_quantity > rhs_quantity) {
      // Resource found in rhs, but lhs capacity exceeds rhs capacity.
      return false;
    }
  }
  return true;
}

/// Test whether this ResourceSet is a superset of the other ResourceSet
bool ResourceSet::IsSuperset(const ResourceSet &other) const {
  return other.IsSubset(*this);
}

/// Test whether this ResourceSet is precisely equal to the other ResourceSet.
bool ResourceSet::IsEqual(const ResourceSet &rhs) const {
  return (this->IsSubset(rhs) && rhs.IsSubset(*this));
}

void ResourceSet::AddOrUpdateResource(const std::string &resource_name,
                                      const FractionalResourceQuantity &capacity) {
  if (capacity > 0) {
    resource_capacity_[resource_name] = capacity;
  }
}

bool ResourceSet::DeleteResource(const std::string &resource_name) {
  if (resource_capacity_.count(resource_name) == 1) {
    resource_capacity_.erase(resource_name);
    return true;
  } else {
    return false;
  }
}

void ResourceSet::SubtractResources(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FractionalResourceQuantity &resource_capacity = resource_pair.second;
    if (resource_capacity_.count(resource_label) == 1) {
      resource_capacity_[resource_label] -= resource_capacity;
    }
    if (resource_capacity_[resource_label] <= 0) {
      resource_capacity_.erase(resource_label);
    }
  }
}

void ResourceSet::SubtractResourcesStrict(const ResourceSet &other) {
  // Subtract the resources, make sure none goes below zero and delete any if new capacity
  // is zero.
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FractionalResourceQuantity &resource_capacity = resource_pair.second;
    RAY_CHECK(resource_capacity_.count(resource_label) == 1)
        << "Attempt to acquire unknown resource: " << resource_label << " capacity "
        << resource_capacity.ToDouble();
    resource_capacity_[resource_label] -= resource_capacity;

    // Ensure that quantity is positive. Note, we have to have the check before
    // erasing the object to make sure that it doesn't get added back.
    RAY_CHECK(resource_capacity_[resource_label] >= 0)
        << "Capacity of resource after subtraction is negative, "
        << resource_capacity_[resource_label].ToDouble() << ".";

    if (resource_capacity_[resource_label] == 0) {
      resource_capacity_.erase(resource_label);
    }
  }
}

// Add a set of resources to the current set of resources subject to upper limits on
// capacity from the total_resource set
void ResourceSet::AddResourcesCapacityConstrained(const ResourceSet &other,
                                                  const ResourceSet &total_resources) {
  const std::unordered_map<std::string, FractionalResourceQuantity> &total_resource_map =
      total_resources.GetResourceAmountMap();
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &to_add_resource_label = resource_pair.first;
    const FractionalResourceQuantity &to_add_resource_capacity = resource_pair.second;
    if (total_resource_map.count(to_add_resource_label) != 0) {
      // If resource exists in total map, add to the local capacity map.
      // If the new capacity is less than the total capacity, set the new capacity to
      // the local capacity (capping to the total).
      const FractionalResourceQuantity &total_capacity =
          total_resource_map.at(to_add_resource_label);
      resource_capacity_[to_add_resource_label] =
          std::min(resource_capacity_[to_add_resource_label] + to_add_resource_capacity,
                   total_capacity);
    } else {
      // Resource does not exist in the total map, it probably got deleted from the total.
      // Don't panic, do nothing and simply continue.
      RAY_LOG(DEBUG) << "[AddResourcesCapacityConstrained] Resource "
                     << to_add_resource_label
                     << " not found in the total resource map. It probably got deleted, "
                        "not adding back to resource_capacity_.";
    }
  }
}

// Perform an outer join.
void ResourceSet::AddResources(const ResourceSet &other) {
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    const std::string &resource_label = resource_pair.first;
    const FractionalResourceQuantity &resource_capacity = resource_pair.second;
    resource_capacity_[resource_label] += resource_capacity;
  }
}

void ResourceSet::CommitBundleResources(const PlacementGroupID &group_id,
                                        const int bundle_index,
                                        const ResourceSet &other) {
  for (const auto &resource_pair : other.GetResourceAmountMap()) {
    // With bundle index (e.g., CPU_group_i_zzz).
    const std::string &resource_label =
        FormatPlacementGroupResource(resource_pair.first, group_id, bundle_index);
    const FractionalResourceQuantity &resource_capacity = resource_pair.second;
    resource_capacity_[resource_label] += resource_capacity;

    // Without bundle index (e.g., CPU_group_zzz).
    const std::string &wildcard_label =
        FormatPlacementGroupResource(resource_pair.first, group_id, -1);
    resource_capacity_[wildcard_label] += resource_capacity;
  }
}

bool ResourceSet::ReturnBundleResources(const PlacementGroupID &group_id,
                                        const int bundle_index, bool need_restore) {
  absl::flat_hash_map<std::string, FractionalResourceQuantity> to_restore;
  for (auto iter = resource_capacity_.begin(); iter != resource_capacity_.end();) {
    const std::string &bundle_resource_label = iter->first;
    // We only consider the indexed resources, ignoring the wildcard resource.
    // This is because when multiple bundles are created on one node, the quantity
    // of the wildcard resources contains resources from multiple bundles.
    if (IsBundleIndex(bundle_resource_label, group_id, bundle_index)) {
      const std::string &resource_label = GetOriginalResourceName(bundle_resource_label);
      const FractionalResourceQuantity &resource_capacity = iter->second;
      to_restore[resource_label] = resource_capacity;
      iter = resource_capacity_.erase(iter);
    } else {
      iter++;
    }
  }
  // For each matching resource to restore (e.g., key like CPU, GPU).
  for (const auto &pair : to_restore) {
    if (need_restore) {
      resource_capacity_[pair.first] += pair.second;
    }
    auto wildcard_resource = FormatPlacementGroupResource(pair.first, group_id, -1);
    resource_capacity_[wildcard_resource] -= pair.second;
    if (resource_capacity_[wildcard_resource] <= 0) {
      resource_capacity_.erase(wildcard_resource);
    }
  }
  return !to_restore.empty();
}

bool ResourceSet::ReturnBundleResources(const ResourceSet &bundle_resources,
                                        bool need_restore) {
  RAY_CHECK(this != &bundle_resources);

  for (auto &entry : bundle_resources.GetResourceAmountMap()) {
    // Make sure it's a bundle resource.
    if (entry.first.find("_group_") == std::string::npos) {
      return false;
    }
  }

  if (!IsSuperset(bundle_resources)) {
    return false;
  }

  SubtractResourcesStrict(bundle_resources);

  if (need_restore) {
    std::unordered_map<std::string, FractionalResourceQuantity> to_restore;
    for (auto &entry : bundle_resources.GetResourceAmountMap()) {
      auto pos = entry.first.find("_group_");
      auto resource_label = entry.first.substr(0, pos);
      // NOTE: use emplace to avoid the wildcard_resource.
      to_restore.emplace(resource_label, entry.second);
    }
    AddResources(ResourceSet(to_restore));
  }
  return true;
}

FractionalResourceQuantity ResourceSet::GetResource(
    const std::string &resource_name) const {
  auto iter = resource_capacity_.find(resource_name);
  return iter == resource_capacity_.end() ? 0 : iter->second;
}

bool ResourceSet::Contains(const std::string &resource_name) const {
  return resource_capacity_.count(resource_name) != 0;
}

bool ResourceSet::ContainsPlacementGroup(const PlacementGroupID &placement_group_id,
                                         int64_t bundle_index) const {
  std::string sub_name =
      "_group_" + std::to_string(bundle_index) + "_" + placement_group_id.Hex();
  const auto &resources = GetResourceMap();

  for (const auto &resource : resources) {
    if (resource.first.find(sub_name) != std::string::npos) {
      return true;
    }
  }
  return false;
};

const ResourceSet ResourceSet::GetNumCpus() const {
  ResourceSet cpu_resource_set;
  const FractionalResourceQuantity cpu_quantity = GetResource(kCPU_ResourceLabel);
  if (cpu_quantity > 0) {
    cpu_resource_set.resource_capacity_[kCPU_ResourceLabel] = cpu_quantity;
  }
  return cpu_resource_set;
}

const ResourceSet ResourceSet::GetMemory() const {
  ResourceSet memory_resource_set;
  const FractionalResourceQuantity cpu_quantity = GetResource(kMemory_ResourceLabel);
  if (cpu_quantity > 0) {
    memory_resource_set.resource_capacity_[kMemory_ResourceLabel] = cpu_quantity;
  }
  return memory_resource_set;
}

std::string format_resource(std::string resource_name, double quantity) {
  if (resource_name == "object_store_memory" ||
      StartsWith(resource_name, kMemory_ResourceLabel)) {
    if (quantity < 1024) {
      return std::to_string(quantity) + " B";
    } else if (quantity < 1024 * 1024) {
      return std::to_string(quantity / 1024) + " KB";
    } else if (quantity < 1024 * 1024 * 1024) {
      return std::to_string(quantity / (1024 * 1024)) + " MB";
    } else {
      return std::to_string(quantity / (1024 * 1024 * 1024)) + " GiB";
    }
  }
  return std::to_string(quantity);
}

const std::string ResourceSet::ToString() const {
  if (resource_capacity_.size() == 0) {
    return "{}";
  } else {
    std::string return_string = "";

    auto it = resource_capacity_.begin();

    // Convert the first element to a string.
    if (it != resource_capacity_.end()) {
      double resource_amount = (it->second).ToDouble();
      return_string +=
          "{" + it->first + ": " + format_resource(it->first, resource_amount) + "}";
      it++;
    }

    // Add the remaining elements to the string (along with a comma).
    for (; it != resource_capacity_.end(); ++it) {
      double resource_amount = (it->second).ToDouble();
      return_string +=
          ", {" + it->first + ": " + format_resource(it->first, resource_amount) + "}";
    }

    return return_string;
  }
}

const std::unordered_map<std::string, double> ResourceSet::GetResourceMap() const {
  std::unordered_map<std::string, double> result;
  for (const auto &resource_pair : resource_capacity_) {
    result[resource_pair.first] = resource_pair.second.ToDouble();
  }
  return result;
};

const std::unordered_map<std::string, FractionalResourceQuantity>
    &ResourceSet::GetResourceAmountMap() const {
  return resource_capacity_;
};

bool ResourceSet::ContainsPlacementGroup() const {
  for (const auto &entry : resource_capacity_) {
    if (entry.first.find("_group_") != std::string::npos) {
      return true;
    }
  }
  return false;
};

bool ResourceSet::ContainsRareResources() const {
  for (auto &entry : resource_capacity_) {
    if (IsRareResource(entry.first)) {
      return true;
    }
  }
  return false;
}

/// ResourceIds class implementation
/// Note(loushang.ls) The old scheduler implementation is user either requests an integer
/// greater than 1 or a fractional less than 1, but can't request a double value greater
/// than 1, it maybe for the GPU resouces. in new scheduler, we will treat integers and
/// fractionals separately, so user can request resource with double value, like
/// {"CPU": 1.5}
ResourceIds::ResourceIds() {}

ResourceIds::ResourceIds(double resource_quantity) {
  FractionalResourceQuantity quantity(resource_quantity);
  RAY_CHECK(quantity > 0);

  size_t whole_quantity = static_cast<size_t>(floor(resource_quantity));
  auto fractional_quantity = quantity - whole_quantity;

  if (whole_quantity > 0) {
    whole_ids_.reserve(whole_quantity);
    for (std::size_t i = 0; i < whole_quantity; ++i) {
      whole_ids_.push_back(i);
    }
  }

  if (fractional_quantity > 0) {
    RAY_CHECK(fractional_quantity < 1);
    fractional_ids_.push_back(std::make_pair(whole_quantity, fractional_quantity));
  }
  total_capacity_ = TotalQuantity();
  decrement_backlog_ = 0;
}

ResourceIds::ResourceIds(const std::vector<int64_t> &whole_ids)
    : whole_ids_(whole_ids), total_capacity_(whole_ids.size()), decrement_backlog_(0) {}

ResourceIds::ResourceIds(
    const std::vector<std::pair<int64_t, FractionalResourceQuantity>> &fractional_ids)
    : fractional_ids_(fractional_ids),
      total_capacity_(TotalQuantity()),
      decrement_backlog_(0) {}

ResourceIds::ResourceIds(
    const std::vector<int64_t> &whole_ids,
    const std::vector<std::pair<int64_t, FractionalResourceQuantity>> &fractional_ids)
    : whole_ids_(whole_ids),
      fractional_ids_(fractional_ids),
      total_capacity_(TotalQuantity()),
      decrement_backlog_(0) {}

bool ResourceIds::Contains(const FractionalResourceQuantity &resource_quantity) const {
  // Note(loushang.ls) we can't use `total_capacity_ >= resource_quantity` here.
  // Image this case: Contains(1.5), but what we have is [0.5, 0.5, 0.5].
  double sum_quantity = resource_quantity.ToDouble();
  if (whole_ids_.size() >= ceil(sum_quantity)) {
    return true;
  }

  size_t whole_quantity = static_cast<size_t>(floor(sum_quantity));
  auto fractional_quantity = resource_quantity - whole_quantity;

  // Check if the integer part is satisfied.
  if (whole_quantity >= 1 && whole_quantity > whole_ids_.size()) {
    return false;
  }

  // Check if the fractional part is satisfied.
  auto iter =
      find_if(fractional_ids_.begin(), fractional_ids_.end(),
              [fractional_quantity](
                  std::pair<int64_t, FractionalResourceQuantity> current_fractional) {
                return current_fractional.second >= fractional_quantity;
              });

  return (iter != fractional_ids_.end());
}

ResourceIds ResourceIds::Acquire(const FractionalResourceQuantity &resource_quantity) {
  RAY_CHECK(Contains(resource_quantity));
  // Acquire result.
  std::vector<int64_t> ids_to_return;

  double sum_quantity = resource_quantity.ToDouble();
  size_t whole_quantity = static_cast<int64_t>(floor(sum_quantity));
  auto fractional_quantity = resource_quantity - whole_quantity;

  if (resource_quantity >= 1) {
    for (std::size_t i = 0; i < whole_quantity; ++i) {
      ids_to_return.push_back(whole_ids_.back());
      whole_ids_.pop_back();
    }
  }

  if (fractional_quantity == 0) {
    return ResourceIds(ids_to_return);
  } else {
    RAY_CHECK(fractional_quantity < 1);
    for (auto &fractional_pair : fractional_ids_) {
      if (fractional_pair.second >= fractional_quantity) {
        auto return_pair = std::make_pair(fractional_pair.first, resource_quantity);
        fractional_pair.second -= fractional_quantity;
        // Remove the fractional pair if the new capacity is 0
        if (fractional_pair.second == 0) {
          std::swap(fractional_pair, fractional_ids_[fractional_ids_.size() - 1]);
          fractional_ids_.pop_back();
        }
        return ResourceIds(ids_to_return, {return_pair});
      }
    }
  }

  // If we get here then there weren't enough available fractional IDs, so we
  // need to use a whole ID.
  RAY_CHECK(whole_ids_.size() > 0);
  int64_t whole_id = whole_ids_.back();
  whole_ids_.pop_back();

  auto return_pair = std::make_pair(whole_id, fractional_quantity);
  // We cannot make use of the implicit conversion because ints have no
  // operator-(const FractionalResourceQuantity&) function.
  const FractionalResourceQuantity remaining_amount =
      FractionalResourceQuantity(1) - fractional_quantity;
  fractional_ids_.push_back(std::make_pair(whole_id, remaining_amount));
  return ResourceIds(ids_to_return, {return_pair});
}

void ResourceIds::Release(const ResourceIds &resource_ids) {
  auto const &whole_ids_to_return = resource_ids.WholeIds();

  int64_t return_resource_count = whole_ids_to_return.size();
  if (return_resource_count > decrement_backlog_) {
    // We are returning more resources than in the decrement backlog, thus set the backlog
    // to zero and insert (count - decrement_backlog resources).
    whole_ids_.insert(whole_ids_.end(), whole_ids_to_return.begin() + decrement_backlog_,
                      whole_ids_to_return.end());
    decrement_backlog_ = 0;
  } else {
    // Do not insert back to whole_ids_. Instead just decrement backlog by the return
    // count
    decrement_backlog_ -= return_resource_count;
  }

  // Return the fractional IDs.
  auto const &fractional_ids_to_return = resource_ids.FractionalIds();
  for (auto const &fractional_pair_to_return : fractional_ids_to_return) {
    int64_t resource_id = fractional_pair_to_return.first;
    auto const &fractional_pair_it = std::find_if(
        fractional_ids_.begin(), fractional_ids_.end(),
        [resource_id](std::pair<int64_t, FractionalResourceQuantity> &fractional_pair) {
          return fractional_pair.first == resource_id;
        });
    if (fractional_pair_it == fractional_ids_.end()) {
      fractional_ids_.push_back(fractional_pair_to_return);
    } else {
      fractional_pair_it->second += fractional_pair_to_return.second;
      RAY_CHECK(fractional_pair_it->second <= 1)
          << "Fractional Resource Id " << fractional_pair_it->first << " capacity is "
          << fractional_pair_it->second.ToDouble() << ". Should have been less than one.";
      // If this makes the ID whole, then return it to the list of whole IDs.
      if (fractional_pair_it->second == 1) {
        if (decrement_backlog_ > 0) {
          // There's a decrement backlog, do not add to whole_ids_
          decrement_backlog_--;
        } else {
          whole_ids_.push_back(resource_id);
        }
        fractional_ids_.erase(fractional_pair_it);
      }
    }
  }
}

ResourceIds ResourceIds::Plus(const ResourceIds &resource_ids) const {
  ResourceIds resource_ids_to_return(whole_ids_, fractional_ids_);
  resource_ids_to_return.Release(resource_ids);
  return resource_ids_to_return;
}

const std::vector<int64_t> &ResourceIds::WholeIds() const { return whole_ids_; }

const std::vector<std::pair<int64_t, FractionalResourceQuantity>>
    &ResourceIds::FractionalIds() const {
  return fractional_ids_;
}

bool ResourceIds::TotalQuantityIsZero() const {
  return whole_ids_.empty() && fractional_ids_.empty();
}

FractionalResourceQuantity ResourceIds::TotalQuantity() const {
  FractionalResourceQuantity total_quantity =
      FractionalResourceQuantity(whole_ids_.size());
  for (auto const &fractional_pair : fractional_ids_) {
    total_quantity += fractional_pair.second;
  }
  return total_quantity;
}

std::string ResourceIds::ToString() const {
  std::string return_string = "Whole IDs: [";
  for (auto const &whole_id : whole_ids_) {
    return_string += std::to_string(whole_id) + ", ";
  }
  return_string += "], Fractional IDs: ";
  for (auto const &fractional_pair : fractional_ids_) {
    double fractional_amount = fractional_pair.second.ToDouble();
    return_string += "(" + std::to_string(fractional_pair.first) + ", " +
                     std::to_string(fractional_amount) + "), ";
  }
  return_string += "]";
  return return_string;
}

void ResourceIds::UpdateCapacity(int64_t new_capacity) {
  // Assert the new capacity is positive for sanity
  RAY_CHECK(new_capacity >= 0);
  int64_t capacity_delta = new_capacity - total_capacity_.ToDouble();
  if (capacity_delta < 0) {
    DecreaseCapacity(-1 * capacity_delta);
  } else {
    IncreaseCapacity(capacity_delta);
  }
}

void ResourceIds::IncreaseCapacity(int64_t increment_quantity) {
  // Adjust with decrement_backlog_
  int64_t actual_increment_quantity = 0;
  actual_increment_quantity =
      std::max<int64_t>(0, increment_quantity - decrement_backlog_);
  decrement_backlog_ = std::max<int64_t>(0, decrement_backlog_ - increment_quantity);

  if (actual_increment_quantity > 0) {
    for (int i = 0; i < actual_increment_quantity; i++) {
      whole_ids_.push_back(-1);  // Dynamic resources are assigned resource id -1.
    }
    total_capacity_ += actual_increment_quantity;
  }
}

void ResourceIds::DecreaseCapacity(int64_t decrement_quantity) {
  // Get total quantity, but casting to int to truncate any fractional resources. Updates
  // are supported only on whole resources.
  int64_t available_quantity = TotalQuantity().ToDouble();
  RAY_LOG(DEBUG) << "[DecreaseCapacity] Available quantity: " << available_quantity;

  if (available_quantity < decrement_quantity) {
    RAY_LOG(DEBUG) << "[DecreaseCapacity] Available quantity < decrement quantity  "
                   << decrement_quantity;
    // We're trying to remove more resources than are available
    // In this case, add the difference to the decrement backlog, and when resources are
    // released the backlog will be cleared
    decrement_backlog_ += (decrement_quantity - available_quantity);
    // To decrease capacity, just acquire resources and forget about them. They are popped
    // from whole_ids when acquired.
    Acquire(available_quantity);
  } else {
    RAY_LOG(DEBUG) << "[DecreaseCapacity] Available quantity > decrement quantity  "
                   << decrement_quantity;
    // Simply acquire resources if sufficient are available
    Acquire(decrement_quantity);
  }
  total_capacity_ -= decrement_quantity;
}

bool ResourceIds::IsWhole(double resource_quantity) const {
  int64_t whole_quantity = resource_quantity;
  return whole_quantity == resource_quantity;
}

/// ResourceIdSet class implementation

ResourceIdSet::ResourceIdSet() {}

ResourceIdSet::ResourceIdSet(const ResourceSet &resource_set) {
  for (auto const &resource_pair : resource_set.GetResourceMap()) {
    auto const &resource_name = resource_pair.first;
    double resource_quantity = resource_pair.second;
    available_resources_[resource_name] = ResourceIds(resource_quantity);
  }
}

ResourceIdSet::ResourceIdSet(
    const std::unordered_map<std::string, ResourceIds> &available_resources)
    : available_resources_(available_resources) {}

bool ResourceIdSet::Contains(const ResourceSet &resource_set) const {
  for (auto const &resource_pair : resource_set.GetResourceAmountMap()) {
    auto const &resource_name = resource_pair.first;
    const FractionalResourceQuantity &resource_quantity = resource_pair.second;

    auto it = available_resources_.find(resource_name);
    if (it == available_resources_.end()) {
      return false;
    }

    if (!it->second.Contains(resource_quantity)) {
      return false;
    }
  }
  return true;
}

ResourceIdSet ResourceIdSet::Acquire(const ResourceSet &resource_set) {
  std::unordered_map<std::string, ResourceIds> acquired_resources;

  for (auto const &resource_pair : resource_set.GetResourceAmountMap()) {
    auto const &resource_name = resource_pair.first;
    const FractionalResourceQuantity &resource_quantity = resource_pair.second;

    auto it = available_resources_.find(resource_name);
    RAY_CHECK(it != available_resources_.end());
    acquired_resources[resource_name] = it->second.Acquire(resource_quantity);
    if (it->second.TotalQuantityIsZero()) {
      available_resources_.erase(it);
    }
  }
  return ResourceIdSet(acquired_resources);
}

void ResourceIdSet::Release(const ResourceIdSet &resource_id_set) {
  for (auto const &resource_pair : resource_id_set.AvailableResources()) {
    auto const &resource_name = resource_pair.first;
    auto const &resource_ids = resource_pair.second;
    RAY_CHECK(!resource_ids.TotalQuantityIsZero());

    auto it = available_resources_.find(resource_name);
    if (it == available_resources_.end()) {
      available_resources_[resource_name] = resource_ids;
    } else {
      it->second.Release(resource_ids);
    }
  }
}

void ResourceIdSet::ReleaseConstrained(const ResourceIdSet &resource_id_set,
                                       const ResourceSet &resources_total) {
  for (auto const &resource_pair : resource_id_set.AvailableResources()) {
    auto const &resource_name = resource_pair.first;
    // Release only if the resource exists in resources_total
    if (resources_total.GetResource(resource_name) != 0) {
      auto const &resource_ids = resource_pair.second;
      RAY_CHECK(!resource_ids.TotalQuantityIsZero());

      auto it = available_resources_.find(resource_name);
      if (it == available_resources_.end()) {
        available_resources_[resource_name] = resource_ids;
      } else {
        it->second.Release(resource_ids);
      }
    }
  }
}

void ResourceIdSet::Clear() { available_resources_.clear(); }

ResourceIdSet ResourceIdSet::Plus(const ResourceIdSet &resource_id_set) const {
  ResourceIdSet resource_id_set_to_return(available_resources_);
  resource_id_set_to_return.Release(resource_id_set);
  return resource_id_set_to_return;
}

void ResourceIdSet::AddOrUpdateResource(const std::string &resource_name,
                                        int64_t capacity) {
  auto it = available_resources_.find(resource_name);
  if (it != available_resources_.end()) {
    // If resource exists, update capacity
    ResourceIds &resid = (it->second);
    resid.UpdateCapacity(capacity);
  } else {
    // If resource does not exist, create
    available_resources_[resource_name] = ResourceIds(capacity);
  }
}

void ResourceIdSet::CommitBundleResourceIds(const PlacementGroupID &group_id,
                                            const int bundle_index,
                                            const std::string &resource_name,
                                            ResourceIds &resource_ids) {
  auto index_name = FormatPlacementGroupResource(resource_name, group_id, bundle_index);
  auto wildcard_name = FormatPlacementGroupResource(resource_name, group_id, -1);
  available_resources_[index_name] = available_resources_[index_name].Plus(resource_ids);
  available_resources_[wildcard_name] =
      available_resources_[wildcard_name].Plus(resource_ids);
}

void ResourceIdSet::ReturnBundleResources(const PlacementGroupID &group_id,
                                          const int bundle_index,
                                          const std::string &original_resource_name) {
  auto index_resource_name =
      FormatPlacementGroupResource(original_resource_name, group_id, bundle_index);
  auto iter_index = available_resources_.find(index_resource_name);
  if (iter_index == available_resources_.end()) {
    return;
  }

  // Erase and transfer the index bundle resource back to the original.
  auto bundle_ids = iter_index->second;
  available_resources_.erase(iter_index);
  available_resources_[original_resource_name] =
      (available_resources_[original_resource_name].Plus(bundle_ids));

  // Also erase the the equivalent number of units from the wildcard resource.
  auto wildcard_name = FormatPlacementGroupResource(original_resource_name, group_id, -1);
  available_resources_[wildcard_name].Acquire(bundle_ids.TotalQuantity());
  if (available_resources_[wildcard_name].TotalQuantityIsZero()) {
    available_resources_.erase(wildcard_name);
  }
}

void ResourceIdSet::DeleteResource(const std::string &resource_name) {
  if (available_resources_.count(resource_name) != 0) {
    available_resources_.erase(resource_name);
  }
}

const std::unordered_map<std::string, ResourceIds> &ResourceIdSet::AvailableResources()
    const {
  return available_resources_;
}

ResourceIdSet ResourceIdSet::GetCpuResources() const {
  std::unordered_map<std::string, ResourceIds> cpu_resources;

  auto it = available_resources_.find(kCPU_ResourceLabel);
  if (it != available_resources_.end()) {
    cpu_resources.insert(*it);
  }
  return ResourceIdSet(cpu_resources);
}

ResourceSet ResourceIdSet::ToResourceSet() const {
  std::unordered_map<std::string, FractionalResourceQuantity> resource_set;
  for (auto const &resource_pair : available_resources_) {
    resource_set[resource_pair.first] = resource_pair.second.TotalQuantity();
  }
  return ResourceSet(resource_set);
}

std::string ResourceIdSet::ToString() const {
  std::string return_string = "AvailableResources: ";

  auto it = available_resources_.begin();

  // Convert the first element to a string.
  if (it != available_resources_.end()) {
    return_string += (it->first + ": {" + it->second.ToString() + "}");
    it++;
  }

  // Add the remaining elements to the string (along with a comma).
  for (; it != available_resources_.end(); ++it) {
    return_string += (", " + it->first + ": {" + it->second.ToString() + "}");
  }

  return return_string;
}

std::vector<flatbuffers::Offset<protocol::ResourceIdSetInfo>> ResourceIdSet::ToFlatbuf(
    flatbuffers::FlatBufferBuilder &fbb) const {
  std::vector<flatbuffers::Offset<protocol::ResourceIdSetInfo>> return_message;
  for (auto const &resource_pair : available_resources_) {
    std::vector<int64_t> resource_ids;
    std::vector<double> resource_fractions;
    for (auto whole_id : resource_pair.second.WholeIds()) {
      resource_ids.push_back(whole_id);
      resource_fractions.push_back(1);
    }

    for (auto const &fractional_pair : resource_pair.second.FractionalIds()) {
      resource_ids.push_back(fractional_pair.first);
      resource_fractions.push_back(fractional_pair.second.ToDouble());
    }

    auto resource_id_set_message = protocol::CreateResourceIdSetInfo(
        fbb, fbb.CreateString(resource_pair.first), fbb.CreateVector(resource_ids),
        fbb.CreateVector(resource_fractions));

    return_message.push_back(resource_id_set_message);
  }

  return return_message;
}

const std::string ResourceIdSet::Serialize() const {
  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(protocol::CreateResourceIdSetInfos(fbb, fbb.CreateVector(ToFlatbuf(fbb))));
  return std::string(fbb.GetBufferPointer(), fbb.GetBufferPointer() + fbb.GetSize());
}

/// SchedulingResources class implementation

SchedulingResources::SchedulingResources()
    : resources_total_(ResourceSet()),
      resources_available_(ResourceSet()),
      resources_load_(ResourceSet()) {}

SchedulingResources::SchedulingResources(const ResourceSet &total)
    : resources_total_(total),
      resources_available_(total),
      resources_load_(ResourceSet()) {}

SchedulingResources::~SchedulingResources() {}

const ResourceSet &SchedulingResources::GetAvailableResources(
    ResourceSet *realtime_available_resources) const {
  if (realtime_available_resources) {
    (*realtime_available_resources) = resources_available_;
    realtime_available_resources->SubtractResources(resources_normal_tasks_);
  }
  return resources_available_;
}

void SchedulingResources::SetAvailableResources(ResourceSet &&newset) {
  resources_available_ = newset;
}

const ResourceSet &SchedulingResources::GetTotalResources() const {
  return resources_total_;
}

void SchedulingResources::SetTotalResources(ResourceSet &&newset) {
  resources_total_ = newset;
}

const ResourceSet &SchedulingResources::GetLoadResources() const {
  return resources_load_;
}

void SchedulingResources::SetLoadResources(ResourceSet &&newset) {
  resources_load_ = newset;
}

// Return specified resources back to SchedulingResources.
void SchedulingResources::Release(const ResourceSet &resources) {
  return resources_available_.AddResourcesCapacityConstrained(resources,
                                                              resources_total_);
}

// Take specified resources from SchedulingResources.
void SchedulingResources::Acquire(const ResourceSet &resources) {
  resources_available_.SubtractResourcesStrict(resources);
}

// The reason we need this method is sometimes we may want add some converted
// resource which is not exist in total resource to the available resource.
// (e.g., placement group)
void SchedulingResources::AddResource(const ResourceSet &resources) {
  resources_total_.AddResources(resources);
  resources_available_.AddResources(resources);
}

void SchedulingResources::SubtractResource(const ResourceSet &resources) {
  resources_total_.SubtractResources(resources);
  resources_available_.SubtractResources(resources);
}

void SchedulingResources::UpdateResourceCapacity(const std::string &resource_name,
                                                 int64_t capacity) {
  if (capacity <= 0) {
    return;
  }

  const FractionalResourceQuantity new_capacity = FractionalResourceQuantity(capacity);
  const FractionalResourceQuantity &current_capacity =
      resources_total_.GetResource(resource_name);
  if (current_capacity > 0) {
    // If the resource exists, add to total and available resources
    const FractionalResourceQuantity capacity_difference =
        new_capacity - current_capacity;
    const FractionalResourceQuantity &current_available_capacity =
        resources_available_.GetResource(resource_name);
    FractionalResourceQuantity new_available_capacity =
        current_available_capacity + capacity_difference;
    resources_total_.AddOrUpdateResource(resource_name, new_capacity);
    if (new_available_capacity > 0) {
      resources_available_.AddOrUpdateResource(resource_name, new_available_capacity);
    } else {
      resources_available_.DeleteResource(resource_name);
    }
  } else {
    // Resource does not exist, just add it to total and available. Do not add to load.
    resources_total_.AddOrUpdateResource(resource_name, new_capacity);
    resources_available_.AddOrUpdateResource(resource_name, new_capacity);
  }
}

bool SchedulingResources::PrepareBundleResources(const PlacementGroupID &group,
                                                 const int bundle_index,
                                                 const ResourceSet &resource_set) {
  if (resource_set.IsSubset(resources_available_)) {
    // Only subtract from available resoruces.
    resources_available_.SubtractResourcesStrict(resource_set);
    return true;
  }
  return false;
}

void SchedulingResources::CommitBundleResources(const PlacementGroupID &group,
                                                const int bundle_index,
                                                const ResourceSet &resource_set) {
  resources_available_.CommitBundleResources(group, bundle_index, resource_set);
  resources_total_.CommitBundleResources(group, bundle_index, resource_set);
}

bool SchedulingResources::ReturnBundleResources(const PlacementGroupID &group_id,
                                                const int bundle_index) {
  bool is_bundle_exist = false;
  std::string suffix = "_group_" + std::to_string(bundle_index) + "_" + group_id.Hex();
  for (const auto &entry : resources_total_.GetResourceAmountMap()) {
    if (EndsWith(entry.first, suffix)) {
      if (resources_available_.GetResource(entry.first) < entry.second) {
        return false;
      }
      is_bundle_exist = true;
    }
  }

  if (!is_bundle_exist) {
    return false;
  }

  bool success = resources_available_.ReturnBundleResources(group_id, bundle_index) &&
                 resources_total_.ReturnBundleResources(group_id, bundle_index,
                                                        /*need_restore=*/false);
  if (success) {
    for (auto &entry : resources_available_.GetResourceAmountMap()) {
      auto total_quantity = resources_total_.GetResource(entry.first);
      if (total_quantity < entry.second) {
        resources_available_.AddOrUpdateResource(entry.first, total_quantity);
      }
    }
  }

  return success;
}

bool SchedulingResources::ReturnBundleResources(const ResourceSet &bundle_resources) {
  bool success = resources_available_.ReturnBundleResources(bundle_resources) &&
                 resources_total_.ReturnBundleResources(bundle_resources,
                                                        /*need_restore=*/false);
  if (success) {
    for (auto &entry : resources_available_.GetResourceAmountMap()) {
      auto total_quantity = resources_total_.GetResource(entry.first);
      if (total_quantity < entry.second) {
        resources_available_.AddOrUpdateResource(entry.first, total_quantity);
      }
    }
  }
  return success;
}

void SchedulingResources::DeleteResource(const std::string &resource_name) {
  resources_total_.DeleteResource(resource_name);
  resources_available_.DeleteResource(resource_name);
  resources_load_.DeleteResource(resource_name);
}

const ResourceSet &SchedulingResources::GetNormalTaskResources() const {
  return resources_normal_tasks_;
}

void SchedulingResources::SetNormalTaskResources(const ResourceSet &newset) {
  resources_normal_tasks_ = newset;
}

const ResourceSet &SchedulingResources::GetNodeRuntimeResources() const {
  return resources_node_runtime_;
}

void SchedulingResources::SetNodeRuntimeResources(const ResourceSet &newset) {
  resources_node_runtime_ = newset;
}

const ResourceSet &SchedulingResources::GetTotalRequiredResources() const {
  return resources_total_required_;
}

ResourceSet &SchedulingResources::GetMutableTotalRequiredResources() {
  return resources_total_required_;
}

std::string SchedulingResources::DebugString() const {
  std::stringstream result;

  auto resources_available = resources_available_;
  resources_available.SubtractResources(resources_normal_tasks_);

  result << "\n- total: " << resources_total_.ToString();
  result << "\n- avail: " << resources_available.ToString();
  result << "\n- normal_task_resources: " << resources_normal_tasks_.ToString();
  result << "\n- total_required_resources: " << resources_total_required_.ToString();
  return result.str();
};

}  // namespace ray
