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

#include "ray/common/task/scheduling_resources.h"

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/bundle_spec.h"
#include "ray/common/id.h"

namespace ray {
class SchedulingResourcesTest : public ::testing::Test {
 public:
  void SetUp() override {
    resource_set = std::make_shared<ResourceSet>();
    resource_id_set = std::make_shared<ResourceIdSet>();
  }

 protected:
  std::shared_ptr<ResourceSet> resource_set;
  std::shared_ptr<ResourceIdSet> resource_id_set;
  std::shared_ptr<ResourceIds> resource_ids;
};

TEST_F(SchedulingResourcesTest, CreateWithDoubleValue) {
  resource_ids = std::make_shared<ResourceIds>(20);
  auto wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(20, wholeIds.size());
  auto fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(0, fractionalIds.size());

  resource_ids = std::make_shared<ResourceIds>(10.5);
  wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(10, wholeIds.size());
  fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(1, fractionalIds.size());
  ASSERT_EQ(0.5, fractionalIds.at(0).second.ToDouble());

  resource_ids = std::make_shared<ResourceIds>(0.5);
  wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(0, wholeIds.size());
  fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(1, fractionalIds.size());
  ASSERT_EQ(0.5, fractionalIds.at(0).second.ToDouble());
}

TEST_F(SchedulingResourcesTest, ContainsMethodTest) {
  resource_ids = std::make_shared<ResourceIds>(10.5);
  ASSERT_TRUE(resource_ids->Contains(5));
  ASSERT_TRUE(resource_ids->Contains(0.5));
  ASSERT_TRUE(resource_ids->Contains(10.5));
  ASSERT_FALSE(resource_ids->Contains(11));

  std::vector<std::pair<int64_t, FractionalResourceQuantity>> fractional_ids = {
      std::make_pair(0, 0.5), std::make_pair(0, 0.5), std::make_pair(0, 0.5)};
  resource_ids = std::make_shared<ResourceIds>(fractional_ids);
  ASSERT_FALSE(resource_ids->Contains(1.5));
}

TEST_F(SchedulingResourcesTest, AcquireMethodTest) {
  resource_ids = std::make_shared<ResourceIds>(10.5);
  auto acquire_res = resource_ids->Acquire(8);
  auto wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(2, wholeIds.size());
  auto fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(1, fractionalIds.size());
  ASSERT_EQ(0.5, fractionalIds.at(0).second.ToDouble());

  resource_ids = std::make_shared<ResourceIds>(10.5);
  acquire_res = resource_ids->Acquire(2.5);
  wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(8, wholeIds.size());
  fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(0, fractionalIds.size());
  ASSERT_EQ(2, acquire_res.WholeIds().size());
  ASSERT_EQ(1, acquire_res.FractionalIds().size());

  resource_ids = std::make_shared<ResourceIds>(10);
  acquire_res = resource_ids->Acquire(2.5);
  wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(7, wholeIds.size());
  fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(1, fractionalIds.size());
  ASSERT_EQ(0.5, fractionalIds.at(0).second.ToDouble());
  ASSERT_EQ(2, acquire_res.WholeIds().size());
  ASSERT_EQ(1, acquire_res.FractionalIds().size());
  ASSERT_EQ(0.5, acquire_res.FractionalIds().at(0).second.ToDouble());
}

TEST_F(SchedulingResourcesTest, ReleaseMethodTest) {
  resource_ids = std::make_shared<ResourceIds>(10);
  auto released_resource = ResourceIds({10, 11}, {std::make_pair(12, 0.5)});
  resource_ids->Release(released_resource);
  auto wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(12, wholeIds.size());
  auto fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(1, fractionalIds.size());
  ASSERT_EQ(0.5, fractionalIds.at(0).second.ToDouble());

  released_resource = ResourceIds({std::make_pair(12, 0.5)});
  resource_ids->Release(released_resource);
  wholeIds = resource_ids->WholeIds();
  ASSERT_EQ(13, wholeIds.size());
  fractionalIds = resource_ids->FractionalIds();
  ASSERT_EQ(0, fractionalIds.size());
}

TEST_F(SchedulingResourcesTest, CommitBundleResources) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);
  resource_set->CommitBundleResources(group_id, 1, resource);
  resource_labels.pop_back();
  resource_labels.push_back("CPU_group_1_" + group_id.Hex());
  resource_labels.push_back("CPU_group_" + group_id.Hex());
  resource_capacity.push_back(1.0);
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource));
}

TEST_F(SchedulingResourcesTest, AddBundleResource) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::string wild_name = "CPU_group_" + group_id.Hex();
  std::string index_name = "CPU_group_1_" + group_id.Hex();
  std::vector<int64_t> whole_ids = {1, 2, 3};
  ResourceIds resource_ids(whole_ids);
  resource_id_set->CommitBundleResourceIds(group_id, 1, "CPU", resource_ids);
  ASSERT_EQ(2, resource_id_set->AvailableResources().size());
  for (auto res : resource_id_set->AvailableResources()) {
    ASSERT_TRUE(res.first == wild_name || res.first == index_name) << res.first;
  }
}

TEST_F(SchedulingResourcesTest, ReturnBundleResources) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);
  resource_set->CommitBundleResources(group_id, 1, resource);
  resource_labels.pop_back();
  resource_labels.push_back("CPU_group_" + group_id.Hex());
  resource_labels.push_back("CPU_group_1_" + group_id.Hex());
  resource_capacity.push_back(1.0);
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource));
  resource_set->ReturnBundleResources(group_id, 1);
  ASSERT_EQ(1, resource_set->IsEqual(resource))
      << resource_set->ToString() << " vs " << resource.ToString();
}

TEST_F(SchedulingResourcesTest, MultipleBundlesAddRemove) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU"};
  std::vector<double> resource_capacity = {1.0};
  ResourceSet resource(resource_labels, resource_capacity);

  // Construct resource set containing two bundles.
  resource_set->CommitBundleResources(group_id, 1, resource);
  resource_set->CommitBundleResources(group_id, 2, resource);
  resource_labels = {
      "CPU_group_" + group_id.Hex(),
      "CPU_group_1_" + group_id.Hex(),
      "CPU_group_2_" + group_id.Hex(),
  };
  resource_capacity = {2.0, 1.0, 1.0};
  ResourceSet result_resource(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource))
      << resource_set->ToString() << " vs " << result_resource.ToString();

  // Return group 2.
  resource_set->ReturnBundleResources(group_id, 2);
  resource_labels = {
      "CPU",
      "CPU_group_" + group_id.Hex(),
      "CPU_group_1_" + group_id.Hex(),
  };
  resource_capacity = {1.0, 1.0, 1.0};
  ResourceSet result_resource2(resource_labels, resource_capacity);
  ASSERT_EQ(1, resource_set->IsEqual(result_resource2))
      << resource_set->ToString() << " vs " << result_resource2.ToString();

  // Return group 1.
  resource_set->ReturnBundleResources(group_id, 1);
  ASSERT_EQ(1, resource_set->IsEqual(ResourceSet({"CPU"}, {2.0})))
      << resource_set->ToString() << " vs " << resource.ToString();
}

TEST_F(SchedulingResourcesTest, MultipleBundlesAddRemoveIdSet) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  ResourceIdSet resource_ids;

  // Construct resource set containing two bundles.
  auto rid1 = ResourceIds({1, 2});
  auto rid2 = ResourceIds({3, 4});
  resource_ids.CommitBundleResourceIds(group_id, 1, "CPU", rid1);
  resource_ids.CommitBundleResourceIds(group_id, 2, "CPU", rid2);
  resource_ids.CommitBundleResourceIds(group_id, 1, "GPU", rid1);
  resource_ids.CommitBundleResourceIds(group_id, 2, "GPU", rid2);
  auto result = ResourceSet(
      {
          "CPU_group_" + group_id.Hex(),
          "CPU_group_1_" + group_id.Hex(),
          "CPU_group_2_" + group_id.Hex(),
          "GPU_group_" + group_id.Hex(),
          "GPU_group_1_" + group_id.Hex(),
          "GPU_group_2_" + group_id.Hex(),
      },
      {4.0, 2.0, 2.0, 4.0, 2.0, 2.0});
  ASSERT_EQ(1, resource_ids.ToResourceSet().IsEqual(result))
      << resource_ids.ToString() << " vs " << result.ToString();

  // Remove the first bundle.
  resource_ids.ReturnBundleResources(group_id, 1, "CPU");
  resource_ids.ReturnBundleResources(group_id, 1, "GPU");
  result = ResourceSet(
      {
          "CPU_group_" + group_id.Hex(),
          "CPU",
          "CPU_group_2_" + group_id.Hex(),
          "GPU_group_" + group_id.Hex(),
          "GPU",
          "GPU_group_2_" + group_id.Hex(),
      },
      {2.0, 2.0, 2.0, 2.0, 2.0, 2.0});
  ASSERT_EQ(1, resource_ids.ToResourceSet().IsEqual(result))
      << resource_ids.ToString() << " vs " << result.ToString();

  // Remove the second bundle.
  resource_ids.ReturnBundleResources(group_id, 2, "CPU");
  resource_ids.ReturnBundleResources(group_id, 2, "GPU");
  result = ResourceSet(
      {
          "CPU",
          "GPU",
      },
      {4.0, 4.0});
  ASSERT_EQ(1, resource_ids.ToResourceSet().IsEqual(result))
      << resource_ids.ToString() << " vs " << result.ToString();
}

TEST_F(SchedulingResourcesTest, ReturnBundleResourcesWithoutRestore) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU"};
  ResourceSet total_resoruces(resource_labels, {2.0});
  SchedulingResources scheduling_resoruces(total_resoruces);

  ResourceSet bundle_resources(resource_labels, {1.0});
  scheduling_resoruces.PrepareBundleResources(group_id, 1, bundle_resources);
  ASSERT_TRUE(total_resoruces.IsEqual(scheduling_resoruces.GetTotalResources()));

  ResourceSet available_resoruces({"CPU"}, {1.0});
  ASSERT_TRUE(available_resoruces.IsEqual(scheduling_resoruces.GetAvailableResources()));

  scheduling_resoruces.CommitBundleResources(group_id, 1, bundle_resources);
  ASSERT_FALSE(total_resoruces.IsEqual(scheduling_resoruces.GetTotalResources()));
  ASSERT_FALSE(available_resoruces.IsEqual(scheduling_resoruces.GetAvailableResources()));

  scheduling_resoruces.ReturnBundleResources(group_id, 1);
  ASSERT_TRUE(total_resoruces.IsEqual(scheduling_resoruces.GetTotalResources()));
  ASSERT_TRUE(total_resoruces.IsEqual(scheduling_resoruces.GetAvailableResources()));
}

TEST_F(SchedulingResourcesTest, ReturnBundleResourcesAdvance) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU", "memory"};
  ResourceSet total_resoruces(resource_labels, {2.0, 2.0});
  SchedulingResources scheduling_resoruces(total_resoruces);

  std::vector<std::string> bundle_resource_labels = {"CPU"};
  ResourceSet bundle_resources(bundle_resource_labels, {1.0});
  scheduling_resoruces.PrepareBundleResources(group_id, 1, bundle_resources);
  scheduling_resoruces.CommitBundleResources(group_id, 1, bundle_resources);

  // It is failed to return non-exist bundle resources, it will return nothing.
  ASSERT_FALSE(scheduling_resoruces.ReturnBundleResources(group_id, 0));

  auto pg_resource_name_with_index = FormatPlacementGroupResource("CPU", group_id, 1);
  auto pg_resource_name_without_index = FormatPlacementGroupResource("CPU", group_id, -1);
  ResourceSet resource_set({pg_resource_name_with_index, pg_resource_name_without_index},
                           {0.5, 0.5});
  ResourceSet available_resoruces = scheduling_resoruces.GetAvailableResources();
  available_resoruces.SubtractResources(resource_set);
  scheduling_resoruces.SetAvailableResources(std::move(available_resoruces));
  // It is failed to return partial bundle resources.
  ASSERT_FALSE(scheduling_resoruces.ReturnBundleResources(group_id, 1));

  available_resoruces = scheduling_resoruces.GetAvailableResources();
  available_resoruces.SubtractResources(resource_set);
  scheduling_resoruces.SetAvailableResources(std::move(available_resoruces));
  // It is failed to return bundle resources that not released yet.
  ASSERT_FALSE(scheduling_resoruces.ReturnBundleResources(group_id, 1));

  available_resoruces = scheduling_resoruces.GetAvailableResources();
  available_resoruces.AddResources(resource_set);
  available_resoruces.AddResources(resource_set);
  scheduling_resoruces.SetAvailableResources(std::move(available_resoruces));
  ASSERT_TRUE(scheduling_resoruces.ReturnBundleResources(group_id, 1));

  ASSERT_TRUE(total_resoruces.IsEqual(scheduling_resoruces.GetTotalResources()));
  ASSERT_TRUE(total_resoruces.IsEqual(scheduling_resoruces.GetAvailableResources()));
}

TEST_F(SchedulingResourcesTest, ReturnPartialBundleResources) {
  PlacementGroupID group_id = PlacementGroupID::Of(JobID::FromInt(1));
  std::vector<std::string> resource_labels = {"CPU", "CUSTOM"};
  ResourceSet total_resoruces(resource_labels, {100.0, 100.0});
  SchedulingResources scheduling_resoruces(total_resoruces);

  ResourceSet bundle0_resources(resource_labels, {10.0, 10.0});
  scheduling_resoruces.PrepareBundleResources(group_id, 0, bundle0_resources);
  scheduling_resoruces.CommitBundleResources(group_id, 0, bundle0_resources);

  ResourceSet bundle1_resources(resource_labels, {10.0, 10.0});
  scheduling_resoruces.PrepareBundleResources(group_id, 1, bundle1_resources);
  scheduling_resoruces.CommitBundleResources(group_id, 1, bundle1_resources);

  RAY_LOG(INFO) << scheduling_resoruces.DebugString();

  auto build_pg_resoruce_set = [](PlacementGroupID group_id, int bound_index,
                                  const ResourceSet &resources) {
    ResourceSet pg_resoruces;
    for (auto &entry : resources.GetResourceAmountMap()) {
      auto pg_resource_name_without_index =
          FormatPlacementGroupResource(entry.first, group_id, -1);
      auto pg_resource_name_with_index =
          FormatPlacementGroupResource(entry.first, group_id, bound_index);
      pg_resoruces.AddOrUpdateResource(pg_resource_name_without_index, entry.second);
      pg_resoruces.AddOrUpdateResource(pg_resource_name_with_index, entry.second);
    }
    return pg_resoruces;
  };

  auto required_resources_00 =
      build_pg_resoruce_set(group_id, 0, ResourceSet({"CPU", "CUSTOM"}, {4.0, 4.0}));

  auto required_resources_01 =
      build_pg_resoruce_set(group_id, 0, ResourceSet({"CPU", "CUSTOM"}, {5.0, 5.0}));

  auto required_resources_1 =
      build_pg_resoruce_set(group_id, 1, ResourceSet({"CPU", "CUSTOM"}, {10.0, 10.0}));

  RAY_LOG(INFO) << "Acquire required_resources_00";
  scheduling_resoruces.Acquire(required_resources_00);
  auto acquired_resources_00 = required_resources_00;

  RAY_LOG(INFO) << "Acquire required_resources_01";
  scheduling_resoruces.Acquire(required_resources_01);
  auto acquired_resources_01 = required_resources_01;

  RAY_LOG(INFO) << "Acquire required_resources_1";
  scheduling_resoruces.Acquire(required_resources_1);
  auto acquired_resources_1 = required_resources_1;

  RAY_LOG(INFO) << "After acquired: " << scheduling_resoruces.DebugString();

  RAY_LOG(INFO) << "---- Verify ----";
  // ResourceSet expected_total_resources(
  //     {"CPU", pg_resource_name_with_index_0, pg_resource_name_with_index_1,
  //      pg_resource_name_without_index},
  //     {100.0, 10.0, 10.0, 20.0});
  // ResourceSet expected_available_resources(
  //     {"CPU", pg_resource_name_without_index, pg_resource_name_with_index_0},
  //     {80.0, 1.0, 1.0});
  // ASSERT_TRUE(expected_total_resources.IsEqual(scheduling_resoruces.GetTotalResources()));
  // ASSERT_TRUE(
  //     expected_available_resources.IsEqual(scheduling_resoruces.GetAvailableResources()));

  auto update_runtime_resources = [&scheduling_resoruces, &acquired_resources_00](
                                      PlacementGroupID group_id, int bundle_index,
                                      const ResourceSet &runtime_resources) {
    RAY_LOG(INFO) << "Runtime resources updated: " << runtime_resources.ToString()
                  << ", last acquired: " << acquired_resources_00.ToString();

    scheduling_resoruces.Release(acquired_resources_00);
    RAY_LOG(INFO) << "After release: " << scheduling_resoruces.DebugString();

    scheduling_resoruces.ReturnBundleResources(acquired_resources_00);
    RAY_LOG(INFO) << "After return: " << scheduling_resoruces.DebugString();

    // acquire again.
    ResourceSet new_required_resources = acquired_resources_00;
    // convert to non-pg resources.
    ASSERT_TRUE(new_required_resources.ReturnBundleResources(acquired_resources_00));
    for (auto &entry : runtime_resources.GetResourceAmountMap()) {
      auto node_available_quality =
          scheduling_resoruces.GetAvailableResources().GetResource(entry.first);
      if (node_available_quality < entry.second) {
        new_required_resources.AddOrUpdateResource(entry.first, node_available_quality);
      } else {
        new_required_resources.AddOrUpdateResource(entry.first, entry.second);
      }
    }

    // 增量预留
    scheduling_resoruces.PrepareBundleResources(group_id, 0, new_required_resources);
    scheduling_resoruces.CommitBundleResources(group_id, 0, new_required_resources);

    // update acquired_resources_00.
    for (auto &entry : new_required_resources.GetResourceAmountMap()) {
      auto pg_resource_name_without_index =
          FormatPlacementGroupResource(entry.first, group_id, -1);
      auto pg_resource_name_with_index =
          FormatPlacementGroupResource(entry.first, group_id, bundle_index);
      acquired_resources_00.AddOrUpdateResource(pg_resource_name_without_index,
                                                entry.second);
      acquired_resources_00.AddOrUpdateResource(pg_resource_name_with_index,
                                                entry.second);
    }

    scheduling_resoruces.Acquire(acquired_resources_00);
  };

  RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();

  {
    ResourceSet runtime_resources_00({"CPU"}, {3.0});
    update_runtime_resources(group_id, 0, runtime_resources_00);
    RAY_LOG(INFO) << "After update: " << scheduling_resoruces.DebugString();
    RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();
  }

  {
    ResourceSet runtime_resources_00({"CPU"}, {5.0});
    update_runtime_resources(group_id, 0, runtime_resources_00);
    RAY_LOG(INFO) << "After update: " << scheduling_resoruces.DebugString();
    RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();
  }

  {
    ResourceSet runtime_resources_00({"CPU"}, {8.0});
    update_runtime_resources(group_id, 0, runtime_resources_00);
    RAY_LOG(INFO) << "After update: " << scheduling_resoruces.DebugString();
    RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();
  }

  {
    ResourceSet runtime_resources_00({"CPU"}, {8.0});
    update_runtime_resources(group_id, 0, runtime_resources_00);
    RAY_LOG(INFO) << "After update: " << scheduling_resoruces.DebugString();
    RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();
  }

  {
    ResourceSet runtime_resources_00({"CPU"}, {100.0});
    update_runtime_resources(group_id, 0, runtime_resources_00);
    RAY_LOG(INFO) << "After update: " << scheduling_resoruces.DebugString();
    RAY_LOG(INFO) << "acquired_resources_00: " << acquired_resources_00.ToString();
  }
}

}  // namespace ray
