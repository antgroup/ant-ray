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

#include <fstream>
#include <string>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/gcs_resource_schedule_context.h"
#include "ray/gcs/gcs_server/gcs_resource_scheduler.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using ::testing::_;
class GcsResourceSchedulerBaseTest : public ::testing::Test {
 public:
  void SetUp() override {
    gcs_resource_manager_ = std::make_shared<gcs::GcsResourceManager>(
        io_service_, nullptr, nullptr, /*redis_broadcast_enabled=*/true);
    gcs_resource_scheduler_ = std::make_shared<gcs::GcsResourceScheduler>(
        *gcs_resource_manager_,
        /*is_node_schedulable_callback=*/
        [this](const gcs::NodeContext &, const gcs::ResourceScheduleContext *) {
          return is_node_schedulable_;
        });
    pg_bundle_location_index_ = std::make_shared<gcs::BundleLocationIndex>();
  }

  void TearDown() override {
    gcs_resource_scheduler_.reset();
    gcs_resource_manager_.reset();
    pg_bundle_location_index_.reset();
  }

  void AddNodeResources(const NodeID &node_id, const std::string &resource_name,
                        double resource_value) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_id(node_id.Binary());
    (*node->mutable_resources_total())[resource_name] = resource_value;
    gcs_resource_manager_->OnNodeAdd(*node);
  }

  void AddNodeResources(const std::string &node_id_string,
                        const std::unordered_map<std::string, double> &resources) {
    const NodeID node_id = NodeID::FromBinary(UniqueID::FromHex(node_id_string).Binary());
    AddNodeResources(node_id, resources);
  }

  void AddNodeResources(const NodeID &node_id,
                        const std::unordered_map<std::string, double> &resources) {
    auto node = Mocker::GenNodeInfo();
    node->mutable_basic_gcs_node_info()->set_node_id(node_id.Binary());
    for (auto &iter : resources) {
      if (iter.second < 0) {
        continue;
      }
      (*node->mutable_resources_total())[iter.first] = iter.second;
      PlacementGroupID pg_id;
      int64_t bundle_index;
      if (ParseBundleResource(iter.first, &pg_id, &bundle_index)) {
        std::shared_ptr<gcs::BundleLocations> bundle_locations =
            std::make_shared<gcs::BundleLocations>();
        BundleID bundle_id = std::make_pair(pg_id, bundle_index);
        pg_bundle_id_set_.insert(bundle_id);
        std::unordered_map<std::string, double> unit_resource;
        (*bundle_locations)[bundle_id] = std::make_pair(
            node_id, Mocker::GenBundleCreation(pg_id, bundle_index, unit_resource));
        pg_bundle_location_index_->AddBundleLocations(pg_id, bundle_locations);
      }
    }
    gcs_resource_manager_->OnNodeAdd(*node);
  }

  void Trim(std::string &str) {
    std::string blanks("\f\v\r\t\n ");
    str.erase(0, str.find_first_not_of(blanks));
    str.erase(str.find_last_not_of(blanks) + 1);
  }

  void ParseNodeResourcesByFile(const std::string &file_path) {
    std::ifstream in_file;
    in_file.open(file_path);
    ASSERT_TRUE(in_file.is_open());
    ParseNodeResources(in_file);
    in_file.close();
  }
  void ParseNodeResources(const std::string &date) {
    std::istringstream in_str(date);
    ParseNodeResources(in_str);
  }

  void ParseNodeResources(std::istream &in) {
    std::string line;
    std::unordered_map<std::string, double> resources;
    std::string node_id;
    while (getline(in, line))  // line中不包括每行的换行符
    {
      Trim(line);
      if (line == "{") {
        getline(in, node_id);
        Trim(node_id);
        resources.clear();
      } else if (line == "}") {
        AddNodeResources(node_id, resources);
      } else if (line.size() > 0) {
        const auto &pair = ParseResourceLabel(line);
        resources[pair.first] = pair.second;
      }
    }
  }

  std::pair<std::string, double> ParseResourceLabel(const std::string resource_str) {
    auto pos = resource_str.find(" ");
    std::string resource_key = resource_str.substr(0, pos);
    std::string resource_value_str = resource_str.substr(pos + 1);
    Trim(resource_key);
    Trim(resource_value_str);
    return std::make_pair(resource_key, atof(resource_value_str.c_str()));
  }

  bool ScheduleResource(const std::vector<const ResourceSet *> &required_resources_list,
                        gcs::SchedulingType scheduling_type,
                        gcs::ResourceScheduleContext *context) {
    int64_t start_time = current_sys_time_us();
    if (schedule_num_ == 0) {
      schedule_start_time_ = start_time;
    }
    gcs_resource_scheduler_->Schedule(required_resources_list, scheduling_type, context);
    RAY_CHECK(context->selected_nodes.size() == 1)
        << "No." << schedule_num_
        << " required_resources:" << required_resources_list[0]->ToString()
        << "Current cluster resources = " << gcs_resource_manager_->ToString();
    auto selected_node_id = context->selected_nodes[0];
    if (!selected_node_id.IsNil()) {
      // Acquire the resources from the selected node.
      RAY_CHECK(gcs_resource_manager_->AcquireResources(selected_node_id,
                                                        *(required_resources_list[0])));
    }
    single_schedule_time_[schedule_num_] = current_sys_time_us() - start_time;
    schedule_stat_list_[schedule_num_] = context->stat_;
    schedule_num_++;
    return true;
  }

  void OutputScheduleTime() {
    // Output result
    auto total_time = current_sys_time_us() - schedule_start_time_;
    std::cout << total_time << std::endl;
    for (int64_t i = 0; i < schedule_num_; i++) {
      const auto &schedule_stat = schedule_stat_list_[i];
      std::cout << i + 1 << ' ' << single_schedule_time_[i] << ' '
                << schedule_stat.GetFindFeasibleNodesStrictlyCost() << ' '
                << schedule_stat.GetAcquireResourcesCost() << ' '
                << schedule_stat.GetPrioritizedNodesCost() << ' '
                << schedule_stat.GetPrioritizedNodesByScorerCost() << ' '
                << schedule_stat.GetSelectNodeCost() << std::endl;
    }
  }

  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<gcs::GcsResourceScheduler> gcs_resource_scheduler_;
  std::shared_ptr<gcs::BundleLocationIndex> pg_bundle_location_index_;
  absl::flat_hash_set<BundleID> pg_bundle_id_set_;
  bool is_node_schedulable_ = true;
  JobID job_id_ = JobID::FromInt(1);
  // for performance test
  std::vector<gcs::ScheduleStat> schedule_stat_list_;
  std::vector<int64_t> single_schedule_time_;
  int64_t schedule_num_ = 0;
  int64_t schedule_start_time_;

 protected:
  instrumented_io_context io_service_;
};

}  // namespace ray