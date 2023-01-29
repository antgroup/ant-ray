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
#include "ray/gcs/gcs_server/test/gcs_resource_scheduler_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {
using ::testing::_;
class GcsResourceSchedulerPerfTest : public GcsResourceSchedulerBaseTest {
 public:
  /// Generate test data
  /// Because gtest redirects the output, you need to copy this function to the new cpp to
  /// run
  void GenDataSet(const std::string &data_path) {
    // num: Number of data
    // base_value: The base value of the random numbers
    // floating_range: Floating range of random numbers
    int num, base_value, floating_range;
    std::cin >> num >> base_value >> floating_range;
    floating_range += 1;
    srand(time(NULL));
    std::ofstream file(data_path);
    file << num << std::endl;
    for (int i = 0; i < num; i++) {
      // CPU, GPU, MEM
      file << rand() % floating_range + base_value << ' '
           << rand() % floating_range + base_value << ' '
           << rand() % floating_range + base_value << std::endl;
    }
    file.close();
  }

  /// Generate test config, the config should be the address of the node
  /// data file and the address of the requested data file.
  /// Because gtest redirects the output, you need to copy this function to the new cpp to
  /// run.
  void GenDataConfig(const std::string &config_path, const std::string &node_data_path,
                     const std::string &req_data_path) {
    std::ofstream file(config_path);
    file << node_data_path << std::endl;
    file << req_data_path << std::endl;
    file.close();
  }

  void TestSchedulerPerf(const std::string &config_path,
                         const gcs::SchedulingType &scheduling_type) {
    if (config_path.empty()) {
      return;
    }

    size_t num;
    std::ifstream in_file;
    const std::string cpu_resource = "CPU";
    const std::string gpu_resource = "GPU";
    const std::string mem_resource = "MEM";
    in_file.open(config_path);
    std::string node_data_path, req_data_path;
    in_file >> node_data_path;
    in_file >> req_data_path;
    in_file.close();

    // Add node resources.
    in_file.open(node_data_path);
    in_file >> num;
    for (size_t i = 1; i <= num; i++) {
      const auto &node_id = NodeID::FromRandom();
      double num_cpu, num_gpu, num_mem;
      in_file >> num_cpu >> num_gpu >> num_mem;
      AddNodeResources(node_id, cpu_resource, num_cpu);
      AddNodeResources(node_id, gpu_resource, num_gpu);
      AddNodeResources(node_id, mem_resource, num_mem);
    }
    in_file.close();

    // Add required resources.
    std::vector<ResourceSet> required_resources_list;
    std::unordered_map<std::string, double> resource_map;
    in_file.open(req_data_path);
    in_file >> num;
    for (size_t i = 1; i <= num; i++) {
      double num_cpu, num_gpu, num_mem;
      in_file >> num_cpu >> num_gpu >> num_mem;
      resource_map[cpu_resource] = num_cpu;
      resource_map[gpu_resource] = num_gpu;
      resource_map[mem_resource] = num_mem;
      required_resources_list.emplace_back(resource_map);
    }
    in_file.close();

    // Scheduling nodes
    std::vector<const ResourceSet *> required_resources_ref_list(1);
    std::vector<gcs::ScheduleStat> schedule_stat_list(num);
    std::vector<int64_t> single_time(num);
    int64_t tot_time = current_sys_time_us();
    for (size_t i = 0; i < num; i++) {
      auto &resource_set = required_resources_list[i];
      required_resources_ref_list[0] = &resource_set;
      auto schedule_context = std::make_unique<gcs::GcsActorScheduleContext>(job_id_);
      single_time[i] = current_sys_time_us();
      ASSERT_TRUE(gcs_resource_scheduler_->Schedule(
          required_resources_ref_list, scheduling_type, schedule_context.get()));

      RAY_CHECK(schedule_context->selected_nodes.size() == 1);
      auto selected_node_id = schedule_context->selected_nodes[0];
      if (!selected_node_id.IsNil()) {
        // Acquire the resources from the selected node.
        RAY_CHECK(
            gcs_resource_manager_->AcquireResources(selected_node_id, resource_set));
      }
      single_time[i] = current_sys_time_us() - single_time[i];
      schedule_stat_list[i] = schedule_context->stat_;
    }
    tot_time = current_sys_time_us() - tot_time;

    // Output result
    std::cout << tot_time << std::endl;
    for (size_t i = 0; i < num; i++) {
      const auto &schedule_stat = schedule_stat_list[i];
      std::cout << i + 1 << ' ' << single_time[i] << ' '
                << schedule_stat.GetFindFeasibleNodesStrictlyCost() << ' '
                << schedule_stat.GetAcquireResourcesCost() << ' '
                << schedule_stat.GetPrioritizedNodesCost() << ' '
                << schedule_stat.GetPrioritizedNodesByScorerCost() << ' '
                << schedule_stat.GetSelectNodeCost() << std::endl;
    }
  }
};

TEST_F(GcsResourceSchedulerPerfTest, TestSpread) {
  // The address of the configuration file, the file should be the address of the node
  // data file and the address of the requested data file
  std::string config_path = "";
  TestSchedulerPerf(config_path, gcs::SchedulingType::SPREAD);
}
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}