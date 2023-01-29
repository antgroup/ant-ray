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
class GcsResourceRequirePlacementGroupSchdulerPerfTest
    : public GcsResourceSchedulerBaseTest {};

TEST_F(GcsResourceRequirePlacementGroupSchdulerPerfTest, TestSpread) {
  /*
    The address of the simulate cluster node resource file,eg:
    {
      100083253092f4b64e62ff9c2898213705ccf540b40554700fd82b28
      CPU 1
      memory 3100
    }
    {
      011163098024b23075199e61401357995dcad5390335d7b0795a077b
      memory 3250
      CPU   1
      CPU_group_1_a963960e252eb9647fa8ba51976c0b000080 1
      memory_group_1_a963960e252eb9647fa8ba51976c0b000080 50
    }
  */
  std::string data_path = "";
  if (data_path.empty()) {
    return;
  }
  ParseNodeResourcesByFile(data_path);

  int schedule_num = pg_bundle_id_set_.size() * 2;
  schedule_stat_list_.clear();
  schedule_stat_list_.resize(schedule_num);
  single_schedule_time_.clear();
  single_schedule_time_.resize(schedule_num);

  gcs::SchedulingType schedule_type = gcs::SchedulingType::AFFINITY_WITH_BUNDLE;
  for (auto iter = pg_bundle_id_set_.begin(); iter != pg_bundle_id_set_.end(); ++iter) {
    BundleID bundle_id = *iter;
    gcs::GcsActorAffinityWithBundleScheduleContext context(job_id_);
    context.SetPlacementGroupBundleID(bundle_id);
    context.SetPlacementGroupBundleLocationIndex(*pg_bundle_location_index_);
    ResourceSet resource_set(AddPlacementGroupConstraint(
        {{"CPU", 0.01}, {"memory", 1}}, bundle_id.first, bundle_id.second));
    ASSERT_TRUE(ScheduleResource({&resource_set}, schedule_type, &context));

    BundleID bundle_id_2 = BundleID(*iter);
    gcs::GcsActorAffinityWithBundleScheduleContext context2(job_id_);
    context2.SetPlacementGroupBundleID(bundle_id_2);
    context2.SetPlacementGroupBundleLocationIndex(*pg_bundle_location_index_);
    ResourceSet resource_set2(AddPlacementGroupConstraint(
        {{"CPU", 0.01}, {"memory", 1}}, bundle_id_2.first, bundle_id_2.second));
    ASSERT_TRUE(ScheduleResource({&resource_set2}, schedule_type, &context2));
  }
  OutputScheduleTime();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}