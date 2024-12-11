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

#include "ray/gcs/gcs_server/gcs_virtual_cluster_manager.h"

// clang-format off
#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"
#include "mock/ray/pubsub/publisher.h"
#include "mock/ray/pubsub/subscriber.h"

// clang-format on

namespace ray {

class GcsVirtualClusterManagerTest : public ::testing::Test {
 public:
  GcsVirtualClusterManagerTest() {
    gcs_publisher_ = std::make_unique<gcs::GcsPublisher>(
        std::make_unique<ray::pubsub::MockPublisher>());
    gcs_table_storage_ = std::make_unique<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_virtual_cluster_manager_ = std::make_unique<gcs::GcsVirtualClusterManager>(
        *gcs_table_storage_, *gcs_publisher_);
  }

  instrumented_io_context io_service_;
  std::unique_ptr<gcs::GcsPublisher> gcs_publisher_;
  std::unique_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::unique_ptr<gcs::GcsVirtualClusterManager> gcs_virtual_cluster_manager_;
};

TEST_F(GcsVirtualClusterManagerTest, TestBasic) {}

}  // namespace ray
