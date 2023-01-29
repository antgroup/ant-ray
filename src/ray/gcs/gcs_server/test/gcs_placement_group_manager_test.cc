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

#include <memory>

#include "gtest/gtest.h"
#include "ray/common/asio/instrumented_io_context.h"
#include "ray/gcs/gcs_server/test/gcs_server_test_util.h"
#include "ray/gcs/test/gcs_test_util.h"

namespace ray {

using ::testing::_;
using StatusCallback = std::function<void(Status status)>;

class MockPlacementGroupScheduler : public gcs::GcsPlacementGroupSchedulerInterface {
 public:
  MockPlacementGroupScheduler() = default;

  void ScheduleUnplacedBundles(
      std::shared_ptr<gcs::GcsPlacementGroup> placement_group,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> failure_handler,
      std::function<void(std::shared_ptr<gcs::GcsPlacementGroup>)> success_handler)
      override {
    absl::MutexLock lock(&mutex_);
    placement_groups_.push_back(placement_group);
  }

  MOCK_METHOD1(DestroyPlacementGroupBundleResourcesIfExists,
               void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(MarkScheduleCancelled, void(const PlacementGroupID &placement_group_id));

  MOCK_METHOD1(
      ReleaseUnusedBundles,
      void(const std::unordered_map<NodeID, std::vector<rpc::Bundle>> &node_to_bundles));

  MOCK_METHOD1(Initialize,
               void(const std::unordered_map<
                    PlacementGroupID, std::vector<std::shared_ptr<BundleSpecification>>>
                        &group_to_bundles));

  MOCK_METHOD1(ReturnBundleResources,
               void(const std::shared_ptr<gcs::BundleLocations> bundle_locations));

  MOCK_METHOD1(RemoveBundlesFromPlacementGroup,
               void(const std::shared_ptr<gcs::GcsPlacementGroup> placement_group));

  MOCK_METHOD2(CancelBundleResourcesFromRaylet,
               void(const rpc::Bundle &bundle, StatusCallback callback));

  MOCK_METHOD0(GetCommittedBundleLocationIndex, const gcs::BundleLocationIndex &());

  absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> GetBundlesOnNode(
      const NodeID &node_id) override {
    absl::flat_hash_map<PlacementGroupID, std::vector<int64_t>> bundles;
    bundles[group_on_dead_node_] = bundles_on_dead_node_;
    return bundles;
  }

  int GetPlacementGroupCount() {
    absl::MutexLock lock(&mutex_);
    return placement_groups_.size();
  }

  PlacementGroupID group_on_dead_node_;
  std::vector<int64_t> bundles_on_dead_node_;
  std::vector<std::shared_ptr<gcs::GcsPlacementGroup>> placement_groups_;
  absl::Mutex mutex_;
};

class GcsPlacementGroupManagerTest : public ::testing::Test {
 public:
  GcsPlacementGroupManagerTest()
      : mock_placement_group_scheduler_(new MockPlacementGroupScheduler()) {
    gcs_pub_sub_ = std::make_shared<GcsServerMocker::MockGcsPubSub>(redis_client_);
    gcs_table_storage_ = std::make_shared<gcs::InMemoryGcsTableStorage>(io_service_);
    gcs_resource_manager_ =
        std::make_shared<gcs::GcsResourceManager>(io_service_, nullptr, nullptr, true);
    gcs_placement_group_manager_.reset(new gcs::GcsPlacementGroupManager(
        io_service_, mock_placement_group_scheduler_, gcs_table_storage_,
        *gcs_resource_manager_, gcs_pub_sub_,
        [this](const JobID &job_id) { return job_namespace_table_[job_id]; }));
    for (int i = 1; i <= 10; i++) {
      auto job_id = JobID::FromInt(i);
      job_namespace_table_[job_id] = "";
    }
  }

  void SetUp() override {
    // mock_placement_group_scheduler_.reset(new MockPlacementGroupScheduler());
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<instrumented_io_context::work> work(
          new instrumented_io_context::work(io_service_));
      io_service_.run();
    }));
  }

  void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  // Make placement group registration sync.
  void RegisterPlacementGroup(const ray::rpc::CreatePlacementGroupRequest &request,
                              StatusCallback callback) {
    std::promise<void> promise;
    JobID job_id = JobID::FromBinary(request.placement_group_spec().creator_job_id());
    std::string ray_namespace = job_namespace_table_[job_id];
    gcs_placement_group_manager_->RegisterPlacementGroup(
        std::make_shared<gcs::GcsPlacementGroup>(request, ray_namespace),
        [&callback, &promise](Status status) {
          RAY_CHECK_OK(status);
          callback(status);
          promise.set_value();
        });
    promise.get_future().get();
  }

  void MockPlacementGroupPrepareAndCommitSuccess(
      const std::shared_ptr<gcs::GcsPlacementGroup> &placement_group) {
    const auto &bundle_indexes = placement_group->GetAllBundleIndexes();
    for (int bundle_index : bundle_indexes) {
      placement_group->GetMutableBundle(bundle_index)
          ->set_node_id(NodeID::FromRandom().Binary());
    }
  }

  // We need this to ensure that `MarkSchedulingDone` and `SchedulePendingPlacementGroups`
  // was already invoked when we have invoked `OnPlacementGroupCreationSuccess`.
  void OnPlacementGroupCreationSuccess(
      const std::shared_ptr<gcs::GcsPlacementGroup> &placement_group,
      const bool isPrepareAndCommitSuccess = true) {
    std::promise<void> promise;
    gcs_placement_group_manager_->WaitPlacementGroup(
        placement_group->GetPlacementGroupID(), [&promise](Status status) {
          RAY_CHECK_OK(status);
          promise.set_value();
        });
    if (isPrepareAndCommitSuccess) {
      MockPlacementGroupPrepareAndCommitSuccess(placement_group);
    }
    gcs_placement_group_manager_->OnPlacementGroupCreationSuccess(placement_group);
    // don't check whether the wait placement group callback is executed if still has
    // unplaced bundles.
    if (isPrepareAndCommitSuccess) {
      promise.get_future().get();
    }
  }

  void AddBundlesForPlacementGroup(
      const PlacementGroupID &placement_group_id,
      const ray::rpc::AddPlacementGroupBundlesRequest &request) {
    std::promise<void> promise;
    gcs_placement_group_manager_->AddBundlesForPlacementGroup(
        placement_group_id, request, [&promise](const Status &status) {
          RAY_CHECK(status.ok());
          promise.set_value();
        });
    promise.get_future().get();
  }

  void RemoveBundlesFromPlacementGroup(const PlacementGroupID &placement_group_id,
                                       const std::vector<int> &bundle_indexes) {
    std::promise<void> promise;
    gcs_placement_group_manager_->RemoveBundlesFromPlacementGroup(
        placement_group_id, bundle_indexes, [&promise](const Status &status) {
          RAY_CHECK(status.ok());
          promise.set_value();
        });
    promise.get_future().get();
  }

  void WaitForExpectedPgCount(int expected_count) {
    auto condition = [this, expected_count]() {
      return mock_placement_group_scheduler_->GetPlacementGroupCount() == expected_count;
    };
    EXPECT_TRUE(WaitForCondition(condition, 10 * 1000));
  }

  std::shared_ptr<MockPlacementGroupScheduler> mock_placement_group_scheduler_;
  std::unique_ptr<gcs::GcsPlacementGroupManager> gcs_placement_group_manager_;
  std::unordered_map<JobID, std::string> job_namespace_table_;

 private:
  std::unique_ptr<std::thread> thread_io_service_;
  instrumented_io_context io_service_;
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  std::shared_ptr<gcs::GcsResourceManager> gcs_resource_manager_;
  std::shared_ptr<GcsServerMocker::MockGcsPubSub> gcs_pub_sub_;
  std::shared_ptr<gcs::RedisClient> redis_client_;
};

TEST_F(GcsPlacementGroupManagerTest, TestBasic) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestSchedulingFailed) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 1);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Check that the placement_group is in state `CREATED`.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestGetPlacementGroupIDByName) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  ASSERT_EQ(
      gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
      PlacementGroupID::FromBinary(request.placement_group_spec().placement_group_id()));
}

TEST_F(GcsPlacementGroupManagerTest, TestRemoveNamedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest("test_name");
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // Remove the named placement group.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group->GetPlacementGroupID(),
      [](const Status &status) { ASSERT_TRUE(status.ok()); });
  ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
            PlacementGroupID::Nil());
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeAdd) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If the creation of placement group fails, it will be rescheduled after a short time.
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  WaitForExpectedPgCount(1);
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingPendingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingLeasingPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::PENDING);

  // Placement group is in leasing state.
  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRemovingCreatedPlacementGroup) {
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // We have ensured that this operation is synchronized.
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  const auto &placement_group_id = placement_group->GetPlacementGroupID();
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  EXPECT_CALL(*mock_placement_group_scheduler_, MarkScheduleCancelled(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->RemovePlacementGroup(placement_group_id,
                                                     [](const Status &status) {});
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::REMOVED);

  // Make sure it is not rescheduled
  gcs_placement_group_manager_->SchedulePendingPlacementGroups();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure we can re-remove again.
  gcs_placement_group_manager_->RemovePlacementGroup(
      placement_group_id, [](const Status &status) { ASSERT_TRUE(status.ok()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenNodeDead) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  auto request2 = Mocker::GenCreatePlacementGroupRequest();
  RegisterPlacementGroup(request2, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 2);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  placement_group->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  placement_group->GetMutableBundle(1)->set_node_id(NodeID::FromRandom().Binary());
  mock_placement_group_scheduler_->placement_groups_.pop_back();

  // If a node dies, we will set the bundles above it to be unplaced and reschedule the
  // placement group. The placement group state is set to `RESCHEDULING` and will be
  // scheduled first.
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());

  // Trigger scheduling `RESCHEDULING` placement group.
  auto finished_group = std::make_shared<gcs::GcsPlacementGroup>(
      placement_group->GetPlacementGroupTableData());
  OnPlacementGroupCreationSuccess(finished_group);
  ASSERT_EQ(finished_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  WaitForExpectedPgCount(1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
  const auto &bundles =
      mock_placement_group_scheduler_->placement_groups_[0]->GetBundles();
  EXPECT_TRUE(NodeID::FromBinary(bundles[0]->GetMutableMessage().node_id()).IsNil());
  EXPECT_FALSE(NodeID::FromBinary(bundles[1]->GetMutableMessage().node_id()).IsNil());

  // If `RESCHEDULING` placement group fails to create, we will schedule it again first.
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_.size(), 0);
  gcs_placement_group_manager_->OnPlacementGroupCreationFailed(placement_group);
  WaitForExpectedPgCount(1);
  ASSERT_EQ(mock_placement_group_scheduler_->placement_groups_[0]->GetPlacementGroupID(),
            placement_group->GetPlacementGroupID());
}

TEST_F(GcsPlacementGroupManagerTest,
       DISABLED_TestAutomaticCleanupWhenActorDeadAndJobDead) {
  // Test the scenario where actor dead -> job dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  // gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  // When both job and actor is dead, placement group should be destroyed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest, DISABLED_TestAutomaticCleanupWhenActorAndJobDead) {
  // Test the scenario where job dead -> actor dead.
  const auto job_id = JobID::FromInt(1);
  const auto actor_id = ActorID::Of(job_id, TaskID::Nil(), 0);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ actor_id);
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  // Placement group shouldn't be cleaned when only an actor is killed.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  // gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
  // gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenActorDead(actor_id);
}

TEST_F(GcsPlacementGroupManagerTest, TestAutomaticCleanupWhenOnlyJobDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(1);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(job_id);
}

TEST_F(GcsPlacementGroupManagerTest,
       TestAutomaticCleanupDoNothingWhenDifferentJobIsDead) {
  // Test placement group is cleaned when both actor & job are dead.
  const auto job_id = JobID::FromInt(1);
  const auto different_job_id = JobID::FromInt(3);
  auto request = Mocker::GenCreatePlacementGroupRequest(
      /* name */ "", rpc::PlacementStrategy::SPREAD,
      /* bundles_count */ 2,
      /* cpu_num */ 1.0,
      /* job_id */ job_id,
      /* actor_id */ ActorID::Nil());
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  auto placement_group_id = placement_group->GetPlacementGroupID();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
  // This shouldn't have been called.
  EXPECT_CALL(*mock_placement_group_scheduler_,
              DestroyPlacementGroupBundleResourcesIfExists(placement_group_id))
      .Times(0);
  // This method should ensure idempotency.
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
  gcs_placement_group_manager_->CleanPlacementGroupIfNeededWhenJobDead(different_job_id);
}

// Test the request of add bundles will fail when its pre-state is `Pending`.
TEST_F(GcsPlacementGroupManagerTest, TestDynamicAddBundlesFailed) {
  // Generate create request.
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  // The state is `Pending` now.

  // Generate resize request.
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  auto add_bundles_request =
      Mocker::GenAddPlacementGroupBundlesRequest(placement_group->GetPlacementGroupID());

  gcs_placement_group_manager_->AddBundlesForPlacementGroup(
      placement_group->GetPlacementGroupID(), add_bundles_request,
      [](const Status &status) { RAY_CHECK(status.IsInvalid()); });
}

TEST_F(GcsPlacementGroupManagerTest, TestDynamicAddBundlesSuccessful) {
  // Generate create request.
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  // Generate resize request.
  auto add_bundles_request =
      Mocker::GenAddPlacementGroupBundlesRequest(placement_group->GetPlacementGroupID());
  AddBundlesForPlacementGroup(placement_group->GetPlacementGroupID(),
                              add_bundles_request);
  WaitForExpectedPgCount(1);
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestDynamicRemoveBundles) {
  // Generate create request that has two bundles.
  auto request = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request,
                         [&registered_placement_group_count](const Status &status) {
                           ++registered_placement_group_count;
                         });

  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);

  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();

  // Make sure the request of remove bundles will fail since its state is `Pending` now.
  gcs_placement_group_manager_->RemoveBundlesFromPlacementGroup(
      placement_group->GetPlacementGroupID(), {1},
      [](const Status &status) { RAY_CHECK(status.IsInvalid()); });

  // Make sure the request of remove bundles will fail since the removing bundle index
  // is non-exist.
  gcs_placement_group_manager_->RemoveBundlesFromPlacementGroup(
      placement_group->GetPlacementGroupID(), {2},
      [](const Status &status) { RAY_CHECK(status.IsNotFound()); });

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);

  RemoveBundlesFromPlacementGroup(placement_group->GetPlacementGroupID(), {1});
  WaitForExpectedPgCount(1);
  placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.clear();
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::UPDATING);

  // Validate the state of bundle.
  const auto &all_bundle_indexes = placement_group->GetAllBundleIndexes();
  ASSERT_EQ(all_bundle_indexes.size(), 2);
  const auto &removing_bundle = placement_group->GetAllInvalidBundles();
  ASSERT_EQ(removing_bundle.size(), 1);
  ASSERT_EQ(removing_bundle[0]->Index(), 1);

  OnPlacementGroupCreationSuccess(placement_group);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
}

TEST_F(GcsPlacementGroupManagerTest, TestRayNamespace) {
  auto request1 = Mocker::GenCreatePlacementGroupRequest("test_name");
  job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
  auto request2 = Mocker::GenCreatePlacementGroupRequest(
      "test_name", rpc::PlacementStrategy::SPREAD, 2, 1.0, JobID::FromInt(11));
  auto request3 = Mocker::GenCreatePlacementGroupRequest("test_name");
  {  // Create a placement group in the empty namespace.
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Create a placement group in the empty namespace.
    job_namespace_table_[JobID::FromInt(11)] = "another_namespace";
    std::atomic<int> registered_placement_group_count(0);
    RegisterPlacementGroup(request2, [&registered_placement_group_count](Status status) {
      ++registered_placement_group_count;
    });

    ASSERT_EQ(registered_placement_group_count, 1);
    WaitForExpectedPgCount(1);
    auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
    mock_placement_group_scheduler_->placement_groups_.pop_back();

    OnPlacementGroupCreationSuccess(placement_group);
    ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::CREATED);
    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request2.placement_group_spec().placement_group_id()));
    ASSERT_NE(gcs_placement_group_manager_->GetPlacementGroupIDByName(
                  "test_name", "another_namespace"),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
  {  // Placement groups with the same namespace, different jobs should still collide.
    std::promise<void> promise;
    gcs_placement_group_manager_->RegisterPlacementGroup(
        std::make_shared<gcs::GcsPlacementGroup>(request3, ""),
        [&promise](Status status) {
          ASSERT_FALSE(status.ok());
          promise.set_value();
        });
    promise.get_future().get();

    ASSERT_EQ(gcs_placement_group_manager_->GetPlacementGroupIDByName("test_name", ""),
              PlacementGroupID::FromBinary(
                  request1.placement_group_spec().placement_group_id()));
  }
}

TEST_F(GcsPlacementGroupManagerTest, TestRescheduleWhenTwoNodeBothDead) {
  // step 1: The pg is ready
  auto request1 = Mocker::GenCreatePlacementGroupRequest();
  std::atomic<int> registered_placement_group_count(0);
  RegisterPlacementGroup(request1, [&registered_placement_group_count](Status status) {
    ++registered_placement_group_count;
  });
  ASSERT_EQ(registered_placement_group_count, 1);
  WaitForExpectedPgCount(1);
  auto placement_group = mock_placement_group_scheduler_->placement_groups_.back();
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  OnPlacementGroupCreationSuccess(placement_group);
  WaitForExpectedPgCount(0);

  // step 2: The first node dead
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(0);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());
  WaitForExpectedPgCount(1);

  // step 3: The second node dead
  mock_placement_group_scheduler_->group_on_dead_node_ =
      placement_group->GetPlacementGroupID();
  mock_placement_group_scheduler_->bundles_on_dead_node_.push_back(1);
  gcs_placement_group_manager_->OnNodeDead(NodeID::FromRandom());
  WaitForExpectedPgCount(1);

  // step 4: Bundle 0 reschdule success
  mock_placement_group_scheduler_->placement_groups_.pop_back();
  placement_group->GetMutableBundle(0)->set_node_id(NodeID::FromRandom().Binary());
  OnPlacementGroupCreationSuccess(placement_group, false);

  // step 5: Determine whether bundle1 is being rescheduled
  WaitForExpectedPgCount(1);
  ASSERT_EQ(placement_group->GetState(), rpc::PlacementGroupTableData::RESCHEDULING);
  // step 6: check other operations can be done
  gcs_placement_group_manager_->RemoveBundlesFromPlacementGroup(
      placement_group->GetPlacementGroupID(), {0}, [](Status status) {});
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
