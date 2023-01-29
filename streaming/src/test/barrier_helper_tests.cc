#include "gtest/gtest.h"
#include "ray/common/id.h"
#include "reliability/barrier_helper.h"

using namespace ray::streaming;
using namespace ray;

class StreamingBarrierHelperTest : public ::testing::Test {
 public:
  void SetUp() { barrier_helper_.reset(new StreamingBarrierHelper()); }
  void TearDown() { barrier_helper_.release(); }

 protected:
  std::unique_ptr<StreamingBarrierHelper> barrier_helper_;
  const ObjectID random_id = ray::ObjectID::FromRandom();
  const ObjectID another_random_id = ray::ObjectID::FromRandom();
};

TEST_F(StreamingBarrierHelperTest, MessageIdByBarrierId) {
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 0);
  uint64_t seq_id = 0;
  uint64_t init_seq_id = 10;
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetMsgIdByBarrierId(random_id, 1, seq_id));

  barrier_helper_->SetMsgIdByBarrierId(random_id, 1, init_seq_id);

  ASSERT_EQ(StreamingStatus::QueueIdNotFound,
            barrier_helper_->GetMsgIdByBarrierId(another_random_id, 1, seq_id));

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetMsgIdByBarrierId(random_id, 1, seq_id));
  ASSERT_EQ(init_seq_id, seq_id);

  barrier_helper_->SetMsgIdByBarrierId(random_id, 2, init_seq_id + 1);

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetMsgIdByBarrierId(random_id, 2, seq_id));
  ASSERT_EQ(init_seq_id + 1, seq_id);

  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 2);
  barrier_helper_->ReleaseBarrierMapSeqIdById(1);
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 1);
  barrier_helper_->ReleaseAllBarrierMapSeqId();
  ASSERT_EQ(barrier_helper_->GetBarrierMapSize(), 0);
}

TEST_F(StreamingBarrierHelperTest, BarrierIdByLastMessageId) {
  uint64_t barrier_id = 0;
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id));

  barrier_helper_->SetBarrierIdByLastMessageId(random_id, 1, 10);

  ASSERT_EQ(
      StreamingStatus::QueueIdNotFound,
      barrier_helper_->GetBarrierIdByLastMessageId(another_random_id, 1, barrier_id));

  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id));
  ASSERT_EQ(barrier_id, 10);

  barrier_helper_->SetBarrierIdByLastMessageId(random_id, 1, 11);
  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
  ASSERT_EQ(barrier_id, 10);
  ASSERT_EQ(StreamingStatus::OK,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
  ASSERT_EQ(barrier_id, 11);
  ASSERT_EQ(StreamingStatus::NoSuchItem,
            barrier_helper_->GetBarrierIdByLastMessageId(random_id, 1, barrier_id, true));
}

TEST_F(StreamingBarrierHelperTest, CheckpointId) {
  uint64_t checkpoint_id = static_cast<uint64_t>(-1);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 0);
  barrier_helper_->SetCurrentMaxCheckpointIdInQueue(random_id, 2);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 2);
  barrier_helper_->SetCurrentMaxCheckpointIdInQueue(random_id, 3);
  barrier_helper_->GetCurrentMaxCheckpointIdInQueue(random_id, checkpoint_id);
  ASSERT_EQ(checkpoint_id, 3);
}

TEST(BarrierHelper, barrier_map_get_set) {
  StreamingBarrierHelper barrier_helper;
  ray::ObjectID channel_id = ray::ObjectID::FromRandom();
  uint64_t seq_id;
  auto status = barrier_helper.GetMsgIdByBarrierId(channel_id, 0, seq_id);
  EXPECT_TRUE(status == StreamingStatus::NoSuchItem);
  EXPECT_TRUE(barrier_helper.GetBarrierMapSize() == 0);

  seq_id = 1;
  barrier_helper.SetMsgIdByBarrierId(channel_id, 0, seq_id);

  uint64_t fetched_seq_id;
  status = barrier_helper.GetMsgIdByBarrierId(channel_id, 0, fetched_seq_id);
  EXPECT_TRUE(status == StreamingStatus::OK);
  EXPECT_TRUE(fetched_seq_id == seq_id);
  EXPECT_TRUE(barrier_helper.GetBarrierMapSize() == 1);

  uint64_t fetched_no_barrier_id;
  status = barrier_helper.GetMsgIdByBarrierId(channel_id, 1, fetched_no_barrier_id);
  EXPECT_TRUE(status == StreamingStatus::NoSuchItem);

  ray::ObjectID other_channel_id = ray::ObjectID::FromRandom();
  status = barrier_helper.GetMsgIdByBarrierId(other_channel_id, 0, fetched_seq_id);
  EXPECT_TRUE(status == StreamingStatus::QueueIdNotFound);

  EXPECT_TRUE(barrier_helper.Contains(0));
  EXPECT_TRUE(!barrier_helper.Contains(1));

  seq_id = 10;
  barrier_helper.SetMsgIdByBarrierId(channel_id, 1, seq_id);
  EXPECT_TRUE(barrier_helper.Contains(1));
  EXPECT_TRUE(barrier_helper.GetBarrierMapSize() == 2);

  barrier_helper.ReleaseBarrierMapSeqIdById(0);
  EXPECT_TRUE(!barrier_helper.Contains(0));
  EXPECT_TRUE(barrier_helper.GetBarrierMapSize() == 1);

  seq_id = 20;
  barrier_helper.SetMsgIdByBarrierId(channel_id, 2, seq_id);
  std::vector<uint64_t> barrier_id_vec;
  barrier_helper.GetAllBarrier(barrier_id_vec);
  EXPECT_TRUE(barrier_id_vec.size() == 2);
  barrier_helper.ReleaseAllBarrierMapSeqId();
  EXPECT_TRUE(barrier_helper.GetBarrierMapSize() == 0);
}

TEST(BarrierHelper, barrier_checkpoint_mapping) {
  StreamingBarrierHelper barrier_helper;
  ray::ObjectID channel_id = ray::ObjectID::FromRandom();
  uint64_t seq_id = 1;
  uint64_t barrier_id = 0;
  barrier_helper.SetMsgIdByBarrierId(channel_id, barrier_id, seq_id);
  uint64_t checkpoint_id = 100;
  barrier_helper.MapBarrierToCheckpoint(barrier_id, checkpoint_id);
  uint64_t fetched_checkpoint_id;
  barrier_helper.GetCheckpointIdByBarrierId(barrier_id, fetched_checkpoint_id);
  EXPECT_TRUE(fetched_checkpoint_id == checkpoint_id);

  barrier_id = 2;
  barrier_helper.MapBarrierToCheckpoint(barrier_id, checkpoint_id);
  barrier_helper.GetCheckpointIdByBarrierId(barrier_id, fetched_checkpoint_id);
  EXPECT_TRUE(fetched_checkpoint_id == checkpoint_id);
  barrier_helper.ReleaseBarrierMapCheckpointByBarrierId(barrier_id);

  auto status1 = barrier_helper.GetCheckpointIdByBarrierId(0, fetched_checkpoint_id);
  auto status2 = barrier_helper.GetCheckpointIdByBarrierId(2, fetched_checkpoint_id);
  EXPECT_TRUE(status1 == status2 && status1 == StreamingStatus::NoSuchItem);
}

TEST(BarrierHelper, logging_mode_barriers) {
  StreamingBarrierHelper barrier_helper;
  ray::ObjectID channel_id = ray::ObjectID::FromRandom();
  barrier_helper.AddLoggingModeBarrier(channel_id, 1, 100);
  barrier_helper.AddLoggingModeBarrier(channel_id, 2, 200);
  auto pair = barrier_helper.GetCurrentLoggingModeCheckpointId(channel_id);
  EXPECT_EQ(2, pair.first);
  EXPECT_EQ(200, pair.second);
  std::list<std::pair<uint64_t, uint64_t>> removed;
  barrier_helper.ReleaseLoggingModeCheckpoints(channel_id, 2, removed);
  EXPECT_EQ(removed.size(), 2);
  EXPECT_EQ(removed.front().first, 1);
  EXPECT_EQ(removed.back().first, 2);
  /// the barrier id list should be empty.
  pair = barrier_helper.GetCurrentLoggingModeCheckpointId(channel_id);
  EXPECT_EQ(INVALID_BARRIER_ID, pair.first);
  EXPECT_EQ(INVALID_BARRIER_ID, pair.second);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
