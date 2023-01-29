#include "data_reader.h"
#include "gtest/gtest.h"
#include "mock_reader.h"

namespace ray {
namespace streaming {
void GenRandomChannelIdVector(std::vector<ObjectID> &input_ids, int n) {
  for (int i = 0; i < n; ++i) {
    input_ids.push_back(ObjectID::FromRandom());
  }
}

class MockReaderTest : public ::testing::Test {
 public:
  MockReaderTest() : runtime_context(new RuntimeContext()) {}

 protected:
  virtual void SetUp() override {
    runtime_context->config_.SetQueueType("mock_queue");
    mock_reader.reset(new MockReader(runtime_context));
  }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockReader> mock_reader;
  std::vector<ObjectID> input_ids;
};

TEST_F(MockReaderTest, test_init_channel) {
  int channel_num = 5;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_reader->Init(input_ids);
  mock_reader->InitChannel();
  EXPECT_EQ(mock_reader->GetChannelMap().size(), channel_num);
  mock_reader->InitChannel();
  EXPECT_EQ(mock_reader->GetChannelMap().size(), channel_num);
  int channel_num_delta = 5;
  GenRandomChannelIdVector(input_ids, channel_num_delta);
  mock_reader->ResetUnreadyIds(input_ids);
  mock_reader->InitChannel();
  EXPECT_EQ(mock_reader->GetChannelMap().size(), channel_num + channel_num_delta);
}

TEST_F(MockReaderTest, test_check_bundle) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_reader->Init(input_ids);
  std::unordered_map<ObjectID, uint64_t> last_msg_map;
  last_msg_map[input_ids[0]] = 0;
  mock_reader->ResetLastMessageIds(last_msg_map);
  std::shared_ptr<StreamingReaderBundle> message(new StreamingReaderBundle());
  message->from = input_ids[0];
  std::shared_ptr<StreamingMessageBundleMeta> meta(
      new StreamingMessageBundleMeta(0, 1, 1, 1, StreamingMessageBundleType::Bundle));
  message->meta = meta;
  // Message bundle in [1,1] will be fine.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::OkBundle);

  meta.reset(
      new StreamingMessageBundleMeta(0, 1, 1, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;

  // First barrier in [1,1] will be accepcted when expected message id 1.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::OkBundle);

  message->message_id = 10;
  meta.reset(
      new StreamingMessageBundleMeta(0, 9, 10, 2, StreamingMessageBundleType::Bundle));
  message->meta = meta;

  // Message bundle in [9, 10] will be thrown when expected message id 10.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::BundleToBeThrown);

  last_msg_map[input_ids[0]] = 20;
  mock_reader->ResetLastMessageIds(last_msg_map);
  meta.reset(
      new StreamingMessageBundleMeta(0, 16, 25, 10, StreamingMessageBundleType::Bundle));
  message->meta = meta;

  // Message bundle in [16, 25] will be splited when expected message id 20.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::BundleToBeSplit);

  meta.reset(
      new StreamingMessageBundleMeta(0, 20, 20, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;
  // Message barrier in [20, 20] will be accepted.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::OkBundle);

  meta.reset(
      new StreamingMessageBundleMeta(0, 21, 21, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;
  // Message barrier in [21, 21] will be dropout when expected message id is 20.
  EXPECT_EQ(mock_reader->CheckBundle(message), BundleCheckStatus::BundleToBeThrown);
}

TEST_F(MockReaderTest, test_valid_bundle) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_reader->Init(input_ids);
  std::unordered_map<ObjectID, uint64_t> last_msg_map;
  last_msg_map[input_ids[0]] = 0;
  mock_reader->ResetLastMessageIds(last_msg_map);
  std::shared_ptr<StreamingReaderBundle> message(new StreamingReaderBundle());
  message->from = input_ids[0];
  std::shared_ptr<StreamingMessageBundleMeta> meta(
      new StreamingMessageBundleMeta(0, 1, 1, 1, StreamingMessageBundleType::Bundle));
  message->meta = meta;
  // Message bundle in [1,1] will be fine.
  EXPECT_TRUE(mock_reader->IsValidBundle(message));

  message->message_id = 10;
  meta.reset(
      new StreamingMessageBundleMeta(0, 9, 10, 2, StreamingMessageBundleType::Bundle));
  message->meta = meta;

  // Message bundle in [9, 10] will be thrown when expected message id 10.
  EXPECT_TRUE(!mock_reader->IsValidBundle(message));

  last_msg_map[input_ids[0]] = 20;
  mock_reader->ResetLastMessageIds(last_msg_map);
  meta.reset(
      new StreamingMessageBundleMeta(0, 16, 25, 10, StreamingMessageBundleType::Bundle));
  message->meta = meta;

  // Message bundle in [16, 25] will be splited when expected message id 20.
  EXPECT_TRUE(mock_reader->IsValidBundle(message));

  meta.reset(
      new StreamingMessageBundleMeta(0, 20, 20, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;
  // Message barrier in [20, 20] will be accepted.
  EXPECT_TRUE(mock_reader->IsValidBundle(message));

  meta.reset(
      new StreamingMessageBundleMeta(0, 21, 21, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;
  // Message barrier in [21, 21] will be dropout when expected message id is 20.
  EXPECT_TRUE(!mock_reader->IsValidBundle(message));

  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
  meta.reset(
      new StreamingMessageBundleMeta(0, 15, 15, 1, StreamingMessageBundleType::Barrier));
  message->meta = meta;
  EXPECT_TRUE(mock_reader->IsValidBundle(message));

  meta.reset(
      new StreamingMessageBundleMeta(0, 11, 15, 5, StreamingMessageBundleType::Bundle));
  message->meta = meta;
  EXPECT_TRUE(mock_reader->IsValidBundle(message));
}

TEST_F(MockReaderTest, test_get_target_barrier_count) {
  // Total 16 channels, 12 non-cyclic channels and 4 cyclic channels.
  int channel_num = 16;
  GenRandomChannelIdVector(input_ids, channel_num);
  std::vector<StreamingQueueInitialParameter> init_params;
  for (int i = 0; i < 12; i++) {
    init_params.push_back({RandomActorID(), nullptr, nullptr, false, true});
  }
  for (int i = 0; i < 4; i++) {
    init_params.push_back({RandomActorID(), nullptr, nullptr, true, true});
  }
  mock_reader->Init(input_ids, init_params);
  EXPECT_EQ(12, mock_reader->GetTargetBarrierCount(input_ids));
}
}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
