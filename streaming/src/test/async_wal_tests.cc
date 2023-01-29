#include "gtest/gtest.h"
#include "test/test_utils.h"
#include "wal.h"

using namespace ray;
using namespace ray::streaming;

TEST(AsyncWALTest, TestWrite) {
  std::vector<ObjectID> channel_ids{ObjectID::FromRandom(), ObjectID::FromRandom(),
                                    ObjectID::FromRandom()};
  AsyncWAL wal(channel_ids);

  const uint64_t checkpoint_id = 0;
  auto bundle_count = 200;
  for (int channel_count = 0; channel_count < 3; channel_count++) {
    for (int i = 0; i < bundle_count; ++i) {
      std::list<StreamingMessagePtr> message_list;
      for (int j = 0; j < i + 1; ++j) {
        uint8_t *data = new uint8_t[j + 1];
        data[0] = j;
        StreamingMessagePtr message =
            util::MakeMessagePtr(data, j + 1, j + 1, StreamingMessageType::Message);
        message_list.push_back(message);
        delete[] data;
      }
      StreamingMessageBundle bundle(message_list, 0, 1, 1,
                                    StreamingMessageBundleType::Bundle);
      uint64_t bundle_size = bundle.ClassBytesSize();
      std::shared_ptr<uint8_t> bundle_bytes(new uint8_t[bundle_size]);
      bundle.ToBytes(bundle_bytes.get());
      StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(
          bundle_bytes.get(), bundle_bytes.get() + kMessageBundleHeaderSize);
      wal.Write(checkpoint_id, bundle_ptr, channel_ids[channel_count]);
    }
  }
  EXPECT_EQ(200, wal.Wait(checkpoint_id, channel_ids[0], bundle_count));
  EXPECT_EQ(200, wal.Wait(checkpoint_id, channel_ids[1], bundle_count));
  EXPECT_EQ(200, wal.Wait(checkpoint_id, channel_ids[2], bundle_count));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
