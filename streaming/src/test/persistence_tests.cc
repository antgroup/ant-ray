#include <cstring>
#include <string>

#include "gtest/gtest.h"
#include "reliability/persistence.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

TEST(PersistenceTest, StoreAndLoad) {
  std::shared_ptr<BundlePersistence> persistence(new BundlePersistence());
  auto channel_id = ObjectID::FromRandom();
  auto bundle_count = 200;
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
    persistence->Store(0, bundle_ptr, channel_id);
  }
  persistence.reset(new BundlePersistence());

  std::vector<StreamingMessageBundleMetaPtr> bundle_vec;
  persistence->Load(0, bundle_vec, channel_id);
  EXPECT_EQ(bundle_vec.size(), bundle_count);
  for (int i = 0; i < bundle_count; ++i) {
    EXPECT_EQ(bundle_vec[i]->GetMessageListSize(), i + 1);
  }
  std::vector<StreamingMessageBundleMetaPtr> bundle_vec2;
  persistence->Load(0, bundle_vec2, channel_id);
  EXPECT_EQ(bundle_vec2.size(), 0);
  persistence->Clear(channel_id, 0);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
