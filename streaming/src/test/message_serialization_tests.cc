#include <cstring>
#include <list>
#include <memory>
#include <string>

#include "common/buffer.h"
#include "gtest/gtest.h"
#include "message/message.h"
#include "message/message_bundle.h"
#include "ray/util/logging.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

TEST(StreamingSerializationTest, streaming_message_serialization_test) {
  uint8_t data[] = {9, 1, 3};
  StreamingMessagePtr message =
      util::MakeMessagePtr(data, 3, 7, StreamingMessageType::Message);
  uint32_t message_length = message->Size();
  auto *bytes = new uint8_t[message_length];
  StreamingMessagePtr new_message = StreamingMessage::FromBytes(message->Data());
  EXPECT_EQ(std::memcmp(new_message->Payload(), data, 3), 0);
  delete[] bytes;
}

TEST(StreamingSerializationTest, streaming_message_empty_bundle_serialization_test) {
  for (int i = 0; i < 10; ++i) {
    StreamingMessageBundle bundle(i, i, i);
    uint64_t bundle_size = bundle.ClassBytesSize();
    uint8_t *bundle_bytes = new uint8_t[bundle_size];
    bundle.ToBytes(bundle_bytes);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(
        bundle_bytes, bundle_bytes + kMessageBundleHeaderSize);

    EXPECT_EQ(bundle.ClassBytesSize(), bundle_ptr->ClassBytesSize());
    EXPECT_EQ(bundle.GetMessageListSize(), bundle_ptr->GetMessageListSize());
    EXPECT_EQ(bundle.GetBundleType(), bundle_ptr->GetBundleType());
    EXPECT_EQ(bundle.GetFirstMessageId(), bundle_ptr->GetFirstMessageId());
    EXPECT_EQ(bundle.GetLastMessageId(), bundle_ptr->GetLastMessageId());

    StreamingMessageBundleMetaPtr meta_ptr = bundle_ptr;
    EXPECT_EQ(meta_ptr->ClassBytesSize(), bundle_ptr->ClassBytesSize());

    std::list<StreamingMessagePtr> s_message_list;
    bundle_ptr->GetMessageList(s_message_list);
    std::list<StreamingMessagePtr> b_message_list;
    bundle.GetMessageList(b_message_list);
    EXPECT_EQ(b_message_list.size(), 0);
    EXPECT_EQ(s_message_list.size(), 0);

    delete[] bundle_bytes;
  }
}
TEST(StreamingSerializationTest, streaming_message_barrier_bundle_serialization_test) {
  for (int i = 0; i < 10; ++i) {
    uint8_t data[] = {1, 2, 3, 4};
    uint32_t data_size = 4;
    uint32_t head_size = sizeof(uint64_t);
    uint64_t checkpoint_id = 777;
    std::shared_ptr<uint8_t> ptr(new uint8_t[data_size + head_size],
                                 std::default_delete<uint8_t[]>());
    // move checkpint_id in head of barrier data
    std::memcpy(ptr.get(), &checkpoint_id, head_size);
    std::memcpy(ptr.get() + head_size, data, data_size);
    StreamingMessagePtr message = util::MakeMessagePtr(data, head_size + data_size, i,
                                                       StreamingMessageType::Barrier);
    std::list<StreamingMessagePtr> message_list;
    message_list.push_back(message);
    // message list will be moved to bundle member
    std::list<StreamingMessagePtr> message_list_cpy(message_list);

    StreamingMessageBundle bundle(message_list_cpy, i, i, i,
                                  StreamingMessageBundleType::Barrier);
    uint64_t bundle_size = bundle.ClassBytesSize();
    uint8_t *bundle_bytes = new uint8_t[bundle_size];
    bundle.ToBytes(bundle_bytes);
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(
        bundle_bytes, bundle_bytes + kMessageBundleHeaderSize);

    EXPECT_TRUE(bundle.ClassBytesSize() == bundle_ptr->ClassBytesSize());
    EXPECT_TRUE(bundle.GetMessageListSize() == bundle_ptr->GetMessageListSize());
    EXPECT_TRUE(bundle.GetBundleType() == bundle_ptr->GetBundleType());
    EXPECT_TRUE(bundle.GetFirstMessageId() == bundle_ptr->GetFirstMessageId());
    EXPECT_TRUE(bundle.GetLastMessageId() == bundle_ptr->GetLastMessageId());
    std::list<StreamingMessagePtr> s_message_list;
    bundle_ptr->GetMessageList(s_message_list);
    EXPECT_TRUE(s_message_list.size() == message_list.size());
    auto m_item = message_list.back();
    auto s_item = s_message_list.back();
    EXPECT_TRUE(s_item->Size() == m_item->Size());
    EXPECT_TRUE(s_item->GetMessageType() == m_item->GetMessageType());
    EXPECT_TRUE(s_item->GetMessageId() == m_item->GetMessageId());
    EXPECT_TRUE(s_item->PayloadSize() == m_item->PayloadSize());
    EXPECT_TRUE(
        std::memcmp(s_item->Payload(), m_item->Payload(), m_item->PayloadSize()) == 0);
    EXPECT_TRUE(*(s_item.get()) == (*(m_item.get())));

    delete[] bundle_bytes;
  }
}

TEST(StreamingSerializationTest, streaming_message_bundle_serialization_test) {
  for (int k = 0; k <= 1000; k++) {
    std::list<StreamingMessagePtr> message_list;

    for (int i = 0; i < 100; ++i) {
      uint8_t *data = new uint8_t[i + 1];
      data[0] = i;
      StreamingMessagePtr message =
          util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);
      message_list.push_back(message);
      delete[] data;
    }
    StreamingMessageBundle messageBundle(message_list, 0, 1, 1,
                                         StreamingMessageBundleType::Bundle);
    size_t message_length = messageBundle.ClassBytesSize();
    uint8_t *bytes = new uint8_t[message_length];
    messageBundle.ToBytes(bytes);

    StreamingMessageBundlePtr bundle_ptr =
        StreamingMessageBundle::FromBytes(bytes, bytes + kMessageBundleHeaderSize);
    EXPECT_EQ(bundle_ptr->ClassBytesSize(), message_length);
    std::list<StreamingMessagePtr> s_message_list;
    bundle_ptr->GetMessageList(s_message_list);
    EXPECT_TRUE(bundle_ptr->operator==(messageBundle));
    StreamingMessageBundleMetaPtr bundle_meta_ptr =
        StreamingMessageBundleMeta::FromBytes(bytes);

    EXPECT_EQ(bundle_meta_ptr->GetBundleType(), bundle_ptr->GetBundleType());
    EXPECT_EQ(bundle_meta_ptr->GetFirstMessageId(), bundle_ptr->GetFirstMessageId());
    EXPECT_EQ(bundle_meta_ptr->GetLastMessageId(), bundle_ptr->GetLastMessageId());
    EXPECT_EQ(bundle_meta_ptr->GetMessageBundleTs(), bundle_ptr->GetMessageBundleTs());
    EXPECT_EQ(bundle_meta_ptr->GetMessageListSize(), bundle_ptr->GetMessageListSize());
    delete[] bytes;
  }
}

TEST(StreamingSerializationTest, streaming_message_bundle_equal_test) {
  std::list<StreamingMessagePtr> message_list;
  std::list<StreamingMessagePtr> message_list_same;
  std::list<StreamingMessagePtr> message_list_cpy;
  for (int i = 0; i < 100; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    for (int j = 0; j < i + 1; ++j) {
      data[j] = i;
    }
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);
    message_list.push_back(message);
    message_list_cpy.push_front(message);
    delete[] data;
  }
  for (int i = 0; i < 100; ++i) {
    uint8_t *data = new uint8_t[i + 1];
    for (int j = 0; j < i + 1; ++j) {
      data[j] = i;
    }
    StreamingMessagePtr message =
        util::MakeMessagePtr(data, i + 1, i + 1, StreamingMessageType::Message);
    message_list_same.push_back(message);
    delete[] data;
  }
  StreamingMessageBundle message_bundle(message_list, 0, 1, 1,
                                        StreamingMessageBundleType::Bundle);
  StreamingMessageBundle message_bundle_same(message_list_same, 0, 1, 1,
                                             StreamingMessageBundleType::Bundle);
  StreamingMessageBundle message_bundle_reverse(message_list_cpy, 0, 1, 1,
                                                StreamingMessageBundleType::Bundle);
  EXPECT_TRUE(message_bundle_same == message_bundle);
  EXPECT_FALSE(message_bundle_reverse == message_bundle);
  size_t message_length = message_bundle.ClassBytesSize();
  uint8_t *bytes = new uint8_t[message_length];
  message_bundle.ToBytes(bytes);

  StreamingMessageBundlePtr bundle_ptr =
      StreamingMessageBundle::FromBytes(bytes, bytes + kMessageBundleHeaderSize);
  EXPECT_EQ(bundle_ptr->ClassBytesSize(), message_length);
  std::list<StreamingMessagePtr> s_message_list;
  bundle_ptr->GetMessageList(s_message_list);
  EXPECT_TRUE(bundle_ptr->operator==(message_bundle));
  delete[] bytes;
}

TEST(StreamingSerializationTest, streaming_message_in_bufferpool_test) {
  uint64_t buffer_pool_size = 1024;
  uint64_t min_size = 128;
  auto buffer_pool = std::make_shared<BufferPool>(buffer_pool_size, min_size);

  std::list<StreamingMessagePtr> message_list;
  // Put 6 messages into bufferpool, each message is 32bytes:
  // kMessageHeaderSize(16bytes) + message content(16bytes)
  int message_count = 6;
  int message_length = 32;
  for (int i = 1; i <= message_count; i++) {
    uint8_t data[message_length];
    memset(data, i, message_length);
    ray::streaming::MemoryBuffer buffer =
        ray::streaming::util::CopyToBufferPool(buffer_pool, data, message_length);
    message_list.push_back(StreamingMessage::FromBytes(buffer.Data(), buffer.Size(), i,
                                                       StreamingMessageType::Message));
  }

  auto bundle = std::make_shared<StreamingMessageBundle>(
      message_list, current_sys_time_ms(), 1, message_count,
      StreamingMessageBundleType::Bundle, message_count * message_length);
  EXPECT_EQ(message_count * message_length, bundle->DataBuffers().DataSize());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
