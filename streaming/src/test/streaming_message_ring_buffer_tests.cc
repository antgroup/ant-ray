#include <unistd.h>

#include <iostream>
#include <set>
#include <thread>

#include "gtest/gtest.h"
#include "message.h"
#include "ray/util/logging.h"
#include "ring_buffer.h"
#include "streaming.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

size_t data_n = 1000000;
TEST(StreamingRingBufferTest, streaming_message_ring_buffer_test) {
  for (int k = 0; k < 10000; ++k) {
    StreamingRingBuffer<StreamingMessagePtr> r_buf(3, StreamingRingBufferType::SPSC_LOCK);
    for (int i = 0; i < 5; ++i) {
      uint8_t data[] = {1, 1, 3};
      data[0] = i;
      StreamingMessagePtr message =
          util::MakeMessagePtr(data, 3, i, StreamingMessageType::Message);
      EXPECT_EQ(r_buf.Push(message), true);
      size_t ith = i >= 3 ? 3 : (i + 1);
      EXPECT_EQ(r_buf.Size(), ith);
    }
    int th = 2;

    while (!r_buf.IsEmpty()) {
      StreamingMessagePtr messagePtr = r_buf.Front();
      r_buf.Pop();
      EXPECT_EQ(messagePtr->PayloadSize(), 3);
      EXPECT_EQ(*(messagePtr->Payload()), th++);
    }
  }
}

TEST(StreamingRingBufferTest, spsc_test) {
  size_t m_num = 1000;
  StreamingRingBuffer<StreamingMessagePtr> r_buf(m_num, StreamingRingBufferType::SPSC);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(std::move(message));
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto &msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->Payload(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(StreamingRingBufferTest, mutex_test) {
  size_t m_num = data_n;
  StreamingRingBuffer<StreamingMessagePtr> r_buf(m_num,
                                                 StreamingRingBufferType::SPSC_LOCK);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->Payload(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(StreamingRingBufferTest, spsc_list_test) {
  size_t m_num = 1000 * sizeof(size_t);
  StreamingRingBuffer<StreamingMessagePtr> r_buf(m_num,
                                                 StreamingRingBufferType::SPSC_LIST);
  std::thread thread([&r_buf]() {
    for (size_t j = 0; j < data_n; ++j) {
      StreamingMessagePtr message =
          util::MakeMessagePtr(reinterpret_cast<uint8_t *>(&j), sizeof(size_t), j,
                               StreamingMessageType::Message);
      while (r_buf.IsFull()) {
      }
      r_buf.Push(message);
    }
  });
  size_t count = 0;
  while (count < data_n) {
    while (r_buf.IsEmpty()) {
    }
    auto &msg = r_buf.Front();
    EXPECT_EQ(std::memcmp(msg->Payload(), &count, sizeof(size_t)), 0);
    r_buf.Pop();
    count++;
  }
  thread.join();
  EXPECT_EQ(count, data_n);
}

TEST(RingBufferImplLockFree, item_test) {
  RingBufferImplLockFree<StreamingMessagePtr>::RingBufferItem item1;
  StreamingMessagePtr ptr = std::make_shared<StreamingMessage>(nullptr, 0, false);
  StreamingMessagePtr ptr2 = ptr;
  item1 = std::move(ptr);
  EXPECT_EQ(ptr.use_count(), 0);
  EXPECT_EQ(ptr2.use_count(), 2);
  EXPECT_EQ(item1.Get().get(), ptr2.get());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
