#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "gtest/gtest.h"
#include "queue.h"
#include "queue_handler.h"
#include "ray/common/status.h"
#include "ray/util/util.h"
#include "streaming.h"
#include "streaming_queue_generated.h"
#include "test/test_utils.h"

using namespace ray;
using namespace ray::streaming;

class WriterQueueTest : public ::testing::Test {
 public:
  void SetUp() {
    queue_id_ = ray::ObjectID::FromRandom();
    actor_id_ = RandomActorID();
    peer_actor_id_ = RandomActorID();
  }

  virtual void TearDown() {}

  void MockNotify(std::shared_ptr<WriterQueue> queue, uint64_t target) {
    std::shared_ptr<NotificationMessage> msg = std::make_shared<NotificationMessage>(
        peer_actor_id_, actor_id_, queue_id_, target, 0);
    queue->OnNotify(msg);
  }

 protected:
  ray::ObjectID queue_id_;
  ray::ActorID actor_id_;
  ray::ActorID peer_actor_id_;
};

TEST_F(WriterQueueTest, Push) {
  WriterQueue queue(queue_id_, actor_id_, peer_actor_id_, false, 1024,
                    std::make_shared<MockTransport>());
  uint8_t meta[10];
  uint8_t data[90];
  EXPECT_TRUE(queue.Push(meta, 10, data, 90, current_sys_time_ms(), 0, 0) ==
              StreamingStatus::OK);
  uint8_t meta2[10];
  uint8_t data2[1024];
  EXPECT_FALSE(queue.Push(meta2, 10, data2, 1024, current_sys_time_ms(), 0, 0) ==
               StreamingStatus::OK);

  EXPECT_EQ(100, queue.QueueSize());
  // Should got a NullQueueItem
  QueueItem item = queue.PopProcessed();
  EXPECT_EQ(100, item.DataSize());
  EXPECT_EQ(0, queue.QueueSize());
}

TEST_F(WriterQueueTest, PushMetaAndData) {
  WriterQueue queue(queue_id_, actor_id_, peer_actor_id_, false, 1024,
                    std::make_shared<MockTransport>());
  uint8_t meta[64];
  uint8_t data[100];
  EXPECT_TRUE(queue.Push(meta, 64, data, 100, current_sys_time_ms(), 0, 0) ==
              StreamingStatus::OK);
  uint8_t meta2[64];
  uint8_t data2[1024];
  EXPECT_FALSE(queue.Push(meta2, 64, data2, 1024, current_sys_time_ms(), 0, 0) ==
               StreamingStatus::OK);

  EXPECT_EQ(64 + 100, queue.QueueSize());
  // Shoule got a NullQueueItem
  QueueItem item = queue.PopProcessed();
  EXPECT_EQ(64 + 100, item.DataSize());
  EXPECT_EQ(0, queue.QueueSize());
}

TEST_F(WriterQueueTest, PushAndSend) {
  WriterQueue queue(queue_id_, actor_id_, peer_actor_id_, false, 1024,
                    std::make_shared<MockTransport>());
  for (int i = 0; i < 10; i++) {
    uint8_t meta[10];
    uint8_t data[90];
    memset(data, i, 90);
    EXPECT_TRUE(queue.Push(meta, 10, data, 90, current_sys_time_ms(), 2 * i, 2 * i + 1) ==
                StreamingStatus::OK);
    EXPECT_EQ(0, queue.PendingCount());
  }
  EXPECT_EQ(10, queue.ProcessedCount());

  uint8_t meta[10];
  uint8_t data[90];
  EXPECT_FALSE(queue.Push(meta, 10, data, 90, current_sys_time_ms(), 0, 0) ==
               StreamingStatus::OK);

  // Pop itmes when receive notify
  const int read_step = 4;
  for (int i = 0; i < read_step; i++) {
    queue.PopProcessed();
  }
  EXPECT_EQ(0, queue.PendingCount());
  EXPECT_EQ(6, queue.ProcessedCount());
}

TEST_F(WriterQueueTest, EvictTest) {
  auto queue = std::make_shared<WriterQueue>(queue_id_, actor_id_, peer_actor_id_, false,
                                             1024, std::make_shared<MockTransport>());
  queue->CreateInternalBuffer(nullptr, 2048);
  uint8_t mock_meta[16];
  uint8_t mock_data[16];
  // The maximum item count that the queue can hold is 1024/(16+16) = 32
  for (int i = 0; i <= 32; i++) {
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(queue->GetBufferPool(), mock_data, 16);
    StreamingStatus status =
        queue->Push(mock_meta, 16, data_buffer.Data(), data_buffer.Size(),
                    current_sys_time_ms(), 2 * i, 2 * i + 1);
    if (i == 32) {
      EXPECT_EQ(StreamingStatus::FullChannel, status);
    } else {
      EXPECT_EQ(StreamingStatus::OK, status);
    }
  }

  uint64_t target = 20;
  MockNotify(queue, target);
  queue->SetQueueEvictionLimit(target);
  // require 11 items capacity, each item is 16+16, but only 10 items(0-9) can be evicted.
  EXPECT_EQ(queue->Evict(11 * (16 + 16)), 10);

  for (int i = 0; i < 33; i++) {
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(queue->GetBufferPool(), mock_data, 16);
    StreamingStatus status =
        queue->Push(mock_meta, 16, data_buffer.Data(), data_buffer.Size(),
                    current_sys_time_ms(), 2 * i, 2 * i + 1);
    if (i >= 10) {
      EXPECT_EQ(StreamingStatus::FullChannel, status);
    } else {
      EXPECT_EQ(StreamingStatus::OK, status);
    }
  }
}

TEST_F(WriterQueueTest, LargeItemTest) {
  uint64_t queue_size = 1024;
  uint64_t oom_limit = queue_size - kMessageBundleHeaderSize;
  uint64_t data_size = oom_limit + 1;
  auto queue =
      std::make_shared<WriterQueue>(queue_id_, actor_id_, peer_actor_id_, false,
                                    queue_size, std::make_shared<MockTransport>());
  queue->CreateInternalBuffer(nullptr, 128);
  auto buffer_pool = queue->GetBufferPool();
  EXPECT_EQ(buffer_pool->GetOOMLimit(), oom_limit);

  uint8_t mock_meta[kMessageBundleHeaderSize];
  MemoryBuffer data_buffer;
  EXPECT_EQ(buffer_pool->GetBufferBlockedTimeout(data_size, &data_buffer, 10),
            StreamingStatus::OutOfMemory);
  buffer_pool->GetDisposableBuffer(data_size, &data_buffer);
  EXPECT_TRUE(buffer_pool->IsDisposableBuffer(data_buffer.Data()));

  StreamingStatus status =
      queue->Push(mock_meta, kMessageBundleHeaderSize, data_buffer.Data(),
                  data_buffer.Size(), current_sys_time_ms(), 0, 1);
  EXPECT_EQ(status, StreamingStatus::OK);
  EXPECT_EQ(queue->ProcessedCount(), 1);
}

class ReaderQueueTest : public ::testing::Test {
 public:
  void SetUp() {
    queue_id_ = ray::ObjectID::FromRandom();
    actor_id_ = RandomActorID();
    peer_actor_id_ = RandomActorID();
  }

  virtual void TearDown() {}

 protected:
  ray::ObjectID queue_id_;
  ray::ActorID actor_id_;
  ray::ActorID peer_actor_id_;
};

TEST_F(ReaderQueueTest, PendingCountTest) {
  ReaderQueue queue(queue_id_, actor_id_, peer_actor_id_, false, nullptr);

  EXPECT_EQ(queue.PendingCount(), 0);

  QueueItem item1(1, nullptr, 0, nullptr, 0, 0, 0, 0, 0);
  QueueItem item2(2, nullptr, 0, nullptr, 0, 0, 0, 0, 0);
  QueueItem item3(3, nullptr, 0, nullptr, 0, 0, 0, 0, 0);
  queue.OnData(item1);
  EXPECT_EQ(queue.PendingCount(), 1);
  queue.OnData(item2);
  EXPECT_EQ(queue.PendingCount(), 2);
  queue.OnData(item3);
  EXPECT_EQ(queue.PendingCount(), 3);
  EXPECT_EQ(queue.ProcessedCount(), 0);

  queue.PopPending();
  EXPECT_EQ(queue.PendingCount(), 2);
  EXPECT_EQ(queue.ProcessedCount(), 1);
  queue.PopPending();
  EXPECT_EQ(queue.PendingCount(), 1);
  EXPECT_EQ(queue.ProcessedCount(), 2);
  queue.PopPending();
  EXPECT_EQ(queue.PendingCount(), 0);
  EXPECT_EQ(queue.ProcessedCount(), 3);
}

class CyclicWriterQueueTest : public WriterQueueTest {};

TEST_F(CyclicWriterQueueTest, EvictNoESTest) {
  auto queue =
      std::make_shared<CyclicWriterQueue>(queue_id_, actor_id_, peer_actor_id_, false,
                                          1024, std::make_shared<MockTransport>());
  queue->CreateInternalBuffer(nullptr, 2048);
  uint8_t mock_meta[16];
  uint8_t mock_data[16];
  // The maximum item count that the queue can hold is 1024/(16+16) = 32
  for (int i = 0; i <= 32; i++) {
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(queue->GetBufferPool(), mock_data, 16);
    StreamingStatus status =
        queue->Push(mock_meta, 16, data_buffer.Data(), data_buffer.Size(),
                    current_sys_time_ms(), 2 * i, 2 * i + 1);
    if (i == 32) {
      EXPECT_EQ(StreamingStatus::FullChannel, status);
    } else {
      EXPECT_EQ(StreamingStatus::OK, status);
    }
  }
  EXPECT_EQ(queue->QueueSize(), 1024);
}

TEST_F(CyclicWriterQueueTest, EvictWithESTest) {
  ElasticBufferConfig config(true);
  auto elasticbuffer = std::make_shared<ElasticBuffer<QueueItem>>(
      config, UpstreamQueueMessageHandler::ser_func,
      UpstreamQueueMessageHandler::deser_func);
  auto queue = std::make_shared<CyclicWriterQueue>(
      queue_id_, actor_id_, peer_actor_id_, false, 1024,
      std::make_shared<MockTransport>(), elasticbuffer);
  queue->CreateInternalBuffer(nullptr, 2048);

  uint8_t mock_meta[16];
  uint8_t mock_data[16];
  for (int i = 0; i <= 32; i++) {
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(queue->GetBufferPool(), mock_data, 16);
    StreamingStatus status =
        queue->Push(mock_meta, 16, data_buffer.Data(), data_buffer.Size(),
                    current_sys_time_ms(), 2 * i, 2 * i + 1);
    EXPECT_EQ(StreamingStatus::OK, status);
  }

  auto pull_msg =
      std::make_shared<PullRequestMessage>(peer_actor_id_, actor_id_, queue_id_, 0, 0);
  boost::asio::io_service service;
  auto promise = std::make_shared<PromiseWrapper>();
  queue::flatbuf::StreamingQueueError error_code;
  queue->OnPull(pull_msg, service,
                [&error_code, &promise](std::shared_ptr<PullResponseMessage> response) {
                  error_code = response->ErrorCode();
                  promise->Notify(ray::Status::OK());
                });

  RAY_IGNORE_EXPR(promise->Wait());
  EXPECT_EQ(error_code, queue::flatbuf::StreamingQueueError::OK);
}

TEST_F(CyclicWriterQueueTest, GetItemsLargeThanTest) {
  ElasticBufferConfig config(true);
  auto elasticbuffer = std::make_shared<ElasticBuffer<QueueItem>>(
      config, UpstreamQueueMessageHandler::ser_func,
      UpstreamQueueMessageHandler::deser_func);
  auto queue = std::make_shared<CyclicWriterQueue>(
      queue_id_, actor_id_, peer_actor_id_, false, 1024,
      std::make_shared<MockTransport>(), elasticbuffer);
  queue->CreateInternalBuffer(nullptr, 2048);

  uint8_t mock_meta[16];
  uint8_t mock_data[16];
  for (int i = 0; i <= 100; i++) {
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(queue->GetBufferPool(), mock_data, 16);
    StreamingStatus status =
        queue->Push(mock_meta, 16, data_buffer.Data(), data_buffer.Size(),
                    current_sys_time_ms(), 2 * i, 2 * i + 1);
    EXPECT_EQ(StreamingStatus::OK, status);
    queue->MockSend();
  }

  std::vector<QueueItem> queue_item_vec;
  int count = queue->GetItemsLargeThan(102, queue_item_vec);
  EXPECT_EQ(count, 50);
  int index = 51;
  for (auto &item : queue_item_vec) {
    EXPECT_EQ(index + 1, item.SeqId());
    index++;
  }
}

TEST_F(CyclicWriterQueueTest, GetBundleLargerThanTest) {
  StreamingConfig config;
  std::shared_ptr<Config> transfer_config = std::make_shared<Config>();
  STREAMING_LOG(INFO) << "config.GetStreamingBufferPoolSize(): "
                      << config.GetStreamingBufferPoolSize();
  transfer_config->Set(ConfigEnum::BUFFER_POOL_SIZE, config.GetStreamingBufferPoolSize());
  transfer_config->Set(ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE,
                       config.GetStreamingBufferPoolMinBufferSize());
  transfer_config->Set(ConfigEnum::ENABLE_COLLOCATE, false);
  ObjectID channel_id = ObjectID::FromRandom();
  ProducerChannelInfo producer_channel_info;
  producer_channel_info.queue_size = config.GetStreamingBufferPoolSize();
  producer_channel_info.channel_id = channel_id;
  producer_channel_info.parameter = {RandomActorID(), nullptr, nullptr, true, false};
  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>(std::string(), TransportType::MEMORY);
  upstream_handler->SetOutTransport(
      channel_id, std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
  upstream_handler->Start();
  STREAMING_LOG(INFO) << "queue_size: " << producer_channel_info.queue_size;
  StreamingQueueProducer producer(transfer_config, producer_channel_info,
                                  upstream_handler);
  producer.CreateTransferChannel();

  int bundle_count = 100;
  for (int i = 0; i < bundle_count; ++i) {
    std::list<StreamingMessagePtr> message_list;
    // Each bundle store 10 messages
    for (int j = 0; j < 10; ++j) {
      uint8_t *data = new uint8_t[j + 1];
      data[0] = j;
      StreamingMessagePtr message =
          util::MakeMessagePtr(data, j + 1, 10 * i + j, StreamingMessageType::Message);
      message_list.push_back(message);
      delete[] data;
    }
    StreamingMessageBundlePtr bundle_ptr2 = std::make_shared<StreamingMessageBundle>(
        message_list, 0, i * 10, (i + 1) * 10 - 1, StreamingMessageBundleType::Bundle);
    EXPECT_EQ(StreamingStatus::OK, producer.ProduceItemToChannel(bundle_ptr2));
  }

  std::vector<StreamingMessageBundlePtr> bundle_vec;
  producer.GetBundleLargerThan(100, bundle_vec);
  EXPECT_EQ(90, bundle_vec.size());
  for (uint32_t i = 0; i < bundle_vec.size(); i++) {
    auto bundle_ptr = bundle_vec[i];
    EXPECT_EQ(bundle_ptr->GetFirstMessageId(), (i + 10) * 10);
    EXPECT_EQ(bundle_ptr->GetLastMessageId(), (i + 10 + 1) * 10 - 1);
    auto message_list = bundle_ptr->GetMessageList();
    int j = 0;
    for (auto &msg_ptr : message_list) {
      uint8_t *data = msg_ptr->Payload();
      EXPECT_EQ(data[0], j);
      j++;
    }
  }
  upstream_handler->Stop();
}

TEST(StreamingQueueMockTest, MemoryTransportTest) {
  STREAMING_LOG(INFO) << "MemoryTransportTest";
  auto func =
      [](std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
        STREAMING_LOG(INFO) << "recv thread";
        STREAMING_CHECK(buffers[0]->Data() != nullptr);
        uint8_t *byte = (uint8_t *)buffers[0]->Data();
        EXPECT_EQ(100, *byte);
        STREAMING_LOG(INFO) << "recv done";
      };

  std::shared_ptr<Transport> transport =
      std::make_shared<SimpleMemoryTransport>(func, nullptr);
  STREAMING_LOG(INFO) << "MemoryTransportTest 1";
  std::thread send_thread([transport] {
    STREAMING_LOG(INFO) << "send thread";
    uint8_t byte = 100;
    auto buffer =
        std::make_unique<ray::streaming::LocalMemoryBuffer>(&byte, 1, true, true);
    transport->Send(std::move(buffer));
    STREAMING_LOG(INFO) << "send thread done";
  });

  if (send_thread.joinable()) send_thread.join();
}

TEST(StreamingQueueMockTest, HandlerTest) {
  // auto up_handler =
  //     std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
  // up_handler->Start();
  // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  // up_handler->Stop();
  // up_handler->Start();
  // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  // up_handler->Stop();

  boost::asio::io_service service;
  boost::asio::io_service::work work(service);
  std::thread t = std::thread([&service]() {
    service.restart();
    service.run();
  });
  service.post([]() { STREAMING_LOG(INFO) << "callback 1"; });
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  service.stop();
  t.join();
  t = std::thread([&service]() {
    service.restart();
    service.run();
  });
  service.post([]() { STREAMING_LOG(INFO) << "callback 2"; });
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  service.stop();
  t.join();
}

constexpr uint64_t QUEUE_SIZE = 1024 * 1024 * 1024;
class MockIntegrationTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    STREAMING_LOG(INFO) << "MockIntegrationTest SetUp";

    upstream_handler_ = std::make_shared<UpstreamQueueMessageHandler>(
        std::string(), TransportType::MEMORY);
    upstream_handler_->Start();
    downstream_handler_ = std::make_shared<DownstreamQueueMessageHandler>(
        std::string(), TransportType::MEMORY);
    downstream_handler_->Start();

    queue_id_ = ray::ObjectID::FromRandom();
    PrepareAddQueue(queue_id_, upstream_handler_, downstream_handler_);
    const StreamingQueueInitialParameter param{ActorID::Nil(), nullptr, nullptr, false,
                                               true};
    upstream_handler_->CreateUpstreamQueue(queue_id_, param, QUEUE_SIZE, QUEUE_SIZE,
                                           false);
    downstream_handler_->CreateDownstreamQueue(queue_id_, param);
  }

  virtual void TearDown() {
    upstream_handler_->Stop();
    downstream_handler_->Stop();
  }

  void PrepareAddQueue(
      ray::ObjectID queue_id,
      std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler,
      std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler) {
    up_transport_ = std::make_shared<SimpleMemoryTransport>(
        [downstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
          downstream_handler->DispatchMessageAsync(buffers);
        },
        [downstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers)
            -> std::shared_ptr<ray::streaming::LocalMemoryBuffer> {
          return downstream_handler->DispatchMessageSync(buffers[0]);
        });
    down_transport_ = std::make_shared<SimpleMemoryTransport>(
        [upstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
          upstream_handler->DispatchMessageAsync(buffers);
        },
        [upstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers)
            -> std::shared_ptr<ray::streaming::LocalMemoryBuffer> {
          return upstream_handler->DispatchMessageSync(buffers[0]);
        });

    upstream_handler->SetOutTransport(queue_id, up_transport_);
    downstream_handler->SetOutTransport(queue_id, down_transport_);
  }

  std::shared_ptr<UpstreamQueueMessageHandler> GetUpstreamHandler() {
    return upstream_handler_;
  }
  std::shared_ptr<DownstreamQueueMessageHandler> GetDownstreamHandler() {
    return downstream_handler_;
  }
  std::shared_ptr<ReaderQueue> GetDownstreamQueue() {
    return downstream_handler_->GetDownQueue(queue_id_);
  }
  std::shared_ptr<WriterQueue> GetUpstreamQueue() {
    return upstream_handler_->GetUpQueue(queue_id_);
  }
  ray::ObjectID GetQueueId() { return queue_id_; }

 public:
  std::thread upstream_thread_;
  std::thread downstream_thread_;
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler_;
  std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler_;
  ray::ObjectID queue_id_;
  std::shared_ptr<Transport> up_transport_;
  std::shared_ptr<Transport> down_transport_;
};

TEST_F(MockIntegrationTest, QueueHandlerTest) {
  STREAMING_LOG(INFO) << "QueueHandlerTest";
  const uint64_t message_count = 1000;
  std::thread writer_thread([this]() {
    STREAMING_LOG(INFO) << "WriterThread";
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    auto upstreaming_queue = GetUpstreamQueue();
    for (uint64_t i = 0; i < message_count; i++) {
      while (!upstreaming_queue->IsInitialized()) {
        STREAMING_LOG(WARNING) << "Writer Queue not ready.";
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      }
      uint8_t meta[10];
      uint8_t data[1000];
      memset(data, i % 100, 1000);
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(upstreaming_queue->GetBufferPool(), data, 1000);
      ASSERT_TRUE(upstreaming_queue->Push(meta, 10, data_buffer.Data(),
                                          data_buffer.Size(), current_sys_time_ms(),
                                          3 * i, 3 * i + 2) == StreamingStatus::OK);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    STREAMING_LOG(INFO) << "WriterThread done";
  });

  std::thread reader_thread([this, message_count]() {
    uint64_t count = 0;
    auto downstreaming_queue = GetDownstreamQueue();
    while (true) {
      uint8_t *data = nullptr;
      uint64_t timeout_ms = 1000;
      QueueItem item = downstreaming_queue->PopPendingBlockTimeout(timeout_ms * 1000);
      if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
        STREAMING_LOG(INFO) << "GetQueueItem timeout.";
        data = nullptr;
      } else {
        data = item.Buffer()->Data();
      }
      if (data == nullptr) {
        continue;
      }

      EXPECT_EQ(data[0], count % 100);
      EXPECT_EQ(data[999], count % 100);
      STREAMING_LOG(DEBUG) << "[Reader] GetQueueItem " << count;
      count++;
      if (count >= message_count) {
        break;
      }
    }

    STREAMING_LOG(INFO) << "ReaderThread done";
    EXPECT_EQ(count, message_count);
  });

  writer_thread.join();
  reader_thread.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST_F(MockIntegrationTest, MultiArgsTest) {
  STREAMING_LOG(INFO) << "MultiArgsTest";
  const uint64_t message_count = 1000;
  std::thread writer_thread([this]() {
    STREAMING_LOG(INFO) << "WriterThread";
    auto upstreaming_queue = GetUpstreamQueue();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    while (!upstreaming_queue->IsInitialized()) {
      STREAMING_LOG(WARNING) << "Writer Queue not ready.";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    for (uint64_t i = 0; i < message_count; i++) {
      uint8_t meta[100];
      memset(meta, i % 100, 100);
      uint8_t data[1000];
      memset(data, i % 100, 1000);
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(upstreaming_queue->GetBufferPool(), data, 1000);
      ASSERT_TRUE(upstreaming_queue->Push(meta, 100, data_buffer.Data(),
                                          data_buffer.Size(), current_sys_time_ms(),
                                          3 * i, 3 * i + 2) == StreamingStatus::OK);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    STREAMING_LOG(INFO) << "WriterThread done";
  });

  std::thread reader_thread([this, message_count]() {
    uint64_t count = 0;
    auto downstreaming_queue = GetDownstreamQueue();
    while (true) {
      uint8_t *meta = nullptr;
      uint8_t *data = nullptr;
      // uint32_t data_size = 0;
      QueueItem item = downstreaming_queue->PopPendingBlockTimeout(1000 * 1000);
      if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
        STREAMING_LOG(INFO) << "GetQueueItem timeout.";
        meta = nullptr;
        data = nullptr;
      } else {
        meta = item.MetaBuffer()->Data();
        data = item.Buffer()->Data();
      }
      if (meta == nullptr || data == nullptr) {
        continue;
      }

      EXPECT_EQ(meta[0], count % 100);
      EXPECT_EQ(meta[99], count % 100);
      EXPECT_EQ(data[0], count % 100);
      EXPECT_EQ(data[999], count % 100);
      STREAMING_LOG(DEBUG) << "[Reader] GetQueueItem done.";
      // if (count % 2 == 1) reader->NotifyConsumedItem(queue_id, 3 * count + 2);
      count++;
      if (count >= message_count) {
        break;
      }
    }

    STREAMING_LOG(INFO) << "ReaderThread done";
    EXPECT_EQ(count, message_count);
  });

  writer_thread.join();
  reader_thread.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST(StreamingQueueMockTest, PromiseWrapperTest) {
  std::shared_ptr<PromiseWrapper> promise = std::make_shared<PromiseWrapper>();
  std::thread notify_thread([&promise] {
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    promise->Notify(ray::Status::OK());
  });

  ray::Status st = promise->Wait();
  if (notify_thread.joinable()) {
    notify_thread.join();
  }
}

TEST(StreamingQueueMockTest, PromiseWrapperTimeoutTest) {
  std::shared_ptr<PromiseWrapper> promise_timeout = std::make_shared<PromiseWrapper>();
  std::thread timeout_thread([&promise_timeout] {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    promise_timeout->Notify(ray::Status::OK());
  });

  uint64_t begin = current_sys_time_ms();
  ray::Status st = promise_timeout->WaitFor(2000);
  uint64_t end = current_sys_time_ms();

  if (timeout_thread.joinable()) {
    timeout_thread.join();
  }

  STREAMING_LOG(INFO) << "end-begin = " << end - begin;
  EXPECT_LE(end - begin, 2000);
  EXPECT_GE(end - begin, 1000);
}

TEST(StreamingQueueMockTest, TestPullResponseMessageError) {
  STREAMING_LOG(INFO) << "error DATA_LOST "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             queue::flatbuf::StreamingQueueError::DATA_LOST);
  STREAMING_LOG(INFO) << "error OK "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             queue::flatbuf::StreamingQueueError::OK);

  ray::ObjectID queue_id = ray::ObjectID::FromRandom();

  ray::ActorID actor_id = RandomActorID();
  ray::ActorID peer_actor_id = RandomActorID();
  STREAMING_LOG(WARNING) << "actor_id: " << actor_id;
  STREAMING_LOG(WARNING) << "peer_actor_id: " << peer_actor_id;

  bool is_upstream_first_pull = 0;
  PullResponseMessage msg(actor_id, peer_actor_id, queue_id, 0, 0, 0,
                          queue::flatbuf::StreamingQueueError::DATA_LOST,
                          is_upstream_first_pull);
  std::unique_ptr<ray::streaming::LocalMemoryBuffer> buffer = msg.ToBytes();

  std::shared_ptr<PullResponseMessage> result_msg =
      PullResponseMessage::FromBytes(buffer->Data());
  STREAMING_LOG(INFO) << "result_msg error: "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             result_msg->ErrorCode());
  EXPECT_EQ(queue::flatbuf::StreamingQueueError::DATA_LOST, result_msg->ErrorCode());
}

TEST(StreamingQueueMockTest, TestGetLastMsgIdError) {
  STREAMING_LOG(INFO) << "error DATA_LOST "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST);
  STREAMING_LOG(INFO) << "error OK "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             queue::flatbuf::StreamingQueueError::OK);

  ray::ObjectID queue_id = ray::ObjectID::FromRandom();

  ray::ActorID actor_id = RandomActorID();
  ray::ActorID peer_actor_id = RandomActorID();
  STREAMING_LOG(WARNING) << "actor_id: " << actor_id;
  STREAMING_LOG(WARNING) << "peer_actor_id: " << peer_actor_id;

  GetLastMsgIdRspMessage msg(actor_id, peer_actor_id, queue_id, 100, 100,
                             queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST);
  std::unique_ptr<ray::streaming::LocalMemoryBuffer> buffer = msg.ToBytes();

  std::shared_ptr<GetLastMsgIdRspMessage> result_msg =
      GetLastMsgIdRspMessage::FromBytes(buffer->Data());
  STREAMING_LOG(INFO) << "result_msg error: "
                      << queue::flatbuf::EnumNameStreamingQueueError(
                             result_msg->ErrorCode());
  EXPECT_EQ(queue::flatbuf::StreamingQueueError::QUEUE_NOT_EXIST,
            result_msg->ErrorCode());
}

TEST_F(MockIntegrationTest, TestDestroyQueue) {
  STREAMING_LOG(INFO) << "TestDestroyQueues";
  upstream_handler_->DeleteUpstreamQueue(queue_id_);
  downstream_handler_->DeleteDownstreamQueue(queue_id_);

  EXPECT_FALSE(upstream_handler_->UpstreamQueueExists(queue_id_));
  EXPECT_FALSE(downstream_handler_->DownstreamQueueExists(queue_id_));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

TEST_F(MockIntegrationTest, TestGetPeerHostname) {
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  auto downstream_handler_2 = std::make_shared<DownstreamQueueMessageHandler>(
      std::string(), TransportType::MEMORY);
  downstream_handler_2->Start();

  ray::ObjectID queue_id_2 = ray::ObjectID::FromRandom();
  PrepareAddQueue(queue_id_2, upstream_handler_, downstream_handler_2);
  upstream_handler_->CreateUpstreamQueue(
      queue_id_2,
      StreamingQueueInitialParameter{downstream_handler_2->GetActorID(), nullptr, nullptr,
                                     false, true},
      QUEUE_SIZE, QUEUE_SIZE);
  downstream_handler_2->CreateDownstreamQueue(
      queue_id_2, StreamingQueueInitialParameter{upstream_handler_->GetActorID(), nullptr,
                                                 nullptr, false, true});
  while (!upstream_handler_->GetUpQueue(queue_id_2)->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_EQ(upstream_handler_->GetUpQueue(queue_id_2)->GetPeerHostName(),
            StreamingUtility::GetHostname());
}

TEST_F(MockIntegrationTest, TestPullQueueMessageContinuous) {
  auto up_queue = upstream_handler_->GetUpQueue(queue_id_);
  auto down_queue = downstream_handler_->GetDownQueue(queue_id_);

  PromiseWrapper promise;
  uint64_t message_count = 50000;
  std::thread writer_thread([&]() {
    STREAMING_LOG(INFO) << "WriterThread";
    auto upstreaming_queue = GetUpstreamQueue();
    while (!upstreaming_queue->IsInitialized()) {
      STREAMING_LOG(WARNING) << "Writer Queue not ready.";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    for (uint64_t i = 0; i < message_count; i++) {
      uint8_t meta[1];
      uint8_t data[1];
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(upstreaming_queue->GetBufferPool(), data, 1000);
      ASSERT_TRUE(upstreaming_queue->Push(meta, 1, data_buffer.Data(), data_buffer.Size(),
                                          current_sys_time_ms(), i,
                                          i) == StreamingStatus::OK);
      if (i == 10000) {
        promise.Notify(ray::Status::OK());
      }
    }
    STREAMING_LOG(INFO) << "WriterThread done";
  });

  RAY_IGNORE_EXPR(promise.Wait());
  bool is_first_pull;
  uint64_t count;

  ASSERT_EQ(
      StreamingQueueStatus::OK,
      downstream_handler_->PullQueue(queue_id_, 0, 0, is_first_pull, count, 10 * 1000));
  STREAMING_LOG(INFO) << "Pull count: " << count;
  uint64_t expect_message_id = 0;
  uint64_t counter = 0;
  while (true) {
    QueueItem item = down_queue->PopPendingBlockTimeout(1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      continue;
    }
    STREAMING_LOG(DEBUG) << "expect_message_id: " << expect_message_id << " MsgIdStart "
                         << item.MsgIdStart();
    ASSERT_TRUE(item.MsgIdStart() <= expect_message_id);
    ASSERT_TRUE(item.MsgIdEnd() <= expect_message_id);
    if (item.MsgIdEnd() == expect_message_id) {
      expect_message_id++;
    }
    if (item.MsgIdEnd() < expect_message_id) {
      expect_message_id = item.MsgIdEnd();
      expect_message_id++;
    }
    if (++counter == message_count) {
      STREAMING_LOG(DEBUG) << "Receive message total: " << message_count;
      break;
    }
    STREAMING_LOG(DEBUG) << "counter: " << counter;
  }
  writer_thread.join();
}

/// send 1-2-3-4-5-...
/// pull 1-2-3-4-5
TEST_F(MockIntegrationTest, TestAvoidDuplicateDataWhenStartUp1) {
  auto up_queue = upstream_handler_->GetUpQueue(queue_id_);
  auto down_queue = downstream_handler_->GetDownQueue(queue_id_);

  PromiseWrapper promise;
  uint64_t message_count = 50000;
  std::thread writer_thread([&]() {
    STREAMING_LOG(INFO) << "WriterThread";
    auto upstreaming_queue = GetUpstreamQueue();
    while (!upstreaming_queue->IsInitialized()) {
      STREAMING_LOG(WARNING) << "Writer Queue not ready.";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    for (uint64_t i = 0; i < message_count; i++) {
      uint8_t meta[1];
      uint8_t data[1];
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(upstreaming_queue->GetBufferPool(), data, 1000);
      ASSERT_TRUE(upstreaming_queue->Push(meta, 1, data_buffer.Data(), data_buffer.Size(),
                                          current_sys_time_ms(), i,
                                          i) == StreamingStatus::OK);
      if (i == 10000) {
        promise.Notify(ray::Status::OK());
      }
    }
    STREAMING_LOG(INFO) << "WriterThread done";
  });

  RAY_IGNORE_EXPR(promise.Wait());
  bool is_first_pull;
  uint64_t count;

  ASSERT_EQ(
      StreamingQueueStatus::OK,
      downstream_handler_->PullQueue(queue_id_, 0, 0, is_first_pull, count, 10 * 1000));
  STREAMING_LOG(INFO) << "Pull count: " << count;
  uint64_t expect_message_id = 0;
  uint64_t counter = 0;
  while (true) {
    QueueItem item = down_queue->PopPendingBlockTimeout(1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      continue;
    }
    STREAMING_LOG(DEBUG) << "expect_message_id: " << expect_message_id << " MsgIdStart "
                         << item.MsgIdStart();
    ASSERT_EQ(item.MsgIdStart(), expect_message_id);
    ASSERT_EQ(item.MsgIdEnd(), expect_message_id);
    expect_message_id++;

    if (++counter == message_count) {
      STREAMING_LOG(DEBUG) << "Receive message total: " << message_count;
      break;
    }
    STREAMING_LOG(DEBUG) << "counter: " << counter;
  }
  writer_thread.join();
}

/// send 2-3-4-5-...
/// pull 1-2-3-4-5
TEST_F(MockIntegrationTest, TestAvoidDuplicateDataWhenStartUp2) {
  auto up_queue = upstream_handler_->GetUpQueue(queue_id_);
  auto down_queue = downstream_handler_->GetDownQueue(queue_id_);

  auto transport = std::dynamic_pointer_cast<SimpleMemoryTransport>(up_transport_);
  transport->SetDrop(true);

  PromiseWrapper promise;
  const uint64_t message_count = 50000;
  const uint64_t dropped_message_count = 1000;
  std::thread writer_thread([&]() {
    STREAMING_LOG(INFO) << "WriterThread";
    auto upstreaming_queue = GetUpstreamQueue();
    while (!upstreaming_queue->IsInitialized()) {
      STREAMING_LOG(WARNING) << "Writer Queue not ready.";
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    for (uint64_t i = 1; i <= message_count; i++) {
      uint8_t meta[1];
      uint8_t data[1];
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(upstreaming_queue->GetBufferPool(), data, 1000);
      ASSERT_TRUE(upstreaming_queue->Push(meta, 1, data_buffer.Data(), data_buffer.Size(),
                                          current_sys_time_ms(), i,
                                          i) == StreamingStatus::OK);
      if (i == dropped_message_count) {
        transport->SetDrop(false);
      }
      if (i == 10000) {
        promise.Notify(ray::Status::OK());
      }
    }
    STREAMING_LOG(INFO) << "WriterThread done";
  });

  RAY_IGNORE_EXPR(promise.Wait());
  bool is_first_pull;
  uint64_t count;

  ASSERT_EQ(StreamingQueueStatus::OK,
            downstream_handler_->PullQueue(queue_id_, 0, /*start_msg_id*/ 1,
                                           is_first_pull, count, 10 * 1000));
  STREAMING_LOG(INFO) << "Pull count: " << count;
  uint64_t expect_message_id = 1001;
  uint64_t counter = 0;
  while (true) {
    QueueItem item = down_queue->PopPendingBlockTimeout(1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "counter: " << counter;
      continue;
    }
    STREAMING_LOG(DEBUG) << "expect_message_id: " << expect_message_id << " MsgIdStart "
                         << item.MsgIdStart();
    if (item.MsgIdStart() == 1) {
      expect_message_id = 1;
    }
    ASSERT_EQ(item.MsgIdStart(), expect_message_id);
    ASSERT_EQ(item.MsgIdEnd(), expect_message_id);
    expect_message_id++;

    if (++counter == message_count + count - dropped_message_count) {
      STREAMING_LOG(DEBUG) << "Receive message total: "
                           << message_count + count - dropped_message_count;
      break;
    }
    STREAMING_LOG(DEBUG) << "counter: " << counter;
  }
  writer_thread.join();
}

class MockIntegrationTestNoSetUp : public MockIntegrationTest,
                                   public ::testing::WithParamInterface<int> {
 public:
  virtual void SetUp() override {
    STREAMING_LOG(INFO) << "MockIntegrationTestNoSetUp SetUp";
  }

  virtual void TearDown() override {}
};

TEST_P(MockIntegrationTestNoSetUp, TestNotifyStartLogging) {
  ray::ObjectID notified_queue_id;
  uint64_t notified_msg_id = 0, notified_seq_id = 0, notified_barrier_id = 0;
  auto promise = std::make_shared<PromiseWrapper>();
  upstream_handler_ = std::make_shared<UpstreamQueueMessageHandler>(
      std::string(), TransportType::MEMORY, ElasticBufferConfig(),
      [&promise, &notified_queue_id, &notified_msg_id, &notified_seq_id,
       &notified_barrier_id](const ray::ObjectID &queue_id, uint64_t msg_id,
                             uint64_t seq_id, uint64_t barrier_id) {
        notified_queue_id = queue_id;
        notified_msg_id = msg_id;
        notified_seq_id = seq_id;
        notified_barrier_id = barrier_id;
        STREAMING_LOG(INFO) << "notified_queue_id: " << notified_queue_id
                            << " notified_msg_id: " << notified_msg_id
                            << " notified_seq_id: " << notified_seq_id
                            << " notified_barrier_id: " << notified_barrier_id;
        promise->Notify(ray::Status::OK());
      });
  upstream_handler_->Start();
  downstream_handler_ = std::make_shared<DownstreamQueueMessageHandler>(
      std::string(), TransportType::MEMORY);
  downstream_handler_->Start();

  queue_id_ = ray::ObjectID::FromRandom();
  PrepareAddQueue(queue_id_, upstream_handler_, downstream_handler_);
  auto writer_queue = upstream_handler_->CreateUpstreamQueue(
      queue_id_,
      StreamingQueueInitialParameter{downstream_handler_->GetActorID(), nullptr, nullptr,
                                     false, true},
      QUEUE_SIZE, QUEUE_SIZE, false);
  auto reader_queue = downstream_handler_->CreateDownstreamQueue(
      queue_id_, StreamingQueueInitialParameter{upstream_handler_->GetActorID(), nullptr,
                                                nullptr, false, true});

  uint64_t barrier_id = 10;
  uint64_t message_id = 100;
  uint64_t seq_id = 1000;
  std::thread reader_thread([&]() {
    auto downstreaming_queue = GetDownstreamQueue();
    downstreaming_queue->NotifyStartLogging(barrier_id, message_id, seq_id);
    STREAMING_LOG(INFO) << "ReaderThread done";
  });

  RAY_IGNORE_EXPR(promise->Wait());
  EXPECT_EQ(seq_id, notified_seq_id);
  EXPECT_EQ(message_id, notified_msg_id);
  EXPECT_EQ(barrier_id, notified_barrier_id);

  reader_thread.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
}

INSTANTIATE_TEST_CASE_P(MockIntegrationTestNoSetUp, MockIntegrationTestNoSetUp,
                        ::testing::Values(0, 5, 10));

int main(int argc, char **argv) {
  // StreamingLog::StartStreamingLog(StreamingLogLevel::TRACE);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
