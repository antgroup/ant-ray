#include <memory>

#include "data_writer.h"
#include "gtest/gtest.h"
#include "mock_reader.h"
#include "mock_writer.h"
#include "test_utils.h"
#include "util/logging.h"

using namespace ray::streaming;
namespace ray {
namespace streaming {
void GenRandomChannelIdVector(std::vector<ObjectID> &input_ids, int n) {
  for (int i = 0; i < n; ++i) {
    input_ids.push_back(ObjectID::FromRandom());
  }
}

/// Mock writer using mock queue.
class MockWriterTest : public ::testing::Test {
 protected:
  MockWriterTest() : runtime_context(new RuntimeContext()) {}
  virtual void SetUp() override {
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetQueueType("mock_queue");
    runtime_context->config_.SetTransferEventDriven(false);
    mock_writer.reset(new MockWriter(runtime_context));
  }
  virtual void TearDown() override {}

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockWriter> mock_writer;
  std::vector<ObjectID> input_ids;
};

TEST_F(MockWriterTest, test_message_avaliablie_in_buffer) {
  int channel_num = 5;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(id));
  }
  mock_writer->BroadcastBarrier(0);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(id));
  }
}

uint8_t data[] = {0x01, 0x02, 0x0f, 0xe, 0x00};
uint32_t data_size = 5;

TEST_F(MockWriterTest, test_write_message_to_buffer_ring) {
  int channel_num = 2;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  for (const auto &id : input_ids) {
    EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(id));
  }
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
  EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(input_ids[1]));
}

TEST_F(MockWriterTest, test_collecting_buffer) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  uint64_t buffer_remain;
  mock_writer->CollectFromRingBuffer(input_ids[0], buffer_remain);
  EXPECT_TRUE(buffer_remain == 0);
  EXPECT_TRUE(mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
  EXPECT_TRUE(mock_writer->GetChannelInfoMap()[input_ids[0]].bundle != nullptr);
}

TEST_F(MockWriterTest, test_write_to_transfer) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  mock_writer->Init(input_ids);
  mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  uint64_t buffer_remain;
  EXPECT_EQ(mock_writer->WriteBufferToChannel(input_ids[0], buffer_remain),
            StreamingStatus::OK);
  EXPECT_TRUE(buffer_remain == 0);
  EXPECT_TRUE(!mock_writer->IsMessageAvailableInBuffer(input_ids[0]));
}

/// Mock writer using streaming queue.
class MockWriterStreamingQueueTest : public ::testing::Test {
 protected:
  MockWriterStreamingQueueTest() : runtime_context(new RuntimeContext()) {}
  virtual void SetUp() override {
    StreamingLog::StartStreamingLog(StreamingLogLevel::TRACE);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetCollocateEnable(false);
    runtime_context->config_.SetTransferEventDriven(true);
    runtime_context->config_.SetStreamingLogLevel(-1);
    runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
    mock_writer.reset(new MockWriter(runtime_context));
  }
  virtual void TearDown() override {}

  void PrepareAddQueue(
      ray::ObjectID queue_id,
      std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler,
      std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler) {
    auto up_transport = std::make_shared<SimpleMemoryTransport>(
        [downstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
          downstream_handler->DispatchMessageAsync(buffers);
        },
        [downstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers)
            -> std::shared_ptr<ray::streaming::LocalMemoryBuffer> {
          return downstream_handler->DispatchMessageSync(buffers[0]);
        });
    auto down_transport = std::make_shared<SimpleMemoryTransport>(
        [upstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers) {
          upstream_handler->DispatchMessageAsync(buffers);
        },
        [upstream_handler](
            std::vector<std::shared_ptr<ray::streaming::LocalMemoryBuffer>> buffers)
            -> std::shared_ptr<ray::streaming::LocalMemoryBuffer> {
          return upstream_handler->DispatchMessageSync(buffers[0]);
        });

    upstream_handler->SetOutTransport(queue_id, up_transport);
    downstream_handler->SetOutTransport(queue_id, down_transport);
  }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockWriter> mock_writer;
  std::vector<ObjectID> input_ids;
};

TEST_F(MockWriterStreamingQueueTest, test_buffer_pool_block_timeout) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);

  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
  upstream_handler->SetOutTransport(
      input_ids[0], std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
  upstream_handler->Start();
  mock_writer->SetQueueMessageHandler(upstream_handler);

  std::vector<StreamingQueueInitialParameter> parameters{
      {RandomActorID(), ray::streaming::util::MockFunction(),
       ray::streaming::util::MockFunction(), false, true}};
  std::vector<uint64_t> msg_id_vec{0};
  std::vector<uint64_t> queue_size_vec{1024 * (1024 + 512)};  // 1.5M
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  mock_writer->Run();

  /// queue size is 1.5M, buffer pool size will be queue_size*2, 3M
  /// In buffer pool internal, the 3M buffer will be split to 2 buffers.
  /// The first one is 1M, and the second one is 2M.
  /// 1. acquire 1K buffer, allocated in the first buffer
  /// 2. acquire 1.1M buffer, allocated in the second buffer
  /// 3. acquire 1M buffer, neither the two internal buffers satisfy.
  /// Make the first two messages consumed in the queue, then acquire 1M buffer,
  /// the consumed buffer should be evicted in DataWriter::GetMemoryBuffer()
  std::unique_ptr<uint8_t> buf1 = std::unique_ptr<uint8_t>(new uint8_t[1024]);  // 1K
  std::unique_ptr<uint8_t> buf2 =
      std::unique_ptr<uint8_t>(new uint8_t[1024 * (1024 + 100)]);  // 1.1M
  EXPECT_NE(0, mock_writer->WriteMessageToBufferRing(input_ids[0], buf1.get(), 1024));
  STREAMING_LOG(INFO) << "write buf1";
  EXPECT_NE(0, mock_writer->WriteMessageToBufferRing(input_ids[0], buf2.get(),
                                                     1024 * (1024 + 100)));
  STREAMING_LOG(INFO) << "write buf2";
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto queue = upstream_handler->GetUpQueue(input_ids[0]);
  queue->OnNotify(std::make_shared<NotificationMessage>(ActorID::Nil(), ActorID::Nil(),
                                                        input_ids[0], /*msg_id*/ 2, 0));
  queue->SetQueueEvictionLimit(2);
  std::unique_ptr<uint8_t> buf3 =
      std::unique_ptr<uint8_t>(new uint8_t[1024 * 1024]);  // 1M
  STREAMING_LOG(INFO) << "write buf3";
  EXPECT_NE(0,
            mock_writer->WriteMessageToBufferRing(input_ids[0], buf3.get(), 1024 * 1024));
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

TEST_F(MockWriterStreamingQueueTest, test_write_large_message) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  auto &qid = input_ids[0];

  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
  upstream_handler->Start();
  auto downstream_handler =
      std::make_shared<DownstreamQueueMessageHandler>("", TransportType::MEMORY);
  downstream_handler->Start();
  PrepareAddQueue(input_ids[0], upstream_handler, downstream_handler);
  mock_writer->SetQueueMessageHandler(upstream_handler);

  std::vector<StreamingQueueInitialParameter> parameters{
      {ActorID::Nil(), ray::streaming::util::MockFunction(),
       ray::streaming::util::MockFunction(), false, true}};
  std::vector<uint64_t> msg_id_vec{0};
  std::vector<uint64_t> queue_size_vec{2 * 1024 * 1024};
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  mock_writer->Run();

  std::vector<TransferCreationStatus> creation_status;
  auto mock_reader = std::make_shared<MockReader>(runtime_context);
  mock_reader->SetQueueMessageHandler(downstream_handler);
  mock_reader->Init(input_ids, parameters, msg_id_vec, -1, creation_status);

  uint64_t data_size = 3 * 1024 * 1024;
  MemoryBuffer buffer;
  EXPECT_EQ(mock_writer->GetMemoryBuffer(qid, data_size, buffer, false),
            StreamingStatus::OK);
  EXPECT_NE(0, mock_writer->WriteMessageToBufferRing(qid, buffer,
                                                     StreamingMessageType::Message));

  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  std::shared_ptr<StreamingReaderBundle> msg;
  StreamingStatus st = mock_reader->GetBundle(10000, msg);
  EXPECT_EQ(st, StreamingStatus::OK);
  STREAMING_CHECK(msg->MetaBuffer());
  STREAMING_CHECK(msg->DataBuffer());
  auto bundlePtr =
      StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
  std::list<StreamingMessagePtr> message_list;
  bundlePtr->GetMessageList(message_list);
  STREAMING_LOG(INFO) << "message_list size: " << message_list.size();
  EXPECT_EQ(message_list.size(), 1);
}

TEST_F(MockWriterStreamingQueueTest, test_bpr) {
  runtime_context->config_.SetStreamingBufferPoolMinBufferSize(1024);
  mock_writer.reset(new MockWriter(runtime_context));
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  auto &qid = input_ids[0];

  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
  upstream_handler->Start();
  auto downstream_handler =
      std::make_shared<DownstreamQueueMessageHandler>("", TransportType::MEMORY);
  downstream_handler->Start();
  PrepareAddQueue(input_ids[0], upstream_handler, downstream_handler);
  mock_writer->SetQueueMessageHandler(upstream_handler);

  std::vector<StreamingQueueInitialParameter> parameters{
      {ActorID::Nil(), ray::streaming::util::MockFunction(),
       ray::streaming::util::MockFunction(), false, true}};
  std::vector<uint64_t> msg_id_vec{0};
  std::vector<uint64_t> queue_size_vec{10 * 1024};  // 10K
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  mock_writer->Run();

  std::vector<TransferCreationStatus> creation_status;
  const uint32_t data_size = 1024;
  uint8_t data[data_size];
  EXPECT_NE(0, mock_writer->WriteMessageToBufferRing(qid, data, data_size));

  /// Write a large buffer, large buffer should not be counted when compute backpressure
  /// rate.
  const uint32_t large_data_size = 20 * 1024;
  auto large_data = std::unique_ptr<uint8_t>(new uint8_t[large_data_size]);
  EXPECT_NE(
      0, mock_writer->WriteMessageToBufferRing(qid, large_data.get(), large_data_size));

  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  std::vector<double> rates;
  mock_writer->GetChannelSetBackPressureRatio(input_ids, rates);
  STREAMING_LOG(INFO) << "bpr: " << rates[0];
  EXPECT_EQ(rates[0], 0);

  // do not flush messages in ringbuffer to queue.
  mock_writer->StopEventService();
  for (int i = 0; i < 10; i++) {
    EXPECT_NE(0, mock_writer->WriteMessageToBufferRing(qid, data, data_size));
  }
  rates.clear();
  mock_writer->GetChannelSetBackPressureRatio(input_ids, rates);
  STREAMING_LOG(INFO) << "bpr: " << rates[0];
  EXPECT_EQ((int)(rates[0] * 100), 53);  // expect 0.53
}

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
