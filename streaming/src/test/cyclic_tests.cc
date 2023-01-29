#include <memory>

#include "data_reader.h"
#include "data_writer.h"
#include "gtest/gtest.h"
#include "mock_reader.h"
#include "mock_writer.h"
#include "test_utils.h"

namespace ray {
namespace streaming {
void GenRandomChannelIdVector(std::vector<ObjectID> &input_ids, int n) {
  for (int i = 0; i < n; ++i) {
    input_ids.push_back(ObjectID::FromRandom());
  }
}

class CyclicWriterTest : public ::testing::Test {
 public:
  CyclicWriterTest() : runtime_context(new RuntimeContext()) {}

 protected:
  virtual void SetUp() override {
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetPlasmaSocketPath(std::string(""));
    runtime_context->config_.SetTransferEventDriven(false);
    runtime_context->config_.SetCollocateEnable(false);
    mock_writer.reset(new MockWriter(runtime_context));
  }
  virtual void TearDown() override {}

  void CyclicFoTestBase() {
    int channel_num = 2;
    GenRandomChannelIdVector(input_ids, channel_num);
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetPlasmaSocketPath(std::string(""));
    runtime_context->config_.SetTransferEventDriven(true);
    runtime_context->config_.SetCollocateEnable(false);
    runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
    // Set queue size 1024*1024, writer step 1024, so the max bundle size is 1024,
    // so if each message size is 1024, every bundle contains only one message.
    runtime_context->config_.SetStreamingWriterConsumedStep(1024);
    mock_writer.reset(new MockWriter(runtime_context));

    upstream_handler =
        std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
    upstream_handler->SetOutTransport(
        input_ids[0], std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
    upstream_handler->SetOutTransport(
        input_ids[1], std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
    upstream_handler->Start();
    mock_writer->SetQueueMessageHandler(upstream_handler);

    std::vector<StreamingQueueInitialParameter> parameters{
        {RandomActorID(), util::MockFunction(), util::MockFunction(), true, false},
        {RandomActorID(), util::MockFunction(), util::MockFunction(), false, false}};
    std::vector<uint64_t> msg_id_vec{0, 0};
    std::vector<uint64_t> queue_size_vec{1024 * 1024, 1024 * 1024};
    mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
    mock_writer->Run();

    uint8_t data[1024];
    uint32_t data_size = 1024;
    for (int i = 0; i < 500; i++) {
      mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
      mock_writer->WriteMessageToBufferRing(input_ids[1], data, data_size);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    while (mock_writer->IsMessageAvailableInBuffer(input_ids[0]) &&
           mock_writer->IsMessageAvailableInBuffer(input_ids[1])) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockWriter> mock_writer;
  std::vector<ObjectID> input_ids;
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler;
};

TEST_F(CyclicWriterTest, TestCyclic) {
  int channel_num = 2;
  GenRandomChannelIdVector(input_ids, channel_num);
  std::vector<StreamingQueueInitialParameter> parameters{
      {RandomActorID(), util::MockFunction(), util::MockFunction(), true, false},
      {RandomActorID(), util::MockFunction(), util::MockFunction(), false, false}};
  std::vector<uint64_t> msg_id_vec{0, 0};
  std::vector<uint64_t> queue_size_vec{1024 * 1024, 1024 * 1024};
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  EXPECT_TRUE(mock_writer->GetUpStreamHandler()->GetUpQueue(input_ids[0])->IsCyclic());
  EXPECT_FALSE(mock_writer->GetUpStreamHandler()->GetUpQueue(input_ids[1])->IsCyclic());
}

TEST_F(CyclicWriterTest, TestEmptyMessageBlock) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(std::string(""));
  runtime_context->config_.SetTransferEventDriven(false);
  runtime_context->config_.SetCollocateEnable(true);
  mock_writer.reset(new MockWriter(runtime_context));

  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
  upstream_handler->SetOutTransport(
      input_ids[0], std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
  upstream_handler->Start();
  mock_writer->SetQueueMessageHandler(upstream_handler);

  std::vector<StreamingQueueInitialParameter> parameters{
      {RandomActorID(), util::MockFunction(), util::MockFunction(), true, false},
      {RandomActorID(), util::MockFunction(), util::MockFunction(), false, false}};
  std::vector<uint64_t> msg_id_vec{0, 0};
  std::vector<uint64_t> queue_size_vec{1024 * 1024, 1024 * 1024};
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  mock_writer->Run();
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  // No empty messages can be sent, because the queue is not initialized.
  EXPECT_EQ(mock_writer->GetSendEmptyCnt(input_ids[0]), 0);
  mock_writer->Stop();
  // All threads can be joined when writer deconstruct.
}

TEST_F(CyclicWriterTest, TestLoggingMode) {
  CyclicFoTestBase();
  uint64_t checkpoint_id = 1;
  mock_writer->OnStartLogging(input_ids[0], 251, 251, checkpoint_id);

  uint8_t data[1024];
  uint32_t data_size = 1024;
  for (int i = 0; i < 100; i++) {
    mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
    mock_writer->WriteMessageToBufferRing(input_ids[1], data, data_size);
  }
  mock_writer->BroadcastBarrier(checkpoint_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0]) &&
         mock_writer->IsMessageAvailableInBuffer(input_ids[1])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  std::vector<StreamingMessageBundleMetaPtr> load_bundle_vec;
  mock_writer->GetAsyncWAL()->Load(checkpoint_id, load_bundle_vec, input_ids[0]);
  EXPECT_EQ(load_bundle_vec.size(), 350);
}

TEST_F(CyclicWriterTest, TestClearCheckpoint) {
  CyclicFoTestBase();
  uint64_t checkpoint_id = 1;
  mock_writer->OnStartLogging(input_ids[0], 251, 251, checkpoint_id);

  mock_writer->BroadcastBarrier(checkpoint_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  mock_writer->ClearCheckpoint(checkpoint_id, checkpoint_id);
  std::vector<StreamingMessageBundleMetaPtr> bundle_vec;
  mock_writer->GetAsyncWAL()->Load(checkpoint_id, bundle_vec, input_ids[0]);
  EXPECT_EQ(bundle_vec.size(), 0);
}

class CyclicIntegrateTest : public ::testing::Test {
 public:
  CyclicIntegrateTest() : runtime_context(new RuntimeContext()) {}

 protected:
  virtual void SetUp() override {
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetPlasmaSocketPath(std::string(""));
    runtime_context->config_.SetTransferEventDriven(false);
    runtime_context->config_.SetCollocateEnable(false);
    mock_writer.reset(new MockWriter(runtime_context));
    writer_actor_id = ActorID::Nil();
    reader_actor_id = ActorID::Nil();
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

  void CyclicFoTestBase(int channel_num) {
    GenRandomChannelIdVector(input_ids, channel_num);
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetPlasmaSocketPath(std::string(""));
    runtime_context->config_.SetTransferEventDriven(true);
    runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
    // runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
    // Set queue size 1024*1024, writer step 1024, so the max bundle size is 1024,
    // so if each message size is 1024, every bundle contains only one message.
    runtime_context->config_.SetStreamingWriterConsumedStep(1024);
    mock_writer.reset(new MockWriter(runtime_context));
    mock_reader.reset(new MockReader(runtime_context));

    upstream_handler =
        std::make_shared<UpstreamQueueMessageHandler>("", TransportType::MEMORY);
    upstream_handler->Start();
    downstream_handler =
        std::make_shared<DownstreamQueueMessageHandler>("", TransportType::MEMORY);
    downstream_handler->Start();
    PrepareAddQueue(input_ids[0], upstream_handler, downstream_handler);

    mock_writer->SetQueueMessageHandler(upstream_handler);
    mock_reader->SetQueueMessageHandler(downstream_handler);

    std::vector<StreamingQueueInitialParameter> parameters(
        channel_num,
        {reader_actor_id, util::MockFunction(), util::MockFunction(), true, false});
    std::vector<uint64_t> msg_id_vec(channel_num, 0);
    std::vector<uint64_t> queue_size_vec(channel_num, 1024 * 1024);
    mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
    mock_writer->Run();

    uint8_t data[1024];
    uint32_t data_size = 1024;
    for (int i = 0; i < 500; i++) {
      mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockWriter> mock_writer;
  std::shared_ptr<MockReader> mock_reader;
  std::vector<ObjectID> input_ids;
  std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler;
  std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler;
  ActorID writer_actor_id;
  ActorID reader_actor_id;
};

TEST_F(CyclicIntegrateTest, TestFO) {
  CyclicFoTestBase(1 /*channel_num*/);
  uint64_t checkpoint_id = 1;
  mock_writer->OnStartLogging(input_ids[0], 251, 251, checkpoint_id);

  /// checkpoint 1
  mock_writer->BroadcastBarrier(checkpoint_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  mock_writer->ClearCheckpoint(checkpoint_id - 1, checkpoint_id);
  uint8_t data[1024];
  uint32_t data_size = 1024;
  for (int i = 0; i < 100; i++) {
    mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  checkpoint_id = 2;
  mock_writer->OnStartLogging(input_ids[0], 551, 551, checkpoint_id);
  /// checkpoint 2
  mock_writer->BroadcastBarrier(checkpoint_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  for (int i = 0; i < 100; i++) {
    mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  auto queue = upstream_handler->GetUpQueue(input_ids[0]);
  EXPECT_EQ(queue->ProcessedCount(), 700);
  EXPECT_EQ(queue->PendingCount(), 0);

  /// Mock reader failover
  mock_reader->Stop();
  mock_reader = nullptr;
  /// recover from checkpoint 2
  runtime_context->config_.SetStreamingRollbackCheckpointId(checkpoint_id);
  mock_reader.reset(new MockReader(runtime_context));
  /// Pull message from 550 + 1
  std::vector<uint64_t> msg_ids{550};
  std::vector<TransferCreationStatus> creation_status;
  std::vector<StreamingQueueInitialParameter> parameters2{
      {writer_actor_id, util::MockFunction(), util::MockFunction(), true, false}};
  downstream_handler->Start();
  mock_reader->SetQueueMessageHandler(downstream_handler);
  mock_reader->Init(input_ids, parameters2, msg_ids, -1, creation_status);

  int count = 0;
  int message_id = 551;
  while (true) {
    std::shared_ptr<StreamingReaderBundle> msg;
    StreamingStatus st = mock_reader->GetBundle(1000, msg);
    if (st != StreamingStatus::OK || msg->IsEmpty()) {
      STREAMING_LOG(INFO) << "read bundle timeout, status = " << (int)st;
      continue;
    }
    StreamingMessageBundlePtr bundlePtr;
    STREAMING_CHECK(msg->MetaBuffer());
    STREAMING_CHECK(msg->DataBuffer());
    bundlePtr = StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
    std::list<StreamingMessagePtr> message_list;
    bundlePtr->GetMessageList(message_list);
    STREAMING_LOG(INFO) << "message_list size: " << message_list.size();
    EXPECT_EQ(message_list.size(), 1);
    STREAMING_LOG(INFO) << "message id: " << message_list.front()->GetMessageId();
    EXPECT_EQ(message_list.front()->GetMessageId(), message_id);
    message_id++;
    count++;
    if (count == 150) {
      break;
    }
  }
}

TEST_F(CyclicIntegrateTest, TestCheckpointTimeout) {
  CyclicFoTestBase(2 /*channel_num*/);
  for (auto &q_id : input_ids) {
    STREAMING_LOG(INFO) << "q_id: " << q_id;
  }
  uint64_t checkpoint_id = 1;
  mock_writer->OnStartLogging(input_ids[0], 101, 101, checkpoint_id);
  mock_writer->OnStartLogging(input_ids[0], 201, 201, checkpoint_id + 1);
  mock_writer->OnStartLogging(input_ids[0], 301, 301, checkpoint_id + 2);
  mock_writer->OnStartLogging(input_ids[1], 101, 101, checkpoint_id);

  /// checkpoint 1
  mock_writer->BroadcastBarrier(checkpoint_id);
  uint8_t data[1024];
  uint32_t data_size = 1024;
  for (int i = 0; i < 100; i++) {
    mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  mock_writer->ClearCheckpoint(checkpoint_id - 1, checkpoint_id);

  checkpoint_id = 3;
  mock_writer->OnStartLogging(input_ids[1], 201, 201, checkpoint_id);

  /// checkpoint 2
  mock_writer->BroadcastBarrier(checkpoint_id);
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  while (mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  mock_writer->ClearCheckpoint(checkpoint_id - 1, checkpoint_id);

  auto &barrier_helper = mock_writer->GetBarrierHelper();
  EXPECT_EQ(3, barrier_helper.GetCurrentLoggingModeCheckpointId(input_ids[0]).first);

  // checkpoint id 2 storage file should be removed.
  std::vector<StreamingMessageBundleMetaPtr> bundle_vec;
  mock_writer->GetAsyncWAL()->Load(2 /*checkpoint id*/, bundle_vec, input_ids[0]);
  EXPECT_EQ(bundle_vec.size(), 0);

  bundle_vec.clear();
  mock_writer->GetAsyncWAL()->Load(3 /*checkpoint id*/, bundle_vec, input_ids[0]);
  EXPECT_EQ(bundle_vec.size(), 300);
}

}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
