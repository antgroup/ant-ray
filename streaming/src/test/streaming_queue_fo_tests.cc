#include <memory>

#include "data_writer.h"
#include "gtest/gtest.h"
#include "hiredis/hiredis.h"
#include "mock_writer.h"
#include "ray/common/test_util.h"
#include "ray/object_manager/plasma/client.h"
#include "test_utils.h"

namespace ray {
namespace streaming {

void GenRandomChannelIdVector(std::vector<ObjectID> &input_ids, int n) {
  for (int i = 0; i < n; ++i) {
    input_ids.push_back(ObjectID::FromRandom());
  }
}

static void flushall_redis(void) {
  redisContext *context = ::redisConnect("127.0.0.1", 6379);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);

  context = redisConnect("127.0.0.1", 6380);
  freeReplyObject(redisCommand(context, "FLUSHALL"));
  freeReplyObject(redisCommand(context, "SET NumRedisShards 1"));
  freeReplyObject(redisCommand(context, "LPUSH RedisShards 127.0.0.1:6380"));
  redisFree(context);
}

class StreamingQueueMockWriterTest : public ::testing::Test {
 protected:
  virtual void SetUp() override {
    runtime_context = std::make_shared<RuntimeContext>();
    STREAMING_LOG(INFO) << "SetUp";
    StreamingLog::StartStreamingLog(StreamingLogLevel::INFO);
    runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
    runtime_context->config_.SetQueueType("streaming_queue");
    runtime_context->config_.SetPlasmaSocketPath(store_socket);
    // config.SetCollocateEnable(false);
    mock_writer.reset(new MockWriter(runtime_context));
    auto upstream_handler = std::make_shared<UpstreamQueueMessageHandler>(
        store_socket, TransportType::MEMORY);
    mock_writer->SetQueueMessageHandler(upstream_handler);
  }

  virtual void TearDown() override {}

  static void SetUpTestCase() {
    STREAMING_LOG(INFO) << "SetUpTestCase";
    TestSetupUtil::StartUpRedisServers(std::vector<int>{6379, 6380});
    // flush redis first.
    flushall_redis();

    // start gcs server
    TestSetupUtil::StartGcsServer("127.0.0.1");

    TestSetupUtil::StartRaylet("127.0.0.1", ray::streaming::util::GenerateRandomPort(),
                               "127.0.0.1", "memory,1000.0,CPU,4.0,resource,10",
                               &store_socket);

    STREAMING_LOG(INFO) << "store_socket: " << store_socket;
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000*1000));
  }

 protected:
  std::shared_ptr<RuntimeContext> runtime_context;
  std::shared_ptr<MockWriter> mock_writer;
  std::vector<ObjectID> input_ids;
  static std::string store_socket;
};
std::string StreamingQueueMockWriterTest::store_socket = "";

uint8_t data[] = {0x01, 0x02, 0x0f, 0xe, 0x00};
uint32_t data_size = 5;
TEST_F(StreamingQueueMockWriterTest, DISABLED_TestCyclic) {
  int channel_num = 2;
  GenRandomChannelIdVector(input_ids, channel_num);
  std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(
      mock_writer->GetUpStreamHandler())
      ->SetOutTransport(input_ids[0],
                        std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
  std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(
      mock_writer->GetUpStreamHandler())
      ->SetOutTransport(input_ids[1],
                        std::make_shared<SimpleMemoryTransport>(nullptr, nullptr));
  std::vector<StreamingQueueInitialParameter> parameters{
      {ActorID::Nil(), util::MockFunction(), util::MockFunction(), true, true},
      {ActorID::Nil(), util::MockFunction(), util::MockFunction(), false, true}};
  mock_writer->GetUpStreamHandler()->Start();
  std::vector<uint64_t> msg_id_vec{0, 0};
  std::vector<uint64_t> queue_size_vec{16, 16};
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  mock_writer->Run();
  EXPECT_TRUE(std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(
                  mock_writer->GetUpStreamHandler())
                  ->GetUpQueue(input_ids[0])
                  ->IsCyclic());
  EXPECT_FALSE(std::dynamic_pointer_cast<UpstreamQueueMessageHandler>(
                   mock_writer->GetUpStreamHandler())
                   ->GetUpQueue(input_ids[1])
                   ->IsCyclic());
}

/// Writer write some data
/// Write dead, release object in plasma
/// Inflight data arrive at reader
/// Reader try get collocate object in plasma, timeout
/// Reader ignore all these invalid data corresponded with the same object id
TEST_F(StreamingQueueMockWriterTest, CollocateTest) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>(store_socket, TransportType::MEMORY);
  auto downstream_handler = std::make_shared<DownstreamQueueMessageHandler>(
      store_socket, TransportType::MEMORY);
  auto up_transport = std::make_shared<SimpleMemoryTransport>(
      downstream_handler->GetAsyncMethod(), downstream_handler->GetSyncMethod());
  auto down_transport = std::make_shared<SimpleMemoryTransport>(
      upstream_handler->GetAsyncMethod(), upstream_handler->GetSyncMethod());
  upstream_handler->SetOutTransport(input_ids[0], up_transport);
  downstream_handler->SetOutTransport(input_ids[0], down_transport);
  upstream_handler->Start();
  downstream_handler->Start();
  auto reader_runtime_context = std::make_shared<RuntimeContext>();
  reader_runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
  reader_runtime_context->config_.SetQueueType("streaming_queue");
  reader_runtime_context->config_.SetPlasmaSocketPath(store_socket);

  auto reader = std::make_shared<DataReader>(reader_runtime_context);
  reader->SetQueueMessageHandler(downstream_handler);
  std::vector<StreamingQueueInitialParameter> parameters{
      {ActorID::Nil(), util::MockFunction(), util::MockFunction(), false, true}};
  std::vector<uint64_t> start_message_id{0};
  std::vector<TransferCreationStatus> creation_status{
      TransferCreationStatus::FreshStarted};
  reader->Init(input_ids, parameters, start_message_id, -1, creation_status);

  mock_writer->SetQueueMessageHandler(upstream_handler);

  std::vector<uint64_t> msg_id_vec{0};
  std::vector<uint64_t> queue_size_vec{1024 * 1024};
  mock_writer->Init(input_ids, parameters, msg_id_vec, queue_size_vec);
  while (!upstream_handler->GetUpQueue(input_ids[0])->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  up_transport->SetInFlight(true);
  mock_writer->Run();
  for (int i = 0; i < 1024; i++) {
    mock_writer->WriteMessageToBufferRing(input_ids[0], data, data_size);
  }
  while (!mock_writer->IsMessageAvailableInBuffer(input_ids[0])) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  mock_writer.reset();

  up_transport->SetInFlight(false);
  std::shared_ptr<StreamingReaderBundle> message;
  StreamingStatus st = reader->GetBundle(20 * 1000, message);
  EXPECT_EQ(st, StreamingStatus::GetBundleTimeOut);
}

// Bundle does not exists in buffer pool when resend from esbuffer
// or pangu.
TEST_F(StreamingQueueMockWriterTest, CollocateTest2) {
  int channel_num = 1;
  GenRandomChannelIdVector(input_ids, channel_num);
  auto upstream_handler =
      std::make_shared<UpstreamQueueMessageHandler>(store_socket, TransportType::MEMORY);
  auto downstream_handler = std::make_shared<DownstreamQueueMessageHandler>(
      store_socket, TransportType::MEMORY);
  auto up_transport = std::make_shared<SimpleMemoryTransport>(
      downstream_handler->GetAsyncMethod(), downstream_handler->GetSyncMethod());
  auto down_transport = std::make_shared<SimpleMemoryTransport>(
      upstream_handler->GetAsyncMethod(), upstream_handler->GetSyncMethod());
  upstream_handler->SetOutTransport(input_ids[0], up_transport);
  downstream_handler->SetOutTransport(input_ids[0], down_transport);
  upstream_handler->Start();
  downstream_handler->Start();

  int32_t queue_size = 1024 * 1024;
  const StreamingQueueInitialParameter param{ActorID::Nil(), nullptr, nullptr, false,
                                             true};
  auto writer_queue = upstream_handler->CreateUpstreamQueue(input_ids[0], param,
                                                            queue_size, queue_size, true);
  auto reader_queue = downstream_handler->CreateDownstreamQueue(input_ids[0], param);

  while (true) {
    if (writer_queue->IsInitialized()) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  for (int i = 0; i < 100; i++) {
    uint8_t meta[10];
    uint8_t data[100];
    memset(data, i % 100, 100);
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(writer_queue->GetBufferPool(), data, 100);
    QueueItem item(i, meta, 10, data_buffer.Data(), data_buffer.Size(), 0,
                   current_sys_time_ms(), i, i);
    ASSERT_TRUE(writer_queue->Queue::Push(item));
    writer_queue->Send();
  }

  for (int i = 0; i < 100; i++) {
    uint8_t meta[10];
    uint8_t data[100];
    memset(data, i % 100, 100);
    QueueItem item(i, meta, 10, data, 100, 0, current_sys_time_ms(), i, i, true);
    ASSERT_TRUE(writer_queue->Queue::Push(item));
    while (!writer_queue->Queue::IsPendingEmpty()) {
      QueueItem item = writer_queue->Queue::PopPending();
      writer_queue->SendItem(item, true, false);
    }
  }

  for (int i = 0; i < 100; i++) {
    uint8_t meta[10];
    uint8_t data[100];
    memset(data, i % 100, 100);
    MemoryBuffer data_buffer =
        util::CopyToBufferPool(writer_queue->GetBufferPool(), data, 100);
    QueueItem item(i, meta, 10, data_buffer.Data(), data_buffer.Size(), 0,
                   current_sys_time_ms(), i, i);
    ASSERT_TRUE(writer_queue->Queue::Push(item));
    writer_queue->Send();
  }

  for (int i = 0; i < 300; i++) {
    QueueItem item = reader_queue->PopPendingBlockTimeout(1000);
    reader_queue->OnConsumed(i, i);
  }

  upstream_handler->Stop();
  downstream_handler->Stop();
}

TEST_F(StreamingQueueMockWriterTest, TestReaderQueue) {
  ReaderQueue queue(ObjectID::FromRandom(), RandomActorID(), RandomActorID(), false,
                    nullptr);
  auto client = std::make_shared<plasma::PlasmaClient>();
  EXPECT_TRUE(client->Connect(store_socket).ok());
  ObjectID collocate_data_object_id = ObjectID::FromRandom();
  EXPECT_FALSE(queue.CreateInternalBuffer(client, collocate_data_object_id));
  EXPECT_FALSE(queue.CreateInternalBuffer(client, collocate_data_object_id));
  RAY_IGNORE_EXPR(client->Disconnect());
}
}  // namespace streaming
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  STREAMING_LOG(INFO) << "argv[1]: " << argv[1];
  ray::TEST_RAYLET_EXEC_PATH = std::string(argv[1]);
  ray::TEST_GCS_SERVER_EXEC_PATH = std::string(argv[2]);
  ray::TEST_REDIS_CLIENT_EXEC_PATH = std::string(argv[3]);
  ray::TEST_REDIS_SERVER_EXEC_PATH = std::string(argv[4]);
  ray::TEST_MOCK_WORKER_EXEC_PATH = std::string(argv[5]);
  return RUN_ALL_TESTS();
}
