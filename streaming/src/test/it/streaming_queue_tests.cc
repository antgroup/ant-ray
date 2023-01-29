#include "streaming_queue_tests.h"

namespace ray {
namespace streaming {

void StreamingQueueUpStreamTestSuite::SetUp() {
  ray::CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id_);
  upstream_handler_ = std::make_shared<UpstreamQueueMessageHandler>(plasma_store_socket_);
  upstream_handler_->Start();
  auto receiver = std::make_shared<MockDataWriter>(upstream_handler_);
  SetReceiver(receiver);

  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::SetUp";
}

void StreamingQueueUpStreamTestSuite::TearDown() {
  if (upstream_handler_) {
    upstream_handler_->Stop();
  }
}

void StreamingQueueUpStreamTestSuite::GetQueueTest() {
  // Sleep 2s, queue should not exist when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    std::shared_ptr<WriterQueue> queue =
        upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }

  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::GetQueueTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::PullPeerAsyncTest() {
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::PullPeerAsyncTest";
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }

  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 80; msg_id++) {
    uint8_t meta[10];
    memset(meta, msg_id, 10);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);

    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 10, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::PullPeerAsyncTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::DeleteQueueTest() {
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    std::shared_ptr<WriterQueue> queue =
        upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }

  for (int seq_id = 0; seq_id < 80; seq_id++) {
    uint8_t meta[10];
    memset(meta, seq_id, 10);
    uint8_t data[100];
    memset(data, seq_id, 100);
    for (auto &queue_id : queue_ids_) {
      STREAMING_LOG(INFO) << "Writer User Push item seq_id: " << seq_id;
      std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_id);
      while (!queue->IsInitialized()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      MemoryBuffer data_buffer =
          util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
      ASSERT_TRUE(queue->Push(meta, 10, data_buffer.Data(), data_buffer.Size(),
                              current_sys_time_ms(), 3 * seq_id,
                              3 * seq_id + 2) == StreamingStatus::OK);
      queue->Send();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  upstream_handler_->DeleteUpstreamQueue(queue_ids_[0]);

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::DeleteQueueTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::GetPeerLastMsgIdTest() {
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 80; msg_id++) {
    uint8_t meta[10];
    memset(meta, msg_id, 10);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 10, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  uint64_t last_recv_msg_id, last_recv_seq_id;
  upstream_handler_->GetLastReceivedMsgId(queue_ids_[0], last_recv_msg_id,
                                          last_recv_seq_id);
  STREAMING_LOG(INFO) << "last_recv_msg_id: " << last_recv_msg_id
                      << " last_recv_seq_id: " << last_recv_seq_id;
  // This test use raw data, there is no msg id, so we check seq_id here.
  ASSERT_EQ(last_recv_msg_id, 80);
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::GetPeerLastMsgIdTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::DirectCallPerfTest() {
  RayFunction async_call_func(
      ray::Language::PYTHON,
      ray::FunctionDescriptorBuilder::FromVector(
          ray::Language::PYTHON, {"", "", "direct_call_perf_test_func", ""}));
  RayFunction sync_call_func(
      ray::Language::PYTHON,
      ray::FunctionDescriptorBuilder::FromVector(
          ray::Language::PYTHON, {"", "", "direct_call_perf_test_func", ""}));
  std::shared_ptr<DirectCallTransport> transport =
      std::make_shared<ray::streaming::DirectCallTransport>(
          peer_actor_id_, async_call_func, sync_call_func);
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id_);
  uint8_t data[1024];
  uint64_t count = 200000;
  while (count--) {
    uint64_t timestamp = current_sys_time_ms();
    memcpy(data, &timestamp, sizeof(uint64_t));
    std::unique_ptr<LocalMemoryBuffer> buffer =
        std::unique_ptr<LocalMemoryBuffer>(new LocalMemoryBuffer(data, 1024, true));
    transport->Send(std::move(buffer));
    std::this_thread::sleep_for(std::chrono::microseconds(10));
  }
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTest() {
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 80; msg_id++) {
    uint8_t meta[10];
    memset(meta, msg_id, 10);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 10, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  std::thread thread_mock_trigger_reconnect([this] {
    auto callback = ActorConnectionListener::GetInstance()->GetActorReconnectedCallback();
    callback(peer_actor_id_);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  if (thread_mock_trigger_reconnect.joinable()) {
    thread_mock_trigger_reconnect.join();
  }
  STREAMING_LOG(INFO)
      << "StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTestTcpKill() {
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // message id starts from 1
  int msg_id = 1;
  while (true) {
    uint8_t meta[10];
    memset(meta, msg_id, 10);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 10, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    msg_id++;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  std::thread thread_mock_trigger_reconnect([] {
    auto callback = ActorConnectionListener::GetInstance()->GetActorReconnectedCallback();
    // callback(peer_actor_id_);
  });

  STREAMING_LOG(INFO)
      << "StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTest sleep";
  std::this_thread::sleep_for(std::chrono::milliseconds(1000 * 60 * 60));
  if (thread_mock_trigger_reconnect.joinable()) {
    thread_mock_trigger_reconnect.join();
  }
  STREAMING_LOG(INFO)
      << "StreamingQueueUpStreamTestSuite::DirectCallConnectionBrokenTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::MultipleArgs() {
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  upstream_handler_->CreateUpstreamQueue(queue_ids_[0], param, 20480, 20480);
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  /// Note: Clear hostname to disable collocate.
  queue->SetPeerHostname("");

  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 100; msg_id++) {
    uint8_t meta[64];
    memset(meta, msg_id, 64);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 64, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::CollocateSendTest() {
  // Sleep 2s, queue should not exist when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 20480, 20480);
  }
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 100; msg_id++) {
    uint8_t meta[64];
    memset(meta, msg_id, 64);
    uint8_t data[100];
    memset(data, msg_id, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    STREAMING_LOG(INFO) << "Writer User Push item msg_id: " << msg_id;
    ASSERT_TRUE(queue->Push(meta, 64, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::CollocateFOTest() {
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::CollocateFOTest";
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    upstream_handler_->CreateUpstreamQueue(queue_id, param, 20480, 20480);
  }
  std::shared_ptr<WriterQueue> queue = upstream_handler_->GetUpQueue(queue_ids_[0]);
  while (!queue->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // Sleep 2s, No valid data when reader pull
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  // message id starts from 1
  for (int msg_id = 1; msg_id <= 100; msg_id++) {
    uint8_t meta[64];
    memset(meta, msg_id % 255, 64);
    uint8_t data[100];
    memset(data, msg_id % 255, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue->GetBufferPool(), data, 100);
    ASSERT_TRUE(queue->Push(meta, 64, data_buffer.Data(), data_buffer.Size(),
                            current_sys_time_ms(), msg_id,
                            msg_id) == StreamingStatus::OK);
    queue->Send();
  }
  STREAMING_LOG(INFO) << "Writer User Push 100 msg done";
  queue.reset();
  upstream_handler_->DeleteUpstreamQueue(queue_ids_[0]);

  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<WriterQueue> queue2 =
      upstream_handler_->CreateUpstreamQueue(queue_ids_[0], param, 20480, 20480);
  while (!queue2->IsInitialized()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  for (int msg_id = 101; msg_id <= 200; msg_id++) {
    uint8_t meta[64];
    memset(meta, msg_id % 255, 64);
    uint8_t data[100];
    memset(data, msg_id % 255, 100);
    MemoryBuffer data_buffer = util::CopyToBufferPool(queue2->GetBufferPool(), data, 100);
    ASSERT_TRUE(queue2->Push(meta, 64, data_buffer.Data(), data_buffer.Size(),
                             current_sys_time_ms(), msg_id,
                             msg_id) == StreamingStatus::OK);
    queue2->Send();
  }
  STREAMING_LOG(INFO) << "Writer User Push 200 msg done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueUpStreamTestSuite::GetPeerHostnameTest() {
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    std::shared_ptr<WriterQueue> queue =
        upstream_handler_->CreateUpstreamQueue(queue_id, param, 10240, 10240);
  }

  for (auto &queue_id : queue_ids_) {
    while (!upstream_handler_->GetUpQueue(queue_id)->IsInitialized()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    ASSERT_EQ(upstream_handler_->GetUpQueue(queue_id)->GetPeerHostName(),
              StreamingUtility::GetHostname());
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "StreamingQueueUpStreamTestSuite::GetPeerHostnameTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::SetUp() {
  ray::CoreWorkerProcess::SetCurrentThreadWorkerId(worker_id_);
  downstream_handler_ =
      std::make_shared<DownstreamQueueMessageHandler>(plasma_store_socket_);
  downstream_handler_->Start();
  auto receiver = std::make_shared<MockDataReader>(downstream_handler_);
  SetReceiver(receiver);
}

void StreamingQueueDownStreamTestSuite::TearDown() {
  if (downstream_handler_) {
    downstream_handler_->Stop();
  }
}

void StreamingQueueDownStreamTestSuite::GetQueueTest() {
  ObjectID &queue_id = queue_ids_[0];
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  downstream_handler_->CreateDownstreamQueue(queue_id, param);

  bool is_upstream_first_pull_ = false;
  uint64_t count;
  downstream_handler_->PullQueue(queue_id, 0, 1, is_upstream_first_pull_, count,
                                 10 * 1000);

  ASSERT_TRUE(is_upstream_first_pull_);

  downstream_handler_->PullQueue(queue_id, 0, 1, is_upstream_first_pull_, count,
                                 10 * 1000);
  ASSERT_FALSE(is_upstream_first_pull_);

  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::GetQueueTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::PullPeerAsyncTest() {
  ObjectID &queue_id = queue_ids_[0];
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_id, param);

  bool is_first_pull;
  uint64_t bundle_count;
  downstream_handler_->PullQueue(queue_id, 0, 1, is_first_pull, bundle_count, 10 * 1000);
  uint64_t count = 0;
  uint8_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    QueueItem item = queue->PopPendingBlockTimeout(1000 * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      data = nullptr;
      data_size = 0;
    } else {
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }
    STREAMING_LOG(INFO) << " data is " << data[0] << " size is " << data_size;
    for (uint32_t i = 0; i < data_size; i++) {
      ASSERT_EQ(data[i], msg_id);
    }

    count++;
    if (count == 80) {
      bool is_upstream_first_pull;
      msg_id = 50;
      uint64_t bundle_count;
      downstream_handler_->PullPeerAsync(queue_id, 0, 50, is_upstream_first_pull,
                                         bundle_count, 1000);
      continue;
    }

    msg_id++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count == 110) {
      break;
    }
  }

  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::PullPeerAsyncTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::DeleteQueueTest() {
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    std::shared_ptr<ReaderQueue> queue =
        downstream_handler_->CreateDownstreamQueue(queue_id, param);

    bool is_first_pull;
    uint64_t count;
    downstream_handler_->PullQueue(queue_id, 0, 1, is_first_pull, count, 10 * 1000);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(3000));

  downstream_handler_->DeleteDownstreamQueue(queue_ids_[0]);

  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::DeleteQueueTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::GetPeerLastMsgIdTest() {
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  RAY_IGNORE_EXPR(downstream_handler_->CreateDownstreamQueue(queue_ids_[0], param));

  // Just wait
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::DirectCallPerfTest() {
  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::DirectCallPerfTest";
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::DirectCallConnectionBrokenTest() {
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_ids_[0], param);

  uint64_t count = 0;
  uint8_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    QueueItem item = queue->PopPendingBlockTimeout(1000 * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      data = nullptr;
      data_size = 0;
    } else {
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }

    for (uint32_t i = 0; i < data_size; i++) {
      ASSERT_EQ(data[i], msg_id);
    }

    count++;
    msg_id++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count == 80) {
      msg_id = 2;
      queue->OnConsumed(1, -1);
      STREAMING_LOG(INFO) << "reader notify msgid: 1";
    }

    if (count == 2 * 80 - 1) {
      break;
    }
  }
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::DirectCallConnectionBrokenTestTcpKill() {
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_ids_[0], param);

  uint64_t count = 0;
  uint64_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    QueueItem item = queue->PopPendingBlockTimeout(1000 * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      data = nullptr;
      data_size = 0;
    } else {
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }

    for (uint32_t i = 0; i < data_size; i++) {
      // ASSERT_EQ(data[i], msg_id);
    }

    count++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count % 20 == 0) {
      queue->OnConsumed(item.MsgIdEnd(), -1);
      STREAMING_LOG(INFO) << "reader notify msgid: " << item.MsgIdEnd();
    }
    msg_id++;
  }
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::CollocateSendTest() {
  ObjectID &queue_id = queue_ids_[0];
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_id, param);

  bool is_first_pull;
  uint64_t bundle_count;
  downstream_handler_->PullQueue(queue_id, 0, 1, is_first_pull, bundle_count, 10 * 1000);
  uint64_t count = 0;
  uint64_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    uint8_t *meta = nullptr;
    uint32_t meta_size = 0;
    uint64_t timeout_ms = 1000;
    QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      meta = nullptr;
      meta_size = 0;
      data = nullptr;
      data_size = 0;
    } else {
      meta = item.MetaBuffer()->Data();
      meta_size = item.MetaBuffer()->Size();
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (meta == nullptr || data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }

    for (uint32_t i = 0; i < meta_size; i++) {
      ASSERT_EQ(meta[i], msg_id);
    }
    for (uint32_t i = 0; i < data_size; i++) {
      ASSERT_EQ(data[i], msg_id);
    }
    count++;
    msg_id++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count == 100) {
      break;
    }
  }

  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::CollocateSendTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::MultipleArgs() {
  ObjectID &queue_id = queue_ids_[0];
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_id, param);

  // std::this_thread::sleep_for(std::chrono::milliseconds(500000));
  bool is_first_pull;
  uint64_t bundle_count;
  downstream_handler_->PullQueue(queue_id, 0, 1, is_first_pull, bundle_count, 10 * 1000);
  uint64_t count = 0;
  uint8_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    uint8_t *meta = nullptr;
    uint32_t meta_size = 0;
    uint64_t timeout_ms = 1000;
    QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      meta = nullptr;
      meta_size = 0;
      data = nullptr;
      data_size = 0;
    } else {
      meta = item.MetaBuffer()->Data();
      meta_size = item.MetaBuffer()->Size();
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (meta == nullptr || data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }

    for (uint32_t i = 0; i < meta_size; i++) {
      EXPECT_EQ(meta[i], msg_id);
    }
    for (uint32_t i = 0; i < data_size; i++) {
      EXPECT_EQ(data[i], msg_id);
    }
    count++;
    msg_id++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count == 100) {
      break;
    }
  }

  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::MultipleArgs done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::CollocateFOTest() {
  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::CollocateFOTest";
  ObjectID &queue_id = queue_ids_[0];
  const StreamingQueueInitialParameter param{
      peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
      StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
  std::shared_ptr<ReaderQueue> queue =
      downstream_handler_->CreateDownstreamQueue(queue_id, param);

  bool is_first_pull;
  uint64_t bundle_count;
  downstream_handler_->PullQueue(queue_id, 0, 1, is_first_pull, bundle_count, 10 * 1000);
  uint64_t count = 0;
  uint64_t msg_id = 1;
  while (true) {
    uint8_t *data = nullptr;
    uint32_t data_size = 0;
    uint8_t *meta = nullptr;
    uint32_t meta_size = 0;
    uint64_t timeout_ms = 1000;
    QueueItem item = queue->PopPendingBlockTimeout(timeout_ms * 1000);
    if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
      STREAMING_LOG(INFO) << "GetQueueItem timeout.";
      meta = nullptr;
      meta_size = 0;
      data = nullptr;
      data_size = 0;
    } else {
      meta = item.MetaBuffer()->Data();
      meta_size = item.MetaBuffer()->Size();
      data = item.Buffer()->Data();
      data_size = item.Buffer()->Size();
    }

    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (meta == nullptr || data == nullptr) {
      STREAMING_LOG(INFO) << "[Reader] data null";
      continue;
    }

    for (uint32_t i = 0; i < meta_size; i++) {
      ASSERT_EQ(meta[i], msg_id % 255) << i;
    }
    for (uint32_t i = 0; i < data_size; i++) {
      ASSERT_EQ(data[i], msg_id % 255);
    }
    count++;
    msg_id++;
    STREAMING_LOG(INFO) << "[Reader] count: " << count;
    if (count % 10 == 0) {
      queue->OnConsumed(item.MsgIdEnd(), -1);
    }
    if (count == 200) {
      break;
    }
    /// Sleep to slow down consumption speed, when upstream side fo, the downstream is
    /// still consuming items in the old object.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }

  STREAMING_LOG(INFO) << "StreamingQueueDownStreamTestSuite::CollocateSendTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueDownStreamTestSuite::GetPeerHostnameTest() {
  /// Clear receiver, makes it looks like the downstream handler has not initialized.
  SetReceiver(nullptr);
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  SetReceiver(std::make_shared<MockDataReader>(downstream_handler_));
  /// Shuffle queues sequence, so the queue can be created randomly.
  std::random_shuffle(queue_ids_.begin(), queue_ids_.end());
  for (auto &queue_id : queue_ids_) {
    const StreamingQueueInitialParameter param{
        peer_actor_id_, StreamingQueueTestSuite::ASYNC_CALL_FUNC,
        StreamingQueueTestSuite::SYNC_CALL_FUNC, false, false};
    downstream_handler_->CreateDownstreamQueue(queue_id, param);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Just wait
  std::this_thread::sleep_for(std::chrono::milliseconds(30000));
  SetReceiver(nullptr);
  status_ = true;
}
}  // namespace streaming
}  // namespace ray
