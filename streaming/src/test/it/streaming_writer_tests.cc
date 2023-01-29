#include "streaming_writer_tests.h"

namespace ray {
namespace streaming {

void StreamingQueueWriterTestSuite::TestWriteMessageToBufferRing(
    std::shared_ptr<DataWriter> writer_client, std::vector<ray::ObjectID> &q_list) {
  const uint8_t temp_data[] = {1, 2, 4, 5};

  uint32_t i = 1;
  while (i <= MESSAGE_BOUND_SIZE) {
    for (auto &q_id : q_list) {
      uint64_t buffer_len = (i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE);
      auto *data = new uint8_t[buffer_len];
      for (uint32_t j = 0; j < buffer_len; ++j) {
        data[j] = j % 128;
      }
      writer_client->WriteMessageToBufferRing(q_id, data, buffer_len,
                                              StreamingMessageType::Message);
      delete[] data;
    }
    if (i % MESSAGE_BARRIER_INTERVAL == 0) {
      STREAMING_LOG(DEBUG) << "broadcast barrier, barrier id => " << i;
      writer_client->BroadcastBarrier(i / MESSAGE_BARRIER_INTERVAL,
                                      i / MESSAGE_BARRIER_INTERVAL, temp_data, 4);
    }
    ++i;
  }

  // Wait a while
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}

void StreamingQueueWriterTestSuite::StreamingWriterStrategyTest(
    std::shared_ptr<RuntimeContext> &runtime_context) {
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));
  uint64_t queue_size = 1 * 1000 * 1000;
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);
  STREAMING_LOG(INFO) << "data_writer Init done";

  data_writer->Run();
  // std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  std::thread test_loop_thread(
      &StreamingQueueWriterTestSuite::TestWriteMessageToBufferRing, this, data_writer,
      std::ref(queue_ids_));
  // test_loop_thread.detach();
  if (test_loop_thread.joinable()) {
    test_loop_thread.join();
  }
  SetReceiver(nullptr);
}

void StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  // Close empty message to avoid send item to downstream when downstream has destroy
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  StreamingWriterStrategyTest(runtime_context);

  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::StreamingWriterExactlyOnceTest";
  status_ = true;
}

void StreamingQueueWriterTestSuite::StreamingWriterAtLeastOnceTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  StreamingWriterStrategyTest(runtime_context);

  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::StreamingWriterAtLeastOnceTest";
  std::this_thread::sleep_for(std::chrono::milliseconds(20000));
  status_ = true;
}

void StreamingQueueWriterTestSuite::StreamingRecreateTest() {
  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::StreamingRecreateTest";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "####Writer first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  uint64_t queue_size = 1 * 1000 * 1000;
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);

  data_writer->Run();

  uint8_t first_data[] = {1, 2, 3};
  data_writer->WriteMessageToBufferRing(queue_ids_[0], first_data, 3);

  STREAMING_LOG(INFO) << "####Writer first round end.";
  // Wait reader read
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  STREAMING_LOG(INFO) << "####Writer second round start.";
  SetReceiver(nullptr);
  auto runtime_context_recreate = std::make_shared<RuntimeContext>();
  runtime_context_recreate->config_ = runtime_context->config_;
  data_writer.reset(new DataWriter(runtime_context_recreate));
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size),
                    std::vector<QueueCreationType>(
                        queue_ids_.size(), QueueCreationType::RECREATE_AND_CLEAR));
  SetReceiver(data_writer);

  data_writer->Run();

  uint8_t second_data[] = {4, 5, 6};
  data_writer->WriteMessageToBufferRing(queue_ids_[0], second_data, 3);

  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
  STREAMING_LOG(INFO) << "####Writer second round end.";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueWriterTestSuite::ElasticBufferTest() {
  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::ElasticBufferTest";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);
  STREAMING_LOG(INFO) << "####Writer first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetElasticBufferEnable(true);
  runtime_context->config_.SetElasticBufferMaxSaveBufferSize(1000);
  runtime_context->config_.SetElasticBufferFlushBufferSize(800);
  runtime_context->config_.SetElasticBufferFileCacheSize(1 << 20);
  runtime_context->config_.SetElasticBufferMaxFileNum(1000);
  runtime_context->config_.SetElasticBufferFileDirectory("/tmp/es_buffer");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));

  const uint8_t temp_data[] = {1, 2, 4, 5};
  const uint32_t barrier_interval = 100;

  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  uint64_t queue_size = 100 * 1000;  /// byte size of byte data
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);

  data_writer->Run();

  uint8_t first_data[100];
  std::memset(first_data, 'H', sizeof(first_data));
  for (uint32_t i = 1; i <= 1000; ++i) {
    data_writer->WriteMessageToBufferRing(queue_ids_[0], first_data, 100,
                                          StreamingMessageType::Message);
    // if (i % barrier_interval == 0) {
    if (i == 100) {
      STREAMING_LOG(INFO) << "broadcast barrier, barrier id => " << i;
      std::this_thread::sleep_for(std::chrono::milliseconds(1500));
      data_writer->BroadcastBarrier(i / barrier_interval, i / barrier_interval, temp_data,
                                    4);
    }
  }
  STREAMING_LOG(DEBUG) << "[ES DEBUG] first write done";
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  STREAMING_LOG(DEBUG) << "[ES DEBUG] clear checkpoint begin";
  data_writer->ClearCheckpoint(0, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(30 * 1000));
  STREAMING_LOG(INFO) << "####Writer second round end.";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueWriterTestSuite::NoSkipSourceBarrier() {
  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::NoSkipSourceBarrier";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  std::vector<uint64_t> channel_msg_id_vec(queue_ids_.size(), 0);

  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetStreamingRole(StreamingRole::SOURCE);
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));

  uint64_t queue_size = 1 * 1000 * 1000;
  data_writer->Init(queue_ids_, init_params, channel_msg_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  data_writer->Run();

  STREAMING_LOG(INFO) << "####Writer first round start.";
  uint8_t first_data[] = {1, 2, 3};
  data_writer->WriteMessageToBufferRing(queue_ids_[0], first_data, 3);
  STREAMING_LOG(INFO) << "####Writer first round end.";
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  data_writer.reset();

  std::shared_ptr<DataWriter> writer_recreate(new DataWriter(runtime_context));
  writer_recreate->Init(queue_ids_, init_params, channel_msg_id_vec,
                        std::vector<uint64_t>(channel_msg_id_vec.size(), queue_size),
                        std::vector<QueueCreationType>(channel_msg_id_vec.size(),
                                                       QueueCreationType::RECREATE));
  writer_recreate->Run();

  STREAMING_LOG(INFO) << "####Writer second round start.";
  const uint8_t second_data[] = {4, 5, 6};
  writer_recreate->BroadcastBarrier(1, 1, second_data, 3);
  STREAMING_LOG(INFO) << "####Writer second round end.";
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  status_ = true;
}

// 1. writer start, send empty message, but no message write
// 2. reader start
// 3. reader reset (FO)
// 4. writer writer message
// check whether reader can receive data or not.
void StreamingQueueWriterTestSuite::StreamingRecreateWithoutDataTest() {
  STREAMING_LOG(INFO)
      << "StreamingQueueWriterTestSuite::StreamingRecreateWithoutDataTest";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  uint64_t queue_size = 1 * 1000 * 1000;
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);

  data_writer->Run();

  // Wait reader read
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  ///////////////////////////////////////////////////////////////////////
  STREAMING_LOG(INFO) << "####Writer begin write: " << queue_ids_[0];
  uint8_t first_data[] = {1, 2, 3};
  for (auto &queue_id : queue_ids_) {
    data_writer->WriteMessageToBufferRing(queue_id, first_data, 3);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
  STREAMING_LOG(INFO)
      << "StreamingQueueWriterTestSuite::StreamingRecreateWithoutDataTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueWriterTestSuite::StreamingEmptyMessageTest() {
  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::StreamingEmptyMessageTest";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));
  uint64_t queue_size = 1 * 1000 * 1000;
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);
  STREAMING_LOG(INFO) << "data_writer Init done";

  data_writer->Run();
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueWriterTestSuite::StreamingRecreateThrownTest() {
  STREAMING_LOG(INFO) << "StreamingQueueWriterTestSuite::StreamingRecreateThrownTest";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "####Writer first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetEmptyMessageTimeInterval(100);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataWriter> data_writer(new DataWriter(runtime_context));
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  uint64_t queue_size = 1 * 1000 * 1000;
  data_writer->Init(queue_ids_, init_params, channel_seq_id_vec,
                    std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(data_writer);

  data_writer->Run();

  uint8_t first_data[] = {1, 2, 3};
  data_writer->WriteMessageToBufferRing(queue_ids_[0], first_data, 3);

  STREAMING_LOG(INFO) << "####Writer first round end.";
  // Wait reader read
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  STREAMING_LOG(INFO) << "####Writer receiver destroy.";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueReaderTestSuite::ReaderLoopForward(
    std::shared_ptr<DataReader> reader_client, std::vector<ray::ObjectID> &queue_id_vec) {
  uint64_t received_message_cnt = 0;
  std::unordered_map<ray::ObjectID, uint64_t> queue_last_cp_id;

  for (auto &q_id : queue_id_vec) {
    queue_last_cp_id[q_id] = 0;
  }
  STREAMING_LOG(INFO) << "Start read message bundle";
  while (true) {
    std::shared_ptr<StreamingReaderBundle> msg;
    StreamingStatus st = reader_client->GetBundle(100, msg);

    STREAMING_LOG(DEBUG) << "read_status=" << st << ", msg = " << *msg;
    if (st != StreamingStatus::OK || msg->IsEmpty()) {
      STREAMING_LOG(DEBUG) << "read bundle timeout, status = " << (int)st;
      continue;
    }

    STREAMING_CHECK(msg.get() && msg->meta.get())
        << "read null pointer message, queue id => " << msg->from.Hex();

    if (msg->meta->GetBundleType() == StreamingMessageBundleType::Barrier) {
      STREAMING_LOG(DEBUG) << "barrier message received => "
                           << msg->meta->GetMessageBundleTs();
      std::unordered_map<ray::ObjectID, ConsumerChannelInfo> *offset_map;
      reader_client->GetOffsetInfo(offset_map);

      for (auto &q_id : queue_id_vec) {
        reader_client->NotifyConsumedItem((*offset_map)[q_id],
                                          (*offset_map)[q_id].current_message_id);
      }
      continue;
    } else if (msg->meta->GetBundleType() == StreamingMessageBundleType::Empty) {
      STREAMING_LOG(DEBUG) << "empty message received => "
                           << msg->meta->GetMessageBundleTs();
      continue;
    }

    StreamingMessageBundlePtr bundlePtr;
    bundlePtr = StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
    std::list<StreamingMessagePtr> message_list;
    bundlePtr->GetMessageList(message_list);
    STREAMING_LOG(DEBUG) << "message size => " << message_list.size()
                         << " from queue id => " << msg->from.Hex()
                         << " last message id => " << msg->meta->GetLastMessageId()
                         << " last barrier id => " << msg->last_barrier_id;
    if (reader_client->GetConfig().GetReliabilityLevel() ==
        ReliabilityLevel::EXACTLY_ONCE) {
      // check barrier for excatly once
      std::unordered_set<uint64_t> cp_id_set;
      cp_id_set.insert(msg->last_barrier_id);

      for (auto &q_id : queue_id_vec) {
        cp_id_set.insert(queue_last_cp_id[q_id]);
        STREAMING_LOG(DEBUG) << "q id " << q_id.Hex() << ", cp id=>"
                             << queue_last_cp_id[q_id];
      }
      STREAMING_LOG(DEBUG) << "cp set size =>" << cp_id_set.size();
      STREAMING_CHECK(cp_id_set.size() <= 2) << cp_id_set.size();

      queue_last_cp_id[msg->from] = msg->last_barrier_id;
    }

    received_message_cnt += message_list.size();
    for (auto &item : message_list) {
      uint64_t i = item->GetMessageId();
      STREAMING_LOG(DEBUG) << "--queueid=" << msg->from << " message id=" << i;

      uint32_t buff_len = i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE;
      if (i > MESSAGE_BOUND_SIZE) break;

      ASSERT_EQ(buff_len, item->PayloadSize());
      uint8_t *compared_data = new uint8_t[buff_len];
      for (uint32_t j = 0; j < item->PayloadSize(); ++j) {
        compared_data[j] = j % 128;
      }
      ASSERT_EQ(std::memcmp(compared_data, item->Payload(), item->PayloadSize()), 0);
      delete[] compared_data;
    }
    STREAMING_LOG(DEBUG) << "Received message count => " << received_message_cnt;
    if (reader_client->GetConfig().IsAtLeastOnce()) {
      if (received_message_cnt >= queue_id_vec.size() * MESSAGE_BOUND_SIZE) {
        STREAMING_LOG(INFO) << "received message count => " << received_message_cnt
                            << ", break";
        break;
      }
    } else {
      if (received_message_cnt == queue_id_vec.size() * MESSAGE_BOUND_SIZE) {
        STREAMING_LOG(INFO) << "received message count => " << received_message_cnt
                            << ", break";
        break;
      }
    }
  }
}

void StreamingQueueReaderTestSuite::StreamingReaderStrategyTest(
    std::shared_ptr<RuntimeContext> &runtime_context) {
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);
  ReaderLoopForward(reader, queue_ids_);
  SetReceiver(nullptr);
  STREAMING_LOG(INFO) << "Reader exit";
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingWriterExactlyOnceTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::StreamingWriterExactlyOnceTest";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  StreamingReaderStrategyTest(runtime_context);
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingWriterAtLeastOnceTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::StreamingWriterAtLeastOnceTest";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::AT_LEAST_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
  StreamingReaderStrategyTest(runtime_context);
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingRecreateTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::StreamingRecreateTest";
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "####Reader first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  const uint8_t first_data[] = {1, 2, 3};
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);
  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr =
      StreamingMessageBundle::FromBytes(message->MetaBuffer(), message->DataBuffer());
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  ASSERT_EQ(message_list.size(), 1);
  ASSERT_EQ(std::memcmp(message_list.front()->Payload(), first_data, 3), 0);

  STREAMING_LOG(INFO) << "####Reader first round end.";
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "####Reader second round start.";
  const uint8_t second_data[] = {4, 5, 6};
  SetReceiver(nullptr);
  auto runtime_context2 = std::make_shared<RuntimeContext>();
  runtime_context2->config_ = runtime_context->config_;
  reader.reset(new DataReader(runtime_context2));
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 =
      StreamingMessageBundle::FromBytes(message2->MetaBuffer(), message2->DataBuffer());
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  ASSERT_EQ(message_list2.size(), 1);
  ASSERT_EQ(std::memcmp(message_list2.front()->Payload(), second_data, 3), 0);
  STREAMING_LOG(INFO) << "####Reader second round end.";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueReaderTestSuite::ElasticBufferTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::ElasticBufferTest";
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "####Reader first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  uint8_t first_data[100];
  std::memset(first_data, 'H', 100);
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);

  int excepted_num = 1000;
  int recv_count = 0;
  while (recv_count < excepted_num) {
    std::shared_ptr<StreamingReaderBundle> message =
        std::make_shared<StreamingReaderBundle>();
    reader->GetBundle(-1, message);
    StreamingMessageBundlePtr msg_ptr =
        StreamingMessageBundle::FromBytes(message->MetaBuffer(), message->DataBuffer());
    if (msg_ptr->GetBundleType() == StreamingMessageBundleType::Barrier) {
      STREAMING_LOG(DEBUG) << "[ES DEBUG] Receieve a barrier continue";
      continue;
    }
    std::list<StreamingMessagePtr> message_list;
    msg_ptr->GetMessageList(message_list);
    recv_count += message_list.size();
    ASSERT_EQ(std::memcmp(message_list.front()->Payload(), first_data, 100), 0);
    STREAMING_LOG(INFO) << " Piece " << recv_count;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(3000));
  // ASSERT_EQ(message_list.size(), 1);
  STREAMING_LOG(INFO) << "####Reader first round end.";

  SetReceiver(nullptr);
  reader.reset(new DataReader(runtime_context));
  SetReceiver(reader);
  std::vector<uint64_t> start_msg_vec(queue_ids_.size(), 200);
  std::vector<TransferCreationStatus> tmp_status;
  reader->Init(queue_ids_, init_params, start_msg_vec, -1, tmp_status);

  recv_count = 0;
  excepted_num = 800;
  while (recv_count < excepted_num) {
    std::shared_ptr<StreamingReaderBundle> message2 =
        std::make_shared<StreamingReaderBundle>();
    reader->GetBundle(-1, message2);
    StreamingMessageBundlePtr msg_ptr2 =
        StreamingMessageBundle::FromBytes(message2->MetaBuffer(), message2->DataBuffer());
    std::list<StreamingMessagePtr> message_list2;
    msg_ptr2->GetMessageList(message_list2);
    // ASSERT_EQ(message_list2.size(), 1);
    ASSERT_EQ(std::memcmp(message_list2.front()->Payload(), first_data, 100), 0);
    recv_count += message_list2.size();
    STREAMING_LOG(INFO) << " Piece2 " << recv_count;
  }

  STREAMING_LOG(INFO) << "####Reader second round end.";

  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueReaderTestSuite::NoSkipSourceBarrier() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::NoSkipSourceBarrier";
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));

  reader->Init(queue_ids_, init_params, -1);
  STREAMING_LOG(INFO) << "####Reader first round start.";
  const uint8_t first_data[] = {1, 2, 3};
  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr =
      StreamingMessageBundle::FromBytes(message->MetaBuffer(), message->DataBuffer());
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  ASSERT_EQ(message_list.size(), 1);
  ASSERT_EQ(std::memcmp(message_list.front()->Payload(), first_data, 3), 0);
  STREAMING_LOG(INFO) << "####Reader first round end.";
  reader.reset();

  const uint8_t second_data[] = {4, 5, 6};
  std::shared_ptr<DataReader> recreate_reader(new DataReader(runtime_context));
  std::vector<uint64_t> queue_message_id({1});
  std::vector<TransferCreationStatus> creation_status;
  recreate_reader->Init(queue_ids_, init_params, queue_message_id, -1, creation_status);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  STREAMING_LOG(INFO) << "####Reader second round start.";
  recreate_reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 =
      StreamingMessageBundle::FromBytes(message2->MetaBuffer(), message2->DataBuffer());
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  ASSERT_EQ(message2->meta->IsBarrier(), true);
  ASSERT_EQ(message_list2.size(), 1);
  ASSERT_EQ(
      std::memcmp(message_list2.front()->Payload() + kBarrierHeaderSize, second_data, 3),
      0);
  STREAMING_LOG(INFO) << "####Reader second round end.";
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingRecreateWithoutDataTest() {
  STREAMING_LOG(INFO)
      << "StreamingQueueReaderTestSuite::StreamingRecreateWithoutDataTest";
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);
  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(1000, message);

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  const uint8_t first_data[] = {1, 2, 3};
  STREAMING_LOG(INFO) << "####Reader recreate";
  SetReceiver(nullptr);
  auto runtime_context2 = std::make_shared<RuntimeContext>();
  runtime_context2->config_ = runtime_context->config_;
  reader.reset(new DataReader(runtime_context2));
  SetReceiver(reader);
  reader->Init(queue_ids_, init_params, -1);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  STREAMING_LOG(INFO) << "####Reader begin read";
  reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 =
      StreamingMessageBundle::FromBytes(message2->MetaBuffer(), message2->DataBuffer());
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  ASSERT_EQ(message_list2.size(), 1);
  ASSERT_EQ(std::memcmp(message_list2.front()->Payload(), first_data, 3), 0);
  STREAMING_LOG(INFO)
      << "StreamingQueueReaderTestSuite::StreamingRecreateWithoutDataTest done";
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingEmptyMessageTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::StreamingEmptyMessageTest";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(50);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  // Set timer_interval to 200ms to get empty message
  reader->Init(queue_ids_, init_params, 200);
  SetReceiver(reader);
  std::shared_ptr<StreamingReaderBundle> msg;
  StreamingStatus st = reader->GetBundle(10 * 1000, msg);
  STREAMING_LOG(INFO) << "GetBundle st: " << st;
  STREAMING_CHECK(msg->MetaBuffer());
  STREAMING_CHECK(!msg->DataBuffer());
  STREAMING_CHECK(StreamingMessageBundle::FromBytes(msg->MetaBuffer(), nullptr));
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingQueueReaderTestSuite::StreamingRecreateThrownTest() {
  STREAMING_LOG(INFO) << "StreamingQueueReaderTestSuite::StreamingRecreateThrownTest";
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "####Reader first round start.";
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  std::shared_ptr<DataReader> reader(new DataReader(runtime_context));
  const uint8_t first_data[] = {1, 2, 3};
  reader->Init(queue_ids_, init_params, -1);
  SetReceiver(reader);
  std::shared_ptr<StreamingReaderBundle> message =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message);
  StreamingMessageBundlePtr msg_ptr =
      StreamingMessageBundle::FromBytes(message->MetaBuffer(), message->DataBuffer());
  std::list<StreamingMessagePtr> message_list;
  msg_ptr->GetMessageList(message_list);
  ASSERT_EQ(message_list.size(), 1);
  ASSERT_EQ(std::memcmp(message_list.front()->Payload(), first_data, 3), 0);

  STREAMING_LOG(INFO) << "####Reader first round end.";
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  STREAMING_LOG(INFO) << "####Reader second round start.";
  SetReceiver(nullptr);

  auto runtime_context2 = std::make_shared<RuntimeContext>();
  runtime_context2->config_ = runtime_context->config_;
  reader.reset(new DataReader(runtime_context2));
  reader->Init(queue_ids_, init_params, -1);
  SetReceiver(reader);
  std::shared_ptr<StreamingReaderBundle> message2 =
      std::make_shared<StreamingReaderBundle>();
  reader->GetBundle(-1, message2);
  StreamingMessageBundlePtr msg_ptr2 =
      StreamingMessageBundle::FromBytes(message2->MetaBuffer(), message2->DataBuffer());
  std::list<StreamingMessagePtr> message_list2;
  msg_ptr2->GetMessageList(message_list2);
  ASSERT_EQ(message_list2.size(), 1);
  ASSERT_EQ(std::memcmp(message_list2.front()->Payload(), first_data, 3), 0);
  STREAMING_LOG(INFO) << "####Reader second round end.";
  SetReceiver(nullptr);
  status_ = true;
}
}  // namespace streaming
}  // namespace ray