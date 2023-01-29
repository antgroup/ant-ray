#include "streaming_exactly_same_writer_tests.h"

namespace ray {
namespace streaming {

void StreamingExactlySameWriterTestSuite::RemoveAllMetaFile(
    const std::vector<ray::ObjectID> &q_list, uint64_t max_checkpoint_id) {
  std::string fake_dir = "/tmp/fake";
  std::shared_ptr<StreamingFileIO> delete_handler(
#ifdef USE_PANGU
      new StreamingPanguFileSystem(fake_dir, true));
  StreamingPanguFileSystem::Init();
  std::string store_prefix = "/zdfs_test/";
#else
      new StreamingLocalFileSystem(fake_dir, true));
  std::string store_prefix = "/tmp/";
#endif

  for (auto &q_item : q_list) {
    for (uint64_t i = 0; i <= max_checkpoint_id; ++i) {
      delete_handler->Delete(store_prefix + q_item.Hex() + "_" + std::to_string(i));
    }
  }

#ifdef USE_PANGU
  StreamingPanguFileSystem::Destory();
#endif
}

void StreamingExactlySameWriterTestSuite::TestWriteMessageToBufferRing(
    DataWriter *writer_client, const std::vector<ray::ObjectID> &q_list) {
  uint64_t rollback_checkpoint_id =
      writer_client->GetConfig().GetStreamingRollbackCheckpointId();
  uint32_t i = 1 + rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL;
  const uint8_t temp_data[] = {1};
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
      writer_client->BroadcastBarrier(i / MESSAGE_BARRIER_INTERVAL,
                                      i / MESSAGE_BARRIER_INTERVAL, temp_data, 1);
      // Sleep for generating empty message bundle
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ++i;
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5 * 1000));
}

void StreamingExactlySameWriterTestSuite::streaming_strategy_test(
    std::shared_ptr<RuntimeContext> &runtime_context,
    const std::vector<ray::ObjectID> &queue_id_vec, DataWriter **writer_client_ptr,
    QueueCreationType queue_creation_type, StreamingRole replay_role,
    bool remove_meta_file) {
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  *writer_client_ptr = new DataWriter(runtime_context);

  STREAMING_LOG(INFO) << "start store first";
  for (size_t i = 0; i < queue_ids_.size(); ++i) {
    STREAMING_LOG(INFO) << " qid hex => " << queue_ids_[i].Hex();
  }
  STREAMING_LOG(INFO) << "Writer Setup.";
  uint64_t queue_size = 1 * 1000 * 1000;
  std::vector<uint64_t> queue_size_vec(queue_ids_.size(), queue_size);
  auto writer_client = *writer_client_ptr;
  uint64_t rollback_checkpoint_id =
      runtime_context->config_.GetStreamingRollbackCheckpointId();
  std::vector<uint64_t> channel_msg_id_vec(
      queue_ids_.size(), rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL);
  runtime_context->config_.SetStreamingRole(replay_role);
  runtime_context->config_.SetQueueType("streaming_queue");
  writer_client->Init(
      queue_ids_, init_params, channel_msg_id_vec, queue_size_vec,
      std::vector<QueueCreationType>(channel_msg_id_vec.size(), queue_creation_type));
  writer_client->Run();
  std::thread test_loop_thread(
      &StreamingExactlySameWriterTestSuite::TestWriteMessageToBufferRing, this,
      writer_client, std::ref(queue_id_vec));

  std::thread timeout_thread([]() {
    std::this_thread::sleep_for(std::chrono::seconds(3 * 60));
    STREAMING_LOG(WARNING) << "test timeout";
    exit(1);
  });
  timeout_thread.detach();
  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));

  if (test_loop_thread.joinable()) {
    test_loop_thread.join();
  }
  if (runtime_context->config_.IsExactlySame() && remove_meta_file) {
    writer_client->Stop();
    // Sleep 50ms for crashing in pangu causeof write empty message and remove
    // file in multithreads
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    RemoveAllMetaFile(queue_id_vec, MESSAGE_BOUND_SIZE / MESSAGE_BARRIER_INTERVAL);
  }
}

void StreamingExactlySameWriterTestSuite::StreamingExactlySameSourceTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(5);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  DataWriter *writer_client = nullptr;
  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Operator";
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_SAME);
  runtime_context->config_.SetStreamingPersistenceCheckpointMaxCnt(100);

  STREAMING_LOG(INFO) << "####Writer first round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &writer_client);
  delete writer_client;
  STREAMING_LOG(INFO) << "####Writer first round end.";

  uint64_t checkpoint_id = param_;
  runtime_context->config_.SetStreamingRollbackCheckpointId(checkpoint_id);

  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Source checkpoint_id: "
                      << checkpoint_id;

  STREAMING_LOG(INFO) << "####Writer second round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &writer_client,
                          QueueCreationType::RECREATE_AND_CLEAR, StreamingRole::SOURCE,
                          true);

  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));
  delete writer_client;
  STREAMING_LOG(INFO) << "####Writer second round end.";
  writer_client = nullptr;
  status_ = true;
}

void StreamingExactlySameWriterTestSuite::StreamingExactlySameOperatorTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(5);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  DataWriter *writer_client = nullptr;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Operator";
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_SAME);

  runtime_context->config_.SetStreamingPersistenceCheckpointMaxCnt(100);
  STREAMING_LOG(INFO) << "####Writer first round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &writer_client);
  delete writer_client;
  STREAMING_LOG(INFO) << "####Writer first round end.";

  uint64_t checkpoint_id = param_;
  STREAMING_LOG(INFO) << "streaming_exactly_same_operator_test checkpoint_id: "
                      << checkpoint_id;
  runtime_context->config_.SetStreamingRollbackCheckpointId(checkpoint_id);
  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Operator";

  STREAMING_LOG(INFO) << "####Writer second round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &writer_client,
                          QueueCreationType::RECREATE_AND_CLEAR, StreamingRole::TRANSFORM,
                          true);
  delete writer_client;
  STREAMING_LOG(INFO) << "####Writer second round end.";

  writer_client = nullptr;
  status_ = true;
}

void StreamingExactlySameReaderTestSuite::RemoveAllMetaFile(
    const std::vector<ray::ObjectID> &q_list, uint64_t max_checkpoint_id) {
  std::string fake_dir = "/tmp/fake";
  std::shared_ptr<StreamingFileIO> delete_handler(
#ifdef USE_PANGU
      new StreamingPanguFileSystem(fake_dir, true));
  StreamingPanguFileSystem::Init();
  std::string store_prefix = "/zdfs_test/";
#else
      new StreamingLocalFileSystem(fake_dir, true));
  std::string store_prefix = "/tmp/";
#endif

  for (auto &q_item : q_list) {
    for (uint64_t i = 0; i <= max_checkpoint_id; ++i) {
      delete_handler->Delete(store_prefix + q_item.Hex() + "_" + std::to_string(i));
    }
  }

#ifdef USE_PANGU
  StreamingPanguFileSystem::Destory();
#endif
}
void StreamingExactlySameReaderTestSuite::ReaderLoopForward(
    DataReader *reader_client, const std::vector<ray::ObjectID> &queue_id_vec,
    std::vector<StreamingMessageBundlePtr> &bundle_vec) {
  uint64_t rollback_checkpoint_id =
      reader_client->GetConfig().GetStreamingRollbackCheckpointId();
  const uint64_t expect_received_message_cnt =
      queue_id_vec.size() *
      (MESSAGE_BOUND_SIZE - rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL);

  STREAMING_LOG(INFO) << "expect_received_message_cnt: " << expect_received_message_cnt;
  uint64_t received_message_cnt = 0;
  std::unordered_map<ray::ObjectID, uint64_t> queue_last_cp_id;

  for (auto &q_id : queue_id_vec) {
    queue_last_cp_id[q_id] = 0;
  }

  while (true) {
    std::shared_ptr<StreamingReaderBundle> msg;
    StreamingStatus read_status = reader_client->GetBundle(1000, msg);
    if (read_status != StreamingStatus::OK || msg->IsEmpty()) {
      STREAMING_LOG(DEBUG) << "read bundle timeout";
      continue;
    }
    StreamingMessageBundlePtr bundle_ptr;
    bundle_ptr = StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());

    if (msg->meta->GetBundleType() == StreamingMessageBundleType::Barrier) {
      STREAMING_LOG(DEBUG) << "barrier message received => "
                           << msg->meta->GetMessageBundleTs();
      std::unordered_map<ObjectID, ConsumerChannelInfo> *offset_map;
      reader_client->GetOffsetInfo(offset_map);

      for (auto &q_id : queue_id_vec) {
        reader_client->NotifyConsumedItem((*offset_map)[q_id],
                                          (*offset_map)[q_id].current_message_id);
      }
      // writer_client->ClearCheckpoint(msg->last_barrier_id);

      continue;
    } else if (msg->meta->GetBundleType() == StreamingMessageBundleType::Empty) {
      STREAMING_LOG(DEBUG) << "empty message received => "
                           << msg->meta->GetMessageBundleTs();
      bundle_vec.push_back(bundle_ptr);
      continue;
    }

    std::list<StreamingMessagePtr> message_list;
    bundle_ptr->GetMessageList(message_list);
    bundle_vec.push_back(bundle_ptr);

    received_message_cnt += message_list.size();
    for (auto &item : message_list) {
      uint64_t i = item->GetMessageId();

      uint32_t buff_len = i % DEFAULT_STREAMING_MESSAGE_BUFFER_SIZE;
      if (i > MESSAGE_BOUND_SIZE) break;

      ASSERT_EQ(buff_len, item->PayloadSize());
      std::unique_ptr<uint8_t> compared_data(new uint8_t[buff_len]);
      for (uint32_t j = 0; j < item->PayloadSize(); ++j) {
        *(compared_data.get() + j) = j % 128;
      }
      ASSERT_EQ(std::memcmp(compared_data.get(), item->Payload(), item->PayloadSize()),
                0);
    }
    STREAMING_LOG(DEBUG) << "Received message count => " << received_message_cnt;
    if (received_message_cnt == expect_received_message_cnt) {
      break;
    }
  }
}

void StreamingExactlySameReaderTestSuite::streaming_strategy_test(
    std::shared_ptr<RuntimeContext> &runtime_context,
    const std::vector<ray::ObjectID> &queue_id_vec, DataReader **reader_client_ptr,
    std::vector<StreamingMessageBundlePtr> &bundle_vec,
    QueueCreationType queue_creation_type, StreamingRole replay_role,
    bool remove_meta_file) {
  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  STREAMING_LOG(INFO) << "start store first";
  for (size_t i = 0; i < queue_ids_.size(); ++i) {
    STREAMING_LOG(INFO) << " qid hex => " << queue_ids_[i].Hex();
  }

  STREAMING_LOG(INFO) << "Reader Setup.";
  auto reader_client = *reader_client_ptr;
  runtime_context->config_.SetStreamingRole(StreamingRole::SINK);
  runtime_context->config_.SetQueueType("streaming_queue");
  uint64_t rollback_checkpoint_id =
      runtime_context->config_.GetStreamingRollbackCheckpointId();
  std::vector<uint64_t> msg_id_vec(queue_ids_.size(),
                                   rollback_checkpoint_id * MESSAGE_BARRIER_INTERVAL);
  std::vector<TransferCreationStatus> creation_status;
  *reader_client_ptr = new DataReader(runtime_context);
  reader_client->Init(queue_ids_, init_params, msg_id_vec, -1, creation_status);
  ReaderLoopForward(reader_client, queue_ids_, bundle_vec);
  // if (test_loop_thread.joinable()) {
  //   test_loop_thread.join();
  // }
  if (runtime_context->config_.IsExactlySame() && remove_meta_file) {
    // writer_client->Stop();
    // Sleep 50ms for crashing in pangu causeof write empty message and remove
    // file in multithreads
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    RemoveAllMetaFile(queue_id_vec, MESSAGE_BOUND_SIZE / MESSAGE_BARRIER_INTERVAL);
  }
}

void StreamingExactlySameReaderTestSuite::StreamingExactlySameSourceTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(5);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  DataReader *reader_client = nullptr;
  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Operator";
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_SAME);

  std::vector<StreamingMessageBundlePtr> first_bundle_vec;
  runtime_context->config_.SetStreamingPersistenceCheckpointMaxCnt(100);
  STREAMING_LOG(INFO) << "####Reader first round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &reader_client, first_bundle_vec);
  delete reader_client;
  STREAMING_LOG(INFO) << "####Reader first round end.";

  std::this_thread::sleep_for(std::chrono::milliseconds(10 * 1000));

  // wait for writer to finish
  std::this_thread::sleep_for(std::chrono::seconds(5));
  uint64_t checkpoint_id = param_;
  std::vector<StreamingMessageBundlePtr> second_bundle_vec;
  runtime_context->config_.SetStreamingRollbackCheckpointId(checkpoint_id);

  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Source";
  STREAMING_LOG(INFO) << "####Reader second round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &reader_client, second_bundle_vec,
                          QueueCreationType::RECREATE_AND_CLEAR, StreamingRole::SOURCE,
                          true);

  // ASSERT_EQ(first_bundle_vec.size(), second_bundle_vec.size());
  uint32_t rollback_meta_vec_size = second_bundle_vec.size();
  uint32_t original_meta_vec_size = first_bundle_vec.size();
  uint64_t meta_ts = 0;
  STREAMING_LOG(INFO) << "original meta vec size " << original_meta_vec_size
                      << ", rollback_meta_vec_size " << rollback_meta_vec_size;

  for (uint32_t i = 0; i < rollback_meta_vec_size; ++i) {
    uint32_t index = original_meta_vec_size - rollback_meta_vec_size + i;
    if (!first_bundle_vec[index]->operator==(second_bundle_vec[i].get())) {
      STREAMING_LOG(INFO) << "i : " << i << " , index => " << index << ", "
                          << first_bundle_vec[index]->ToString() << "|"
                          << second_bundle_vec[i]->ToString();
      STREAMING_CHECK(false);
    }
    ASSERT_TRUE(first_bundle_vec[index]->operator==(second_bundle_vec[i].get()));
    ASSERT_TRUE(meta_ts <= first_bundle_vec[index]->GetMessageBundleTs());
    meta_ts = first_bundle_vec[index]->GetMessageBundleTs();
  }

  delete reader_client;
  reader_client = nullptr;
  STREAMING_LOG(INFO) << "####Reader second round end.";
  STREAMING_LOG(INFO)
      << "StreamingExactlySameReaderTestSuite::StreamingExactlySameSourceTest done 3";

  status_ = true;
}

void StreamingExactlySameReaderTestSuite::StreamingExactlySameOperatorTest() {
  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(5);
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);
  DataReader *reader_client = nullptr;

  STREAMING_LOG(INFO) << "Streaming Strategy => EXACTLY_SAME Operator";
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_SAME);

  std::vector<StreamingMessageBundlePtr> first_bundle_vec;
  runtime_context->config_.SetStreamingPersistenceCheckpointMaxCnt(100);

  STREAMING_LOG(INFO) << "####Reader first round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &reader_client, first_bundle_vec);
  delete reader_client;
  STREAMING_LOG(INFO) << "####Reader first round end.";

  // wait for writer to finish
  std::this_thread::sleep_for(std::chrono::seconds(10));
  uint64_t checkpoint_id = param_;
  STREAMING_LOG(INFO) << "streaming_exactly_same_operator_test checkpoint_id: "
                      << checkpoint_id;
  std::vector<StreamingMessageBundlePtr> second_bundle_vec;
  runtime_context->config_.SetStreamingRollbackCheckpointId(checkpoint_id);
  STREAMING_LOG(INFO) << "Streaming Replay Start => EXACTLY_SAME Operator";

  STREAMING_LOG(INFO) << "####Reader second round start.";
  streaming_strategy_test(runtime_context, queue_ids_, &reader_client, second_bundle_vec,
                          QueueCreationType::RECREATE_AND_CLEAR, StreamingRole::TRANSFORM,
                          true);
  STREAMING_LOG(INFO) << "####Reader second round end.";
  // ASSERT_EQ(first_bundle_vec.size(), second_bundle_vec.size());
  uint32_t rollback_meta_vec_size = second_bundle_vec.size();
  uint32_t original_meta_vec_size = first_bundle_vec.size();
  uint64_t meta_ts = 0;
  for (uint32_t i = 0; i < rollback_meta_vec_size; ++i) {
    uint32_t index = original_meta_vec_size - rollback_meta_vec_size + i;
    if (!first_bundle_vec[index]->operator==(second_bundle_vec[i].get())) {
      STREAMING_LOG(INFO) << "i : " << i << " , index => " << index << ", "
                          << first_bundle_vec[index]->ToString() << "|"
                          << second_bundle_vec[i]->ToString();
    }
    ASSERT_TRUE(first_bundle_vec[index]->operator==(second_bundle_vec[i].get()));
    ASSERT_TRUE(meta_ts <= first_bundle_vec[index]->GetMessageBundleTs());
    meta_ts = first_bundle_vec[index]->GetMessageBundleTs();
  }

  delete reader_client;
  reader_client = nullptr;
  status_ = true;
}

}  // namespace streaming
}  // namespace ray