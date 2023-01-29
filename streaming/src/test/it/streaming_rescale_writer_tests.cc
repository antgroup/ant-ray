#include "streaming_rescale_writer_tests.h"

namespace ray {
namespace streaming {

void StreamingRescaleWriterTestSuite::PingMessage(std::shared_ptr<DataWriter> writer,
                                                  std::vector<ray::ObjectID> queue_id_vec,
                                                  uint8_t *data, size_t data_size) {
  STREAMING_LOG(INFO) << "Writer PingMessage";
  for (auto &q_id : queue_id_vec) {
    writer->WriteMessageToBufferRing(q_id, data, data_size,
                                     StreamingMessageType::Message);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}

void StreamingRescaleWriterTestSuite::PingGlobalBarrier(
    std::shared_ptr<DataWriter> writer, uint8_t *data, size_t data_size,
    uint64_t barrier_id) {
  STREAMING_LOG(INFO) << "Writer PingGlobalBarrier";
  writer->BroadcastBarrier(barrier_id, barrier_id, data, data_size);
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}

void StreamingRescaleWriterTestSuite::PingPartialBarrier(
    std::shared_ptr<DataWriter> writer, uint8_t *data, size_t data_size,
    uint64_t global_barrier_id, uint64_t barrier_id) {
  STREAMING_LOG(INFO) << "Writer PingPartialBarrier";
  writer->BroadcastPartialBarrier(global_barrier_id, barrier_id, data, data_size);
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}

void StreamingRescaleWriterTestSuite::StreamingRescaleExactlyOnceTest() {
  STREAMING_LOG(INFO)
      << "StreamingRescaleWriterTestSuite::StreamingRescaleExactlyOnceTest";
  for (auto &queue_id : queue_ids_) {
    STREAMING_LOG(INFO) << "queue_id: " << queue_id;
  }

  StreamingQueueInitialParameter param{peer_actor_id_,
                                       StreamingQueueTestSuite::ASYNC_CALL_FUNC,
                                       StreamingQueueTestSuite::SYNC_CALL_FUNC};
  std::vector<StreamingQueueInitialParameter> init_params(queue_ids_.size(), param);

  auto runtime_context = std::make_shared<RuntimeContext>();
  runtime_context->config_.SetEmptyMessageTimeInterval(100);
  runtime_context->config_.SetReliabilityLevel(ReliabilityLevel::EXACTLY_ONCE);
  runtime_context->config_.SetQueueType("streaming_queue");
  runtime_context->config_.SetPlasmaSocketPath(plasma_store_socket_);

  std::shared_ptr<DataWriter> writer(new DataWriter(runtime_context));
  std::vector<uint64_t> channel_seq_id_vec(queue_ids_.size(), 0);
  uint64_t queue_size = 1 * 1000 * 1000;
  writer->Init(queue_ids_, init_params, channel_seq_id_vec,
               std::vector<uint64_t>(queue_ids_.size(), queue_size));
  SetReceiver(writer);

  writer->Run();
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  size_t test_len = 8;
  uint8_t test_data1[8] = {0x01, 0x02, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  PingMessage(writer, queue_ids_, test_data1, test_len);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  PingGlobalBarrier(writer, test_data1, test_len, 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  STREAMING_LOG(INFO) << "streaming writer scaleup";
  // Append rescale queue ids to original queue ids
  std::vector<ray::ObjectID> scaleup_vec(queue_ids_);
  scaleup_vec.insert(scaleup_vec.begin(), rescale_queue_ids_.begin(),
                     rescale_queue_ids_.end());
  std::vector<StreamingQueueInitialParameter> scaleup_param_vec(init_params);
  scaleup_param_vec.resize(scaleup_vec.size(), param);

  writer->Rescale(scaleup_vec, scaleup_vec, scaleup_param_vec);
  // Wait reader rescale success
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  PingPartialBarrier(writer, test_data1, test_len, 1, 1);
  writer->ClearPartialCheckpoint(1, 1);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  uint8_t test_data2[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  PingMessage(writer, scaleup_vec, test_data2, test_len);

  STREAMING_LOG(INFO) << "streaming writer scaledown";

  std::vector<ray::ObjectID> scaledown_vec(scaleup_vec);
  scaledown_vec.erase(scaledown_vec.begin());
  std::vector<StreamingQueueInitialParameter> scaledown_param_vec(scaleup_param_vec);
  scaledown_param_vec.erase(scaledown_param_vec.begin());

  writer->Rescale(scaledown_vec, scaledown_param_vec);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  PingPartialBarrier(writer, test_data2, test_len, 1, 2);
  writer->ClearPartialCheckpoint(1, 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  uint8_t test_data3[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x08, 0x09, 0x06};
  PingMessage(writer, scaledown_vec, test_data3, test_len);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  PingGlobalBarrier(writer, test_data3, test_len, 2);
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));
  writer->ClearCheckpoint(1, 2);
  PingMessage(writer, scaledown_vec, test_data3, test_len);
  SetReceiver(nullptr);
  status_ = true;
}

void StreamingRescaleReaderTestSuite::PingMessage(std::shared_ptr<DataReader> reader,
                                                  std::vector<ray::ObjectID> queue_id_vec,
                                                  uint8_t *data, size_t data_size) {
  STREAMING_LOG(INFO) << "Reader PingMessage";
  for (size_t i = 0; i < queue_id_vec.size(); ++i) {
    std::shared_ptr<StreamingReaderBundle> msg;
    reader->GetBundle(-1, msg);
    STREAMING_LOG(INFO) << "PingMessage GetBundle return";
    STREAMING_CHECK(!msg->IsEmpty());
    StreamingMessageBundlePtr bundlePtr;
    bundlePtr = StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
    std::list<StreamingMessagePtr> message_list;
    bundlePtr->GetMessageList(message_list);
    STREAMING_LOG(WARNING) << "PingMessage bundle tostring: " << bundlePtr->ToString();
    ASSERT_EQ(std::memcmp(data, message_list.back()->Payload(), data_size), 0);
  }
}

void StreamingRescaleReaderTestSuite::PingGlobalBarrier(
    std::shared_ptr<DataReader> reader, uint8_t *data, size_t data_size,
    uint64_t barrier_id) {
  STREAMING_LOG(INFO) << "Reader PingGlobalBarrier";
  std::shared_ptr<StreamingReaderBundle> msg;
  reader->GetBundle(-1, msg);
  STREAMING_LOG(INFO) << "PingGlobalBarrier GetBundle return";
  STREAMING_CHECK(!msg->IsEmpty());
  StreamingMessageBundlePtr partial_barrier_bundle =
      StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
  std::list<StreamingMessagePtr> barrier_message_list;
  partial_barrier_bundle->GetMessageList(barrier_message_list);
  STREAMING_LOG(INFO) << "global barrier bundle => " << partial_barrier_bundle->ToString()
                      << ", from qid => " << msg->from << ", global barrier id => "
                      << msg->last_barrier_id;
  ASSERT_EQ(partial_barrier_bundle->IsBarrier(), true);
  ASSERT_EQ(barrier_message_list.back()->IsBarrier(), true);
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(barrier_message_list.back()->Payload(),
                                            &barrier_header);
  ASSERT_EQ(barrier_header.barrier_id, barrier_id);
  ASSERT_EQ(barrier_header.IsGlobalBarrier(), true);
  ASSERT_EQ(std::memcmp(data, barrier_message_list.back()->Payload() + kBarrierHeaderSize,
                        data_size),
            0);
}

void StreamingRescaleReaderTestSuite::PingPartialBarrier(
    std::shared_ptr<DataReader> reader, uint8_t *data, size_t data_size,
    uint64_t global_barrier_id, uint64_t barrier_id) {
  STREAMING_LOG(INFO) << "Reader PingPartialBarrier";
  std::shared_ptr<StreamingReaderBundle> msg;
  reader->GetBundle(-1, msg);
  STREAMING_LOG(INFO) << "PingPartialBarrier GetBundle return";
  STREAMING_CHECK(!msg->IsEmpty());
  StreamingMessageBundlePtr partial_barrier_bundle =
      StreamingMessageBundle::FromBytes(msg->MetaBuffer(), msg->DataBuffer());
  std::list<StreamingMessagePtr> barrier_message_list;
  partial_barrier_bundle->GetMessageList(barrier_message_list);
  STREAMING_LOG(INFO) << "partial barrier bundle => "
                      << partial_barrier_bundle->ToString() << ", from qid => "
                      << msg->from << ", barrier id => " << msg->last_barrier_id;
  ASSERT_EQ(partial_barrier_bundle->IsBarrier(), true);
  ASSERT_EQ(barrier_message_list.back()->IsBarrier(), true);
  StreamingBarrierHeader barrier_header;
  StreamingMessage::GetBarrierIdFromRawData(barrier_message_list.back()->Payload(),
                                            &barrier_header);
  ASSERT_EQ(barrier_header.partial_barrier_id, barrier_id);
  ASSERT_EQ(barrier_header.IsPartialBarrier(), true);
  ASSERT_EQ(std::memcmp(data, barrier_message_list.back()->Payload() + kBarrierHeaderSize,
                        data_size),
            0);
}

void StreamingRescaleReaderTestSuite::StreamingRescaleExactlyOnceTest() {
  STREAMING_LOG(INFO)
      << "StreamingRescaleReaderTestSuite::StreamingRescaleExactlyOnceTest";
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

  size_t test_len = 8;
  uint8_t test_data1[8] = {0x01, 0x02, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  STREAMING_LOG(INFO) << "Ping message from reader.";
  PingMessage(reader, queue_ids_, test_data1, test_len);
  STREAMING_LOG(INFO) << "Ping global barrier from reader.";
  PingGlobalBarrier(reader, test_data1, test_len, 1);

  STREAMING_LOG(INFO) << "Streaming reader scaleup.";
  // Append rescale queue ids to original queue ids
  std::vector<ray::ObjectID> scaleup_vec(queue_ids_);
  scaleup_vec.insert(scaleup_vec.begin(), rescale_queue_ids_.begin(),
                     rescale_queue_ids_.end());
  std::vector<StreamingQueueInitialParameter> scaleup_param_vec(init_params);
  scaleup_param_vec.resize(scaleup_vec.size(), param);

  reader->Rescale(scaleup_vec, scaleup_vec, scaleup_param_vec);
  PingPartialBarrier(reader, test_data1, test_len, 1, 1);
  reader->ClearPartialCheckpoint(1, 1);

  uint8_t test_data2[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x03, 0x08, 0x06};
  PingMessage(reader, scaleup_vec, test_data2, test_len);

  STREAMING_LOG(INFO) << "Streaming reader scaledown.";

  std::vector<ray::ObjectID> scaledown_vec(scaleup_vec);
  scaledown_vec.erase(scaledown_vec.begin());
  std::vector<StreamingQueueInitialParameter> scaledown_param_vec(scaleup_param_vec);
  scaledown_param_vec.erase(scaledown_param_vec.begin());

  reader->Rescale(scaledown_vec, scaledown_param_vec);
  PingPartialBarrier(reader, test_data2, test_len, 1, 2);
  reader->ClearPartialCheckpoint(1, 2);

  uint8_t test_data3[8] = {0x02, 0x01, 0x04, 0x05, 0x07, 0x08, 0x09, 0x06};
  PingMessage(reader, scaledown_vec, test_data3, test_len);
  PingGlobalBarrier(reader, test_data3, test_len, 2);
  PingMessage(reader, scaledown_vec, test_data3, test_len);
  SetReceiver(nullptr);
  status_ = true;
}

}  // namespace streaming
}  // namespace ray