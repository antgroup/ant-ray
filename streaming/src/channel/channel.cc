#include "channel.h"
namespace ray {
namespace streaming {

ProducerChannel::ProducerChannel(std::shared_ptr<Config> &transfer_config,
                                 ProducerChannelInfo &p_channel_info)
    : transfer_config_(transfer_config), channel_info_(p_channel_info) {}

ConsumerChannel::ConsumerChannel(std::shared_ptr<Config> &transfer_config,
                                 ConsumerChannelInfo &c_channel_info)
    : transfer_config_(transfer_config), channel_info_(c_channel_info) {}

StreamingQueueProducer::StreamingQueueProducer(
    std::shared_ptr<Config> &transfer_config, ProducerChannelInfo &p_channel_info,
    std::shared_ptr<UpstreamQueueMessageHandler> upstream_handler)
    : ProducerChannel(transfer_config, p_channel_info),
      queue_(nullptr),
      upstream_handler_(upstream_handler) {
  STREAMING_LOG(INFO) << "StreamingQueueProducer::StreamingQueueProducer";
}

StreamingQueueProducer::~StreamingQueueProducer() {
  DestroyTransferChannel();
  STREAMING_LOG(INFO) << "Streaming queue producer deconstructed";
}

StreamingStatus StreamingQueueProducer::CreateTransferChannel() {
  queue_id_ = channel_info_.channel_id;
  STREAMING_LOG(INFO) << "CreateQueue qid: " << channel_info_.channel_id
                      << " data_size: " << channel_info_.queue_size
                      << " actor_id: " << channel_info_.parameter.actor_id;

  if (upstream_handler_->UpstreamQueueExists(channel_info_.channel_id)) {
    RAY_LOG(INFO) << "StreamingQueueWriter::CreateQueue duplicate!!!";
    return StreamingStatus::OK;
  }

  queue_ = upstream_handler_->CreateUpstreamQueue(
      channel_info_.channel_id, channel_info_.parameter, channel_info_.queue_size,
      transfer_config_->GetInt32(ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE),
      transfer_config_->GetBool(ConfigEnum::ENABLE_COLLOCATE));
  STREAMING_LOG(INFO) << "StreamingQueueProducer::CreateTransferChannel done";
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::DestroyTransferChannel() {
  if (queue_) {
    upstream_handler_->DeleteUpstreamQueue(queue_id_);
    queue_ = nullptr;
  }
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ClearTransferCheckpoint(
    uint64_t checkpoint_offset) {
  queue_->SetQueueEvictionLimit(checkpoint_offset);
  return StreamingStatus::OK;
}

void StreamingQueueProducer::RefreshChannelInfo() {
  uint64_t consumed_message_id = queue_->GetMinConsumedMsgID();
  auto &queue_info = channel_info_.queue_info;
  if (consumed_message_id != static_cast<uint64_t>(-1)) {
    queue_info.consumed_message_id =
        std::max(queue_info.consumed_message_id, consumed_message_id);
  }
  uint64_t consumed_bundle_id = queue_->GetMinConsumedBundleID();
  if (consumed_bundle_id != static_cast<uint64_t>(-1)) {
    if (queue_info.consumed_bundle_id != static_cast<uint64_t>(-1)) {
      queue_info.consumed_bundle_id =
          std::max(queue_info.consumed_bundle_id, consumed_bundle_id);
    } else {
      queue_info.consumed_bundle_id = consumed_bundle_id;
    }
  }
}

void StreamingQueueProducer::RefreshProfilingInfo() {
  queue_->GetProfilingInfo(channel_info_.debug_infos);
}

uint64_t StreamingQueueProducer::GetLastReceivedMsgId() {
  uint64_t last_queue_msg_id, last_queue_seq_id;
  upstream_handler_->GetLastReceivedMsgId(queue_id_, last_queue_msg_id,
                                          last_queue_seq_id);
  return last_queue_msg_id;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(uint8_t *data,
                                                             uint32_t data_size,
                                                             uint64_t start_id,
                                                             uint64_t end_id) {
  STREAMING_CHECK(false) << "Nonsupport";
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::ProduceItemToChannel(
    StreamingMessageBundlePtr &bundle) {
  std::shared_ptr<ray::streaming::LocalMemoryBuffer> meta_buffer = bundle->MetaBuffer();
  uint64_t msg_id_end = bundle->GetLastMessageId();
  uint64_t msg_id_start =
      (bundle->GetMessageListSize() == 0 ? msg_id_end
                                         : msg_id_end - bundle->GetMessageListSize() + 1);
  auto &data_buffers = bundle->DataBuffers();
  StreamingStatus status = PushQueueItem(meta_buffer->Data(), meta_buffer->Size(),
                                         data_buffers.Data(), data_buffers.DataSize(),
                                         current_sys_time_ms(), msg_id_start, msg_id_end);

  if (status != StreamingStatus::OK) {
    STREAMING_LOG(DEBUG) << channel_info_.channel_id
                         << " => PushQueueItem failed, maybe queue is full"
                         << ", PushQueueItem status message => " << status;

    // Assume that only status FullChannel and OK are acceptable.
    // FullChannel means queue is full at that moment.
    STREAMING_CHECK(status == StreamingStatus::FullChannel ||
                    status == StreamingStatus::ElasticBufferRemain)
        << "status => " << status
        << ", perhaps data block is so large that it can't be stored in"
        << ", data block size => " << data_buffers.DataSize();

    return status;
  }
  return StreamingStatus::OK;
}
uint64_t StreamingQueueProducer::GetLastBundleId() { return queue_->GetCurrentSeqId(); }

StreamingStatus StreamingQueueProducer::PushQueueItem(
    uint8_t *meta_data, uint32_t meta_data_size, uint8_t *data, uint32_t data_size,
    uint64_t timestamp, uint64_t msg_id_start, uint64_t msg_id_end) {
  STREAMING_CHECK(queue_ != nullptr);

  STREAMING_CHECK(queue_->IsInitialized());
  StreamingStatus status = queue_->Push(meta_data, meta_data_size, data, data_size,
                                        timestamp, msg_id_start, msg_id_end);
  return status;
}

StreamingStatus StreamingQueueProducer::NotifyChannelConsumed(uint64_t offset_id) {
  queue_->SetQueueEvictionLimit(offset_id);
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueProducer::NotifyRemoveLimit(uint64_t offset_id) {
  queue_->SetQueueLimit(offset_id);
  return StreamingStatus::OK;
}

void StreamingQueueProducer::GetMetrics(
    std::unordered_map<std::string, double> &metrics) {
  metrics[std::string("writer.seq_id")] = queue_->GetCurrentSeqId();
  metrics[std::string("writer.evict_count")] = queue_->GetEvictCount();
  metrics[std::string("writer.evict_fail_count")] = queue_->GetEvictFailCount();
  metrics[std::string("writer.queue.max_sent_msg_id")] = queue_->GetLastSentMsgId();
  metrics[std::string("writer.queue.max_sent_seq_id")] = queue_->GetLastSentSeqId();
  metrics[std::string("writer.queue.min_consumed_msg_id")] =
      queue_->GetMinConsumedMsgID();
  metrics[std::string("writer.queue.eviction_limit")] = queue_->EvictionLimit();
  metrics[std::string("writer.queue.ignored_eviction_limit")] =
      queue_->GetIgnoredEvictLimit();
  metrics[std::string("writer.queue.reconnected_count")] = queue_->GetReconnectedCount();
  metrics[std::string("writer.queue.is_collocate")] = queue_->IsCollocate();
  metrics[std::string("writer.queue.initialized")] = queue_->IsInitialized();
  metrics[std::string("writer.queue.push_elasticbuffer_count")] =
      queue_->GetPushElasticBufferCount();
  metrics[std::string("writer.queue.clear_elasticbuffer_count")] =
      queue_->GetClearElasticBufferCount();
  metrics[std::string("writer.queue.push_elasitcbuffer_size")] =
      queue_->GetPushElasticBufferSize();
}

std::shared_ptr<BufferPool> StreamingQueueProducer::GetBufferPool() {
  // Wait until the queue is ready.
  if (queue_->IsInitialized()) {
    return queue_->GetBufferPool();
  } else {
    return nullptr;
  }
}

bool StreamingQueueProducer::IsInitialized() {
  return queue_ ? queue_->IsInitialized() : false;
}

void StreamingQueueProducer::GetBundleLargerThan(
    uint64_t msg_id, std::vector<StreamingMessageBundlePtr> &bundle_vec) {
  std::vector<QueueItem> item_vec;
  auto cyclic_queue = std::dynamic_pointer_cast<CyclicWriterQueue>(queue_);
  STREAMING_CHECK(cyclic_queue) << "No such valid cyclic queue.";
  cyclic_queue->GetItemsLargeThan(msg_id, item_vec);
  for (auto &item : item_vec) {
    StreamingMessageBundlePtr bundle_ptr = StreamingMessageBundle::FromBytes(
        item.MetaBuffer()->Data(), item.Buffer() ? item.Buffer()->Data() : nullptr);
    bundle_vec.push_back(bundle_ptr);
  }
}

int StreamingQueueProducer::Evict(uint32_t data_size) { return queue_->Evict(data_size); }

double StreamingQueueProducer::GetUsageRate() { return queue_->GetUsageRate(); }

StreamingQueueConsumer::StreamingQueueConsumer(
    std::shared_ptr<Config> &transfer_config, ConsumerChannelInfo &c_channel_info,
    std::shared_ptr<DownstreamQueueMessageHandler> downstream_handler)
    : ConsumerChannel(transfer_config, c_channel_info),
      queue_(nullptr),
      downstream_handler_(downstream_handler) {
  STREAMING_LOG(INFO) << "StreamingQueueConsumer Init.";
}

StreamingQueueConsumer::~StreamingQueueConsumer() {
  DestroyTransferChannel();
  STREAMING_LOG(INFO) << "StreamingQueueConsumer Destroy";
}

StreamingQueueStatus StreamingQueueConsumer::GetQueue(
    const ObjectID &queue_id, uint64_t checkpoint_id, uint64_t start_msg_id,
    StreamingQueueInitialParameter &init_param) {
  STREAMING_LOG(INFO) << "GetQueue qid: " << queue_id << " start_msg_id: " << start_msg_id
                      << " actor_id: " << init_param.actor_id;

  if (downstream_handler_->DownstreamQueueExists(queue_id)) {
    RAY_LOG(INFO) << "StreamingQueueReader:: Already got this queue.";
    return StreamingQueueStatus::OK;
  }

  STREAMING_LOG(INFO) << "Create ReaderQueue " << queue_id
                      << " pull from start_msg_id: " << start_msg_id;
  queue_ = downstream_handler_->CreateDownstreamQueue(queue_id, init_param);
  STREAMING_CHECK(queue_ != nullptr);

  bool is_first_pull;
  uint64_t count;
  return downstream_handler_->PullQueue(queue_id, checkpoint_id, start_msg_id,
                                        is_first_pull, count, /*timout ms*/ 5000);
}

TransferCreationStatus StreamingQueueConsumer::CreateTransferChannel() {
  StreamingQueueStatus status =
      GetQueue(channel_info_.channel_id, channel_info_.barrier_id,
               channel_info_.current_message_id + 1, channel_info_.parameter);

  if (status == StreamingQueueStatus::OK) {
    return TransferCreationStatus::PullOk;
  } else if (status == StreamingQueueStatus::
                           NoValidData /*|| status == StreamingQueueStatus::FreshPull*/) {
    return TransferCreationStatus::FreshStarted;
  } else if (status == StreamingQueueStatus::Timeout) {
    return TransferCreationStatus::Timeout;
  } else if (status == StreamingQueueStatus::DataLost) {
    return TransferCreationStatus::DataLost;
  }
  STREAMING_LOG(FATAL) << "Invalid StreamingQueueStatus, status=" << status;
  return TransferCreationStatus::Invalid;
}

StreamingStatus StreamingQueueConsumer::DestroyTransferChannel() {
  if (queue_) {
    downstream_handler_->DeleteDownstreamQueue(channel_info_.channel_id);
    queue_ = nullptr;
  }
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ClearTransferCheckpoint(
    uint64_t checkpoint_id, uint64_t checkpoint_offset) {
  return StreamingStatus::OK;
}

void StreamingQueueConsumer::RefreshChannelInfo() {
  auto &queue_info = channel_info_.queue_info;
  queue_info.last_message_id = queue_->GetLastRecvMsgId();
}

void StreamingQueueConsumer::RefreshProfilingInfo() {
  queue_->GetProfilingInfo(channel_info_.debug_infos);
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(
    uint64_t &offset_id, uint8_t *&data, uint32_t &data_size, uint32_t timeout,
    uint64_t &item_received_ts) {
  STREAMING_CHECK(false) << "Nonsupport";
  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::ConsumeItemFromChannel(
    std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout,
    uint64_t &item_received_ts) {
  QueueItem item;
  if (timeout == 0) {
    item = queue_->PopPending();
  } else {
    item = queue_->PopPendingBlockTimeout(timeout * 1000);
  }
  if (item.SeqId() == QUEUE_INVALID_SEQ_ID) {
    STREAMING_LOG(DEBUG) << "GetQueueItem timeout.";
    item_received_ts = item.TimestampReceived();
    return StreamingStatus::OK;
  }

  message->FillBundle(item.MetaBuffer(), item.Buffer());
  message->bundle_id = item.SeqId();

  item_received_ts = item.TimestampReceived();

  STREAMING_LOG(DEBUG) << "GetQueueItem qid: " << channel_info_.channel_id
                       << " seq_id: " << item.SeqId() << " msg_id: (" << item.MsgIdStart()
                       << "," << item.MsgIdEnd() << ")"
                       << " data_size: " << item.DataSize();

  return StreamingStatus::OK;
}

StreamingStatus StreamingQueueConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  queue_->OnConsumed(offset_id, channel_info_.queue_info.consumed_bundle_id);
  return StreamingStatus::OK;
}

void StreamingQueueConsumer::GetMetrics(
    std::unordered_map<std::string, double> &metrics) {
  metrics[std::string("reader.seq_id")] = queue_->GetLastRecvSeqId();
  metrics[std::string("reader.last_consumer_pop_msg_id")] = queue_->GetLastPopMsgId();
  metrics[std::string("reader.last_consumer_pop_seq_id")] = queue_->GetLastPopSeqId();
  metrics[std::string("reader.queue.last_item_latency")] = queue_->GetLastItemLatency();
  metrics[std::string("reader.queue.last_item_transfer_latency")] =
      queue_->GetLastItemTransferLatency();
  metrics[std::string("reader.queue.last_item_latency_large_than_10ms")] =
      queue_->GetLastItemLatencyLargeThan10ms();
  metrics[std::string("reader.queue.last_item_latency_large_than_5ms")] =
      queue_->GetLastItemLatencyLargeThan5ms();
  metrics[std::string("reader.queue.reconnected_count")] = queue_->GetReconnectedCount();
  metrics[std::string("reader.queue.notify_count")] = queue_->GetNotifyCount();
  metrics[std::string("reader.queue.pending_count")] = queue_->PendingCount();
}

void StreamingQueueConsumer::NotifyStartLogging(uint64_t barrier_id, uint64_t msg_id,
                                                uint64_t seq_id) {
  queue_->NotifyStartLogging(barrier_id, msg_id, seq_id);
}

// For mock queue transfer
struct MockQueueItem {
  uint64_t bundle_id;
  uint64_t message_id;
  uint32_t data_size;
  std::shared_ptr<uint8_t> data;
};

std::once_flag mock_queue_flag;
class MockQueue {
 public:
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBuffer<MockQueueItem>>>
      message_buffer;
  std::unordered_map<ObjectID, std::shared_ptr<AbstractRingBuffer<MockQueueItem>>>
      consumed_buffer;
  std::unordered_map<ObjectID, StreamingQueueInfo> queue_info_map;
  static std::mutex mutex;
  static MockQueue *mock_queue;
  static MockQueue &GetMockQueue() {
    // NOTE(lingxuan.zlx): new a heap object to gurantee mock queue will not be
    // deconstruted in unstable order.
    std::call_once(mock_queue_flag, []() { MockQueue::mock_queue = new MockQueue(); });
    return *mock_queue;
  }
};
MockQueue *MockQueue::mock_queue = nullptr;
std::mutex MockQueue::mutex;

StreamingStatus MockProducer::CreateTransferChannel() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  mock_queue.message_buffer[channel_info_.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(10000);
  mock_queue.consumed_buffer[channel_info_.channel_id] =
      std::make_shared<RingBufferImplThreadSafe<MockQueueItem>>(10000);
  STREAMING_LOG(INFO) << "ConfigEnum::BUFFER_POOL_SIZE: "
                      << transfer_config_->GetInt32(ConfigEnum::BUFFER_POOL_SIZE);
  STREAMING_LOG(INFO) << "ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE: "
                      << transfer_config_->GetInt32(
                             ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE);
  buffer_pool_ = std::make_shared<BufferPool>(
      transfer_config_->GetInt32(ConfigEnum::BUFFER_POOL_SIZE),
      transfer_config_->GetInt32(ConfigEnum::BUFFER_POOL_MIN_BUFFER_SIZE));
  return StreamingStatus::OK;
}

MockProducer::~MockProducer() {
  DestroyTransferChannel();
  STREAMING_LOG(INFO) << "Mock queue producer deconstructed";
}

StreamingStatus MockProducer::DestroyTransferChannel() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  mock_queue.message_buffer.erase(channel_info_.channel_id);
  mock_queue.consumed_buffer.erase(channel_info_.channel_id);
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::ProduceItemToChannel(uint8_t *data, uint32_t data_size,
                                                   uint64_t start_id, uint64_t end_id) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &ring_buffer = mock_queue.message_buffer[channel_info_.channel_id];
  if (ring_buffer->Full()) {
    return StreamingStatus::OutOfMemory;
  }
  MockQueueItem item;
  item.message_id = end_id;
  item.data.reset(new uint8_t[data_size]);
  item.data_size = data_size;
  current_bundle_id_ = channel_info_.current_bundle_id + 1;
  item.bundle_id = current_bundle_id_;
  STREAMING_LOG(DEBUG) << "Produce item bundle id " << item.bundle_id;
  std::memcpy(item.data.get(), data, data_size);
  ring_buffer->Push(item);
  mock_queue.queue_info_map[channel_info_.channel_id].last_message_id = item.message_id;
  return StreamingStatus::OK;
}

StreamingStatus MockProducer::ProduceItemToChannel(StreamingMessageBundlePtr &bundle) {
  auto buffer = new uint8_t[bundle->ClassBytesSize()];
  bundle->ToBytes(buffer);
  STREAMING_LOG(DEBUG) << "DataBuffers size " << bundle->DataBuffers().DataSize()
                       << " bundle size " << bundle->ClassBytesSize() << " message id "
                       << bundle->GetLastMessageId() << " message list size "
                       << bundle->GetMessageListSize();
  auto status =
      ProduceItemToChannel(buffer, bundle->ClassBytesSize(),
                           bundle->GetLastMessageId() - bundle->GetMessageListSize(),
                           bundle->GetLastMessageId());
  if (status == StreamingStatus::OK) {
    bundle->DataBuffers().Release(buffer_pool_);
  }
  delete[] buffer;
  return status;
}

void MockProducer::RefreshChannelInfo() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  channel_info_.queue_info.consumed_message_id =
      mock_queue.queue_info_map[channel_info_.channel_id].consumed_message_id;
  channel_info_.queue_info.consumed_bundle_id =
      mock_queue.queue_info_map[channel_info_.channel_id].consumed_bundle_id;
}

StreamingStatus MockConsumer::ConsumeItemFromChannel(uint64_t &offset_id, uint8_t *&data,
                                                     uint32_t &data_size,
                                                     uint32_t timeout,
                                                     uint64_t &item_received_ts) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &channel_id = channel_info_.channel_id;
  if (mock_queue.message_buffer.find(channel_id) == mock_queue.message_buffer.end()) {
    return StreamingStatus::NoSuchItem;
  }
  if (mock_queue.message_buffer[channel_id]->Empty()) {
    return StreamingStatus::NoSuchItem;
  }
  MockQueueItem item = mock_queue.message_buffer[channel_id]->Front();
  mock_queue.message_buffer[channel_id]->Pop();
  mock_queue.consumed_buffer[channel_id]->Push(item);
  offset_id = item.message_id;
  data_size = item.data_size;
  data = (uint8_t *)malloc(data_size);
  memcpy(data, item.data.get(), data_size);
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::ConsumeItemFromChannel(
    std::shared_ptr<StreamingReaderBundle> &message, uint32_t timeout,
    uint64_t &timestamp) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &channel_id = channel_info_.channel_id;
  if (mock_queue.message_buffer.find(channel_id) == mock_queue.message_buffer.end()) {
    return StreamingStatus::NoSuchItem;
  }

  if (mock_queue.message_buffer[channel_id]->Empty()) {
    return StreamingStatus::NoSuchItem;
  }
  auto item = mock_queue.message_buffer[channel_id]->Front();
  mock_queue.message_buffer[channel_id]->Pop();
  mock_queue.consumed_buffer[channel_id]->Push(item);

  message->FillBundle(std::make_shared<LocalMemoryBuffer>(
                          item.data.get(), kMessageBundleHeaderSize, true, true),
                      std::make_shared<LocalMemoryBuffer>(
                          item.data.get() + kMessageBundleHeaderSize,
                          item.data_size - kMessageBundleHeaderSize, true, true));
  message->bundle_id = item.bundle_id;
  STREAMING_LOG(DEBUG) << "Read message bundle id " << message->bundle_id;
  return StreamingStatus::OK;
}

StreamingStatus MockConsumer::NotifyChannelConsumed(uint64_t offset_id) {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  auto &channel_id = channel_info_.channel_id;
  auto buffer_it = mock_queue.consumed_buffer.find(channel_id);

  if (buffer_it == mock_queue.consumed_buffer.end()) {
    STREAMING_LOG(WARNING)
        << "Reader consumed buffer has not been initialized yet, return ok.";
    return StreamingStatus::OK;
  }

  auto ring_buffer = buffer_it->second;

  while (!ring_buffer->Empty() && ring_buffer->Front().message_id < offset_id) {
    ring_buffer->Pop();
  }
  mock_queue.queue_info_map[channel_id].consumed_bundle_id =
      channel_info_.queue_info.consumed_bundle_id;
  return StreamingStatus::OK;
}

void MockConsumer::RefreshChannelInfo() {
  std::unique_lock<std::mutex> lock(MockQueue::mutex);
  MockQueue &mock_queue = MockQueue::GetMockQueue();
  channel_info_.queue_info.last_message_id =
      mock_queue.queue_info_map[channel_info_.channel_id].last_message_id;
}

}  // namespace streaming
}  // namespace ray
